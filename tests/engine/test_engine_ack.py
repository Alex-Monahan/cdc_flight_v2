"""Acknowledgement invariant, restarts at random moments and at the Java crash points, frozen reader (jvm, localpg)."""
import random
import threading
import time

import pytest

from cdc_v2 import crashpoints
from cdc_v2 import lsn as L
from cdc_v2.deadline import Deadline
from cdc_v2.stream import frames as F
from cdc_v2.stream.engine import Engine, EngineExited, EngineFatal
from conftest import LONG
from stream_helpers import Collector, evidence, rss_kb, wait_for

pytestmark = [pytest.mark.jvm, pytest.mark.localpg, pytest.mark.timeout(1800)]


class Probe(threading.Thread):
    """Reads the slot's confirmed_flush_lsn every 50 ms."""

    def __init__(self, pair, slot):
        super().__init__(daemon=True)
        self.pair, self.slot = pair, slot
        self.samples = []           # (monotonic time after the read, confirmed_flush int)
        self.stop_ev = threading.Event()

    def run(self):
        with self.pair.connect(self.pair.replica_port) as c:
            while not self.stop_ev.is_set():
                v = c.execute("select confirmed_flush_lsn::text from pg_replication_slots where slot_name = %s",
                              (self.slot,)).fetchone()[0]
                self.samples.append((time.monotonic(), L.parse(v)))
                time.sleep(0.05)

    def stop(self):
        self.stop_ev.set()
        self.join(10)


class Writer(threading.Thread):
    def __init__(self, pair, schema, rate=50):
        super().__init__(daemon=True)
        self.pair, self.schema, self.rate = pair, schema, rate
        self.stop_ev = threading.Event()
        self.n = 0

    def run(self):
        with self.pair.connect(self.pair.primary_port) as c:
            while not self.stop_ev.is_set():
                self.n += 1
                c.execute("insert into %s.a values (%d, 'v')" % (self.schema, self.n))
                c.execute("insert into %s.unpub values (%d)" % (self.schema, self.n))
                time.sleep(1.0 / self.rate)

    def stop(self):
        self.stop_ev.set()
        self.join(10)


def test_slot_never_ahead_of_durable_and_follows_it(pair, scenario, artifacts):
    sc = scenario("create table {s}.a (id int primary key, v text)", "create table {s}.unpub (id int)")
    pair.p("alter publication %s drop table %s.unpub" % (sc.pub, sc.schema))
    start_cf = L.parse(pair.slot(sc.slot)[0])
    sent = []                                   # (monotonic time before durable(), position)
    probe = Probe(pair, sc.slot)
    writer = Writer(pair, sc.schema)
    with Engine(sc.settings(), None, artifacts) as eng:
        col = Collector(eng)
        col.keep_frames = False
        col.run(lambda c: eng.hello is not None, timeout=60)
        probe.start()
        writer.start()
        # phase 1: 6 s without any DURABLE: the slot must not move
        t_end = time.monotonic() + 6
        while time.monotonic() < t_end:
            for f in eng.frames(Deadline.after(0.5)):
                col.feed(f)
        phase1 = [v for _t, v in probe.samples]
        assert set(phase1) == {start_cf}, "the slot moved without any DURABLE"
        # phase 2: DURABLE for each END, lagging three transactions behind, while writes continue
        busy_delays = []
        t_end = time.monotonic() + (60 if LONG else 15)
        done = 0
        while time.monotonic() < t_end:
            for f in eng.frames(Deadline.after(0.2)):
                col.feed(f)
            ready = col.txns[:max(0, len(col.txns) - 3)]
            if len(ready) > done:
                p = ready[-1].end.ack_position
                sent.append((time.monotonic(), p))
                eng.durable(p)
                done = len(ready)
        writer.stop()
        # idle: DURABLE for the last END, measure the delay until the slot shows it
        for f in eng.frames(Deadline.after(2)):
            col.feed(f)
        last = col.txns[-1].end.ack_position
        t_idle = time.monotonic()
        sent.append((t_idle, last))
        eng.durable(last)
        while True:
            for f in eng.frames(Deadline.after(0.2)):
                col.feed(f)
            if probe.samples and probe.samples[-1][1] >= last:
                break
            assert time.monotonic() - t_idle < 30, "the slot did not follow the last DURABLE"
        idle_delay = time.monotonic() - t_idle
        # heartbeats while only unpublished tables are written
        hb_before = col.heartbeats[-1].ack_position if col.heartbeats else None
        with pair.connect(pair.primary_port) as c:
            for i in range(30):
                c.execute("insert into %s.unpub values (%d)" % (sc.schema, 100000 + i))
                time.sleep(0.2)
        for f in eng.frames(Deadline.after(7)):
            col.feed(f)
        hb_after = col.heartbeats[-1].ack_position
        probe.stop()
        eng.stop(Deadline.after(20))
    # invariant at every sample: confirmed_flush <= the newest DURABLE sent before the sample was taken
    violations = 0
    delays = []
    for t, v in probe.samples:
        allowed = max([p for ts, p in sent if ts <= t] + [start_cf])
        if v > allowed:
            violations += 1
    for ts, p in sent[:-1]:
        reached = next((t for t, v in probe.samples if t >= ts and v >= p), None)
        if reached is not None:
            delays.append(reached - ts)
    assert violations == 0
    assert all(col.txns[i].end.event_count == len(col.txns[i].rows) for i in range(len(col.txns)))
    delays.sort()
    evidence("ack_invariant.json", {
        "test": "tests/engine/test_engine_ack.py::test_slot_never_ahead_of_durable_and_follows_it",
        "samples": len(probe.samples), "durables_sent": len(sent), "violations": violations,
        "transactions": len(col.txns), "phase1_slot_moved": False,
        "busy_delay_s": {"p50": round(delays[len(delays) // 2], 3), "max": round(delays[-1], 3)} if delays else None,
        "idle_delay_s": round(idle_delay, 3), "heartbeat_interval_ms": 5000, "poll_interval_ms": 50,
        "heartbeat_ack_before_unpublished_writes": hb_before and L.format(hb_before),
        "heartbeat_ack_after_unpublished_writes": hb_after and L.format(hb_after),
        "heartbeat_moved_with_unpublished_wal": bool(hb_before is not None and hb_after > hb_before)})


def run_with_restarts(pair, sc, artifacts, plan):
    """A pipeline in miniature: applies whole transactions with ack_position above its applied position, sends
    DURABLE after each, and survives engine deaths (planned kills, crash points). A writer commits transactions of
    three rows all the time until the last step."""
    applied_pos = L.parse(pair.slot(sc.slot)[0])
    applied = {}
    restarts = []
    stop_writing = threading.Event()
    written = {"txns": 0}

    def write():
        with pair.connect(pair.primary_port) as c:
            i = 0
            while not stop_writing.is_set():
                c.execute("insert into %s.a select g, 'v' from generate_series(%d, %d) g" % (sc.schema, i * 3, i * 3 + 2))
                i += 1
                written["txns"] = i
                time.sleep(0.01)

    wt = threading.Thread(target=write, daemon=True)
    wt.start()
    try:
        for step in plan:
            if step.get("until_done"):
                stop_writing.set()
                wt.join(30)
            cf_at_start = L.parse(pair.slot(sc.slot)[0])
            assert cf_at_start <= applied_pos
            env = crashpoints.env_for(**step["crash"]) if step.get("crash") else None
            first = None
            died = None
            eng = Engine(sc.settings(heartbeat_interval_ms=300), None, artifacts, env=env)
            col = Collector(eng)
            col.keep_frames = False
            t0 = time.monotonic()
            try:
                while True:
                    if step.get("kill_after") is not None and time.monotonic() - t0 > step["kill_after"]:
                        eng.kill()
                        died = "killed"
                        break
                    try:
                        for f in eng.frames(Deadline.after(0.2)):
                            col.feed(f)
                            if isinstance(f, F.End):
                                t = col.txns[-1]
                                if first is None:
                                    first = t
                                assert t.end.event_count == len(t.rows)
                                if t.end.ack_position <= applied_pos:
                                    continue              # redelivered, already applied
                                for _tb, r in t.rows:
                                    assert r["a0"] not in applied, "applied twice"
                                    applied[r["a0"]] = True
                                applied_pos = t.end.ack_position
                                eng.durable(applied_pos)
                    except (EngineExited, EngineFatal, F.EngineBroken) as e:
                        died = "%s %s" % (type(e).__name__, getattr(e, "status", ""))
                        break
                    if step.get("until_done") and len(applied) == 3 * written["txns"]:
                        eng.stop(Deadline.after(20))
                        died = "stopped"
                        break
                    assert time.monotonic() - t0 < step.get("timeout", 90), "no progress in step %r" % (step,)
            finally:
                eng.kill()
            restarts.append({"step": step, "how": died, "cf_at_start": L.format(cf_at_start),
                             "first_txn_ack": first and L.format(first.end.ack_position),
                             "first_after_cf": first is None or first.end.ack_position > cf_at_start,
                             "applied_rows": len(applied)})
            if first is not None:
                assert first.end.ack_position > cf_at_start      # the first transaction is the one after the last ack
    finally:
        stop_writing.set()
    assert sorted(applied) == list(range(3 * written["txns"]))
    return restarts


def test_restart_after_random_kills_and_java_crash_points(pair, scenario, artifacts):
    sc = scenario("create table {s}.a (id int primary key, v text)")
    rng = random.Random(4242)
    plan = [{"kill_after": rng.uniform(1.5, 6.0)} for _ in range(6 if LONG else 3)]
    for point in ("engine.ack_marking", "engine.acked"):
        for seed in (1, 2, 3):
            plan.append({"crash": {"point": point, "hit": "seed:%d:%d" % (seed, 5), "action": "kill"}})
    plan.append({"until_done": True, "timeout": 300})
    restarts = run_with_restarts(pair, sc, artifacts, plan)
    crashed = [r for r in restarts if r["how"] and r["how"].startswith("EngineExited") and "-9" in r["how"]]
    assert len(crashed) == 6, restarts                  # every armed Java point was reached and killed the JVM
    evidence("restarts.json", {"test": "tests/engine/test_engine_ack.py::test_restart_after_random_kills_and_java_crash_points",
                               "seed": 4242, "runs": restarts})


def test_frozen_reader(pair, scenario, artifacts):
    """The reader stops for longer than wal_sender_timeout: resident memory stays flat, the walsender stays
    connected, then everything arrives."""
    freeze_s = 180 if LONG else 75
    sc = scenario("create table {s}.f (id bigint primary key, v text)")
    with Engine(sc.settings(heap_mb=512), None, artifacts) as eng:
        col = Collector(eng)
        col.keep_frames = False
        col.run(lambda c: eng.hello is not None, timeout=60)
        pid0 = pair.slot(sc.slot)[2]
        n = 300_000
        pair.p("insert into %s.f select g, md5(g::text) from generate_series(1, %d) g" % (sc.schema, n))
        samples = []
        t0 = time.monotonic()
        while time.monotonic() - t0 < freeze_s:
            slot = pair.slot(sc.slot)
            samples.append({"t": round(time.monotonic() - t0, 1), "rss_mb": round(rss_kb(eng.pid) / 1024, 1),
                            "slot_active_pid": slot[2]})
            time.sleep(5)
        assert all(s["slot_active_pid"] == pid0 for s in samples), "the walsender changed or went away"
        col.run(lambda c: sum(len(t.rows) for t in c.txns) >= n, timeout=300)
        eng.stop(Deadline.after(20))
    rss = [s["rss_mb"] for s in samples[2:]]
    assert max(rss) - min(rss) < 64, samples           # flat while frozen
    evidence("frozen_reader.json", {"test": "tests/engine/test_engine_ack.py::test_frozen_reader",
                                    "freeze_s": freeze_s, "rows": n, "wal_sender_timeout": "60s (default)",
                                    "samples": samples, "all_rows_arrived": True})
