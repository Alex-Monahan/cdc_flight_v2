"""Acknowledgement (jvm, localpg): only END records are acknowledged (plan C2), the held list keeps following,
restarts at random moments and at the Java crash points, a negative control, DURABLE edge cases, the idle bound,
unpublished WAL, frozen reader.

The pipeline stand-in follows the contract: its applied position is the ack_position of the last END it has
applied; it lags a few transactions; it sends DURABLE only for that position; it ignores HEARTBEAT frames; at a
kill it drops what it had not applied and every unread frame.
"""
import collections
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
from stream_helpers import Collector, evidence, rss_kb

pytestmark = [pytest.mark.jvm, pytest.mark.localpg, pytest.mark.timeout(3600)]


class Probe(threading.Thread):
    """Reads the slot's confirmed_flush_lsn every 25 ms."""

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
                time.sleep(0.025)

    def stop(self):
        self.stop_ev.set()
        self.join(10)

    def latest(self):
        return self.samples[-1][1] if self.samples else None


def violations(samples, history, start):
    """Samples where confirmed_flush_lsn is above the newest applied position the pipeline had before the read."""
    out, i, allowed = 0, 0, start
    for t, v in samples:
        while i < len(history) and history[i][0] <= t:
            allowed = max(allowed, history[i][1])
            i += 1
        out += v > allowed
    return out


class Pipeline:
    """The stand-in described in the module docstring. `ahead=1` is the negative control: it sends DURABLE for the
    END after the last one it applied, which must make the checks fail."""

    def __init__(self, pair, sc, artifacts, lag=3, ahead=0, heartbeat_ms=300):
        self.pair, self.sc, self.artifacts = pair, sc, artifacts
        self.lag, self.ahead, self.heartbeat_ms = lag, ahead, heartbeat_ms
        self.start = L.parse(pair.slot(sc.slot)[0])
        self.applied_pos = self.start
        self.history = []                        # (monotonic time when sent, position) of every DURABLE
        self.applied_history = []                # (monotonic time, applied position)
        self.rows = collections.Counter()
        self.msgs = collections.Counter()
        self.lives = []
        self.heartbeats = 0
        self.heartbeats_before_their_end = 0

    def apply(self, txn, buffer, eng):
        for _tb, r in txn.rows:
            self.rows[r["a0"]] += 1
        for m in txn.messages:
            self.msgs[m.content] += 1
        self.applied_pos = txn.end.ack_position
        self.applied_history.append((time.monotonic(), self.applied_pos))
        p = self.applied_pos
        if self.ahead and buffer:
            p = buffer[0].end.ack_position      # negative control: one END too far
        self.history.append((time.monotonic(), p))
        eng.durable(p)

    def life(self, kill_after=None, crash=None, done=None, timeout=300):
        env = crashpoints.env_for(**crash) if crash else None
        cf_start = L.parse(self.pair.slot(self.sc.slot)[0])
        eng = Engine(self.sc.settings(heartbeat_interval_ms=self.heartbeat_ms), None, self.artifacts, env=env)
        col = Collector(eng)
        col.keep_frames = False
        buffer = collections.deque()
        first, how, idle_since = None, None, None
        last_hb_commit = None
        t0 = time.monotonic()
        try:
            while True:
                if kill_after is not None and time.monotonic() - t0 > kill_after:
                    how = "killed"
                    break
                got = False
                try:
                    for f in eng.frames(Deadline.after(0.1)):
                        got = got or not isinstance(f, (F.Heartbeat, F.BatchEnd))   # idle = no data frames
                        if isinstance(f, F.Heartbeat):
                            self.heartbeats += 1
                            last_hb_commit = f.positions.get("lsn_commit")
                            continue                  # never moves the applied position
                        col.feed(f)
                        if isinstance(f, F.End):
                            txn = col.txns.pop()
                            if last_hb_commit is not None and last_hb_commit == f.ack_position:
                                self.heartbeats_before_their_end += 1
                            assert f.event_count == len(txn.rows) + len(txn.messages) + len(txn.truncates)
                            if first is None:
                                first = txn
                            if f.ack_position <= self.applied_pos:
                                continue              # redelivered, already applied
                            buffer.append(txn)
                            while len(buffer) > self.lag:
                                self.apply(buffer.popleft(), buffer, eng)
                except (EngineExited, EngineFatal, F.EngineBroken) as e:
                    how = "%s %s" % (type(e).__name__, getattr(e, "status", ""))
                    break
                if done is not None:
                    idle_since = None if got else (idle_since or time.monotonic())
                    if idle_since and time.monotonic() - idle_since > 1.5:
                        while buffer:                 # nothing more arrives: apply the rest
                            self.apply(buffer.popleft(), buffer, eng)
                        if done(self):
                            eng.stop(Deadline.after(20))
                            how = "stopped"
                            break
                assert time.monotonic() - t0 < timeout, "no end of this engine life"
        finally:
            eng.kill()                                # unread frames and the unapplied buffer are dropped
        self.lives.append({"how": how, "kill_after": kill_after, "crash": crash, "cf_at_start": L.format(cf_start),
                           "first_txn_ack": first and L.format(first.end.ack_position),
                           "rows_applied": sum(self.rows.values())})
        if first is not None:
            assert first.end.ack_position > cf_start  # the first transaction is the one after the last ack
        return how


def write_backlog(pair, schema, rows, every_message=20, start=0):
    """Single-row transactions; every `every_message`-th transaction holds only a transactional message."""
    msgs = []
    with pair.connect(pair.primary_port) as c:
        for i in range(start, start + rows):
            c.execute("insert into %s.a values (%d, 'v')" % (schema, i))
            if every_message and i % every_message == 0:
                m = "m%d" % i
                c.execute("select pg_logical_emit_message(true, 'cdc_v2_t', %s)", (m,))
                msgs.append(m.encode())
    return msgs


def test_end_only_acknowledgement_under_a_heartbeat_storm(pair, scenario, artifacts):
    """Second review, item 6(a): a backlog of 40,000 single-row transactions plus 2,000 message-only transactions,
    heartbeat.interval.ms=1, kills at random points."""
    n = 40_000
    sc = scenario("create table {s}.a (id int primary key, v text)")
    msgs = write_backlog(pair, sc.schema, n)
    pipe = Pipeline(pair, sc, artifacts, lag=3, heartbeat_ms=1)
    probe = Probe(pair, sc.slot)
    probe.start()
    rng = random.Random(6061)
    try:
        for _ in range(4 if LONG else 3):
            pipe.life(kill_after=rng.uniform(2.0, 8.0))
        pipe.life(done=lambda p: sum(p.rows.values()) >= n and sum(p.msgs.values()) >= len(msgs), timeout=900)
    finally:
        probe.stop()
    bad = violations(probe.samples, pipe.history, pipe.start)
    assert bad == 0
    assert sorted(pipe.rows) == list(range(n)) and set(pipe.rows.values()) == {1}
    assert sorted(pipe.msgs) == sorted(msgs) and set(pipe.msgs.values()) == {1}
    evidence("end_only_heartbeat_storm.json", {
        "test": "tests/engine/test_engine_ack.py::test_end_only_acknowledgement_under_a_heartbeat_storm",
        "single_row_transactions": n, "message_only_transactions": len(msgs), "heartbeat_interval_ms": 1,
        "lag_transactions": 3, "seed": 6061, "lives": pipe.lives, "probe_samples": len(probe.samples),
        "violations": bad, "heartbeat_frames": pipe.heartbeats,
        "heartbeats_carrying_the_position_of_the_next_end": pipe.heartbeats_before_their_end,
        "missing": 0, "doubled": 0})


def continuous_writer(pair, schema, stop_ev, written):
    with pair.connect(pair.primary_port) as c:
        i = 0
        while not stop_ev.is_set():
            c.execute("insert into %s.a values (%d, 'v')" % (schema, i))
            i += 1
            written["rows"] = i
            time.sleep(0.003)


def run_pipeline_with_writer(pair, sc, artifacts, plan, ahead=0, final_done=None):
    pipe = Pipeline(pair, sc, artifacts, lag=3, ahead=ahead)
    probe = Probe(pair, sc.slot)
    stop_ev, written = threading.Event(), {"rows": 0}
    wt = threading.Thread(target=continuous_writer, args=(pair, sc.schema, stop_ev, written), daemon=True)
    probe.start()
    wt.start()
    try:
        for step in plan:
            pipe.life(**step)
        stop_ev.set()
        wt.join(30)
        pipe.life(done=final_done or (lambda p: sum(p.rows.values()) >= written["rows"]), timeout=300)
    finally:
        stop_ev.set()
        probe.stop()
    return pipe, probe, written["rows"]


def test_restart_after_random_kills_and_java_crash_points(pair, scenario, artifacts):
    sc = scenario("create table {s}.a (id int primary key, v text)")
    rng = random.Random(4242)
    plan = [{"kill_after": rng.uniform(1.5, 6.0)} for _ in range(6 if LONG else 3)]
    for point in ("engine.ack_marking", "engine.acked", "engine.flush_sent"):
        for seed in (1, 2, 3):
            plan.append({"crash": {"point": point, "hit": "seed:%d:%d" % (seed, 5), "action": "kill"}})
    pipe, probe, written = run_pipeline_with_writer(pair, sc, artifacts, plan)
    crashed = [x for x in pipe.lives if x["how"] and x["how"].startswith("EngineExited") and "-9" in x["how"]]
    assert len(crashed) == 9, pipe.lives              # every armed Java point was reached and killed the JVM
    bad = violations(probe.samples, pipe.history, pipe.start)
    assert bad == 0
    assert sorted(pipe.rows) == list(range(written)) and set(pipe.rows.values()) == {1}
    evidence("restarts.json", {"test": "tests/engine/test_engine_ack.py::test_restart_after_random_kills_and_java_crash_points",
                               "seed": 4242, "lag_transactions": 3, "rows": written, "violations": bad,
                               "lives": pipe.lives})


def test_negative_control_one_end_ahead_is_caught(pair, scenario, artifacts):
    """A pipeline that sends DURABLE one END ahead of what it applied must make the checks fail."""
    sc = scenario("create table {s}.a (id int primary key, v text)")
    pipe, probe, written = run_pipeline_with_writer(pair, sc, artifacts, [{"kill_after": 3.0}, {"kill_after": 3.0}],
                                                    ahead=1, final_done=lambda p: True)
    # the same check as the other tests: the slot against the positions the pipeline had applied
    bad = violations(probe.samples, pipe.applied_history, pipe.start)
    lost = set(range(written)) - set(pipe.rows)
    evidence("negative_control.json", {"test": "tests/engine/test_engine_ack.py::test_negative_control_one_end_ahead_is_caught",
                                       "rows_lost_over_the_kills": len(lost), "samples_above_applied": bad})
    assert bad > 0, "the checks did not catch a pipeline that acknowledges one END too far"


def test_held_list_keeps_following_far_behind(pair, scenario, artifacts):
    """Second review, item 7: 60,000 single-row transactions read with no DURABLE; then DURABLE for END #20,000,
    #36,000 and the last one. The slot comes within the stated bound (k − 1 END records, k ≤ 2 × lead / limit)
    within seconds and is never above."""
    n = 60_000
    sc = scenario("create table {s}.a (id int primary key, v text)")
    write_backlog(pair, sc.schema, n, every_message=0)
    acks = []
    probe = Probe(pair, sc.slot)
    with Engine(sc.settings(heartbeat_interval_ms=200), None, artifacts) as eng:
        col = Collector(eng)
        col.keep_frames = False
        while len(acks) < n:
            for f in eng.frames(Deadline.after(1)):
                col.feed(f)
                if isinstance(f, F.End):
                    acks.append(col.txns.pop().end.ack_position)
        probe.start()
        bound = max(1, 2 * n // 4096)
        results = []
        index = {a: i + 1 for i, a in enumerate(acks)}
        prev = None
        for idx in (20_000, 36_000, n):
            p = acks[idx - 1]
            t0 = time.monotonic()
            eng.durable(p)
            while True:
                for f in eng.frames(Deadline.after(0.1)):
                    pass
                cf = probe.latest()
                if cf is not None and cf != prev and cf in index:
                    break
                assert time.monotonic() - t0 < 20, "the slot did not follow DURABLE for END #%d" % idx
            time.sleep(0.5)                              # let a second flush show if one comes
            cf = probe.latest()
            prev = cf
            confirmed_idx = index[cf]
            results.append({"durable_for_end": idx, "confirmed_end": confirmed_idx, "behind": idx - confirmed_idx,
                            "seconds": round(time.monotonic() - t0, 2)})
        probe.stop()
        eng.stop(Deadline.after(20))
    assert all(v <= max(acks[r["durable_for_end"] - 1] for r in results) for _t, v in probe.samples)
    assert all(r["behind"] <= bound - 1 for r in results), results
    assert results[-1]["behind"] == 0                 # the newest END is always held
    evidence("held_list_bound.json", {"test": "tests/engine/test_engine_ack.py::test_held_list_keeps_following_far_behind",
                                      "transactions": n, "limit": 4096, "stated_bound_end_records": bound - 1,
                                      "results": results})


def test_idle_acknowledgement_bound(pair, scenario, artifacts):
    """Second review, item 6(b): on an idle source the slot reaches the last DURABLE within heartbeat.interval.ms
    plus 1 s (the stated bound: one heartbeat interval until the batch call, one poll, the flush)."""
    sc = scenario("create table {s}.a (id int primary key, v text)")
    out = {}
    for hb in (300, 500):
        with Engine(sc.settings(heartbeat_interval_ms=hb), None, artifacts) as eng:
            col = Collector(eng)
            col.keep_frames = False
            delays = []
            for i in range(5):
                pair.p("insert into %s.a values (%d, 'v')" % (sc.schema, hb * 10 + i))
                col.run(lambda c: len(c.txns) >= 1, timeout=30)
                p = col.txns.pop().end.ack_position
                time.sleep(hb / 1000.0 * 1.5)        # idle
                t0 = time.monotonic()
                eng.durable(p)
                while L.parse(pair.slot(sc.slot)[0]) < p:
                    for _f in eng.frames(Deadline.after(0.02)):
                        pass
                    assert time.monotonic() - t0 < 10
                delays.append(time.monotonic() - t0)
            eng.stop(Deadline.after(20))
        out[hb] = {"max_s": round(max(delays), 3), "all_s": [round(d, 3) for d in delays]}
        assert max(delays) <= hb / 1000.0 + 1.0
    evidence("idle_ack_bound.json", {"test": "tests/engine/test_engine_ack.py::test_idle_acknowledgement_bound",
                                     "bound": "heartbeat.interval.ms + 1 s", "measured": out})


def test_slot_never_ahead_of_durable_and_follows_it(pair, scenario, artifacts):
    sc = scenario("create table {s}.a (id int primary key, v text)", "create table {s}.unpub (id int)")
    pair.p("alter publication %s drop table %s.unpub" % (sc.pub, sc.schema))
    start_cf = L.parse(pair.slot(sc.slot)[0])
    sent = []
    probe = Probe(pair, sc.slot)
    stop_ev = threading.Event()

    def writer():
        with pair.connect(pair.primary_port) as c:
            i = 0
            while not stop_ev.is_set():
                i += 1
                c.execute("insert into %s.a values (%d, 'v')" % (sc.schema, i))
                c.execute("insert into %s.unpub values (%d)" % (sc.schema, i))
                time.sleep(0.02)

    wt = threading.Thread(target=writer, daemon=True)
    with Engine(sc.settings(heartbeat_interval_ms=500), None, artifacts) as eng:
        col = Collector(eng)
        col.keep_frames = False
        col.run(lambda c: eng.hello is not None, timeout=60)
        probe.start()
        wt.start()
        t_end = time.monotonic() + 6                     # phase 1: no DURABLE: the slot must not move
        while time.monotonic() < t_end:
            for f in eng.frames(Deadline.after(0.5)):
                col.feed(f)
        assert {v for _t, v in probe.samples} == {start_cf}, "the slot moved without any DURABLE"
        t_end = time.monotonic() + (60 if LONG else 15)  # phase 2: DURABLE lagging three transactions
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
        stop_ev.set()
        wt.join(10)
        for f in eng.frames(Deadline.after(2)):
            col.feed(f)
        last = col.txns[-1].end.ack_position
        sent.append((time.monotonic(), last))
        eng.durable(last)
        t0 = time.monotonic()
        while probe.latest() is None or probe.latest() < last:
            for f in eng.frames(Deadline.after(0.1)):
                col.feed(f)
            assert time.monotonic() - t0 < 1.5, "idle: the slot did not follow within heartbeat interval + 1 s"
        # item 6(c): WAL for unpublished tables only: neither the slot nor the frame positions move
        cf_before = probe.latest()
        hb_mark = len(col.heartbeats)
        with pair.connect(pair.primary_port) as c:
            for i in range(30):
                c.execute("insert into %s.unpub values (%d)" % (sc.schema, 100000 + i))
                time.sleep(0.2)
        for f in eng.frames(Deadline.after(3)):
            col.feed(f)
        hb_positions = {tuple(sorted(h.positions.items())) for h in col.heartbeats[hb_mark:]}
        cf_after = probe.latest()
        txns_during = len(col.txns)
        probe.stop()
        eng.stop(Deadline.after(20))
    bad = violations(probe.samples, sent, start_cf)
    assert bad == 0
    assert cf_after == cf_before == last
    assert len(hb_positions) == 1 and dict(next(iter(hb_positions))).get("lsn_commit") == last
    evidence("ack_invariant.json", {
        "test": "tests/engine/test_engine_ack.py::test_slot_never_ahead_of_durable_and_follows_it",
        "samples": len(probe.samples), "durables_sent": len(sent), "violations": bad,
        "transactions": txns_during, "phase1_slot_moved": False,
        "unpublished_wal_only": {"slot_moved": cf_after != cf_before, "heartbeat_positions_seen": len(hb_positions)}})


def test_durable_edge_cases(pair, scenario, artifacts):
    """Malformed, backward, repeated, between two held records, after STOP: the slot as specified, the engine alive."""
    sc = scenario("create table {s}.a (id int primary key, v text)")
    with Engine(sc.settings(heartbeat_interval_ms=200), None, artifacts) as eng:
        col = Collector(eng)
        col.keep_frames = False
        for i in range(4):
            pair.p("insert into %s.a values (%d, 'v')" % (sc.schema, i))
        col.run(lambda c: len(c.txns) >= 4, timeout=60)
        acks = [t.end.ack_position for t in col.txns]
        start = L.parse(pair.slot(sc.slot)[0])

        def settle(want, secs=1.5):
            t0 = time.monotonic()
            while time.monotonic() - t0 < secs:
                for f in eng.frames(Deadline.after(0.1)):
                    col.feed(f)
                if want is not None and L.parse(pair.slot(sc.slot)[0]) == want:
                    return
            if want is not None:
                assert L.parse(pair.slot(sc.slot)[0]) == want

        for bad in ("DURABLE", "DURABLE x", "DURABLE -1", "DURABLE 1.5", "DURABLE 18446744073709551616",
                    "DURABLE 0x10", "durable 5", "", "DURABLE  "):
            eng.proc.stdin.write((bad + "\n").encode())
            eng.proc.stdin.flush()
        settle(None)
        assert eng.alive and L.parse(pair.slot(sc.slot)[0]) == start
        assert sum("BAD_COMMAND" in l for l in eng.log_lines) >= 1
        eng.durable(acks[1] + 1)                # between the 2nd and the 3rd END: the 2nd is confirmed
        settle(acks[1])
        eng.durable(acks[0])                    # backward: nothing changes (Engine.durable sends only increases;
        eng.proc.stdin.write(("DURABLE %d\n" % acks[0]).encode())   # the raw line too)
        eng.proc.stdin.flush()
        settle(None)
        assert L.parse(pair.slot(sc.slot)[0]) == acks[1]
        for _ in range(3):                      # repeated
            eng.proc.stdin.write(("DURABLE %d\n" % acks[1]).encode())
        eng.proc.stdin.flush()
        settle(None)
        assert eng.alive and L.parse(pair.slot(sc.slot)[0]) == acks[1]
        assert eng.stop(Deadline.after(20)) == 0
    # after STOP nothing is applied: a DURABLE written after STOP finds the JVM gone (documented)
    assert L.parse(pair.slot(sc.slot)[0]) == acks[1]


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
    assert max(rss) - rss[0] < 64, samples             # no growth while frozen (the GC may give memory back)
    evidence("frozen_reader.json", {"test": "tests/engine/test_engine_ack.py::test_frozen_reader",
                                    "freeze_s": freeze_s, "rows": n, "wal_sender_timeout": "60s (default)",
                                    "samples": samples, "all_rows_arrived": True})


def test_durable_sent_before_the_redelivery_still_acknowledges(pair, scenario, artifacts):
    """After a restart the pipeline may send DURABLE p before the redelivered transactions with ack <= p arrive;
    the engine keeps p and acknowledges them when they come (CdcEngine.Consumer.lastDurable)."""
    sc = scenario("create table {s}.a (id int primary key, v text)")
    pair.p("insert into %s.a values (1, 'x')" % sc.schema)
    with Engine(sc.settings(), None, artifacts) as eng:
        col = Collector(eng).run(lambda c: len(c.txns) >= 1, timeout=60)
        p = col.txns[0].end.ack_position
        eng.stop(Deadline.after(20))
    before = L.parse(pair.slot(sc.slot)[0])
    assert before < p                                   # nothing acknowledged yet
    with Engine(sc.settings(heartbeat_interval_ms=500), None, artifacts) as eng:
        eng.durable(p)                                  # before anything arrives
        col = Collector(eng)
        t0 = time.monotonic()
        while L.parse(pair.slot(sc.slot)[0]) < p:
            for f in eng.frames(Deadline.after(0.5)):
                col.feed(f)
            assert time.monotonic() - t0 < 30, "the early DURABLE was lost"
        assert col.txns and col.txns[0].end.ack_position == p
        eng.stop(Deadline.after(20))
