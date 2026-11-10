"""Streaming rate of stock Debezium (EngineMain, unchanged) on this machine (items 2, 3, 5). Results are appended to
evidence/p2/local_streaming/stream.jsonl, one JSON object per measurement.

    tools/with-env uv run --group spike python spikes/local_throughput/stream_bench.py --plan main      # item 2
    tools/with-env uv run --group spike python spikes/local_throughput/stream_bench.py --plan primary   # item 3
    tools/with-env uv run --group spike python spikes/local_throughput/stream_bench.py --plan restart   # item 5
    options: --reps 3 --tests big1m,big10m,backlog,live,mixed --configs best,default --default-reps 1

Tests (slot on --node, default replica; consumer = stream_consumer.py: frames counted and acknowledged):
  big1m / big10m  INSERT INTO stream_li SELECT * FROM lineitem LIMIT n, one transaction
  backlog         1M one-row transactions by pgbench (16 clients) while no engine runs, then drained
  live            pgbench -R 500 / 2000 / 4000 / 8000 one-row transactions per second for 60 s each, engine running,
                  per-event delay = arrival in the consumer - source.ts_ms (commit time on the primary)
  mixed           upd_keyed (default replica identity) and upd_full (REPLICA IDENTITY FULL): 1M-row UPDATE and
                  1M-row DELETE, one transaction each (after a 1M-row load that is measured too)
"""
from __future__ import annotations

import argparse
import os
import re
import signal
import struct
import subprocess
import tempfile
import threading
import time

import common as C
import stream_common as S

PGBENCH = "/opt/homebrew/opt/postgresql@18/bin/pgbench"
SMALL_SQL = ("INSERT INTO public.stream_small VALUES (nextval('public.stream_small_seq'), :k, 4730, 2, 17.00, "
             "25000.50, 0.04, 0.02, 'N', 'O', '1996-03-13', '1996-02-12', '1996-03-22', 'DELIVER IN PERSON', "
             "'TRUCK', 'egular courts above the');\n")


def pg_env() -> dict:
    e = dict(os.environ)
    e.update(PGHOST="localhost", PGPORT=str(C.PRIMARY_PORT), PGUSER="cdc_v2_admin", PGDATABASE="cdc_v2_test",
             PGSSLMODE="verify-full", PGSSLROOTCERT=str(S.PG_HOME / "tls" / "ca.crt"),
             PGPASSFILE=str(S.PG_HOME / "secrets" / "pgpass"))
    return e


def pgbench(extra: list[str], timeout=1200) -> dict:
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, dir=S.WORK) as f:
        f.write("\\set k random(1, 2000000000)\n" + SMALL_SQL)
        script = f.name
    t = time.time()
    p = subprocess.run([PGBENCH, "-n", "-f", script, *extra], env=pg_env(), capture_output=True, text=True,
                       timeout=timeout)
    os.unlink(script)
    out = p.stdout + p.stderr
    tps = re.search(r"tps = ([\d.]+)", out)
    n = re.search(r"number of transactions actually processed: (\d+)", out)
    lat = re.search(r"latency average = ([\d.]+) ms", out)
    return {"rc": p.returncode, "tps": float(tps.group(1)) if tps else None, "tx": int(n.group(1)) if n else None,
            "latency_avg_ms": float(lat.group(1)) if lat else None, "wall_s": round(time.time() - t, 2),
            "error": C.redact(out[-300:]) if p.returncode else None}


def primary_lsn() -> str:
    with C.pg_connect("primary", "admin") as p:
        return p.execute("select pg_current_wal_lsn()").fetchone()[0]


def wait_replayed(lsn, t_from, timeout=900) -> float | None:
    with C.pg_connect("replica", "admin") as r:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if r.execute("select pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,)).fetchone()[0]:
                return round(time.time() - t_from, 3)
            time.sleep(0.02)
    return None


class FirstEventWatch:
    """Polls the consumer's stats file and records when the event counter first passes `base`."""

    def __init__(self, eng, base):
        self.eng, self.base, self.at = eng, base, None
        self.stop = threading.Event()
        threading.Thread(target=self.loop, daemon=True).start()

    def loop(self):
        while not self.stop.wait(0.01):
            s = self.eng.stats()
            if s.get("events", 0) > self.base:
                self.at = s.get("last_event_at")  # arrival time of the batch that first passed base
                return


def start_engine(cfg, node, latency=False, label="engine"):
    eng = S.Engine(node, cfg, latency=latency, label=label)
    if not eng.wait_polling(120):
        raise RuntimeError("engine did not start: %s" % eng.errors())
    # canary: proves the stream is live before the measurement. Whatever the server re-sends first (transactions the
    # slot had not confirmed when the previous engine stopped) arrives before the canary; we wait until the counter
    # is stable and record how many events that was.
    with C.pg_connect("primary", "admin") as p:
        p.execute(SMALL_SQL.replace(":k", "1"))
    if not eng.wait_events(1, 60):
        raise RuntimeError("canary not received: %s" % eng.errors())
    last, since = -1, time.time()
    while time.time() - since < 3.0:
        e = eng.events()
        if e != last:
            last, since = e, time.time()
        time.sleep(0.1)
    eng.redelivered_on_start = last - 1
    return eng


def stop_engine(eng, node):
    """Give the engine time to confirm the last transaction to the server (offset flush 1 s + next poll), then stop."""
    time.sleep(3)
    eng.stop()
    S.wait_slot_idle(node)


def measure_tx(eng, node, sql, expect, label) -> dict:
    """One source transaction (sql) on the primary; measures delivery of its `expect` events."""
    base = eng.events()
    st0 = S.slot_stats(node)
    th0 = eng.thread_cpu()
    smp = S.Sampler(eng, node)
    la = C.load_avg()
    t0 = time.time()
    with C.pg_connect("primary", "admin") as p:
        n = p.execute(sql).rowcount
    tc = time.time()
    lsn = primary_lsn()
    watch = FirstEventWatch(eng, base)
    replay_s = wait_replayed(lsn, tc) if node == "replica" else 0.0
    ok = eng.wait_events(base + expect, max(600, expect / 4000))
    watch.stop.set()
    s = eng.stats()
    th1 = eng.thread_cpu()
    res = smp.finish()
    st1 = S.slot_stats(node)
    last, first = s.get("last_event_at"), watch.at
    rows = s.get("events", 0) - base
    out = {"test": label, "rows_written": n, "rows_received": rows, "ok": ok and rows == expect,
           "load_avg": la, "tx_exec_s": round(tc - t0, 2), "commit_to_replayed_s": replay_s,
           "commit_to_first_event_s": round(first - tc, 3) if first else None,
           "commit_to_last_event_s": round(last - tc, 3) if last else None,
           "first_to_last_s": round(last - first, 3) if first and last else None,
           "rows_per_s_first_to_last": round(rows / (last - first)) if first and last and last > first else None,
           "rows_per_s_commit_to_last": round(rows / (last - tc)) if last else None,
           **res, "jvm_threads_cpu_s": S.thread_groups(S.thread_delta(th0, th1)),
           "slot_stats_delta": S.diff(st0, st1)}
    w = out["window_s"]
    out["cpu_pct"] = {k: (round(100 * v / w) if v is not None else None) for k, v in out["cpu_s"].items()}
    if rows:
        out["us_per_row"] = {k: (round(1e6 * v / rows, 1) if v is not None else None)
                             for k, v in {**out["cpu_s"], "streaming_thread": out["jvm_threads_cpu_s"]["streaming"]}.items()}
    return out


def t_big(cfg, node, n, rep) -> dict:
    S.truncate("stream_li")
    eng = start_engine(cfg, node, label=f"big{n}")
    try:
        r = measure_tx(eng, node, f"INSERT INTO public.stream_li SELECT * FROM public.lineitem LIMIT {n}", n,
                       f"big{n // 1000000}m")
        r["redelivered_on_start"] = eng.redelivered_on_start
    finally:
        stop_engine(eng, node)
    S.truncate("stream_li")
    return r


def t_backlog(cfg, node, n, rep) -> dict:
    S.wait_slot_idle(node)
    clients = 16
    la = C.load_avg()
    pb = pgbench(["-c", str(clients), "-j", "4", "-t", str(n // clients)])
    lsn = primary_lsn()
    if node == "replica":
        wait_replayed(lsn, time.time())
    st0 = S.slot_stats(node)
    t_start = time.time()
    eng = S.Engine(node, cfg, label="backlog")
    try:
        eng.wait_polling(120)
        smp = S.Sampler(eng, node)
        th0 = eng.thread_cpu()
        ok = eng.wait_events(n, 1800)
        th1 = eng.thread_cpu()
        time.sleep(3)  # anything beyond n would show up here
        s = eng.stats()
        res = smp.finish()
    finally:
        stop_engine(eng, node)
    st1 = S.slot_stats(node)
    first, last = s.get("first_event_at"), s.get("last_event_at")
    ev = s.get("events", 0)
    out = {"test": "backlog", "tx_written": pb["tx"], "pgbench": pb, "events": ev, "ok": ok and ev == n,
           "extra_events": ev - n, "load_avg": la,
           "spawn_to_first_event_s": round(first - t_start, 2) if first else None,
           "first_to_last_s": round(last - first, 2) if first and last else None,
           "tx_per_s": round(ev / (last - first)) if first and last and last > first else None,
           **res, "jvm_threads_cpu_s": S.thread_groups(S.thread_delta(th0, th1)),
           "slot_stats_delta": S.diff(st0, st1)}
    if ev:
        out["us_per_tx"] = {k: (round(1e6 * v / ev, 1) if v is not None else None)
                            for k, v in {**out["cpu_s"], "streaming_thread": out["jvm_threads_cpu_s"]["streaming"]}.items()}
    with C.pg_connect("primary", "admin") as p:
        p.execute("TRUNCATE public.stream_small")
    return out


def pct(v, q):
    if not v:
        return None
    v = sorted(v)
    return v[min(len(v) - 1, int(round(q / 100 * (len(v) - 1))))]


class ReplayLag:
    def __init__(self):
        self.samples = []
        self.stop = threading.Event()
        threading.Thread(target=self.loop, daemon=True).start()

    def loop(self):
        with C.pg_connect("primary", "admin") as p:
            while not self.stop.wait(0.25):
                r = p.execute("select extract(epoch from replay_lag), pg_wal_lsn_diff(pg_current_wal_lsn(), "
                              "replay_lsn) from pg_stat_replication where application_name = 'replica'").fetchone()
                if r:
                    self.samples.append((time.time(), float(r[0] or 0), int(r[1] or 0)))


def t_live(cfg, node, rates, rep) -> list[dict]:
    eng = start_engine(cfg, node, latency=True, label="live")
    outs = []
    try:
        for rate in rates:
            clients = max(4, min(32, rate // 250))
            base = eng.events()
            th0 = eng.thread_cpu()
            smp = S.Sampler(eng, node)
            rl = ReplayLag()
            la = C.load_avg()
            t0 = time.time()
            pb = pgbench(["-c", str(clients), "-j", str(min(8, clients)), "-R", str(rate), "-T", "60"])
            t1 = time.time()
            expect = pb["tx"] or 0
            ok = eng.wait_events(base + expect, 300)
            drained = eng.stats().get("last_event_at")
            th1 = eng.thread_cpu()
            res = smp.finish()
            rl.stop.set()
            # delays of the events committed inside the phase window
            delays = []
            with open(eng.lat_path, "rb") as f:
                data = f.read()
            for off in range(0, len(data), 32):
                arr, ts, _lsn, _tx = struct.unpack_from(">qqqq", data, off)
                if t0 * 1000 <= ts <= t1 * 1000:
                    delays.append(arr - ts)
            rlag = [x[1] for x in rl.samples if t0 <= x[0] <= t1]
            out = {"test": "live", "rate_target": rate, "clients": clients, "pgbench": pb, "load_avg": la,
                   "events_in_window": len(delays), "kept_up": ok,
                   "drain_after_load_end_s": round(drained - t1, 2) if drained else None,
                   "delay_ms": {"p50": pct(delays, 50), "p99": pct(delays, 99), "max": max(delays) if delays else None},
                   "replay_lag_s": {"p50": pct(rlag, 50), "p99": pct(rlag, 99), "max": max(rlag) if rlag else None},
                   **res, "jvm_threads_cpu_s": S.thread_groups(S.thread_delta(th0, th1))}
            if expect:
                out["us_per_tx"] = {k: (round(1e6 * v / expect, 1) if v is not None else None)
                                    for k, v in {**out["cpu_s"],
                                                 "streaming_thread": out["jvm_threads_cpu_s"]["streaming"]}.items()}
            outs.append(out)
            out["redelivered_on_start"] = eng.redelivered_on_start
            print("  live", rate, out["pgbench"]["tps"], out["delay_ms"], out["replay_lag_s"], flush=True)
            time.sleep(3)
    finally:
        stop_engine(eng, node)
        try:
            os.unlink(eng.lat_path)
        except OSError:
            pass
    with C.pg_connect("primary", "admin") as p:
        p.execute("TRUNCATE public.stream_small")
    return outs


def t_mixed(cfg, node, rep) -> list[dict]:
    S.truncate("upd_keyed", "upd_full")
    eng = start_engine(cfg, node, label="mixed")
    outs = []
    try:
        for t in ("upd_keyed", "upd_full"):
            outs.append(measure_tx(eng, node, f"INSERT INTO public.{t} SELECT * FROM public.lineitem LIMIT 1000000",
                                   1000000, f"insert_{t}"))
            outs.append(measure_tx(eng, node, f"UPDATE public.{t} SET l_comment = left(l_comment, 30) || 'u'",
                                   1000000, f"update_{t}"))
            outs.append(measure_tx(eng, node, f"DELETE FROM public.{t}", 1000000, f"delete_{t}"))
        for o in outs:
            o["redelivered_on_start"] = eng.redelivered_on_start
    finally:
        stop_engine(eng, node)
    S.truncate("upd_keyed", "upd_full")
    return outs


def t_restart(cfg, node, sig, rep) -> dict:
    """Item 5: stop the JVM in the middle of a 10M-row transaction; restart; where does the server resume?"""
    n = 10_000_000
    S.truncate("stream_li")
    eng = start_engine(cfg, node, label="restart")
    with C.pg_connect(node, "admin") as c:
        cf0 = c.execute("select confirmed_flush_lsn, restart_lsn from pg_replication_slots where slot_name=%s",
                        (S.SLOT,)).fetchone()
    base = eng.events()
    t0 = time.time()
    with C.pg_connect("primary", "admin") as p:
        p.execute(f"INSERT INTO public.stream_li SELECT * FROM public.lineitem LIMIT {n}")
    tc = time.time()
    eng.wait_events(base + n // 2, 1200)
    got1 = eng.events() - base
    ts = time.time()
    os.kill(eng.jvm_pid, sig)
    eng.consumer.wait(timeout=120)
    eng.props_path.unlink(missing_ok=True)  # it holds the local runtime password
    t_dead = time.time()
    S.wait_slot_idle(node)
    with C.pg_connect(node, "admin") as c:
        cf1 = c.execute("select confirmed_flush_lsn, restart_lsn from pg_replication_slots where slot_name=%s",
                        (S.SLOT,)).fetchone()
    t_re = time.time()
    eng2 = S.Engine(node, cfg, label="restart2")
    eng2.wait_polling(120)
    t_poll = time.time()
    ok = eng2.wait_events(n, 1800)
    s2 = eng2.stats()
    total2 = s2.get("events", 0)
    extra = 0
    time.sleep(3)
    total2b = eng2.events()
    stop_engine(eng2, node)
    S.truncate("stream_li")
    return {"test": "restart", "signal": signal.Signals(sig).name, "insert_s": round(tc - t0, 1),
            "events_before_stop": got1, "stop_after_commit_s": round(ts - tc, 1),
            "jvm_gone_after_signal_s": round(t_dead - ts, 2),
            "slot_before_tx": [str(x) for x in cf0], "slot_after_stop": [str(x) for x in cf1],
            "confirmed_flush_moved": str(cf0[0]) != str(cf1[0]),
            "restart_spawn_to_polling_s": round(t_poll - t_re, 2),
            "restart_spawn_to_first_event_s": round(s2["first_event_at"] - t_re, 2) if s2.get("first_event_at") else None,
            "events_after_restart": total2b, "whole_transaction_resent": total2b >= n, "ok": ok,
            "restart_first_to_last_s": round(s2["last_event_at"] - s2["first_event_at"], 1)
            if s2.get("first_event_at") else None, "extra_events_beyond_tx": total2b - n, "_": extra}


def t_confirm(cfg, node, extra, label) -> dict:
    """Does the slot's confirmed_flush_lsn cover the last transaction before an idle period, and after a clean stop?"""
    S.fresh_slot(node)
    eng = S.Engine(node, cfg, extra=extra, label="confirm")
    eng.wait_polling(120)
    with C.pg_connect("primary", "admin") as p:
        p.execute("INSERT INTO public.stream_li SELECT * FROM public.lineitem LIMIT 1000")
        after = p.execute("select pg_current_wal_lsn()").fetchone()[0]
    eng.wait_events(1000, 60)

    def confirmed():
        with C.pg_connect(node, "admin") as c:
            return c.execute("select confirmed_flush_lsn >= %s::pg_lsn, confirmed_flush_lsn::text from "
                             "pg_replication_slots where slot_name = %s", (after, S.SLOT)).fetchone()

    out = {"test": "confirm", "variant": label, "extra": extra, "lsn_after_commit": str(after)}
    for wait in (2, 10):
        time.sleep(wait if wait == 2 else 8)
        c = confirmed()
        out[f"covered_after_{wait}s_idle"] = c[0]
    eng.stop()
    S.wait_slot_idle(node)
    out["covered_after_clean_stop"] = confirmed()[0]
    eng2 = S.Engine(node, cfg, extra=extra, label="confirm2")
    eng2.wait_polling(120)
    time.sleep(5)
    out["redelivered_events_on_restart"] = eng2.events()
    eng2.stop()
    S.wait_slot_idle(node)
    S.truncate("stream_li")
    return out


def run(plan, reps, tests, configs, default_reps, node):
    for rep in range(1, reps + 1):
        for cfg in configs:
            if cfg == "default" and rep > default_reps:
                continue
            for t in tests:
                print(f"rep {rep} cfg {cfg} node {node} test {t} load {C.load_avg()}", flush=True)
                # every test starts from its own new slot: Debezium confirms the last transaction before an idle
                # period or a stop only on a later batch (measured: test "confirm"), so a reused slot re-sends it
                S.wait_slot_idle(node)
                S.fresh_slot(node)
                try:
                    if t == "confirm":
                        rs = [t_confirm(cfg, node, {}, "flush_1s_no_heartbeat"),
                              t_confirm(cfg, node, {"heartbeat.interval.ms": "1000", "offset.flush.interval.ms": "0"},
                                        "flush_0_heartbeat_1s")]
                    elif t == "big1m":
                        rs = [t_big(cfg, node, 1_000_000, rep)]
                    elif t == "big10m":
                        rs = [t_big(cfg, node, 10_000_000, rep)]
                    elif t == "backlog":
                        rs = [t_backlog(cfg, node, 1_000_000, rep)]
                    elif t == "live":
                        rs = t_live(cfg, node, [500, 2000, 4000, 8000], rep)
                    elif t == "mixed":
                        rs = t_mixed(cfg, node, rep)
                    elif t == "restart_term":
                        rs = [t_restart(cfg, node, signal.SIGTERM, rep)]
                    elif t == "restart_kill":
                        rs = [t_restart(cfg, node, signal.SIGKILL, rep)]
                    else:
                        raise ValueError(t)
                except Exception as e:  # noqa: BLE001
                    rs = [{"test": t, "error": C.redact(e)[:500]}]
                for r in rs:
                    r.update(plan=plan, rep=rep, cfg=cfg, node=node, utc=C.utc())
                    S.write_evidence("stream.jsonl", r)
                    print("   ", {k: r.get(k) for k in ("test", "ok", "rows_per_s_first_to_last", "tx_per_s",
                                                         "commit_to_first_event_s", "cpu_pct", "us_per_row",
                                                         "us_per_tx", "jvm_peak_rss_mb", "error",
                                                         "whole_transaction_resent",
                                                         "restart_spawn_to_first_event_s")}, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", default="main", choices=["main", "primary", "restart", "custom", "confirm"])
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--tests", default=None)
    ap.add_argument("--configs", default="best,default")
    ap.add_argument("--default-reps", type=int, default=3)
    ap.add_argument("--node", default=None)
    a = ap.parse_args()
    defaults = {"main": ("big1m,big10m,backlog,live,mixed", "replica"),
                "primary": ("big1m,backlog", "primary"),
                "restart": ("restart_term,restart_kill", "replica"),
                "custom": ("big1m", "replica"), "confirm": ("confirm", "replica")}
    tests, node = defaults[a.plan]
    tests = (a.tests or tests).split(",")
    node = a.node or node
    S.truncate(*S.TABLES)
    run(a.plan, a.reps, tests, a.configs.split(","), a.default_reps, node)


if __name__ == "__main__":
    main()
