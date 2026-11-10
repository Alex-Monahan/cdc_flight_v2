"""Effect on and of the replica during a long backfill read (item 5). Writes evidence/p2/local_throughput/hazards.json.

    tools/with-env uv run --group spike python spikes/local_throughput/hazards.py [--scenarios baseline,vacuum,lock,add_column,vacuum_truncate] [--duration 75]

Each scenario recreates public.hz_li (a ~2M-row slice of lineitem) on the primary, waits for replay, then runs:
  - an active reader on the replica: one REPEATABLE READ READ ONLY transaction that COPYs hz_li (binary) again and
    again for --duration seconds (a backfill worker holding its snapshot);
  - an idle reader: same transaction kind, reads hz_li once, then stays idle in transaction (a worker between chunks);
  - a modest write load on the primary: 5 transactions/s of 200 inserts into hz_load and 100 updates of hz_li;
  - a lag sampler (every 0.5 s): replay lag in bytes and seconds;
  - the scenario's action on the primary at t = 5 s:
      baseline         nothing
      vacuum           VACUUM hz_li at 5 s and again at 30 s (dead tuples from the update load)
      lock             BEGIN; LOCK TABLE hz_li IN ACCESS EXCLUSIVE MODE; COMMIT
      add_column       ALTER TABLE hz_li ADD COLUMN hz_extra int
      vacuum_truncate  rows on the last 10 % of pages deleted (and replayed) before the readers start; VACUUM at 5 s
                       can then truncate the tail (needs an ACCESS EXCLUSIVE lock)
Every error is recorded with SQLSTATE, message, detail and the time since the action.
"""
from __future__ import annotations

import argparse
import threading
import time

import common as C


def recreate():
    with C.pg_connect("primary", "admin") as p:
        p.execute("DROP TABLE IF EXISTS public.hz_li")
        p.execute("CREATE TABLE public.hz_li AS SELECT * FROM public.lineitem WHERE l_orderkey <= 4000000")
        p.execute("ALTER TABLE public.hz_li ADD PRIMARY KEY (l_orderkey, l_linenumber)")
        p.execute("GRANT SELECT ON public.hz_li TO cdc_v2_runtime")
        p.execute("CREATE TABLE IF NOT EXISTS public.hz_load (id bigserial primary key, at timestamptz default now(), "
                  "pad text)")
        p.execute("VACUUM ANALYZE public.hz_li")
        n = p.execute("select count(*) from public.hz_li").fetchone()[0]
    C.wait_replica_caught_up()
    return n


def err(e: Exception, t_action: float | None) -> dict:
    d = getattr(e, "diag", None)
    return {"type": type(e).__name__, "sqlstate": getattr(e, "sqlstate", None),
            "message": C.redact(getattr(d, "message_primary", None) or str(e)).strip(),
            "detail": C.redact(getattr(d, "message_detail", None) or ""),
            "seconds_after_action": round(time.time() - t_action, 1) if t_action else None}


class Run:
    def __init__(self, duration: float):
        self.duration = duration
        self.stop = threading.Event()
        self.t0 = time.time()
        self.t_action: float | None = None
        self.lag: list[dict] = []
        self.active = {"passes": 0, "bytes": 0, "error": None, "ended_s": None}
        self.idle = {"error": None, "ended_s": None}
        self.writes = {"txns": 0, "errors": []}
        self.notices: list[str] = []

    def rel(self):
        return round(time.time() - self.t0, 1)

    def active_reader(self):
        try:
            with C.pg_connect("replica", "runtime") as c:
                c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                with c.cursor() as cur:
                    while time.time() - self.t0 < self.duration:
                        with cur.copy("COPY (SELECT * FROM public.hz_li) TO STDOUT (FORMAT binary)") as cp:
                            for d in cp:
                                self.active["bytes"] += len(d)
                        self.active["passes"] += 1
                c.execute("COMMIT")
        except Exception as e:
            self.active["error"] = err(e, self.t_action)
        self.active["ended_s"] = self.rel()

    def idle_reader(self):
        try:
            with C.pg_connect("replica", "runtime") as c:
                c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                c.execute("SELECT count(*) FROM public.hz_li WHERE l_orderkey < 1000").fetchone()
                while time.time() - self.t0 < self.duration:
                    time.sleep(0.5)
                c.execute("SELECT 1").fetchone()  # the first statement after the idle period
                c.execute("COMMIT")
        except Exception as e:
            self.idle["error"] = err(e, self.t_action)
        self.idle["ended_s"] = self.rel()

    def writer(self):
        with C.pg_connect("primary", "admin", autocommit=False) as c:
            k = 0
            while not self.stop.is_set():
                try:
                    c.execute("INSERT INTO public.hz_load (pad) SELECT md5(g::text) FROM generate_series(1, 200) g")
                    c.execute("UPDATE public.hz_li SET l_comment = left(md5(random()::text), 40) "
                              "WHERE l_orderkey >= %s AND l_orderkey < %s", (k * 40 + 1, k * 40 + 41))
                    c.commit()
                    self.writes["txns"] += 1
                except Exception as e:
                    c.rollback()
                    self.writes["errors"].append(C.redact(e)[:200])
                k = (k + 1) % 90000
                time.sleep(0.2)

    def sampler(self):
        with C.pg_connect("primary", "admin") as p, C.pg_connect("replica", "admin") as r:
            while not self.stop.is_set():
                lsn = p.execute("select pg_current_wal_lsn()").fetchone()[0]
                b, s, paused = r.execute(
                    "select pg_wal_lsn_diff(%s::pg_lsn, pg_last_wal_replay_lsn()), "
                    "extract(epoch from now() - pg_last_xact_replay_timestamp()), "
                    "pg_get_wal_replay_pause_state()", (lsn,)).fetchone()
                self.lag.append({"t": self.rel(), "bytes": int(b), "seconds": round(float(s or 0), 2)})
                time.sleep(0.5)


def conflicts() -> dict:
    with C.pg_connect("replica", "admin") as r:
        row = r.execute("select confl_tablespace, confl_lock, confl_snapshot, confl_bufferpin, confl_deadlock, "
                        "confl_active_logicalslot from pg_stat_database_conflicts where datname = current_database()"
                        ).fetchone()
    return dict(zip(["tablespace", "lock", "snapshot", "bufferpin", "deadlock", "active_logicalslot"], row))


def scenario(name: str, duration: float) -> dict:
    rows = recreate()
    pre = {}
    if name == "vacuum_truncate":
        with C.pg_connect("primary", "admin") as p:
            pages = p.execute("select pg_relation_size('public.hz_li') / 8192").fetchone()[0]
            cut = int(pages * 0.9)
            pre["deleted_rows"] = p.execute(f"DELETE FROM public.hz_li WHERE ctid >= '({cut},0)'::tid").rowcount
            pre["pages_before"] = pages
        C.wait_replica_caught_up()
    c0 = conflicts()
    run = Run(duration)
    ths = [threading.Thread(target=f, daemon=True) for f in (run.active_reader, run.idle_reader, run.writer,
                                                              run.sampler)]
    for t in ths:
        t.start()
    time.sleep(5)
    run.t_action = time.time()
    action = {"at_s": run.rel()}
    with C.pg_connect("primary", "admin") as p:
        p.add_notice_handler(lambda d: run.notices.append(C.redact(d.message_primary)))
        t = time.time()
        if name == "vacuum":
            p.execute("VACUUM (VERBOSE) public.hz_li")
            action["vacuum1_s"] = round(time.time() - t, 2)
        elif name == "lock":
            p.execute("BEGIN")
            p.execute("LOCK TABLE public.hz_li IN ACCESS EXCLUSIVE MODE")
            p.execute("COMMIT")
        elif name == "add_column":
            p.execute("ALTER TABLE public.hz_li ADD COLUMN hz_extra int")
        elif name == "vacuum_truncate":
            p.execute("VACUUM (VERBOSE) public.hz_li")
            action["pages_after"] = p.execute("select pg_relation_size('public.hz_li') / 8192").fetchone()[0]
        action["primary_statement_s"] = round(time.time() - t, 2)
        if name == "vacuum":
            time.sleep(max(0.0, 30 - (time.time() - run.t0)))
            t = time.time()
            p.execute("VACUUM (VERBOSE) public.hz_li")
            action["vacuum2_s"] = round(time.time() - t, 2)
    ths[0].join()
    ths[1].join()
    time.sleep(3)
    run.stop.set()
    ths[2].join()
    ths[3].join()
    c1 = conflicts()
    lag_max_b = max((x["bytes"] for x in run.lag), default=0)
    lag_max_s = max((x["seconds"] for x in run.lag), default=0)
    return {"scenario": name, "rows": rows, "pre": pre, "action": action, "duration_s": duration,
            "active_reader": run.active, "idle_reader": run.idle, "writes": run.writes,
            "replay_lag_max_bytes": lag_max_b, "replay_lag_max_s": lag_max_s,
            "replay_lag_samples": run.lag[::2], "conflicts_delta": {k: c1[k] - c0[k] for k in c1},
            "vacuum_notices": [n for n in run.notices if "truncat" in n.lower() or "pages" in n.lower()][:10]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", default="baseline,vacuum,lock,add_column,vacuum_truncate")
    ap.add_argument("--duration", type=float, default=75)
    a = ap.parse_args()
    ev = {"started_utc": C.utc(), "load_avg": C.load_avg()}
    with C.pg_connect("replica", "admin") as r:
        ev["replica_settings"] = dict(r.execute(
            "select name, setting || coalesce(unit, '') from pg_settings where name in ('hot_standby_feedback',"
            "'max_standby_streaming_delay','max_standby_archive_delay','wal_receiver_status_interval',"
            "'hot_standby')").fetchall())
    with C.pg_connect("primary", "admin") as p:
        ev["primary_settings"] = dict(p.execute(
            "select name, setting from pg_settings where name in ('vacuum_truncate','autovacuum')").fetchall())
    ev["scenarios"] = []
    for s in a.scenarios.split(","):
        r = scenario(s, a.duration)
        ev["scenarios"].append(r)
        print(s, {k: r[k] for k in ("action", "active_reader", "idle_reader", "replay_lag_max_s",
                                     "replay_lag_max_bytes", "conflicts_delta", "vacuum_notices")}, flush=True)
    with C.pg_connect("primary", "admin") as p:
        p.execute("DROP TABLE IF EXISTS public.hz_li")
        p.execute("DROP TABLE IF EXISTS public.hz_load")
    ev["finished_utc"] = C.utc()
    C.write_json("hazards.json", ev)


if __name__ == "__main__":
    main()
