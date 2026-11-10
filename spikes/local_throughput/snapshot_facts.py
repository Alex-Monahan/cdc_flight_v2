"""Snapshot facts on the hot standby (PLAN §2 A 5 part 1, item 2). Writes evidence/p2/local_throughput/snapshot_facts.json.

    tools/with-env uv run --group spike python spikes/local_throughput/snapshot_facts.py [--rounds 8]

1. pg_export_snapshot() on the standby; N=4 importers with SET TRANSACTION SNAPSHOT; a row committed on the primary
   after the export (and replayed) is invisible to importers and visible to a fresh transaction; SERIALIZABLE on the
   standby; importing after the exporter ended.
2. Mixed-state test. Table snap_t (3M rows: id, bal, pad) on the primary; sum(bal) = 0 always. Writer threads on the
   primary move one unit between two random rows per transaction and periodically rewrite 1 % of the rows (new tuple
   versions on other pages). Parallel readers on the replica check count(*) = N, count(DISTINCT id) = N, sum(bal) = 0:
     a. DuckDB's Postgres reader, threads 8, pg_pages_per_task 200;
     b. our parallel COPY with a shared exported snapshot (control: must be consistent);
     c. our parallel COPY, every worker its own snapshot (negative control: shows the test can see a mixed state).
3. Which statements DuckDB sends (pg_debug_show_queries, captured from a child process).
"""
from __future__ import annotations

import argparse
import io
import random
import subprocess
import sys
import threading
import time

import duckdb

import common as C

N_ROWS = 3_000_000


def facts_export() -> dict:
    f: dict = {}
    with C.pg_connect("replica", "runtime") as r:
        f["replica_in_recovery"] = r.execute("select pg_is_in_recovery()").fetchone()[0]
        f["server_version"] = r.execute("select current_setting('server_version')").fetchone()[0]
        f["settings"] = dict(r.execute(
            "select name, setting from pg_settings where name in ('hot_standby_feedback','max_standby_streaming_delay',"
            "'max_standby_archive_delay','default_transaction_isolation')").fetchall())
    exp = C.pg_connect("replica", "runtime")
    exp.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    try:
        snap = exp.execute("select pg_export_snapshot()").fetchone()[0]
        f["export_on_standby"] = {"ok": True, "snapshot_id_shape": snap.split("-")[0] + "-<n>-<n>" if snap else None}
    except Exception as e:
        f["export_on_standby"] = {"ok": False, "error": C.redact(e)}
        return f
    with C.pg_connect("primary", "admin") as p:
        p.execute("CREATE TABLE IF NOT EXISTS public.snap_marker (k text primary key, at timestamptz default now())")
        p.execute("GRANT SELECT ON public.snap_marker TO cdc_v2_runtime")
        p.execute("DELETE FROM public.snap_marker")
    C.wait_replica_caught_up()
    # marker written after the export
    with C.pg_connect("primary", "admin") as p:
        p.execute("INSERT INTO public.snap_marker (k) VALUES ('after_export')")
    C.wait_replica_caught_up()
    imps = []
    seen = []
    for _ in range(4):
        c = C.pg_connect("replica", "runtime")
        c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        c.execute(f"SET TRANSACTION SNAPSHOT '{snap}'")
        seen.append(c.execute("select count(*) from public.snap_marker").fetchone()[0])
        imps.append(c)
    with C.pg_connect("replica", "runtime") as fresh:
        fresh_seen = fresh.execute("select count(*) from public.snap_marker").fetchone()[0]
    f["import_4_readers"] = {"ok": True, "marker_rows_seen_by_importers": seen, "marker_rows_seen_by_fresh": fresh_seen,
                             "consistent": all(s == 0 for s in seen) and fresh_seen == 1}
    for c in imps:
        c.execute("COMMIT")
        c.close()
    exp.execute("COMMIT")
    exp.close()
    # import after the exporter has ended
    with C.pg_connect("replica", "runtime") as c:
        try:
            c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            c.execute(f"SET TRANSACTION SNAPSHOT '{snap}'")
            f["import_after_exporter_ended"] = {"ok": True}
        except Exception as e:
            f["import_after_exporter_ended"] = {"ok": False, "error": C.redact(e).replace(snap, "<snapshot>")}
    with C.pg_connect("replica", "runtime") as c:
        try:
            c.execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY")
            c.execute("select 1")
            f["serializable_on_standby"] = {"ok": True}
        except Exception as e:
            f["serializable_on_standby"] = {"ok": False, "error": C.redact(e)}
    with C.pg_connect("replica", "runtime") as c:
        try:
            c.execute("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY")
            c.execute("select pg_export_snapshot()")
            f["export_in_read_committed"] = {"ok": True}
        except Exception as e:
            f["export_in_read_committed"] = {"ok": False, "error": C.redact(e)}
    return f


def setup_table() -> None:
    with C.pg_connect("primary", "admin") as p:
        have = p.execute("select to_regclass('public.snap_t') is not null").fetchone()[0]
        if have and p.execute("select count(*) from public.snap_t").fetchone()[0] == N_ROWS:
            p.execute("UPDATE public.snap_t SET bal = 0 WHERE bal <> 0")
        else:
            p.execute("DROP TABLE IF EXISTS public.snap_t")
            p.execute("CREATE TABLE public.snap_t (id int primary key, bal bigint not null, gen int not null, "
                      "pad text not null)")
            p.execute(f"INSERT INTO public.snap_t SELECT g, 0, 0, repeat(md5(g::text), 3) "
                      f"FROM generate_series(1, {N_ROWS}) g")
            p.execute("GRANT SELECT ON public.snap_t TO cdc_v2_runtime")
        p.execute("VACUUM ANALYZE public.snap_t")
    C.wait_replica_caught_up()


class Writers:
    def __init__(self, n: int = 4):
        self.stop = threading.Event()
        self.txns = 0
        self.rewrites = 0
        self.errors: list[str] = []
        self.ts = [threading.Thread(target=self.transfer, daemon=True) for _ in range(n)]
        self.ts.append(threading.Thread(target=self.rewrite, daemon=True))

    def transfer(self):
        rnd = random.Random()
        with C.pg_connect("primary", "admin", autocommit=False) as c:
            while not self.stop.is_set():
                a, b = rnd.randint(1, N_ROWS), rnd.randint(1, N_ROWS)
                try:
                    c.execute("UPDATE public.snap_t SET bal = bal - 1 WHERE id = %s", (a,))
                    c.execute("UPDATE public.snap_t SET bal = bal + 1 WHERE id = %s", (b,))
                    c.commit()
                    self.txns += 1
                except Exception as e:
                    c.rollback()
                    self.errors.append(C.redact(e)[:200])

    def rewrite(self):
        rnd = random.Random()
        with C.pg_connect("primary", "admin", autocommit=False) as c:
            while not self.stop.is_set():
                m = rnd.randint(0, 99)
                # 1 % of the rows get new versions; the unit moves from the lowest id of the slice to the highest
                c.execute("UPDATE public.snap_t SET gen = gen + 1 WHERE id %% 100 = %s", (m,))
                c.execute("UPDATE public.snap_t SET bal = bal - 1 WHERE id = (SELECT min(id) FROM public.snap_t "
                          "WHERE id %% 100 = %s)", (m,))
                c.execute("UPDATE public.snap_t SET bal = bal + 1 WHERE id = (SELECT max(id) FROM public.snap_t "
                          "WHERE id %% 100 = %s)", (m,))
                c.commit()
                self.rewrites += 1
                time.sleep(0.2)

    def start(self):
        for t in self.ts:
            t.start()
        return self

    def finish(self):
        self.stop.set()
        for t in self.ts:
            t.join()


def check_duckdb(con) -> dict:
    t0 = time.time()
    n, nd, s = con.execute("SELECT count(*), count(DISTINCT id), sum(bal) FROM pg.public.snap_t").fetchone()
    return {"count": n, "distinct": nd, "sum": int(s), "wall_s": round(time.time() - t0, 2),
            "mixed": not (n == N_ROWS and nd == N_ROWS and s == 0)}


def copy_worker_read(dsn, snap, preds, out, idx):
    import psycopg
    import pyarrow as pa
    import pyarrow.csv as pc
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        if snap:
            c.execute(f"SET TRANSACTION SNAPSHOT '{snap}'")
        tables = []
        for i in idx:
            buf = bytearray()
            with c.cursor().copy(f"COPY (SELECT id, bal FROM public.snap_t WHERE {preds[i]}) TO STDOUT (FORMAT csv)") as cp:
                for d in cp:
                    buf += d
            tables.append(pc.read_csv(io.BytesIO(buf), read_options=pc.ReadOptions(column_names=["id", "bal"])))
            time.sleep(0.05)  # stretch the read so that commits on the primary land in between
        c.execute("COMMIT")
    t = pa.concat_tables(tables)
    out.extend([t])


def check_copy(shared: bool, workers: int = 8, n_chunks: int = 64) -> dict:
    with C.pg_connect("replica", "runtime") as c:
        pages = c.execute("select pg_relation_size('public.snap_t') / 8192").fetchone()[0]
    step = -(-pages // n_chunks)
    preds = [f"ctid >= '({i * step},0)'::tid AND ctid < '({(i + 1) * step},0)'::tid" for i in range(n_chunks - 1)]
    preds.append(f"ctid >= '({(n_chunks - 1) * step},0)'::tid")
    snap_conn, snap = None, None
    if shared:
        snap_conn = C.pg_connect("replica", "runtime")
        snap_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        snap = snap_conn.execute("select pg_export_snapshot()").fetchone()[0]
    out: list = []
    t0 = time.time()
    ths = [threading.Thread(target=copy_worker_read,
                            args=(C.dsn("replica", "runtime"), snap, preds, out, list(range(w, n_chunks, workers))))
           for w in range(workers)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    if snap_conn:
        snap_conn.execute("COMMIT")
        snap_conn.close()
    import pyarrow as pa
    import pyarrow.compute as pc
    tbl = pa.concat_tables(out)
    n = tbl.num_rows
    nd = len(pc.unique(tbl["id"]))
    s = pc.sum(tbl["bal"]).as_py()
    return {"count": n, "distinct": nd, "sum": s, "wall_s": round(time.time() - t0, 2),
            "mixed": not (n == N_ROWS and nd == N_ROWS and s == 0)}


def duckdb_queries() -> list[str]:
    code = f"""
import duckdb, sys
sys.path.insert(0, {str(C.HERE)!r})
import common as C
con = duckdb.connect(config={{"threads": 4}})
con.execute("LOAD postgres")
con.execute("SET pg_pages_per_task = 20000")
con.execute(f"ATTACH '{{C.dsn('replica','runtime')}}' AS pg (TYPE postgres, READ_ONLY)")
con.execute("SET pg_debug_show_queries = true")
print(con.execute("SELECT count(*), sum(bal) FROM pg.public.snap_t").fetchall())
"""
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300, cwd=C.HERE)
    lines = [C.redact(x) for x in p.stdout.splitlines() if x.strip()]
    return [x for x in lines if not x.startswith("SELECT") or "snapshot" in x.lower() or "recovery" in x.lower()
            ][:40] + [f"... {len(lines)} lines in total"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=8)
    a = ap.parse_args()
    ev = {"started_utc": C.utc(), "load_avg": C.load_avg()}
    ev["export_import"] = facts_export()
    print(ev["export_import"], flush=True)
    setup_table()
    ev["duckdb_statements"] = duckdb_queries()
    con = duckdb.connect(config={**C.md_config("2GB"), "threads": 8})
    con.execute("LOAD postgres")
    con.execute("SET pg_pages_per_task = 200")
    con.execute(f"ATTACH '{C.dsn('replica', 'runtime')}' AS pg (TYPE postgres, READ_ONLY)")
    res = {"duckdb_threads8": [], "copy_shared_snapshot": [], "copy_own_snapshots": []}
    w = Writers().start()
    time.sleep(2)
    for i in range(a.rounds):
        for name, fn in (("duckdb_threads8", lambda: check_duckdb(con)),
                         ("copy_shared_snapshot", lambda: check_copy(True)),
                         ("copy_own_snapshots", lambda: check_copy(False))):
            r = fn()
            res[name].append(r)
            print(i, name, r, flush=True)
    w.finish()
    ev["writer"] = {"transfer_txns": w.txns, "rewrite_txns": w.rewrites, "errors": w.errors[:5]}
    ev["mixed_state"] = {k: {"rounds": len(v), "mixed_rounds": sum(r["mixed"] for r in v), "results": v}
                         for k, v in res.items()}
    ev["finished_utc"] = C.utc()
    C.write_json("snapshot_facts.json", ev)
    print({k: (v["rounds"], v["mixed_rounds"]) for k, v in ev["mixed_state"].items()})


if __name__ == "__main__":
    main()
