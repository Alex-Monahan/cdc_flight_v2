"""End to end, replica -> MotherDuck staging table in cdc_v2_spike_tp, as a local indication (item 3 and 4).

    tools/with-env uv run --group spike python spikes/local_throughput/e2e_md.py --routes upload,duckdb,copy_parquet,loaders [--repeats 3] [--threads 8] [--workers 4] [--k 1,2,4]
    tools/with-env uv run --group spike python spikes/local_throughput/e2e_md.py --drop      # drop cdc_v2_spike_tp

Routes (results appended to evidence/p2/local_throughput/e2e.jsonl):
  upload        plain upload bound: CREATE TABLE ... AS SELECT * FROM read_parquet(<one local file>) - no Postgres
  duckdb        (i) one DuckDB process attached to the replica and to MotherDuck:
                    CREATE TABLE stage AS SELECT * FROM pg.public.lineitem
  copy_parquet  (ii) parallel COPY (csv, ctid ranges, shared snapshot) -> Arrow -> local Parquet chunks, then one
                    CREATE TABLE stage AS SELECT * FROM read_parquet(<chunks>); both phases timed
  loaders       (iii) K loader processes, each with its own MotherDuck connection, append the Parquet chunks of (ii)
                    to one pre-created staging table, one INSERT (= one transaction) per chunk
The MotherDuck connections use single attach mode on cdc_v2_spike_tp only; only create/drop use a workspace-mode
connection, with exactly one statement.
"""
from __future__ import annotations

import argparse
import io
import json
import multiprocessing as mp
import shutil
import time

import duckdb

import common as C

E2E = C.WORK / "e2e"
CHUNKS = E2E / "chunks"
ONEFILE = E2E / "lineitem.parquet"


def md_connect(threads: int | None = None, memory_limit: str = "2GB"):
    cfg = C.md_config(memory_limit)
    if threads:
        cfg["threads"] = threads
    return duckdb.connect(f"md:{C.MD_DB}?attach_mode=single", config=cfg)


def ensure_db():
    con = duckdb.connect("md:", config=C.md_config("1GB"))
    try:
        con.execute(f"CREATE DATABASE IF NOT EXISTS {C.MD_DB}")
    finally:
        con.close()


def drop_db():
    con = duckdb.connect("md:", config=C.md_config("1GB"))
    try:
        con.execute(f"DROP DATABASE IF EXISTS {C.MD_DB}")
    finally:
        con.close()


def ensure_onefile():
    if ONEFILE.exists():
        return
    E2E.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={**C.md_config("2GB"), "threads": 4})
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{C.dsn('replica', 'runtime')}' AS pg (TYPE postgres, READ_ONLY)")
    con.execute(f"COPY (SELECT * FROM pg.public.lineitem) TO '{ONEFILE}' (FORMAT parquet)")
    con.close()


def rss_cpu():
    return C.self_cpu(), C.self_maxrss_mb()


def route_upload(threads: int) -> dict:
    ensure_onefile()
    con = md_connect(threads)
    c0 = C.self_cpu()
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE TABLE stage_upload AS SELECT * FROM read_parquet('{ONEFILE}')")
    wall = time.time() - t0
    n = con.execute("SELECT count(*) FROM stage_upload").fetchone()[0]
    con.execute("DROP TABLE stage_upload")
    con.close()
    return {"wall_s": round(wall, 2), "rows": n, "client_cpu_s": round(C.self_cpu() - c0, 2),
            "parquet_bytes": ONEFILE.stat().st_size}


def route_duckdb(threads: int) -> dict:
    con = md_connect(threads)
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{C.dsn('replica', 'runtime')}' AS pg (TYPE postgres, READ_ONLY)")
    con.execute("SELECT count(*) FROM pg.public.lineitem WHERE false").fetchall()
    sampler = C.BackendSampler().start()
    c0 = C.self_cpu()
    t0 = time.time()
    con.execute("CREATE OR REPLACE TABLE stage_i AS SELECT * FROM pg.public.lineitem")
    wall = time.time() - t0
    pg = sampler.stop()
    cpu = C.self_cpu() - c0
    n = con.execute("SELECT count(*) FROM stage_i").fetchone()[0]
    con.execute("DROP TABLE stage_i")
    con.close()
    return {"wall_s": round(wall, 2), "rows": n, "client_cpu_s": round(cpu, 2), **pg}


def _chunk_worker(dsn, snap, preds, q, out):
    import psycopg
    import pyarrow.parquet as pq
    import pyarrow.csv as pcsv
    from bench_copy import _arrow_opts
    ro, po, co = _arrow_opts("csv")
    c0 = C.self_cpu()
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        c.execute(f"SET TRANSACTION SNAPSHOT '{snap}'")
        cols = ", ".join(C.COLUMNS)
        while True:
            i = q.get()
            if i is None:
                break
            buf = bytearray()
            with c.cursor().copy(f"COPY (SELECT {cols} FROM public.lineitem WHERE {preds[i]}) TO STDOUT (FORMAT csv)") as cp:
                for d in cp:
                    buf += d
            tbl = pcsv.read_csv(io.BytesIO(buf), read_options=ro, parse_options=po, convert_options=co)
            pq.write_table(tbl, CHUNKS / f"part_{i:04d}.parquet")
        c.execute("COMMIT")
    out.put({"cpu": C.self_cpu() - c0, "rss": C.self_maxrss_mb()})


def make_chunks(workers: int, n_chunks: int = 32) -> dict:
    from bench_copy import chunk_predicates
    shutil.rmtree(CHUNKS, ignore_errors=True)
    CHUNKS.mkdir(parents=True)
    preds = chunk_predicates("ctid", n_chunks, "replica")
    sc = C.pg_connect("replica", "runtime")
    sc.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    snap = sc.execute("select pg_export_snapshot()").fetchone()[0]
    ctx = mp.get_context("spawn")
    q, out = ctx.Queue(), ctx.Queue()
    for i in range(len(preds)):
        q.put(i)
    for _ in range(workers):
        q.put(None)
    t0 = time.time()
    ps = [ctx.Process(target=_chunk_worker, args=(C.dsn("replica", "runtime"), snap, preds, q, out))
          for _ in range(workers)]
    for p in ps:
        p.start()
    res = [out.get(timeout=1800) for _ in ps]
    for p in ps:
        p.join()
    wall = time.time() - t0
    sc.execute("COMMIT")
    sc.close()
    size = sum(f.stat().st_size for f in CHUNKS.glob("*.parquet"))
    return {"read_to_parquet_s": round(wall, 2), "read_client_cpu_s": round(sum(r["cpu"] for r in res), 2),
            "read_peak_rss_mb_sum": round(sum(r["rss"] for r in res), 1), "chunk_files": len(preds),
            "chunk_bytes": size}


def route_copy_parquet(threads: int, workers: int) -> dict:
    ch = make_chunks(workers)
    con = md_connect(threads)
    c0 = C.self_cpu()
    t0 = time.time()
    con.execute(f"CREATE OR REPLACE TABLE stage_ii AS SELECT * FROM read_parquet('{CHUNKS}/*.parquet')")
    load = time.time() - t0
    n = con.execute("SELECT count(*) FROM stage_ii").fetchone()[0]
    con.execute("DROP TABLE stage_ii")
    con.close()
    return {**ch, "load_s": round(load, 2), "load_client_cpu_s": round(C.self_cpu() - c0, 2),
            "wall_s": round(ch["read_to_parquet_s"] + load, 2), "rows": n}


def _loader(files, threads, out):
    con = md_connect(threads, "1GB")
    c0 = C.self_cpu()
    times = []
    for f in files:
        t = time.time()
        con.execute(f"INSERT INTO stage_iii SELECT * FROM read_parquet('{f}')")
        times.append(round(time.time() - t, 2))
    con.close()
    out.put({"cpu": C.self_cpu() - c0, "rss": C.self_maxrss_mb(), "insert_s": times, "done": time.time()})


def route_loaders(k: int, threads: int) -> dict:
    files = sorted(str(f) for f in CHUNKS.glob("*.parquet"))
    if not files:
        make_chunks(4)
        files = sorted(str(f) for f in CHUNKS.glob("*.parquet"))
    con = md_connect(threads)
    con.execute(f"CREATE OR REPLACE TABLE stage_iii {C.MD_DDL}")
    ctx = mp.get_context("spawn")
    out = ctx.Queue()
    per_thread = max(1, threads // k)
    ps = [ctx.Process(target=_loader, args=(files[j::k], per_thread, out)) for j in range(k)]
    t0 = time.time()
    for p in ps:
        p.start()
    res = [out.get(timeout=3600) for _ in ps]
    for p in ps:
        p.join()
    wall = max(r["done"] for r in res) - t0
    n = con.execute("SELECT count(*) FROM stage_iii").fetchone()[0]
    con.execute("DROP TABLE stage_iii")
    con.close()
    ins = sorted(x for r in res for x in r["insert_s"])
    return {"k": k, "threads_per_loader": per_thread, "wall_s": round(wall, 2), "rows": n,
            "client_cpu_s": round(sum(r["cpu"] for r in res), 2),
            "peak_rss_mb_sum": round(sum(r["rss"] for r in res), 1),
            "insert_s_median": ins[len(ins) // 2], "insert_s_max": ins[-1], "chunks": len(files)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--routes", default="upload,duckdb,copy_parquet,loaders")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--k", default="1,2,4")
    ap.add_argument("--label", default="")
    ap.add_argument("--drop", action="store_true")
    a = ap.parse_args()
    if a.drop:
        drop_db()
        print(f"dropped {C.MD_DB}")
        return
    ensure_db()
    rows_total = C.table_rows()
    for rep in range(a.repeats):
        for route in a.routes.split(","):
            ks = [int(x) for x in a.k.split(",")] if route == "loaders" else [None]
            for k in ks:
                la = C.load_avg()
                t = time.time()
                if route == "upload":
                    r = route_upload(a.threads)
                elif route == "duckdb":
                    r = route_duckdb(a.threads)
                elif route == "copy_parquet":
                    r = route_copy_parquet(a.threads, a.workers)
                else:
                    r = route_loaders(k, a.threads)
                rec = {"route": route, "repeat": rep + 1, "threads": a.threads, "workers": a.workers,
                       "label": a.label, "load_avg": la, **r, "utc": C.utc()}
                rec["rows_per_s"] = round(rec["rows"] / rec["wall_s"])
                rec["rows_ok"] = rec["rows"] == rows_total
                C.append_jsonl("e2e.jsonl", rec)
                print(json.dumps(rec), flush=True)


if __name__ == "__main__":
    main()
