"""One measurement of DuckDB's Postgres reader against the replica. Prints one JSON object.

    tools/with-env uv run --group spike python spikes/local_throughput/bench_duckdb.py \
        --threads 4 [--pages-per-task 1000] [--binary 1] [--ctid 1] [--pool 15] [--sink hash|parquet] \
        [--memory-limit 4GB]

ATTACH '<replica dsn>' AS pg (TYPE postgres, READ_ONLY). Every column is materialised:
  hash     SELECT count(*), sum(hash(<all 16 columns>)) FROM pg.public.lineitem   (sink discarded)
  parquet  COPY (SELECT * FROM pg.public.lineitem) TO '<work>/lineitem.parquet'  (what a staged load writes)
The replica's backends of the runtime role are sampled for CPU time with psutil (BackendSampler).
"""
from __future__ import annotations

import argparse
import json
import os
import time

import duckdb

import common as C


def run(threads: int, pages_per_task: int, binary: bool, ctid: bool, pool: int, sink: str, memory_limit: str,
        node: str = "replica") -> dict:
    con = duckdb.connect(config={**C.md_config(memory_limit), "threads": threads})
    con.execute("LOAD postgres")
    con.execute(f"SET pg_pages_per_task = {pages_per_task}")
    con.execute(f"SET pg_use_binary_copy = {str(binary).lower()}")
    con.execute(f"SET pg_use_ctid_scan = {str(ctid).lower()}")
    con.execute(f"SET pg_pool_max_connections = {pool}")
    con.execute(f"ATTACH '{C.dsn(node, 'runtime')}' AS pg (TYPE postgres, READ_ONLY)")
    con.execute("SELECT count(*) FROM pg.public.lineitem WHERE false").fetchall()  # catalog load outside the timer
    cols = ", ".join(C.COLUMNS)
    out = C.WORK / f"bench_{os.getpid()}.parquet"
    if sink == "hash":
        sql = f"SELECT count(*), sum(hash({cols})) FROM pg.public.lineitem"
    else:
        sql = f"COPY (SELECT {cols} FROM pg.public.lineitem) TO '{out}' (FORMAT parquet)"
    sampler = C.BackendSampler(node).start()
    la = C.load_avg()
    c0 = C.self_cpu()
    t0 = time.time()
    r = con.execute(sql).fetchall()
    wall = time.time() - t0
    c1 = C.self_cpu()
    pg = sampler.stop()
    con.close()
    rows = int(r[0][0])
    size = None
    if sink == "parquet":
        size = out.stat().st_size
        out.unlink()
    nbytes = C.table_heap_bytes()
    return {"route": "duckdb", "threads": threads, "pages_per_task": pages_per_task, "binary": binary,
            "ctid_scan": ctid, "pool": pool, "sink": sink, "memory_limit": memory_limit, "node": node,
            "load_avg": la, "wall_s": round(wall, 3), "rows": rows, "rows_per_s": round(rows / wall),
            "heap_mb_per_s": round(nbytes / wall / 1e6, 1), "parquet_bytes": size,
            "client_cpu_s": round(c1 - c0, 2), "client_cpu_util": round((c1 - c0) / wall, 2),
            "backend_cpu_s": pg["backend_cpu_s"], "backend_cpu_util": round(pg["backend_cpu_s"] / wall, 2),
            "backends_seen": pg["backends_seen"], "client_peak_rss_mb": round(C.self_maxrss_mb(), 1),
            "utc": C.utc()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--pages-per-task", type=int, default=1000)
    ap.add_argument("--binary", type=int, default=1)
    ap.add_argument("--ctid", type=int, default=1)
    ap.add_argument("--pool", type=int, default=15)
    ap.add_argument("--sink", choices=["hash", "parquet"], default="hash")
    ap.add_argument("--memory-limit", default="4GB")
    a = ap.parse_args()
    print(json.dumps(run(a.threads, a.pages_per_task, bool(a.binary), bool(a.ctid), a.pool, a.sink,
                         a.memory_limit)))


if __name__ == "__main__":
    main()
