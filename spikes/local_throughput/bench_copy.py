"""One measurement of our own parallel COPY reader against the replica. Prints one JSON object.

    tools/with-env uv run --group spike python spikes/local_throughput/bench_copy.py \
        --workers 4 --fmt binary --split ctid --sink count [--chunks 32] [--snapshot] [--node replica]

--fmt   text | csv | binary                    (COPY ... TO STDOUT (FORMAT x))
--split ctid  (TID range scan: ctid >= '(a,0)' AND ctid < '(b,0)')
        pk    (primary-key range: l_orderkey >= a AND l_orderkey < b)
--sink  count  bytes are read and counted, nothing else
        arrow  each chunk is parsed into an Arrow table (pyarrow.csv, one thread per worker) - text and csv
        rows   each row is decoded by psycopg's C loaders (binary) and counted - binary only
--snapshot  the coordinator exports a snapshot (pg_export_snapshot) and every worker imports it.

Worker processes (spawn) each hold one connection in a REPEATABLE READ READ ONLY transaction and pull chunks from a
shared queue. Each worker reports its own CPU time, peak RSS and the CPU time of its Postgres backend (read with
psutil just before it disconnects; the backends are local processes).
"""
from __future__ import annotations

import argparse
import io
import json
import multiprocessing as mp
import time

import common as C


def chunk_predicates(split: str, n_chunks: int, node: str) -> list[str]:
    with C.pg_connect(node, "runtime") as c:
        if split == "ctid":
            pages = c.execute("select pg_relation_size('public.lineitem') / current_setting('block_size')::int"
                              ).fetchone()[0]
            step = -(-pages // n_chunks)
            preds = []
            for i in range(n_chunks):
                a, b = i * step, (i + 1) * step
                if i == n_chunks - 1:
                    preds.append(f"ctid >= '({a},0)'::tid")
                else:
                    preds.append(f"ctid >= '({a},0)'::tid AND ctid < '({b},0)'::tid")
            return preds
        lo, hi = c.execute("select min(l_orderkey), max(l_orderkey) from public.lineitem").fetchone()
        step = -(-(hi - lo + 1) // n_chunks)
        return [f"l_orderkey >= {lo + i * step} AND l_orderkey < {lo + (i + 1) * step}" for i in range(n_chunks)]


ARROW_TYPES = None


def _arrow_opts(fmt: str):
    import pyarrow as pa
    import pyarrow.csv as pc
    dec = pa.decimal128(15, 2)
    types = dict(zip(C.COLUMNS, [pa.int64(), pa.int64(), pa.int64(), pa.int32(), dec, dec, dec, dec,
                                 pa.string(), pa.string(), pa.date32(), pa.date32(), pa.date32(), pa.string(),
                                 pa.string(), pa.string()]))
    read = pc.ReadOptions(column_names=C.COLUMNS, use_threads=False, block_size=8 << 20)
    if fmt == "text":
        parse = pc.ParseOptions(delimiter="\t", quote_char=False, escape_char="\\")
        conv = pc.ConvertOptions(column_types=types, null_values=["\\N"], strings_can_be_null=True)
    else:
        parse = pc.ParseOptions()
        conv = pc.ConvertOptions(column_types=types)
    return read, parse, conv


BIN_TYPES = ["int8", "int8", "int8", "int4", "numeric", "numeric", "numeric", "numeric", "bpchar", "bpchar", "date",
             "date", "date", "bpchar", "bpchar", "varchar"]


def worker(wid, dsn, snapshot, fmt, sink, preds, q, ready, go, out):
    import psutil
    import psycopg
    import pyarrow.csv as pc
    conn = psycopg.connect(dsn, autocommit=True)
    bpid = conn.info.backend_pid
    conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    if snapshot:
        conn.execute(f"SET TRANSACTION SNAPSHOT '{snapshot}'")
    opts = _arrow_opts(fmt) if sink == "arrow" else None
    bproc = psutil.Process(bpid)
    ready.put(wid)
    go.wait()
    t = bproc.cpu_times()
    b0 = t.user + t.system
    c0 = C.self_cpu()
    rows = nbytes = chunks = 0
    cols = ", ".join(C.COLUMNS)
    with conn.cursor() as cur:
        while True:
            i = q.get()
            if i is None:
                break
            sql = f"COPY (SELECT {cols} FROM public.lineitem WHERE {preds[i]}) TO STDOUT (FORMAT {fmt})"
            with cur.copy(sql) as cp:
                if sink == "count":
                    for data in cp:
                        nbytes += len(data)
                elif sink == "arrow":
                    buf = bytearray()
                    for data in cp:
                        buf += data
                    nbytes += len(buf)
                    tbl = pc.read_csv(io.BytesIO(buf), read_options=opts[0], parse_options=opts[1],
                                      convert_options=opts[2])
                    rows += tbl.num_rows
                    del tbl, buf
                elif sink == "rows":
                    cp.set_types(BIN_TYPES)
                    for _ in cp.rows():
                        rows += 1
            if sink == "count":
                rows = -1
            chunks += 1
    c1 = C.self_cpu()
    t = bproc.cpu_times()
    b1 = t.user + t.system
    conn.execute("COMMIT")
    conn.close()
    out.put({"wid": wid, "rows": rows, "bytes": nbytes, "chunks": chunks, "client_cpu_s": c1 - c0,
             "backend_cpu_s": b1 - b0, "maxrss_mb": C.self_maxrss_mb(), "done_at": time.time()})


def run(workers: int, fmt: str, split: str, sink: str, n_chunks: int, snapshot: bool, node: str,
        max_chunks: int | None = None) -> dict:
    preds = chunk_predicates(split, n_chunks, node)
    if max_chunks:
        preds = preds[:max_chunks]
    d = C.dsn(node, "runtime")
    ctx = mp.get_context("spawn")
    q, ready, out, go = ctx.Queue(), ctx.Queue(), ctx.Queue(), ctx.Event()
    snap_conn = None
    snap = None
    if snapshot:
        snap_conn = C.pg_connect(node, "runtime")
        snap_conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        snap = snap_conn.execute("select pg_export_snapshot()").fetchone()[0]
    for i in range(len(preds)):
        q.put(i)
    for _ in range(workers):
        q.put(None)
    procs = [ctx.Process(target=worker, args=(w, d, snap, fmt, sink, preds, q, ready, go, out))
             for w in range(workers)]
    for p in procs:
        p.start()
    for _ in procs:
        ready.get(timeout=60)
    la = C.load_avg()
    t0 = time.time()
    go.set()
    res = [out.get(timeout=3600) for _ in procs]
    wall = max(r["done_at"] for r in res) - t0
    for p in procs:
        p.join()
    if snap_conn:
        snap_conn.execute("COMMIT")
        snap_conn.close()
    nbytes = sum(r["bytes"] for r in res)
    rows = sum(r["rows"] for r in res) if sink != "count" else None
    if rows is None and not max_chunks:  # count-only over the whole table: the row count recorded by setup
        rows = C.table_rows()
    client_cpu = sum(r["client_cpu_s"] for r in res)
    backend_cpu = sum(r["backend_cpu_s"] for r in res)
    return {"route": "copy", "workers": workers, "fmt": fmt, "split": split, "sink": sink, "chunks": len(preds),
            "snapshot": snapshot, "node": node, "load_avg": la, "wall_s": round(wall, 3), "rows": rows,
            "bytes": nbytes, "rows_per_s": round(rows / wall) if rows else None,
            "mb_per_s": round(nbytes / wall / 1e6, 1), "client_cpu_s": round(client_cpu, 2),
            "backend_cpu_s": round(backend_cpu, 2), "client_cpu_util": round(client_cpu / wall, 2),
            "backend_cpu_util": round(backend_cpu / wall, 2),
            "client_peak_rss_mb_sum": round(sum(r["maxrss_mb"] for r in res), 1),
            "client_peak_rss_mb_max_worker": round(max(r["maxrss_mb"] for r in res), 1), "utc": C.utc()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--fmt", choices=["text", "csv", "binary"], default="binary")
    ap.add_argument("--split", choices=["ctid", "pk"], default="ctid")
    ap.add_argument("--sink", choices=["count", "arrow", "rows"], default="count")
    ap.add_argument("--chunks", type=int, default=32)
    ap.add_argument("--max-chunks", type=int)
    ap.add_argument("--snapshot", action="store_true")
    ap.add_argument("--node", default="replica")
    a = ap.parse_args()
    print(json.dumps(run(a.workers, a.fmt, a.split, a.sink, a.chunks, a.snapshot, a.node, a.max_chunks)))


if __name__ == "__main__":
    main()
