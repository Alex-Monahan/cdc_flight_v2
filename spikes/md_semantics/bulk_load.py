"""Q8 (SPEC 3.1): bulk-load rate into MotherDuck at TPC-H lineitem width — FIRST INDICATION ONLY.

Client-side methods move data from this machine to MotherDuck, so on a laptop they measure the laptop's uplink, not
MotherDuck. The numbers that count are measured later by running this same script inside a Flight.

Methods (each loads into a fresh table; rate = rows / wall time including the commit):
  parquet_insert   INSERT INTO t SELECT * FROM read_parquet(<local files>)
  parquet_copy     COPY t FROM <local parquet files>
  arrow_stream     INSERT INTO t SELECT * FROM <registered pyarrow RecordBatchReader> (streams, bounded memory)
  appender         DuckDBPyConnection.append(table, pandas DataFrame) per 1M-row chunk (needs pandas)
  csv_insert       INSERT INTO t SELECT * FROM read_csv(<local CSV files>)
  parallel_parquet P processes, each its own connection, each loading a share of the files into the same table
  server_generator INSERT INTO t SELECT <generator over range()> — runs entirely on the server
  server_copy      INSERT INTO t2 SELECT * FROM t — table to table on the server

Portable: everything comes from the environment or arguments; temporary files go under CDC_V2_SPIKE_TMP (or the
system temp directory) and are deleted at the end.
  motherduck_token        MotherDuck token
  CDC_V2_BULK_ROWS        comma list of row counts (default 1000000,10000000)
  CDC_V2_BULK_METHODS     comma list of methods (default: all)
  CDC_V2_BULK_PARALLEL    processes for parallel_parquet (default 4)
  CDC_V2_BULK_DB          database name (default cdc_v2_spike_md_bulk)
  CDC_V2_SPIKE_TMP, CDC_V2_SPIKE_EVIDENCE, CDC_V2_MEMORY_LIMIT  (see common.py)

Run:  tools/with-env uv run --with pyarrow --with pandas python spikes/md_semantics/bulk_load.py
"""
from __future__ import annotations

import multiprocessing as mp
import os
import shutil
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import LINEITEM_DDL, Evidence, connect, create_db, drop_db, err_class, lineitem_select, tmp_dir  # noqa

DB = os.environ.get("CDC_V2_BULK_DB", "cdc_v2_spike_md_bulk")
CHUNK = 1_000_000
ALL = ["parquet_insert", "parquet_copy", "arrow_stream", "appender", "csv_insert", "parallel_parquet",
       "server_generator", "server_copy"]


def generate(rows: int, d: Path, csv: bool) -> tuple[list[Path], list[Path], int]:
    """Write `rows` lineitem rows as 1M-row Parquet (and CSV) files with a local in-memory DuckDB."""
    d.mkdir(parents=True, exist_ok=True)
    local = duckdb.connect(config={"memory_limit": os.environ.get("CDC_V2_MEMORY_LIMIT", "4GB"),
                                   "temp_directory": str(tmp_dir() / "duckdb_tmp")})
    pq, cs = [], []
    for i, start in enumerate(range(0, rows, CHUNK)):
        n = min(CHUNK, rows - start)
        p = d / f"part_{i:04d}.parquet"
        local.execute(f"COPY ({lineitem_select(start, n)}) TO '{p}' (FORMAT parquet, COMPRESSION zstd)")
        pq.append(p)
        if csv:
            c = d / f"part_{i:04d}.csv"
            local.execute(f"COPY ({lineitem_select(start, n)}) TO '{c}' (FORMAT csv, HEADER true)")
            cs.append(c)
    local.close()
    size = sum(p.stat().st_size for p in pq)
    return pq, cs, size


def fresh(con, name: str) -> None:
    con.execute(f"CREATE OR REPLACE TABLE {name} {LINEITEM_DDL}")


def files_sql(files: list[Path]) -> str:
    return "[" + ",".join(f"'{f}'" for f in files) + "]"


def par_worker(files: list[str], q, ready, go) -> None:
    con = connect(DB)
    ready.put(1)
    go.wait()
    t0 = time.time()
    try:
        con.execute(f"INSERT INTO t_par SELECT * FROM read_parquet([{','.join(repr(f) for f in files)}])")
        q.put(("ok", time.time() - t0))
    except Exception as e:
        q.put(("error", err_class(e)))
    con.close()


def run_method(ev: Evidence, con, method: str, rows: int, pq: list[Path], cs: list[Path], parallel: int) -> dict:
    t0 = time.time()
    if method == "parquet_insert":
        fresh(con, "t_load")
        con.execute(f"INSERT INTO t_load SELECT * FROM read_parquet({files_sql(pq)})")
    elif method == "parquet_copy":
        fresh(con, "t_load")
        con.execute(f"COPY t_load FROM '{pq[0].parent}/*.parquet' (FORMAT parquet)")
    elif method == "arrow_stream":
        import pyarrow as pa
        import pyarrow.parquet as papq
        fresh(con, "t_load")
        schema = papq.ParquetFile(pq[0]).schema_arrow

        def batches():
            for f in pq:
                yield from papq.ParquetFile(f).iter_batches(batch_size=122_880)

        reader = pa.RecordBatchReader.from_batches(schema, batches())
        con.register("arrow_src", reader)
        t0 = time.time()  # reader setup excluded; reading the files is part of the stream
        con.execute("INSERT INTO t_load SELECT * FROM arrow_src")
        con.unregister("arrow_src")
    elif method == "appender":
        import pyarrow.parquet as papq
        fresh(con, "t_load")
        for f in pq:
            df = papq.read_table(f).to_pandas()
            con.append("t_load", df)
    elif method == "csv_insert":
        fresh(con, "t_load")
        con.execute(f"INSERT INTO t_load SELECT * FROM read_csv({files_sql(cs)}, header = true, "
                    f"columns = {{{', '.join(repr(c.split()[0]) + ': ' + repr(' '.join(c.split()[1:])) for c in LINEITEM_DDL.strip()[1:-1].replace(chr(10), ' ').split(', '))}}})")
    elif method == "parallel_parquet":
        fresh(con, "t_par")
        shares = [[str(f) for f in pq[i::parallel]] for i in range(parallel)]
        shares = [s for s in shares if s]
        ctx = mp.get_context("spawn")
        q, ready, go = ctx.Queue(), ctx.Queue(), ctx.Event()
        ps = [ctx.Process(target=par_worker, args=(s, q, ready, go)) for s in shares]
        for p in ps:
            p.start()
        for _ in ps:
            ready.get()
        t0 = time.time()  # all connections open; time the load only
        go.set()
        outs = [q.get() for _ in ps]
        for p in ps:
            p.join()
        errs = [o[1] for o in outs if o[0] == "error"]
        if errs:
            raise RuntimeError(errs[0])
        n = con.execute("SELECT count(*) FROM t_par").fetchone()[0]
        con.execute("DROP TABLE t_par")
        dt = time.time() - t0
        return {"rows": n, "seconds": round(dt, 2), "rows_per_s": round(n / dt), "processes": len(shares)}
    elif method == "server_generator":
        fresh(con, "t_load")
        con.execute(f"INSERT INTO t_load {lineitem_select(0, rows)}")
    elif method == "server_copy":
        fresh(con, "t_src")
        con.execute(f"INSERT INTO t_src {lineitem_select(0, rows)}")
        con.execute("CREATE OR REPLACE TABLE t_load AS SELECT * FROM t_src LIMIT 0")
        t0 = time.time()
        con.execute("INSERT INTO t_load SELECT * FROM t_src")
        dt = time.time() - t0
        con.execute("DROP TABLE t_src")
        n = con.execute("SELECT count(*) FROM t_load").fetchone()[0]
        con.execute("DROP TABLE t_load")
        return {"rows": n, "seconds": round(dt, 2), "rows_per_s": round(n / dt)}
    dt = time.time() - t0
    n = con.execute("SELECT count(*) FROM t_load").fetchone()[0]
    con.execute("DROP TABLE t_load")
    return {"rows": n, "seconds": round(dt, 2), "rows_per_s": round(n / dt)}


def main() -> None:
    rows_list = [int(x) for x in os.environ.get("CDC_V2_BULK_ROWS", "1000000,10000000").split(",")]
    methods = [m for m in os.environ.get("CDC_V2_BULK_METHODS", ",".join(ALL)).split(",") if m]
    parallel = int(os.environ.get("CDC_V2_BULK_PARALLEL", "4"))
    ev = Evidence(os.environ.get("CDC_V2_BULK_EVIDENCE_NAME", "bulk_load"))
    ev.put("caveat", "client-side methods are limited by this machine's uplink; the numbers that count are "
                     "measured by running this script inside a Flight")
    ev.log("CAVEAT: client-side rates below are limited by the uplink of the machine running this; first "
           "indication only.")
    work = tmp_dir() / "bulk"
    create_db(DB)
    con = connect(DB)
    res = {}
    try:
        for rows in rows_list:
            need_files = any(m not in ("server_generator", "server_copy") for m in methods)
            pq, cs, size = ([], [], 0)
            if need_files:
                t0 = time.time()
                pq, cs, size = generate(rows, work / str(rows), csv="csv_insert" in methods)
                ev.log(f"== {rows:,} rows: generated {len(pq)} Parquet files, {size / 1e6:.0f} MB "
                       f"({size / rows:.0f} B/row compressed) in {time.time() - t0:.0f} s")
            for m in methods:
                try:
                    r = run_method(ev, con, m, rows, pq, cs, parallel)
                    if size and m not in ("server_generator", "server_copy"):
                        r["parquet_MB_per_s"] = round(size / 1e6 / r["seconds"], 1)
                    ev.log(f"   {m:18s} {rows:>11,} rows: {r['seconds']:8.1f} s {r['rows_per_s']:>10,} rows/s  {r}")
                except Exception as e:
                    r = {"error": err_class(e)}
                    ev.log(f"   {m:18s} {rows:>11,} rows: ERROR {err_class(e)}")
                    for t in ("t_load", "t_par", "t_src"):
                        try:
                            con.execute(f"DROP TABLE IF EXISTS {t}")
                        except Exception:
                            pass
                res[f"{m}@{rows}"] = r
            shutil.rmtree(work / str(rows), ignore_errors=True)
    finally:
        ev.put("results", res)
        con.close()
        drop_db(DB)
        shutil.rmtree(work, ignore_errors=True)
        ev.log("database dropped; temporary files removed")
        ev.write()


if __name__ == "__main__":
    main()
