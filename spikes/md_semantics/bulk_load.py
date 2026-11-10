"""Q8 (SPEC 3.1): bulk-load rate into MotherDuck at TPC-H lineitem width.

On a laptop the client-side methods measure the laptop's uplink, not MotherDuck: first indication only. The same
script runs unchanged inside a Flight (flights/spike_md), where the numbers count.

Methods (each loads into a fresh table; rate = rows / wall time including the commit):
  parquet_insert   INSERT INTO t SELECT * FROM read_parquet(<local files>)
  parquet_copy     COPY t FROM <local parquet files>
  arrow_stream     INSERT INTO t SELECT * FROM <registered pyarrow RecordBatchReader> (streams, bounded memory)
  appender         DuckDBPyConnection.append(table, pandas DataFrame) per file (needs pandas)
  csv_insert       INSERT INTO t SELECT * FROM read_csv(<local CSV files>)
  parallel_parquet P processes, each its own connection, each loading a share of the Parquet files into one table
  parallel_arrow   the same, each process streaming its share as Arrow record batches
  server_generator INSERT INTO t SELECT <generator over range()> — runs entirely on the server
  server_copy      INSERT INTO t2 SELECT * FROM t — table to table on the server
Each result also has the client's CPU seconds (user + system; for parallel methods the loader processes' own CPU
during the load plus the parent's) per million rows, and the peak resident memory (Linux: VmRSS sampled every
50 ms; parallel: the largest loader process).

Portable: everything comes from the environment; temporary files go under CDC_V2_SPIKE_TMP (or the system temp
directory) and are deleted at the end.
  motherduck_token / MOTHERDUCK_TOKEN   MotherDuck token
  CDC_V2_BULK_ROWS        comma list of row counts (default 1000000,10000000)
  CDC_V2_BULK_METHODS     comma list of methods (default: all)
  CDC_V2_BULK_PARALLEL    comma list of process counts for the parallel_* methods (default 4)
  CDC_V2_BULK_CHUNK       rows per generated Parquet file (default 250000)
  CDC_V2_BULK_DB          database name (default cdc_v2_spike_md_bulk)
  CDC_V2_BULK_SOURCE      generator (default; synthetic lineitem-shaped rows) or tpch (dbgen lineitem, sf = rows/6M)
  CDC_V2_SPIKE_TMP, CDC_V2_SPIKE_EVIDENCE, CDC_V2_MEMORY_LIMIT  (see common.py)

Run:  tools/with-env uv run --with pyarrow --with pandas python spikes/md_semantics/bulk_load.py
"""
from __future__ import annotations

import multiprocessing as mp
import os
import shutil
import sys
import threading
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (LINEITEM_DDL, Evidence, cgroup_cpus, connect, cpu_seconds, create_db, drop_db,  # noqa: E402
                    err_class, lineitem_select, peak_rss_mb, tmp_dir)

DB = os.environ.get("CDC_V2_BULK_DB", "cdc_v2_spike_md_bulk")
CHUNK = int(os.environ.get("CDC_V2_BULK_CHUNK", "250000"))
ALL = ["parquet_insert", "parquet_copy", "arrow_stream", "appender", "csv_insert", "parallel_parquet",
       "parallel_arrow", "server_generator", "server_copy"]
SERVER = ("server_generator", "server_copy")


def generate(rows: int, d: Path, csv: bool) -> tuple[list[Path], list[Path], int, int]:
    """Write lineitem rows as CHUNK-row Parquet (and CSV) files with a local in-memory DuckDB."""
    d.mkdir(parents=True, exist_ok=True)
    cfg = {"memory_limit": os.environ.get("CDC_V2_MEMORY_LIMIT", "4GB"),
           "temp_directory": str(tmp_dir() / "duckdb_tmp")}
    if cgroup_cpus():
        cfg["threads"] = cgroup_cpus()
    local = duckdb.connect(config=cfg)
    pq, cs = [], []
    source = os.environ.get("CDC_V2_BULK_SOURCE", "generator")
    if source == "tpch":  # real TPC-H lineitem from DuckDB's tpch extension; scale factor chosen from rows
        local.execute("INSTALL tpch")
        local.execute("LOAD tpch")
        local.execute(f"CALL dbgen(sf = {max(rows / 6_000_000, 0.01)})")
        rows = local.execute("SELECT count(*) FROM lineitem").fetchone()[0]
    for i, start in enumerate(range(0, rows, CHUNK)):
        n = min(CHUNK, rows - start)
        p = d / f"part_{i:04d}.parquet"
        q = (f"SELECT * FROM lineitem LIMIT {n} OFFSET {start}" if source == "tpch"
             else lineitem_select(start, n))
        local.execute(f"COPY ({q}) TO '{p}' (FORMAT parquet, COMPRESSION zstd)")
        pq.append(p)
        if csv:
            c = d / f"part_{i:04d}.csv"
            local.execute(f"COPY ({q}) TO '{c}' (FORMAT csv, HEADER true)")
            cs.append(c)
    local.close()
    return pq, cs, sum(p.stat().st_size for p in pq), rows


def fresh(con, name: str) -> None:
    con.execute(f"CREATE OR REPLACE TABLE {name} {LINEITEM_DDL}")


def files_sql(files) -> str:
    return "[" + ",".join(f"'{f}'" for f in files) + "]"


class RssSampler:
    """Peak resident memory of this process while a block runs (Linux: VmRSS every 50 ms)."""

    def __init__(self):
        self.peak, self.stop = 0, threading.Event()
        self.t = threading.Thread(target=self._run, daemon=True)

    @staticmethod
    def _rss() -> int:
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) * 1024
        except OSError:
            pass
        return 0

    def _run(self):
        while not self.stop.is_set():
            self.peak = max(self.peak, self._rss())
            time.sleep(0.05)

    def __enter__(self):
        self.t.start()
        return self

    def __exit__(self, *a):
        self.stop.set()
        self.t.join()
        if self.peak == 0:  # no /proc (macOS): the process high-water mark
            self.peak = int(peak_rss_mb() * 2**20)


def arrow_reader(files):
    import pyarrow as pa
    import pyarrow.parquet as papq
    schema = papq.ParquetFile(files[0]).schema_arrow

    def batches():
        for f in files:
            yield from papq.ParquetFile(f).iter_batches(batch_size=122_880)

    return pa.RecordBatchReader.from_batches(schema, batches())


def par_worker(files: list[str], kind: str, q, ready, go) -> None:
    con = connect(DB)
    ready.put(1)
    go.wait()
    t0, c0 = time.time(), cpu_seconds()
    try:
        with RssSampler() as rs:
            if kind == "arrow":
                con.register("arrow_src", arrow_reader(files))
                con.execute("INSERT INTO t_par SELECT * FROM arrow_src")
            else:
                con.execute(f"INSERT INTO t_par SELECT * FROM read_parquet({files_sql(files)})")
        q.put(("ok", time.time() - t0, cpu_seconds() - c0, rs.peak))
    except Exception as e:
        q.put(("error", err_class(e), 0.0, 0))
    con.close()


def load(con, method: str, rows: int, pq, cs, parallel: int) -> dict:
    """Returns rows, seconds and, for parallel methods, cpu_s and peak_rss_mb of the loaders."""
    if method in ("parallel_parquet", "parallel_arrow"):
        fresh(con, "t_par")
        shares = [s for s in ([str(f) for f in pq[i::parallel]] for i in range(parallel)) if s]
        ctx = mp.get_context("spawn")
        q, ready, go = ctx.Queue(), ctx.Queue(), ctx.Event()
        kind = "arrow" if method == "parallel_arrow" else "parquet"
        ps = [ctx.Process(target=par_worker, args=(s, kind, q, ready, go)) for s in shares]
        for p in ps:
            p.start()
        for _ in ps:
            ready.get()
        t0 = time.time()  # all connections open: time the load only
        go.set()
        outs = [q.get() for _ in ps]
        dt = time.time() - t0
        for p in ps:
            p.join()
        errs = [o[1] for o in outs if o[0] == "error"]
        if errs:
            raise RuntimeError(errs[0])
        n = con.execute("SELECT count(*) FROM t_par").fetchone()[0]
        con.execute("DROP TABLE t_par")
        return {"rows": n, "seconds": dt, "processes": len(shares), "cpu_s": sum(o[2] for o in outs),
                "peak_rss_mb": round(max(o[3] for o in outs) / 2**20), "peak_rss_note": "largest loader process"}
    if method == "server_copy":
        fresh(con, "t_src")
        con.execute(f"INSERT INTO t_src {lineitem_select(0, rows)}")
        con.execute("CREATE OR REPLACE TABLE t_load AS SELECT * FROM t_src LIMIT 0")
        t0 = time.time()
        con.execute("INSERT INTO t_load SELECT * FROM t_src")
        dt = time.time() - t0
        con.execute("DROP TABLE t_src")
    else:
        fresh(con, "t_load")
        t0 = time.time()
        if method == "parquet_insert":
            con.execute(f"INSERT INTO t_load SELECT * FROM read_parquet({files_sql(pq)})")
        elif method == "parquet_copy":
            con.execute(f"COPY t_load FROM '{pq[0].parent}/*.parquet' (FORMAT parquet)")
        elif method == "arrow_stream":
            con.register("arrow_src", arrow_reader(pq))
            con.execute("INSERT INTO t_load SELECT * FROM arrow_src")
            con.unregister("arrow_src")
        elif method == "appender":
            import pyarrow as pa
            import pyarrow.parquet as papq
            for f in pq:
                tbl = papq.read_table(f)
                # pandas holds DECIMAL as Python objects, which append() mis-infers; send float64 (DuckDB casts back)
                tbl = tbl.cast(pa.schema([pa.field(x.name, pa.float64()) if pa.types.is_decimal(x.type) else x
                                          for x in tbl.schema]))
                con.append("t_load", tbl.to_pandas())
        elif method == "csv_insert":
            cols = ", ".join(repr(c.split()[0]) + ": " + repr(" ".join(c.split()[1:]))
                             for c in LINEITEM_DDL.strip()[1:-1].replace(chr(10), " ").split(", "))
            con.execute(f"INSERT INTO t_load SELECT * FROM read_csv({files_sql(cs)}, header = true, "
                        f"columns = {{{cols}}})")
        elif method == "server_generator":
            con.execute(f"INSERT INTO t_load {lineitem_select(0, rows)}")
        else:
            raise ValueError(method)
        dt = time.time() - t0
    n = con.execute("SELECT count(*) FROM t_load").fetchone()[0]
    con.execute("DROP TABLE t_load")
    return {"rows": n, "seconds": dt}


def run_method(con, method: str, rows: int, pq, cs, parallel: int) -> dict:
    c0 = cpu_seconds()
    with RssSampler() as rs:
        r = load(con, method, rows, pq, cs, parallel)
    if "cpu_s" not in r:
        r["cpu_s"] = cpu_seconds() - c0  # includes setup statements: an upper bound
    r.setdefault("peak_rss_mb", round(rs.peak / 2**20))
    r["rows_per_s"] = round(r["rows"] / r["seconds"])
    r["cpu_s_per_million_rows"] = round(r["cpu_s"] / r["rows"] * 1e6, 2)
    r["seconds"] = round(r["seconds"], 2)
    r["cpu_s"] = round(r["cpu_s"], 2)
    return r


def main() -> None:
    rows_list = [int(x) for x in os.environ.get("CDC_V2_BULK_ROWS", "1000000,10000000").split(",")]
    methods = [m for m in os.environ.get("CDC_V2_BULK_METHODS", ",".join(ALL)).split(",") if m]
    par_list = [int(x) for x in os.environ.get("CDC_V2_BULK_PARALLEL", "4").split(",")]
    ev = Evidence(os.environ.get("CDC_V2_BULK_EVIDENCE_NAME", "bulk_load"))
    ev.put("client_cpus_cgroup", cgroup_cpus())
    in_flight = bool(os.environ.get("MOTHERDUCK_FLIGHT_ID"))
    ev.put("in_flight", in_flight)
    if not in_flight:
        ev.log("CAVEAT: client-side rates below are limited by the uplink of the machine running this; first "
               "indication only.")
    work = tmp_dir() / "bulk"
    create_db(DB)
    con = connect(DB)
    res = {}
    try:
        for rows in rows_list:
            pq, cs, size, actual = [], [], 0, rows
            if any(m not in SERVER for m in methods):
                t0 = time.time()
                pq, cs, size, actual = generate(rows, work / str(rows), csv="csv_insert" in methods)
                ev.log(f"== {actual:,} rows: generated {len(pq)} Parquet files, {size / 1e6:.0f} MB "
                       f"({size / actual:.0f} B/row compressed) in {time.time() - t0:.0f} s")
            for m0 in methods:
                for par in (par_list if m0.startswith("parallel_") else [0]):
                    m = f"{m0}_{par}" if par else m0
                    try:
                        r = run_method(con, m0, rows, pq, cs, par)
                        if size and m0 not in SERVER:
                            r["parquet_MB_per_s"] = round(size / 1e6 / r["seconds"], 1)
                        ev.log(f"   {m:20s} {r['rows']:>11,} rows: {r['seconds']:7.1f} s {r['rows_per_s']:>10,} "
                               f"rows/s  {r}")
                    except Exception as e:
                        r = {"error": err_class(e)}
                        ev.log(f"   {m:20s} {rows:>11,} rows: ERROR {err_class(e)}")
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
