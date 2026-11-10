"""Speed and memory of the transform (opt-in, local DuckDB only; indications, measured on a shared laptop).

    CDC_V2_Y_MEASURE=1 uv run pytest tests/transform/test_transform_measure.py -s

Rows per second and input bytes per second on one core (`threads=1`), for a table of TPC-H lineitem width, a
narrow table and a table of 200 columns, both directions, batch sizes 1, 1,000 and 100,000. The peak client memory
for a batch with one 100 MB value is measured in a fresh process (maximum resident set size).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import pyarrow as pa
import pytest

from cdc_v2 import transform as T
import pgref as R
from tables import col, numeric_typmod, table

pytestmark = pytest.mark.skipif(os.environ.get("CDC_V2_Y_MEASURE") != "1", reason="set CDC_V2_Y_MEASURE=1")
V18 = 180006

LINEITEM = [("l_orderkey", "int8"), ("l_partkey", "int8"), ("l_suppkey", "int8"), ("l_linenumber", "int4"),
            ("l_quantity", "num152"), ("l_extendedprice", "num152"), ("l_discount", "num152"), ("l_tax", "num152"),
            ("l_returnflag", "bpchar"), ("l_linestatus", "bpchar"), ("l_shipdate", "date"),
            ("l_commitdate", "date"), ("l_receiptdate", "date"), ("l_shipinstruct", "bpchar"),
            ("l_shipmode", "bpchar"), ("l_comment", "varchar")]
NARROW = [("id", "int8"), ("v", "int4")]
CYCLE = ["int4", "num152", "text", "date", "timestamptz", "float8", "int8", "bool", "numeric", "interval"]
WIDE = [("c%03d" % i, CYCLE[i % len(CYCLE)]) for i in range(200)]


def make_table(cols):
    out = []
    for i, (n, t) in enumerate(cols):
        if t == "num152":
            out.append(col(i + 1, n, "numeric", numeric_typmod(15, 2)))
        else:
            out.append(col(i + 1, n, t))
    return table(out, key=(1,))


def values(t, n, k):
    """(transport array, text array) of n values of kind t."""
    if t in ("int8", "int4"):
        v = [(i * 7919 + k) % 2_000_000_000 for i in range(n)]
        return pa.array(v, pa.int64() if t == "int8" else pa.int32()), pa.array([str(x) for x in v])
    if t == "bool":
        v = [(i + k) % 2 == 0 for i in range(n)]
        return pa.array(v), pa.array(["t" if x else "f" for x in v])
    if t == "float8":
        v = [((i * 31 + k) % 100000) / 7.0 for i in range(n)]
        return pa.array(v), pa.array([R.float8_text(x) for x in v])
    if t in ("num152", "numeric"):
        s = ["%d.%02d" % ((i * 13 + k) % 10000000, i % 100) for i in range(n)]
        return pa.array(s), pa.array(s)
    if t == "date":
        d = [8000 + (i + k) % 3000 for i in range(n)]
        return pa.array([x - R.PG_EPOCH_DAYS for x in d], pa.int32()), pa.array([R.date_text(x) for x in d])
    if t == "timestamptz":
        u = [1_600_000_000_000_000 + (i * 1_234_567_891 + k) for i in range(n)]
        return pa.array([x - R.PG_EPOCH_US for x in u], pa.int64()), pa.array([R.ts_text(x, True) for x in u])
    if t == "interval":
        iv = [(i % 13, i % 31, (i * 1000003) % 86400000000) for i in range(n)]
        return pa.array([R.interval_transport(*x) for x in iv]), pa.array([R.interval_postgres(*x) for x in iv])
    if t == "bpchar":
        s = ["%-10s" % ("x%d" % ((i + k) % 1000)) for i in range(n)]
        return pa.array(s), pa.array(s)
    s = ["comment %d of row %d" % (k, i) for i in range(n)]
    return pa.array(s), pa.array(s)


def batches(cols, n):
    tr, tx = {"seq": pa.array(range(n), pa.int64()), "op": pa.array(["c"] * n)}, {}
    for i, (_name, t) in enumerate(cols):
        a, b = values(t, n, i)
        tr["a%d" % i] = a
        tx["a%d" % i] = b
    return pa.record_batch(tr), pa.record_batch(tx)


def nbytes(b):
    return sum(buf.size for c in b.columns for buf in c.buffers() if buf is not None)


def measure(tr, plan, batch, min_rows, min_s=0.5):
    fn = tr.stream if "seq" in batch.schema.names else tr.backfill
    fn(plan, batch)                               # compile and warm
    rows, t0 = 0, time.perf_counter()
    while rows < min_rows or time.perf_counter() - t0 < min_s:
        fn(plan, batch)
        rows += batch.num_rows
    dt = time.perf_counter() - t0
    calls = rows // batch.num_rows
    return {"rows_per_s": round(rows / dt), "mb_per_s": round(nbytes(batch) * calls / dt / 1e6, 1),
            "us_per_call": round(dt / calls * 1e6, 1), "calls": calls}


def test_speed():
    tr = T.Transformer(T.TransformSettings(threads=1, memory_limit="2GB"))
    results = {}
    for name, cols in (("lineitem", LINEITEM), ("narrow", NARROW), ("wide200", WIDE)):
        plan = T.plan(make_table(cols), server_version_num=V18)
        for n in (1, 1000, 100000):
            tb, xb = batches(cols, n)
            for direction, b in (("stream", tb), ("backfill", xb)):
                r = measure(tr, plan, b, min_rows=max(n, 3000) if n > 1 else 2000)
                results["%s/%s/%d" % (name, direction, n)] = r
                print(name, direction, n, r)
    from test_transform_crosspath import write
    write("speed.json", {"test": "tests/transform/test_transform_measure.py::test_speed", "threads": 1,
                         "machine": "shared 10-core laptop (other workers may run); indications only",
                         "duckdb": __import__("duckdb").__version__, "pyarrow": pa.__version__, "results": results})


MEM_CHILD = r"""
import json, os, subprocess, sys, threading, time
sys.path.insert(0, sys.argv[1])
import pyarrow as pa
from cdc_v2 import transform as T
from tables import one
kind, direction = sys.argv[2], sys.argv[3]
size = 100 * 1024 * 1024
def rss():
    # the current resident set size (ru_maxrss is a lifetime peak, which the building of the input sets)
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True).stdout
    return int(out.strip()) * 1024
peak = [0]
stop = threading.Event()
def sample():
    while not stop.is_set():
        peak[0] = max(peak[0], rss())
        time.sleep(0.005)
t = T.Transformer(T.TransformSettings(threads=1, memory_limit="2GB"))
p = T.plan(one(kind), server_version_num=180006)
T.Transformer.backfill(t, p, pa.record_batch({"a0": pa.array(["\\x00" if kind == "bytea" else "x"])}))
if kind == "bytea":
    value = b"\xab" * size
    arr = pa.array([value], pa.binary()) if direction == "stream" else pa.array(["\\x" + value.hex()], pa.string())
    del value
else:
    arr = pa.array(["y" * size], pa.string())
batch = pa.record_batch({"a0": arr})
batch_bytes = sum(b.size for b in arr.buffers() if b is not None)
import gc
gc.collect()
time.sleep(0.5)
base = rss()
th = threading.Thread(target=sample, daemon=True)
th.start()
t0 = time.perf_counter()
out = (t.stream if direction == "stream" else t.backfill)(p, batch)
dt = time.perf_counter() - t0
time.sleep(0.05)
stop.set()
th.join()
assert len(out.column(0)[0].as_buffer()) == size
peak = max(peak[0], rss())
print(json.dumps({"kind": kind, "direction": direction, "value_bytes": size, "batch_bytes": batch_bytes,
                  "rss_before_mb": round(base / 1e6), "peak_rss_mb": round(peak / 1e6),
                  "extra_over_batch": round((peak - base) / batch_bytes, 2), "seconds": round(dt, 3)}))
"""


def test_memory_with_a_100mb_value():
    here = os.path.dirname(os.path.abspath(__file__))
    out = []
    for kind, direction in (("bytea", "backfill"), ("bytea", "stream"), ("text", "backfill"), ("text", "stream")):
        p = subprocess.run([sys.executable, "-c", MEM_CHILD, here, kind, direction], capture_output=True, text=True,
                           timeout=600, env=dict(os.environ, PYTHONPATH=os.path.join(here, "..", "..", "src")))
        assert p.returncode == 0, p.stderr[-2000:]
        r = json.loads(p.stdout.strip().splitlines()[-1])
        print(r)
        out.append(r)
    from test_transform_crosspath import write
    write("memory_100mb.json", {"test": "tests/transform/test_transform_measure.py::test_memory_with_a_100mb_value",
                                "measure": "growth of the maximum resident set size during one conversion, as a "
                                           "multiple of the batch's buffer bytes", "results": out})
