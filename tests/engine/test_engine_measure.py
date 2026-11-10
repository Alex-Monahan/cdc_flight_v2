"""Throughput indications at lineitem width (opt-in: CDC_V2_E_MEASURE=1; markers jvm, localpg).

A backlog is written to the slot first; then the engine starts and a reader that only decodes frames drains it.
The same backlog is drained by the spike program (jvm/src/cdc_v2/engine/EngineMain.java, struct format, no
acknowledgement wait). Rates are rows per second from the first frame to the last END; CPU from `ps` (whole JVM)
and from the engine's EXIT line (per thread kind, our handler). Indications only: shared development machine,
source on the same host.
"""
import json
import os
import re
import struct
import subprocess
import tempfile
import time

import pytest

from cdc_v2.deadline import Deadline
from cdc_v2.stream import frames as F
from cdc_v2.stream.engine import Engine, build_properties
from stream_helpers import evidence

pytestmark = [pytest.mark.jvm, pytest.mark.localpg, pytest.mark.timeout(7200),
              pytest.mark.skipif(os.environ.get("CDC_V2_E_MEASURE") != "1", reason="opt-in: CDC_V2_E_MEASURE=1")]

LINEITEM = """create table {s}.lineitem (
  l_orderkey bigint not null, l_partkey bigint not null, l_suppkey bigint not null, l_linenumber integer not null,
  l_quantity numeric(15,2) not null, l_extendedprice numeric(15,2) not null, l_discount numeric(15,2) not null,
  l_tax numeric(15,2) not null, l_returnflag char(1) not null, l_linestatus char(1) not null,
  l_shipdate date not null, l_commitdate date not null, l_receiptdate date not null,
  l_shipinstruct char(25) not null, l_shipmode char(10) not null, l_comment varchar(44) not null,
  primary key (l_orderkey, l_linenumber))"""

ROWS_SQL = """insert into {s}.lineitem select g / 4 + {base}, g % 200000, g % 10000, g % 4 + 1, (g % 50) + 1,
  ((g % 50) + 1) * 901.25, (g % 11) / 100.0, (g % 9) / 100.0, 'N', 'O',
  date '1992-01-01' + g % 2500, date '1992-01-31' + g % 2500, date '1992-01-15' + g % 2500,
  'DELIVER IN PERSON', 'TRUCK', md5(g::text)
  from generate_series({lo}, {hi}) g"""


def cputime_s(pid):
    out = subprocess.run(["ps", "-o", "cputime=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    if not out:
        return None
    parts = [float(x) for x in re.split("[:]", out.replace("-", ":"))]
    s = 0.0
    for p in parts:
        s = s * 60 + p
    return s


def write_backlog(pair, schema, txn_rows, total):
    base = int(time.time() * 1000) % 1_000_000_000 * 10
    with pair.connect(pair.primary_port) as c:
        sql = ROWS_SQL
        if txn_rows == 1:
            with c.cursor() as cur:
                for i in range(total):
                    cur.execute(sql.format(s=schema, base=base, lo=i, hi=i))
        else:
            for lo in range(0, total, txn_rows):
                c.execute(sql.format(s=schema, base=base, lo=lo, hi=min(total, lo + txn_rows) - 1))
    pair.wait_replayed(120)


def drain_engine(pair, sc, artifacts, total, **kw):
    """A reader that discards: frame headers and the row count of ROWS frames only (no checksum, no Arrow), so the
    rate is the engine's, not the reader's. Correctness of the same stream is covered by the other suites."""
    eng = Engine(sc.settings(**kw), None, artifacts)
    try:
        f = os.fdopen(os.dup(eng._frames_fd), "rb", buffering=1 << 20)
        os.set_blocking(f.fileno(), True)
        rows, t_first = 0, None
        while rows < total:
            hdr = f.read(20)
            assert len(hdr) == 20, "engine ended"
            ftype, length = hdr[4], struct.unpack_from("<I", hdr, 16)[0]
            payload = f.read(length + 4)
            if ftype == F.HELLO:
                continue
            if t_first is None:
                t_first = time.monotonic()
            if ftype == F.ROWS:
                n1 = struct.unpack_from("<I", payload, 0)[0]
                n2 = struct.unpack_from("<I", payload, 4 + n1)[0]
                rows += struct.unpack_from("<I", payload, 8 + n1 + n2 + 4)[0]
            elif ftype == F.FATAL:
                raise AssertionError("FATAL")
        t_last = time.monotonic()
        cpu = cputime_s(eng.pid)
        ws_pid = pair.slot(sc.slot)[2]
        ws_cpu = cputime_s(ws_pid) if ws_pid else None
        f.close()
        eng.stop(Deadline.after(30))
        stop_line = next((l for l in reversed(eng.log_lines) if "CDCV2 STOP" in l), "")
        fields = dict(re.findall(r"(\w+)=(-?\d+)", stop_line))
    finally:
        eng.kill()
    secs = t_last - t_first
    return {"program": "engine", "rows": rows, "seconds": round(secs, 2), "rows_per_s": round(rows / secs),
            "jvm_cpu_s": cpu, "jvm_cpu_us_per_row": round(cpu * 1e6 / rows, 1) if cpu else None,
            "walsender_cpu_s": ws_cpu,
            "threads_cpu_ms": {k: int(v) for k, v in fields.items() if k.startswith("cpu_") or k in (
                "gc_ms", "handler_cpu_ms", "handler_write_ms", "process_cpu_ms")}}


def drain_spike(pair, sc, artifacts, total, props_extra=None):
    """The P2 spike program, struct format, acknowledgement off."""
    p = build_properties(sc.settings())
    props = {k: v for k, v in p.items() if not k.startswith("cdc_v2.") and k not in ("converters", "cdcv2.type")}
    props.update({"cdc_v2.format": "struct", "cdc_v2.ack": "none", "max.batch.size": "8192",
                  "max.queue.size": "32768"})
    props.update(props_extra or {})
    d = tempfile.mkdtemp(prefix="cdc_v2_e_spike_")
    path = os.path.join(d, "engine.properties")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)
    with os.fdopen(fd, "w") as f:
        for k, v in props.items():
            f.write("%s=%s\n" % (k, v.replace("\\", "\\\\").replace(":", "\\:").replace("=", "\\=")))
    r, w = os.pipe()
    proc = subprocess.Popen([artifacts.java, "-Xmx512m", "-XX:+UseParallelGC", "-Dcdc_v2.frames=/dev/fd/%d" % w,
                             "-cp", artifacts.classpath, "cdc_v2.engine.EngineMain", path],
                            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, pass_fds=(w,))
    os.close(w)
    try:
        f = os.fdopen(r, "rb", buffering=1 << 20)
        events, t_first = 0, None
        while events < total:
            hdr = f.read(20)
            assert len(hdr) == 20
            magic, seq, n, length = struct.unpack(">iqii", hdr)
            f.read(length)
            if n and t_first is None:
                t_first = time.monotonic()
            events += n
        t_last = time.monotonic()
        cpu = cputime_s(proc.pid)
    finally:
        os.remove(path)
        os.rmdir(d)
        proc.kill()
        proc.wait(30)
    secs = t_last - t_first
    return {"program": "spike EngineMain (struct, ack none)", "rows": events, "seconds": round(secs, 2),
            "rows_per_s": round(events / secs), "jvm_cpu_s": cpu,
            "jvm_cpu_us_per_row": round(cpu * 1e6 / events, 1) if cpu else None}


def test_measure_throughput(pair, scenario, artifacts):
    """Our engine never acknowledges here (no DURABLE), so every run drains the same backlog from the slot; the
    spike program acknowledges, so it runs last for each backlog."""
    results = []
    variants = [("default: batch 8192, queue 32768, 1 processing thread", {}),
                ("batch 2048, queue 8192", {"max_batch_size": 2048, "max_queue_size": 8192}),
                ("batch 32768, queue 131072", {"max_batch_size": 32768, "max_queue_size": 131072}),
                ("record.processing.threads 2", {"record_processing_threads": 2})]
    for txn_rows, total in [(1, 20_000), (1000, 300_000), (1_000_000, 1_000_000)]:
        sc = scenario(LINEITEM)
        t0 = time.monotonic()
        write_backlog(pair, sc.schema, txn_rows, total)
        load_s = time.monotonic() - t0
        runs = variants if txn_rows == 1_000_000 else variants[:1]
        for name, kw in runs:
            r = drain_engine(pair, sc, artifacts, total, **kw)
            r.update(variant=name, rows_per_txn=txn_rows, load_s=round(load_s, 1))
            results.append(r)
            print(json.dumps(r))
        r = drain_spike(pair, sc, artifacts, total)
        r.update(rows_per_txn=txn_rows)
        results.append(r)
        print(json.dumps(r))
        sc.close()
    evidence("throughput.json", {"test": "CDC_V2_E_MEASURE=1 uv run pytest -m 'jvm and localpg' "
                                         "tests/engine/test_engine_measure.py -s",
                                 "note": "indications only: shared 10-core laptop, source, walsender, JVM and reader "
                                         "on the same host; load average recorded below",
                                 "load_average": os.getloadavg(), "heap_mb": 512, "gc": "parallel",
                                 "cases": results})
