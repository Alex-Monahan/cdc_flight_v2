"""Shared helpers for the local read-throughput spike (PLAN §2 A 5, part 1).

Everything here talks to the spike's private cluster only (home ~/.cache/cdc_flight_v2/pg-spike-tp, primary 55500,
replica 55501), through the DSNs that `tools/localpg conninfo --json` prints (passwords stay in the cluster's pgpass
file; nothing secret passes through this code). MotherDuck: only the database cdc_v2_spike_tp.
"""
from __future__ import annotations

import json
import os
import re
import resource
import socket
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
LOCALPG = HERE / "localpg"
PRIMARY_PORT = 55500
REPLICA_PORT = 55501
ALLOWED_PORTS = range(55500, 55510)
TABLE = "public.lineitem"
MD_DB = "cdc_v2_spike_tp"
EVIDENCE = Path(os.environ.get("CDC_V2_SPIKE_EVIDENCE", REPO / "evidence" / "p2" / "local_throughput"))
WORK = Path(os.environ.get("CDC_V2_SPIKE_TP_WORK", Path.home() / ".cache" / "cdc_flight_v2" / "spike_tp"))

COLUMNS = ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount",
           "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct",
           "l_shipmode", "l_comment"]

PG_DDL = """CREATE TABLE public.lineitem (
    l_orderkey bigint NOT NULL, l_partkey bigint NOT NULL, l_suppkey bigint NOT NULL, l_linenumber integer NOT NULL,
    l_quantity numeric(15,2) NOT NULL, l_extendedprice numeric(15,2) NOT NULL, l_discount numeric(15,2) NOT NULL,
    l_tax numeric(15,2) NOT NULL, l_returnflag char(1) NOT NULL, l_linestatus char(1) NOT NULL,
    l_shipdate date NOT NULL, l_commitdate date NOT NULL, l_receiptdate date NOT NULL,
    l_shipinstruct char(25) NOT NULL, l_shipmode char(10) NOT NULL, l_comment varchar(44) NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber))"""

MD_DDL = """(
    l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INTEGER,
    l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2),
    l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE,
    l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR)"""


# ---------------------------------------------------------------- Postgres connection info

_CONNINFO: dict | None = None


def conninfo() -> dict:
    global _CONNINFO
    if _CONNINFO is None:
        out = subprocess.run([str(LOCALPG), "conninfo", "--json"], check=True, capture_output=True, text=True,
                             timeout=60).stdout
        _CONNINFO = json.loads(out)
    return _CONNINFO


def dsn(node: str = "replica", role: str = "runtime") -> str:
    d = conninfo()[node][f"dsn_{role}"]
    port = int(re.search(r"port=(\d+)", d).group(1))
    if port not in ALLOWED_PORTS:  # never the shared cluster, never 5432
        raise SystemExit(f"refusing port {port}")
    return d


def pg_connect(node: str = "replica", role: str = "runtime", autocommit: bool = True, **kw):
    import psycopg
    return psycopg.connect(dsn(node, role), autocommit=autocommit, **kw)


def postmaster_pid(node: str = "replica") -> int:
    with pg_connect(node, "admin") as c:
        bpid = c.execute("select pg_backend_pid()").fetchone()[0]
    import psutil
    return psutil.Process(bpid).ppid()


# ---------------------------------------------------------------- measurement helpers

def load_avg() -> list[float]:
    return [round(x, 2) for x in os.getloadavg()]


def self_cpu() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


def self_maxrss_mb() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / (1024 * 1024) if sys.platform == "darwin" else r / 1024


class BackendSampler:
    """Samples the CPU time of the replica's client backends of one role (by process title) every `interval` s.

    Backends disappear when their connection closes; the last sample of each pid is kept, so up to `interval`
    seconds of CPU per backend can be missed. Also records the recovery (startup) process for replay effects.
    """

    def __init__(self, node: str = "replica", match: str = "cdc_v2_runtime", interval: float = 0.2):
        import psutil
        self.psutil = psutil
        self.pm = psutil.Process(postmaster_pid(node))
        self.match = match
        self.interval = interval
        self.base: dict[int, float] = {}
        self.last: dict[int, float] = {}
        self.startup0 = self.startup1 = None
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    def _procs(self):
        out = []
        for p in self.pm.children():
            try:
                out.append((p, " ".join(p.cmdline())))
            except Exception:
                pass
        return out

    def _startup_cpu(self):
        for p, cmd in self._procs():
            if "startup" in cmd:
                t = p.cpu_times()
                return t.user + t.system
        return None

    def _sample(self):
        for p, cmd in self._procs():
            if self.match in cmd:
                try:
                    t = p.cpu_times()
                    self.last[p.pid] = t.user + t.system
                except Exception:
                    pass

    def start(self):
        self._sample()
        self.base = dict(self.last)
        self.startup0 = self._startup_cpu()
        self._t.start()
        return self

    def _run(self):
        while not self._stop.wait(self.interval):
            self._sample()

    def stop(self) -> dict:
        self._sample()
        self._stop.set()
        self._t.join()
        self.startup1 = self._startup_cpu()
        cpu = sum(v - self.base.get(pid, 0.0) for pid, v in self.last.items())
        return {"backend_cpu_s": round(cpu, 3), "backends_seen": len(set(self.last) - set()),
                "startup_cpu_s": (round(self.startup1 - self.startup0, 3)
                                  if self.startup0 is not None and self.startup1 is not None else None)}


# ---------------------------------------------------------------- redaction and evidence

_SECRETS: list[str] = []
for _v in (os.environ.get("motherduck_token"), os.environ.get("MOTHERDUCK_TOKEN")):
    if _v and len(_v) > 8:
        _SECRETS.append(_v)
for _v in (str(Path.home()), os.environ.get("USER"), os.environ.get("LOGNAME"), socket.gethostname(),
           socket.gethostname().split(".")[0]):
    if _v and len(_v) >= 3:
        _SECRETS.append(_v)
_RX = [re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
       re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")]


def redact(s: object) -> str:
    s = str(s)
    home = str(Path.home())
    s = s.replace(home, "~")
    for v in sorted(_SECRETS, key=len, reverse=True):
        if v != home:
            s = re.sub(rf"(?<![A-Za-z0-9_]){re.escape(v)}(?![A-Za-z0-9_])", "<redacted>", s)
    for rx in _RX:
        s = rx.sub("<redacted>", s)
    return s


def write_json(name: str, data) -> Path:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    p = EVIDENCE / name
    p.write_text(redact(json.dumps(data, indent=1, default=str)) + "\n")
    return p


def append_jsonl(name: str, rec: dict) -> None:
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    with open(EVIDENCE / name, "a") as f:
        f.write(redact(json.dumps(rec, default=str)) + "\n")


def summarize(values: list[float]) -> dict:
    values = [v for v in values if v is not None]
    if not values:
        return {"n": 0}
    med = statistics.median(values)
    return {"n": len(values), "median": round(med, 3), "min": round(min(values), 3), "max": round(max(values), 3),
            "spread_pct": round(100.0 * (max(values) - min(values)) / med, 1) if med else None}


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def wait_replica_caught_up(timeout: float = 600.0) -> float:
    """Wait until the replica has replayed everything the primary has written; returns seconds waited."""
    t0 = time.time()
    with pg_connect("primary", "admin") as p, pg_connect("replica", "admin") as r:
        target = p.execute("select pg_current_wal_lsn()").fetchone()[0]
        while True:
            done = r.execute("select pg_last_wal_replay_lsn() >= %s::pg_lsn", (target,)).fetchone()[0]
            if done:
                return time.time() - t0
            if time.time() - t0 > timeout:
                raise TimeoutError("replica did not catch up")
            time.sleep(0.5)


def table_rows() -> int:
    """Row count of public.lineitem as recorded by setup_data.py (the table is static during read runs)."""
    return int(json.loads((EVIDENCE / "setup.json").read_text())["replica"]["rows"])


def table_heap_bytes() -> int:
    return int(json.loads((EVIDENCE / "setup.json").read_text())["replica"]["heap_bytes"])


def md_config(memory_limit: str = "4GB") -> dict:
    WORK.mkdir(parents=True, exist_ok=True)
    return {"memory_limit": memory_limit, "temp_directory": str(WORK / "duckdb_tmp")}
