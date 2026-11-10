"""Shared helpers for the MotherDuck semantics spike (PLAN P2, §2 A item 3).

Every database touched here is named cdc_v2_spike_md_*. Connections to an existing database use single attach mode,
so no other database of the account is attached to the session. Only create_db()/drop_db() open a workspace-mode
connection, and they run exactly one CREATE/DROP statement on it; nothing ever lists the account's databases.

Environment:
  motherduck_token     MotherDuck token (provided by tools/with-env locally, by a Flight secret on a Flight)
  CDC_V2_SPIKE_TMP     temporary directory (default ~/.cache/cdc_flight_v2/spike_md, or $TMPDIR/... on a Flight)
  CDC_V2_SPIKE_EVIDENCE  directory for evidence files (default evidence/p2/md_semantics next to the repo root)
  CDC_V2_MEMORY_LIMIT  local DuckDB memory limit (default 4GB)
"""
from __future__ import annotations

import json
import os
import re
import socket
import statistics
import sys
import time
from pathlib import Path

import duckdb

PREFIX = "cdc_v2_spike_md_"
REPO = Path(__file__).resolve().parents[2]


def tmp_dir() -> Path:
    d = os.environ.get("CDC_V2_SPIKE_TMP")
    if d:
        p = Path(d)
    elif (Path.home() / ".cache").exists():
        p = Path.home() / ".cache" / "cdc_flight_v2" / "spike_md"
    else:
        import tempfile
        p = Path(tempfile.gettempdir()) / "cdc_v2_spike_md"
    p.mkdir(parents=True, exist_ok=True)
    return p


def evidence_dir() -> Path:
    p = Path(os.environ.get("CDC_V2_SPIKE_EVIDENCE", REPO / "evidence" / "p2" / "md_semantics"))
    p.mkdir(parents=True, exist_ok=True)
    return p


def _config() -> dict:
    return {
        "memory_limit": os.environ.get("CDC_V2_MEMORY_LIMIT", "4GB"),
        "temp_directory": str(tmp_dir() / "duckdb_tmp"),
    }


def check_name(db: str) -> str:
    if not re.fullmatch(PREFIX + r"[a-z0-9_]+", db):
        raise ValueError("refusing database name outside the project prefix")
    return db


def connect(db: str, **extra) -> duckdb.DuckDBPyConnection:
    """A connection whose session has exactly one database attached: db (single attach mode)."""
    check_name(db)
    params = "attach_mode=single"
    for k, v in extra.items():
        params += f"&{k}={v}"
    return duckdb.connect(f"md:{db}?{params}", config=_config())


def create_db(db: str, replace: bool = True) -> None:
    check_name(db)
    con = duckdb.connect("md:", config=_config())
    try:
        if replace:
            con.execute(f"DROP DATABASE IF EXISTS {db}")
        con.execute(f"CREATE DATABASE {db}")
    finally:
        con.close()


def drop_db(db: str) -> None:
    check_name(db)
    con = duckdb.connect("md:", config=_config())
    try:
        con.execute(f"DROP DATABASE IF EXISTS {db}")
    finally:
        con.close()


# ---------------------------------------------------------------- redaction

_SECRETS: list[str] = []


def _init_redaction() -> None:
    for name in ("motherduck_token", "MOTHERDUCK_TOKEN"):
        v = os.environ.get(name)
        if v and len(v) > 8:
            _SECRETS.append(v)
    for v in (os.environ.get("USER"), os.environ.get("LOGNAME"), socket.gethostname(),
              socket.gethostname().split(".")[0], str(Path.home())):
        if v and len(v) >= 3:
            _SECRETS.append(v)


_init_redaction()
_RX = [
    re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
    re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    re.compile(r"(?i)\b[a-z0-9-]+(\.[a-z0-9-]+)*\.(internal|local|lan|compute|amazonaws\.com)\b"),
]


def redact(s: object) -> str:
    s = str(s)
    for v in sorted(_SECRETS, key=len, reverse=True):
        s = s.replace(v, "<redacted>")
    for rx in _RX:
        s = rx.sub("<redacted>", s)
    return s


# ---------------------------------------------------------------- evidence

class Evidence:
    """Collects lines and a JSON summary; writes <name>.txt and <name>.json into the evidence directory."""

    def __init__(self, name: str):
        self.name = name
        self.lines: list[str] = []
        self.data: dict = {"spike": name, "duckdb_client": duckdb.__version__,
                           "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                           "note": "measured from a laptop over the public internet; performance numbers are "
                                   "indications only"}

    def log(self, *parts) -> None:
        line = redact(" ".join(str(p) for p in parts))
        print(line, flush=True)
        self.lines.append(line)

    def put(self, key: str, value) -> None:
        self.data[key] = value

    def write(self) -> Path:
        self.data["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        d = evidence_dir()
        (d / f"{self.name}.txt").write_text(redact("\n".join(self.lines) + "\n"))
        (d / f"{self.name}.json").write_text(redact(json.dumps(self.data, indent=2, default=str)) + "\n")
        return d / f"{self.name}.txt"


def pct(values, p: float):
    if not values:
        return None
    v = sorted(values)
    k = min(len(v) - 1, max(0, int(round(p / 100.0 * (len(v) - 1)))))
    return v[k]


def dist(values) -> dict:
    if not values:
        return {"n": 0}
    return {"n": len(values), "min": min(values), "p50": pct(values, 50), "p90": pct(values, 90),
            "p99": pct(values, 99), "max": max(values), "mean": statistics.fmean(values)}


def err_class(e: BaseException) -> str:
    lines = redact(str(e)).splitlines() or [""]
    return f"{type(e).__name__}: {lines[0][:300]}"


def changed(cur) -> int:
    """Row count returned by DuckDB for INSERT/UPDATE/DELETE."""
    r = cur.fetchone()
    return int(r[0]) if r else 0


LINEITEM_DDL = """(
    l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INTEGER,
    l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2),
    l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE,
    l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR)"""


def lineitem_select(start: int, count: int, src: str = "range") -> str:
    """A generator query producing `count` lineitem-width rows with keys start..start+count-1 (l_orderkey unique)."""
    return f"""SELECT
        i AS l_orderkey, (i * 7) % 2000000 AS l_partkey, (i * 13) % 100000 AS l_suppkey, (i % 7 + 1)::INTEGER AS l_linenumber,
        ((i % 50) + 1)::DECIMAL(15,2) AS l_quantity, ((i % 100000) * 1.37)::DECIMAL(15,2) AS l_extendedprice,
        ((i % 11) / 100.0)::DECIMAL(15,2) AS l_discount, ((i % 9) / 100.0)::DECIMAL(15,2) AS l_tax,
        ['A','N','R'][(i % 3) + 1] AS l_returnflag, ['F','O'][(i % 2) + 1] AS l_linestatus,
        DATE '1992-01-01' + (i % 2500)::INTEGER AS l_shipdate, DATE '1992-01-31' + (i % 2500)::INTEGER AS l_commitdate,
        DATE '1992-01-10' + (i % 2500)::INTEGER AS l_receiptdate,
        ['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'][(i % 4) + 1] AS l_shipinstruct,
        ['TRUCK','MAIL','REG AIR','AIR','FOB','RAIL','SHIP'][(i % 7) + 1] AS l_shipmode,
        'carefully final packages ' || md5(i::VARCHAR)[:16] AS l_comment
    FROM (SELECT range AS i FROM range({start}, {start + count})) g"""


if __name__ == "__main__":
    print("helpers only", file=sys.stderr)
