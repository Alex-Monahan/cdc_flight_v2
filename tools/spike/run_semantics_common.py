"""Shared helpers of the run-semantics spike drivers (tools/spike/run_semantics_*.py).

Every Flight, secret and database used here is named cdc_v2_spike_runs*; only the database cdc_v2_spike_runs_db is
read or written. Evidence goes to evidence/p2/run_semantics/ as JSON, redacted (no token, no user name, no host
name of ours, no local path).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import statistics
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))
from cdc_v2 import flightctl as fc  # noqa: E402

PREFIX = "cdc_v2_spike_runs"
DB = "cdc_v2_spike_runs_db"
OUT = os.path.join(ROOT, "evidence", "p2", "run_semantics")
SRC_DIR = os.path.join(ROOT, "flights", "spike_runs")
F16_USD_PER_HOUR = 0.60  # documented, tier 1 (us-east-1): https://motherduck.com/docs/about-motherduck/billing/flight-sizes
REQ_DUCKDB = "duckdb==1.5.6\n"

DEFAULT_CONFIG = {
    "CDC_V2_RUNS_MODE": "ticker", "CDC_V2_RUNS_FLIGHT": "?", "CDC_V2_RUNS_DB": DB, "CDC_V2_RUNS_TICK_S": "0.2",
    "CDC_V2_RUNS_DURATION_S": "600", "CDC_V2_RUNS_CHILDREN": "0", "CDC_V2_RUNS_ON_TERM": "ignore",
    "CDC_V2_RUNS_TARGET": "others", "CDC_V2_RUNS_FAIL": "exit3", "CDC_V2_RUNS_STDOUT": "pipe",
    "CDC_V2_RUNS_MAIN_S": "20", "CDC_V2_RUNS_WD_EXIT": "killpg1", "CDC_V2_RUNS_FREEZE": "pipeline",
    "CDC_V2_RUNS_FREEZE_AT_S": "30", "CDC_V2_RUNS_STALE_S": "60", "CDC_V2_RUNS_LEADER_END": "release",
    "CDC_V2_RUNS_LEAD_S": "120", "CDC_V2_RUNS_RELEASE_BEFORE_S": "5", "CDC_V2_RUNS_CHILD_S": "900",
    "CDC_V2_RUNS_CONT_AFTER_S": "100", "CDC_V2_RUNS_EXIT_CODE": "0", "CDC_V2_RUNS_CONFIG_TAG": "c1",
    "CDC_V2_RUNS_GRANDCHILD": "1", "CDC_V2_RUNS_TICK_VIA": "duckdb", "CDC_V2_RUNS_REGION": "us-east-1",
}

ctl = fc.FlightCtl()
_db = None
_HOME = os.path.expanduser("~")
_JWT = re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}")


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(ts: float | None) -> str | None:
    return None if ts is None else dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat()


def unix(value) -> float | None:
    t = fc.parse_time(value)
    return t.timestamp() if t else None


def db():
    """Connection to the spike database only (attach_mode=single: no other database is attached)."""
    global _db
    if _db is None:
        import duckdb
        _db = duckdb.connect("md:%s?attach_mode=single" % DB)
        _db.execute("SET TimeZone = 'UTC'")
    return _db


def setup_db() -> None:
    import duckdb
    con = duckdb.connect("md:")
    con.execute("CREATE DATABASE IF NOT EXISTS %s" % DB)
    con.close()
    c = db()
    c.execute("CREATE TABLE IF NOT EXISTS ticks (flight VARCHAR, run_number BIGINT, role VARCHAR, pid INTEGER, "
              "seq BIGINT, client_unix DOUBLE, server_ts TIMESTAMPTZ, tag VARCHAR)")
    c.execute("CREATE TABLE IF NOT EXISTS events (flight VARCHAR, run_number BIGINT, role VARCHAR, pid INTEGER, "
              "kind VARCHAR, client_unix DOUBLE, server_ts TIMESTAMPTZ, detail VARCHAR)")
    c.execute("CREATE TABLE IF NOT EXISTS leader (id INTEGER PRIMARY KEY, run_number BIGINT, epoch BIGINT, "
              "heartbeat_at TIMESTAMPTZ, state VARCHAR)")
    c.execute("CREATE TABLE IF NOT EXISTS guard_leader (id INTEGER PRIMARY KEY, run_number BIGINT, epoch BIGINT, "
              "heartbeat_at TIMESTAMPTZ, state VARCHAR)")


def redact(obj):
    if isinstance(obj, dict):
        return {k: redact(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [redact(v) for v in obj]
    if isinstance(obj, str):
        s = obj.replace(_HOME, "~")
        return _JWT.sub("<redacted-jwt>", s)
    return obj


def save(name: str, obj) -> str:
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name + ".json")
    with open(path, "w") as f:
        json.dump(redact(obj), f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    print("wrote", os.path.relpath(path, ROOT), flush=True)
    return path


def read_src(name: str = "main.py") -> str:
    with open(os.path.join(SRC_DIR, name)) as f:
        return f.read()


def deploy(name: str, *, source: str = "main.py", requirements: str = REQ_DUCKDB, config: dict | None = None,
           max_runtime_sec: int | None = None, schedule: str | None = None, source_text: str | None = None):
    assert name.startswith(PREFIX)
    cfg = dict(DEFAULT_CONFIG)
    cfg["CDC_V2_RUNS_FLIGHT"] = name
    cfg.update(config or {})
    kwargs = {"requirements_txt": requirements, "instance_type": "F16", "config": cfg}
    if max_runtime_sec is not None:
        kwargs["max_runtime_sec"] = max_runtime_sec
    if schedule is not None:
        kwargs["schedule_cron"] = schedule
    action, summary = ctl.deploy(name, source_text if source_text is not None else read_src(source), **kwargs)
    print("deploy", name, action, "version", summary.get("current_version"), flush=True)
    return summary


def run(name: str, **config) -> dict:
    r = ctl.run(name, {k: str(v) for k, v in config.items()} or None)
    r["local_requested_at"] = now().isoformat()
    return r


def wait_state(name: str, n: int, states=("RUNNING",), timeout=180, poll=0.5) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        r = ctl.get_run(name, n)
        if r["state"] in states or fc.run_is_terminal(r):
            return r
        if time.monotonic() > deadline:
            raise RuntimeError("run %s of %s not in %s after %ss" % (n, name, states, timeout))
        time.sleep(poll)


def parse_logs(lines: list[dict]) -> dict:
    out = {"first_line_unix": None, "events": [], "guard": None, "lines": len(lines)}
    for ln in lines:
        text = ln.get("line") or ""
        if text.startswith("FIRST ") and out["first_line_unix"] is None:
            try:
                out["first_line_unix"] = float(text.split()[1])
            except (IndexError, ValueError):
                pass
        elif text.startswith("EV "):
            try:
                out["events"].append(json.loads(text[3:]))
            except ValueError:
                pass
        elif text.startswith("GUARD "):
            try:
                out["guard"] = json.loads(text[6:])
            except ValueError:
                pass
    return out


def logs(name: str, n: int) -> list[dict]:
    return ctl.logs(name, n)


def tick_summary(flight: str, run_number: int) -> list[dict]:
    rows = db().execute(
        "SELECT role, pid, count(*), min(epoch_ms(server_ts)) / 1000.0, max(epoch_ms(server_ts)) / 1000.0, "
        "max(client_unix) FROM ticks WHERE flight = ? AND run_number = ? GROUP BY ALL ORDER BY 4",
        [flight, run_number]).fetchall()
    return [{"role": r[0], "pid": r[1], "rows": r[2], "first_server": iso(r[3]), "last_server": iso(r[4]),
             "first_server_unix": r[3], "last_server_unix": r[4], "last_client_unix": r[5]} for r in rows]


def event_rows(flight: str, run_number: int | None = None) -> list[dict]:
    sql = ("SELECT run_number, role, pid, kind, client_unix, epoch_ms(server_ts) / 1000.0, detail FROM events "
           "WHERE flight = ?")
    params = [flight]
    if run_number is not None:
        sql += " AND run_number = ?"
        params.append(run_number)
    rows = db().execute(sql + " ORDER BY server_ts", params).fetchall()
    return [{"run_number": r[0], "role": r[1], "pid": r[2], "kind": r[3], "client_unix": r[4],
             "server_unix": r[5], "server": iso(r[5]), "detail": json.loads(r[6]) if r[6] else None} for r in rows]


def dist(values) -> dict:
    v = sorted(x for x in values if x is not None)
    if not v:
        return {"n": 0}
    p95 = v[min(len(v) - 1, int(round(0.95 * (len(v) - 1))))]
    return {"n": len(v), "min": round(v[0], 3), "median": round(statistics.median(v), 3), "p95": round(p95, 3),
            "max": round(v[-1], 3), "mean": round(statistics.fmean(v), 3)}


def run_timing(run: dict, first_line_unix: float | None = None) -> dict:
    t = {}
    sch, cre, sta, end, can = (unix(run.get(k)) for k in ("scheduled_at", "created_at", "started_at", "ended_at",
                                                           "cancelled_at"))
    if sch and sta:
        t["scheduled_to_started_s"] = round(sta - sch, 3)
    if cre and sch:
        t["created_minus_scheduled_s"] = round(cre - sch, 3)
    if sta and end:
        t["started_to_ended_s"] = round(end - sta, 3)
    if sta and can:
        t["started_to_cancelled_s"] = round(can - sta, 3)
    if first_line_unix:
        if sta:
            t["started_to_first_line_s"] = round(first_line_unix - sta, 3)
        if sch:
            t["scheduled_to_first_line_s"] = round(first_line_unix - sch, 3)
        if end:
            t["first_line_to_ended_s"] = round(end - first_line_unix, 3)
    return t


def slim_run(r: dict) -> dict:
    keep = ("run_number", "state", "is_scheduled", "flight_version", "scheduled_at", "created_at", "started_at",
            "ended_at", "cancelled_at", "exit_code")
    return {k: r.get(k) for k in keep}


def cleanup_flight(name: str) -> dict:
    out = {"flight": name}
    f = ctl.find_flight(name)
    if not f:
        out["existed"] = False
        return out
    try:
        ctl.unschedule(name)
    except fc.FlightCtlError as exc:
        out["unschedule_error"] = str(exc)[:200]
    cancelled = []
    for r in ctl.active_runs(f["flight_id"]):
        try:
            ctl.cancel_by_id(f["flight_id"], r["run_number"])
            cancelled.append(r["run_number"])
        except fc.FlightCtlError as exc:
            cancelled.append({"run_number": r["run_number"], "error": str(exc)[:200]})
    out["cancelled"] = cancelled
    out["deleted"] = ctl.delete(name)
    return out
