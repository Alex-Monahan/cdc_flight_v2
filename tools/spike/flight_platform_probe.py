#!/usr/bin/env python3
"""P2 platform spike driver: deploys cdc_v2_spike_hello (F16) with flights/spike_hello and measures the platform.

    tools/with-env uv run python tools/spike/flight_platform_probe.py [step ...]

Steps (default: all, in this order): secret deploy facts net exception exit3 ticks cancel noreq schedule limits
cleanup. Evidence goes to evidence/p2/flight_platform/ as JSON, redacted: no token, no user name, no host name, no
database name outside cdc_v2_*. The secret value and the HMAC nonce are random per invocation, live only in this
process and are never written; only the comparison results are.
"""
import datetime as dt
import hashlib
import hmac
import json
import os
import secrets
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))
from cdc_v2 import flightctl as fc  # noqa: E402

NAME = "cdc_v2_spike_hello"
SIZE_NAME = "cdc_v2_spike_size"
SECRET = "cdc_v2_spike_secret"  # a secret NAME, not a value. secret-scan: allow
SRC = os.path.join(ROOT, "flights", "spike_hello", "main.py")
REQ = os.path.join(ROOT, "flights", "spike_hello", "requirements.txt")
OUT = os.path.join(ROOT, "evidence", "p2", "flight_platform")
F16_USD_PER_HOUR = 0.60  # documented, Tier 1 (us-east-1)

NONCE = secrets.token_hex(16)
SECRET_VALUE = secrets.token_hex(16)
ctl = fc.FlightCtl()


def now():
    return dt.datetime.now(dt.timezone.utc)


def keyed(value):
    return hmac.new(NONCE.encode(), value.encode(), hashlib.sha256).hexdigest()[:16]


def redact(obj):
    """Drop the nonce and HMAC values from anything written to evidence."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "CDC_V2_PROBE_NONCE":
                out[k] = "<redacted>"
            elif k.endswith("_hmac"):
                out[k] = None if v is None else "<present>"
            else:
                out[k] = redact(v)
        return out
    if isinstance(obj, list):
        return [redact(v) for v in obj]
    if isinstance(obj, str):
        return obj.replace(NONCE, "<redacted>").replace(SECRET_VALUE, "<redacted>")
    return obj


def save(name, obj):
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name + ".json")
    with open(path, "w") as f:
        json.dump(redact(obj), f, indent=2, sort_keys=True)
        f.write("\n")
    print("wrote", os.path.relpath(path, ROOT), flush=True)


def facts_from_logs(lines):
    facts = {}
    for ln in lines:
        text = ln.get("line") or ""
        if text.startswith("FACT "):
            try:
                facts.update(json.loads(text[5:]))
            except ValueError:
                pass
    return facts


def timing(run, facts):
    t = {k: run.get(k) for k in ("created_at", "scheduled_at", "started_at", "ended_at", "cancelled_at")}
    c, s, e = (fc.parse_time(run.get(k)) for k in ("created_at", "started_at", "ended_at"))
    first = facts.get("first_line_unix")
    if c and s:
        t["created_to_started_s"] = round((s - c).total_seconds(), 3)
    if s and e:
        t["started_to_ended_s"] = round((e - s).total_seconds(), 3)
        t["f16_cost_usd_if_billed_started_to_ended"] = round((e - s).total_seconds() / 3600 * F16_USD_PER_HOUR, 6)
    if c and e:
        t["created_to_ended_s"] = round((e - c).total_seconds(), 3)
    if first:
        f = dt.datetime.fromtimestamp(first, dt.timezone.utc)
        if s:
            t["started_to_first_line_s"] = round((f - s).total_seconds(), 3)
        if c:
            t["created_to_first_line_s"] = round((f - c).total_seconds(), 3)
        if e:
            t["first_line_to_ended_s"] = round((e - f).total_seconds(), 3)
    return t


def run_and_collect(label, config=None, timeout=600, poll=0.5):
    requested = now()
    run = ctl.run(NAME, config)
    returned = now()
    final = ctl.wait(NAME, run["run_number"], timeout=timeout, poll=poll)
    lines = ctl.logs(NAME, run["run_number"])
    facts = facts_from_logs(lines)
    rec = {"label": label, "per_run_config_override": sorted(config) if config else None,
           "local_requested_at": requested.isoformat(), "local_run_call_returned_at": returned.isoformat(),
           "md_run_flight_returned": run, "final": final, "timing": timing(final, facts),
           "log_line_count": len(lines), "logs": lines, "facts": facts}
    save("run_%s" % label, rec)
    return rec


def step_secret():
    out = ctl.create_secret(SECRET, {"CDC_V2_PROBE_SECRET": SECRET_VALUE})
    save("secret_create", {"result": out, "listed": ctl.list_secrets()})


def step_deploy():
    with open(SRC) as f:
        src = f.read()
    with open(REQ) as f:
        req = f.read()
    action, summary = ctl.deploy(NAME, src, requirements_txt=req, instance_type="F16",
                                 secret_names=[SECRET],
                                 config={"CDC_V2_PROBE_MODE": "facts", "CDC_V2_PROBE_FLIGHT_NAME": NAME,
                                         "CDC_V2_PROBE_TICKS": "20", "CDC_V2_PROBE_NONCE": NONCE})
    save("deploy", {"action": action, "summary": summary, "versions": ctl.versions(NAME, 5),
                    "source_bytes": len(src.encode()), "requirements": req})


def step_facts():
    rec = run_and_collect("facts")
    con = ctl.con
    username = con.execute("SELECT username FROM md_user_info()").fetchone()[0]
    local_dbs = con.execute("SELECT count(*), count(*) FILTER (WHERE starts_with(database_name, 'cdc_v2')) "
                            "FROM duckdb_databases() WHERE NOT internal").fetchone()
    md = rec["facts"].get("motherduck", {})
    sec = rec["facts"].get("secret", {})
    save("identity_and_secret_check", {
        "flight_token_user_equals_creator": md.get("username_hmac") == keyed(username),
        "flight_databases_total": md.get("databases_total"), "flight_databases_cdc_v2": md.get("databases_cdc_v2"),
        "flight_databases_other_than_cdc_v2": md.get("databases_other"),
        "local_session_databases_total": local_dbs[0], "local_session_databases_cdc_v2": local_dbs[1],
        "secret_env_names_present": sec.get("names_present"),
        "secret_namespaced_value_matches": sec.get("namespaced_hmac") == keyed(SECRET_VALUE),
        "secret_bare_value_matches": sec.get("bare_hmac") == keyed(SECRET_VALUE),
    })


def step_net():
    run_and_collect("net1", {"CDC_V2_PROBE_MODE": "net"})
    run_and_collect("net2", {"CDC_V2_PROBE_MODE": "net"})


def step_exception():
    run_and_collect("exception", {"CDC_V2_PROBE_MODE": "exception"})


def step_exit3():
    run_and_collect("exit3", {"CDC_V2_PROBE_MODE": "exit3"})


def step_ticks():
    """Log visibility: how long after our code prints a line can MD_GET_FLIGHT_LOGS return it."""
    requested = now()
    run = ctl.run(NAME, {"CDC_V2_PROBE_MODE": "ticks"})
    n = run["run_number"]
    states, seen, polls = [], {}, 0
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        r = ctl.get_run(NAME, n)
        t = now()
        if not states or states[-1]["state"] != r["state"]:
            states.append({"state": r["state"], "local_observed_at": t.isoformat()})
        if r["state"] in ("RUNNING",) or fc.run_is_terminal(r):
            for ln in ctl.logs(NAME, n):
                text = ln.get("line") or ""
                if text.startswith("TICK ") and text not in seen:
                    seen[text] = {"local_seen_at": t.timestamp(), "reported_at": ln.get("reported_at")}
            polls += 1
        if fc.run_is_terminal(r):
            break
        time.sleep(0.5)
    lags = []
    for text, info in sorted(seen.items(), key=lambda kv: int(kv[0].split()[1])):
        _, i, ts = text.split()
        lag = {"tick": int(i), "printed_unix": float(ts), "visible_after_s": round(info["local_seen_at"] - float(ts), 2)}
        if info["reported_at"]:
            lag["reported_at_minus_printed_s"] = round(fc.parse_time(info["reported_at"]).timestamp() - float(ts), 3)
        lags.append(lag)
    final = ctl.get_run(NAME, n)
    vis = [x["visible_after_s"] for x in lags]
    save("run_ticks_log_visibility", {
        "local_requested_at": requested.isoformat(), "states_observed_polling_0_5s": states, "final": final,
        "timing": timing(final, facts_from_logs(ctl.logs(NAME, n))), "log_polls": polls, "ticks": lags,
        "visible_after_s_min": min(vis) if vis else None, "visible_after_s_max": max(vis) if vis else None,
        "visible_after_s_median": sorted(vis)[len(vis) // 2] if vis else None,
        "note": "visible_after_s compares the Flight's clock with this machine's clock (both NTP); polling 0.5 s"})


def step_cancel():
    """Cancel from outside, from this SQL connection: time from the cancel call to the CANCELLED state."""
    run = ctl.run(NAME, {"CDC_V2_PROBE_MODE": "ticks", "CDC_V2_PROBE_TICKS": "120"})
    n = run["run_number"]
    while True:
        r = ctl.get_run(NAME, n)
        if r["state"] == "RUNNING" and any((ln.get("line") or "").startswith("TICK 2 ") for ln in ctl.logs(NAME, n)):
            break
        if fc.run_is_terminal(r):
            break
        time.sleep(0.5)
    t_cancel = now()
    result = ctl.cancel(NAME, n)
    t_returned = now()
    states = []
    while True:
        r = ctl.get_run(NAME, n)
        if not states or states[-1]["state"] != r["state"]:
            states.append({"state": r["state"], "local_observed_at": now().isoformat()})
        if fc.run_is_terminal(r):
            break
        time.sleep(0.5)
    time.sleep(5)
    lines = ctl.logs(NAME, n)
    ticks = [ln["line"] for ln in lines if (ln.get("line") or "").startswith("TICK ")]
    last_tick_ts = float(ticks[-1].split()[2]) if ticks else None
    final = ctl.get_run(NAME, n)
    try:
        again = {"ok": True, "result": ctl.cancel(NAME, n)}
    except fc.FlightCtlError as exc:
        again = {"ok": False, "error": str(exc)}
    save("run_cancel", {
        "cancel_called_at": t_cancel.isoformat(), "cancel_returned_at": t_returned.isoformat(),
        "cancel_result": result, "states_after_cancel": states, "final": final,
        "last_tick_printed_after_cancel_call_s": round(last_tick_ts - t_cancel.timestamp(), 2) if last_tick_ts else None,
        "ticks_logged": len(ticks), "last_log_lines": [ln.get("line") for ln in lines[-5:]],
        "cancel_again_on_terminal_run": again, "timing": timing(final, facts_from_logs(lines))})


def step_noreq():
    """What is preinstalled: run with empty requirements, then restore."""
    with open(REQ) as f:
        req = f.read()
    try:
        ctl.update(NAME, requirements_txt="")
        upd = {"empty_requirements_accepted": True}
    except fc.FlightCtlError as exc:
        upd = {"empty_requirements_accepted": False, "error": str(exc)}
        ctl.update(NAME, requirements_txt="# no requirements\n")
    rec = run_and_collect("no_requirements")
    ctl.update(NAME, requirements_txt=req)
    save("no_requirements_update", upd | {"versions": ctl.versions(NAME, 3)})
    return rec


def step_schedule():
    """A real scheduled run: '* * * * *', wait for one run with is_scheduled, then clear the schedule."""
    out = {}
    for cron in ("*/30 * * * * *", "@hourly", "0 0 1 1 *"):
        try:
            r = ctl.update(NAME, schedule_cron=cron)
            out[cron] = {"accepted": True, "schedule_cron": r.get("schedule_cron"),
                         "schedule_status": r.get("schedule_status")}
        except fc.FlightCtlError as exc:
            out[cron] = {"accepted": False, "error": str(exc)[:300]}
    before = {r["run_number"] for r in ctl.runs(NAME, 50)}
    set_at = now()
    r = ctl.update(NAME, schedule_cron="* * * * *")
    out["* * * * *"] = {"accepted": True, "schedule_cron": r.get("schedule_cron"),
                        "schedule_status": r.get("schedule_status"), "set_at": set_at.isoformat()}
    scheduled = None
    deadline = time.monotonic() + 150
    try:
        while time.monotonic() < deadline:
            new = [x for x in ctl.runs(NAME, 10) if x["run_number"] not in before]
            if new:
                scheduled = new[-1]
                break
            time.sleep(2)
    finally:
        cleared = ctl.unschedule(NAME)
    if scheduled:
        scheduled = ctl.wait(NAME, scheduled["run_number"], timeout=300)
        out["scheduled_run"] = scheduled
        out["scheduled_run_timing"] = timing(scheduled, facts_from_logs(ctl.logs(NAME, scheduled["run_number"])))
    out["cleared"] = {"schedule_cron": cleared.get("schedule_cron"), "schedule_status": cleared.get("schedule_status")}
    try:
        ctl.query(fc.build_update_sql(ctl.resolve(NAME)["flight_id"], schedule_cron=""))
        out["clear_absent_schedule"] = {"error": None}
    except fc.FlightCtlError as exc:
        out["clear_absent_schedule"] = {"error": str(exc)[:300]}
    time.sleep(70)
    out["runs_after_clear_70s"] = [{"run_number": x["run_number"], "is_scheduled": x["is_scheduled"],
                                    "created_at": x["created_at"]} for x in ctl.runs(NAME, 5)]
    save("schedule", out)


def step_limits():
    """Source and requirements size limits (create only: nothing runs)."""
    base = 'def main():\n    print("size probe")\n\nif __name__ == "__main__":\n    main()\n'
    results = []

    def attempt(kind, size):
        if kind == "source":
            src, req = base + "#" * (size - len(base) - 1) + "\n", None
        else:
            src, req = base, "duckdb==1.5.6\n" + "#" * (size - 15) + "\n"
        try:
            s = ctl.create(SIZE_NAME, src, requirements_txt=req, instance_type="F4")
            ok, err = True, None
            ctl.delete(s["flight_id"])
        except fc.FlightCtlError as exc:
            ok, err = False, str(exc)[:300]
        results.append({"kind": kind, "bytes": size, "accepted": ok, "error": err})
        print(kind, size, ok, flush=True)

    for size in (199_999, 200_000, 200_001, 204_800, 204_801, 250_000):
        attempt("source", size)
    for size in (19_999, 20_000, 20_001, 20_480, 20_481, 25_000):
        attempt("requirements", size)
    save("definition_limits", {"results": results, "leftover": ctl.list_flights(SIZE_NAME)})


def step_cleanup():
    out = {}
    for name in (NAME, SIZE_NAME):
        if ctl.find_flight(name):
            ctl.unschedule(name)
            out[name] = ctl.delete(name)
    out["secret_drop"] = ctl.drop_secret(SECRET)
    out["flights_left_cdc_v2"] = ctl.list_flights()
    out["secrets_left_cdc_v2"] = ctl.list_secrets()
    save("cleanup", out)


STEPS = ["secret", "deploy", "facts", "net", "exception", "exit3", "ticks", "cancel", "noreq", "schedule", "limits",
         "cleanup"]

if __name__ == "__main__":
    chosen = sys.argv[1:] or STEPS
    if "deploy" not in chosen and any(s in chosen for s in ("facts", "net", "ticks")):
        print("note: the nonce changes per invocation; include 'deploy' so the Flight carries the current one")
    for s in chosen:
        print("==", s, flush=True)
        globals()["step_" + s]()
