#!/usr/bin/env python3
"""Q4 cancel from inside: run B of a Flight cancels run A with MD_CANCEL_FLIGHT_RUN, using only the Flight's own
environment (MOTHERDUCK_TOKEN, MOTHERDUCK_FLIGHT_ID, MOTHERDUCK_FLIGHT_RUN_NUMBER).

    tools/with-env .venv/bin/python tools/spike/run_semantics_cancel.py [same_flight[:N]] [self] [cross_flight]

Run A (victim): ticker mode, a row every 200 ms through MotherDuck's Postgres endpoint (server-side now()), a
child process and a detached grandchild doing the same, handlers on every catchable signal that log the signal and
then try to COMMIT a transaction, SIGTERM ignored (so a grace period becomes visible).
Run B (canceller): lists its Flight's runs and cancels every other RUNNING run.
Evidence: cancel_same_flight.json, cancel_self.json, cancel_cross_flight.json.
"""
import sys
import time

import run_semantics_common as c

NAME = "cdc_v2_spike_runs_cancel"
OTHER = "cdc_v2_spike_runs_cancel2"
VICTIM = {"CDC_V2_RUNS_MODE": "ticker", "CDC_V2_RUNS_CHILDREN": "1", "CDC_V2_RUNS_ON_TERM": "ignore",
          "CDC_V2_RUNS_TICK_S": "0.2", "CDC_V2_RUNS_DURATION_S": "300", "CDC_V2_RUNS_CHILD_S": "300",
          "CDC_V2_RUNS_TICK_VIA": "pgwire"}


def wait_roles(flight, n, roles, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        have = {t["role"] for t in c.tick_summary(flight, n)}
        if roles <= have:
            return have
        time.sleep(1)
    return {t["role"] for t in c.tick_summary(flight, n)}


def victim_report(flight, n, cancel_call_unix):
    final = c.ctl.get_run(flight, n)
    lines = c.logs(flight, n)
    p = c.parse_logs(lines)
    ticks = c.tick_summary(flight, n)
    rows = c.db().execute(
        "SELECT role, client_unix, epoch_ms(server_ts) / 1000.0 FROM ticks WHERE flight = ? AND run_number = ? "
        "ORDER BY server_ts", [flight, n]).fetchall()
    offsets = [r[2] - r[1] for r in rows]
    per_role = []
    for t in ticks:
        d = dict(t)
        if cancel_call_unix:
            d["last_row_server_minus_cancel_call_s"] = round(t["last_server_unix"] - cancel_call_unix, 3)
            d["last_row_client_minus_cancel_call_s"] = round(t["last_client_unix"] - cancel_call_unix, 3)
            d["rows_after_cancel_call"] = c.db().execute(
                "SELECT count(*) FROM ticks WHERE flight = ? AND run_number = ? AND role = ? AND "
                "epoch_ms(server_ts) / 1000.0 > ?", [flight, n, t["role"], cancel_call_unix]).fetchone()[0]
        per_role.append(d)
    events = c.event_rows(flight, n)
    signals = [e for e in p["events"] if e["kind"] == "signal"]
    after = [e for e in p["events"] if e["kind"].startswith("after_signal")]
    return {
        "final": c.slim_run(final), "timing": c.run_timing(final, p["first_line_unix"]),
        "cancelled_at_minus_cancel_call_s": round(c.unix(final["cancelled_at"]) - cancel_call_unix, 3)
        if final.get("cancelled_at") and cancel_call_unix else None,
        "ticks_per_role": per_role,
        "server_minus_client_clock_s": c.dist(offsets),
        "signals_logged_on_stdout": signals, "after_signal_commit_attempts_stdout": after,
        "db_events": events, "log_tail": [ln.get("line") for ln in lines[-15:]], "log_lines": len(lines),
    }


def step_same_flight(suffix=""):
    a = c.run(NAME, **VICTIM)
    n = a["run_number"]
    c.wait_state(NAME, n)
    roles = wait_roles(NAME, n, {"main:v1", "child", "grandchild"})
    time.sleep(5)
    b = c.run(NAME, CDC_V2_RUNS_MODE="canceller", CDC_V2_RUNS_TARGET="others")
    bf = c.ctl.wait(NAME, b["run_number"], timeout=240, poll=1)
    c.ctl.wait(NAME, n, timeout=240, poll=1)
    time.sleep(20)
    bp = c.parse_logs(c.logs(NAME, b["run_number"]))
    calls = [e for e in bp["events"] if e["kind"] == "cancel_result"]
    call = next((x for x in calls if x.get("run_number") == n), None)
    call_unix = call["call_client_unix"] if call else None
    out = {"victim_run": n, "victim_config": VICTIM, "roles_ticking_before_cancel": sorted(roles),
           "canceller_run": b["run_number"], "canceller_final": c.slim_run(bf),
           "canceller_timing": c.run_timing(bf, bp["first_line_unix"]), "canceller_events": bp["events"],
           "cancel_call": call, "victim": victim_report(NAME, n, call_unix)}
    c.save("cancel_same_flight" + suffix, out)


def step_self():
    r = c.run(NAME, CDC_V2_RUNS_MODE="canceller", CDC_V2_RUNS_TARGET="self")
    final = c.ctl.wait(NAME, r["run_number"], timeout=240, poll=0.5)
    time.sleep(10)
    lines = c.logs(NAME, r["run_number"])
    p = c.parse_logs(lines)
    c.save("cancel_self", {"run": r["run_number"], "final": c.slim_run(final),
                           "timing": c.run_timing(final, p["first_line_unix"]), "events": p["events"],
                           "db_events": c.event_rows(NAME, r["run_number"]),
                           "log_tail": [ln.get("line") for ln in lines[-10:]]})


def step_cross_flight():
    other = c.ctl.find_flight(OTHER)
    a = c.run(OTHER, **dict(VICTIM, CDC_V2_RUNS_CHILDREN="0"))
    n = a["run_number"]
    c.wait_state(OTHER, n)
    wait_roles(OTHER, n, {"main:v1"})
    time.sleep(3)
    b = c.run(NAME, CDC_V2_RUNS_MODE="canceller", CDC_V2_RUNS_TARGET="%s:%d" % (other["flight_id"], n))
    bf = c.ctl.wait(NAME, b["run_number"], timeout=240, poll=1)
    try:
        c.ctl.wait(OTHER, n, timeout=60, poll=1)
    except c.fc.FlightCtlError:
        pass
    time.sleep(10)
    bp = c.parse_logs(c.logs(NAME, b["run_number"]))
    call = next((e for e in bp["events"] if e["kind"] == "cancel_result"), None)
    vic = victim_report(OTHER, n, call["call_client_unix"] if call else None)
    still = c.ctl.get_run(OTHER, n)
    if c.fc.run_is_active(still):
        c.ctl.cancel(OTHER, n)
        vic["driver_cancelled_afterwards"] = True
    c.save("cancel_cross_flight", {"victim_flight": OTHER, "victim_run": n, "canceller_flight": NAME,
                                   "canceller_run": b["run_number"], "canceller_final": c.slim_run(bf),
                                   "canceller_events": bp["events"], "cancel_call": call, "victim": vic})


def main():
    steps = sys.argv[1:] or ["same_flight", "self", "cross_flight"]
    c.setup_db()
    c.deploy(NAME, config=VICTIM)
    c.deploy(OTHER, config=VICTIM)
    for s in steps:
        print("==", s, flush=True)
        name, _, suffix = s.partition(":")  # same_flight:2 writes cancel_same_flight_2.json
        globals()["step_" + name](*(["_" + suffix] if suffix else []))


if __name__ == "__main__":
    main()
