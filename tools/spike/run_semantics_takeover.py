#!/usr/bin/env python3
"""Q9 take-over timing end to end, with a stand-in leader (a measuring instrument, not the real protocol).

    tools/with-env .venv/bin/python tools/spike/run_semantics_takeover.py [planned] [cancel_outside] [die_child]
                                                                          [freeze_all]

Flight cdc_v2_spike_runs_leader, mode leader, scheduled '* * * * *'. Every run: read the leader row
(run_number, epoch, heartbeat_at, state) in cdc_v2_spike_runs_db; a heartbeat younger than 30 s with state
'leading' => exit 0 at once. Otherwise raise the epoch with a compare-and-swap (UPDATE ... WHERE epoch = old
RETURNING), cancel the previous leader's run if it is still RUNNING (MD_CANCEL_FLIGHT_RUN from inside), start the
pipeline child (heartbeat every 10 s, fenced by the epoch) and watch it like the Q8 bootstrap (60 s hard exit).

Phases (each new leader's behaviour comes from the Flight config at its start; the driver switches the config back
to the benign 'release after 100 s' as soon as the leader under test leads):
  planned         the leader releases its record 5 s before a minute boundary and exits; the next run takes over
  cancel_outside  the driver cancels the leader's run (MD_CANCEL_FLIGHT_RUN from this machine)
  die_child       the pipeline child SIGKILLs itself; the bootstrap exits at once without releasing
  freeze_all      the pipeline SIGSTOPs the bootstrap and itself: only the next run's guard can end it
Evidence: takeover.json.
"""
import sys
import time

import run_semantics_common as c

NAME = "cdc_v2_spike_runs_leader"
BENIGN = {"CDC_V2_RUNS_MODE": "leader", "CDC_V2_RUNS_LEADER_END": "release", "CDC_V2_RUNS_LEAD_S": "100",
          "CDC_V2_RUNS_RELEASE_BEFORE_S": "5", "CDC_V2_RUNS_STALE_S": "60", "CDC_V2_RUNS_WD_EXIT": "killpg1"}


def set_config(**over):
    cfg = dict(c.DEFAULT_CONFIG, CDC_V2_RUNS_FLIGHT=NAME, **BENIGN)
    cfg.update(over)
    c.ctl.update(NAME, config=cfg)


def events_since(t):
    return [e for e in c.event_rows(NAME) if e["server_unix"] and e["server_unix"] >= t]


def wait_event(pred, since, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for e in events_since(since):
            if pred(e):
                return e
        time.sleep(3)
    return None


def next_takeover(after_t, exclude_run=None, timeout=300):
    return wait_event(lambda e: e["kind"] == "takeover" and e["server_unix"] > after_t
                      and e["run_number"] != exclude_run, after_t, timeout)


def wait_leading(end_kind, since, timeout=420):
    return wait_event(lambda e: e["kind"] == "leading" and (e["detail"] or {}).get("end") == end_kind, since, timeout)


def step_planned(results):
    t0 = time.time()
    set_config()
    handovers = []
    deadline = t0 + 600
    seen = set()
    while time.time() < deadline and len(handovers) < 3:
        for e in events_since(t0):
            if e["kind"] == "released" and (e["run_number"], e["server_unix"]) not in seen:
                tk = next_takeover(e["server_unix"], exclude_run=e["run_number"], timeout=150)
                seen.add((e["run_number"], e["server_unix"]))
                handovers.append({
                    "released_by_run": e["run_number"], "released_at": e["server"],
                    "released_seconds_into_minute": round(e["server_unix"] % 60, 2),
                    "takeover_by_run": tk["run_number"] if tk else None, "takeover_at": tk["server"] if tk else None,
                    "release_to_takeover_s": round(tk["server_unix"] - e["server_unix"], 3) if tk else None,
                    "takeover_prev": (tk["detail"] or {}).get("prev") if tk else None})
                print("planned", handovers[-1]["release_to_takeover_s"], flush=True)
        time.sleep(5)
    results["planned"] = {"handovers": handovers}


def step_cancel_outside(results):
    t0 = time.time()
    set_config(CDC_V2_RUNS_LEADER_END="run")
    lead = wait_leading("run", t0)
    set_config()
    if not lead:
        results["cancel_outside"] = {"error": "no leader with end=run appeared"}
        return
    time.sleep(15)
    n = lead["run_number"]
    t_call = time.time()
    res = c.ctl.cancel(NAME, n)
    t_ret = time.time()
    tk = next_takeover(t_call, exclude_run=n, timeout=240)
    last_hb = c.db().execute("SELECT max(epoch_ms(server_ts)) FROM events WHERE flight = ? AND run_number = ? "
                             "AND kind = 'leading'", [NAME, n]).fetchone()[0]
    results["cancel_outside"] = {
        "leader_run": n, "cancel_called_at": c.iso(t_call), "cancel_took_s": round(t_ret - t_call, 3),
        "cancel_result": res, "takeover_by_run": tk["run_number"] if tk else None,
        "takeover_at": tk["server"] if tk else None,
        "cancel_to_takeover_s": round(tk["server_unix"] - t_call, 3) if tk else None,
        "takeover_prev": (tk["detail"] or {}).get("prev") if tk else None,
        "cancel_called_seconds_into_minute": round(t_call % 60, 2), "leading_event_ms": last_hb,
        "leader_final": c.slim_run(c.ctl.get_run(NAME, n))}
    print("cancel_outside", results["cancel_outside"]["cancel_to_takeover_s"], flush=True)


def step_die_child(results):
    t0 = time.time()
    set_config(CDC_V2_RUNS_LEADER_END="die_child", CDC_V2_RUNS_LEAD_S="40")
    lead = wait_leading("die_child", t0)
    set_config()
    if not lead:
        results["die_child"] = {"error": "no leader with end=die_child appeared"}
        return
    n = lead["run_number"]
    die = wait_event(lambda e: e["kind"] == "pipeline_sigkill_self" and e["run_number"] == n, t0, 120)
    t_die = die["server_unix"] if die else time.time()
    tk = next_takeover(t_die, exclude_run=n, timeout=240)
    final = c.ctl.wait(NAME, n, timeout=120)
    results["die_child"] = {
        "leader_run": n, "pipeline_killed_at": die["server"] if die else None,
        "killed_seconds_into_minute": round(t_die % 60, 2),
        "leader_run_terminal": c.slim_run(final),
        "kill_to_run_terminal_s": round((c.unix(final.get("ended_at")) or 0) - t_die, 3),
        "takeover_by_run": tk["run_number"] if tk else None, "takeover_at": tk["server"] if tk else None,
        "kill_to_takeover_s": round(tk["server_unix"] - t_die, 3) if tk else None,
        "takeover_prev": (tk["detail"] or {}).get("prev") if tk else None}
    print("die_child", results["die_child"]["kill_to_takeover_s"], flush=True)


def step_freeze_all(results):
    t0 = time.time()
    set_config(CDC_V2_RUNS_LEADER_END="freeze_all", CDC_V2_RUNS_LEAD_S="30")
    lead = wait_leading("freeze_all", t0)
    set_config()
    if not lead:
        results["freeze_all"] = {"error": "no leader with end=freeze_all appeared"}
        return
    n = lead["run_number"]
    fr = wait_event(lambda e: e["kind"] == "freezing" and e["run_number"] == n, t0, 120)
    t_fr = fr["server_unix"] if fr else time.time()
    tk = next_takeover(t_fr, exclude_run=n, timeout=300)
    canc = wait_event(lambda e: e["kind"] == "cancelled_previous" and (e["detail"] or {}).get("run_number") == n,
                      t_fr, 120)
    try:
        final = c.ctl.wait(NAME, n, timeout=120)
    except c.fc.FlightCtlError:
        final = c.ctl.get_run(NAME, n)
    end = c.unix(final.get("ended_at")) or c.unix(final.get("cancelled_at"))
    results["freeze_all"] = {
        "leader_run": n, "frozen_at": fr["server"] if fr else None, "frozen_seconds_into_minute": round(t_fr % 60, 2),
        "takeover_by_run": tk["run_number"] if tk else None, "takeover_at": tk["server"] if tk else None,
        "freeze_to_takeover_s": round(tk["server_unix"] - t_fr, 3) if tk else None,
        "takeover_prev": (tk["detail"] or {}).get("prev") if tk else None,
        "cancelled_previous_event": canc, "frozen_run_final": c.slim_run(final),
        "freeze_to_frozen_run_terminal_s": round(end - t_fr, 3) if end else None}
    print("freeze_all", results["freeze_all"]["freeze_to_takeover_s"],
          results["freeze_all"]["freeze_to_frozen_run_terminal_s"], flush=True)


def main():
    steps = sys.argv[1:] or ["planned", "cancel_outside", "die_child", "freeze_all"]
    c.setup_db()
    c.db().execute("INSERT OR REPLACE INTO leader VALUES (1, 0, 0, now() - INTERVAL 1 HOUR, 'released')")
    c.deploy(NAME, config=BENIGN)
    set_at = c.now()
    c.ctl.update(NAME, schedule_cron="* * * * *")
    results = {}
    try:
        for s in steps:
            print("==", s, flush=True)
            globals()["step_" + s](results)
    finally:
        c.ctl.unschedule(NAME)
        cleared = c.now()
        time.sleep(5)
        for r in c.ctl.runs(NAME, 200):
            if c.fc.run_is_active(r):
                try:
                    c.ctl.cancel(NAME, r["run_number"])
                except c.fc.FlightCtlError:
                    pass
    time.sleep(5)
    runs = sorted(c.ctl.runs(NAME, 200), key=lambda r: r["run_number"])
    c.save("takeover", {"flight": NAME, "schedule_set_at": set_at.isoformat(), "schedule_cleared_at": cleared.isoformat(),
                        "benign_config": BENIGN, "results": results,
                        "runs": [dict(c.slim_run(r), **c.run_timing(r)) for r in runs],
                        "events": c.event_rows(NAME)})


if __name__ == "__main__":
    main()
