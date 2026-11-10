#!/usr/bin/env python3
"""Q8 heartbeat hard-exit (SPEC 4.2): a minimal bootstrap watches the pipeline's heartbeat and hard-exits the run
when it is older than 60 s.

    tools/with-env .venv/bin/python tools/spike/run_semantics_watchdog.py [variants] [frozen_bootstrap]

Process tree of one run (flights/spike_runs/main.py, mode watchdog):
  main.py (bootstrap, no database connection) -> pipeline (own process group; heartbeat row every 10 s, then touches
  a local file) -> pgrand (same group, a row every 1 s);  main.py -> helper (own session, a row every 1 s, sends
  SIGCONT to the pipeline group 100 s after the freeze, to show whether frozen processes survived the run's end).
At 30 s the pipeline freezes its own process group with SIGSTOP.

variants: four runs in parallel, one per way of ending: osexit1 (os._exit(1) of the bootstrap alone), osexit0,
          killpg1 (SIGKILL to the pipeline group, then os._exit(1)), killpg0. watchdog_variants.json
frozen_bootstrap: the pipeline SIGSTOPs main.py and then its own group: nothing in the run is left to act. Runs on
          a Flight with max_runtime_sec = 180 to show what bounds it. watchdog_frozen_bootstrap.json
"""
import sys
import time

import run_semantics_common as c

NAME = "cdc_v2_spike_runs_wd"
NAME_MAXRT = "cdc_v2_spike_runs_wd_maxrt"
CFG = {"CDC_V2_RUNS_MODE": "watchdog", "CDC_V2_RUNS_FREEZE": "pipeline", "CDC_V2_RUNS_FREEZE_AT_S": "30",
       "CDC_V2_RUNS_STALE_S": "60", "CDC_V2_RUNS_CONT_AFTER_S": "100"}


def analyse(flight, n, final):
    lines = c.logs(flight, n)
    p = c.parse_logs(lines)
    ev = p["events"]
    freeze = next((e for e in ev if e["kind"] in ("freezing",)), None)
    hard = next((e for e in ev if e["kind"] == "hard_exit"), None)
    stale = next((e for e in ev if e["kind"] == "heartbeat_stale"), None)
    end = c.unix(final.get("ended_at")) or c.unix(final.get("cancelled_at"))
    ticks = c.tick_summary(flight, n)
    db_ev = c.event_rows(flight, n)
    db_freeze = next((e for e in db_ev if e["kind"] == "freezing"), None)
    f_t = freeze["t"] if freeze else (db_freeze["client_unix"] if db_freeze else None)
    rows_after_end = {}
    if end:
        for role, cnt, last in c.db().execute(
                "SELECT role, count(*), max(epoch_ms(server_ts)) / 1000.0 FROM ticks WHERE flight = ? AND "
                "run_number = ? AND epoch_ms(server_ts) / 1000.0 > ? GROUP BY ALL", [flight, n, end + 1]).fetchall():
            rows_after_end[role] = {"rows": cnt, "last": c.iso(last)}
    for t in ticks:
        if end:
            t["last_row_minus_run_end_s"] = round(t["last_server_unix"] - end, 3)
    return {
        "run": n, "final": c.slim_run(final), "timing": c.run_timing(final, p["first_line_unix"]),
        "freeze_at": c.iso(f_t), "heartbeat_stale_logged_at": c.iso(stale["t"]) if stale else None,
        "hard_exit_logged_at": c.iso(hard["t"]) if hard else None,
        "freeze_to_hard_exit_s": round(hard["t"] - f_t, 3) if hard and f_t else None,
        "freeze_to_run_terminal_s": round(end - f_t, 3) if end and f_t else None,
        "hard_exit_to_run_terminal_s": round(end - hard["t"], 3) if end and hard else None,
        "rows_written_more_than_1s_after_run_end": rows_after_end,
        "ticks_per_role": ticks, "db_events": db_ev, "events": ev,
        "log_tail": [ln.get("line") for ln in lines[-12:]],
    }


def step_variants():
    runs = {}
    for v in ("osexit1", "osexit0", "killpg1", "killpg0"):
        r = c.run(NAME, CDC_V2_RUNS_WD_EXIT=v)
        runs[v] = r["run_number"]
    finals = {v: c.ctl.wait(NAME, n, timeout=600, poll=1) for v, n in runs.items()}
    time.sleep(150)  # helper sends SIGCONT 100 s after the freeze; watch for rows after the run ended
    out = {}
    for v, n in runs.items():
        out[v] = analyse(NAME, n, c.ctl.get_run(NAME, n))
        print(v, out[v]["final"]["state"], out[v]["final"]["exit_code"], "freeze->terminal",
              out[v]["freeze_to_run_terminal_s"], "after-end rows", out[v]["rows_written_more_than_1s_after_run_end"],
              flush=True)
    c.save("watchdog_variants", {"flight": NAME, "config": CFG, "variants": out})


def step_frozen_bootstrap():
    c.deploy(NAME_MAXRT, config=dict(CFG, CDC_V2_RUNS_FREEZE="all", CDC_V2_RUNS_CONT_AFTER_S="400"),
             max_runtime_sec=180)
    r = c.run(NAME_MAXRT)
    n = r["run_number"]
    final = c.ctl.wait(NAME_MAXRT, n, timeout=900, poll=1)
    time.sleep(60)
    res = analyse(NAME_MAXRT, n, c.ctl.get_run(NAME_MAXRT, n))
    res["max_runtime_sec"] = 180
    sta = c.unix(final.get("started_at"))
    end = c.unix(final.get("ended_at")) or c.unix(final.get("cancelled_at"))
    res["terminal_minus_started_plus_max_runtime_s"] = round(end - sta - 180, 3) if sta and end else None
    c.save("watchdog_frozen_bootstrap", {"flight": NAME_MAXRT, "result": res})
    print("frozen bootstrap", res["final"], flush=True)


def main():
    steps = sys.argv[1:] or ["variants", "frozen_bootstrap"]
    c.setup_db()
    c.deploy(NAME, config=CFG)
    for s in steps:
        print("==", s, flush=True)
        globals()["step_" + s]()


if __name__ == "__main__":
    main()
