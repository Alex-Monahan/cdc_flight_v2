#!/usr/bin/env python3
"""Q7 process model: does the run end when main.py exits while children live? Does the platform react to silence?

    tools/with-env .venv/bin/python tools/spike/run_semantics_process.py [orphan_pipe] [orphan_devnull] [silent]

orphan_*: main.py starts a child (same session) that starts a detached grandchild (double fork + setsid); both
          write a row every second for 240 s; main.py exits after 20 s. `pipe`: child and grandchild keep the run's
          stdout; `devnull`: their stdout is /dev/null. Evidence: process_orphan_pipe.json, process_orphan_devnull.json
silent:   no output at all for 6 minutes (a database row every 5 s), then exit 0. Evidence: process_silent.json
"""
import sys
import time

import run_semantics_common as c

NAME = "cdc_v2_spike_runs_proc"
CFG = {"CDC_V2_RUNS_MODE": "orphan", "CDC_V2_RUNS_TICK_S": "1", "CDC_V2_RUNS_MAIN_S": "20",
       "CDC_V2_RUNS_CHILD_S": "240"}


def orphan(stdout):
    r = c.run(NAME, CDC_V2_RUNS_STDOUT=stdout)
    n = r["run_number"]
    final = c.ctl.wait(NAME, n, timeout=600, poll=1)
    end = c.unix(final.get("ended_at")) or c.unix(final.get("cancelled_at"))
    # keep watching the table until the children's own 240 s would be over
    deadline = c.unix(final["started_at"]) + 20 + 240 + 30
    while time.time() < deadline:
        time.sleep(10)
    lines = c.logs(NAME, n)
    p = c.parse_logs(lines)
    main_exit = next((e for e in p["events"] if e["kind"] == "main_exiting"), None)
    ticks = c.tick_summary(NAME, n)
    for t in ticks:
        if end:
            t["last_row_server_minus_run_end_s"] = round(t["last_server_unix"] - end, 3)
        if main_exit:
            t["last_row_server_minus_main_exit_s"] = round(t["last_server_unix"] - main_exit["t"], 3)
    out = {"run": n, "stdout_of_children": stdout, "final": c.slim_run(final),
           "timing": c.run_timing(final, p["first_line_unix"]),
           "main_exit_logged_at": c.iso(main_exit["t"]) if main_exit else None,
           "run_end_minus_main_exit_s": round(end - main_exit["t"], 3) if end and main_exit else None,
           "process_table_at_main_exit": main_exit.get("procs") if main_exit else None,
           "ticks_per_role": ticks, "db_events": c.event_rows(NAME, n), "events": p["events"],
           "log_tail": [ln.get("line") for ln in lines[-10:]]}
    c.save("process_orphan_%s" % stdout, out)


def step_orphan_pipe():
    orphan("pipe")


def step_orphan_devnull():
    orphan("devnull")


def step_silent():
    r = c.run(NAME, CDC_V2_RUNS_MODE="silent", CDC_V2_RUNS_DURATION_S="360")
    n = r["run_number"]
    final = c.ctl.wait(NAME, n, timeout=900, poll=5)
    lines = c.logs(NAME, n)
    p = c.parse_logs(lines)
    c.save("process_silent", {"run": n, "silent_seconds": 360, "final": c.slim_run(final),
                              "timing": c.run_timing(final, p["first_line_unix"]),
                              "ticks": c.tick_summary(NAME, n),
                              "log_lines": [{"line": ln.get("line"), "reported_at": ln.get("reported_at")}
                                            for ln in lines if not (ln.get("line") or "").startswith(" ")][-12:]})


def main():
    steps = sys.argv[1:] or ["orphan_pipe", "orphan_devnull", "silent"]
    c.setup_db()
    c.deploy(NAME, config=CFG)
    for s in steps:
        print("==", s, flush=True)
        globals()["step_" + s]()


if __name__ == "__main__":
    main()
