#!/usr/bin/env python3
"""Q6 retries and out-of-memory kills.

    tools/with-env .venv/bin/python tools/spike/run_semantics_retries.py [ondemand] [scheduled]

ondemand:  four failing runs (sys.exit(3), uncaught exception, OOM in a child process, OOM in main.py), each
           waited for; then 4 minutes of watching the run list for a run the driver did not start; retries_ondemand.json
scheduled: '* * * * *' with every run failing (exit 3) for 4 minutes: exactly one run per minute, or retries?
           retries_scheduled.json
Retry settings: the documented MD_CREATE_FLIGHT parameters are recorded with the evidence (none is a retry).
"""
import sys
import time

import run_semantics_common as c

NAME = "cdc_v2_spike_runs_fail"
CFG = {"CDC_V2_RUNS_MODE": "fail", "CDC_V2_RUNS_FAIL": "exit3"}
DOCUMENTED_CREATE_PARAMS = ["name", "source_code", "schedule_cron", "requirements_txt", "config", "access_token_name",
                            "flight_secret_names", "instance_type", "max_runtime_sec", "metadata"]


def report(n):
    f = c.ctl.get_run(NAME, n)
    lines = c.logs(NAME, n)
    p = c.parse_logs(lines)
    return {"final": c.slim_run(f), "timing": c.run_timing(f, p["first_line_unix"]), "events": p["events"],
            "log_tail": [ln.get("line") for ln in lines[-12:]], "log_lines": len(lines)}


def step_ondemand():
    before = {r["run_number"] for r in c.ctl.runs(NAME, 100)}
    started, reports = [], {}
    for kind in ("exit3", "exception", "oom_child", "oom_main"):
        r = c.run(NAME, CDC_V2_RUNS_FAIL=kind)
        started.append(r["run_number"])
        c.ctl.wait(NAME, r["run_number"], timeout=600, poll=1)
        time.sleep(5)
        reports[kind] = report(r["run_number"])
        print(kind, reports[kind]["final"]["state"], reports[kind]["final"]["exit_code"], flush=True)
    watch = []
    t_end = time.time() + 240
    while time.time() < t_end:
        time.sleep(30)
        watch.append({"at": c.now().isoformat(),
                      "runs_not_started_by_driver": [r["run_number"] for r in c.ctl.runs(NAME, 100)
                                                     if r["run_number"] not in before
                                                     and r["run_number"] not in started]})
    c.save("retries_ondemand", {"flight": NAME, "runs": reports, "started_by_driver": started,
                                "watch_4_min": watch,
                                "flight_definition": {k: v for k, v in c.ctl.resolve(NAME).items()
                                                      if k not in ("config",)},
                                "documented_create_parameters": DOCUMENTED_CREATE_PARAMS})


def step_scheduled():
    before = {r["run_number"] for r in c.ctl.runs(NAME, 100)}
    c.ctl.update(NAME, config=dict(c.DEFAULT_CONFIG, CDC_V2_RUNS_FLIGHT=NAME, **CFG))
    set_at = c.now()
    c.ctl.update(NAME, schedule_cron="* * * * *")
    time.sleep(240)
    c.ctl.unschedule(NAME)
    cleared_at = c.now()
    time.sleep(60)
    runs = sorted((r for r in c.ctl.runs(NAME, 100) if r["run_number"] not in before), key=lambda r: r["run_number"])
    for r in runs:
        if c.fc.run_is_active(r):
            c.ctl.wait(NAME, r["run_number"], timeout=120)
    runs = sorted((r for r in c.ctl.runs(NAME, 100) if r["run_number"] not in before), key=lambda r: r["run_number"])
    minutes = [r["scheduled_at"][:16] for r in runs if r.get("is_scheduled")]
    c.save("retries_scheduled", {"flight": NAME, "schedule_set_at": set_at.isoformat(),
                                 "schedule_cleared_at": cleared_at.isoformat(),
                                 "runs": [dict(c.slim_run(r), **c.run_timing(r)) for r in runs],
                                 "runs_per_scheduled_minute": {m: minutes.count(m) for m in sorted(set(minutes))}})


def main():
    steps = sys.argv[1:] or ["ondemand", "scheduled"]
    c.setup_db()
    c.deploy(NAME, config=CFG)
    for s in steps:
        print("==", s, flush=True)
        globals()["step_" + s]()


if __name__ == "__main__":
    main()
