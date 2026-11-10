#!/usr/bin/env python3
"""Q5 maximum run time: which max_runtime_sec values are accepted, and what happens when a run exceeds it.

    tools/with-env .venv/bin/python tools/spike/run_semantics_maxruntime.py [probe] [exceed] [--seconds S]

probe:  MD_UPDATE_FLIGHT with a range of max_runtime_sec values (nothing runs); max_runtime_limits.json.
exceed: max_runtime_sec = S (default: the smallest accepted value >= 60), two runs of the victim (signals logged,
        SIGTERM ignored, child + detached grandchild, a row every 200 ms with MotherDuck's clock), then 3 minutes
        of watching for a retry; max_runtime_exceed.json.
"""
import argparse
import json
import os
import time

import run_semantics_common as c

NAME = "cdc_v2_spike_runs_maxrt"
VICTIM = {"CDC_V2_RUNS_MODE": "ticker", "CDC_V2_RUNS_CHILDREN": "1", "CDC_V2_RUNS_ON_TERM": "ignore",
          "CDC_V2_RUNS_TICK_S": "0.2", "CDC_V2_RUNS_DURATION_S": "900", "CDC_V2_RUNS_CHILD_S": "900",
          "CDC_V2_RUNS_TICK_VIA": "pgwire"}
VALUES = [-1, 0, 1, 5, 10, 30, 59, 60, 61, 120, 3600, 28800, 28801, 43200, 86400, 604800]


def current_max(name):
    v = c.ctl.versions(name, 1)
    return v[0].get("max_runtime_sec") if v else None


def step_probe():
    results = []
    for v in VALUES:
        try:
            c.ctl.update(NAME, max_runtime_sec=v)
            results.append({"max_runtime_sec": v, "accepted": True, "read_back": current_max(NAME)})
        except c.fc.FlightCtlError as exc:
            results.append({"max_runtime_sec": v, "accepted": False, "error": str(exc)[:300]})
        print(results[-1], flush=True)
    c.save("max_runtime_limits", {"flight": NAME, "results": results})
    return results


def step_exceed(seconds):
    c.ctl.update(NAME, max_runtime_sec=seconds)
    before = {r["run_number"] for r in c.ctl.runs(NAME, 50)}
    runs = []
    for _ in range(2):
        r = c.run(NAME)
        runs.append(r["run_number"])
        time.sleep(2)
    finals = {}
    for n in runs:
        finals[n] = c.ctl.wait(NAME, n, timeout=seconds + 600, poll=1)
    time.sleep(15)
    reports = []
    for n in runs:
        f = finals[n] = c.ctl.get_run(NAME, n)
        lines = c.logs(NAME, n)
        p = c.parse_logs(lines)
        sta = c.unix(f.get("started_at"))
        limit_at = sta + seconds if sta else None
        ticks = c.tick_summary(NAME, n)
        for t in ticks:
            if limit_at:
                t["last_row_server_minus_limit_s"] = round(t["last_server_unix"] - limit_at, 3)
        sig = [e for e in p["events"] if e["kind"] == "signal"]
        for e in sig:
            if limit_at:
                e["t_minus_limit_s"] = round(e["t"] - limit_at, 3)
        end = c.unix(f.get("ended_at")) or c.unix(f.get("cancelled_at"))
        reports.append({
            "run": n, "final": c.slim_run(f), "timing": c.run_timing(f, p["first_line_unix"]),
            "terminal_minus_limit_s": round(end - limit_at, 3) if end and limit_at else None,
            "ticks_per_role": ticks, "signals": sig,
            "after_signal": [e for e in p["events"] if e["kind"].startswith("after_signal")],
            "db_events": c.event_rows(NAME, n), "log_tail": [ln.get("line") for ln in lines[-12:]],
        })
    watch_until = time.time() + 180
    while time.time() < watch_until:
        time.sleep(20)
    after = [c.slim_run(r) for r in c.ctl.runs(NAME, 50) if r["run_number"] not in before]
    c.save("max_runtime_exceed", {"flight": NAME, "max_runtime_sec": seconds, "victim_config": VICTIM,
                                  "runs": reports, "all_runs_after_3_min": after,
                                  "extra_runs_not_started_by_driver": [r for r in after
                                                                       if r["run_number"] not in runs]})


def step_stagger(seconds, count=6, gap=17):
    """How exact is the timeout? `count` runs started `gap` s apart with max_runtime_sec = `seconds`: if the
    timeout were enforced per run, each would end at started_at + seconds; clustered cancel times mean a sweep."""
    cfg = dict(c.DEFAULT_CONFIG, CDC_V2_RUNS_FLIGHT=NAME)
    cfg.update(VICTIM, CDC_V2_RUNS_CHILDREN="0", CDC_V2_RUNS_TICK_VIA="duckdb", CDC_V2_RUNS_TICK_S="1")
    c.ctl.update(NAME, max_runtime_sec=seconds, config=cfg)
    runs = []
    for i in range(count):
        runs.append(c.run(NAME)["run_number"])
        if i < count - 1:
            time.sleep(gap)
    rows = []
    for n in runs:
        f = c.ctl.wait(NAME, n, timeout=seconds + 900, poll=2)
        sta, can = c.unix(f.get("started_at")), c.unix(f.get("cancelled_at")) or c.unix(f.get("ended_at"))
        ticks = c.tick_summary(NAME, n)
        rows.append({"run": n, "final": c.slim_run(f), "limit_at": c.iso(sta + seconds),
                     "terminal_minus_limit_s": round(can - sta - seconds, 3) if sta and can else None,
                     "terminal_seconds_into_minute": round(can % 60, 3) if can else None,
                     "last_row_minus_terminal_s": round(ticks[-1]["last_server_unix"] - can, 3) if ticks and can
                     else None})
        print(rows[-1]["run"], rows[-1]["terminal_minus_limit_s"], flush=True)
    c.save("max_runtime_stagger_%ds" % seconds, {"flight": NAME, "max_runtime_sec": seconds, "gap_s": gap,
                                                 "runs": rows,
                                                 "terminal_minus_limit_s": c.dist(r["terminal_minus_limit_s"]
                                                                                  for r in rows)})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("steps", nargs="*", default=["probe", "exceed"])
    ap.add_argument("--seconds", type=int)
    a = ap.parse_args()
    c.setup_db()
    c.deploy(NAME, config=VICTIM)
    seconds = a.seconds
    if "probe" in a.steps:
        res = step_probe()
        ok = sorted(r["max_runtime_sec"] for r in res if r["accepted"] and r["max_runtime_sec"] >= 60)
        seconds = seconds or (ok[0] if ok else 60)
    if "exceed" in a.steps:
        if seconds is None:
            path = os.path.join(c.OUT, "max_runtime_limits.json")
            res = json.load(open(path))["results"]
            ok = sorted(r["max_runtime_sec"] for r in res if r["accepted"] and r["max_runtime_sec"] >= 60)
            seconds = ok[0] if ok else 60
        step_exceed(seconds)
    if "stagger" in a.steps:
        step_stagger(seconds or 60)


if __name__ == "__main__":
    main()
