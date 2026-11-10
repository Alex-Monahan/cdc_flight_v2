#!/usr/bin/env python3
"""Q1 overlap: a Flight scheduled '* * * * *' whose runs last longer than a minute. Start, skip or queue?

    tools/with-env .venv/bin/python tools/spike/run_semantics_overlap.py [--minutes 13] [--run-seconds 330]

Deploys cdc_v2_spike_runs_overlap (ticker mode, one database row every 5 s, runs last --run-seconds), sets the
schedule, polls the run list every 2 s for --minutes, fires 3 on-demand runs (MD_RUN_FLIGHT) half-way, clears the
schedule, observes 40 s more, then cancels what is still active. Evidence: evidence/p2/run_semantics/overlap.json.
"""
import argparse
import time

import run_semantics_common as c

NAME = "cdc_v2_spike_runs_overlap"


def snapshot(t0):
    runs = c.ctl.runs(NAME, 60)
    return {"t_s": round(time.time() - t0, 1), "at": c.now().isoformat(),
            "runs": {r["run_number"]: r["state"] for r in runs}}, runs


def max_concurrency(intervals):
    events = []
    for s, e in intervals:
        events.append((s, 1))
        events.append((e, -1))
    cur = best = 0
    for _, d in sorted(events, key=lambda x: (x[0], x[1])):
        cur += d
        best = max(best, cur)
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=13)
    ap.add_argument("--run-seconds", type=int, default=330)
    a = ap.parse_args()
    c.setup_db()
    c.deploy(NAME, config={"CDC_V2_RUNS_MODE": "ticker", "CDC_V2_RUNS_TICK_S": "5",
                           "CDC_V2_RUNS_DURATION_S": str(a.run_seconds)})
    t0 = time.time()
    first_seen, snaps, on_demand = {}, [], []
    set_at = c.now()
    c.ctl.update(NAME, schedule_cron="* * * * *")
    end = t0 + a.minutes * 60
    fired = False
    while time.time() < end:
        snap, runs = snapshot(t0)
        snaps.append(snap)
        for r in runs:
            first_seen.setdefault(r["run_number"], {"first_seen_at": snap["at"], "first_seen_state": r["state"],
                                                    "is_scheduled": r["is_scheduled"]})
        if not fired and time.time() > t0 + a.minutes * 30:
            for _ in range(3):
                rr = c.run(NAME)
                on_demand.append({"run_number": rr["run_number"], "returned_state": rr.get("state"),
                                  "requested_at": rr["local_requested_at"],
                                  "active_runs_before": sum(1 for x in runs if fc_active(x))})
            fired = True
        time.sleep(2)
    cleared_at = c.now()
    cleared = c.ctl.unschedule(NAME)
    after = []
    t1 = time.time()
    while time.time() < t1 + 40:
        snap, runs = snapshot(t0)
        after.append(snap)
        time.sleep(2)
    final_runs = c.ctl.runs(NAME, 60)
    still_active = [r["run_number"] for r in final_runs if fc_active(r)]
    for n in still_active:
        try:
            c.ctl.cancel(NAME, n)
        except c.fc.FlightCtlError as exc:
            print("cancel", n, exc)
    time.sleep(3)
    final_runs = c.ctl.runs(NAME, 60)
    rows = []
    intervals_sched, intervals_all = [], []
    for r in sorted(final_runs, key=lambda x: x["run_number"]):
        ticks = c.tick_summary(NAME, r["run_number"])
        s = c.unix(r.get("started_at"))
        e = c.unix(r.get("ended_at")) or c.unix(r.get("cancelled_at"))
        if s and e:
            intervals_all.append((s, e))
            if r["is_scheduled"]:
                intervals_sched.append((s, e))
        row = c.slim_run(r)
        row.update(c.run_timing(r))
        row.update(first_seen.get(r["run_number"], {}))
        row["ticks_rows"] = sum(t["rows"] for t in ticks)
        row["code_first_row"] = ticks[0]["first_server"] if ticks else None
        row["code_last_row"] = ticks[-1]["last_server"] if ticks else None
        rows.append(row)
    sched = [r for r in rows if r["is_scheduled"]]
    minutes = sorted({r["scheduled_at"][:16] for r in sched if r.get("scheduled_at")})
    # concurrency seen in polling snapshots (RUNNING at the same time)
    snap_running = max((sum(1 for st in s["runs"].values() if st == "RUNNING") for s in snaps), default=0)
    out = {
        "flight": NAME, "schedule": "* * * * *", "run_seconds": a.run_seconds, "observe_minutes": a.minutes,
        "schedule_set_at": set_at.isoformat(), "schedule_cleared_at": cleared_at.isoformat(),
        "cleared": {"schedule_cron": cleared.get("schedule_cron"), "schedule_status": cleared.get("schedule_status")},
        "scheduled_runs": len(sched), "scheduled_minutes": minutes, "distinct_scheduled_minutes": len(minutes),
        "max_concurrent_scheduled_runs_by_interval": max_concurrency(intervals_sched),
        "max_concurrent_all_runs_by_interval": max_concurrency(intervals_all),
        "max_running_in_one_poll": snap_running,
        "on_demand_fired_while_active": on_demand,
        "active_when_driver_cancelled": still_active,
        "runs": rows, "poll_snapshots": snaps, "after_clear_snapshots": after,
    }
    c.save("overlap", out)
    print("scheduled runs", len(sched), "max concurrent", out["max_concurrent_all_runs_by_interval"], flush=True)


def fc_active(r):
    return c.fc.run_is_active(r)


if __name__ == "__main__":
    main()
