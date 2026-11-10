#!/usr/bin/env python3
"""Q2 schedule timing and Q3 the short guard run (cost of the every-minute schedule).

    tools/with-env .venv/bin/python tools/spike/run_semantics_schedule.py variants [--repeat 5]
    tools/with-env .venv/bin/python tools/spike/run_semantics_schedule.py scheduled [--minutes 34]

Two guard Flights, both F16:
  cdc_v2_spike_runs_guard_duckdb  flights/spike_runs/guard_duckdb.py, requirements duckdb==1.5.6
  cdc_v2_spike_runs_guard_pg      flights/spike_runs/guard_pgwire.py, no requirements (stdlib Postgres wire client)
Each reads one row of guard_leader in cdc_v2_spike_runs_db; this driver keeps that row's heartbeat fresh (every 5 s)
so every guard sees a healthy leader and exits 0.

`variants` runs each variant --repeat times on demand (sequentially) and writes guard_variants.json.
`scheduled` sets '* * * * *' on both Flights for --minutes, polls the run lists every 2 s (first-seen state of each
run record), clears the schedules, collects every run's log and writes schedule_timing.json.
"""
import argparse
import threading
import time

import run_semantics_common as c

DUCK = "cdc_v2_spike_runs_guard_duckdb"
PG = "cdc_v2_spike_runs_guard_pg"
BASE = {"CDC_V2_RUNS_DB": c.DB, "CDC_V2_GUARD_TABLE": "guard_leader", "CDC_V2_GUARD_DSN_OPTS": "attach_mode=single",
        "CDC_V2_GUARD_FAST_EXIT": "0", "CDC_V2_RUNS_REGION": "us-east-1"}
STOP = threading.Event()


def heartbeat_loop():
    import duckdb
    con = duckdb.connect("md:%s?attach_mode=single" % c.DB)
    con.execute("INSERT OR REPLACE INTO guard_leader VALUES (1, 0, 1, now(), 'leading')")
    while not STOP.is_set():
        try:
            con.execute("UPDATE guard_leader SET heartbeat_at = now() WHERE id = 1")
        except Exception as exc:  # noqa: BLE001
            print("heartbeat error", type(exc).__name__, flush=True)
        STOP.wait(5)
    con.close()


def deploy(name, source, requirements):
    assert name.startswith(c.PREFIX)
    action, s = c.ctl.deploy(name, c.read_src(source), requirements_txt=requirements, instance_type="F16",
                             config=dict(BASE))
    print("deploy", name, action, s.get("current_version"), flush=True)


def collect(name, r):
    lines = c.logs(name, r["run_number"])
    p = c.parse_logs(lines)
    row = c.slim_run(r)
    row.update(c.run_timing(r, p["first_line_unix"]))
    row["guard"] = p["guard"]
    row["log_lines"] = len(lines)
    if not p["guard"]:
        row["log_tail"] = [ln.get("line") for ln in lines[-6:]]
    return row


def summarize(rows):
    dur = [r.get("started_to_ended_s") for r in rows]
    return {
        "runs": len(rows),
        "succeeded": sum(1 for r in rows if r["state"] == "SUCCEEDED"),
        "started_to_ended_s": c.dist(dur),
        "started_to_first_line_s": c.dist(r.get("started_to_first_line_s") for r in rows),
        "first_line_to_ended_s": c.dist(r.get("first_line_to_ended_s") for r in rows),
        "in_code_s": c.dist((r["guard"] or {}).get("total_in_code_s") for r in rows),
        "connect_s": c.dist((r["guard"] or {}).get("connect_s") for r in rows),
        "decisions": sorted({(r["guard"] or {}).get("decision", "none") for r in rows}),
        "cost_per_day_usd_if_billed_started_to_ended":
            round(c.dist(dur).get("mean", 0) * 1440 / 3600 * c.F16_USD_PER_HOUR, 3) if any(dur) else None,
    }


def step_variants(repeat):
    variants = [
        ("duckdb_single", DUCK, {}),
        ("duckdb_single_fast_exit", DUCK, {"CDC_V2_GUARD_FAST_EXIT": "1"}),
        ("duckdb_attach_all", DUCK, {"CDC_V2_GUARD_DSN_OPTS": "none"}),
        ("pgwire", PG, {}),
        ("pgwire_fast_exit", PG, {"CDC_V2_GUARD_FAST_EXIT": "1"}),
    ]
    out = {}
    for label, name, cfg in variants:
        rows = []
        for _ in range(repeat):
            r = c.run(name, **cfg)
            final = c.ctl.wait(name, r["run_number"], timeout=180, poll=0.5)
            time.sleep(1)
            rows.append(collect(name, final))
        out[label] = {"flight": name, "per_run_config": cfg, "summary": summarize(rows), "runs": rows}
        print(label, out[label]["summary"]["started_to_ended_s"], flush=True)
    c.save("guard_variants", {"note": "on-demand runs, one after another; F16; heartbeat kept fresh by the driver",
                              "variants": out})


def step_scheduled(minutes):
    names = (DUCK, PG)
    before = {n: {r["run_number"] for r in c.ctl.runs(n, 100)} for n in names}
    first_seen = {n: {} for n in names}
    transitions = {n: {} for n in names}
    set_at = {}
    for n in names:
        set_at[n] = c.now().isoformat()
        c.ctl.update(n, schedule_cron="* * * * *")
    end = time.time() + minutes * 60
    try:
        while time.time() < end:
            for n in names:
                at = c.now().isoformat()
                for r in c.ctl.runs(n, 10):
                    k = r["run_number"]
                    if k in before[n]:
                        continue
                    first_seen[n].setdefault(k, {"first_seen_at": at, "first_seen_state": r["state"],
                                                 "created_at": r.get("created_at"),
                                                 "scheduled_at": r.get("scheduled_at")})
                    tr = transitions[n].setdefault(k, [])
                    if not tr or tr[-1][0] != r["state"]:
                        tr.append((r["state"], at))
            time.sleep(2)
    finally:
        cleared = {n: c.ctl.unschedule(n).get("schedule_status") for n in names}
        cleared_at = c.now().isoformat()
    time.sleep(75)  # let a run planned for the last minute finish (and show whether one still fires)
    out = {"schedule_set_at": set_at, "schedule_cleared_at": cleared_at, "cleared_status": cleared,
           "observe_minutes": minutes, "flights": {}}
    for n in names:
        runs = sorted((r for r in c.ctl.runs(n, 100) if r["run_number"] not in before[n]),
                      key=lambda r: r["run_number"])
        rows = []
        for r in runs:
            if c.fc.run_is_active(r):
                r = c.ctl.wait(n, r["run_number"], timeout=120)
            row = collect(n, r)
            row.update(first_seen[n].get(r["run_number"], {}))
            row["observed_transitions"] = transitions[n].get(r["run_number"])
            fs = c.unix(row.get("first_seen_at"))
            sch = c.unix(r.get("scheduled_at"))
            if fs and sch:
                row["first_seen_minus_scheduled_s"] = round(fs - sch, 1)
            rows.append(row)
        sched = [r for r in rows if r["is_scheduled"]]
        minutes_seen = [r["scheduled_at"][:16] for r in sched]
        dup = sorted({m for m in minutes_seen if minutes_seen.count(m) > 1})
        expected = []
        if sched:
            t = c.unix(sched[0]["scheduled_at"])
            last = c.unix(sched[-1]["scheduled_at"])
            while t <= last + 1:
                expected.append(c.iso(t)[:16])
                t += 60
        missed = sorted(set(expected) - set(minutes_seen))
        out["flights"][n] = {
            "scheduled_runs": len(sched), "on_demand_runs": len(rows) - len(sched),
            "duplicated_minutes": dup, "missed_minutes": missed,
            "scheduled_to_started_s": c.dist(r.get("scheduled_to_started_s") for r in sched),
            "scheduled_to_first_line_s": c.dist(r.get("scheduled_to_first_line_s") for r in sched),
            "created_minus_scheduled_s": c.dist(r.get("created_minus_scheduled_s") for r in sched),
            "first_seen_states": sorted({r.get("first_seen_state") or "?" for r in sched}),
            "summary": summarize(sched),
            "runs": rows,
        }
        print(n, out["flights"][n]["summary"]["started_to_ended_s"], "missed", missed, "dup", dup, flush=True)
    c.save("schedule_timing", out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["variants", "scheduled"])
    ap.add_argument("--repeat", type=int, default=5)
    ap.add_argument("--minutes", type=float, default=34)
    a = ap.parse_args()
    c.setup_db()
    deploy(DUCK, "guard_duckdb.py", c.REQ_DUCKDB)
    deploy(PG, "guard_pgwire.py", "")
    hb = threading.Thread(target=heartbeat_loop, daemon=True)
    hb.start()
    time.sleep(2)
    try:
        if a.step == "variants":
            step_variants(a.repeat)
        else:
            step_scheduled(a.minutes)
    finally:
        STOP.set()
        hb.join(10)


if __name__ == "__main__":
    main()
