#!/usr/bin/env python3
"""Q2 supplement: is the next scheduled run visible before it starts?

    tools/with-env .venv/bin/python tools/spike/run_semantics_pending.py [flight] [--minutes 2]

While FLIGHT (default cdc_v2_spike_runs_guard_pg) is scheduled every minute, compare from second 40 of a minute
on: MD_LIST_FLIGHT_RUNS (limit 1000) against MD_GET_FLIGHT_RUN of the next run number. Evidence:
pending_record_probe.json.
"""
import argparse
import time

import run_semantics_common as c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("flight", nargs="?", default="cdc_v2_spike_runs_guard_pg")
    ap.add_argument("--minutes", type=int, default=2)
    a = ap.parse_args()
    fid = c.ctl.find_flight(a.flight)["flight_id"]
    obs = []
    for _ in range(a.minutes):
        while int(time.time()) % 60 != 40:
            time.sleep(0.2)
        last = max(r["run_number"] for r in c.ctl.query(c.fc.build_list_runs_sql(fid, 1000)))
        for _ in range(12):
            at = c.now().isoformat()
            listed = [(r["run_number"], r["state"]) for r in c.ctl.query(c.fc.build_list_runs_sql(fid, 1000))]
            try:
                g = c.ctl.query(c.fc.build_get_run_sql(fid, last + 1))
                got = [{"run_number": r["run_number"], "state": r["state"], "created_at": r["created_at"],
                        "scheduled_at": r["scheduled_at"], "is_scheduled": r["is_scheduled"]} for r in g]
            except c.fc.FlightCtlError as exc:
                got = "error: " + str(exc)[:200]
            obs.append({"at": at, "next_run_number": last + 1,
                        "next_run_in_list": any(n == last + 1 for n, _ in listed),
                        "list_first_two": listed[:2], "md_get_flight_run_next": got})
            print(obs[-1], flush=True)
            time.sleep(2)
    c.save("pending_record_probe", {"flight": a.flight, "observations": obs})


if __name__ == "__main__":
    main()
