#!/usr/bin/env python3
"""Clean-up of the run-semantics spike: every cdc_v2_spike_runs* Flight is unscheduled, its active runs cancelled,
then deleted; cdc_v2_spike_runs* Flight secrets and the database cdc_v2_spike_runs_db are dropped. Lists what is
left (names starting with cdc_v2_spike_runs only). Evidence: cleanup.json.

    tools/with-env .venv/bin/python tools/spike/run_semantics_cleanup.py
"""
import time

import duckdb

import run_semantics_common as c


def main():
    out = {"at": c.now().isoformat(), "flights": [], "secrets": [], "database": None}
    for f in c.ctl.list_flights(c.PREFIX):
        out["flights"].append(c.cleanup_flight(f["flight_name"]))
    for s in c.ctl.list_secrets():
        if s["name"].startswith(c.PREFIX):
            out["secrets"].append(c.ctl.drop_secret(s["name"]))
    if c._db is not None:
        c._db.close()
        c._db = None
    time.sleep(2)
    con = duckdb.connect("md:")
    con.execute("DROP DATABASE IF EXISTS %s" % c.DB)
    out["database"] = {"dropped": c.DB}
    out["left_flights"] = [{"flight_name": f["flight_name"], "schedule_status": f.get("schedule_status")}
                           for f in c.ctl.list_flights(c.PREFIX)]
    out["left_secrets"] = [s["name"] for s in c.ctl.list_secrets() if s["name"].startswith(c.PREFIX)]
    out["left_databases"] = [r[0] for r in con.execute(
        "SELECT database_name FROM duckdb_databases() WHERE starts_with(database_name, ?)", [c.PREFIX]).fetchall()]
    c.save("cleanup", out)
    print("left:", out["left_flights"], out["left_secrets"], out["left_databases"], flush=True)


if __name__ == "__main__":
    main()
