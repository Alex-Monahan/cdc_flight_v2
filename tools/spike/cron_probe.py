#!/usr/bin/env python3
"""P2: what the server itself accepts as schedule_cron (bypasses flightctl's client-side 5-field check), and what
clearing a schedule does. Each accepted schedule is cleared at once. Writes evidence/p2/flight_platform/cron_server.json.

    tools/with-env uv run python tools/spike/cron_probe.py
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))
from cdc_v2 import flightctl as fc  # noqa: E402

ctl = fc.FlightCtl()
NAME = "cdc_v2_spike_cron"
SRC = 'def main():\n    print("cron probe")\n\nif __name__ == "__main__":\n    main()\n'
CRONS = ("*/30 * * * * *", "0 0 1 1 * *", "@hourly", "@yearly", "0 0 31 2 *", "61 * * * *", "0 0 1 1 MON",
         "0 0 1 JAN *", "0 0 1 1 *")


def main():
    out = {"cases": []}
    fid = ctl.create(NAME, SRC, instance_type="F4")["flight_id"]

    def raw_update(cron):
        sql = "SELECT * FROM MD_UPDATE_FLIGHT(flight_id := %s, schedule_cron := %s)" % (
            fc.sql_literal(fc.parse_flight_id(fid)), fc.sql_literal(cron))
        r = ctl.query(sql)[0]
        return {"schedule_cron": r["schedule_cron"], "schedule_status": r["schedule_status"]}

    def clear(label):
        try:
            out[label] = {"error": None} | raw_update("")
        except fc.FlightCtlError as exc:
            out[label] = {"error": str(exc)[:300]}

    try:
        clear("clear_never_scheduled")
        for cron in CRONS:
            try:
                case = {"cron": cron, "accepted": True} | raw_update(cron)
                case["after_clear"] = raw_update("")
            except fc.FlightCtlError as exc:
                case = {"cron": cron, "accepted": False, "error": str(exc)[:300]}
            out["cases"].append(case)
            print(json.dumps(case), flush=True)
        clear("clear_already_cleared")
        out["runs"] = ctl.runs(NAME)
    finally:
        ctl.delete(NAME)
        out["left"] = ctl.list_flights(NAME)
    path = os.path.join(ROOT, "evidence", "p2", "flight_platform", "cron_server.json")
    with open(path, "w") as fh:
        json.dump(out, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(json.dumps({k: v for k, v in out.items() if k != "cases"}))


if __name__ == "__main__":
    main()
