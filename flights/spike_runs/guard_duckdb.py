"""Short guard run with the duckdb package: read one leader row, see a fresh heartbeat, exit 0.

P2 run-semantics spike (docs/spike/run_semantics.md, question 3). requirements: duckdb==1.5.6.
Config: CDC_V2_RUNS_DB (cdc_v2_* only), CDC_V2_GUARD_TABLE (default guard_leader),
CDC_V2_GUARD_DSN_OPTS (default 'attach_mode=single'; 'none' connects to plain md: and attaches every database).
CDC_V2_GUARD_FAST_EXIT=1 ends with os._exit(0). Prints `FIRST <unix>` first and `GUARD {json}` last. The token comes from MOTHERDUCK_TOKEN and is never printed.
"""
import time

T0 = time.time()
print("FIRST %.3f" % T0, flush=True)

import json  # noqa: E402
import os  # noqa: E402


def main():
    db = os.environ.get("CDC_V2_RUNS_DB", "cdc_v2_spike_runs_db")
    if not db.startswith("cdc_v2_"):
        raise SystemExit("database must be cdc_v2_*")
    table = os.environ.get("CDC_V2_GUARD_TABLE", "guard_leader")
    opts = os.environ.get("CDC_V2_GUARD_DSN_OPTS", "attach_mode=single")
    out = {"variant": "duckdb", "dsn_opts": opts, "run": os.environ.get("MOTHERDUCK_FLIGHT_RUN_NUMBER")}
    t = time.time()
    import duckdb
    out["import_s"] = round(time.time() - t, 3)
    t = time.time()
    if opts == "none":
        con = duckdb.connect("md:")
        sql_table = "%s.main.%s" % (db, table)
    else:
        con = duckdb.connect("md:%s?%s" % (db, opts))
        sql_table = table
    out["connect_s"] = round(time.time() - t, 3)
    t = time.time()
    row = con.execute("SELECT run_number, epoch, state, date_diff('millisecond', heartbeat_at, now()) FROM %s "
                      "WHERE id = 1" % sql_table).fetchone()
    out["query_s"] = round(time.time() - t, 3)
    con.close()
    if row:
        out.update(leader_run=row[0], epoch=row[1], state=row[2], heartbeat_age_ms=int(row[3]))
        out["decision"] = "exit_healthy" if row[2] == "leading" and row[3] < 30000 else "would_take_over"
    else:
        out["decision"] = "no_leader_row"
    out["total_in_code_s"] = round(time.time() - T0, 3)
    print("GUARD " + json.dumps(out), flush=True)
    if os.environ.get("CDC_V2_GUARD_FAST_EXIT") == "1":
        os._exit(0)  # skip interpreter teardown


if __name__ == "__main__":
    main()
