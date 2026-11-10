#!/usr/bin/env python3
"""Finding 0: where is now() evaluated? Through the duckdb client and through MotherDuck's Postgres endpoint.

    tools/with-env .venv/bin/python tools/spike/run_semantics_clock.py

Creates the database cdc_v2_spike_runs_clock, inserts rows whose timestamp comes from now() / DEFAULT now() /
get_current_timestamp() through the duckdb client and now() through the Postgres endpoint (stdlib client from
flights/spike_runs/guard_pgwire.py), compares each with this machine's clock taken just before the statement,
then drops the database. Evidence: clock_probe_local.json.
"""
import importlib.util
import os
import time

import duckdb

import run_semantics_common as c

DB = "cdc_v2_spike_runs_clock"


def main():
    spec = importlib.util.spec_from_file_location("guard_pgwire", os.path.join(c.SRC_DIR, "guard_pgwire.py"))
    pg_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pg_mod)
    root = duckdb.connect("md:")
    root.execute("CREATE DATABASE IF NOT EXISTS %s" % DB)
    con = duckdb.connect("md:%s?attach_mode=single" % DB)
    con.execute("CREATE OR REPLACE TABLE clockprobe (k VARCHAR, c0 DOUBLE, ts TIMESTAMPTZ DEFAULT now())")
    rtt = {}
    stmts = [("duckdb_values_now", "INSERT INTO clockprobe VALUES ('duckdb_values_now', ?, now())"),
             ("duckdb_default_now", "INSERT INTO clockprobe (k, c0) VALUES ('duckdb_default_now', ?)"),
             ("duckdb_get_current_timestamp",
              "INSERT INTO clockprobe VALUES ('duckdb_get_current_timestamp', ?, get_current_timestamp())")]
    for k, sql in stmts:
        for _ in range(10):
            t0 = time.time()
            con.execute(sql, [t0])
            rtt.setdefault(k, []).append(time.time() - t0)
    pg = pg_mod.PgWire("pg.us-east-1-aws.motherduck.com", 5432, "postgres", os.environ["MOTHERDUCK_TOKEN"], DB,
                       options="--attach_mode=single")
    for _ in range(10):
        t0 = time.time()
        pg.query("INSERT INTO clockprobe VALUES ('pgwire_now', %r, now())" % t0)
        rtt.setdefault("pgwire_now", []).append(time.time() - t0)
    pg.close()
    rows = con.execute("SELECT k, epoch_ms(ts) / 1000.0 - c0 FROM clockprobe").fetchall()
    out = {"database": DB, "methods": {}}
    for k in rtt:
        out["methods"][k] = {"stored_minus_client_clock_s": c.dist(r[1] for r in rows if r[0] == k),
                             "statement_round_trip_s": c.dist(rtt[k])}
    con.close()
    root.execute("DROP DATABASE %s" % DB)
    out["dropped"] = True
    out["left"] = [r[0] for r in root.execute(
        "SELECT database_name FROM duckdb_databases() WHERE database_name = ?", [DB]).fetchall()]
    c.save("clock_probe_local", out)
    for k, v in out["methods"].items():
        print(k, v["stored_minus_client_clock_s"]["median"], v["statement_round_trip_s"]["median"])


if __name__ == "__main__":
    main()
