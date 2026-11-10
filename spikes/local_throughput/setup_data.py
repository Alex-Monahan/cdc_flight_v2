"""Load TPC-H lineitem into the spike's private primary; wait until the replica has replayed it.

    tools/with-env uv run --group spike python spikes/local_throughput/setup_data.py [--sf 2] [--force]

Starts the cluster (spikes/local_throughput/localpg up), generates lineitem with DuckDB's tpch extension, creates
public.lineitem on the primary with Postgres-native types and primary key (l_orderkey, l_linenumber), loads it through
DuckDB's Postgres writer (binary COPY), runs VACUUM ANALYZE, grants SELECT to the runtime role and waits for replay.
A table that already holds the expected row count is kept unless --force is given.
"""
from __future__ import annotations

import argparse
import subprocess
import time

import duckdb

import common as C


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=float, default=2.0)
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    subprocess.run([str(C.LOCALPG), "up"], check=True, timeout=900)
    ev = {"started_utc": C.utc(), "sf": a.sf, "load_avg_start": C.load_avg()}

    with C.pg_connect("primary", "admin") as p:
        have = p.execute("select to_regclass('public.lineitem') is not null").fetchone()[0]
        n = p.execute("select count(*) from public.lineitem").fetchone()[0] if have else 0
    expected = int(round(6_001_215 * a.sf, -4))
    if have and abs(n - expected) < expected * 0.01 and not a.force:
        print(f"lineitem already loaded ({n} rows); --force to reload")
    else:
        t0 = time.time()
        con = duckdb.connect(config=C.md_config("3GB"))
        con.execute("INSTALL tpch; LOAD tpch; INSTALL postgres; LOAD postgres")
        con.execute(f"CALL dbgen(sf := {a.sf})")
        for t in ("customer", "nation", "orders", "part", "partsupp", "region", "supplier"):
            con.execute(f"DROP TABLE {t}")
        ev["generate_s"] = round(time.time() - t0, 1)
        with C.pg_connect("primary", "admin") as p:
            p.execute("DROP TABLE IF EXISTS public.lineitem")
            p.execute(C.PG_DDL)
            p.execute("GRANT SELECT ON public.lineitem TO cdc_v2_runtime")
        t1 = time.time()
        con.execute(f"ATTACH '{C.dsn('primary', 'admin')}' AS pg (TYPE postgres)")
        cols = ", ".join(C.COLUMNS)
        con.execute(f"INSERT INTO pg.public.lineitem ({cols}) SELECT {cols} FROM lineitem ORDER BY l_orderkey, l_linenumber")
        con.close()
        ev["load_s"] = round(time.time() - t1, 1)
        t2 = time.time()
        with C.pg_connect("primary", "admin") as p:
            p.execute("VACUUM (ANALYZE) public.lineitem")
        ev["vacuum_analyze_s"] = round(time.time() - t2, 1)
    ev["replay_wait_s"] = round(C.wait_replica_caught_up(), 1)
    with C.pg_connect("replica", "runtime") as r:
        ev["replica"] = dict(zip(
            ["rows", "relpages", "heap_bytes", "total_bytes", "table_size_pretty", "server_version"],
            r.execute("""select (select count(*) from public.lineitem), relpages,
                                pg_relation_size('public.lineitem'), pg_total_relation_size('public.lineitem'),
                                pg_size_pretty(pg_relation_size('public.lineitem')), current_setting('server_version')
                         from pg_class where oid = 'public.lineitem'::regclass""").fetchone()))
        ev["avg_row_bytes_heap"] = round(ev["replica"]["heap_bytes"] / ev["replica"]["rows"], 1)
    ev["finished_utc"] = C.utc()
    print(ev)
    C.write_json("setup.json", ev)


if __name__ == "__main__":
    main()
