"""Fix round item 3: what the check "keys whose first record is a create have no destination row" costs when no
other statement would see those keys (an insert-only change set). Local DuckDB, a table of 10 M rows, change sets
of 10,000 inserts; increasing BIGINT keys and random UUID keys; the check on and off.

    uv run python tests/apply/apply_checkcost.py [--rows 10000000] [--reps 5]
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))

import duckdb  # noqa: E402

from apply_harness import spill_dir  # noqa: E402
from apply_standin import StandInTx  # noqa: E402
from cdc_v2.apply import ApplySettings, Column, LocalChangeSet, TableMeta, Target, apply_group  # noqa: E402
from cdc_v2.testing.harness_evidence import write_evidence  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--batch", type=int, default=10_000)
    ap.add_argument("--reps", type=int, default=5)
    args = ap.parse_args()
    out = {"rows": args.rows, "batch": args.batch, "reps": args.reps, "runs": []}
    for kind, typ, gen in [("increasing", "BIGINT", "CAST(i AS BIGINT)"), ("random_uuid", "UUID", "uuid()")]:
        meta = TableMeta((Column(0, "id", typ, key=True), Column(1, "v", "VARCHAR")))
        conn = duckdb.connect()
        conn.execute("SET TimeZone = 'UTC'")
        conn.execute("CREATE SCHEMA s")
        conn.execute("CREATE TABLE s.t AS SELECT %s AS id, 'value ' || i AS v FROM range(%d) t(i)" % (gen, args.rows))
        mk = duckdb.connect()
        mk.execute("SET arrow_lossless_conversion = true")
        for check in (False, True):
            times = []
            for r in range(args.reps):
                lo = args.rows + (2 * r + (1 if check else 0)) * args.batch
                key = "CAST(i AS BIGINT)" if kind == "increasing" else "uuid()"
                rows = mk.execute("SELECT 0 AS txn, i AS seq, 'c' AS op, %s AS a0, 'new ' || i AS a1, "
                                  "CAST(NULL AS %s) AS k0 FROM range(%d, %d) t(i)"
                                  % (key, typ, lo, lo + args.batch)).to_arrow_table()
                settings = ApplySettings(local_temp_directory=spill_dir(), check_new_keys=check)
                t0 = time.monotonic()
                with StandInTx(conn) as tx:
                    counts = apply_group(tx, Target("s", "t"), LocalChangeSet(rows, None, meta), settings=settings)
                times.append(time.monotonic() - t0)
            out["runs"].append({"keys": kind, "check": check, "seconds_median": round(statistics.median(times), 4),
                                "seconds_all": [round(x, 4) for x in times], "shapes": counts.shapes})
        conn.close()
    out["command"] = "uv run python tests/apply/apply_checkcost.py --rows %d --batch %d --reps %d" % (
        args.rows, args.batch, args.reps)
    out["note"] = "local DuckDB in memory on this laptop; other build workers may have been running"
    write_evidence(HERE.parents[1] / "evidence" / "p3" / "apply" / "check_new_keys_cost.json", out)
    for r in out["runs"]:
        print(r["keys"], "check" if r["check"] else "no check", r["seconds_median"], r["shapes"])


if __name__ == "__main__":
    main()
