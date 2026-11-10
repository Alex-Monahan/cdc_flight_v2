"""What the stream checks cost (docs/design/apply.md 2.5): change sets of `--batch` inserts (increasing BIGINT keys
and random UUID keys) with `check_new_keys` off and on, and change sets of `--batch` updates (the per-key count
inside K3). Local DuckDB (10 M rows) or MotherDuck (`--target md`, a database of its own, dropped at the end).
`--src` runs another source tree of the package (for a before/after comparison).

    uv run python tests/apply/apply_checkcost.py [--rows 10000000] [--reps 5]
    CDC_V2_MD_PREFIX=cdc_v2_t_a_ tools/with-env uv run python tests/apply/apply_checkcost.py --target md \\
        --rows 1000000 --label after
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--batch", type=int, default=10_000)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--target", choices=["local", "md"], default="local")
    ap.add_argument("--src", default=str(HERE.parents[1] / "src"))
    ap.add_argument("--label", default="")
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    sys.path.insert(0, str(HERE))
    sys.path.insert(0, args.src)
    import duckdb
    from apply_harness import spill_dir
    from apply_standin import StandInTx
    from cdc_v2.apply import ApplySettings, Column, LocalChangeSet, TableMeta, Target, apply_group

    out = {"rows": args.rows, "batch": args.batch, "reps": args.reps, "target": args.target, "label": args.label,
           "runs": []}
    dest = None
    if args.target == "md":
        from cdc_v2.testing.md import TestDestination
        dest = TestDestination()
    try:
        cases = [("insert", "increasing", "BIGINT"), ("insert", "random_uuid", "UUID"), ("update", "increasing", "BIGINT")]
        for n, (what, kind, typ) in enumerate(cases):
            gen = "CAST(i AS BIGINT)" if typ == "BIGINT" else "uuid()"
            meta = TableMeta((Column(0, "id", typ, key=True), Column(1, "v", "VARCHAR"),
                              Column(2, "big", "VARCHAR", unchanged_possible=True)))
            if dest is None:
                conn = duckdb.connect()
                conn.execute("SET TimeZone = 'UTC'")
            else:
                conn = dest.conn
            conn.execute("CREATE SCHEMA IF NOT EXISTS s")
            table = "t%d" % n
            conn.execute("CREATE OR REPLACE TABLE s.%s AS SELECT %s AS id, 'value ' || i AS v, 'big' AS big FROM range(%d) t(i)"
                         % (table, gen, args.rows))
            mk = duckdb.connect()
            mk.execute("SET arrow_lossless_conversion = true")
            for check in ((False, True) if what == "insert" else (True,)):
                times, sent = [], []
                for r in range(args.reps):
                    if what == "insert":
                        lo = args.rows + (2 * r + (1 if check else 0)) * args.batch
                        key = "CAST(i AS BIGINT)" if kind == "increasing" else "uuid()"
                        rows = mk.execute("SELECT 0 AS txn, i AS seq, 'c' AS op, %s AS a0, 'new ' || i AS a1, 'b' AS a2, false AS u2, "
                                          "CAST(NULL AS %s) AS k0 FROM range(%d, %d) t(i)"
                                          % (key, typ, lo, lo + args.batch)).to_arrow_table()
                    else:
                        lo = r * args.batch * 3
                        rows = mk.execute("SELECT 0 AS txn, i AS seq, 'u' AS op, CAST(i AS BIGINT) AS a0, "
                                          "'upd %d ' || i AS a1, CAST(NULL AS VARCHAR) AS a2, true AS u2, CAST(NULL AS BIGINT) AS k0 FROM range(%d, %d, 3) t(i)"
                                          % (r, lo, lo + args.batch * 3)).to_arrow_table()
                    settings = ApplySettings(local_temp_directory=spill_dir(), check_new_keys=check)
                    t0 = time.monotonic()
                    with StandInTx(conn) as tx:
                        counts = apply_group(tx, Target("s", table), LocalChangeSet(rows, None, meta),
                                             settings=settings)
                    times.append(time.monotonic() - t0)
                    sent.append(counts.statements)
                out["runs"].append({"change_set": what, "keys": kind, "check": check,
                                    "seconds_median": round(statistics.median(times), 4),
                                    "seconds_all": [round(x, 4) for x in times], "round_trips": max(sent),
                                    "shapes": counts.shapes})
            mk.close()
            if dest is None:
                conn.close()
    finally:
        if dest is not None:
            dest.drop()
    out["command"] = "apply_checkcost.py --target %s --rows %d --batch %d --reps %d" % (
        args.target, args.rows, args.batch, args.reps)
    for r in out["runs"]:
        print(args.label, r["change_set"], r["keys"], "check" if r["check"] else "no check", r["seconds_median"],
              r["round_trips"], r["shapes"])
    if args.out:
        Path(args.out).write_text(json.dumps(out, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()
