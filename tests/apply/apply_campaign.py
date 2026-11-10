"""Opt-in campaign for package A: the three property scenarios over consecutive seeds for a time budget, with a
ledger (seed, counts, outcome) written to evidence/p3/apply/campaign_<target>.json. Also writes the statement
shapes of a sample table (evidence/p3/apply/shapes.json).

    uv run python tests/apply/apply_campaign.py --seconds 600                       # local DuckDB
    CDC_V2_MD_PREFIX=cdc_v2_t_a_ tools/with-env uv run python tests/apply/apply_campaign.py --target md --seconds 300

A failing seed is written to the ledger with its message and the campaign goes on; the exit status is 1 if any
seed failed. Reproduce one with apply_harness.stream_scenario / catch_up_scenario and the seed.
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))

from apply_harness import catch_up_scenario, local_conn, stream_scenario  # noqa: E402
from cdc_v2.testing.harness_evidence import write_evidence  # noqa: E402

EVIDENCE = HERE.parents[1] / "evidence" / "p3" / "apply"


def shapes_evidence():
    """The SQL of every statement shape for a sample table: a key, a column that can be unchanged of each kind
    that matters (text, interval, struct with a double, list), a double and an interval that cannot."""
    from apply_sim import KeyedSource, change_set_table, create_table, random_meta
    from apply_standin import StandInTx
    from cdc_v2.apply import Column, LocalChangeSet, TableMeta, Target, apply_group
    from cdc_v2.apply.statements import Shapes
    meta = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "big", "VARCHAR", unchanged_possible=True),
                      Column(2, "iv", "INTERVAL", unchanged_possible=True),
                      Column(3, "st", "STRUCT(a DOUBLE, b VARCHAR)", unchanged_possible=True),
                      Column(4, "l", "INTEGER[]", unchanged_possible=True), Column(5, "d", "DOUBLE"),
                      Column(6, "w", "INTERVAL")))
    kinds = {0: "bigint", 1: "varchar", 2: "interval", 3: "struct", 4: "intlist", 5: "double", 6: "interval"}
    seen = {}
    sets = 0
    for seed in range(40):
        rng = random.Random(seed)
        src = KeyedSource(meta, rng, kinds, toast_rate=0.7)
        src.seed_rows(8)
        conn = local_conn()
        create_table(conn, "s", "t", meta, src.snapshot())
        for _ in range(3):
            recs = src.ops_txn(rng.randint(1, 6))
            with StandInTx(conn) as tx:
                counts = apply_group(tx, Target("s", "t"), LocalChangeSet(change_set_table(meta, recs), None, meta))
            sets += 1
            texts = [s for s in tx.sent if not s.startswith("SELECT")]
            for shape, sql in zip([x for x in counts.shapes if x not in ("plan", "lookup")], texts):
                seen.setdefault(shape, set()).add(sql)
    sh = Shapes(meta, Target("s", "t"), list(meta.unchanged_columns))
    write_evidence(EVIDENCE / "shapes.json", {
        "table": [{"position": c.position, "name": c.name, "type": c.type, "key": c.key,
                   "unchanged_possible": c.unchanged_possible} for c in meta.columns],
        "change_sets": sets, "shape_bound": sh.shape_bound(), "shapes_used": len(seen),
        "one_text_per_shape": all(len(v) == 1 for v in seen.values()),
        "shapes": {k: sorted(v)[0] for k, v in sorted(seen.items())}})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=["local", "md"], default="local")
    ap.add_argument("--seconds", type=float, default=300)
    ap.add_argument("--start", type=int, default=10_000_000)
    ap.add_argument("--no-shapes", action="store_true")
    args = ap.parse_args()
    if not args.no_shapes and args.target == "local":
        shapes_evidence()
    if args.target == "md":
        from cdc_v2.testing.md import TestDestination
        dest = TestDestination()
        conn_of = lambda: dest.conn
    else:
        dest = None
        conn_of = local_conn
    t_end = time.monotonic() + args.seconds
    entries, failures, seed, n = [], 0, args.start, 0
    t0 = time.monotonic()
    try:
        while time.monotonic() < t_end:
            kind = seed % 3
            form = "relation" if seed % 4 == 3 else "local"
            table = "t%d" % n
            try:
                if kind == 0:
                    e = stream_scenario(conn_of(), seed, keyed=True, form=form, table=table)
                elif kind == 1:
                    e = stream_scenario(conn_of(), seed, keyed=False, form=form, table=table)
                else:
                    e = catch_up_scenario(conn_of(), seed, form=form, table=table)
            except Exception as ex:                             # noqa: BLE001 - a failure is a result
                failures += 1
                e = {"seed": seed, "form": form, "kind": ["keyed", "keyless", "catch_up"][kind], "ok": False,
                     "error": "%s: %s" % (type(ex).__name__, str(ex).split("\n")[0][:300]),
                     "where": traceback.format_exc().strip().split("\n")[-3:]}
            entries.append(e)
            seed += 1
            n += 1
    finally:
        if dest is not None:
            dest.drop()
    write_evidence(EVIDENCE / ("campaign_%s.json" % args.target), {
        "target": args.target, "seconds": round(time.monotonic() - t0, 1), "seeds": len(entries),
        "first_seed": args.start, "failures": failures,
        "keyed_stream": sum(1 for e in entries if e.get("mode") == "stream" and e.get("keyed")),
        "keyless_stream": sum(1 for e in entries if e.get("mode") == "stream" and not e.get("keyed")),
        "catch_up": sum(1 for e in entries if e.get("mode") == "catch_up"),
        "records": sum(e.get("records", 0) for e in entries),
        "catch_up_with_unresolved_keys": sum(1 for e in entries if e.get("unresolved")),
        "command": "uv run python tests/apply/apply_campaign.py --target %s --seconds %d --start %d"
                   % (args.target, args.seconds, args.start),
        "concurrent_load": os.environ.get("CDC_V2_CAMPAIGN_NOTE", "other build workers may have been running"),
        "ledger": entries})
    print("seeds %d failures %d" % (len(entries), failures))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
