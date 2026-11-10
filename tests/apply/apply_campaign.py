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

from apply_harness import (catch_up_scenario, cut_scenario, local_conn, rounds_scenario,  # noqa: E402
                           stream_scenario, test_settings)
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
                counts = apply_group(tx, Target("s", "t"), LocalChangeSet(change_set_table(meta, recs), None, meta),
                                     settings=test_settings())
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


KINDS = ["keyed", "keyless", "catch_up", "rounds", "cut", "keyed_form"]
#: keyed_form: composite keys with a part whose form can change (X1 of the second fix round)
FORM_OPS = ["form_and_move"] * 4 + ["move_form", "move", "insert", "update", "delete", "swap_via_temp"]


def run_one(kind, conn, seed, form, table):
    if kind == "keyed":
        return stream_scenario(conn, seed, keyed=True, form=form, table=table)
    if kind == "keyed_form":
        return stream_scenario(conn, seed, keyed=True, form=form, table=table, n_keys=2, ops=FORM_OPS,
                               key_kinds=["double", "interval", "int", "double"])
    if kind == "keyless":
        return stream_scenario(conn, seed, keyed=False, form=form, table=table)
    if kind == "catch_up":
        return catch_up_scenario(conn, seed, form=form, table=table)
    if kind == "rounds":
        return rounds_scenario(conn, seed, form=form, table=table, repeat=seed % 2 == 1)
    return cut_scenario(conn, seed, form=form, table=table)


def controls(limit):
    """Item 12: how many examples each wrong-on-purpose planner survives before a property catches it."""
    from apply_controls import caught_after, everything_unresolved, last_record_decides, plain_values
    out = {"last_record_decides (keyed stream)": caught_after(
        lambda s: stream_scenario(local_conn(), s, keyed=True, apply_fn=last_record_decides), 30_000_000, limit)}
    with plain_values():
        out["plain GROUP BY on values (keyless stream)"] = caught_after(
            lambda s: stream_scenario(local_conn(), s, keyed=False), 30_000_000, limit)
    out["everything unresolved (catch-up rounds, bound 7c)"] = caught_after(
        lambda s: rounds_scenario(local_conn(), s, apply_fn=everything_unresolved), 30_000_000, limit)
    return out


def code_version():
    import subprocess
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(HERE), capture_output=True, text=True,
                              timeout=10).stdout.strip()
    except Exception:                                           # noqa: BLE001
        return "unknown"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=["local", "md"], default="local")
    ap.add_argument("--seconds", type=float, default=300)
    ap.add_argument("--start", type=int, default=10_000_000)
    ap.add_argument("--no-shapes", action="store_true")
    ap.add_argument("--controls", type=int, default=0, help="run the negative controls with up to N examples each")
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
    counts = {k: 0 for k in KINDS}
    failed, seed, n, records, unresolved, rounds_records = [], args.start, 0, 0, 0, 0
    t0 = time.monotonic()
    try:
        while time.monotonic() < t_end:
            kind = KINDS[seed % len(KINDS)]
            form = "relation" if seed % 4 == 3 else "local"
            try:
                e = run_one(kind, conn_of(), seed, form, "t%d" % n)
                records += e.get("records", 0)
                if kind == "rounds":
                    unresolved += e["unresolved"]
                    rounds_records += e["records"]
            except Exception as ex:                             # noqa: BLE001 - a failure is a result
                failed.append({"seed": seed, "form": form, "kind": kind,
                               "error": "%s: %s" % (type(ex).__name__, str(ex).split("\n")[0][:300]),
                               "where": traceback.format_exc().strip().split("\n")[-3:],
                               "repeat": "apply_campaign.run_one(%r, local_conn(), %d, %r, 't')" % (kind, seed, form)})
            counts[kind] += 1
            seed += 1
            n += 1
    finally:
        if dest is not None:
            dest.drop()
    summary = {
        "target": args.target, "code_version": code_version(), "seconds": round(time.monotonic() - t0, 1),
        "seeds": n, "seed_range": [args.start, seed - 1], "scenarios": counts, "records": records,
        "failures": len(failed), "failed_seeds": failed,
        "rounds_unresolved_rate": round(unresolved / rounds_records, 4) if rounds_records else None,
        "command": "uv run python tests/apply/apply_campaign.py --target %s --seconds %d --start %d%s"
                   % (args.target, args.seconds, args.start, " --controls %d" % args.controls if args.controls else ""),
        "concurrent_load": os.environ.get("CDC_V2_CAMPAIGN_NOTE", "other build workers may have been running"),
        "note": "the scenario of a seed is KINDS[seed % 5]; form relation when seed % 4 == 3; passing seeds are "
                "counted, not listed (repeat any with run_one)"}
    if args.controls:
        summary["negative_controls_examples_to_catch"] = controls(args.controls)
    write_evidence(EVIDENCE / ("campaign_%s.json" % args.target), summary)
    print("seeds %d failures %d" % (n, len(failed)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
