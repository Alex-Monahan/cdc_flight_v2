"""The self-test of the fuzzers: every wrong variant of the toy must be found by the fuzzer the task names, within a
stated bound, and the correct toy must pass the same runs. `tools/fuzz variants` writes the table to
evidence/p3/fuzzers/variants.json (found by, after how many seeds and seconds). A planted bug that a fuzzer does not
find within the bound is a defect of the fuzzer: fix the fuzzer, not the bound.
"""
from __future__ import annotations

import time
from pathlib import Path

from cdc_v2.testing.harness_evidence import write_evidence

from .campaign import Campaign
from .run import FAILURES

# variant -> (fuzzer, campaign keyword arguments, what must find it)
TABLE = {
    "ack_early": ("fault", {"kinds": ["kill_point", "kill_point_later", "kill_group"]},
                  "fault fuzzer (process kills): a lost change"),
    "position_apart": ("fault", {"kinds": ["kill_point", "kill_point_later", "kill_group"]},
                       "fault fuzzer (process kills): a change applied twice"),
    "split": ("workload", {}, "the consumer loops (the end-state oracle cannot see it)"),
    "reorder": ("workload", {}, "workload fuzzer"),
    "no_fence": ("leadership", {"rounds": 4}, "leadership fuzzer: two writers"),
    "skip_on_reconnect": ("fault", {"kinds": ["net_reset", "net_drop", "net_blackhole", "src_terminate"]},
                          "fault fuzzer (network and source backends)"),
    "swallow": ("workload", {}, "workload fuzzer"),
    "trust_frames": ("stream", {}, "stream fuzzer"),
}
FINDINGS = ("diverged", "violation", "stuck")          # an `error` is not the fuzzer finding the bug
BOUND_SEEDS = {"fault": 6, "workload": 4, "stream": 4, "leadership": 3}


def run_variants(names=None, source_node="replica", out=None, log=print, budget=None, correct=True,
                 ledger_root=None, merge=False) -> dict:
    extra = {"ledger_root": ledger_root} if ledger_root else {}
    rows = []
    for v in names or list(TABLE):
        fuzzer, kw, expect = TABLE[v]
        kw = dict(kw)
        rounds = kw.pop("rounds", 10)
        bound = BOUND_SEEDS[fuzzer]
        t = time.monotonic()
        c = Campaign(fuzzer, "toy:" + v, source_node, seeds=None, hours=None, workers=1, budget_s=budget,
                     rounds=rounds, stop_on_failure=True, log=log, **kw, **extra)
        c.seeds = _next_free(c, bound)
        res = c.run()
        found = [r for r in res if r["outcome"] in FINDINGS]
        row = {"variant": v, "fuzzer": fuzzer, "expected_finder": expect, "bound_seeds": bound,
               "seeds_run": [r["seed"] for r in res], "found": bool(found),
               "seeds_until_found": (res.index(found[0]) + 1) if found else None,
               "seconds_until_found": round(sum(r["duration_s"] for r in res[:res.index(found[0]) + 1]), 1)
               if found else None,
               "first_failure_s_in_run": found[0].get("first_failure_s") if found else None,
               "outcome": found[0]["outcome"] if found else None,
               "reason": (found[0]["reason"] or "")[:400] if found else None,
               "failure_id": found[0].get("failure_id") if found else None,
               "wall_s": round(time.monotonic() - t, 1)}
        if correct:
            cc = Campaign(fuzzer, "toy:correct", source_node, seeds=row["seeds_run"], workers=1, budget_s=budget,
                          rounds=rounds, log=log, **kw, **extra)
            cc.seeds = row["seeds_run"]
            cres = cc.run()
            outcomes = {str(r["seed"]): r["outcome"] for r in cres}
            from . import ledger
            for r in ledger.read_runs(cc.root, fuzzer):         # seeds the correct toy had run already
                if r.get("system") == "toy-correct" and r.get("source_mode") == source_node \
                        and r["seed"] in row["seeds_run"] and str(r["seed"]) not in outcomes:
                    outcomes[str(r["seed"])] = r["outcome"]
            row["correct_toy_same_seeds"] = outcomes
            row["correct_toy_passed"] = bool(outcomes) and all(o == "pass" for o in outcomes.values())
        rows.append(row)
        log("variant %s: %s" % (v, row))
    doc = {"about": "Wrong variants of the toy replicator (tests/fuzz/toy/toy.py), the fuzzer that must find each, "
                    "and the correct toy on the same seeds (docs/testing/fuzzers.md section 2).",
           "source_node": source_node, "variants": rows,
           "all_found": all(r["found"] for r in rows),
           "correct_toy_passed": all(r.get("correct_toy_passed", True) for r in rows)}
    if out and merge and Path(out).exists():
        import json
        old = json.loads(Path(out).read_text())
        new = {r["variant"]: r for r in rows}
        doc["variants"] = [new.pop(r["variant"], r) for r in old["variants"]] + list(new.values())
        doc["all_found"] = all(r["found"] for r in doc["variants"])
        doc["correct_toy_passed"] = all(r.get("correct_toy_passed", True) for r in doc["variants"])
    if out:
        write_evidence(Path(out), doc)
    return doc


def _next_free(c, n):
    from . import ledger
    held = ledger.held_seeds(c.root, c.fuzzer, c._system_label(), c.source_node)
    out, s = [], 1
    while len(out) < n:
        if s not in held:
            out.append(s)
        s += 1
    return out
