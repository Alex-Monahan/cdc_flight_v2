"""The self-test of the fuzzers: every wrong variant of the toy must be found by the fuzzer the task names, within a
stated bound, for the right reason (the signal that found it), and the correct toy must pass the same runs.
`tools/fuzz variants` writes the table to evidence/p3/fuzzers/variants.json (found by which fuzzer and which signal,
after how many seeds and seconds). A planted bug that a fuzzer does not find within the bound is a defect of the
fuzzer: fix the fuzzer, not the bound.

Signals: `oracle` (the positioned comparison), `consumer loop (<check>)`, `writer log` (two writers),
`system self-report` (the system's own ApplyInvariantError, plan C3a), `catch-up deadline` (stuck).
"""
from __future__ import annotations

import threading
import time
from pathlib import Path

from cdc_v2.testing.harness_evidence import write_evidence

from .campaign import Campaign

PROCESS = ["kill_point", "kill_point_later", "kill_group"]
# row -> what runs and what must find it
TABLE = {
    "ack_early": dict(variant="ack_early", fuzzer="fault", kinds=PROCESS,
                      expect="fault fuzzer (process kills): a lost change"),
    "ack_early_oracle": dict(variant="ack_early", fuzzer="fault", kinds=PROCESS, ignore_self_report=True,
                             oracle_only=True, signal="oracle",
                             expect="the same, with the toy's self-report ignored and no consumer loop: the oracle"),
    "position_apart": dict(variant="position_apart", fuzzer="fault", kinds=PROCESS,
                           expect="fault fuzzer (process kills): a change applied twice"),
    "position_apart_oracle": dict(variant="position_apart", fuzzer="fault", kinds=PROCESS, ignore_self_report=True,
                                  oracle_only=True, signal="oracle",
                                  expect="the same, with the toy's self-report ignored and no consumer loop: the "
                                         "oracle"),
    "split": dict(variant="split", fuzzer="workload", signal="consumer loop",
                  expect="the consumer loops (the end-state oracle cannot see it)"),
    "reorder": dict(variant="reorder", fuzzer="workload", expect="workload fuzzer"),
    "no_fence": dict(variant="no_fence", fuzzer="leadership", rounds=4, signal="writer log",
                     expect="leadership fuzzer: two writers"),
    "skip_on_reconnect": dict(variant="skip_on_reconnect", fuzzer="fault",
                              kinds=["net_reset", "net_drop", "net_blackhole", "src_terminate"],
                              expect="fault fuzzer (network and source backends)"),
    "swallow": dict(variant="swallow", fuzzer="workload",
                    expect="workload fuzzer (through the toy's simulated destination conflicts)"),
    "swallow_dest_errors": dict(variant="swallow", fuzzer="fault", kinds=["dest_error"],
                                options={"toy": {"conflict_rate": 0.0}},
                                expect="fault fuzzer, destination errors only, no simulated conflicts"),
    "trust_frames": dict(variant="trust_frames", fuzzer="stream", expect="stream fuzzer"),
    "keyless_toast_null": dict(variant="keyless_toast_null", fuzzer="workload",
                               expect="workload fuzzer: a keyless table with an unchanged TOASTed column"),
    "deferrable_by_key": dict(variant="deferrable_by_key", fuzzer="workload",
                              expect="workload fuzzer: a deferrable key with FULL applied by its key (rule K)"),
}
FINDINGS = ("diverged", "violation", "stuck")          # an `error` is not the fuzzer finding the bug
BOUND_SEEDS = {"fault": 6, "workload": 4, "stream": 4, "leadership": 3}


def signal_of(rec: dict) -> str:
    reason = rec.get("reason") or ""
    if rec["outcome"] == "violation" and reason.startswith("consumer loop"):
        return "consumer loop (%s)" % reason.split(":", 1)[1].split()[0]
    if rec["outcome"] == "violation" and "writer log" in reason:
        return "writer log"
    if rec["outcome"] == "violation":
        return "leadership invariant"
    if rec["outcome"] == "diverged" and reason.startswith("oracle"):
        return "oracle"
    if rec["outcome"] == "diverged" and "the system reports" in reason:
        return "system self-report"
    if rec["outcome"] == "stuck":
        return "catch-up deadline"
    return rec["outcome"]


def _campaign(row, system, seeds, source_node, budget, log, extra, stop_on_failure):
    c = Campaign(row["fuzzer"], system, source_node, seeds=None, workers=1, budget_s=budget,
                 rounds=row.get("rounds", 10), stop_on_failure=stop_on_failure, log=log, kinds=row.get("kinds"),
                 options=row.get("options"), ignore_self_report=row.get("ignore_self_report", False),
                 observers=not row.get("oracle_only", False), **extra)
    c.seeds = seeds if seeds is not None else _next_free(c, BOUND_SEEDS[row["fuzzer"]])
    return c


_CLAIMED = set()
_CLAIM_LOCK = threading.Lock()


def run_row(name, source_node="replica", log=print, budget=None, correct=True, ledger_root=None,
            seeds=None) -> dict:
    row = TABLE[name]
    extra = {"ledger_root": ledger_root} if ledger_root else {}
    t = time.monotonic()
    c = _campaign(row, "toy:" + row["variant"], seeds, source_node, budget, log, extra, True)
    res = c.run()
    want = row.get("signal")
    found = [r for r in res if r["outcome"] in FINDINGS and (not want or signal_of(r).startswith(want))]
    other = [r for r in res if r["outcome"] in FINDINGS and r not in found]
    first = found[0] if found else None
    upto = res[:res.index(first) + 1] if first else res
    out = {"row": name, "variant": row["variant"], "fuzzer": row["fuzzer"], "kinds": row.get("kinds"),
           "options": row.get("options"), "self_report_ignored": bool(row.get("ignore_self_report")),
           "consumer_loops": not row.get("oracle_only", False),
           "expected_finder": row["expect"], "required_signal": want, "bound_seeds": BOUND_SEEDS[row["fuzzer"]],
           "seeds_run": [r["seed"] for r in res], "found": bool(first),
           "signal": signal_of(first) if first else None,
           "other_findings": [{"seed": r["seed"], "signal": signal_of(r)} for r in other],
           "seeds_until_found": len(upto) if first else None,
           "seconds_until_found": round(sum(r["duration_s"] for r in upto), 1) if first else None,
           "first_failure_s_in_run": first.get("first_failure_s") if first else None,
           "outcome": first["outcome"] if first else None,
           "reason": (first["reason"] or "")[:400] if first else None,
           "failure_id": first.get("failure_id") if first else None, "commit": res[0].get("commit") if res else None}
    if correct:
        with _CLAIM_LOCK:                                      # two rows never run one seed of the correct toy
            mine = [s_ for s_ in out["seeds_run"] if (row["fuzzer"], s_) not in _CLAIMED]
            _CLAIMED.update((row["fuzzer"], s_) for s_ in mine)
        cc = _campaign(row, "toy:correct", mine, source_node, budget, log, extra, False)
        cres = cc.run() if mine else []
        if len(mine) < len(out["seeds_run"]):
            time.sleep(1)
            deadline = time.monotonic() + 1800                 # wait for the row that runs the other seeds
            from . import ledger as _l
            while time.monotonic() < deadline and not all(
                    any(r.get("system") == "toy-correct" and r["seed"] == s_ for r in _l.read_runs(cc.root,
                                                                                                  row["fuzzer"]))
                    for s_ in out["seeds_run"]):
                time.sleep(5)
        outcomes = {str(r["seed"]): r["outcome"] for r in cres}
        from . import ledger
        for r in ledger.read_runs(cc.root, row["fuzzer"]):         # seeds the correct toy had run already
            if r.get("system") == "toy-correct" and r.get("source_mode") == source_node \
                    and r["seed"] in out["seeds_run"] and str(r["seed"]) not in outcomes:
                outcomes[str(r["seed"])] = r["outcome"]
        out["correct_toy_same_seeds"] = outcomes
        out["correct_toy_passed"] = bool(outcomes) and all(o == "pass" for o in outcomes.values())
    out["wall_s"] = round(time.monotonic() - t, 1)
    log("variant row %s: found %s by %s" % (name, out["found"], out["signal"]))
    return out


def run_variants(names=None, source_node="replica", out=None, log=print, budget=None, correct=True,
                 ledger_root=None, merge=False, workers=2) -> dict:
    names = list(names or TABLE)
    rows, lock = {}, threading.Lock()
    todo = list(names)
    from . import ledger as _l
    root = Path(ledger_root) if ledger_root else __import__("cdc_v2.testing.fuzz.campaign",
                                                             fromlist=["LEDGER_ROOT"]).LEDGER_ROOT
    reserved, seeds_of = {}, {}
    for n in names:                                            # seeds per row, never shared by two rows
        row = TABLE[n]
        key = (row["fuzzer"], "toy-" + row["variant"])
        taken = reserved.setdefault(key, set(_l.held_seeds(root, row["fuzzer"], key[1], source_node)))
        picked, s_ = [], 1
        while len(picked) < BOUND_SEEDS[row["fuzzer"]]:
            if s_ not in taken:
                picked.append(s_)
                taken.add(s_)
            s_ += 1
        seeds_of[n] = picked

    def worker():
        while True:
            with lock:
                if not todo:
                    return
                name = todo.pop(0)
            r = run_row(name, source_node, log, budget, correct, ledger_root, seeds_of[name])
            with lock:
                rows[name] = r
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, min(2, workers)))]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    rows = [rows[n] for n in names if n in rows]
    doc = {"about": "Wrong variants of the toy replicator (tests/fuzz/toy/toy.py), the fuzzer and the signal that "
                    "found each, and the correct toy on the same seeds (docs/testing/fuzzers.md section 2).",
           "source_node": source_node, "variants": rows}
    if out and merge and Path(out).exists():
        import json
        old = json.loads(Path(out).read_text())
        new = {r["row"]: r for r in rows}
        doc["variants"] = [new.pop(r.get("row", r["variant"]), r) for r in old["variants"]] + list(new.values())
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
