"""`tools/fuzz` (docs/testing/fuzzers.md)."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from . import ledger, pool
from .campaign import LEDGER_ROOT, REPO, Campaign, parse_seeds


def _opts(pairs):
    out = {}
    for p in pairs or []:
        k, _, v = p.partition("=")
        try:
            val = json.loads(v)
        except ValueError:
            val = v
        cur = out
        parts = k.split(".")
        for part in parts[:-1]:
            cur = cur.setdefault(part, {})
        cur[parts[-1]] = val
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="tools/fuzz")
    ap.add_argument("--ledger-root", default=str(LEDGER_ROOT), help=argparse.SUPPRESS)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run a campaign")
    r.add_argument("--fuzzer", required=True, choices=("workload", "fault", "stream", "leadership"))
    r.add_argument("--system", required=True, help="toy or toy:<variant>")
    r.add_argument("--source-node", default="replica", choices=("replica", "primary"))
    g = r.add_mutually_exclusive_group()
    g.add_argument("--seeds", help="A..B or a,b,c (seeds the ledger holds are skipped)")
    g.add_argument("--hours", type=float)
    r.add_argument("--workers", type=int, default=1)
    r.add_argument("--budget", type=float, help="seconds of load per run")
    r.add_argument("--rounds", type=int, default=10, help="leadership: rounds per run")
    r.add_argument("--kinds", help="fault: only these kinds (comma separated)")
    r.add_argument("--reduce", action="store_true", help="reduce each failure to a regression test")
    r.add_argument("--option", action="append", help="system option key=value (JSON), e.g. toy.conflict_rate=0")
    r.add_argument("--keep", action="store_true", help="keep the work directory of passing runs")
    st = sub.add_parser("status", help="the ledger")
    st.add_argument("--write", action="store_true", help="write evidence/robustness/ledger.json")
    s = sub.add_parser("suite-1.7", help="the suite of SPEC 1.7")
    s.add_argument("--system", default="toy")
    s.add_argument("--source-node", default="replica", choices=("replica", "primary"))
    s.add_argument("--seeds", default="1,2,3")
    s.add_argument("--soak-minutes", type=float, default=15.0)
    s.add_argument("--min-soak-faults", type=int, default=200)
    s.add_argument("--workers", type=int, default=2)
    s.add_argument("--out", default=None)
    v = sub.add_parser("variants", help="the wrong variants of the toy and the fuzzer that must find each")
    v.add_argument("--only", help="comma separated variants")
    v.add_argument("--source-node", default="replica", choices=("replica", "primary"))
    v.add_argument("--out", default=str(REPO / "evidence" / "p3" / "fuzzers" / "variants.json"))
    o = sub.add_parser("stream-offline", help="the stream fuzzer against the engine's reader, offline")
    o.add_argument("--cases", type=int, default=100_000)
    o.add_argument("--seed", type=int, default=None, help="first case seed (default: after the ledger's)")
    o.add_argument("--negative-controls", action="store_true")
    o.add_argument("--out", default=None, help="also write the summary to this evidence file")
    rd = sub.add_parser("reduce", help="reduce a kept failure to a seeded regression test")
    rd.add_argument("failure_id")
    sub.add_parser("clean", help="remove what killed campaigns left behind (by their lock records only)")
    a = ap.parse_args(argv)
    root = Path(a.ledger_root)

    if a.cmd == "run":
        if a.workers > 2:
            print("at most 2 workers on this shared machine", file=sys.stderr)
            return 2
        c = Campaign(a.fuzzer, a.system, a.source_node, seeds=parse_seeds(a.seeds) if a.seeds else None,
                     hours=a.hours, workers=a.workers, budget_s=a.budget, rounds=a.rounds, options=_opts(a.option),
                     reduce_failures=a.reduce, keep_workdir=a.keep, ledger_root=root,
                     kinds=a.kinds.split(",") if a.kinds else None)
        if a.seeds is None and a.hours is None:
            c.seeds = None
            c.hours = None
            print("give --seeds or --hours", file=sys.stderr)
            return 2
        res = c.run()
        bad = [x for x in res if x["outcome"] not in ("pass", "infrastructure")]
        print(json.dumps({"runs": len(res), "outcomes": {o: sum(1 for x in res if x["outcome"] == o)
                                                         for o in sorted({x["outcome"] for x in res})}}))
        return 1 if bad or c.refused or c.worker_errors else 0
    if a.cmd == "status":
        data = ledger.write(root, REPO) if a.write else ledger.build(root, REPO)
        print(ledger.status_text(data))
        return 0
    if a.cmd == "suite-1.7":
        from .suite17 import run_suite
        out = Path(a.out) if a.out else REPO / "evidence" / "p3" / "fuzzers" / ("suite-1.7-" + a.system.replace(
            ":", "-") + "-" + a.source_node)
        res = run_suite(a.system, out, REPO, a.source_node, parse_seeds(a.seeds), soak_s=a.soak_minutes * 60,
                        min_soak_faults=a.min_soak_faults, workers=min(2, a.workers))
        print(json.dumps({"all_passed": res["all_passed"], "seconds": res["seconds"]}))
        return 0 if res["all_passed"] else 1
    if a.cmd == "variants":
        from .variants import run_variants
        res = run_variants(a.only.split(",") if a.only else None, a.source_node, a.out)
        print(json.dumps({"all_found": res["all_found"], "correct_toy_passed": res["correct_toy_passed"]}))
        return 0 if res["all_found"] and res["correct_toy_passed"] else 1
    if a.cmd == "stream-offline":
        from .run import git_state, utc_now
        from .stream_fuzzer import load_corpus, run_offline
        last = max([r.get("seeds", [r["seed"], r["seed"]])[1] for r in ledger.read_runs(root, "stream")
                    if r.get("system") == "engine-reader"] or [0])
        first = a.seed if a.seed is not None else last + 1
        t0 = time.monotonic()
        res = run_offline(range(first, first + a.cases))
        rec = {"fuzzer": "stream", "system": "engine-reader", "self_test": False, "tier": "offline",
               "source_mode": "offline", "seed": first, "seeds": [first, first + a.cases - 1], **git_state(REPO),
               "start": utc_now(), "duration_s": round(time.monotonic() - t0, 3),
               "outcome": "pass" if not res["failures"] else "diverged", "reason": "",
               "counts": {"cases": res["cases"]}, "faults": res["by_kind"], "no_effect": {},
               "detected": res["detected"], "undetected_by_class": res["undetected_by_class"],
               "failing_cases": res["failing_cases"], "corpus": res["corpus"]}
        if a.negative_controls:
            from .stream_fuzzer import TrustingAssembler
            neg = run_offline(range(first, first + min(a.cases, 2000)), assembler_cls=TrustingAssembler,
                              decoder="lenient")
            rec["negative_control"] = {"cases": neg["cases"], "failures_found": neg["failures"]}
        ledger.append_run(root, rec)
        if a.out:
            from cdc_v2.testing.harness_evidence import write_evidence
            write_evidence(Path(a.out), dict(rec, about="The stream fuzzer against package E's frame decoder and the "
                                                        "reference assembler, offline (docs/testing/fuzzers.md §5).",
                                             command="tools/fuzz stream-offline --cases %d --seed %d%s" % (
                                                 a.cases, first, " --negative-controls" if a.negative_controls
                                                 else "")))
        print(json.dumps({k: rec[k] for k in ("outcome", "counts", "duration_s", "undetected_by_class")}))
        return 0 if rec["outcome"] == "pass" else 1
    if a.cmd == "reduce":
        from . import reduce as red
        fid = a.failure_id
        fz = fid.split("-")[0]
        fdir = root / fz / "failures" / fid
        meta = json.loads((fdir / "failure.json").read_text())
        rec = json.loads((fdir / "bundle" / "record.json").read_text())
        src = Path(meta["local_bundle"].replace("<runs>", str(pool.CACHE / "fuzz" / "runs"))) \
            if meta.get("local_bundle") else fdir / "bundle"
        if not (src / "replay.jsonl").exists():
            src = fdir / "bundle"
        case = red.case_from_bundle(src, dict(rec, system_spec=meta["system_spec"]))
        small, stats = red.reduce(case, pool.CACHE / "fuzz" / "reduce" / fid, log=print)
        if small is None:
            print(json.dumps({"reproduced": False, **stats}))
            return 1
        out = red.write_regression(root, fid, small, stats, bool(meta.get("self_test")))
        print(json.dumps(out))
        return 0
    if a.cmd == "clean":
        for line in pool.clean():
            print(json.dumps(line))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
