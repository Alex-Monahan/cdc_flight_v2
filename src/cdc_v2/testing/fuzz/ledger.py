"""The campaign ledger under evidence/robustness/ (PLAN §6): what proves the quantities of SPEC §4.

* `<fuzzer>/runs.jsonl`: one line per run (written by the campaign runner through `write_evidence`).
* `<fuzzer>/failures/<id>/`: the reduced case, the path of its regression test, later the fixing commit.
* `ledger.json`: written only by `tools/fuzz status --write`, computed from the run lines and git; nothing typed by
  hand.

Counter restart (SPEC §4: after a fix to correctness code the counters of the affected fuzzer restart from zero).
`AFFECTS` names, per fuzzer, the paths whose change affects it (the pipeline's source, the engine, the bootstrap;
not documents, not tests). The restart commit of a fuzzer is the newest commit that touches one of them and is not
exempted in `exempt.json` (commit, reason). Only runs made on that commit or a descendant, on a clean tree, are
counted. Runs of a self-test system (the toy) are shown apart and never count toward the bar.
"""
from __future__ import annotations

import json
import subprocess
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

from cdc_v2.testing.harness_evidence import write_evidence

FUZZERS = ("workload", "fault", "stream", "leadership", "value")

_PIPELINE = ["src/cdc_v2/stream", "src/cdc_v2/apply", "src/cdc_v2/md", "src/cdc_v2/pg", "src/cdc_v2/transform",
             "src/cdc_v2/backfill", "src/cdc_v2/state", "src/cdc_v2/leader", "src/cdc_v2/supervisor.py",
             "src/cdc_v2/pipeline.py", "src/cdc_v2/config.py", "src/cdc_v2/lsn.py", "src/cdc_v2/deadline.py",
             "src/cdc_v2/artifacts.py", "jvm/src", "flights/cdc_v2"]
# A change of a pinned dependency is a change to correctness code (plan 6.2, "The ledger of SPEC section 4").
PINS = ["jvm/debezium.lock.json", "jvm/jdk.lock.json", "uv.lock", "pyproject.toml"]
# The measuring instrument: the fuzz package, the oracle, the workload generator, the fault harness. A change to it
# restarts the counters of every bar it measures; only an exemption (with its reason) avoids that.
INSTRUMENT = ["src/cdc_v2/testing", "src/cdc_v2/oracle", "src/cdc_v2/crashpoints.py", "src/cdc_v2/faultsites.py",
              "src/cdc_v2/evidence.py", "tools/fuzz"]
AFFECTS = {
    "workload": _PIPELINE + PINS + INSTRUMENT,
    "fault": _PIPELINE + PINS + INSTRUMENT,
    "stream": ["src/cdc_v2/stream", "src/cdc_v2/transform", "jvm/src"] + PINS + INSTRUMENT,
    "leadership": ["src/cdc_v2/leader", "src/cdc_v2/md", "src/cdc_v2/state", "src/cdc_v2/supervisor.py",
                   "src/cdc_v2/pipeline.py", "src/cdc_v2/stream", "flights/cdc_v2"] + PINS + INSTRUMENT,
    "value": ["src/cdc_v2/transform", "src/cdc_v2/apply", "src/cdc_v2/stream", "jvm/src"] + PINS + INSTRUMENT,
}
# Until the owner rules: a bar "on local Postgres and on PlanetScale, in replica mode and in primary mode" is met
# when its totals are met and each of the four combinations holds at least this fraction of each quantity.
COMBO_FRACTION = 0.25
COMBO_QUANTITIES = ("seeds", "hours", "faults", "rounds")

# SPEC §4 robustness bar. Workload and fault: on local Postgres and on PlanetScale, in replica and primary mode.
MINIMUMS = {
    "workload": {"seeds": 1000, "hours": 100.0},
    "fault": {"seeds": 500, "hours": 100.0, "faults": 10000, "per_kind": 100, "divergences": 0},
    "stream": {},
    "leadership": {"rounds": 1000},
    "value": {"values_per_family": 1_000_000},
}
REQUIRED_COMBOS = {"workload": [("local", "replica"), ("local", "primary"), ("planetscale", "replica"),
                                ("planetscale", "primary")],
                   "fault": [("local", "replica"), ("local", "primary"), ("planetscale", "replica"),
                             ("planetscale", "primary")]}
DIVERGENT = ("diverged", "violation")


class Git:
    def __init__(self, repo: Path):
        self.repo = Path(repo)
        self._anc = {}

    def run(self, *args) -> subprocess.CompletedProcess:
        return subprocess.run(["git", *args], cwd=self.repo, capture_output=True, text=True, timeout=60)

    def commits_touching(self, paths: list) -> list:
        out = self.run("log", "--no-merges", "--format=%H", "--", *paths)
        return [c for c in out.stdout.split() if c]

    def missing_fixes(self, run_commit: str, paths: list, exempt: set) -> list:
        """Commits of HEAD's history that touch `paths`, are not exempt, and are not in `run_commit`'s history: the
        fixes the run did not have (merge order does not matter)."""
        key = ("missing", run_commit)
        if key not in self._anc:
            out = self.run("rev-list", "--no-merges", "%s..HEAD" % run_commit, "--", *paths)
            if out.returncode != 0:
                self._anc[key] = ["<unknown commit>"]
            else:
                self._anc[key] = [c for c in out.stdout.split() if c not in exempt
                                  and not any(c.startswith(e) for e in exempt if len(e) >= 7)]
        return self._anc[key]

    def is_ancestor(self, a: str, b: str) -> bool:
        key = (a, b)
        if key not in self._anc:
            self._anc[key] = self.run("merge-base", "--is-ancestor", a, b).returncode == 0
        return self._anc[key]


def load_exempt(root: Path) -> list:
    p = Path(root) / "exempt.json"
    if not p.exists():
        return []
    data = json.loads(p.read_text())
    return data.get("exempt", []) if isinstance(data, dict) else data


def restart_commit(git: Git, fuzzer: str, exempt: list) -> Optional[dict]:
    """The newest non-exempt commit that touches a path of AFFECTS[fuzzer], with why; None if none touches."""
    skip = {e["commit"] for e in exempt}
    for c in git.commits_touching(AFFECTS[fuzzer]):
        if c in skip or any(c.startswith(s) for s in skip if len(s) >= 7):
            continue
        files = [f for f in git.run("show", "--format=", "--name-only", c).stdout.split()
                 if any(f == p or f.startswith(p.rstrip("/") + "/") for p in AFFECTS[fuzzer])]
        subject = git.run("show", "-s", "--format=%s", c).stdout.strip()
        return {"commit": c, "why": "touches %s" % ", ".join(files[:5]), "subject": subject[:120]}
    return None


def read_runs(root: Path, fuzzer: str) -> list:
    p = Path(root) / fuzzer / "runs.jsonl"
    if not p.exists():
        return []
    out = []
    for line in p.read_text().splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


def append_run(root: Path, rec: dict) -> None:
    write_evidence(Path(root) / rec["fuzzer"] / "runs.jsonl", json.dumps(rec, sort_keys=True, default=str) + "\n",
                   append=True)


def held_seeds(root: Path, fuzzer: str, system: str, source_mode: str, tier: str = "local") -> set:
    """Seeds the ledger holds for this fuzzer, system, tier and mode (an infrastructure outcome holds none)."""
    return {r["seed"] for r in read_runs(root, fuzzer) if r.get("system") == system
            and r.get("source_mode") == source_mode and r.get("tier", "local") == tier
            and r.get("outcome") != "infrastructure"}


def failures(root: Path, fuzzer: str) -> list:
    d = Path(root) / fuzzer / "failures"
    out = []
    if d.exists():
        for f in sorted(d.glob("*/failure.json")):
            out.append(json.loads(f.read_text()))
    return out


def summarize(runs: list) -> dict:
    faults, no_effect, outcomes = Counter(), Counter(), Counter()
    seeds, hours, rounds = set(), 0.0, 0
    for r in runs:
        outcomes[r.get("outcome")] += 1
        if r.get("outcome") == "infrastructure":
            continue
        seeds.add(r["seed"])
        hours += float(r.get("duration_s") or 0) / 3600.0
        rounds += int((r.get("counts") or {}).get("rounds") or 0)
        faults.update(r.get("faults") or {})
        no_effect.update(r.get("no_effect") or {})
    return {"runs": len(runs), "seeds": len(seeds), "hours": round(hours, 4), "rounds": rounds,
            "faults": sum(faults.values()), "faults_per_kind": dict(sorted(faults.items())),
            "no_effect_per_kind": dict(sorted(no_effect.items())), "outcomes": dict(sorted(outcomes.items())),
            "divergences": outcomes.get("diverged", 0) + outcomes.get("violation", 0)}


def against(fuzzer: str, s: dict, supported_kinds: Optional[list] = None) -> dict:
    """Each number of `s` next to its minimum, with met / not met."""
    out = {}
    for k, need in MINIMUMS.get(fuzzer, {}).items():
        if k == "per_kind":
            kinds = supported_kinds if supported_kinds is not None else sorted(s["faults_per_kind"])
            low = {kk: s["faults_per_kind"].get(kk, 0) for kk in kinds if s["faults_per_kind"].get(kk, 0) < need}
            out[k] = {"minimum": need, "kinds": len(kinds), "below": low, "met": bool(kinds) and not low}
        elif k == "divergences":
            out[k] = {"value": s["divergences"], "maximum": need, "met": s["divergences"] <= need and s["runs"] > 0}
        elif k == "values_per_family":
            out[k] = {"minimum": need, "met": False, "note": "the value fuzzer is built in the P4 type wave"}
        else:
            v = s.get(k, 0)
            out[k] = {"value": v, "minimum": need, "met": v >= need}
    return out


def build(root: Path, repo: Path) -> dict:
    """ledger.json content: per fuzzer, system, tier and mode; counted runs only toward the bar."""
    git = Git(repo)
    exempt = load_exempt(root)
    out = {"about": "Written by `tools/fuzz status --write` from runs.jsonl and git (docs/testing/fuzzers.md). "
                    "Self-test runs (the toy) never count toward SPEC §4.",
           "minimums": MINIMUMS, "exempt_commits": exempt, "fuzzers": {}}
    for fz in FUZZERS:
        runs = read_runs(root, fz)
        rc = restart_commit(git, fz, exempt)
        groups = defaultdict(list)
        for r in runs:
            groups[(r.get("system"), r.get("tier", "local"), r.get("source_mode"))].append(r)
        entry = {"restart": rc, "affects": AFFECTS[fz], "groups": [], "failures": []}
        totals = []
        for (system, tier, mode), rs in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
            self_test = any(r.get("self_test") for r in rs)
            skip = {e["commit"] for e in exempt}
            counted = [] if self_test else [r for r in rs if r.get("clean") and r.get("commit") and
                                            not git.missing_fixes(r["commit"], AFFECTS[fz], skip)]
            g = {"system": system, "tier": tier, "source_mode": mode, "self_test": self_test,
                 "all_runs": summarize(rs), "counted": summarize(counted)}
            kinds = sorted({k for r in rs for k, v in ((r.get("config") or {}).get("schedule") or {}).get(
                "weights", {}).items()}) or None
            if self_test:
                g["note"] = "self-test of the fuzzers: never counted toward the bar"
            else:
                g["against_minimum"] = against(fz, g["counted"], kinds)
                totals.extend(counted)
            g["weights"] = _weights(rs)
            entry["groups"].append(g)
        tot = summarize(totals)
        entry["counted_total"] = tot
        kinds_all = sorted({k for r in totals for k in ((r.get("config") or {}).get("schedule") or {}).get(
            "weights", {})}) or None
        entry["against_minimum_total"] = against(fz, tot, kinds_all)
        combos = REQUIRED_COMBOS.get(fz)
        if combos:
            have = {(g["tier"], g["source_mode"]) for g in entry["groups"] if not g["self_test"]
                    and g["counted"]["runs"]}
            per = {}
            for c in combos:
                rs = [r for r in totals if (r.get("tier", "local"), r.get("source_mode")) == c]
                sm = summarize(rs)
                need = {q: MINIMUMS[fz][q] * COMBO_FRACTION for q in COMBO_QUANTITIES if q in MINIMUMS[fz]}
                per["%s/%s" % c] = {q: {"value": sm[q], "minimum": need[q], "met": sm[q] >= need[q]} for q in need}
            entry["combos"] = {"required": ["%s/%s" % c for c in combos], "fraction": COMBO_FRACTION,
                               "missing": ["%s/%s" % c for c in combos if c not in have],
                               "per_combination": per,
                               "all_met": all(v["met"] for d in per.values() for v in d.values())}
        met = all(v.get("met") for v in entry["against_minimum_total"].values()) if MINIMUMS.get(fz) else None
        if combos and (entry["combos"]["missing"] or not entry["combos"]["all_met"]):
            met = False
        entry["bar_met"] = met
        for f in failures(root, fz):
            entry["failures"].append({k: f.get(k) for k in ("id", "system", "seed", "outcome", "regression_test",
                                                            "fixed_commit", "self_test")})
        entry["failures_open"] = sum(1 for f in entry["failures"] if not f.get("fixed_commit")
                                     and not f.get("self_test"))
        entry["failures_fixed"] = sum(1 for f in entry["failures"] if f.get("fixed_commit"))
        if fz == "value":
            entry["note"] = "not built yet: the value fuzzer belongs to the P4 type wave (task of package F)"
        out["fuzzers"][fz] = entry
    return out


def _weights(rs):
    for r in reversed(rs):
        sch = (r.get("config") or {}).get("schedule")
        if sch:
            return {"weights": sch.get("weights"), "not_attacked": sch.get("not_attacked")}
    return None


def write(root: Path, repo: Path) -> dict:
    data = build(root, repo)
    write_evidence(Path(root) / "ledger.json", data)
    return data


def status_text(data: dict) -> str:
    lines = []
    for fz, e in data["fuzzers"].items():
        rc = e.get("restart")
        lines.append("%s: restart %s (%s); bar met: %s; failures open %d, fixed %d" % (
            fz, rc["commit"][:10] if rc else "-", rc["why"] if rc else "no commit touches its paths",
            e.get("bar_met"), e.get("failures_open", 0), e.get("failures_fixed", 0)))
        for g in e["groups"]:
            a, c = g["all_runs"], g["counted"]
            lines.append("  %-28s %-6s %-8s runs %4d seeds %4d hours %7.3f faults %6d rounds %5d outcomes %s%s" % (
                g["system"], g["tier"], g["source_mode"], a["runs"], a["seeds"], a["hours"], a["faults"],
                a["rounds"], a["outcomes"], " (self-test)" if g["self_test"] else " counted: %d seeds, %.3f h"
                % (c["seeds"], c["hours"])))
        for combo, qs in ((e.get("combos") or {}).get("per_combination") or {}).items():
            lines.append("    %-20s %s" % (combo, "  ".join("%s %s/%s%s" % (q, round(v["value"], 3), v["minimum"],
                                                                         "" if v["met"] else " (not met)")
                                                        for q, v in qs.items())))
    ex = data.get("exempt_commits") or []
    lines.append("exemptions (%d), every entry in full, for the orchestrator's review at each gate:" % len(ex))
    for x in ex:
        lines.append("  " + json.dumps(x, sort_keys=True))
    return "\n".join(lines)
