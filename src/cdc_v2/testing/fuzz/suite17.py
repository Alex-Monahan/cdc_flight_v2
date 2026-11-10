"""The suite of SPEC 1.7, a fixed part of the fault fuzzer: `tools/fuzz suite-1.7 --system <name>`.

1. Every declared crash point with at least 3 seeds (the crash runner of docs/testing/harness.md; a point that is
   never reached is a failure), and the points immediately before a commit again with `kill_later` (COMMIT in
   flight).
2. Each network fault, destination errors, source backend termination, slot loss and the full spill area: one
   fault-fuzzer run per group, with only that group's kinds scheduled.
3. A randomized soak of `soak_s` seconds with every supported kind, which must inject at least `min_soak_faults`
   counted faults (SPEC: ≥ 1 h and ≥ 200 faults; this round runs 15 minutes on the toy).

Pass = oracle equality after every run, every point reached, every group's faults observed to have had their
effect. The output directory holds the crash ledger and summary, the run lines and `summary.json` — the format of
the evidence of SPEC 1.7.
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from cdc_v2.testing.crash import Config, CrashRunner
from cdc_v2.testing.harness_evidence import register_root, write_evidence

from . import pool
from .faultkinds import Schedule
from .run import RunConfig, run_once
from .system import make_system

GROUPS = {
    "network": ["net_drop", "net_reset", "net_blackhole", "net_latency"],
    "destination": ["dest_error", "dest_delay", "dest_hang"],
    "source_backends": ["src_terminate", "src_cancel"],
    "slot_loss": ["slot_drop", "slot_invalidate"],
    "spill": ["spill_full"],
}


EXPECTED_PARTS = ("crash_points",) + tuple(GROUPS) + ("soak",)


def judge(parts: dict) -> tuple:
    """(all_passed, failed parts): every expected part must be present and must have passed; a missing part, or a
    part without an explicit pass, never passes. A group passes when skipped only if the system declared it not
    applicable with a reason (printed in the summary)."""
    failed = []
    for name in EXPECTED_PARTS:
        p = parts.get(name)
        if p is None:
            failed.append("%s: missing" % name)
        elif name == "crash_points":
            if not (p.get("all_passed") is True and p.get("missing_points") == []):
                failed.append("crash_points: not all passed or points missing")
        elif p.get("passed") is not True:
            failed.append("%s: %s" % (name, "skipped, not declared not applicable" if p.get("skipped")
                                      else "not passed"))
    return not failed, failed


class _Leases:
    """At most `n` homes for the crash runner's scenarios, checked out by a scenario's system when it sets up and
    given back when it tears down (the runner makes new worker threads for every call, so a home per thread would
    keep growing; every running cluster also holds System V shared memory, which the machine has little of)."""

    def __init__(self, n):
        self.n = n
        self.free = []
        self.all = []
        self.cond = threading.Condition()

    def checkout(self):
        with self.cond:
            while not self.free and len(self.all) >= self.n:
                self.cond.wait(5)
            if self.free:
                return self.free.pop()
            lease = pool.acquire(wait_s=600)
            self.all.append(lease)
            return lease

    def give_back(self, lease):
        with self.cond:
            self.free.append(lease)
            self.cond.notify()

    def get(self):
        """A home for sequential use by the caller (taken out of the shared set until release)."""
        return self.checkout()

    def release(self):
        for lease in self.all:
            lease.release()


class _LazyLease:
    """Stands in for a lease until the scenario sets up; then checks one out of `_Leases` and gives it back after
    teardown. The crash runner's probe instance never sets up and so never holds a home."""

    def __init__(self, leases):
        self._leases, self._lease = leases, None

    def __getattr__(self, name):
        if self._lease is None:
            self._lease = self._leases.checkout()
        return getattr(self._lease, name)

    def give_back(self):
        if self._lease is not None:
            self._leases.give_back(self._lease)
            self._lease = None


def _scenario_system(system_name, leases, source_node):
    lazy = _LazyLease(leases)
    sysm = make_system(system_name, lazy, source_node)
    teardown = sysm.teardown

    def teardown_and_give_back():
        try:
            teardown()
        finally:
            lazy.give_back()
    sysm.teardown = teardown_and_give_back
    return sysm


SPEC_SOAK_S = 3600.0          # SPEC 1.7: a randomized soak of at least 1 h ...
SPEC_SOAK_FAULTS = 200        # ... with at least 200 faults (that had an effect); never lowered
SELF_TEST_SOAK_S = 900.0      # the toy may run 15 minutes; its result is labelled a self-test
SMOKE_SOAK_S = 120.0          # a routine smoke run: its soak never passes


def run_suite(system_name: str, out_dir: Path, repo: Path, source_node: str = "replica", seeds=(1, 2, 3),
              soak_s=None, min_soak_faults: int = SPEC_SOAK_FAULTS, group_budget_s: float = 30.0, workers: int = 2,
              points=None, log=print, smoke: bool = False) -> dict:
    out_dir = Path(out_dir)
    if min_soak_faults < SPEC_SOAK_FAULTS and not smoke:
        raise ValueError("the soak's minimum of %d faults is not lowered" % SPEC_SOAK_FAULTS)
    work = pool.CACHE / "fuzz" / "suite" / time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    work.mkdir(parents=True, exist_ok=True)
    register_root(work, "<run>")
    register_root(repo, "<repo>")
    for name in ("crash_ledger.jsonl", "runs.jsonl"):        # one suite run per output directory, never appended
        (out_dir / name).unlink(missing_ok=True)
    leases = _Leases(max(1, workers))
    from .run import git_state
    self_test = bool(make_system(system_name, None, source_node).is_self_test)
    if soak_s is None:
        soak_s = SMOKE_SOAK_S if smoke else SELF_TEST_SOAK_S if self_test else SPEC_SOAK_S
    need_s = soak_s if (self_test or smoke) else SPEC_SOAK_S
    summary = {"system": system_name, "source_node": source_node, "seeds": list(seeds), "parts": {},
               **git_state(repo), "self_test": self_test, "smoke": smoke,
               "label": ("SMOKE RUN: its soak never passes" if smoke else
                         "SELF-TEST of the fuzzers on the toy (15-minute soak allowed; never counts for SPEC 1.7)"
                         if self_test else "SPEC 1.7")}
    t0 = time.monotonic()
    try:
        def factory():
            return _scenario_system(system_name, leases, source_node)

        probe = factory()
        pts = list(points or probe.crash_points())
        commit_pts = [p for p in pts if p.endswith("commit_sent")]
        cfg = Config(workers=workers, crash_timeout=30.0, catch_up_timeout=90.0, workload_interval=0.25,
                     max_workload_steps=200, stop_timeout=15.0)
        runner = CrashRunner(factory, work / "crash", ledger_path=out_dir / "crash_ledger.jsonl", config=cfg)
        runner.declared = pts
        runner.run_crash_points(points=pts, seeds=tuple(seeds))
        if commit_pts:
            runner.run_crash_points(points=commit_pts, seeds=tuple(seeds), action="kill_later", delay_ms=0)
        crash = runner.summary()
        runner.write_summary(out_dir / "crash_summary.json")
        summary["parts"]["crash_points"] = {k: crash[k] for k in ("coverage", "missing_points", "runs", "outcomes",
                                                                  "all_passed")}
        log("crash points: %s" % json.dumps(summary["parts"]["crash_points"]))
        lease = leases.get()
        runs = []
        for gi, (group, kinds) in enumerate(GROUPS.items()):
            sysm = make_system(system_name, lease, source_node)
            kinds = [k for k in kinds if sysm.supports(k)]
            if not kinds:
                reason = (sysm.not_applicable() or {}).get(group)
                summary["parts"][group] = {"skipped": "the system supports none of %s" % GROUPS[group],
                                           "not_applicable": reason, "passed": bool(reason)}
                continue
            sch = Schedule(sysm, only=kinds, per_episode=(len(kinds), len(kinds) + 1), cover_all=True)
            rec = run_once(sysm, 1000 + gi, work / group, RunConfig(fuzzer="fault", budget_s=group_budget_s,
                                                                     episode_s=8.0, schedule=sch), repo)
            missing = [k for k in kinds if not rec["faults"].get(k)]
            ok = rec["outcome"] == "pass" and not missing
            summary["parts"][group] = {"outcome": rec["outcome"], "faults": rec["faults"],
                                       "no_effect": rec["no_effect"], "kinds_without_effect": missing,
                                       "passed": ok, "reason": rec["reason"][:300]}
            runs.append(rec)
            log("%s: %s" % (group, json.dumps(summary["parts"][group])))
        sysm = make_system(system_name, lease, source_node)
        sch = Schedule(sysm, per_episode=(3, 6))
        rec = run_once(sysm, 2000, work / "soak", RunConfig(fuzzer="fault", budget_s=soak_s, episode_s=10.0,
                                                            schedule=sch), repo)
        counted = sum(rec["faults"].values())
        summary["parts"]["soak"] = {"outcome": rec["outcome"], "seconds": rec["duration_s"],
                                    "faults_with_effect": counted, "faults_without_effect": sum(
                                        rec["no_effect"].values()),
                                    "per_kind": rec["faults"], "no_effect": rec["no_effect"],
                                    "minimum_faults": SPEC_SOAK_FAULTS, "soak_s": soak_s,
                                    "minimum_seconds": need_s,
                                    "passed": (not smoke and rec["outcome"] == "pass" and counted >= SPEC_SOAK_FAULTS
                                               and rec["duration_s"] >= need_s),
                                    "reason": rec["reason"][:300], "not_attacked": sch.not_attacked}
        runs.append(rec)
        log("soak: %s" % json.dumps({k: v for k, v in summary["parts"]["soak"].items() if k != "per_kind"}))
        for r in runs:
            write_evidence(out_dir / "runs.jsonl", json.dumps(r, sort_keys=True, default=str) + "\n", append=True)
    finally:
        leases.release()
    parts = summary["parts"]
    summary["all_passed"], summary["failed_parts"] = judge(parts)
    summary["seconds"] = round(time.monotonic() - t0, 1)
    write_evidence(out_dir / "summary.json", summary)
    return summary
