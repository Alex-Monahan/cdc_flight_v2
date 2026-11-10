"""The reducer: a failing run becomes the smallest case that still fails, and a seeded regression test.

A case is the replay log of the run (setup comes from the seed's configuration; then `txn` and `driver` entries),
plus the faults of the fault log, in order. `replay_case` runs a case: a fresh scenario, the first instance started
and frozen once it holds a position, every entry replayed on one connection in log order, the instance continued
(so it meets the whole backlog in large groups, which makes order and grouping bugs repeatable), then the faults one
after another, then quiesce, catch-up and the oracle, with the consumer loops running all the time.

`reduce` removes rounds, then writers, then single entries, then faults, while the failure stays (delta debugging;
a candidate counts as failing if any of `attempts` runs fails, because a run is not fully deterministic), and
writes `case.json` and `tests/fuzz/regressions/test_<id>.py`. SPEC §4: every failure becomes a seeded regression
test before it is fixed.
"""
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Callable, Optional

from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.testing.harness_evidence import write_evidence

from . import faultkinds as fk
from . import observers as obs
from . import pool
from .run import Supervisor
from .system import make_system

REGRESSION_DIR = Path(__file__).resolve().parents[4] / "tests" / "fuzz" / "regressions"


def ddmin(items: list, fails: Callable[[list], bool], budget: int = 64) -> list:
    """Classic delta debugging: a 1-minimal sublist of `items` for which `fails` holds (within `budget` tests)."""
    n, tests = 2, 0
    while len(items) >= 2 and tests < budget:
        chunk = max(1, len(items) // n)
        subsets = [items[i:i + chunk] for i in range(0, len(items), chunk)]
        reduced = False
        for i in range(len(subsets)):
            complement = [x for j, s in enumerate(subsets) if j != i for x in s]
            tests += 1
            if complement and fails(complement):
                items, n, reduced = complement, max(n - 1, 2), True
                break
            if tests >= budget:
                break
        if not reduced:
            if n >= len(items):
                break
            n = min(len(items), n * 2)
    return items


def entries_of(log_entries: list) -> list:
    return [e for e in log_entries if e.get("type") in ("txn", "driver")]


def fault_from_dict(d: dict):
    kind, params = d["kind"], dict(d.get("params") or {})
    classes = {"kill_point": fk.KillPoint, "kill_point_later": fk.KillPointLater, "hang_point": fk.HangPoint,
               "kill_group": fk.KillGroup, "freeze": fk.Freeze, "src_lock": fk.SrcLock,
               "src_long_txn": fk.SrcLongTxn, "src_wal_switch": fk.SrcWalSwitch,
               "src_restart_primary": fk.SrcRestartPrimary, "slot_drop": fk.SlotDrop,
               "slot_invalidate": fk.SlotInvalidate, "replica_restart": fk.ReplicaRestart,
               "replica_promote": fk.ReplicaPromote, "spill_full": fk.SpillFull}
    if kind in fk.Net.MODES:
        return fk.Net(kind, **params)
    if kind in ("src_terminate", "src_cancel"):
        return fk.SrcSignal(kind, **params)
    if kind in ("dest_error", "dest_delay", "dest_hang"):
        return fk.Dest(kind, **params)
    return classes[kind](**params)


def replay_case(case: dict, workdir: Path, variant: Optional[str] = None, catch_up_s: float = 90.0) -> dict:
    """Run a case once. Returns {outcome, reason}."""
    name = case["system"]
    if variant is not None:
        name = name.split(":")[0] + ":" + variant
    lease = pool.acquire(wait_s=600)
    system = make_system(name, lease, case.get("source_mode", "replica"), case.get("options"))
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    sup = observers_ = None
    out = {"outcome": None, "reason": ""}
    try:
        system.setup(workdir, case["seed"])
        from cdc_v2.testing.faults import FaultControl
        fc = FaultControl(workdir / "faults")
        sup = Supervisor(system, workdir, fc).start()
        t = time.monotonic()
        while system.applied_position() is None:
            if time.monotonic() - t > 120:
                raise RuntimeError("no position within 120 s")
            time.sleep(0.05)
        wl = system.wl
        checks = obs.checks_for(wl.cfg.features, wl.cfg.schema, wl.transfer_table_names, wl.transfer_sum)
        observers_ = obs.Observers(checks, system.observer_connect, 0.005).start() if checks else None
        sup.pause()
        p = sup.current
        if p is not None and p.alive():
            p.stop_group()
        with system.db.connect("admin", system.primary_node) as c:
            for e in case["entries"]:
                for s in e["statements"]:
                    c.execute(s)
        if p is not None and p.alive():
            p.cont_group()
        sup.resume()
        ctx = fk.Ctx(system, sup, fc, _NoLoad(), time.monotonic())
        for d in case.get("faults", []):
            f = fault_from_dict(d)
            try:
                f.inject(ctx)
                time.sleep(float(d.get("duration", 0.5)))
                f.heal(ctx)
            except Exception:                              # noqa: BLE001 - a fault that cannot act is skipped
                pass
        q = system.quiesce()
        try:
            system.wait_for_applied(q, Deadline.after(catch_up_s))
        except DeadlineExceeded:
            out.update(outcome="stuck", reason="no catch-up")
            return out
        r = system.compare(q)
        if not r.equal:
            out.update(outcome="diverged", reason="oracle")
        elif observers_ is not None and observers_.violations():
            # the destination equals the source now: if an invariant fails on it, the reduced workload broke the
            # invariant at the source itself (removed entries), and the case says nothing about the system
            conn = system.observer_connect()
            broken = [lp.check.name for lp in observers_.loops if lp.observe_once(conn) is False]
            if broken:
                out.update(outcome="invalid", reason="the case breaks the invariant at the source: %s" % broken)
            else:
                out.update(outcome="violation", reason="consumer loop")
        else:
            out["outcome"] = "pass"
        return out
    except Exception as e:                                 # noqa: BLE001
        out.update(outcome="error", reason="%s: %s" % (type(e).__name__, str(e)[:200]))
        return out
    finally:
        if observers_ is not None:
            out["observers"] = observers_.stop()
        if sup is not None:
            try:
                sup.stop()
            except Exception:                              # noqa: BLE001
                pass
        try:
            system.teardown()
        finally:
            lease.release()


class _NoLoad:
    def pause(self, timeout=None):
        pass

    def resume(self):
        pass


def case_from_bundle(bundle_dir: Path, record: dict) -> dict:
    from cdc_v2.testing.workload import read_log
    log = read_log(bundle_dir / "replay.jsonl")
    faults = []
    try:
        for f in json.loads((bundle_dir / "fault_log.json").read_text()):
            if f.get("effect"):
                d = {"kind": f["kind"], "params": f.get("params") or {}}
                if f.get("t_inject") is not None and f.get("t_heal") is not None:
                    d["duration"] = round(max(0.1, f["t_heal"] - f["t_inject"]), 3)
                faults.append(d)
    except FileNotFoundError:
        pass
    return {"system": record["system_spec"], "source_mode": record["source_mode"], "seed": record["seed"],
            "options": record.get("options") or {}, "entries": entries_of(log), "faults": faults,
            "fuzzer": record["fuzzer"], "outcome": record["outcome"]}


def reduce(case: dict, workdir: Path, attempts: int = 2, budget: int = 40,
           log: Optional[Callable[[str], None]] = None) -> tuple:
    """(smallest failing case, stats)."""
    runs = {"n": 0}
    t0 = time.monotonic()

    def fails(c):
        for _ in range(attempts):
            runs["n"] += 1
            res = replay_case(c, workdir / ("r%d" % runs["n"]))
            if log:
                log("reduce: %d entries %d faults -> %s" % (len(c["entries"]), len(c["faults"]), res["outcome"]))
            if res["outcome"] == c.get("outcome"):
                return True                                # the same failure; another one is not the same case
        return False

    if not fails(case):
        return None, {"reproduced": False, "replays": runs["n"], "seconds": round(time.monotonic() - t0, 1)}
    cur = dict(case)
    rounds = sorted({e.get("round", 0) for e in cur["entries"]})
    keep_rounds = ddmin(rounds, lambda rs: fails(dict(cur, entries=[e for e in cur["entries"]
                                                                       if e.get("round", 0) in set(rs)])), budget)
    cur["entries"] = [e for e in cur["entries"] if e.get("round", 0) in set(keep_rounds)]
    writers = sorted({e.get("writer") for e in cur["entries"] if e["type"] == "txn"})
    if len(writers) > 1:
        keep_w = ddmin(writers, lambda ws: fails(dict(cur, entries=[e for e in cur["entries"] if e["type"] != "txn"
                                                                    or e.get("writer") in set(ws)])), budget)
        cur["entries"] = [e for e in cur["entries"] if e["type"] != "txn" or e.get("writer") in set(keep_w)]
    idx = list(range(len(cur["entries"])))
    keep_i = ddmin(idx, lambda ii: fails(dict(cur, entries=[cur["entries"][i] for i in ii])), budget)
    cur["entries"] = [cur["entries"][i] for i in keep_i]
    if cur["faults"]:
        fi = list(range(len(cur["faults"])))
        if fails(dict(cur, faults=[])):
            cur["faults"] = []
        else:
            keep_f = ddmin(fi, lambda ii: fails(dict(cur, faults=[cur["faults"][i] for i in ii])), budget)
            cur["faults"] = [cur["faults"][i] for i in keep_f]
    return cur, {"reproduced": True, "replays": runs["n"], "seconds": round(time.monotonic() - t0, 1),
                 "entries": len(cur["entries"]), "faults": len(cur["faults"]),
                 "from_entries": len(case["entries"]), "from_faults": len(case["faults"])}


TEMPLATE = '''"""Seeded regression test {fid} (generated by `tools/fuzz reduce`; SPEC §4: every failure becomes one).

Found by the {fuzzer} fuzzer on {system} ({mode} mode), seed {seed}: {outcome}. The reduced case has {n_entries}
workload entries and {n_faults} faults: {case_rel}.
"""
import json

from pathlib import Path

import pytest

from cdc_v2.testing.fuzz.reduce import replay_case

ROOT = Path(__file__).resolve().parents[3]
CASE = json.loads((ROOT / "{case_rel}").read_text())
pytestmark = [pytest.mark.localpg, pytest.mark.timeout(600)]


def _run(tmp_path, variant=None):
    return replay_case(CASE, tmp_path / "case", variant=variant)

{tests}'''

TEST_SELF = '''
def test_{name}_correct_toy_passes(tmp_path):
    assert _run(tmp_path, "correct")["outcome"] == "pass"


@pytest.mark.xfail(strict=True, reason="planted bug of the toy: the case must keep failing it")
def test_{name}_planted_bug_is_still_found(tmp_path):
    assert _run(tmp_path)["outcome"] == "pass"
'''

TEST_REAL = '''
def test_{name}_passes(tmp_path):
    assert _run(tmp_path)["outcome"] == "pass"
'''


def write_regression(root: Path, fid: str, case: dict, stats: dict, self_test: bool) -> dict:
    """case.json and failure.json under evidence/robustness/<fuzzer>/failures/<id>/, and the test file."""
    fdir = Path(root) / case["fuzzer"] / "failures" / fid
    case_path = fdir / "case.json"
    write_evidence(case_path, case)
    repo = REGRESSION_DIR.parents[2]
    case_rel = str(case_path.resolve().relative_to(repo.resolve()))
    name = fid.replace("-", "_").replace(":", "_")
    test_path = REGRESSION_DIR / ("test_%s.py" % name)
    body = TEMPLATE.format(fid=fid, fuzzer=case["fuzzer"], system=case["system"], mode=case["source_mode"],
                           seed=case["seed"], outcome=case["outcome"], n_entries=len(case["entries"]),
                           n_faults=len(case["faults"]), case_rel=case_rel,
                           tests=(TEST_SELF if self_test else TEST_REAL).format(name=name))
    write_evidence(test_path, body)
    meta_path = fdir / "failure.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {"id": fid}
    meta.update(regression_test=str(test_path.relative_to(repo)), reduced=stats, fixed_commit=meta.get(
        "fixed_commit"))
    write_evidence(meta_path, meta)
    return meta
