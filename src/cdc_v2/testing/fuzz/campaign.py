"""The campaign runner: many seeds of one fuzzer against one system, in the background, resumable.

* It takes the next seeds that the ledger does not hold yet (an explicit range, or upward for a number of hours).
* One home of the pool per scenario, held under a file lock (pool.py); a cluster is reused across seeds until a
  fault kind breaks it, then rebuilt.
* It runs at low priority (nice 10), writes one ledger line per finished run, and on a failure keeps a bundle and
  (optionally) reduces it to a seeded regression test. SIGINT or SIGTERM stops it after cleaning up; after SIGKILL,
  `tools/fuzz clean` removes what its lock records name.
"""
from __future__ import annotations

import json
import os
import re
import signal
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

from cdc_v2.testing.harness_evidence import register_root, write_evidence

from . import ledger, pool
from .faultkinds import Schedule
from .run import FAILURES, RunConfig, bundle, run_once
from .system import make_system

REPO = Path(__file__).resolve().parents[4]
LEDGER_ROOT = REPO / "evidence" / "robustness"
RUNS_DIR = pool.CACHE / "fuzz" / "runs"

# budget per run of each fuzzer (seconds of load) unless given
DEFAULT_BUDGET = {"workload": 40.0, "fault": 60.0, "stream": 40.0, "leadership": None}
MAX_EVIDENCE_FILE = 256 << 10


def parse_seeds(text: str) -> list:
    m = re.match(r"^(\d+)\.\.(\d+)$", text or "")
    if m:
        return list(range(int(m.group(1)), int(m.group(2)) + 1))
    return [int(x) for x in (text or "").split(",") if x]


class Campaign:
    def __init__(self, fuzzer: str, system: str, source_node: str = "replica", seeds: Optional[list] = None,
                 hours: Optional[float] = None, workers: int = 1, budget_s: Optional[float] = None,
                 rounds: int = 10, options: Optional[dict] = None, reduce_failures: bool = False,
                 ledger_root: Path = LEDGER_ROOT, keep_workdir: bool = False, kinds: Optional[list] = None,
                 stop_on_failure: bool = False, log=print):
        self.stop_on_failure = stop_on_failure
        if fuzzer not in ("workload", "fault", "stream", "leadership"):
            raise ValueError("fuzzer is workload, fault, stream or leadership (the value fuzzer is P4)")
        if workers > 2:
            raise ValueError("at most 2 scenarios in parallel on this shared machine")
        self.fuzzer, self.system, self.source_node = fuzzer, system, source_node
        self.seeds, self.hours, self.workers = seeds, hours, max(1, workers)
        self.budget_s = budget_s if budget_s is not None else DEFAULT_BUDGET.get(fuzzer)
        self.rounds = rounds
        self.options = dict(options or {})
        if fuzzer == "stream":
            self.options.setdefault("toy", {}).setdefault("stream_fault_rate", 0.35)
        self.reduce_failures = reduce_failures
        self.root = Path(ledger_root)
        self.keep_workdir = keep_workdir
        self.kinds = kinds
        self.log = log
        self.id = time.strftime("%Y%m%dT%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:4]
        self.abort = threading.Event()
        self._lock = threading.Lock()
        self._queue = None
        self._deadline = None
        self.results = []
        self.refused = 0
        self.worker_errors = 0

    # ------------------------------------------------------------------ seeds
    def _system_label(self) -> str:
        base, _, variant = self.system.partition(":")
        return "%s-%s" % (base, variant or "correct") if base == "toy" else self.system

    def _make_queue(self):
        held = ledger.held_seeds(self.root, self.fuzzer, self._system_label(), self.source_node)
        if self.seeds is not None:
            return [s for s in self.seeds if s not in held]
        start = max(held) + 1 if held else 1
        return iter(range(start, 1 << 31))

    def _next_seed(self):
        with self._lock:
            if self.abort.is_set():
                return None
            if self._deadline is not None and time.monotonic() > self._deadline:
                return None
            if isinstance(self._queue, list):
                return self._queue.pop(0) if self._queue else None
            return next(self._queue)

    # ------------------------------------------------------------------ one run
    def _run_one(self, lease, seed: int) -> dict:
        system = make_system(self.system, lease, self.source_node, self.options)
        workdir = RUNS_DIR / self.id / ("%s-%s-%d" % (self.fuzzer, system.name, seed))
        register_root(RUNS_DIR, "<runs>")
        register_root(REPO, "<repo>")
        if self.fuzzer == "leadership":
            from .leadership import LeadershipRun
            workdir.mkdir(parents=True, exist_ok=True)
            lr = LeadershipRun(system, seed, workdir, REPO, rounds=self.rounds)
            rec = lr.execute()
            run_like = lr
        else:
            cfg = RunConfig(fuzzer=self.fuzzer, budget_s=self.budget_s, keep_workdir=self.keep_workdir,
                            abort=self.abort)
            if self.fuzzer == "fault":
                cfg.schedule = Schedule(system, only=self.kinds)
            holder = {}
            rec = run_once(system, seed, workdir, cfg, REPO, on_failure=lambda r: holder.setdefault("run", r))
            run_like = holder.get("run")
        rec["system_spec"] = self.system
        rec["options"] = self.options
        rec["campaign"] = self.id
        if rec["outcome"] == "aborted":
            return rec                                     # a stopped run holds no seed and writes no line
        if rec["outcome"] in FAILURES:
            fid = "%s-%s-%s-s%d-%s" % (self.fuzzer, rec["system"], rec["source_mode"], seed,
                                       (rec.get("commit") or "nocommit")[:7])
            rec["failure_id"] = fid
            try:
                self._keep_failure(fid, rec, run_like, workdir)
            except Exception as e:                         # noqa: BLE001 - the run's line is still written
                rec["failure_kept"] = "no: %s" % type(e).__name__
        try:
            ledger.append_run(self.root, rec)
        except Exception as e:                             # noqa: BLE001 - e.g. EvidenceError: keep the facts
            slim = {k: rec.get(k) for k in ("fuzzer", "system", "self_test", "tier", "source_mode", "seed",
                                            "commit", "clean", "start", "duration_s", "counts", "faults",
                                            "no_effect", "outcome", "failure_id", "system_spec", "campaign")}
            slim["reason"] = "the full record was refused by the evidence writer (%s)" % type(e).__name__
            ledger.append_run(self.root, slim)
            self.refused += 1
        return rec

    def _keep_failure(self, fid, rec, run_like, workdir):
        fdir = self.root / self.fuzzer / "failures" / fid
        local = workdir.parent / ("bundle-" + fid)
        if run_like is not None and hasattr(run_like, "fault_log") and hasattr(run_like, "checkpoints"):
            bundle(run_like, local)
        else:
            local.mkdir(parents=True, exist_ok=True)
            for name in ("replay.jsonl", "events.jsonl"):
                if (workdir / name).exists():
                    (local / name).write_bytes((workdir / name).read_bytes())
            (local / "record.json").write_text(json.dumps(rec, indent=1, sort_keys=True, default=str))
            if run_like is not None and getattr(run_like, "oracle_result", None):
                (local / "oracle.json").write_text(json.dumps(run_like.oracle_result, default=str))
        meta = {"id": fid, "fuzzer": self.fuzzer, "system": rec["system"], "system_spec": self.system,
                "source_mode": rec["source_mode"], "seed": rec["seed"], "outcome": rec["outcome"],
                "reason": rec.get("reason"), "commit": rec.get("commit"), "self_test": rec.get("self_test"),
                "first_failure_s": rec.get("first_failure_s"), "bundle": "bundle/", "regression_test": None,
                "fixed_commit": None}
        meta["local_bundle"] = str(local)
        write_evidence(fdir / "failure.json", meta)
        for p in sorted(local.iterdir()):
            if not p.is_file():
                continue
            text = p.read_text(errors="replace")
            if len(text) > MAX_EVIDENCE_FILE:
                if p.name == "replay.jsonl":
                    continue                               # kept locally; the reduced case is the evidence
                text = "[first part omitted: %d bytes]\n" % (len(text) - MAX_EVIDENCE_FILE) + \
                    text[-MAX_EVIDENCE_FILE:]
            try:
                write_evidence(fdir / "bundle" / p.name, text)
            except Exception as e:                         # noqa: BLE001 - recorded, never silently dropped
                write_evidence(fdir / "bundle" / (p.name + ".refused.txt"), "not kept: %s\n" % type(e).__name__)
        if self.reduce_failures and self.fuzzer != "leadership":
            from . import reduce as red
            case = red.case_from_bundle(local, dict(rec, system_spec=self.system, options=self.options))
            small, stats = red.reduce(case, workdir.parent / ("reduce-" + fid), log=self.log)
            if small is not None:
                red.write_regression(self.root, fid, small, stats, bool(rec.get("self_test")))
            else:
                meta["reduced"] = stats
                write_evidence(fdir / "failure.json", meta)

    # ------------------------------------------------------------------ workers
    def _worker(self, idx):
        lease = None
        try:
            lease = pool.acquire(wait_s=600)
            while not self.abort.is_set():
                seed = self._next_seed()
                if seed is None:
                    break
                t = time.monotonic()
                try:
                    rec = self._run_one(lease, seed)
                except Exception as e:                     # noqa: BLE001 - never a silent end of a worker
                    self.worker_errors += 1
                    self.log("seed %d: the runner failed: %s: %s" % (seed, type(e).__name__, str(e)[:200]))
                    continue
                self.results.append(rec)
                if self.stop_on_failure and rec["outcome"] in FAILURES:
                    self.abort.set()
                self.log("%s %s %s seed %d: %s in %.1f s%s" % (self.fuzzer, rec["system"], self.source_node, seed,
                                                               rec["outcome"], time.monotonic() - t,
                                                               (" (%s)" % rec["reason"][:160]) if rec["reason"]
                                                               else ""))
        finally:
            if lease is not None:
                if lease.record.get("dirty"):
                    try:
                        lease.cluster(rebuild=True)
                    except Exception:                      # noqa: BLE001
                        pass
                lease.release()

    def run(self) -> list:
        try:
            os.nice(10)
        except OSError:
            pass
        self._queue = self._make_queue()
        self._deadline = time.monotonic() + self.hours * 3600 if self.hours else None
        old = {}

        def stop(signum, frame):
            self.abort.set()
        if threading.current_thread() is threading.main_thread():
            for s in (signal.SIGINT, signal.SIGTERM):
                old[s] = signal.signal(s, stop)
        try:
            threads = [threading.Thread(target=self._worker, args=(i,), daemon=True) for i in range(self.workers)]
            for th in threads:
                th.start()
            while any(th.is_alive() for th in threads):
                for th in threads:
                    th.join(0.5)
        finally:
            for s, h in old.items():
                signal.signal(s, h)
        return self.results
