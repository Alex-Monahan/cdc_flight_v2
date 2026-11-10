"""One fuzz run = one seed against one system: set up, start, load in episodes, check, tear down.

The workload fuzzer and the fault fuzzer share this engine; the fault fuzzer adds a seeded schedule of faults to
every episode (faultkinds.py). An episode loads the source for `episode_s` seconds (rounds of the seeded workload,
paced), then a checkpoint: the load stops at a round boundary, every fault is healed, `quiesce() → Q`, the run waits
until the destination's applied position is at or past Q (deadline), and `compare(position=Q)`. The consumer loops
(observers.py) run the whole time.

Outcomes: `pass`; `diverged` (the oracle); `violation` (a consumer loop); `stuck` (no catch-up before the deadline:
a failure, never a retry); `error` (anything else that went wrong in the system or the run); `infrastructure` (the
scenario could not be set up, or the source itself failed under the workload: it carries its proof and counts
neither as a pass nor as a failure).
"""
from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
import threading
import time
import traceback
from collections import Counter
from pathlib import Path
from typing import Callable, Optional

from cdc_v2 import crashpoints, lsn
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.testing import procs
from cdc_v2.testing.faults import FaultControl
from cdc_v2.testing.harness_evidence import scrub

from . import observers as obs

OUTCOMES = ("pass", "diverged", "violation", "stuck", "error", "infrastructure")
FAILURES = ("diverged", "violation", "stuck", "error")
MIN_OBSERVATIONS = 20


def git_state(repo: Path) -> dict:
    def g(*a):
        return subprocess.run(["git", *a], cwd=repo, capture_output=True, text=True, timeout=30).stdout.strip()
    try:
        # clean = no change (also no new file) outside evidence/ (the ledger itself is written during campaigns)
        return {"commit": g("rev-parse", "HEAD"),
                "clean": g("status", "--porcelain", "--", ".", ":(exclude)evidence") == ""}
    except Exception:                                      # noqa: BLE001
        return {"commit": None, "clean": False}


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class Infrastructure(Exception):
    """The scenario could not run (setup, or the source failed under the workload for an allowed cause); carries
    the proof."""


# A failure of the workload at the source is `infrastructure` only for a cause on this short list (plan 6.2, "The
# ledger of SPEC section 4"): a lost connection while an injected fault was active (or had just healed). The paused
# load of a primary kill has no writer running, so it cannot fail. Every other source error is a failure of the run:
# the pipeline itself can break the application's writes (H-014: SQLSTATE 55000, no replica identity).
CONNECTION_LOST_SQLSTATES = ("57P01", "57P02", "57P03")
FAULT_GRACE_S = 3.0


class FaultWindow:
    """Whether an injected fault is active now (or ended within FAULT_GRACE_S): faults enter and leave it."""

    def __init__(self):
        self._lock = threading.Lock()
        self.active = 0
        self.last_end = None

    def enter(self):
        with self._lock:
            self.active += 1

    def leave(self):
        with self._lock:
            self.active = max(0, self.active - 1)
            self.last_end = time.monotonic()

    def now(self) -> bool:
        with self._lock:
            return self.active > 0 or (self.last_end is not None
                                       and time.monotonic() - self.last_end < FAULT_GRACE_S)


def source_error(e: BaseException, window: Optional[FaultWindow]) -> dict:
    sqlstate = getattr(e, "sqlstate", None)
    lost = type(e).__name__ in ("OperationalError", "ConnectionTimeout") and (
        sqlstate is None or sqlstate.startswith("08") or sqlstate in CONNECTION_LOST_SQLSTATES)
    during = bool(window is not None and window.now())
    return {"type": type(e).__name__, "sqlstate": sqlstate, "message": str(e)[:200], "connection_lost": lost,
            "during_fault": during, "allowed": lost and during}


def judge_source_errors(errors: list) -> None:
    """Raise Infrastructure when every error is on the allow-list, _Outcome('error') otherwise."""
    bad = [e for e in errors if not e.get("allowed")]
    if bad:
        e = bad[0]
        raise _Outcome("error", "the workload failed at the source outside the allow-list (%s, SQLSTATE %s): %s"
                       % (e["type"], e["sqlstate"], e["message"]))
    if errors:
        raise Infrastructure("the workload lost its source connection during an injected fault: %s"
                             % errors[0]["message"])


# ------------------------------------------------------------------------------------------------- supervisor

class Supervisor:
    """Keeps one instance of the system running, as the platform's schedule does: when the instance exits (any
    reason), a new one starts after `restart_delay`. A fault can arm the next start (`arm_next`) with crash-point
    variables; the instance that consumes the arm is returned by `armed_proc(token)`."""

    def __init__(self, system, workdir: Path, fault_control: Optional[FaultControl] = None,
                 restart_delay: float = 0.3):
        self.system, self.workdir, self.fc, self.restart_delay = system, Path(workdir), fault_control, restart_delay
        self.current = None
        self.procs = []
        self.exits = []
        self._n = 0
        self._arm = []
        self._armed = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._thread = None
        self._dead_since = None

    def hits_path(self, tag: str) -> str:
        return str(self.workdir / ("hits-%s.jsonl" % tag))

    def env(self, tag: str, armed: Optional[dict] = None) -> dict:
        env = dict(crashpoints.env_for(None, log=self.hits_path(tag), tag=tag))
        if armed:
            env.update(crashpoints.env_for(armed["point"], hit=armed["hit"], action=armed["action"],
                                           log=self.hits_path(tag), tag=tag, delay_ms=armed.get("delay_ms", 0)))
        if self.fc is not None:
            env.update(self.fc.env(tag=tag))
        return env

    def _start_one(self):
        with self._lock:
            self._n += 1
            tag = "i%d" % self._n
            armed = self._arm.pop(0) if self._arm else None
        p = self.system.start(tag, self.env(tag, armed))
        with self._lock:
            self.current = p
            self.procs.append(p)
            if armed:
                self._armed[armed["token"]] = p

    def _loop(self):
        while not self._stop.is_set():
            p = self.current
            if not self._paused.is_set() and (p is None or not p.alive()):
                if p is not None and p.poll() is not None and not getattr(p, "_noted", False):
                    p._noted = True
                    self.exits.append({"tag": p.name, "exit": p.poll().as_dict(),
                                       "lived_s": round(time.monotonic() - p.started, 3)})
                if self._dead_since is None:
                    self._dead_since = time.monotonic()
                if time.monotonic() - self._dead_since >= self.restart_delay:
                    self._dead_since = None
                    try:
                        self._start_one()
                    except Exception:                      # noqa: BLE001 - tried again on the next tick
                        time.sleep(0.5)
            self._stop.wait(0.02)

    def start(self):
        self._start_one()
        self._thread = threading.Thread(target=self._loop, name="supervisor", daemon=True)
        self._thread.start()
        return self

    def arm_next(self, point: str, hit: str, action: str = "kill", delay_ms: int = 0) -> str:
        token = "arm%d" % random.getrandbits(32)
        with self._lock:
            self._arm.append({"token": token, "point": point, "hit": hit, "action": action, "delay_ms": delay_ms})
        return token

    def armed_proc(self, token):
        return self._armed.get(token)

    def kill_current(self):
        p = self.current
        if p is not None and p.alive():
            p.kill_tree()
            try:
                p.wait(10)
            except procs.DeadlineExceeded:
                pass
        return p

    def pause(self):
        """Nested: overlapping faults each pause and resume; restarts resume when the last one resumes."""
        with self._lock:
            self._pauses = getattr(self, "_pauses", 0) + 1
            self._paused.set()

    def resume(self):
        with self._lock:
            self._pauses = max(0, getattr(self, "_pauses", 0) - 1)
            if not self._pauses:
                self._paused.clear()

    def stop(self, timeout=15):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
        for p in self.procs:
            if p.alive() or not p.group_gone:
                p.kill_tree()
        for p in self.procs:
            try:
                p.wait(timeout)
            except procs.DeadlineExceeded:
                pass
        procs.assert_no_survivors(self.procs, timeout)


# ------------------------------------------------------------------------------------------------- load

class Load:
    """Rounds of the seeded workload in a thread, paced; `pause()` returns at a round boundary."""

    def __init__(self, workload, pace_s: float = 0.25):
        self.wl, self.pace_s = workload, pace_s
        self.rounds = 0
        self.errors = []
        self.window = FaultWindow()
        self._run = threading.Event()
        self._idle = threading.Event()
        self._idle.set()
        self._stop = threading.Event()
        self._gate = threading.Lock()
        self._thread = threading.Thread(target=self._loop, name="load", daemon=True)
        self._thread.start()

    def _loop(self):
        while not self._stop.is_set():
            with self._gate:
                if not self._run.is_set():
                    self._idle.set()
                    go = False
                else:
                    self._idle.clear()
                    go = True
            if not go:
                self._run.wait(0.05)
                continue
            try:
                self.wl.run_round()
                self.rounds += 1
            except Exception as e:                         # noqa: BLE001 - the run records it
                self.errors.append(source_error(e, self.window))
                self._run.clear()
                continue
            if self.pace_s:
                time.sleep(self.pace_s)

    def resume(self):
        self._run.set()

    def pause(self, timeout=300):
        with self._gate:
            self._run.clear()
        if not self._idle.wait(timeout):
            raise DeadlineExceeded("the workload round did not end")

    def stop(self):
        self._run.clear()
        self._stop.set()
        self._thread.join(timeout=300)


# ------------------------------------------------------------------------------------------------- the run

class RunConfig:
    def __init__(self, fuzzer="workload", budget_s=60.0, episode_s=10.0, catch_up_s=90.0, pace_s=0.25,
                 observer_interval_s=0.005, schedule=None, keep_workdir=False, start_timeout_s=120.0, abort=None,
                 ignore_self_report=False, observers=True):
        self.ignore_self_report = ignore_self_report  # do not end a run on the system's own divergence report
        self.observers = observers                    # False: no consumer loops, the oracle alone decides
        self.abort = abort                     # threading.Event: stop at the next episode, record nothing
        self.fuzzer, self.budget_s, self.episode_s = fuzzer, budget_s, episode_s
        self.catch_up_s, self.pace_s, self.observer_interval_s = catch_up_s, pace_s, observer_interval_s
        self.schedule = schedule               # faultkinds.Schedule for the fault fuzzer
        self.keep_workdir = keep_workdir
        self.start_timeout_s = start_timeout_s

    def to_dict(self):
        d = {k: v for k, v in self.__dict__.items() if k not in ("schedule", "abort")}
        d["schedule"] = self.schedule.to_dict() if self.schedule is not None else None
        return d


class Run:
    def __init__(self, system, seed: int, workdir: Path, config: RunConfig, repo: Path):
        self.system, self.seed, self.workdir, self.cfg = system, seed, Path(workdir), config
        self.repo = repo
        self.rec = {"fuzzer": config.fuzzer, "system": system.name, "self_test": bool(system.is_self_test),
                    "tier": system.tier, "source_mode": system.source_node, "seed": seed, **git_state(repo),
                    "start": utc_now(), "outcome": None, "reason": "", "failure_id": None,
                    "counts": {}, "faults": {}, "no_effect": {}, "config": config.to_dict()}
        self.fault_log = []
        self.checkpoints = []
        self.first_failure_s = None
        self.t0 = time.monotonic()

    def _elapsed(self):
        return round(time.monotonic() - self.t0, 3)

    def _watch(self, observers_):
        """A consumer loop's violation ends the run at once (its time is the time to detection)."""
        if observers_ is not None and observers_.violations():
            raise _Outcome("violation", "consumer loop: %s" % _violation_summary(observers_))

    def checkpoint(self, load, sup, observers_, final=False):
        cp = {"t": self._elapsed(), "final": final}
        self._watch(observers_)
        load.pause()
        q = self.system.quiesce()
        cp["q"] = lsn.format(q)
        t = time.monotonic()
        since = time.time()
        deadline = Deadline.after(self.cfg.catch_up_s)
        while True:
            try:
                self.system.wait_for_applied(q, deadline.within(2.0))
                break
            except DeadlineExceeded as e:
                self._watch(observers_)
                said = "" if self.cfg.ignore_self_report else self.system.divergence_reported(since)
                if said:
                    cp["catch_up_s"] = round(time.monotonic() - t, 3)
                    self.checkpoints.append(cp)
                    raise _Outcome("diverged", "the system reports that the destination diverged from the source "
                                   "(%s), stopped short of %s" % (said, lsn.format(q))) from None
                if deadline.expired:
                    cp["catch_up_s"] = round(time.monotonic() - t, 3)
                    self.checkpoints.append(cp)
                    raise _Outcome("stuck", "no catch-up to %s within %.0f s: %s" % (
                        lsn.format(q), self.cfg.catch_up_s, e)) from None
        cp["catch_up_s"] = round(time.monotonic() - t, 3)
        r = self.system.compare(q)
        cp["equal"] = bool(r.equal)
        cp["compare_s"] = round(sum(getattr(tr, "seconds", 0) for tr in r.tables), 3)
        self.checkpoints.append(cp)
        if not r.equal:
            self.oracle_result = r.to_dict()
            raise _Outcome("diverged", "oracle: %s" % _diff_summary(self.oracle_result))
        self.final_rows = sum((tr.source_rows or 0) for tr in r.tables)
        if observers_ is not None and observers_.violations():
            raise _Outcome("violation", "consumer loop: %s" % _violation_summary(observers_))
        judge_source_errors(load.errors)
        if not final:
            load.resume()

    def execute(self) -> dict:
        system, cfg = self.system, self.cfg
        sup = load = observers_ = scheduler = None
        self.oracle_result = None
        self.final_rows = None
        try:
            try:
                system.setup(self.workdir, self.seed)
            except Exception as e:                         # noqa: BLE001
                raise Infrastructure("setup: %s: %s" % (type(e).__name__, str(e)[:300])) from None
            fc = FaultControl(self.workdir / "faults")
            sup = Supervisor(system, self.workdir, fc).start()
            t = time.monotonic()
            while system.applied_position() is None:
                if time.monotonic() - t > cfg.start_timeout_s:
                    raise _Outcome("stuck", "the first instance did not reach a position within %.0f s"
                                   % cfg.start_timeout_s)
                time.sleep(0.05)
            wl = system.wl
            checks = obs.checks_for(wl.cfg.features, wl.cfg.schema, wl.transfer_table_names, wl.transfer_sum)
            observers_ = obs.Observers(checks, system.observer_connect, cfg.observer_interval_s).start() \
                if checks and cfg.observers else None
            load = Load(wl, cfg.pace_s)
            rng = random.Random("%d/run" % self.seed)
            end = time.monotonic() + cfg.budget_s
            episode = 0
            while time.monotonic() < end:
                if cfg.abort is not None and cfg.abort.is_set():
                    raise _Outcome("aborted", "the campaign was stopped")
                episode += 1
                load.resume()
                if cfg.schedule is not None:
                    from .faultkinds import Episode
                    scheduler = Episode(cfg.schedule.plan(rng, cfg.episode_s), system, sup, fc, load, self.fault_log)
                    th = threading.Thread(target=scheduler.run, args=(cfg.episode_s,), name="episode", daemon=True)
                    th.start()
                    while th.is_alive():
                        th.join(0.2)
                        self._watch(observers_)
                    scheduler = None
                else:
                    stop_at = time.monotonic() + min(cfg.episode_s, max(0.0, end - time.monotonic()))
                    while time.monotonic() < stop_at:
                        time.sleep(0.2)
                        self._watch(observers_)
                self.checkpoint(load, sup, observers_, final=time.monotonic() >= end)
            if not self.checkpoints or not self.checkpoints[-1]["final"]:
                self.checkpoint(load, sup, observers_, final=True)
            self.rec["outcome"] = "pass"
        except _Outcome as o:
            if o.outcome == "aborted":
                self.rec["outcome"], self.rec["reason"] = "aborted", o.reason
            else:
                self._fail(o.outcome, o.reason)
        except Infrastructure as e:
            self.rec["outcome"], self.rec["reason"] = "infrastructure", scrub(str(e))
        except Exception as e:                             # noqa: BLE001
            self._fail("error", "%s: %s | %s" % (type(e).__name__, str(e)[:300],
                                                 traceback.format_exc(limit=4).replace("\n", " / ")[-800:]))
        finally:
            self._finish(sup, load, observers_)
        return self.rec

    def _fail(self, outcome, reason):
        if self.first_failure_s is None:
            self.first_failure_s = self._elapsed()
        self.rec["outcome"], self.rec["reason"] = outcome, scrub(reason)

    def _finish(self, sup, load, observers_):
        if load is not None:
            try:
                load.stop()
            except Exception:                              # noqa: BLE001
                pass
        if observers_ is not None:
            self.rec["observers"] = observers_.stop()
            if self.rec["outcome"] == "pass" and observers_.violations():
                self._fail("violation", "consumer loop: %s" % _violation_summary(observers_))
            blind = [o["check"] for o in self.rec["observers"] if o["observations"] < MIN_OBSERVATIONS]
            if self.rec["outcome"] == "pass" and blind:
                self._fail("error", "consumer loops that observed fewer than %d times prove nothing: %s"
                           % (MIN_OBSERVATIONS, ", ".join(blind)))
        if sup is not None:
            try:
                sup.stop()
            except procs.SurvivorError as e:
                if self.rec["outcome"] == "pass":
                    self._fail("error", "survivor: %s" % e)
            self.rec["instances"] = {"started": len(sup.procs), "exits": Counter(
                (e["exit"].get("signal_name") or "code %s" % e["exit"].get("code")) for e in sup.exits)}
        extra = {}
        try:
            extra = self.system.run_counts()
        except Exception:                                  # noqa: BLE001
            pass
        try:
            self.system.teardown()
        except Exception as e:                             # noqa: BLE001
            if self.rec["outcome"] == "pass":
                self._fail("error", "teardown: %s" % e)
        faults, no_effect = Counter(), Counter()
        for f in self.fault_log:
            (faults if f.get("effect") else no_effect)[f["kind"]] += 1
        if self.cfg.fuzzer == "stream":
            faults["stream_bytes"] = extra.get("stream_faults", 0)
            self.rec["stream_detected"] = extra.get("stream_detected", 0)
        wl = getattr(self.system, "wl", None)
        stats = dict(wl.stats) if wl is not None else {}
        self.rec.update(
            duration_s=self._elapsed(), faults=dict(faults), no_effect=dict(no_effect),
            counts={"rounds": wl.round if wl is not None else 0,
                    "transactions": sum(v for k, v in stats.items() if k.startswith("txn_")),
                    "rolled_back": stats.get("txn_rollback", 0) + stats.get("txn_large_rollback", 0),
                    "final_rows": self.final_rows, "checkpoints": len(self.checkpoints),
                    "observations": sum(o["observations"] for o in self.rec.get("observers", []))},
            checkpoints=self.checkpoints[-20:], first_failure_s=self.first_failure_s,
            faults_without_effect=[{k: f.get(k) for k in ("kind", "params", "error", "proof")}
                                   for f in self.fault_log if not f.get("effect")][:20])
        if self.oracle_result is not None:
            self.rec["oracle"] = _oracle_brief(self.oracle_result)


class _Outcome(Exception):
    def __init__(self, outcome, reason):
        super().__init__(reason)
        self.outcome, self.reason = outcome, reason


def _diff_summary(d: dict) -> str:
    bad = [t for t in d.get("tables", []) if not t.get("equal")]
    return "; ".join("%s missing %s extra %s different %s" % (t.get("source"), t.get("missing_rows"),
                                                              t.get("extra_rows"), t.get("different_rows"))
                     for t in bad[:6]) or "not equal"


def _oracle_brief(d: dict) -> dict:
    return {"equal": d.get("equal"), "tables": [
        {k: t.get(k) for k in ("source", "equal", "source_rows", "destination_rows", "missing_rows", "extra_rows",
                               "different_rows", "missing_on_destination")}
        for t in d.get("tables", []) if not t.get("equal")]}


def _violation_summary(observers_) -> str:
    out = []
    for lp in observers_.loops:
        r = lp.report
        if r.violations:
            out.append("%s %d of %d (%s)" % (r.check, r.violations, r.observations,
                                             json.dumps(r.first_violations[:1], default=str)[:300]))
    return "; ".join(out)


def bundle(run: Run, dest: Path) -> Path:
    """Copy what a failure needs to be understood and replayed (generated data only) to `dest` (local, scrubbed
    when it is written as evidence)."""
    dest.mkdir(parents=True, exist_ok=True)
    for name in ("replay.jsonl", "events.jsonl", "toy.json"):
        p = run.workdir / name
        if p.exists():
            shutil.copy(p, dest / name)
    fl = run.workdir / "faults" / "faults.log.jsonl"
    if fl.exists():
        shutil.copy(fl, dest / "faults.log.jsonl")
    for p in sorted(run.workdir.glob("i*.log"))[-4:] + sorted(run.workdir.glob("a*.log"))[-2:]:
        shutil.copy(p, dest / p.name)
    (dest / "record.json").write_text(json.dumps(run.rec, indent=1, sort_keys=True, default=str))
    (dest / "fault_log.json").write_text(json.dumps(run.fault_log, indent=1, sort_keys=True, default=str))
    if run.oracle_result is not None:
        (dest / "oracle.json").write_text(json.dumps(run.oracle_result, indent=1, sort_keys=True, default=str))
    return dest


def run_once(system, seed: int, workdir: Path, config: RunConfig, repo: Path,
             on_failure: Optional[Callable] = None) -> dict:
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    run = Run(system, seed, workdir, config, repo)
    rec = run.execute()
    if rec["outcome"] in FAILURES and on_failure is not None:
        on_failure(run)
    if not config.keep_workdir and rec["outcome"] not in FAILURES:
        shutil.rmtree(workdir, ignore_errors=True)
    run.rec = rec
    return rec


__all__ = ["OUTCOMES", "FAILURES", "Run", "RunConfig", "Supervisor", "Load", "run_once", "bundle", "git_state",
           "Infrastructure"]
_ = os
