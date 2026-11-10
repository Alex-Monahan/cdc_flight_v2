"""The leadership fuzzer: rounds of seeded actions over at least three competing runs, under workload.

One run of this fuzzer = one seed = one scenario with `rounds` rounds. The system's platform (contract C6; the toy
brings a minimal one) starts a run every `tick_s`, as the every-minute schedule does; a newcomer that finds a
healthy leader exits. On top, each round draws actions: start (also several at once), pause for a random time
(shorter and longer than the stale age), kill, cancel as the platform does it, a planned hand-over at a short
maximum run time, and faults of the fault fuzzer that need no supervisor (network, source, destination, spill).

Invariants, checked at the end of every round:

1. Never two writers: over the whole writer log (system.writer_log(): order, epoch, run, position, xid, kind) the
   epochs never fall, positions rise strictly (a resynchronisation row may repeat the last position), every source
   transaction appears once, and a run writes under one epoch only. The log is written by the data transactions
   themselves, so this check does not rest on the fence it tests.
2. Never a lost or duplicated change: oracle equality at a position at the end of every round.
3. Progress: after the last disturbance some run leads and catches up within `catch_up_s`.
4. A newcomer that finds a healthy leader exits within `newcomer_exit_s`.
"""
from __future__ import annotations

import random
import threading
import time
import traceback

from cdc_v2 import lsn
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.testing.harness_evidence import scrub

from . import faultkinds as fk
from . import observers as obs
from .run import FAILURES, Infrastructure, Load, git_state, judge_source_errors, utc_now
from .run import _Outcome as _RunOutcome

PLATFORM_FAULTS = ["net_drop", "net_reset", "net_blackhole", "net_latency", "src_terminate", "dest_error",
                   "dest_delay", "spill_full"]


def check_writer_log(rows: list) -> list:
    """Problems in the writer log (empty = none). rows: (order, epoch, run, position, xid, kind) by order."""
    problems = []
    last_epoch, last_pos, seen, run_epoch, epoch_run = None, None, set(), {}, {}
    for n, epoch, run, pos, xid, kind in rows:
        if last_epoch is not None and epoch < last_epoch:
            problems.append({"two_writers": True, "order": n, "epoch": epoch, "after_epoch": last_epoch, "run": run})
        if run in run_epoch and run_epoch[run] != epoch:
            problems.append({"run_with_two_epochs": run, "epochs": [run_epoch[run], epoch]})
        run_epoch.setdefault(run, epoch)
        if epoch in epoch_run and epoch_run[epoch] != run:
            problems.append({"epoch_with_two_runs": epoch, "runs": [epoch_run[epoch], run], "order": n})
        epoch_run.setdefault(epoch, run)
        if kind == "takeover" and last_epoch is not None and epoch <= last_epoch:
            problems.append({"takeover_without_new_epoch": epoch, "order": n, "run": run})
        if last_pos is not None and (pos < last_pos or (kind == "txn" and pos == last_pos)):
            problems.append({"position_not_rising": True, "order": n, "position": lsn.format(int(pos)),
                             "after": lsn.format(int(last_pos)), "kind": kind})
        if kind == "txn":
            if pos in seen:
                problems.append({"transaction_twice": lsn.format(int(pos)), "order": n})
            seen.add(pos)
        last_epoch = max(epoch, last_epoch) if last_epoch is not None else epoch
        last_pos = max(pos, last_pos) if last_pos is not None else pos
        if len(problems) >= 10:
            break
    return problems


class _PlatformSup:
    """Enough of a supervisor for the faults that act on links, the source or the destination."""

    def __init__(self, platform):
        self.platform = platform

    @property
    def current(self):
        alive = self.platform.alive()
        return alive[-1].proc if alive else None

    def pause(self):
        pass

    def resume(self):
        pass

    def kill_current(self):
        return None


class LeadershipRun:
    def __init__(self, system, seed, workdir, repo, rounds=10, round_s=6.0, tick_s=1.0, catch_up_s=60.0,
                 newcomer_exit_s=10.0, pace_s=0.2, fault_prob=0.3):
        self.system, self.seed, self.workdir, self.repo = system, seed, workdir, repo
        self.rounds, self.round_s, self.tick_s = rounds, round_s, tick_s
        self.catch_up_s, self.newcomer_exit_s, self.pace_s, self.fault_prob = catch_up_s, newcomer_exit_s, pace_s, \
            fault_prob
        self.rec = {"fuzzer": "leadership", "system": system.name, "self_test": bool(system.is_self_test),
                    "tier": system.tier, "source_mode": system.source_node, "seed": seed, **git_state(repo),
                    "start": utc_now(), "outcome": None, "reason": "", "failure_id": None, "faults": {},
                    "no_effect": {}, "config": {"rounds": rounds, "round_s": round_s, "tick_s": tick_s,
                                                "catch_up_s": catch_up_s, "newcomer_exit_s": newcomer_exit_s}}
        self.actions = []
        self.fault_log = []
        self.oracle_result = None
        self.first_failure_s = None
        self.t0 = time.monotonic()

    def _t(self):
        return round(time.monotonic() - self.t0, 3)

    def execute(self):
        system = self.system
        platform = load = observers_ = None
        done_rounds = 0
        tick_stop = threading.Event()
        try:
            try:
                system.setup(self.workdir, self.seed)
            except Exception as e:                         # noqa: BLE001
                raise Infrastructure("setup: %s: %s" % (type(e).__name__, str(e)[:300])) from None
            from cdc_v2.testing.faults import FaultControl
            fc = FaultControl(self.workdir / "faults")
            platform = system.platform()
            env = fc.env(tag="")

            def ticker():
                while not tick_stop.is_set():
                    platform.poll()
                    platform.start_run(env)
                    tick_stop.wait(self.tick_s)
            threading.Thread(target=ticker, name="platform-schedule", daemon=True).start()
            t = time.monotonic()
            while system.applied_position() is None:
                if time.monotonic() - t > 120:
                    raise _Fail("stuck", "no run reached a position within 120 s")
                time.sleep(0.05)
            wl = system.wl
            checks = obs.checks_for(wl.cfg.features, wl.cfg.schema, wl.transfer_table_names, wl.transfer_sum)
            observers_ = obs.Observers(checks, system.observer_connect, 0.005).start() if checks else None
            load = Load(wl, self.pace_s)
            rng = random.Random("%d/leadership" % self.seed)
            sup = _PlatformSup(platform)
            for r in range(self.rounds):
                load.resume()
                self._round(r, rng, platform, env, sup, fc, load)
                load.pause()
                q = system.quiesce()
                try:
                    system.wait_for_applied(q, Deadline.after(self.catch_up_s))
                except DeadlineExceeded:
                    raise _Fail("stuck", "round %d: no run led and caught up to %s within %.0f s"
                                % (r, lsn.format(q), self.catch_up_s)) from None
                res = system.compare(q)
                if not res.equal:
                    self.oracle_result = res.to_dict()
                    raise _Fail("diverged", "round %d: oracle not equal" % r)
                problems = check_writer_log(system.writer_log())
                if problems:
                    raise _Fail("violation", "round %d: writer log: %s" % (r, problems[:3]))
                slow = self._slow_newcomers(platform)
                if slow:
                    raise _Fail("violation", "round %d: newcomers that found a healthy leader exited late: %s"
                                % (r, slow[:3]))
                if observers_ is not None and observers_.violations():
                    raise _Fail("violation", "round %d: consumer loop" % r)
                try:
                    judge_source_errors(load.errors)
                except _RunOutcome as o:
                    raise _Fail(o.outcome, o.reason) from None
                done_rounds += 1
            self.rec["outcome"] = "pass"
        except _Fail as f:
            self.first_failure_s = self._t()
            self.rec["outcome"], self.rec["reason"] = f.outcome, scrub(f.reason)
        except Infrastructure as e:
            self.rec["outcome"], self.rec["reason"] = "infrastructure", scrub(str(e))
        except Exception as e:                             # noqa: BLE001
            self.first_failure_s = self._t()
            self.rec["outcome"] = "error"
            self.rec["reason"] = scrub("%s: %s | %s" % (type(e).__name__, str(e)[:300],
                                                       traceback.format_exc(limit=4).replace("\n", " / ")[-600:]))
        finally:
            tick_stop.set()
            if load is not None:
                load.stop()
            if observers_ is not None:
                self.rec["observers"] = observers_.stop()
            runs = 0
            if platform is not None:
                runs = len(platform.runs)
                try:
                    platform.stop_all()
                except Exception as e:                     # noqa: BLE001
                    if self.rec["outcome"] == "pass":
                        self.rec["outcome"], self.rec["reason"] = "error", "survivor: %s" % e
            try:
                system.teardown()
            except Exception:                              # noqa: BLE001
                pass
            faults, no_effect = {}, {}
            for f in self.fault_log:
                d = faults if f.get("effect") else no_effect
                d[f["kind"]] = d.get(f["kind"], 0) + 1
            wl = getattr(system, "wl", None)
            stats = dict(wl.stats) if wl is not None else {}
            self.rec.update(duration_s=self._t(), faults=faults, no_effect=no_effect, first_failure_s=self.first_failure_s,
                            counts={"rounds": done_rounds, "runs_started": runs,
                                    "actions": len(self.actions),
                                    "transactions": sum(v for k, v in stats.items() if k.startswith("txn_")),
                                    "workload_rounds": wl.round if wl is not None else 0},
                            actions=self.actions[-40:])
        return self.rec

    def _leader(self, alive):
        """The alive run that took over last (by the system's own events), or None."""
        last = {}
        for e in getattr(self.system, "events", lambda: [])():
            if e.get("kind") == "took_over":
                last[e["tag"]] = e.get("t", 0)
        cands = [r for r in alive if r.tag in last]
        return max(cands, key=lambda r: last[r.tag]) if cands else None

    def _slow_newcomers(self, platform):
        """Runs that never took over and lived longer than newcomer_exit_s (alive or ended), unless this fuzzer
        paused, killed or cancelled them (their life is then ours, not theirs)."""
        leaders = {e["tag"] for e in getattr(self.system, "events", lambda: [])() if e.get("kind") == "took_over"}
        touched = {a.get("run") for a in self.actions if a.get("action") in ("pause", "kill", "cancel")}
        now = time.monotonic()
        slow = []
        for r in platform.runs:
            if r.tag in leaders or r.tag in touched:
                continue
            took = (r.ended_at or now) - r.started
            if took > self.newcomer_exit_s:
                slow.append({"run": r.tag, "alive": r.ended_at is None, "seconds": round(took, 2)})
        return slow

    def _round(self, r, rng, platform, env, sup, fc, load):
        t_end = time.monotonic() + self.round_s
        n = rng.randint(3, 8)
        offsets = sorted(rng.uniform(0, self.round_s * 0.8) for _ in range(n))
        threads = []
        t0 = time.monotonic()
        for off in offsets:
            delay = off - (time.monotonic() - t0)
            if delay > 0:
                time.sleep(delay)
            kind = rng.choices(["start", "pause", "kill", "cancel", "handover", "fault"], [3, 4, 2, 2, 1,
                                                                                            3 * self.fault_prob])[0]
            alive = platform.alive()
            act = {"round": r, "t": self._t(), "action": kind}
            if kind == "start":
                k = rng.choice([1, 1, 2, 3])
                act["runs"] = [platform.start_run(env).tag for _ in range(k)]
            elif kind == "handover":
                act["runs"] = [platform.start_run(env, max_runtime_s=round(rng.uniform(1.0, 3.0), 2)).tag]
            elif kind in ("pause", "kill", "cancel") and alive:
                run = rng.choice(alive)
                leader = self._leader(alive)
                if kind == "pause" and leader is not None and rng.random() < 0.7:
                    run = leader                   # a paused leader that resumes after a take-over: the fence's case
                act["run"] = run.tag
                if kind == "kill":
                    run.proc.kill_tree()
                elif kind == "cancel":
                    platform.cancel(run)
                else:
                    dur = rng.uniform(0.3, 1.5) if rng.random() < 0.4 else rng.uniform(2.5, 5.0)
                    act["seconds"] = round(dur, 2)
                    if not _signal(run.proc.stop_group):
                        act["skipped"] = "the run's group was gone"

                    def cont(p=run.proc, d=dur):
                        time.sleep(d)
                        _signal(p.cont_group)
                    th = threading.Thread(target=cont, daemon=True)
                    th.start()
                    threads.append(th)
            elif kind == "fault":
                kinds = [k for k in PLATFORM_FAULTS if self.system.supports(k)]
                if kinds:
                    fault, dur = fk.make_fault(rng.choice(kinds), rng, self.system)
                    act["fault"] = fault.kind
                    ep = fk.Episode([(0.0, dur, fault)], self.system, sup, fc, load, self.fault_log)
                    th = threading.Thread(target=ep.run, args=(dur + 0.1,), daemon=True)
                    th.start()
                    threads.append(th)
            else:
                act["skipped"] = "no run alive"
            self.actions.append(act)
        left = t_end - time.monotonic()
        if left > 0:
            time.sleep(left)
        for th in threads:
            th.join(60)


def _signal(fn) -> bool:
    """A signal to a run's process group. macOS answers EPERM (not ESRCH) for a group whose leader has just exited
    and whose members are zombies; that run is gone, which is not an error of the fuzzer."""
    try:
        return bool(fn())
    except PermissionError:
        return False


class _Fail(Exception):
    def __init__(self, outcome, reason):
        super().__init__(reason)
        self.outcome, self.reason = outcome, reason


__all__ = ["LeadershipRun", "check_writer_log", "FAILURES"]
