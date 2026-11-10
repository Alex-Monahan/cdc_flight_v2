"""Crash runner: kills a system under test at every crash point, at random times, or freezes it, then restarts it and
checks the result (SPEC 1.7, 4.2, 4.5; the fault and leadership fuzzers of SPEC §4).

The runner knows nothing about the pipeline. It drives an `Adapter`, which a test supplies:

    class MyAdapter(Adapter):
        name = "toy-correct"
        def crash_points(self): ...            # names the system under test declares
        def setup(self, workdir, seed): ...    # fresh source and destination for one scenario
        def start(self, tag, env): ...         # start one instance with extra env; returns procs.Proc
        def workload(self, step, seed): ...    # one seeded workload step on the source
        def wait_caught_up(self, timeout): ... # raise (e.g. procs.DeadlineExceeded) if not caught up in time
        def check(self): ...                   # {"ok": bool, ...details}; later the oracle

    runner = CrashRunner(MyAdapter, workdir, ledger_path="evidence/.../ledger.jsonl")
    runner.run_crash_points(seeds=(1, 2, 3))
    runner.run_random_kills(seeds=(1, 2, 3))
    runner.run_freeze(seeds=(1, 2, 3))
    runner.write_summary("evidence/.../summary.json")

Modes and outcomes (one ledger line per scenario):

``crash_point``  for each point and seed: start with the point armed (hit chosen from the seed), feed workload until
                 the process dies. It must die by SIGKILL with the point's ``fire`` line in the hit log, otherwise the
                 outcome is ``not_reached`` or ``wrong_exit`` (failures, never passes). Then restart without the
                 switch, wait until caught up, kill, check.
``random_kill``  start, kill the process group at a random time from the seed, restart; several times; then catch up
                 and check. The ledger lists the last point each killed instance reached.
``freeze``       SIGSTOP the instance (at a random time, or at a crash point with action ``stop``), optionally start a
                 second instance while it is frozen, SIGCONT, catch up, check.

Outcomes: ``pass``; failures ``not_reached``, ``wrong_exit``, ``no_catch_up``, ``diverged`` (check failed),
``survivor`` (a process outlived the scenario), ``error`` (anything else).

The ledger holds no absolute path and no user name: every line goes through `harness_evidence.write_evidence`.
"""
import concurrent.futures
import json
import os
import random
import re
import sys
import threading
import time
import traceback
import zlib

from cdc_v2 import crashpoints
from cdc_v2.testing import procs
from cdc_v2.testing.harness_evidence import register_root, scrub, write_evidence

FAILURES = ("not_reached", "wrong_exit", "no_catch_up", "diverged", "survivor", "error")


class Adapter:
    """Interface the runner drives. One instance per scenario (the runner calls the factory each time)."""

    name = "adapter"

    def crash_points(self):
        raise NotImplementedError

    def max_hit(self, point):
        """Highest hit the runner may arm for ``point``; None = Config.max_hit. Default: 1 for points declared with
        ``once=True`` in this process's registry."""
        cp = crashpoints.get(point)
        return 1 if cp is not None and cp.once else None

    def setup(self, workdir, seed):
        raise NotImplementedError

    def start(self, tag, env):
        raise NotImplementedError

    def workload(self, step, seed):
        raise NotImplementedError

    def wait_caught_up(self, timeout):
        raise NotImplementedError

    def check(self):
        raise NotImplementedError

    def teardown(self):
        pass


class Config:
    def __init__(self, max_hit=3, crash_timeout=20.0, catch_up_timeout=30.0, workload_interval=0.05,
                 max_workload_steps=400, kills=4, kill_after=(0.05, 0.8), freeze_after=(0.1, 0.6),
                 freeze_for=(0.3, 1.2), settle_after_cont=0.3, workers=1, stop_timeout=10.0):
        self.max_hit = max_hit
        self.crash_timeout = crash_timeout
        self.catch_up_timeout = catch_up_timeout
        self.workload_interval = workload_interval
        self.max_workload_steps = max_workload_steps
        self.kills = kills
        self.kill_after = kill_after
        self.freeze_after = freeze_after
        self.freeze_for = freeze_for
        self.settle_after_cont = settle_after_cont
        self.workers = workers
        self.stop_timeout = stop_timeout

    def as_dict(self):
        return dict(self.__dict__)


def hit_seed(point, seed):
    """The 64-bit seed of ``CDC_V2_CRASH_HIT=seed:S:M`` for a point and a scenario seed (differs per point)."""
    return (zlib.crc32(point.encode()) * 1_000_003 + seed) & ((1 << 64) - 1)


class _Scenario:
    """State of one scenario: adapter, fleet, hit log, workload pacing."""

    def __init__(self, runner, mode, point, seed):
        self.runner, self.mode, self.point, self.seed = runner, mode, point, seed
        label = re.sub(r"[^A-Za-z0-9_.-]", "_", "%s-%s-%d" % (mode, point or "any", seed))
        self.dir = os.path.join(runner.workdir, label)
        os.makedirs(self.dir, exist_ok=True)
        self.hits_path = os.path.join(self.dir, "hits.jsonl")
        self.adapter = runner.factory()
        self.fleet = procs.Fleet()
        self.step = 0
        self.last_feed = 0.0
        self.t0 = time.monotonic()
        self.rec = {"adapter": self.adapter.name, "mode": mode, "point": point, "seed": seed, "outcome": None,
                    "reason": "", "timings": {}}

    def feed(self, force=False):
        cfg = self.runner.cfg
        now = time.monotonic()
        if not force and (now - self.last_feed < cfg.workload_interval or self.step >= cfg.max_workload_steps):
            return
        self.adapter.workload(self.step, self.seed)
        self.step += 1
        self.last_feed = now

    def feed_for(self, seconds):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            self.feed()
            time.sleep(min(0.01, max(0.0, end - time.monotonic())))

    def start(self, tag, crash_env=None):
        env = dict(crash_env or crashpoints.env_for(None, log=self.hits_path, tag=tag))
        p = self.adapter.start(tag, env)
        self.fleet.add(p)
        return p

    def hits(self, tag=None):
        return [h for h in crashpoints.read_log(self.hits_path) if tag is None or h.get("tag") == tag]

    def timing(self, key, since):
        self.rec["timings"][key] = round(time.monotonic() - since, 3)

    def catch_up_and_check(self):
        t = time.monotonic()
        self.feed(force=True)
        try:
            self.adapter.wait_caught_up(self.runner.cfg.catch_up_timeout)
        except Exception as exc:        # noqa: BLE001 - any failure to catch up is the outcome
            self.timing("catch_up_s", t)
            return self.fail("no_catch_up", "%s: %s" % (type(exc).__name__, exc))
        self.timing("catch_up_s", t)
        return self.stop_and_check()

    def stop_and_check(self):
        try:
            statuses = self.fleet.kill_all(self.runner.cfg.stop_timeout)
            self.rec["exits_at_stop"] = {k: v.as_dict() for k, v in statuses.items()}
        except procs.SurvivorError as exc:
            return self.fail("survivor", str(exc))
        result = self.adapter.check()
        self.rec["check"] = result
        if result.get("ok"):
            self.rec["outcome"] = "pass"
        else:
            self.fail("diverged", result.get("reason", "check failed"))
        return self.rec

    def fail(self, outcome, reason):
        self.rec["outcome"] = outcome
        self.rec["reason"] = reason
        return self.rec

    def finish(self):
        try:
            self.fleet.kill_all(self.runner.cfg.stop_timeout)
        except procs.SurvivorError as exc:
            if self.rec["outcome"] in (None, "pass"):
                self.fail("survivor", str(exc))
        except Exception as exc:        # noqa: BLE001
            if self.rec["outcome"] in (None, "pass"):
                self.fail("error", "stop: %s: %s" % (type(exc).__name__, exc))
        try:
            self.adapter.teardown()
        except Exception as exc:        # noqa: BLE001
            if self.rec["outcome"] in (None, "pass"):
                self.fail("error", "teardown: %s: %s" % (type(exc).__name__, exc))
        self.rec["hits"] = [{k: h.get(k) for k in ("tag", "point", "hit", "fire", "action")} for h in self.hits()]
        self.rec["workload_steps"] = self.step
        self.timing("total_s", self.t0)
        if self.rec["outcome"] is None:
            self.fail("error", "scenario ended without an outcome")
        return self.rec


class CrashRunner:
    def __init__(self, adapter_factory, workdir, ledger_path=None, config=None):
        self.factory = adapter_factory
        self.workdir = os.path.abspath(str(workdir))
        os.makedirs(self.workdir, exist_ok=True)
        register_root(self.workdir, "<run>")
        self.ledger_path = ledger_path
        self.cfg = config or Config()
        self.records = []
        self._lock = threading.Lock()
        probe = adapter_factory()
        self.adapter_name = probe.name
        self.declared = list(probe.crash_points())
        unknown = [p for p in self.declared if not crashpoints.NAME_RE.match(p)]
        if unknown:
            raise ValueError("adapter names invalid crash points: %r" % unknown)

    # -- bookkeeping -------------------------------------------------------------------------------------------
    def _record(self, rec):
        rec = json.loads(json.dumps(rec, default=str))
        rec["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            self.records.append(rec)
            if self.ledger_path:
                write_evidence(self.ledger_path, json.dumps(rec, sort_keys=True) + "\n", append=True)
        return rec

    def _run_all(self, jobs):
        """Run scenario callables, in parallel when cfg.workers > 1. Returns their records in job order."""
        out = [None] * len(jobs)

        def one(i):
            fn, mode, point, seed = jobs[i]
            sc = _Scenario(self, mode, point, seed)
            try:
                fn(sc)
            except Exception as exc:    # noqa: BLE001 - the ledger records it
                sc.fail("error", "%s: %s | %s" % (type(exc).__name__, exc,
                                                   traceback.format_exc(limit=3).replace("\n", " / ")))
            out[i] = self._record(sc.finish())

        if self.cfg.workers <= 1:
            for i in range(len(jobs)):
                one(i)
        else:
            with concurrent.futures.ThreadPoolExecutor(self.cfg.workers) as ex:
                list(ex.map(one, range(len(jobs))))
        return out

    # -- mode (a): every crash point -----------------------------------------------------------------------------
    def run_crash_points(self, points=None, seeds=(1, 2, 3), action="kill"):
        points = list(points or self.declared)
        jobs = [(lambda sc, p=p: self._crash_point(sc, p, action), "crash_point", p, s) for p in points for s in seeds]
        return self._run_all(jobs)

    def _crash_point(self, sc, point, action):
        cfg = self.cfg
        hs, mx = hit_seed(point, sc.seed), sc.adapter.max_hit(point) or cfg.max_hit
        sc.rec.update(action=action, hit_spec="seed:%d:%d" % (hs, mx), hit=crashpoints.hit_from_seed(hs, mx))
        sc.adapter.setup(sc.dir, sc.seed)
        sc.feed(force=True)
        t = time.monotonic()
        a = sc.start("a", crashpoints.env_for(point, hit=sc.rec["hit_spec"], action=action, log=sc.hits_path,
                                              tag="a"))
        deadline = t + cfg.crash_timeout
        while a.alive() and time.monotonic() < deadline:
            sc.feed()
            time.sleep(0.005)
        sc.timing("to_crash_s", t)
        fired = [h for h in sc.hits("a") if h.get("fire")]
        if a.alive():
            a.kill_and_wait(cfg.stop_timeout)
            return sc.fail("not_reached", "process did not die at %s within %.0f s (hit %d)"
                           % (point, cfg.crash_timeout, sc.rec["hit"]))
        status = a.poll()
        sc.rec["exit"] = status.as_dict()
        if not fired or fired[-1]["point"] != point:
            return sc.fail("not_reached", "process exited (%r) without firing %s; log tail: %s"
                           % (status, point, scrub(a.log_text()[-400:])))
        if action == "kill" and not status.killed:
            return sc.fail("wrong_exit", "expected SIGKILL at %s, got %r" % (point, status))
        sc.start("b")
        return sc.catch_up_and_check()

    # -- mode (b): random kills ----------------------------------------------------------------------------------
    def run_random_kills(self, seeds=(1, 2, 3), kills=None):
        n = kills or self.cfg.kills
        return self._run_all([(lambda sc: self._random_kill(sc, n), "random_kill", None, s) for s in seeds])

    def _random_kill(self, sc, n):
        cfg = self.cfg
        rng = random.Random(sc.seed)
        sc.adapter.setup(sc.dir, sc.seed)
        sc.feed(force=True)
        kills = []
        for i in range(n):
            tag = "k%d" % i
            p = sc.start(tag)
            after = rng.uniform(*cfg.kill_after)
            sc.feed_for(after)
            if not p.alive():
                return sc.fail("wrong_exit", "%s exited by itself (%r) before its kill" % (tag, p.poll()))
            p.kill_tree()
            st = p.wait(cfg.stop_timeout)
            h = sc.hits(tag)
            kills.append({"tag": tag, "after_s": round(after, 3), "exit": st.as_dict(),
                          "last_point": h[-1]["point"] if h else None, "points_hit": len(h)})
        sc.rec["kills"] = kills
        sc.start("final")
        return sc.catch_up_and_check()

    # -- mode (c): freeze and continue ---------------------------------------------------------------------------
    def run_freeze(self, seeds=(1, 2, 3), second_instance=True, at_points=None):
        jobs = []
        for s in seeds:
            if at_points:
                jobs += [(lambda sc, p=p: self._freeze(sc, second_instance, p), "freeze", p, s) for p in at_points]
            else:
                jobs.append((lambda sc: self._freeze(sc, second_instance, None), "freeze", None, s))
        return self._run_all(jobs)

    def _freeze(self, sc, second_instance, at_point):
        cfg = self.cfg
        rng = random.Random(sc.seed)
        sc.adapter.setup(sc.dir, sc.seed)
        sc.feed(force=True)
        sc.rec["second_instance"] = bool(second_instance)
        if at_point:
            hs, mx = hit_seed(at_point, sc.seed), sc.adapter.max_hit(at_point) or cfg.max_hit
            spec = "seed:%d:%d" % (hs, mx)
            sc.rec.update(hit_spec=spec, hit=crashpoints.hit_from_seed(hs, mx), action="stop")
            a = sc.start("a", crashpoints.env_for(at_point, hit=spec, action="stop", log=sc.hits_path, tag="a"))
            deadline = time.monotonic() + cfg.crash_timeout
            while not [h for h in sc.hits("a") if h.get("fire")]:
                if time.monotonic() >= deadline or not a.alive():
                    if a.alive():
                        a.kill_and_wait(cfg.stop_timeout)
                    return sc.fail("not_reached", "never stopped at %s" % at_point)
                sc.feed()
                time.sleep(0.005)
            a.wait_state("T", 5)
        else:
            a = sc.start("a")
            sc.feed_for(rng.uniform(*cfg.freeze_after))
            if not a.alive():
                return sc.fail("wrong_exit", "a exited by itself (%r) before the freeze" % (a.poll(),))
            a.stop_group()
            a.wait_state("T", 5)
        h = sc.hits("a")
        sc.rec["frozen_after_point"] = h[-1]["point"] if h else None
        frozen_for = rng.uniform(*cfg.freeze_for)
        t = time.monotonic()
        if second_instance:
            sc.start("b")
        sc.feed_for(frozen_for)
        sc.rec["frozen_s"] = round(time.monotonic() - t, 3)
        a.cont_group()
        sc.feed_for(cfg.settle_after_cont)
        if not a.alive():
            sc.rec["a_exit_after_cont"] = a.poll().as_dict()
        return sc.catch_up_and_check()

    # -- summary -------------------------------------------------------------------------------------------------
    def summary(self):
        recs = self.records
        outcomes = {}
        for r in recs:
            outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
        reached = sorted({r["point"] for r in recs if r["mode"] == "crash_point"
                          and r["outcome"] not in ("not_reached", "error")})
        by_point = {}
        for r in recs:
            if r["mode"] == "crash_point":
                d = by_point.setdefault(r["point"], {})
                d[r["outcome"]] = d.get(r["outcome"], 0) + 1
        crash_runs = [r for r in recs if r["mode"] == "crash_point"]
        missing = [p for p in self.declared if p not in reached] if crash_runs else list(self.declared)
        return {
            "adapter": self.adapter_name,
            "declared_points": self.declared,
            "reached_points": reached,
            "missing_points": missing,
            "coverage": "%d/%d" % (len(self.declared) - len(missing), len(self.declared)),
            "runs": len(recs),
            "runs_by_mode": {m: sum(1 for r in recs if r["mode"] == m) for m in sorted({r["mode"] for r in recs})},
            "outcomes": outcomes,
            "by_point": by_point,
            "failures": [{k: r.get(k) for k in ("mode", "point", "seed", "outcome", "reason")}
                         for r in recs if r["outcome"] != "pass"],
            "all_passed": bool(recs) and all(r["outcome"] == "pass" for r in recs),
            "config": self.cfg.as_dict(),
        }

    def write_summary(self, path):
        return write_evidence(path, self.summary())


def _load_factory(spec):
    import importlib

    mod_name, _, attr = spec.partition(":")
    obj = importlib.import_module(mod_name)
    for part in attr.split("."):
        obj = getattr(obj, part)
    return obj


def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser(prog="python -m cdc_v2.testing.crash", description="Run the crash harness.")
    ap.add_argument("--adapter", required=True, help="module:factory returning an Adapter")
    ap.add_argument("--mode", action="append", choices=("points", "random", "freeze"), required=True)
    ap.add_argument("--seeds", default="1,2,3")
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--ledger")
    ap.add_argument("--summary")
    ap.add_argument("--workers", type=int, default=1)
    a = ap.parse_args(argv)
    seeds = [int(s) for s in a.seeds.split(",") if s]
    runner = CrashRunner(_load_factory(a.adapter), a.workdir, a.ledger, Config(workers=a.workers))
    if "points" in a.mode:
        runner.run_crash_points(seeds=seeds)
    if "random" in a.mode:
        runner.run_random_kills(seeds=seeds)
    if "freeze" in a.mode:
        runner.run_freeze(seeds=seeds)
    s = runner.summary()
    if a.summary:
        runner.write_summary(a.summary)
    print(json.dumps({k: s[k] for k in ("adapter", "coverage", "runs", "outcomes", "all_passed")}))
    return 0 if s["all_passed"] and not s["missing_points"] else 1


if __name__ == "__main__":
    sys.exit(main())
