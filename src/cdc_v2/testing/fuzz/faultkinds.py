"""The fault kinds of the fault fuzzer and their seeded schedule.

Each fault is an object with a kind (system.FAULT_KINDS), its parameters, `inject()` and `heal()`, logged with
times. A fault counts only when its effect was observed (`effect`, with a `proof`); an injection that hit nothing
is logged with `effect: False` and counted as `no_effect` (the ledger keeps both per kind). FAULT_KINDS says how each
kind proves its effect.

`Schedule.plan(rng, episode_s)` draws the faults of one episode from weights over the kinds the system supports:
1 to 3 faults at random offsets with random durations, so faults overlap (a kill during a black hole). Kinds that
need a quiet source (a primary that is killed or lost) pause the workload while they act, so that the workload's
own connections never fail half-way through a transaction.
"""
from __future__ import annotations

import random
import threading
import time
from typing import Optional

from cdc_v2 import crashpoints
from cdc_v2.testing import procs

from .system import FAULT_KINDS

DEFAULT_WEIGHTS = {
    "kill_point": 10, "kill_point_later": 4, "kill_group": 6, "freeze": 6, "hang_point": 3,
    "net_drop": 4, "net_reset": 5, "net_blackhole": 5, "net_latency": 4,
    "src_terminate": 5, "src_cancel": 2, "src_lock": 2, "src_long_txn": 2, "src_wal_switch": 2,
    "src_restart_primary": 2,
    "dest_error": 7, "dest_delay": 4, "dest_hang": 3,
    "slot_drop": 5, "slot_invalidate": 1,
    "replica_restart": 4, "replica_promote": 1,
    "spill_full": 5,
}
QUIET_SOURCE = {"src_restart_primary", "replica_promote"}
DEST_SITES = ("md.connect", "md.execute", "md.fence", "md.commit")


class Fault:
    kind = ""

    def __init__(self, **params):
        self.params = params
        self.effect = None
        self.proof = {}
        self.error = None
        self.t_inject = self.t_heal = None

    def inject(self, ctx):
        raise NotImplementedError

    def heal(self, ctx):
        pass

    def to_dict(self):
        return {"kind": self.kind, "params": self.params, "effect": bool(self.effect), "proof": self.proof,
                "error": self.error, "t_inject": self.t_inject, "t_heal": self.t_heal}


class Ctx:
    def __init__(self, system, sup, fc, load, t0):
        self.system, self.sup, self.fc, self.load, self.t0 = system, sup, fc, load, t0

    def t(self):
        return round(time.monotonic() - self.t0, 3)

    def admin(self, node=None):
        return self.system.db.connect("admin", node or self.system.primary_node)

    def boot(self, node=None):
        return self.system.cluster.connect("bootstrap", node or self.system.primary_node, self.system.db.name)


# ------------------------------------------------------------------------------------------------- process

class KillPoint(Fault):
    kind = "kill_point"
    action = "kill"

    def inject(self, ctx):
        self.token = ctx.sup.arm_next(self.params["point"], self.params["hit"], self.action,
                                      self.params.get("delay_ms", 0))
        ctx.sup.kill_current()

    def heal(self, ctx):
        deadline = time.monotonic() + self.params.get("wait_s", 12.0)
        p = None
        while time.monotonic() < deadline:
            p = ctx.sup.armed_proc(self.token)
            if p is not None and not p.alive():
                break
            time.sleep(0.05)
        if p is None:
            self.proof = {"armed_instance": None}
            self.effect = False
            return
        if p.alive():
            p.kill_tree()
            self.proof = {"instance": p.name, "reached": False}
            self.effect = False
            return
        st = p.poll()
        fired = [h for h in crashpoints.read_log(ctx.sup.hits_path(p.name)) if h.get("fire") and h["pid"] == p.pid]
        self.proof = {"instance": p.name, "exit": st.as_dict(), "fired": fired[-1]["point"] if fired else None,
                      "hit": fired[-1]["hit"] if fired else None}
        self.effect = bool(st.killed and fired and fired[-1]["point"] == self.params["point"])


class KillPointLater(KillPoint):
    kind = "kill_point_later"
    action = "kill_later"


class HangPoint(Fault):
    kind = "hang_point"

    def inject(self, ctx):
        self.token = ctx.sup.arm_next(self.params["point"], self.params["hit"], "hang")
        ctx.sup.kill_current()

    def heal(self, ctx):
        deadline = time.monotonic() + self.params.get("wait_s", 12.0)
        p, fired = None, []
        while time.monotonic() < deadline:
            p = ctx.sup.armed_proc(self.token)
            if p is not None:
                fired = [h for h in crashpoints.read_log(ctx.sup.hits_path(p.name)) if h.get("fire")]
                if fired or not p.alive():
                    break
            time.sleep(0.05)
        if fired and p.alive():
            time.sleep(self.params.get("hang_s", 1.0))
            self.effect = p.alive()
            self.proof = {"instance": p.name, "hung_at": fired[-1]["point"], "alive_while_hung": p.alive()}
        else:
            self.effect = False
            self.proof = {"instance": p.name if p else None, "reached": bool(fired)}
        if p is not None and p.alive():
            p.kill_tree()


class KillGroup(Fault):
    kind = "kill_group"

    def inject(self, ctx):
        p = ctx.sup.current
        if p is None or not p.alive():
            self.effect = False
            self.proof = {"alive": False}
            return
        p.kill_tree()
        try:
            st = p.wait(10)
        except procs.DeadlineExceeded:
            st = None
        self.effect = bool(st and st.killed)
        self.proof = {"instance": p.name, "exit": st.as_dict() if st else None}


class Freeze(Fault):
    kind = "freeze"

    def inject(self, ctx):
        self.p = ctx.sup.current
        if self.p is None or not self.p.alive():
            self.effect = False
            return
        ctx.sup.pause()                        # a frozen process is alive; the supervisor must not double-start
        self.p.stop_group()
        try:
            self.proof = {"instance": self.p.name, "state": self.p.wait_state("T", 5)}
            self.effect = True
        except procs.DeadlineExceeded:
            self.effect = False

    def heal(self, ctx):
        p = getattr(self, "p", None)
        if p is not None:
            p.cont_group()
        ctx.sup.resume()


# ------------------------------------------------------------------------------------------------- network

class Net(Fault):
    MODES = {"net_drop": "close", "net_reset": "reset", "net_blackhole": "blackhole", "net_latency": "delay"}

    def __init__(self, kind, **params):
        super().__init__(**params)
        self.kind = kind

    _busy = {}
    _lock = threading.Lock()

    def inject(self, ctx):
        self.px = ctx.system.links()["source"]
        with Net._lock:
            if Net._busy.get(id(self.px)):
                self.skipped = True                        # overlapping faults on one link would undo each other
                return
            Net._busy[id(self.px)] = True
        self.skipped = False
        self.s0 = self.px.stats()
        mode = self.MODES[self.kind]
        if mode == "delay":
            self.px.set_fault("delay", seconds=self.params["seconds"])
        else:
            self.px.set_fault(mode)

    def heal(self, ctx):
        if getattr(self, "skipped", True):
            self.effect = False
            self.proof = {"skipped": "another network fault held the link"}
            return
        s1 = self.px.stats()
        self.px.set_fault("pass")
        with Net._lock:
            Net._busy.pop(id(self.px), None)
        d = {k: s1.get(k, 0) - self.s0.get(k, 0) for k in ("closed_by_fault", "reset_by_fault", "accept_closed",
                                                          "accept_reset", "accepted", "bytes_up", "bytes_down")}
        self.proof = dict(d, active_at_inject=self.s0.get("active"), held_up=s1.get("held_up"),
                          held_down=s1.get("held_down"))
        if self.kind == "net_drop":
            self.effect = d["closed_by_fault"] + d["accept_closed"] > 0
        elif self.kind == "net_reset":
            self.effect = d["reset_by_fault"] + d["accept_reset"] > 0
        elif self.kind == "net_blackhole":
            self.effect = (self.s0.get("active") or 0) > 0 or d["accepted"] > 0
        else:
            self.effect = d["bytes_up"] + d["bytes_down"] > 0


# ------------------------------------------------------------------------------------------------- source

class SrcSignal(Fault):
    def __init__(self, kind, **params):
        super().__init__(**params)
        self.kind = kind

    def inject(self, ctx):
        fn = "pg_terminate_backend" if self.kind == "src_terminate" else "pg_cancel_backend"
        active = "" if self.kind == "src_terminate" else " AND state = 'active'"     # a cancel needs a statement
        n, tries = 0, 0
        nodes = sorted({ctx.system.slot_node, ctx.system.primary_node})
        # a cancel acts only on a running statement: look for one for up to 3 s (a terminate acts at once)
        deadline = time.monotonic() + (3.0 if active else 0.0)
        while True:
            tries += 1
            for node in nodes:
                with ctx.admin(node) as c:
                    rows = c.execute("SELECT %s(pid) FROM pg_stat_activity WHERE application_name LIKE %%s "
                                     "AND pid <> pg_backend_pid() AND datname = current_database()%s" % (fn, active),
                                     (ctx.system.app_prefix + "%",)).fetchall()
                n += sum(1 for (ok,) in rows if ok)
            if n or time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        self.effect = n > 0
        self.proof = {"backends": n, "nodes": nodes, "tries": tries}


class SrcLock(Fault):
    kind = "src_lock"

    def inject(self, ctx):
        table = self.params["table"]
        self.done = threading.Event()
        self.result = {}

        def hold():
            try:
                with ctx.admin() as c:
                    c.execute("BEGIN")
                    c.execute("SET LOCAL lock_timeout = '3s'")
                    s, t = table.split(".", 1)
                    c.execute('LOCK TABLE "%s"."%s" IN ACCESS EXCLUSIVE MODE' % (s, t))
                    self.result["granted"] = True
                    end = time.monotonic() + self.params["seconds"]
                    waiters = 0
                    while time.monotonic() < end:
                        with ctx.admin() as c2:
                            waiters = max(waiters, c2.execute(
                                "SELECT count(*) FROM pg_locks l JOIN pg_class r ON r.oid = l.relation "
                                "JOIN pg_namespace n ON n.oid = r.relnamespace WHERE NOT l.granted AND "
                                "n.nspname = %s AND r.relname = %s", (s, t)).fetchone()[0])
                        time.sleep(0.2)
                    self.result["waiters"] = waiters
                    c.execute("COMMIT")
            except Exception as e:                         # noqa: BLE001
                self.result["error"] = type(e).__name__
            finally:
                self.done.set()
        threading.Thread(target=hold, daemon=True).start()

    def heal(self, ctx):
        self.done.wait(self.params["seconds"] + 15)
        self.effect = bool(self.result.get("granted"))
        self.proof = dict(self.result, table=self.params["table"])


class SrcLongTxn(Fault):
    kind = "src_long_txn"

    def inject(self, ctx):
        self.c = ctx.admin()
        self.c.execute("BEGIN")
        self.xid = self.c.execute("SELECT txid_current()").fetchone()[0]

    def heal(self, ctx):
        try:
            self.c.execute("COMMIT")
            self.c.close()
            self.effect = True
            self.proof = {"held_xid": True}
        except Exception as e:                             # noqa: BLE001
            self.effect = True
            self.proof = {"held_xid": True, "close": type(e).__name__}


class SrcWalSwitch(Fault):
    kind = "src_wal_switch"

    def inject(self, ctx):
        ctx.system.heartbeat()
        with ctx.boot() as c:
            before = c.execute("SELECT pg_walfile_name(pg_current_wal_lsn())").fetchone()[0]
            c.execute("SELECT pg_switch_wal()")
            c.execute("CHECKPOINT")
            ctx.system.heartbeat()
            after = c.execute("SELECT pg_walfile_name(pg_current_wal_lsn())").fetchone()[0]
        self.effect = before != after
        self.proof = {"wal_file_changed": before != after}


def _postmaster_pid(system, node):
    for n in system.cluster.status().get("nodes", []):
        if n.get("name") == node:
            return n.get("pid")
    return None


class SrcRestartPrimary(Fault):
    kind = "src_restart_primary"

    def inject(self, ctx):
        node = ctx.system.primary_node
        before = _postmaster_pid(ctx.system, node)
        ctx.system.lease.localpg("kill", node)
        ctx.system.lease.localpg("up")
        after = _postmaster_pid(ctx.system, node)
        self.effect = before is not None and after is not None and before != after
        self.proof = {"node": node, "pid_changed": self.effect}


class ReplicaRestart(Fault):
    kind = "replica_restart"

    def inject(self, ctx):
        node = "replica" if ctx.system.primary_node != "replica" else ctx.system.standby_node()
        before = _postmaster_pid(ctx.system, node)
        ctx.system.lease.localpg("kill", node)
        ctx.system.lease.localpg("up")
        after = _postmaster_pid(ctx.system, node)
        self.effect = before is not None and after is not None and before != after
        self.proof = {"node": node, "pid_changed": self.effect}


class ReplicaPromote(Fault):
    kind = "replica_promote"

    def inject(self, ctx):
        sysm = ctx.system
        old, new = sysm.primary_node, sysm.standby_node()
        ctx.sup.pause()
        try:
            sysm.lease.mark_dirty("replica promoted")
            sysm.lease.localpg("kill", old)
            out = sysm.lease.localpg("promote", new, "--json").stdout
            sysm.lease.localpg("remove", old)          # the old primary never comes back (no split brain)
            sysm.lease.localpg("add-standby", "--from", new)
            sysm.cluster.refresh()
            sysm.after_promotion(old, new)
            self.effect = True
            self.proof = {"promoted": new, "lost": old, "report": out[-200:] if out else ""}
        finally:
            ctx.sup.resume()


# ------------------------------------------------------------------------------------------------- destination

class Dest(Fault):
    def __init__(self, kind, **params):
        super().__init__(**params)
        self.kind = kind

    def inject(self, ctx):
        site, hits, skip = self.params["site"], self.params["hits"], self.params["skip"]
        if self.kind == "dest_error":
            self.rid = ctx.fc.raise_at(site, exception="duckdb.IOException", message="injected", hits=hits,
                                       skip=skip)
        elif self.kind == "dest_delay":
            self.rid = ctx.fc.delay_at(site, self.params["seconds"], hits=hits, skip=skip)
        else:
            self.rid = ctx.fc.block_at(site, seconds=self.params["seconds"], hits=hits, skip=skip)

    def heal(self, ctx):
        fired = ctx.fc.fired(rule=self.rid)
        ctx.fc.remove(self.rid)
        self.effect = bool(fired)
        self.proof = {"site": self.params["site"], "fired": len(fired)}


# ------------------------------------------------------------------------------------------------- slot

class SlotDrop(Fault):
    kind = "slot_drop"

    def inject(self, ctx):
        sysm = ctx.system
        stopped = self.params.get("while_stopped")
        if stopped:
            ctx.sup.pause()
            ctx.sup.kill_current()
        try:
            node = sysm.slot_node
            deadline = time.monotonic() + 15
            dropped = False
            while time.monotonic() < deadline and not dropped:
                with ctx.admin(node) as c:
                    row = c.execute("SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s",
                                    (sysm.slot_name,)).fetchone()
                    if row is None:
                        break
                    if row[0]:
                        c.execute("SELECT pg_terminate_backend(%s)", (row[0],))
                        time.sleep(0.05)
                        continue
                    try:
                        c.execute("SELECT pg_drop_replication_slot(%s)", (sysm.slot_name,))
                        dropped = True
                    except Exception:                      # noqa: BLE001 - taken again by the system meanwhile
                        time.sleep(0.05)
            self.effect = dropped
            self.proof = {"dropped": dropped, "while_stopped": bool(stopped), "node": node}
        finally:
            if stopped:
                ctx.sup.resume()


class SlotInvalidate(Fault):
    kind = "slot_invalidate"

    def inject(self, ctx):
        sysm = ctx.system
        node = sysm.slot_node
        p = ctx.sup.current
        ctx.sup.pause()
        if p is not None and p.alive():
            p.stop_group()
        self.proof = {"node": node}
        was_dirty = sysm.lease.record.get("dirty")
        sysm.lease.mark_dirty("slot_invalidate changed max_slot_wal_keep_size")
        try:
            sysm.lease.localpg("set", node, "max_slot_wal_keep_size=2MB")
            if node != sysm.primary_node:
                sysm.lease.localpg("set", sysm.primary_node, "max_slot_wal_keep_size=-1")
            with ctx.boot() as c:
                c.execute("CREATE TABLE IF NOT EXISTS cdc_v2_fuzz.ballast (id bigint, pad text)")
                for _ in range(6):
                    c.execute("INSERT INTO cdc_v2_fuzz.ballast SELECT g, repeat('x', 400) "
                              "FROM generate_series(1, 4000) g")
                    c.execute("SELECT pg_switch_wal()")
                c.execute("TRUNCATE cdc_v2_fuzz.ballast")
                c.execute("CHECKPOINT")
            deadline = time.monotonic() + 30
            lost = False
            while time.monotonic() < deadline and not lost:
                if node != sysm.primary_node:
                    with ctx.boot(node) as c:
                        c.execute("CHECKPOINT")
                with ctx.admin(node) as c:
                    row = c.execute("SELECT wal_status, invalidation_reason FROM pg_replication_slots "
                                    "WHERE slot_name = %s", (sysm.slot_name,)).fetchone()
                lost = row is None or row[0] == "lost" or bool(row[1])
                if not lost:
                    with ctx.boot() as c:
                        c.execute("INSERT INTO cdc_v2_fuzz.ballast SELECT g, repeat('x', 400) "
                                  "FROM generate_series(1, 4000) g")
                        c.execute("SELECT pg_switch_wal()")
                        c.execute("TRUNCATE cdc_v2_fuzz.ballast")
                        c.execute("CHECKPOINT")
                    time.sleep(0.3)
            self.effect = lost
            self.proof["invalidated"] = lost
        finally:
            ok = sysm.lease.localpg("unset", node, "max_slot_wal_keep_size", check=False).returncode == 0
            if node != sysm.primary_node:
                ok = sysm.lease.localpg("unset", sysm.primary_node, "max_slot_wal_keep_size",
                                        check=False).returncode == 0 and ok
            if ok and not was_dirty:
                sysm.lease.record["dirty"] = False
                sysm.lease.save()
            if p is not None and p.alive():
                p.cont_group()
            ctx.sup.resume()


class SpillFull(Fault):
    kind = "spill_full"

    def inject(self, ctx):
        self.since = time.time()
        ctx.system.set_spill_full(True)

    def heal(self, ctx):
        self.effect = bool(ctx.system.spill_full_seen(self.since))
        ctx.system.set_spill_full(False)
        self.proof = {"reported_full": self.effect}


# ------------------------------------------------------------------------------------------------- schedule

def make_fault(kind: str, rng: random.Random, system) -> tuple:
    """(fault, duration_s) for one kind, parameters drawn from rng."""
    pts = system.crash_points()
    if kind in ("kill_point", "hang_point"):
        p = rng.choice([x for x in pts if not crashpoints.get(x) or not crashpoints.get(x).once] or pts)
        hit = "seed:%d:%d" % (rng.getrandbits(48), 3)
        f = (KillPoint if kind == "kill_point" else HangPoint)(point=p, hit=hit, hang_s=rng.uniform(0.5, 3.0))
        return f, 0.5
    if kind == "kill_point_later":
        cps = [x for x in pts if x.endswith("commit_sent")] or pts
        return KillPointLater(point=rng.choice(cps), hit="seed:%d:3" % rng.getrandbits(48),
                              delay_ms=rng.choice([0, 0, 1, 5])), 0.5
    if kind == "kill_group":
        return KillGroup(), 0.1
    if kind == "freeze":
        return Freeze(), rng.choice([rng.uniform(0.2, 1.5), rng.uniform(2.5, 5.0)])
    if kind in Net.MODES:
        return Net(kind, seconds=round(rng.uniform(0.05, 0.4), 3)), rng.uniform(0.3, 4.0)
    if kind in ("src_terminate", "src_cancel"):
        return SrcSignal(kind), 0.1
    if kind == "src_lock":
        return SrcLock(table=rng.choice(system.compared_tables()), seconds=round(rng.uniform(0.5, 3.0), 2)), 0.1
    if kind == "src_long_txn":
        return SrcLongTxn(), rng.uniform(1.0, 6.0)
    if kind == "src_wal_switch":
        return SrcWalSwitch(), 0.1
    if kind == "src_restart_primary":
        return SrcRestartPrimary(), 0.1
    if kind in ("dest_error", "dest_delay", "dest_hang"):
        site = rng.choice([s for s in DEST_SITES if s in system.fault_sites()] or system.fault_sites())
        return Dest(kind, site=site, hits=rng.randint(1, 3), skip=rng.randint(0, 5),
                    seconds=round(rng.uniform(0.2, 2.5), 2)), rng.uniform(1.0, 4.0)
    if kind == "slot_drop":
        return SlotDrop(while_stopped=rng.random() < 0.3), 0.1
    if kind == "slot_invalidate":
        return SlotInvalidate(), 0.1
    if kind == "replica_restart":
        return ReplicaRestart(), 0.1
    if kind == "replica_promote":
        return ReplicaPromote(), 0.1
    if kind == "spill_full":
        return SpillFull(), rng.uniform(0.8, 4.0)
    raise ValueError("unknown fault kind %r" % kind)


class Schedule:
    """Which kinds, with which weights, how many faults per episode."""

    def __init__(self, system, weights: Optional[dict] = None, only: Optional[list] = None, per_episode=(1, 3),
                 cover_all: bool = False):
        self.cover_all = cover_all
        w = dict(weights or DEFAULT_WEIGHTS)
        if only:
            w = {k: w.get(k, 1) for k in only}
        unknown = [k for k in w if k not in FAULT_KINDS]
        if unknown:
            raise ValueError("unknown fault kinds %r" % unknown)
        self.weights = {k: v for k, v in w.items() if v > 0 and system.supports(k)}
        self.not_attacked = sorted(k for k in FAULT_KINDS if k not in self.weights)
        self.system = system
        self.per_episode = per_episode
        if not self.weights:
            raise ValueError("the system supports none of the scheduled fault kinds")

    def to_dict(self):
        return {"weights": self.weights, "not_attacked": self.not_attacked, "per_episode": list(self.per_episode)}

    def plan(self, rng: random.Random, episode_s: float) -> list:
        kinds, weights = zip(*sorted(self.weights.items()))
        out = []
        picks = [rng.choices(kinds, weights)[0] for _ in range(rng.randint(*self.per_episode))]
        if self.cover_all:                                 # every kind once per episode, then the random draws
            first = list(kinds)
            rng.shuffle(first)
            picks = first + picks[len(first):]
        for kind in picks:
            fault, dur = make_fault(kind, rng, self.system)
            out.append((round(rng.uniform(0.0, episode_s * 0.7), 3), round(dur, 3), fault))
        return sorted(out, key=lambda x: x[0])


class Episode:
    """Runs the planned faults of one episode: each in a thread of its own (so they overlap), inject at its offset,
    heal after its duration. Every fault ends healed before `run` returns."""

    def __init__(self, plan, system, sup, fc, load, log: list):
        self.plan, self.system, self.sup, self.fc, self.load, self.log = plan, system, sup, fc, load, log
        self._quiet = threading.Lock()

    def _one(self, offset, dur, fault, t0, ctx):
        delay = offset - (time.monotonic() - t0)
        if delay > 0:
            time.sleep(delay)
        quiet = fault.kind in QUIET_SOURCE
        window = getattr(self.load, "window", None)
        if window is not None:
            window.enter()
        try:
            if quiet:
                self._quiet.acquire()
                self.load.pause()
            fault.t_inject = ctx.t()
            fault.inject(ctx)
            if dur:
                time.sleep(dur)
            fault.heal(ctx)
        except Exception as e:                             # noqa: BLE001 - logged; the fault is not counted
            fault.error = "%s: %s" % (type(e).__name__, str(e)[:160])
            try:
                fault.heal(ctx)
            except Exception:                              # noqa: BLE001
                pass
            fault.effect = False                           # a failed injection is never counted
        finally:
            fault.t_heal = ctx.t()
            if window is not None:
                window.leave()
            if quiet:
                self.load.resume()
                self._quiet.release()
            self.log.append(fault.to_dict())

    def run(self, episode_s):
        t0 = time.monotonic()
        ctx = Ctx(self.system, self.sup, self.fc, self.load, t0)
        threads = [threading.Thread(target=self._one, args=(o, d, f, t0, ctx), daemon=True) for o, d, f in self.plan]
        for th in threads:
            th.start()
        left = episode_s - (time.monotonic() - t0)
        if left > 0:
            time.sleep(left)
        for th in threads:
            th.join(120)
