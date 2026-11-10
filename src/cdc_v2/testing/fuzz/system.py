"""The system under test, as the fuzzers see it.

`System` extends the crash runner's `Adapter` (docs/testing/harness.md section 4) by what the fuzzers need and no
more. `PgSystem` implements the part that every system on a local Postgres pair shares: the home from the pool, a
fresh database and workload per scenario, the slot node (replica or primary), the source-side heartbeat write, the
positioned oracle comparison and the TCP proxy on the link pipeline → source. A concrete system adds how to start
an instance, where its destination is, its crash points and fault sites, its spill hook and the facts of the
two-writers check. The toy (tests/fuzz/toy/system.py) is one; package S/L's pipeline will be the next.

Fault kinds and workload features are named in `FAULT_KINDS` and `WORKLOAD_FEATURES`; `supports(kind)` says which
ones a system can take, and the ledger reports the rest as not attacked.
"""
from __future__ import annotations

import dataclasses
import importlib.util
import os
import sys
import time
from pathlib import Path
from typing import Optional

from cdc_v2 import lsn
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.testing import pg as tpg
from cdc_v2.testing.crash import Adapter
from cdc_v2.testing.harness_evidence import register_root
from cdc_v2.testing.workload import Features, Workload, WorkloadConfig

REPO = Path(__file__).resolve().parents[4]

# kind -> (group, what it does, how its effect is proven); docs/testing/fuzzers.md section 4 repeats this table
FAULT_KINDS = {
    "kill_point": ("process", "SIGKILL at a declared crash point with a seeded hit",
                   "the instance died by SIGKILL and its hit log has the fire line of that point"),
    "kill_point_later": ("process", "SIGKILL from a second thread at a point before a commit (COMMIT in flight)",
                         "as kill_point, with action kill_later"),
    "kill_group": ("process", "SIGKILL of the instance's process group at a random time",
                   "the instance was alive and died by SIGKILL"),
    "freeze": ("process", "SIGSTOP of the process group for a random time, then SIGCONT",
               "ps showed the instance stopped (state T)"),
    "hang_point": ("process", "the instance hangs at a crash point with a live process, then is killed",
                   "the fire line of the point, with the process still alive"),
    "net_drop": ("network", "FIN on every open connection of the proxied link, and on new ones, for a time",
                 "the proxy closed at least one connection (closed_by_fault or accept_closed rose)"),
    "net_reset": ("network", "RST on every open connection of the proxied link, and on new ones, for a time",
                  "the proxy reset at least one connection (reset_by_fault or accept_reset rose)"),
    "net_blackhole": ("network", "nothing forwarded in either direction for a time",
                      "at least one connection was open or opened while the hole was set"),
    "net_latency": ("network", "every chunk delayed by a fixed time, for a time",
                    "bytes moved through the proxy while the delay was set"),
    "src_terminate": ("source", "pg_terminate_backend of the system's own SQL sessions (application_name)",
                      "at least one backend was terminated"),
    "src_cancel": ("source", "pg_cancel_backend of the system's own SQL sessions (application_name)",
                   "at least one backend was signalled"),
    "src_lock": ("source", "ACCESS EXCLUSIVE lock on a published table for a bounded time",
                 "the lock was granted and held"),
    "src_long_txn": ("source", "a transaction with an assigned id held open on the primary",
                     "the transaction id was assigned and held"),
    "src_wal_switch": ("source", "pg_switch_wal() and CHECKPOINT on the primary",
                       "the WAL file name changed"),
    "src_restart_primary": ("source", "crash and restart of the primary (localpg kill, up)",
                            "the primary's postmaster pid changed"),
    "dest_error": ("destination", "an error raised at a destination fault site (md.*) with seeded hits",
                   "the fault log has a fired line of the rule"),
    "dest_delay": ("destination", "a delay at a destination fault site", "the fault log has a fired line"),
    "dest_hang": ("destination", "a block at a destination fault site for a time", "the fault log has a fired line"),
    "slot_drop": ("slot", "the slot dropped (holder terminated first, or while no instance runs)",
                  "pg_drop_replication_slot succeeded"),
    "slot_invalidate": ("slot", "the slot invalidated by max_slot_wal_keep_size while the instance is frozen",
                        "pg_replication_slots shows wal_status lost or an invalidation reason"),
    "replica_restart": ("replica", "crash and restart of the replica (localpg kill, up)",
                        "the replica's postmaster pid changed"),
    "replica_promote": ("replica", "the primary lost, the replica promoted, a new standby added",
                        "the replica runs on a new timeline"),
    "spill_full": ("spill", "the spill area made full through the system's hook, for a time",
                   "the system reported the full spill area (an event of its own)"),
}
WORKLOAD_FEATURES = [f.name for f in dataclasses.fields(Features)]


class System(Adapter):
    """What a fuzzer needs from a system under test. Methods of `Adapter` (crash runner) are kept as they are."""

    name = "system"
    tier = "local"                       # 'local'; the stage 2 tiers later
    source_node = "replica"              # where the slot is: 'replica' (default, SPEC R17) or 'primary'
    is_self_test = False                 # True for the toy: its runs never count toward the bar

    def supports(self, kind: str) -> bool:
        """A fault kind of FAULT_KINDS or a workload feature of WORKLOAD_FEATURES."""
        raise NotImplementedError

    def capabilities(self) -> dict:
        return {"faults": {k: self.supports(k) for k in FAULT_KINDS},
                "workload": {k: self.supports(k) for k in WORKLOAD_FEATURES}}

    def fault_sites(self) -> list:
        return []

    def links(self) -> dict:
        """{name: NetProxy} of the links that are behind a proxy (at least 'source': pipeline → source)."""
        return {}

    def set_spill_full(self, full: bool) -> None:
        raise NotImplementedError

    def run_counts(self) -> dict:
        """Counts the system keeps of its own during a run (read before teardown): `stream_faults` (byte faults
        injected on the engine-to-applier path) and `stream_detected` (broken streams it detected)."""
        return {}

    def divergence_reported(self, since: float) -> str:
        """Non-empty when the system itself reported, repeatedly since `since` (wall clock), that its destination
        no longer matches the source (in stream mode an unresolved unchanged value or a missing row to delete is
        `ApplyInvariantError`, plan C3a). The run then ends as `diverged` without waiting for its deadline."""
        return ""

    def writer_log(self) -> list:
        """Facts for 'never two writers': [(order, epoch, run, position, xid, kind)] in commit order."""
        raise NotImplementedError

    def platform(self):
        raise NotImplementedError


class PgSystem(System):
    """A system on a home of the fuzz pool: one fresh database, workload and destination per scenario."""

    slot_name = "cdc_v2_fuzz_slot"
    app_prefix = "cdc_v2_"                # application_name prefix of the system's own SQL sessions

    def __init__(self, lease, source_node: str = "replica", options: Optional[dict] = None):
        if source_node not in ("replica", "primary"):
            raise ValueError("source_node is replica or primary")
        self.lease = lease
        self.source_node = source_node
        self.options = dict(options or {})
        self.workdir = None
        self.cluster = None
        self.db = None
        self.wl = None
        self.primary_node = "primary"
        self.slot_node = "replica" if source_node == "replica" else "primary"
        self.proxies = {}
        self._last_q = None
        self._last_beat = 0.0

    # ------------------------------------------------------------------ workload
    def features(self) -> Features:
        return Features(**{k: v for k, v in self.options.get("features", {}).items()}) if \
            "features" in self.options else Features()

    def workload_config(self, seed: int) -> WorkloadConfig:
        kw = dict(writers=2, txns_per_round=10, large_txn_rows=300, large_txn_prob=0.01)
        kw.update(self.options.get("workload", {}))
        return WorkloadConfig(seed=seed, features=self.features(), **kw)

    def _wl_connect(self):
        return self.db.connect("admin", self.primary_node)

    # ------------------------------------------------------------------ life
    def setup(self, workdir, seed):
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)
        register_root(self.workdir, "<run>")
        self.cluster = self.lease.cluster()
        self.db = tpg.PrivateDatabase(self.cluster)
        self.lease.note("databases", self.db.name)
        self.wl = Workload(self.workload_config(seed), connect=self._wl_connect,
                                 log_path=self.workdir / "replay.jsonl")
        self.wl.setup()
        self.db.sql("CREATE SCHEMA cdc_v2_fuzz")
        self.db.sql("CREATE TABLE cdc_v2_fuzz.heartbeat (id integer PRIMARY KEY, n bigint NOT NULL)")
        self.db.sql("INSERT INTO cdc_v2_fuzz.heartbeat VALUES (1, 0)")
        for s in ("public", "cdc_v2_fuzz"):
            self.db.sql("GRANT USAGE ON SCHEMA %s TO cdc_v2_runtime" % s)
            self.db.sql("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO cdc_v2_runtime" % s)
        self.start_links()

    def start_links(self):
        from cdc_v2.testing.netproxy import NetProxy
        port = self.cluster.port(self.slot_node)
        old = self.proxies.get("source")
        want = old.port if old is not None else None
        if old is not None:
            old.stop()
        self.proxies["source"] = NetProxy("localhost", port, port=want, port_range=self.lease.proxy_range).start()

    def links(self):
        return dict(self.proxies)

    def teardown(self):
        for p in self.proxies.values():
            try:
                p.stop()
            except Exception:                              # noqa: BLE001
                pass
        self.proxies = {}
        self.lease.forget_procs()
        if self.wl is not None:
            self.wl.close()
        if self.db is not None:
            try:
                self.cluster.refresh()
                self.db.drop()
                self.lease.forget("databases", self.db.name)
            except Exception:                              # noqa: BLE001 - a broken cluster is rebuilt next time
                self.lease.mark_dirty("teardown could not drop the database")
            self.db = None

    # ------------------------------------------------------------------ source
    def source(self):
        return self.db.source(node=self.primary_node)

    def node_port(self, node: str) -> int:
        return self.cluster.port(node)

    def standby_node(self) -> str:
        """A running standby of the current primary ('replica' while the pair is as built)."""
        nodes = [n for n in self.cluster.info["nodes"] if n.get("role") == "standby" and n.get("running", True)]
        names = [n["name"] for n in nodes]
        if "replica" in names:
            return "replica"
        if not names:
            raise RuntimeError("no running standby")
        return names[0]

    def after_promotion(self, old: str, new: str) -> None:
        """The primary `old` is lost and `new` was promoted: writes go to `new`; a slot that lived on `old` is lost
        and the system finds it missing (its documented answer: a new slot and a backfill)."""
        self.primary_node = new
        if self.slot_node == old:
            self.slot_node = new
        self.start_links()

    def heartbeat(self):
        """The source-side heartbeat write (SPEC 4.4): a published table, never compared."""
        with self.db.connect("admin", self.primary_node) as c:
            c.execute("UPDATE cdc_v2_fuzz.heartbeat SET n = n + 1 WHERE id = 1")
        self._last_beat = time.monotonic()

    def quiesce(self) -> int:
        self._last_q = self.wl.quiesce()
        return self._last_q

    # ------------------------------------------------------------------ destination (subclasses)
    def applied_position(self) -> Optional[int]:
        raise NotImplementedError

    def destination(self):
        """An oracle `Destination` on a connection of its own; `close_destination` closes it."""
        raise NotImplementedError

    def close_destination(self, dest):
        pass

    def observer_connect(self):
        raise NotImplementedError

    def compared_tables(self) -> list:
        return self.wl.table_names()

    def oracle_config(self):
        from cdc_v2.oracle import OracleConfig
        return OracleConfig(tables=self.compared_tables(), source_workers=1, timeout_s=180, position_wait_s=60,
                            md_statement_s=120)

    def wait_for_applied(self, q: int, deadline: Deadline) -> int:
        last = None
        while True:
            try:
                last = self.applied_position()
            except Exception:                              # noqa: BLE001 - the destination may be busy
                last = last
            if last is not None and last >= q:
                return last
            if time.monotonic() - self._last_beat > 0.5:
                try:
                    self.heartbeat()
                except Exception:                          # noqa: BLE001 - a source fault may still heal
                    pass
            deadline.sleep(0.05, "applied %s, waiting for %s" % (
                lsn.format(last) if last is not None else "nothing", lsn.format(q)))

    def compare(self, q: int):
        from cdc_v2.oracle import compare, stand_in_applied_position
        dest = self.destination()
        try:
            return compare(self.source(), dest, self.oracle_config(), position=q, slot_name=self.slot_name,
                           applied_position=stand_in_applied_position)
        finally:
            self.close_destination(dest)

    # ------------------------------------------------------------------ the crash runner's adapter
    def workload(self, step, seed):
        """One seeded workload round (the crash runner calls it repeatedly while it waits)."""
        self.wl.run_round()

    def wait_caught_up(self, timeout):
        q = self.quiesce()
        self.wait_for_applied(q, Deadline.after(timeout))

    def check(self):
        try:
            r = self.compare(self._last_q)
        except Exception as e:                             # noqa: BLE001
            return {"ok": False, "reason": "oracle: %s: %s" % (type(e).__name__, str(e)[:200])}
        d = r.to_dict()
        bad = [t for t in d.get("tables", []) if not t.get("equal")]
        return {"ok": bool(r.equal), "reason": "" if r.equal else "diverged: %s" % ", ".join(
            "%s (missing %s, extra %s, different %s)" % (t.get("source"), t.get("missing_rows"), t.get("extra_rows"),
                                                         t.get("different_rows")) for t in bad[:5]),
                "position": lsn.format(self._last_q) if self._last_q else None}


# ------------------------------------------------------------------------------------------------- registry

SYSTEMS = {"toy": "tests/fuzz/toy/system.py:ToySystem"}


def _load(spec: str):
    path, _, attr = spec.partition(":")
    full = REPO / path
    name = "cdc_v2_fuzz_sys_" + full.stem
    if name in sys.modules:
        mod = sys.modules[name]
    else:
        d = str(full.parent)
        if d not in sys.path:
            sys.path.insert(0, d)
        s = importlib.util.spec_from_file_location(name, full)
        mod = importlib.util.module_from_spec(s)
        sys.modules[name] = mod
        s.loader.exec_module(mod)
    return getattr(mod, attr)


def system_class(name: str):
    """`toy` or `toy:<variant>` -> (class, variant)."""
    base, _, variant = name.partition(":")
    if base not in SYSTEMS:
        raise ValueError("unknown system %r (known: %s)" % (name, ", ".join(sorted(SYSTEMS))))
    return _load(SYSTEMS[base]), (variant or None)


def make_system(name: str, lease, source_node: str = "replica", options: Optional[dict] = None):
    cls, variant = system_class(name)
    opts = dict(options or {})
    if variant:
        opts["variant"] = variant
    return cls(lease, source_node=source_node, options=opts)


__all__ = ["FAULT_KINDS", "WORKLOAD_FEATURES", "System", "PgSystem", "make_system", "system_class",
           "DeadlineExceeded"]
