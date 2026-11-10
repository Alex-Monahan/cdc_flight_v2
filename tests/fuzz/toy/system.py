"""The toy replicator as a fuzz `System` (src/cdc_v2/testing/fuzz/system.py), and its minimal platform.

    from cdc_v2.testing.fuzz import pool, system
    lease = pool.acquire()
    toy = system.make_system("toy:split", lease, source_node="replica")
    toy.setup(workdir, seed); p = toy.start("a", env); ...; toy.teardown(); lease.release()

The toy reads the slot through the proxied link (`links()["source"]`), writes a DuckDB file in the scenario's
directory, and declares its crash points and fault sites in toy.py. Its runs are the fuzzers' self-test: they never
count toward the bar (`is_self_test`).
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import uuid
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(HERE))

import toy as toy_mod                                      # noqa: E402

from cdc_v2.testing import procs                          # noqa: E402
from cdc_v2.testing.fuzz.system import PgSystem            # noqa: E402
from cdc_v2.testing.workload import Features               # noqa: E402

TOY = HERE / "toy.py"
DEFAULTS = {"conflict_rate": 0.1, "stream_fault_rate": 0.0, "guard": "wait", "stale_s": 2.0, "hb_interval_s": 0.5,
            "batch_changes": 400, "poll_s": 0.05, "max_runtime_s": 0, "spill_quota": 8 << 20,
            "statement_timeout_s": 10, "client_timeout_s": 8, "split_gap_s": 0.02, "dest_lock_s": 30}
UNSUPPORTED_FEATURES = {"ddl"}
UNSUPPORTED_FAULTS = set()


def ro_connect(path, deadline_s=10.0):
    """A read-only connection to the toy's DuckDB file, waiting for a writer's file lock."""
    end = time.monotonic() + deadline_s
    while True:
        try:
            con = duckdb.connect(str(path), read_only=True)
            con.execute("SET TimeZone = 'UTC'")
            return con
        except duckdb.IOException as e:
            if "lock" not in str(e).lower() or time.monotonic() >= end:
                raise
            time.sleep(0.002)


class PerStatement:
    """A connection-like object that opens the file for each statement: one statement, one snapshot, and the
    toy's writer is never blocked for longer than one statement. Results are fetched before the file is closed."""

    class _Result:
        def __init__(self, rows):
            self.rows, self.i = rows, 0

        def fetchone(self):
            if self.i >= len(self.rows):
                return None
            self.i += 1
            return self.rows[self.i - 1]

        def fetchall(self):
            out, self.i = self.rows[self.i:], len(self.rows)
            return out

        def fetchmany(self, n):
            out = self.rows[self.i:self.i + n]
            self.i += len(out)
            return out

    def __init__(self, path, lock_wait_s=5.0):
        self.path, self.lock_wait_s = path, lock_wait_s

    def execute(self, sql, params=None):
        con = ro_connect(self.path, self.lock_wait_s)
        try:
            cur = con.execute(sql, params) if params is not None else con.execute(sql)
            return self._Result(cur.fetchall() if cur.description else [])
        finally:
            con.close()

    def interrupt(self):
        pass

    def close(self):
        pass


class ToyRun:
    def __init__(self, number, proc, started, max_runtime_s):
        self.number, self.proc, self.started, self.max_runtime_s = number, proc, started, max_runtime_s
        self.state = "RUNNING"
        self.cancelled_at = None
        self.ended_at = None

    @property
    def tag(self):
        return self.proc.name


class ToyPlatform:
    """The toy's stand-in for contract C6's LocalPlatform: runs are toy process groups. A cancel marks the run
    cancelled first and kills it `cancel_lag_s` later without a signal before (the real platform: about 2.5 s); the
    maximum run time is enforced late (`grace_s` after it), as the real platform does."""

    def __init__(self, system, cancel_lag_s=0.5, grace_s=1.0):
        self.system, self.cancel_lag_s, self.grace_s = system, cancel_lag_s, grace_s
        self.runs = []
        self._n = 0
        self._lock = threading.Lock()

    def start_run(self, env=None, max_runtime_s=None):
        with self._lock:
            self._n += 1
            n = self._n
        args = ["--guard", "exit", "--run-id", "run%d-%s" % (n, uuid.uuid4().hex[:6])]
        if max_runtime_s:
            args += ["--max-runtime", "%.3f" % max_runtime_s]
        p = self.system.start("r%d" % n, env or {}, extra_args=args)
        run = ToyRun(n, p, time.monotonic(), max_runtime_s)
        self.runs.append(run)
        return run

    def run_state(self, run):
        self.poll()
        return run.state

    def cancel(self, run):
        if run.state != "RUNNING":
            return False
        run.state = "CANCELLED"
        run.cancelled_at = time.monotonic()

        def later():
            run.proc.kill_tree()
        t = threading.Timer(self.cancel_lag_s, later)
        t.daemon = True
        t.start()
        return True

    def poll(self):
        now = time.monotonic()
        for r in self.runs:
            if r.ended_at is None and not r.proc.alive():
                r.ended_at = now
                if r.state == "RUNNING":
                    r.state = "EXITED"
            if r.ended_at is None and r.max_runtime_s and now - r.started > r.max_runtime_s + self.grace_s:
                r.proc.kill_tree()
                r.state = "TIMED_OUT"

    def alive(self):
        self.poll()
        return [r for r in self.runs if r.ended_at is None]

    def stop_all(self, timeout=10):
        for r in self.runs:
            if r.proc.alive() or not r.proc.group_gone:
                r.proc.kill_tree()
        for r in self.runs:
            try:
                r.proc.wait(timeout)
            except procs.DeadlineExceeded:
                pass
        procs.assert_no_survivors([r.proc for r in self.runs], timeout)


class ToySystem(PgSystem):
    is_self_test = True
    slot_name = "cdc_v2_toy_slot"
    app_prefix = "cdc_v2_toy_"

    def __init__(self, lease, source_node="replica", options=None):
        super().__init__(lease, source_node, options)
        self.variant = self.options.get("variant") or "correct"
        if self.variant not in toy_mod.VARIANTS:
            raise ValueError("unknown toy variant %r" % self.variant)
        self.name = "toy-%s" % self.variant
        self.settings = dict(DEFAULTS)
        self.settings.update(self.options.get("toy", {}))
        self.dest_path = self.cfg_path = self.events_path = self.spill_dir = None
        self._platform = None

    def features(self):
        if "features" in self.options:
            return Features(**self.options["features"])
        return Features(truncate=True, transfers=True, parent_child=True, ledger=True, large_marked=True,
                        large_rollback=True, large_txn=True, keyless_toast=True, deferrable_key=True)

    def supports(self, kind):
        if kind in UNSUPPORTED_FEATURES or kind in UNSUPPORTED_FAULTS:
            return False
        return True

    def crash_points(self):
        return toy_mod.points_for(self.variant)

    def fault_sites(self):
        return list(toy_mod.SITES)

    # ------------------------------------------------------------------ life
    def setup(self, workdir, seed):
        super().setup(workdir, seed)
        self.dest_path = self.workdir / "dest.duckdb"
        self.events_path = self.workdir / "events.jsonl"
        self.spill_dir = self.workdir / "spill"
        self.spill_dir.mkdir(exist_ok=True)
        self.seed = seed
        self.write_config()

    def write_config(self):
        info = self.cluster.info
        cfg = dict(self.settings)
        cfg.update({
            "variant": self.variant, "seed": self.seed, "slot": self.slot_name,
            "source": {"host": "localhost", "port": self.proxies["source"].port, "dbname": self.db.name,
                       "user": info["roles"]["runtime"], "passfile": info["pgpass"], "sslrootcert": info["ca_cert"]},
            "nudge": ({"host": "localhost", "port": self.node_port(self.primary_node)}
                      if self.slot_node != self.primary_node else None),
            "tables": self.compared_tables(), "dest_path": str(self.dest_path), "events": str(self.events_path),
            "spill_dir": str(self.spill_dir)})
        self.cfg_path = self.workdir / "toy.json"
        self.cfg_path.write_text(json.dumps(cfg, indent=1, sort_keys=True))

    def after_promotion(self, old, new):
        super().after_promotion(old, new)
        self.write_config()

    def start(self, tag, env, extra_args=()):
        if self.settings.get("sabotage_replica_identity") and not getattr(self, "_sabotaged", False):
            # a system that takes the replica identity away from a published table (H-014): the application's
            # UPDATE and DELETE then fail at the source with SQLSTATE 55000; the run must fail, not be infrastructure
            self.db.sql("CREATE PUBLICATION cdc_v2_toy_pub FOR TABLE public.w_nk")
            self.db.sql("ALTER TABLE public.w_nk REPLICA IDENTITY DEFAULT")
            self._sabotaged = True
        env = dict(env or {})
        env["PYTHONPATH"] = str(REPO / "src") + os.pathsep + os.environ.get("PYTHONPATH", "")
        p = procs.start([sys.executable, str(TOY), "--config", str(self.cfg_path), "--tag", tag, *extra_args],
                        env=env, name=tag, log_path=str(self.workdir / ("%s.log" % tag)))
        self.lease.note_proc(p.pid)
        return p

    def platform(self):
        if self._platform is None:
            self._platform = ToyPlatform(self)
        return self._platform

    # ------------------------------------------------------------------ destination
    def applied_position(self):
        if not self.dest_path.exists():
            return None
        try:
            row = PerStatement(self.dest_path).execute(
                "SELECT applied_lsn, resync FROM _cdc.stream_state WHERE slot_name = ?", [self.slot_name]).fetchone()
        except duckdb.CatalogException:
            return None
        if row is None or row[1]:
            return None
        return int(row[0])

    def destination(self):
        from cdc_v2.oracle import Destination
        return Destination(conn=ro_connect(self.dest_path, 60))

    def close_destination(self, dest):
        try:
            dest.conn.close()
        except Exception:                                  # noqa: BLE001
            pass

    def observer_connect(self):
        return PerStatement(self.dest_path)

    def writer_log(self):
        try:
            return [tuple(r) for r in PerStatement(self.dest_path, 30).execute(
                "SELECT n, epoch, run_id, commit_lsn, xid, kind FROM _cdc.toy_log ORDER BY n").fetchall()]
        except duckdb.CatalogException:
            return []

    def events(self):
        out = []
        try:
            with open(self.events_path, encoding="utf-8") as f:
                for line in f:
                    if line.endswith("\n"):
                        out.append(json.loads(line))
        except FileNotFoundError:
            pass
        return out

    # ------------------------------------------------------------------ hooks
    def set_spill_full(self, full):
        filler = self.spill_dir / "filler.bin"
        if full:
            with open(filler, "wb") as f:
                f.truncate(int(self.settings["spill_quota"]))
        else:
            try:
                filler.unlink()
            except FileNotFoundError:
                pass

    def fence_points(self):
        """The fence drill of the leadership fuzzer: (point, inside the destination transaction)."""
        return [("toyrep.fenced", True), ("toyrep.commit_sent", True), ("toyrep.slot_advanced", False)]

    def interrupt_earlier_epochs(self):
        """The toy's equivalent of design 4.5's interrupt: a file destination has no server connections; a held old
        run holds the file (and the newcomer waits) or holds nothing."""
        return "not applicable: a file destination has no server connections to interrupt"

    def committed_positions(self):
        return [e["position"] for e in self.events() if e["kind"] == "committed"]

    def run_counts(self):
        ev = self.events()
        return {"stream_faults": sum(1 for e in ev if e["kind"] == "stream_fault"),
                "stream_detected": sum(1 for e in ev if e["kind"] == "stream_broken")}

    def divergence_reported(self, since):
        n = [e.get("detail", "") for e in self.events() if e["kind"] == "retry" and e.get("t", 0) >= since
             and e.get("error") == "ApplyInvariantError"]
        return "%d times ApplyInvariantError, last: %s" % (len(n), n[-1][:120]) if len(n) >= 3 else ""

    def spill_full_seen(self, since):
        return any(e["kind"] == "spill_full" and e["t"] >= since for e in self.events())
