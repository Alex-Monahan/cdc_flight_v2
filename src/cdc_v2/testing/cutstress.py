"""Stress test of the cut against a live Postgres pair (docs/design/cut.md, "What was measured").

Writers on the primary commit counted evidence: every transaction adds one to exactly one row of
``cdc_v2_cut.counter``.  Cut takers on the standby take cuts with the production library (cdc_v2.pg.cut) and read
the counters under each snapshot.  A logical slot delivers every transaction with its id and positions.  The
checker tests every cut against the stream: the sound rules must never be violated; the deliberately wrong rules
are counted (negative control); the size of the unknown set is tabulated.

Everything here talks to the cluster that `tools/localpg --home H --port-base B` manages, and to nothing else.
Ports are always explicit.  Test code: never imported by the engine.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import threading
import time
from array import array
from bisect import bisect_left, bisect_right
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import psycopg

from cdc_v2 import lsn as L
from cdc_v2.deadline import Deadline
from cdc_v2.pg import cut as C
from cdc_v2.testing import cutnarrow as N
from cdc_v2.testing import pgoutput as P

REPO = Path(__file__).resolve().parents[3]
HOME_DEFAULT = "~/.cache/cdc_flight_v2/pg-cut"
PORT_BASE_DEFAULT = 55600
#: The directory of the Postgres binaries.  tools/localpg reads this variable; everything here reaches Postgres
#: through tools/localpg, so setting it is all it takes to run the scenarios on another major version.
PG_BIN_ENV = "CDC_V2_PG_BIN"
SCHEMA, PUB, SLOT = "cdc_v2_cut", "cdc_v2_cut_pub", "cdc_v2_cut_slot"
COUNTER = SCHEMA + ".counter"
FORBIDDEN_PORT = 5432
UNKNOWN_SCAN_CAP = 20000


class StressError(Exception):
    pass


# --------------------------------------------------------------------------------------------------------------
# The cluster
# --------------------------------------------------------------------------------------------------------------


class Cluster:
    """The private cluster, reached only through tools/localpg and through the connection strings it prints."""

    def __init__(self, home: str = HOME_DEFAULT, port_base: int = PORT_BASE_DEFAULT):
        self.home = os.path.abspath(os.path.expanduser(home))
        self.port_base = int(port_base)
        if self.port_base <= FORBIDDEN_PORT <= self.port_base + 7:
            raise StressError("refusing a port range that contains 5432")
        self._info: Optional[dict] = None

    @staticmethod
    def localpg_env(environ=None) -> dict:
        """The environment tools/localpg runs in: the caller's, without the libpq variables (``PG...``), so that
        nothing falls back to a default port or host.  ``CDC_V2_PG_BIN`` passes through: it selects the
        Postgres binaries, exactly as it does for tools/localpg on the command line."""
        environ = os.environ if environ is None else environ
        return {k: v for k, v in environ.items() if not k.startswith("PG")}

    def localpg(self, *args: str, timeout: float = 300, check: bool = True) -> str:
        cmd = [str(REPO / "tools" / "localpg"), "--home", self.home, "--port-base", str(self.port_base), *args]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=self.localpg_env())
        if check and r.returncode != 0:
            raise StressError("localpg %s failed (%d): %s" % (" ".join(args[:2]), r.returncode,
                                                               self.redact(r.stderr.strip() or r.stdout.strip())))
        return r.stdout

    def info(self, refresh: bool = False) -> dict:
        if self._info is None or refresh:
            self._info = json.loads(self.localpg("conninfo", "--json"))
            for node in self._info["nodes"]:
                if not self.port_base <= node["port"] <= self.port_base + 7:
                    raise StressError("node port outside this cluster's range")
        return self._info

    def node(self, name: str) -> dict:
        return next(n for n in self.info()["nodes"] if n["name"] == name)

    def connect(self, name: str, role: str = "admin", **kw) -> psycopg.Connection:
        dsn = self.node(name)["dsn_" + role]
        if "port=%d" % self.node(name)["port"] not in dsn:
            raise StressError("connection string without an explicit port")
        return psycopg.connect(dsn, autocommit=True, connect_timeout=10, **kw)

    def boot_sql(self, name: str, sql: str, timeout: float = 120) -> str:
        """SQL as the bootstrap superuser (Unix socket), through localpg."""
        return self.localpg("sql", name, "--role", "bootstrap", "--db", self.info()["database"], "-c", sql,
                            timeout=timeout)

    def role(self, which: str) -> str:
        return self.info()["roles"][which]

    def redact(self, text: str) -> str:
        """Evidence must not show the cluster home, the user's home directory or the user name."""
        out = text.replace(self.home, "<home>").replace(os.path.expanduser("~"), "<userhome>")
        user = os.environ.get("USER") or os.environ.get("LOGNAME")
        if user and len(user) > 2:
            out = out.replace(user, "<user>")
        for which in ("bootstrap", "admin", "runtime"):
            if self._info:
                out = out.replace(self._info["roles"][which], "<%s role>" % which)
        return out

    def version(self) -> str:
        with self.connect("primary") as conn:
            return conn.execute("SHOW server_version").fetchone()[0]

    def server_facts(self) -> dict:
        """What an evidence file says about the server, so that a run on another major version shows it."""
        with self.connect("primary") as conn:
            return {"server_version": conn.execute("SHOW server_version").fetchone()[0],
                    "server_version_num": C.server_version_num(conn),
                    "binaries_from_" + PG_BIN_ENV: bool(os.environ.get(PG_BIN_ENV))}


# --------------------------------------------------------------------------------------------------------------
# Setup and teardown
# --------------------------------------------------------------------------------------------------------------

GRANTS = ("pg_log_standby_snapshot()", "pg_switch_wal()", "pg_wal_replay_pause()", "pg_wal_replay_resume()")


def setup(cl: Cluster, keys: list[int], slot_node: str = "replica", slot: str = SLOT) -> None:
    """Fresh evidence tables, publication and slot.  Counters start at zero, after the slot exists, so the
    stream shows every increment."""
    admin = cl.role("admin")
    cl.boot_sql("primary", "; ".join("GRANT EXECUTE ON FUNCTION %s TO %s" % (f, admin) for f in GRANTS))
    drop_slot(cl, slot_node, slot)
    with cl.connect("primary") as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA)
        conn.execute("DROP PUBLICATION IF EXISTS " + PUB)
        for row in conn.execute("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                                "WHERE n.nspname = %s AND c.relkind = 'r'", (SCHEMA,)).fetchall():
            conn.execute('DROP TABLE %s."%s"' % (SCHEMA, row[0]))
        conn.execute("CREATE TABLE %s (k int PRIMARY KEY, n bigint NOT NULL) WITH (fillfactor = 50)" % COUNTER)
        conn.execute("CREATE TABLE %s.side (k int PRIMARY KEY, v bigint NOT NULL) WITH (fillfactor = 50)" % SCHEMA)
        conn.execute("CREATE TABLE %s.bulk (w int, pad text)" % SCHEMA)
        conn.execute("INSERT INTO %s SELECT k, 0 FROM unnest(%%s::int[]) k" % COUNTER, (keys,))
        conn.execute("INSERT INTO %s.side SELECT k, 0 FROM unnest(%%s::int[]) k" % SCHEMA, (keys,))
        conn.execute("ALTER TABLE %s SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 2000)" % COUNTER)
        conn.execute("CREATE PUBLICATION %s FOR TABLE %s" % (PUB, COUNTER))
        target = conn.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
    wait_replay(cl, L.parse(target))
    create_slot(cl, slot_node, slot)


def wait_replay(cl: Cluster, target: int, timeout: float = 60) -> None:
    with cl.connect("replica") as conn:
        C.wait_replay_reached(conn, target, Deadline.after(timeout))


def create_slot(cl: Cluster, node: str, slot: str = SLOT) -> None:
    """On a standby the creation waits for a record that lists the running transactions; ask the primary for one
    until the creation returns."""
    done: list = []

    def create() -> None:
        try:
            with cl.connect(node) as conn:
                conn.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
            done.append(None)
        except Exception as exc:  # reported by the caller
            done.append(exc)

    t = threading.Thread(target=create, daemon=True)
    t.start()
    with cl.connect("primary") as conn:
        deadline = time.monotonic() + 60
        while not done and time.monotonic() < deadline:
            conn.execute("SELECT pg_log_standby_snapshot()")
            time.sleep(0.2)
    t.join(5)
    if not done or done[0] is not None:
        raise StressError("slot creation failed: %s" % (done[0] if done else "timeout"))


def drop_slot(cl: Cluster, node: str, slot: str = SLOT) -> bool:
    try:
        with cl.connect(node) as conn:
            for _ in range(50):
                row = conn.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot,)).fetchone()
                if row is None:
                    return False
                if not row[0]:
                    conn.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
                    return True
                time.sleep(0.2)
    except psycopg.Error:
        return False
    return False


# --------------------------------------------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------------------------------------------

INC = "UPDATE %s SET n = n + 1 WHERE k = {key};" % COUNTER
BODY = {
    "plain": INC + " COMMIT;",
    "async": INC + " COMMIT;",
    "abort": INC + " IF i % 3 = 0 THEN ROLLBACK; ELSE COMMIT; END IF;",
    "subxact": ("FOR j IN 1..{subs} LOOP BEGIN UPDATE %s.side SET v = v + 1 WHERE k = {key}; "
                "EXCEPTION WHEN division_by_zero THEN NULL; END; END LOOP; " % SCHEMA) + INC + " COMMIT;",
    "big": ("INSERT INTO %s.bulk SELECT {key}, repeat('x', 200) FROM generate_series(1, {rows}); " % SCHEMA)
           + INC + " COMMIT; DELETE FROM %s.bulk WHERE w = {key}; COMMIT;" % SCHEMA,
    "drop": ("FOR j IN 1..{tables} LOOP EXECUTE format('CREATE TABLE %s.w%%s_%%s (a int)', {key}, j); END LOOP; "
             "COMMIT; FOR j IN 1..{tables} LOOP EXECUTE format('DROP TABLE %s.w%%s_%%s', {key}, j); END LOOP; "
             % (SCHEMA, SCHEMA)) + INC + " COMMIT;",
    "long": INC + " PERFORM pg_sleep({hold}); COMMIT;",
}
DEFAULTS = {"burst": 200, "pause": 0.1, "subs": 100, "rows": 20000, "tables": 20, "hold": 0.3, "slots": 4,
            "abort_share": 0.2}
KIND_DEFAULTS = {"big": {"burst": 2, "pause": 0.5}, "drop": {"burst": 10, "pause": 0.05},
                 "subxact": {"burst": 20, "pause": 0.05}, "long": {"burst": 3}, "twophase": {"pause": 0.0}}
CLOSER_KEY = 1   # one last increment after the writers stopped, so that every cut gets its E1


@dataclass
class WriterSpec:
    kind: str
    opts: dict

    @classmethod
    def parse(cls, text: str) -> "WriterSpec":
        kind, *rest = text.split(":")
        if kind not in BODY and kind != "twophase":
            raise StressError("unknown writer kind %r" % kind)
        opts = {**DEFAULTS, **KIND_DEFAULTS.get(kind, {})}
        for item in rest:
            name, value = item.split("=")
            if name not in DEFAULTS:
                raise StressError("unknown writer option %r" % name)
            opts[name] = type(DEFAULTS[name])(value)
        return cls(kind, opts)

    def keys(self, index: int) -> list[int]:
        base = (index + 1) * 100
        return [base + s for s in range(self.opts["slots"])] if self.kind == "twophase" else [base]


def writer_loop(cl: Cluster, spec: WriterSpec, keys: list[int], stop: threading.Event, stats: Counter) -> None:
    try:
        if spec.kind == "twophase":
            return twophase_loop(cl, spec, keys, stop, stats)
        sql = "DO $$ BEGIN FOR i IN 1..%d LOOP %s END LOOP; END $$" % (
            spec.opts["burst"], BODY[spec.kind].format(key=keys[0], **spec.opts))
        with cl.connect("primary") as conn:
            if spec.kind == "async":
                conn.execute("SET synchronous_commit = off")
            while not stop.is_set():
                conn.execute(sql)
                stats["bursts_" + spec.kind] += 1
                stop.wait(spec.opts["pause"])
    except Exception as exc:
        stats["writer_errors"] += 1
        stats["writer_error: " + cl.redact(str(exc))[:200]] += 1


def twophase_loop(cl: Cluster, spec: WriterSpec, keys: list[int], stop: threading.Event, stats: Counter) -> None:
    """Prepares one transaction per key, then another session finishes them in random order (some are rolled
    back).  The slot has two_phase off, so the stream shows each at its COMMIT PREPARED."""
    rnd = random.Random(keys[0])
    seq = 0
    with cl.connect("primary") as a, cl.connect("primary") as b:
        while not stop.is_set():
            gids = []
            for key in keys:
                seq += 1
                gid = "cdc_v2_cut_%d_%d" % (key, seq)
                a.execute("BEGIN")
                a.execute(INC.format(key=key))
                a.execute("PREPARE TRANSACTION '%s'" % gid)
                gids.append(gid)
            rnd.shuffle(gids)
            for gid in gids:
                if rnd.random() < spec.opts["abort_share"]:
                    b.execute("ROLLBACK PREPARED '%s'" % gid)
                    stats["rolled_back_prepared"] += 1
                else:
                    b.execute("COMMIT PREPARED '%s'" % gid)
                    stats["committed_prepared"] += 1
            stop.wait(spec.opts["pause"])


# --------------------------------------------------------------------------------------------------------------
# The stream
# --------------------------------------------------------------------------------------------------------------


class Stream:
    """Every delivered transaction, in stream order, in compact arrays.  Appended by one thread; the checker
    reads a prefix."""

    def __init__(self) -> None:
        self.e = array("Q")      # end of the commit record
        self.slen = array("I")   # E minus the start of the commit record
        self.xid = array("I")
        self.key = array("i")
        self.n = array("I")      # the counter value this transaction wrote
        self.idx: dict[int, array] = {}  # key -> stream index of the transaction that wrote n = position + 1
        self.head = 0            # E of the last transaction received
        self.count = 0           # transactions completely stored (the prefix the checker may read)
        self.errors: list[str] = []
        self.stats: Counter = Counter()

    def add(self, txn: P.Transaction, dec: P.StreamDecoder) -> None:
        if len(self.e) and txn.e <= self.e[-1]:
            i = bisect_right(self.e, txn.e) - 1   # delivered again after a restart of the slot's node
            if i < 0 or self.e[i] != txn.e or self.xid[i] != txn.xid:
                self.errors.append("redelivered transaction differs: E=%s xid=%d" % (L.format(txn.e), txn.xid))
            self.stats["redelivered"] += 1
            return
        found = None
        for _lsn, table, msg in txn.changes:
            if table != COUNTER:
                self.errors.append("change of unexpected table %s" % table)
            elif not isinstance(msg, P.Update) or found is not None:
                self.errors.append("unexpected change in transaction %d" % txn.xid)
            else:
                names = dec.column_names(COUNTER)
                found = (int(msg.new[names.index("k")]), int(msg.new[names.index("n")]))
        if found is None:
            self.stats["transactions_without_counter"] += 1
            return
        key, n = found
        ix = self.idx.setdefault(key, array("I"))
        if n != len(ix) + 1:
            self.errors.append("key %d: counter %d delivered after %d" % (key, n, len(ix)))
            return
        if len(ix) and (txn.xid - self.xid[ix[-1]]) % C.XID_MODULUS >= C.XID_HALF:
            self.stats["xid_not_increasing_per_key"] += 1
        self.stats["page_header_before_commit"] += txn.s % 8192 in (24, 40)
        self.e.append(txn.e)
        self.slen.append(txn.e - txn.s)
        self.xid.append(txn.xid)
        self.key.append(key)
        self.n.append(n)
        ix.append(len(self.e) - 1)
        self.count = len(self.e)
        self.head = txn.e


def stream_loop(cl: Cluster, node: str, stream: Stream, stop: threading.Event, batch: int = 60000,
                slot: str = SLOT) -> None:
    """Reads the slot through the SQL interface.  Reconnects when the node restarts; a transaction delivered
    twice (the slot's confirmed position was not yet on disk) is recognised by its E."""
    while not stop.is_set():
        try:
            with cl.connect(node) as conn:
                dec = P.StreamDecoder()
                while not stop.is_set():
                    rows = conn.execute(P.SQL_GET_BINARY_CHANGES, (slot, batch, PUB)).fetchall()
                    for txn in dec.feed(rows):
                        stream.add(txn, dec)
                    if dec.e_order_violations:
                        stream.stats["e_order_violations"] += dec.e_order_violations
                        dec.e_order_violations = 0
                    stream.stats["calls"] += 1
                    stream.stats["rows"] += len(rows)
                    if len(rows) < 100:
                        stop.wait(0.05)
        except (psycopg.OperationalError, psycopg.errors.AdminShutdown, psycopg.errors.CannotConnectNow,
                psycopg.errors.ObjectInUse) as exc:
            stream.stats["stream_reconnects"] += 1
            stream.stats["stream_reconnect: " + type(exc).__name__] += 1
            stop.wait(0.5)
        except Exception as exc:
            stream.errors.append("stream reader: " + cl.redact(str(exc))[:300])
            return


# --------------------------------------------------------------------------------------------------------------
# Cut takers
# --------------------------------------------------------------------------------------------------------------


@dataclass
class CutRecord:
    way: str
    r0: int
    r_stmt: int
    r1: int
    xmin: int
    xmax: int
    head: int           # E of the last transaction the stream reader had received before R0 was read
    vis: dict           # key -> counter value the snapshot sees


READ_COUNTERS = "SELECT k, n FROM " + COUNTER


def taker_loop(cl: Cluster, way: str, stream: Stream, out: deque, stop: threading.Event, stats: Counter,
               sleep_s: float) -> None:
    """Takes cuts the way the pipeline does: the settle floor is read once per connection, a snapshot of a
    standby that has not settled is dropped by the library, and the ``quiet`` way is the library's
    ``quiet_only``.  Only the ``exported`` way uses a test switch (``accept_export_defect``): it exists to show
    the server defect."""
    while not stop.is_set():
        try:
            with cl.connect("replica") as conn, cl.connect("replica") as reader:
                floor = C.settle_floor(conn)
                while not stop.is_set():
                    head = stream.head
                    try:
                        if way == "own":
                            cut = C.take_standby_cut_own(conn, floor)
                            vis = dict(conn.execute(READ_COUNTERS).fetchall())
                        else:
                            try:
                                cut = C.take_standby_cut_exported(conn, floor,
                                                                  accept_export_defect=(way == "exported"),
                                                                  quiet_only=(way == "quiet"))
                            except C.SnapshotNotQuiet:
                                stats["quiet_snapshots_dropped"] += 1
                                continue
                            C.import_snapshot(reader, cut.snapshot_id)
                            vis = dict(reader.execute(READ_COUNTERS).fetchall())
                            C.end_transaction(reader)
                        C.end_transaction(conn)
                    except C.StandbyNotSettled:
                        stats["unsettled_snapshots_dropped"] += 1
                        stop.wait(0.005)
                        continue
                    except psycopg.errors.QueryCanceled:
                        stats["taker_query_canceled"] += 1   # conflict with recovery; the cut is dropped
                        for c in (conn, reader):
                            c.rollback()
                        continue
                    out.append(CutRecord(way, cut.r0, cut.r_stmt, cut.r1, cut.bounds.xmin, cut.bounds.xmax, head,
                                         vis))
                    stats["cuts_taken_" + way] += 1
                    if sleep_s:
                        time.sleep(sleep_s)
        except C.CutError as exc:
            stats["taker_cut_error: " + str(exc)[:80]] += 1
            stop.wait(0.2)
        except psycopg.Error as exc:
            stats["taker_reconnects"] += 1
            stats["taker_reconnect: " + type(exc).__name__] += 1
            stop.wait(0.3)


# --------------------------------------------------------------------------------------------------------------
# The checker
# --------------------------------------------------------------------------------------------------------------

SOUND_RULES = ("inside", "outside", "library", "sharp", "evidence")   # sharp: test-only narrowing
WRONG_RULES = ("stmt_as_r0", "r1_inside", "r1_outside", "naive_sharp", "xmax_inside", "xmin_outside")
#: Written into every result, so that an evidence file says which checks it had.  A result without
#: "missing_rows" is from before that step existed: a row that a snapshot lost was then skipped, not reported.
CHECKER_STEPS = ("unknown_window", "per_key_boundary", "missing_rows")
TAKER_RULES = "settle floor read once per connection; unsettled snapshots dropped by the library"


@dataclass
class Checker:
    stream: Stream
    block_size: int = 8192
    cuts: int = 0
    violations: Counter = field(default_factory=Counter)
    violation_examples: list = field(default_factory=list)
    violations_by_way: Counter = field(default_factory=Counter)
    #: "way/rule" -> count, and up to three examples each: the first 40 examples alone can all be of one way.
    violations_by_way_and_rule: Counter = field(default_factory=Counter)
    examples_by_way_and_rule: dict = field(default_factory=dict)
    last_bad_cut: Optional[CutRecord] = None
    wrong: Counter = field(default_factory=Counter)
    wrong_examples: dict = field(default_factory=dict)
    hist: dict = field(default_factory=lambda: {k: Counter() for k in (
        "base", "xid", "sharp", "xid+sharp", "base_exported", "base_own", "base_quiet", "really_visible")})
    stats: Counter = field(default_factory=Counter)

    def ready(self, rec: CutRecord, n: int) -> bool:
        return n > 0 and self.stream.e[n - 1] > rec.r1

    def _violation(self, rule: str, rec: CutRecord, e1: int, j: Optional[int], note: str) -> None:
        self.violations[rule] += 1
        self.violations_by_way[rec.way] += 1
        self.violations_by_way_and_rule[rec.way + "/" + rule] += 1
        if self.last_bad_cut is not rec:
            self.last_bad_cut = rec
            self.stats["cuts_with_a_violation_" + rec.way] += 1
            if rec.xmin == rec.xmax:
                self.stats["quiet_cuts_with_a_violation_" + rec.way] += 1
        if len(self.violation_examples) < 40:
            self.violation_examples.append(self._describe(rule, rec, e1, j, note))
        examples = self.examples_by_way_and_rule.setdefault(rec.way + "/" + rule, [])
        if len(examples) < 3:
            examples.append(self._describe(rule, rec, e1, j, note))

    def _wrong(self, rule: str, hits: set, rec: CutRecord, e1: int, j: int) -> None:
        if rule not in hits:
            hits.add(rule)
            self.wrong[rule] += 1
            examples = self.wrong_examples.setdefault(rule, [])
            if len(examples) < 3:
                examples.append(self._describe(rule, rec, e1, j, ""))

    def _describe(self, rule: str, rec: CutRecord, e1: int, j: Optional[int], note: str) -> dict:
        st, f = self.stream, L.format
        d = {"rule": rule, "way": rec.way, "R0": f(rec.r0), "in_statement": f(rec.r_stmt), "R1": f(rec.r1),
             "E1": f(e1), "xmin": rec.xmin, "xmax": rec.xmax, "note": note}
        if j is not None:
            d.update(S=f(st.e[j] - st.slen[j]), E=f(st.e[j]), xid=st.xid[j], key=st.key[j], n=st.n[j],
                     snapshot_sees_n=rec.vis.get(st.key[j]))
        return d

    def check(self, rec: CutRecord, n: int) -> None:
        """One cut against the first ``n`` stream transactions (the stream has passed R1)."""
        st = self.stream
        e_all = st.e
        i1 = bisect_right(e_all, rec.r1, 0, n)
        e1 = e_all[i1]
        cut = C.StandbyCut(r0=rec.r0, r1=rec.r1, bounds=C.SnapshotBounds(rec.xmin, rec.xmax), e1=e1, way=rec.way)
        block = self.block_size
        self.cuts += 1
        self.stats["cuts_" + rec.way] += 1
        hits: set = set()
        # 1. The transactions the position rule calls unknown: R0 < E <= E1.  Each verdict of the library (with
        #    the transaction id) against the evidence; and, as a measurement only, the verdict with the start
        #    of the commit record on top (cdc_v2.testing.cutnarrow: the pipeline's stream does not deliver it).
        lo = bisect_right(e_all, rec.r0, 0, n)
        hi = i1 + 1
        base = hi - lo
        after_xid = after_sharp = after_both = really_visible = 0
        for j in range(lo, min(hi, lo + UNKNOWN_SCAN_CAP)):
            e, xid = e_all[j], st.xid[j]
            s = e - st.slen[j]
            visible = st.n[j] <= rec.vis.get(st.key[j], 0)
            really_visible += visible
            v_xid = cut.classify(e, xid)
            v_both = N.classify_with_start(cut, e, xid, s, block)
            sharp_out = N.starts_after_r1(cut, s, block)
            if (v_xid is C.INSIDE and not visible) or (v_xid is C.OUTSIDE and visible):
                self._violation("library", rec, e1, j, "verdict %s by transaction id" % v_xid.name)
            if (v_both is C.INSIDE and not visible) or (v_both is C.OUTSIDE and visible):
                self._violation("sharp", rec, e1, j, "verdict %s by transaction id and record start"
                                % v_both.name)
            if sharp_out and visible:
                self._violation("sharp", rec, e1, j, "record starts after R1 but the snapshot sees it")
            after_xid += v_xid is C.UNKNOWN
            after_sharp += not sharp_out
            after_both += v_both is C.UNKNOWN
        if base > UNKNOWN_SCAN_CAP:
            self.stats["unknown_scan_capped"] += 1
        else:
            for name, value in (("base", base), ("xid", after_xid), ("sharp", after_sharp),
                                ("xid+sharp", after_both), ("base_" + rec.way, base),
                                ("really_visible", really_visible)):
                self.hist[name][value] += 1
        # 2. Per key, the last transaction the snapshot sees and the first it does not see.  Per key, E, the
        #    record start and the transaction id grow with the counter, so these two decide every rule for
        #    every transaction of the key.
        for key, nvis in rec.vis.items():
            ix = st.idx.get(key)
            have = bisect_left(ix, n) if ix is not None else 0   # of this key, within the prefix
            if nvis > have:
                self._violation("outside", rec, e1, None, "key %d: the snapshot sees %d, the stream has "
                                "passed E1 and delivered only %d" % (key, nvis, have))
                continue
            if nvis >= 1:
                j = ix[nvis - 1]
                e, xid = e_all[j], st.xid[j]
                s = e - st.slen[j]
                if e > e1:
                    self._violation("outside", rec, e1, j, "the snapshot sees a transaction with E > E1")
                if cut.classify(e, xid) is C.OUTSIDE:
                    self._violation("library", rec, e1, j, "library says outside, the snapshot sees it")
                elif N.classify_with_start(cut, e, xid, s, block) is C.OUTSIDE:
                    self._violation("sharp", rec, e1, j, "record starts after R1 but the snapshot sees it")
                if e > rec.r1:
                    self._wrong("r1_outside", hits, rec, e1, j)
                if s > rec.r1:
                    self._wrong("naive_sharp", hits, rec, e1, j)
                if C.widen_xid(xid, rec.xmax) >= rec.xmin:
                    self._wrong("xmin_outside", hits, rec, e1, j)
            if nvis < have:
                j = ix[nvis]
                e, xid = e_all[j], st.xid[j]
                if e <= rec.r0:
                    self._violation("inside", rec, e1, j, "E <= R0 but the snapshot does not see it")
                if cut.classify(e, xid) is C.INSIDE:     # the record start never makes an answer "inside"
                    self._violation("library", rec, e1, j, "library says inside, the snapshot does not see it")
                if e <= rec.r1:
                    self._wrong("r1_inside", hits, rec, e1, j)
                if e <= rec.r_stmt:      # own way: the first statement's position is R1, so this equals r1_inside
                    self._wrong("stmt_as_r0", hits, rec, e1, j)
                if C.widen_xid(xid, rec.xmax) < rec.xmax:
                    self._wrong("xmax_inside", hits, rec, e1, j)
                if e <= rec.head:
                    # Counts pairs of a cut and a key, not cuts (the name is kept: evidence files carry it).
                    # Every such cut has R0 below the stream, and is counted once as that further down.
                    self.stats["cuts_that_miss_a_received_transaction"] += 1
        # 3. Every counter row exists before the stream starts and is never deleted.  A snapshot that returns
        #    no row for a key has lost the row (wrong hint bits do that), and step 2 would skip the key.
        for key in list(st.idx):
            if key not in rec.vis:
                self.stats["rows_missing_from_a_cut"] += 1
                self._violation("evidence", rec, e1, None, "the snapshot returns no row for key %d" % key)
        if rec.r0 < rec.head:
            self.stats["cuts_with_r0_below_received"] += 1
        if rec.r1 == rec.r0:
            self.stats["cuts_with_r0_eq_r1"] += 1
        if rec.xmin == rec.xmax:
            self.stats["quiet_cuts_" + rec.way] += 1      # no transaction running: exact by transaction id


def histogram_rows(hist: Counter) -> dict:
    total = sum(hist.values()) or 1
    buckets = [("0", 0, 0), ("1", 1, 1), ("2", 2, 2), ("3-5", 3, 5), ("6-10", 6, 10), ("11-100", 11, 100),
               ("101+", 101, 10 ** 12)]
    out = {name: sum(c for v, c in hist.items() if lo <= v <= hi) for name, lo, hi in buckets}
    out["share_empty"] = round(out["0"] / total, 6)
    out["max"] = max(hist) if hist else 0
    out["mean"] = round(sum(v * c for v, c in hist.items()) / total, 4)
    return out


# --------------------------------------------------------------------------------------------------------------
# Disturbances
# --------------------------------------------------------------------------------------------------------------

CHAOS = ("ddl", "walswitch", "pause", "restart", "crash")


def chaos_loop(cl: Cluster, actions: tuple, period: float, seed: int, stop: threading.Event, stats: Counter) -> None:
    rnd = random.Random(seed)
    step = 0
    while not stop.wait(period):
        action = actions[step % len(actions)]
        step += 1
        try:
            if action == "ddl":
                with cl.connect("primary") as conn:
                    conn.execute("SET lock_timeout = '10s'")
                    which = stats["chaos_ddl"] % 5
                    conn.execute(("ALTER TABLE %s ADD COLUMN extra int DEFAULT 7",
                                  "ALTER TABLE %s REPLICA IDENTITY FULL",
                                  "VACUUM FULL %s",
                                  "ALTER TABLE %s DROP COLUMN extra",
                                  "ALTER TABLE %s REPLICA IDENTITY DEFAULT")[which] % COUNTER)
            elif action == "walswitch":
                with cl.connect("primary") as conn:
                    conn.execute("SELECT pg_switch_wal()")
            elif action == "pause":
                with cl.connect("replica") as conn:
                    conn.execute("SELECT pg_wal_replay_pause()")
                    stop.wait(rnd.uniform(0.2, 1.0))
                    conn.execute("SELECT pg_wal_replay_resume()")
            elif action == "restart":
                cl.localpg("set", "replica", "shared_buffers=%dMB" % (129 + stats["chaos_restart"] % 2))
            elif action == "crash":
                cl.localpg("kill", "replica")
                stop.wait(rnd.uniform(0.0, 0.5))
                cl.localpg("up")
            stats["chaos_" + action] += 1
        except Exception as exc:
            stats["chaos_error: %s: %s" % (action, cl.redact(str(exc))[:120])] += 1


def calm_down(cl: Cluster, cfg: "RunConfig") -> None:
    """Undo what the disturbances may have left: paused replay, a stopped standby, changed settings."""
    if "crash" in cfg.chaos or "restart" in cfg.chaos:
        cl.localpg("up", check=False)
    if "restart" in cfg.chaos:
        cl.localpg("unset", "replica", "shared_buffers", check=False)
    if cfg.apply_delay_ms:
        cl.localpg("unset", "replica", "recovery_min_apply_delay", check=False)
    if "pause" in cfg.chaos:
        try:
            with cl.connect("replica") as conn:
                conn.execute("SELECT pg_wal_replay_resume()")
        except psycopg.Error:
            pass


# --------------------------------------------------------------------------------------------------------------
# One run
# --------------------------------------------------------------------------------------------------------------


@dataclass
class RunConfig:
    label: str = "run"
    seconds: float = 20.0
    writers: tuple = ("plain", "async", "subxact", "abort")
    takers_own: int = 1
    takers_exported: int = 1
    takers_quiet: int = 0       # exported way, quiet snapshots only (the library's rule for a defect server)
    taker_sleep: float = 0.001
    slot_node: str = "replica"
    chaos: tuple = ()
    chaos_period: float = 3.0
    apply_delay_ms: int = 0
    max_cuts: int = 0
    seed: int = 1


def _drain(checker: Checker, cuts: deque, pending: deque) -> None:
    while cuts:
        pending.append(cuts.popleft())
    n = checker.stream.count
    while pending and checker.ready(pending[0], n):
        checker.check(pending.popleft(), n)


def run(cl: Cluster, cfg: RunConfig, log=lambda text: None) -> dict:
    specs = [WriterSpec.parse(w) for w in cfg.writers]
    sessions = sum(2 if s.kind == "twophase" else 1 for s in specs)
    if sessions > 4:
        raise StressError("at most 4 writer sessions (a two-phase writer uses 2)")
    if cfg.takers_own + cfg.takers_exported + cfg.takers_quiet > 4:
        raise StressError("at most 4 cut takers")
    unknown = set(cfg.chaos) - set(CHAOS)
    if unknown:
        raise StressError("unknown disturbance %s" % sorted(unknown))
    cl.info(refresh=True)
    keys_of = [s.keys(i) for i, s in enumerate(specs)]
    keys = [CLOSER_KEY] + [k for ks in keys_of for k in ks]
    facts = cl.server_facts()
    with cl.connect("primary") as conn:
        prepared = int(conn.execute("SHOW max_prepared_transactions").fetchone()[0])
    if any(s.kind == "twophase" for s in specs) and prepared < 8:
        raise StressError("two-phase writers need max_prepared_transactions >= 8 on every node")
    setup(cl, keys, cfg.slot_node)
    with cl.connect("replica") as conn:
        block = N.wal_block_size(conn)
    stream, stats, cuts, pending = Stream(), Counter(), deque(), deque()
    checker = Checker(stream, block)
    stop_w, stop_t, stop_s, stop_c = (threading.Event() for _ in range(4))
    writers = [threading.Thread(target=writer_loop, args=(cl, s, ks, stop_w, stats), daemon=True)
               for s, ks in zip(specs, keys_of)]
    takers = [threading.Thread(target=taker_loop, args=(cl, way, stream, cuts, stop_t, stats, cfg.taker_sleep),
                               daemon=True)
              for way in ["own"] * cfg.takers_own + ["exported"] * cfg.takers_exported
              + ["quiet"] * cfg.takers_quiet]
    reader = threading.Thread(target=stream_loop, args=(cl, cfg.slot_node, stream, stop_s), daemon=True)
    chaos = threading.Thread(target=chaos_loop, args=(cl, cfg.chaos, cfg.chaos_period, cfg.seed, stop_c, stats),
                             daemon=True)
    result: dict = {"label": cfg.label, "config": {k: (list(v) if isinstance(v, tuple) else v)
                                                   for k, v in vars(cfg).items()},
                    **facts, "wal_block_size": block, "checker_steps": list(CHECKER_STEPS),
                    "taker_rules": TAKER_RULES,
                    "load_average_start": [round(x, 2) for x in os.getloadavg()],
                    "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    t0 = time.monotonic()
    problems: list[str] = []
    try:
        if cfg.apply_delay_ms:
            cl.localpg("set", "replica", "recovery_min_apply_delay=%dms" % cfg.apply_delay_ms)
        for t in [reader, *writers, *takers] + ([chaos] if cfg.chaos else []):
            t.start()
        next_log = t0 + 30
        while time.monotonic() - t0 < cfg.seconds and not (cfg.max_cuts and checker.cuts >= cfg.max_cuts):
            _drain(checker, cuts, pending)
            if stream.errors:
                break
            if time.monotonic() >= next_log:
                next_log += 30
                log("%5.0fs cuts=%d transactions=%d violations=%d waiting=%d" % (
                    time.monotonic() - t0, checker.cuts, stream.count, sum(checker.violations.values()),
                    len(pending)))
            time.sleep(0.02)
        result["load_seconds"] = round(time.monotonic() - t0, 1)
        stop_w.set()
        stop_c.set()
        for t in writers:
            t.join(120)
        if cfg.chaos:
            chaos.join(120)
        calm_down(cl, cfg)
        time.sleep(0.3)
        stop_t.set()
        for t in takers:
            t.join(60)
        with cl.connect("primary") as conn:
            conn.execute(INC.format(key=CLOSER_KEY))
            final = dict(conn.execute(READ_COUNTERS).fetchall())
            end = L.parse(conn.execute("SELECT pg_current_wal_flush_lsn()::text").fetchone()[0])
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline and not stream.errors and any(
                len(stream.idx.get(k, ())) < v for k, v in final.items()):
            _drain(checker, cuts, pending)
            time.sleep(0.05)
        stop_s.set()
        reader.join(60)
        _drain(checker, cuts, pending)
        # completeness: the stream delivered exactly the committed increments, and the standby ends equal
        for k, v in sorted(final.items()):
            got = len(stream.idx.get(k, ()))
            if got != v:
                problems.append("key %d: %d committed, %d delivered" % (k, v, got))
        wait_replay(cl, end, timeout=120)
        with cl.connect("replica") as conn:
            if dict(conn.execute(READ_COUNTERS).fetchall()) != final:
                problems.append("standby counters differ from the primary's after catching up")
        result["committed_transactions"] = sum(final.values())
    except Exception as exc:
        problems.append("run failed: %s: %s" % (type(exc).__name__, cl.redact(str(exc))[:300]))
    finally:
        for ev in (stop_w, stop_c, stop_t, stop_s):
            ev.set()
        for t in [reader, *writers, *takers]:
            if t.is_alive():
                t.join(30)
        try:
            calm_down(cl, cfg)
        finally:
            result["slot_dropped"] = drop_slot(cl, cfg.slot_node)
    problems += stream.errors
    writer_errors = {k: v for k, v in stats.items() if k.startswith(("writer_error", "chaos_error"))}
    result.update(
        seconds=round(time.monotonic() - t0, 1),
        load_average_end=[round(x, 2) for x in os.getloadavg()],
        cuts_checked=checker.cuts,
        cuts_not_checked=len(pending) + len(cuts),
        transactions_delivered=stream.count,
        violations=dict(checker.violations),
        violations_by_way=dict(checker.violations_by_way),
        violations_by_way_and_rule=dict(sorted(checker.violations_by_way_and_rule.items())),
        violation_examples=checker.violation_examples,
        violation_examples_by_way_and_rule=checker.examples_by_way_and_rule,
        e_order_violations=stream.stats.get("e_order_violations", 0),
        wrong_rules={r: checker.wrong.get(r, 0) for r in WRONG_RULES},
        wrong_rule_examples=checker.wrong_examples,
        unknown_set={name: histogram_rows(h) for name, h in checker.hist.items()},
        checker=dict(checker.stats),
        stream=dict(stream.stats),
        activity={k: v for k, v in sorted(stats.items()) if k not in writer_errors},
        problems=problems + ["%s (x%d)" % kv for kv in writer_errors.items()],
    )
    result["ok"] = not result["violations"] and not result["problems"] and not result["e_order_violations"]
    return result


def summary(result: dict) -> str:
    us = result["unknown_set"]
    lines = [
        "%s: %s" % (result["label"], "OK" if result["ok"] else "FAILED"),
        "  Postgres %s, %.0f s, writers %s, disturbances %s, slot on %s" % (
            result["server_version"], result["seconds"], ",".join(result["config"]["writers"]),
            ",".join(result["config"]["chaos"]) or "none", result["config"]["slot_node"]),
        "  cuts checked %d (not checked %d), transactions delivered %d" % (
            result["cuts_checked"], result["cuts_not_checked"], result["transactions_delivered"]),
        "  violations of the sound rules: %s%s" % (result["violations"] or "none", "  by way of taking the cut: "
                                                   "%s" % result["violations_by_way"] if result["violations"]
                                                   else ""),
        "  stream out of E order: %d" % result["e_order_violations"],
        "  wrong rules caught (cuts): %s" % ", ".join("%s=%d" % kv for kv in result["wrong_rules"].items()),
        "  unknown set (transactions per cut)   empty     mean   max",
    ]
    for name in ("base", "xid", "sharp", "xid+sharp", "really_visible"):
        lines.append("    %-34s %6.2f%% %8.3f %5d" % (name, 100 * us[name]["share_empty"], us[name]["mean"],
                                                      us[name]["max"]))
    for name in ("cuts_with_r0_below_received", "cuts_that_miss_a_received_transaction", "cuts_with_r0_eq_r1"):
        lines.append("  %s: %d" % (name, result["checker"].get(name, 0)))
    for way in ("own", "exported", "quiet"):
        n = result["checker"].get("cuts_" + way, 0)
        if n:
            q = result["checker"].get("quiet_cuts_" + way, 0)
            lines.append("  cuts taken the %s way: %d, of them quiet (xmin = xmax): %d (%.1f%%), with a violation: %d"
                         % (way, n, q, 100.0 * q / n, result["checker"].get("cuts_with_a_violation_" + way, 0)))
            if result["checker"].get("cuts_with_a_violation_" + way, 0):
                lines.append("    of the cuts with a violation, quiet: %d" % result["checker"].get(
                    "quiet_cuts_with_a_violation_" + way, 0))
    if result.get("violations_by_way_and_rule"):
        lines.append("  violations by way and rule: %s" % ", ".join(
            "%s=%d" % kv for kv in result["violations_by_way_and_rule"].items()))
    if result["activity"].get("quiet_snapshots_dropped"):
        lines.append("  snapshots dropped by the quiet takers: %d" % result["activity"]["quiet_snapshots_dropped"])
    lines.append("  snapshots dropped because the standby had not settled: %d"
                 % result["activity"].get("unsettled_snapshots_dropped", 0))
    lines.append("  checker steps: %s; rows missing from a cut: %d" % (
        ", ".join(result.get("checker_steps", ())), result["checker"].get("rows_missing_from_a_cut", 0)))
    lines.append("  load average at start %s, at end %s" % (result.get("load_average_start"),
                                                          result.get("load_average_end")))
    for p in result["problems"]:
        lines.append("  PROBLEM: " + p)
    return "\n".join(lines)


# --------------------------------------------------------------------------------------------------------------
# Command line (tools/cutstress)
# --------------------------------------------------------------------------------------------------------------


def _csv(text: str) -> tuple:
    return tuple(x for x in text.split(",") if x)


def main(argv: Optional[list] = None) -> int:
    import argparse

    for name in [k for k in os.environ if k.startswith("PG")]:
        del os.environ[name]   # nothing here may fall back to a default port or host
    p = argparse.ArgumentParser(prog="cutstress", description="Stress test and model check of the cut.")
    p.add_argument("--home", default=HOME_DEFAULT)
    p.add_argument("--port-base", type=int, default=PORT_BASE_DEFAULT)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="writers on the primary, cut takers on the standby, check every cut")
    r.add_argument("--label", default="run")
    r.add_argument("--seconds", type=float, default=20)
    r.add_argument("--writers", type=_csv, default=RunConfig.writers, help="kinds: %s, twophase; options as "
                   "kind:name=value (%s)" % (", ".join(BODY), ", ".join(DEFAULTS)))
    r.add_argument("--takers-own", type=int, default=1)
    r.add_argument("--takers-exported", type=int, default=1)
    r.add_argument("--takers-quiet", type=int, default=0, help="exported way, quiet snapshots only")
    r.add_argument("--taker-sleep", type=float, default=0.001)
    r.add_argument("--slot-node", choices=("replica", "primary"), default="replica")
    r.add_argument("--chaos", type=_csv, default=(), help="of: " + ", ".join(CHAOS))
    r.add_argument("--chaos-period", type=float, default=3.0)
    r.add_argument("--apply-delay-ms", type=int, default=0)
    r.add_argument("--max-cuts", type=int, default=0)
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--out", help="write the full result as JSON to this file")
    m = sub.add_parser("model", help="explore the model (no Postgres)")
    m.add_argument("--sizes", type=_csv, default=("1:4", "2:3"), help="transactions:records, comma separated")
    m.add_argument("--primary", type=_csv, default=("1:1", "2:1", "3:1", "2:2"), help="transactions:markers")
    from cdc_v2.testing import cutprimary
    cutprimary.add_commands(sub)
    args = p.parse_args(argv)
    if args.cmd == "model":
        return _model(args)
    cl = Cluster(args.home, args.port_base)
    if args.cmd != "run":
        return cutprimary.dispatch(cl, args)
    cfg = RunConfig(label=args.label, seconds=args.seconds, writers=tuple(args.writers),
                    takers_own=args.takers_own, takers_exported=args.takers_exported, takers_quiet=args.takers_quiet,
                    taker_sleep=args.taker_sleep,
                    slot_node=args.slot_node, chaos=tuple(args.chaos), chaos_period=args.chaos_period,
                    apply_delay_ms=args.apply_delay_ms, max_cuts=args.max_cuts, seed=args.seed)
    result = run(cl, cfg, log=lambda text: print(text, flush=True))
    if args.out:
        Path(args.out).write_text(cl.redact(json.dumps(result, indent=1, sort_keys=True)) + "\n")
    print(cl.redact(summary(result)))
    return 0 if result["ok"] else 1


def _model(args) -> int:
    from cdc_v2.testing import cutmodel as M

    for size in args.sizes:
        n, max_len = (int(x) for x in size.split(":"))
        t = time.monotonic()
        r = M.explore(n, max_len)
        print("standby model, %d transaction(s), up to %d records: %d worlds, %d reader interleavings, %.1f s" % (
            n, max_len, r["worlds"], r["interleavings"], time.monotonic() - t))
        print("  sound rules violated: %s" % (sorted(set(r["violations"]) & set(M.SOUND)) or "none"))
        print("  wrong rules refuted:  %s" % ", ".join(
            "%s=%d" % (k, r["violations"][k]) for k in M.WRONG if k in r["violations"]))
    r = M.explore(1, 3, advance_first=True)
    print("a server that moves the position before the commit becomes visible (1 transaction, up to 3 records): "
          "violated %s" % sorted(set(r["violations"]) & set(M.SOUND)))
    for size in args.primary:
        n, markers = (int(x) for x in size.split(":"))
        r = M.explore_primary(n, markers)
        print("primary model, %d transaction(s), %d marker(s): %d histories; sound rules violated: %s; "
              "wrong rules refuted: %s" % (n, markers, r["histories"],
                                           sorted(set(r["violations"]) & set(M.PRIMARY_SOUND)) or "none",
                                           sorted(set(r["violations"]) & set(M.PRIMARY_WRONG))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
