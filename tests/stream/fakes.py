"""Hermetic stand-ins for the stream's tests (tier T0): a scripted slot and engine, a source, catalog values, and a
reference model of the source tables.

    slot = ScriptedSlot()
    slot.txn([("rows", K, [ins(1, "a"), ins(2, "b")])])     # one source transaction
    engine = ScriptedEngine(slot, start)                    # yields HELLO, then every transaction after the slot's
                                                            # confirmed position, with seq numbers of this life
    engine.durable(p)                                       # confirms the newest END <= p, like the real engine

The engine returns frame objects of `cdc_v2.stream.frames` (no bytes): the frame codec is package E's and is tested
there.
"""
from __future__ import annotations

import collections
import itertools
import threading
from dataclasses import dataclass, field
from typing import Optional

import pyarrow as pa

from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.pg.catalog import Column, SourceTable, TypeInfo
from cdc_v2.stream import frames as F

# ------------------------------------------------------------------------------------------------ catalog values

PG = {"int8": (20, 8, "bigint"), "text": (25, -1, "text"), "int4": (23, 4, "integer"),
      "interval": (1186, 16, "interval"), "float8": (701, 8, "double precision")}


def pgtype(name):
    oid, typlen, _ = PG[name]
    return TypeInfo(oid=oid, schema="pg_catalog", name=name, kind="base", typlen=typlen)


def col(attnum, name, typ):
    ti = pgtype(typ)
    toast = ti.typlen == -1
    return Column(attnum=attnum, name=name, type_oid=ti.oid, typmod=-1, type_name=PG[typ][2], type=ti, ndims=0,
                  not_null=False, has_default=False, generated="", identity="", storage="x" if toast else "p",
                  toastable=toast)


def table(schema, name, columns, key=(), oid=16384, replica_identity=None, published=True):
    ri = replica_identity or ("default" if key else "full")
    return SourceTable(oid=oid, schema=schema, name=name, relfilenode=oid, relkind="ordinary", is_partition=False,
                       partition_root=None, persistence="permanent", rls_enabled=False, rls_forced=False,
                       rls_applies_to_reader=False, columns=tuple(columns),
                       max_attnum=max((c.attnum for c in columns), default=0),
                       key=tuple(c.name for c in columns if c.attnum in key), key_attnums=tuple(key),
                       key_source="primary key" if key else None, has_primary_key=bool(key),
                       replica_identity=ri, replica_identity_index=None, replica_identity_index_is_key=False,
                       published=published, publication_columns=None, publication_row_filter=None)


#: The keyed test table: id bigint primary key, v text (can be unchanged TOAST).
K = ("public", "k")
#: The keyless test table (REPLICA IDENTITY FULL): a bigint, b text.
N = ("public", "n")
#: The heartbeat table of package P (published, not replicated).
HB = ("cdc_v2", "heartbeat")


def default_catalog():
    return {K: table("public", "k", [col(1, "id", "int8"), col(2, "v", "text")], key=(1,), oid=16400),
            N: table("public", "n", [col(1, "a", "int8"), col(2, "b", "text")], oid=16401),
            HB: table("cdc_v2", "heartbeat", [col(1, "slot", "text"), col(2, "n", "int8")], key=(1,), oid=16402)}


_FORM = {"int8": ("int64", F.P_INT64, "INT64"), "text": ("text", F.P_UTF8, "STRING"),
         "int4": ("int32", F.P_INT32, "INT32"), "float8": ("float64", F.P_FLOAT64, "FLOAT64"),
         "interval": ("interval_text", F.P_UTF8, "STRING")}


def schema_frame(t: SourceTable, version=1, keyless_listed=True) -> F.Schema:
    cols = []
    for i, c in enumerate(t.columns):
        form, phys, ctype = _FORM[c.type.name]
        cols.append(F.Column(i, c.name, ctype, "", form, phys, c.type_name, -1, -1, True,
                             c.attnum in t.key_attnums, c.type.typlen == -1))
    keypos = tuple(i for i, c in enumerate(t.columns) if c.attnum in t.key_attnums)
    return F.Schema(t.schema, t.name, version, bool(keypos), keypos, tuple(cols))


# ------------------------------------------------------------------------------------------------ records

def ins(*values):
    return ("c", values, None, None)


def upd(*values, unchanged=(), old=None):
    """An update: new image `values` (None at an unchanged position), the positions sent as unchanged, the old
    image (tables without a key)."""
    return ("u", values, tuple(unchanged), old)


def dele(*key_or_old, keyless=False):
    return ("d", key_or_old, None, "keyless" if keyless else None)


def keychange(old_key, *new_values):
    """A key change of a keyed table: `d(old key)` then `c(new, k=old key)` (two records)."""
    return [("d", (old_key,), None, None), ("c", new_values, None, old_key)]


def rows_batch(t: SourceTable, records, seq0: int) -> pa.RecordBatch:
    """A ROWS batch in transport form for `records` of table `t` (keyed: a<i>, u<i>, k<i>; keyless: a<i>, b<i>,
    u<i>)."""
    n = len(t.columns)
    has_key = bool(t.key_attnums)
    keypos = [i for i, c in enumerate(t.columns) if c.attnum in t.key_attnums]
    upos = [i for i, c in enumerate(t.columns) if c.type.typlen == -1]
    seq, op = [], []
    a = [[] for _ in range(n)]
    b = [[] for _ in range(n)]
    u = {i: [] for i in upos}
    k = {i: [] for i in keypos}
    for j, (o, vals, unch, extra) in enumerate(records):
        seq.append(seq0 + j)
        op.append(o)
        unch = unch or ()
        if has_key:
            if o == "d":
                key = vals
                for i in range(n):
                    a[i].append(key[keypos.index(i)] if i in keypos else None)
            else:
                for i in range(n):
                    a[i].append(None if i in unch else vals[i])
            for i in keypos:
                k[i].append(extra if (o == "c" and extra is not None and not isinstance(extra, str)) else None)
        else:
            old = extra if o == "u" else (vals if o == "d" else None)
            for i in range(n):
                if o == "d":
                    a[i].append(None)
                else:
                    a[i].append(None if i in unch else vals[i])
                b[i].append(None if old is None else old[i])
        for i in upos:
            u[i].append(i in unch)
    arrays = [pa.array(seq, pa.int64()), pa.array(op, pa.string())]
    names = ["seq", "op"]
    for i, c in enumerate(t.columns):
        arrays.append(pa.array(a[i], F.ARROW_TYPES[_FORM[c.type.name][1]]))
        names.append("a%d" % i)
    if not has_key:
        for i, c in enumerate(t.columns):
            arrays.append(pa.array(b[i], F.ARROW_TYPES[_FORM[c.type.name][1]]))
            names.append("b%d" % i)
    for i in upos:
        arrays.append(pa.array(u[i], pa.bool_()))
        names.append("u%d" % i)
    for i in keypos:
        arrays.append(pa.array(k[i], F.ARROW_TYPES[_FORM[t.columns[i].type.name][1]]))
        names.append("k%d" % i)
    return pa.RecordBatch.from_arrays(arrays, names=names)


# ------------------------------------------------------------------------------------------------ the slot

@dataclass
class STxn:
    xid: int
    commit_lsn: int
    commit_ms: int
    parts: list                    # [("rows", key, [records]), ("truncate", key), ("message",)]
    fatal: Optional[dict] = None   # a FATAL instead of this transaction: {"failing": {...}, "table": key|None}


class ScriptedSlot:
    """The source's slot as the stream sees it: committed transactions in commit order and the confirmed position."""

    def __init__(self, catalog=None, start_lsn=0x1000000):
        self.catalog = catalog or default_catalog()
        self.txns: list = []
        self.lsn = start_lsn
        self.confirmed = start_lsn
        self.consistent_point = start_lsn
        self.xid = 1000
        self.lock = threading.Lock()
        self.active: Optional[tuple] = None
        self.exists = True
        self.lives = 0
        self.durables: list = []
        self.max_confirmed_seen = start_lsn

    def txn(self, parts, ms=None, fatal=None) -> STxn:
        with self.lock:
            self.xid += 1
            self.lsn += 0x100
            t = STxn(self.xid, self.lsn, ms if ms is not None else 1_700_000_000_000 + self.xid, list(parts), fatal)
            self.txns.append(t)
            return t

    def confirm(self, p):
        with self.lock:
            if p > self.confirmed:
                self.confirmed = p
            self.max_confirmed_seen = max(self.max_confirmed_seen, self.confirmed)


class ScriptedEngine:
    """One engine life over a `ScriptedSlot`: HELLO, then the transactions after the slot's confirmed position."""

    pid_counter = itertools.count(5000)

    def __init__(self, slot: ScriptedSlot, start, *, heartbeat_every=0.5, schema_versions=None, catalog=None,
                 keyless=True, include=None, rows_per_frame=64):
        self.slot = slot
        self.rows_per_frame = rows_per_frame
        self.start = start
        self.engine_id = start.engine_id
        self.hello = None
        self._q = collections.deque()
        self._seq = 0
        self._sent = set()
        self.dead = False
        self.alive = True
        self.durable_sent = []
        self.catalog = catalog or slot.catalog
        self.pid = next(self.pid_counter)
        self.heartbeat_every = heartbeat_every
        self.delivered_ends = []
        self._built_until = None
        self._last_frame = 0.0
        slot.lives += 1
        slot.active = (self.pid, self.pid * 10)
        self._q.append(F.Hello(1, "scripted", "cdc_v2_slot", start.start_position, self.pid, self.engine_id,
                               self.pid, self.pid * 10, "Debezium Streaming"))
        self._versions = {}
        self._pos = None

    # the frames of the transactions after the confirmed position, built lazily (new transactions appear)
    def _fill(self):
        with self.slot.lock:
            txns = list(self.slot.txns)
            confirmed = self.slot.confirmed if self._pos is None else self._pos
        if self._pos is None:
            self._pos = confirmed
        for t in txns:
            if t.commit_lsn <= self._pos:
                continue
            self._pos = t.commit_lsn
            if t.fatal is not None and (t.fatal.get("culprit") is None or self._included(t.fatal["culprit"])):
                self._q.append(("fatal", t.fatal))
                return
            self._emit(t)

    def _next_seq(self):
        s = self._seq
        self._seq += 1
        return s

    def _included(self, key) -> bool:
        inc = getattr(self.start, "table_include", None)
        if not inc:
            return True
        import re
        pats = [p.replace("\\,", ",") for p in re.split(r"(?<!\\),", inc)]
        return any(re.fullmatch(p, "%s.%s" % key) for p in pats)

    def _emit(self, t: STxn):
        q = self._q
        q.append(F.Begin(self._next_seq(), "%d:0" % t.xid, t.commit_ms, {"txId": t.xid, "lsn": t.commit_lsn - 0x80},
                         t.commit_lsn - 0x100))
        counts = collections.Counter()
        events = 0
        for part in t.parts:
            if part[0] in ("rows", "truncate") and not self._included(part[1]):
                continue                                      # Debezium: outside table.include.list, not counted
            if part[0] == "rows":
                key, recs = part[1], part[2]
                st = self.catalog[key]
                if key not in self._versions:
                    self._versions[key] = 1
                    q.append(schema_frame(st, 1))
                for j in range(0, len(recs), self.rows_per_frame):       # at most one Debezium batch per frame
                    part_recs = recs[j:j + self.rows_per_frame]
                    seq0 = self._seq
                    self._seq += len(part_recs)
                    q.append(F.Rows(key[0], key[1], self._versions[key], rows_batch(st, part_recs, seq0)))
                counts["%s.%s" % key] += len(recs)
                events += len(recs)
            elif part[0] == "truncate":
                key = part[1]
                q.append(F.Truncate(self._next_seq(), key[0], key[1], {}))
                counts["%s.%s" % key] += 1
                events += 1
            elif part[0] == "message":
                q.append(F.Message(self._next_seq(), "cdc_v2_m", True, b"x", {}))
                counts[F_MSG] += 1
                events += 1
            elif part[0] == "schema":                         # a new SCHEMA version of a table (its catalog value)
                key, st = part[1], part[2]
                self._versions[key] = self._versions.get(key, 0) + 1
                self.catalog = dict(self.catalog)
                self.catalog[key] = st
                q.append(schema_frame(st, self._versions[key]))
        q.append(F.End(self._next_seq(), "%d:1" % t.xid, t.commit_ms, {"txId": t.xid, "lsn_commit": t.commit_lsn,
                                                                       "lsn": t.commit_lsn},
                       t.commit_lsn, events, tuple(sorted(counts.items()))))
        q.append(F.BatchEnd(1, None))

    def next_frame(self, deadline: Deadline):
        if self.dead:
            raise F.EngineBroken("the frame stream is closed")
        if not self._q:
            self._fill()
        if not self._q:
            import time
            left = deadline.remaining()
            time.sleep(min(left, 0.01))
            if not self._q:
                self._fill()
            if not self._q:
                now = time.monotonic()
                if self.heartbeat_every is not None and now - self._last_frame >= self.heartbeat_every:
                    self._last_frame = now               # Debezium's heartbeat record: the engine is alive
                    return F.Heartbeat(self._next_seq(), {"lsn": self._pos or 0})
                raise DeadlineExceeded("no frame")
        f = self._q.popleft()
        import time
        self._last_frame = time.monotonic()
        if isinstance(f, tuple) and f[0] == "fatal":
            self.dead = True
            info = f[1]
            table = info.get("table") or ("", "")
            from cdc_v2.stream.engine import EngineFatal
            raise EngineFatal(F.Fatal("io.debezium.DebeziumException", "scripted failure", (), {}, table[0],
                                      table[1], info.get("failing")))
        if isinstance(f, F.End):
            self.delivered_ends.append(f.ack_position)
        return f

    def durable(self, p):
        self.durable_sent.append(p)
        self.slot.durables.append(p)
        held = [e for e in self.delivered_ends if e <= p]
        if held:
            self.slot.confirm(max(held))
        self._durable_max = max(getattr(self, "_durable_max", 0), p)

    def kill(self):
        self.dead = True
        self.alive = False
        if self.slot.active == (self.pid, self.pid * 10):
            self.slot.active = None
        return -9

    def stop(self, deadline):
        return self.kill()


F_MSG = "io.debezium.connector.postgresql.LogicalDecodingMessageMonitor$LogicalDecodingMessageId@1"


@dataclass
class Facts:
    confirmed_flush_lsn: Optional[int]
    active: bool
    holder: Optional[tuple]
    invalid: bool = False
    active_pid: Optional[int] = None


class FakeSource:
    """The source as the stream sees it, over a ScriptedSlot."""

    def __init__(self, slot: ScriptedSlot, version=180006, heartbeat_problems=()):
        self.slot = slot
        self.server_version_num = version
        self.problems = list(heartbeat_problems)
        self.created = 0
        self.advanced = []

    def version(self, deadline):
        return self.server_version_num

    def tables(self, deadline, names=None):
        cat = dict(self.slot.catalog)
        if names is not None:
            return {k: v for k, v in cat.items() if k in set(map(tuple, names))}
        return cat

    def slot_facts(self, deadline):
        s = self.slot
        if not s.exists:
            return None
        return Facts(s.confirmed, s.active is not None, s.active, False, s.active[0] if s.active else None)

    def terminate_holder(self, deadline, expect=None):
        self.slot.active = None

    def create_slot(self, deadline):
        self.created += 1
        self.slot.exists = True
        return self.slot.consistent_point

    def heartbeat_problems(self, deadline):
        return list(self.problems)

    def advance_slot(self, deadline):
        with self.slot.lock:
            self.slot.confirmed = max(self.slot.confirmed, self.slot.lsn)
            self.advanced.append(self.slot.confirmed)
            return self.slot.confirmed


# ------------------------------------------------------------------------------------------------ the model

class Model:
    """The source tables after the transactions of a slot (the reference the destination must equal)."""

    def __init__(self):
        self.keyed = {}                                  # id -> v
        self.keyless = collections.Counter()             # (a, b) -> count

    def apply(self, txns):
        for t in txns:
            if t.fatal is not None:
                continue
            for part in t.parts:
                if part[0] == "truncate":
                    if part[1] == K:
                        self.keyed.clear()
                    elif part[1] == N:
                        self.keyless.clear()
                elif part[0] == "rows":
                    for o, vals, unch, extra in part[2]:
                        if part[1] == K:
                            if o == "d":
                                del self.keyed[vals[0]]
                            elif o == "c":
                                self.keyed[vals[0]] = vals[1]
                            else:
                                if 1 not in (unch or ()):
                                    self.keyed[vals[0]] = vals[1]
                        elif part[1] == N:
                            if o in ("d", "u"):
                                old = vals if o == "d" else extra
                                self.keyless[tuple(old)] -= 1
                                if self.keyless[tuple(old)] == 0:
                                    del self.keyless[tuple(old)]
                            if o in ("c", "u"):
                                new = list(vals)
                                if o == "u":
                                    for i in (unch or ()):
                                        new[i] = extra[i]
                                self.keyless[tuple(new)] += 1
        return self
