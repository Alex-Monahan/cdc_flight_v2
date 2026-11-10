"""A minimal decoder for the pgoutput logical replication protocol, version 1.  For tests only.

Input is what ``pg_logical_slot_get_binary_changes(slot, NULL, NULL, 'proto_version', '1',
'publication_names', ...)`` returns: rows ``(lsn, xid, data)``.  ``data`` is one protocol message.

Decoded: BEGIN, COMMIT, RELATION, INSERT, UPDATE, DELETE, TRUNCATE.  TYPE, ORIGIN and MESSAGE are parsed just far
enough to be skipped safely.  Anything else (streaming and two-phase messages need a higher protocol version or
an option this decoder never asks for) raises :class:`ProtocolError`.

Positions (see docs/design/cut.md, "The stream"):

* BEGIN ``final_lsn``  = start of the transaction's commit record (``S``);
* COMMIT ``commit_lsn`` = the same start; COMMIT ``end_lsn`` = end of the commit record (``E``);
* the ``lsn`` column of the row that carries COMMIT is ``E`` as well; the one of BEGIN is the transaction's first
  record; the one of a change is the start of that change's record.

Layout source: ``src/backend/replication/logical/proto.c`` (``logicalrep_write_*``), Postgres 16 to 18.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Optional

from cdc_v2.pg.cut import to_lsn

#: Microseconds between the Unix epoch and the Postgres epoch (2000-01-01).
PG_EPOCH_UNIX_US = 946_684_800_000_000


class ProtocolError(Exception):
    pass


@dataclass(frozen=True)
class Begin:
    final_lsn: int  # start of the commit record
    commit_time_us: int  # microseconds since 2000-01-01
    xid: int
    kind: str = "B"


@dataclass(frozen=True)
class Commit:
    flags: int
    commit_lsn: int  # start of the commit record
    end_lsn: int  # end of the commit record: E
    commit_time_us: int
    kind: str = "C"


@dataclass(frozen=True)
class Column:
    name: str
    type_oid: int
    type_mod: int
    is_key: bool


@dataclass(frozen=True)
class Relation:
    rel_id: int
    namespace: str
    name: str
    replica_identity: str
    columns: tuple[Column, ...]
    kind: str = "R"


@dataclass(frozen=True)
class Insert:
    rel_id: int
    new: tuple
    kind: str = "I"


@dataclass(frozen=True)
class Update:
    rel_id: int
    old_kind: Optional[str]  # None, "K" (key) or "O" (full old row)
    old: Optional[tuple]
    new: tuple
    kind: str = "U"


@dataclass(frozen=True)
class Delete:
    rel_id: int
    old_kind: str  # "K" or "O"
    old: tuple
    kind: str = "D"


@dataclass(frozen=True)
class Truncate:
    rel_ids: tuple[int, ...]
    cascade: bool
    restart_identity: bool
    kind: str = "T"


@dataclass(frozen=True)
class Skipped:
    """TYPE ('Y'), ORIGIN ('O') or MESSAGE ('M'): recognised, checked for length, not interpreted."""

    kind: str
    detail: dict = field(default_factory=dict)


class Unchanged:
    """Marker for an unchanged TOASTed value in a tuple."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNCHANGED"


UNCHANGED = Unchanged()


class _Reader:
    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes):
        self.buf = buf
        self.pos = 0

    def need(self, n: int) -> None:
        if n < 0 or self.pos + n > len(self.buf):
            raise ProtocolError(f"message truncated: need {n} bytes at offset {self.pos} of {len(self.buf)}")

    def u8(self) -> int:
        self.need(1)
        v = self.buf[self.pos]
        self.pos += 1
        return v

    def i16(self) -> int:
        self.need(2)
        (v,) = struct.unpack_from("!h", self.buf, self.pos)
        self.pos += 2
        return v

    def i32(self) -> int:
        self.need(4)
        (v,) = struct.unpack_from("!i", self.buf, self.pos)
        self.pos += 4
        return v

    def u32(self) -> int:
        self.need(4)
        (v,) = struct.unpack_from("!I", self.buf, self.pos)
        self.pos += 4
        return v

    def i64(self) -> int:
        self.need(8)
        (v,) = struct.unpack_from("!q", self.buf, self.pos)
        self.pos += 8
        return v

    def u64(self) -> int:
        self.need(8)
        (v,) = struct.unpack_from("!Q", self.buf, self.pos)
        self.pos += 8
        return v

    def cstr(self) -> str:
        end = self.buf.find(b"\x00", self.pos)
        if end < 0:
            raise ProtocolError(f"unterminated string at offset {self.pos}")
        v = self.buf[self.pos:end].decode("utf-8")
        self.pos = end + 1
        return v

    def take(self, n: int) -> bytes:
        self.need(n)
        v = self.buf[self.pos:self.pos + n]
        self.pos += n
        return v

    def done(self, what: str) -> None:
        if self.pos != len(self.buf):
            raise ProtocolError(f"{what}: {len(self.buf) - self.pos} trailing bytes")


def _tuple(r: _Reader) -> tuple:
    n = r.i16()
    if n < 0:
        raise ProtocolError(f"negative column count {n}")
    out = []
    for _ in range(n):
        k = r.u8()
        if k == 0x6E:  # 'n' NULL
            out.append(None)
        elif k == 0x75:  # 'u' unchanged TOAST
            out.append(UNCHANGED)
        elif k == 0x74:  # 't' text
            out.append(r.take(r.i32()).decode("utf-8"))
        elif k == 0x62:  # 'b' binary
            out.append(r.take(r.i32()))
        else:
            raise ProtocolError(f"unknown column kind {k:#x}")
    return tuple(out)


def decode(data: bytes) -> Any:
    """Decode one pgoutput message."""
    data = bytes(data)
    if not data:
        raise ProtocolError("empty message")
    r = _Reader(data)
    tag = chr(r.u8())
    if tag == "B":
        msg: Any = Begin(final_lsn=r.u64(), commit_time_us=r.i64(), xid=r.u32())
    elif tag == "C":
        msg = Commit(flags=r.u8(), commit_lsn=r.u64(), end_lsn=r.u64(), commit_time_us=r.i64())
    elif tag == "R":
        rel_id = r.u32()
        namespace = r.cstr() or "pg_catalog"  # an empty string stands for pg_catalog
        name = r.cstr()
        ident = chr(r.u8())
        natts = r.i16()
        cols = []
        for _ in range(natts):
            flags = r.u8()
            cols.append(Column(name=r.cstr(), type_oid=r.u32(), type_mod=r.i32(), is_key=bool(flags & 1)))
        msg = Relation(rel_id, namespace, name, ident, tuple(cols))
    elif tag == "I":
        rel_id = r.u32()
        if chr(r.u8()) != "N":
            raise ProtocolError("INSERT without new tuple")
        msg = Insert(rel_id, _tuple(r))
    elif tag == "U":
        rel_id = r.u32()
        k = chr(r.u8())
        old_kind, old = None, None
        if k in ("K", "O"):
            old_kind, old = k, _tuple(r)
            k = chr(r.u8())
        if k != "N":
            raise ProtocolError(f"UPDATE: expected new tuple, got {k!r}")
        msg = Update(rel_id, old_kind, old, _tuple(r))
    elif tag == "D":
        rel_id = r.u32()
        k = chr(r.u8())
        if k not in ("K", "O"):
            raise ProtocolError(f"DELETE: expected old tuple, got {k!r}")
        msg = Delete(rel_id, k, _tuple(r))
    elif tag == "T":
        n = r.i32()
        flags = r.u8()
        if n < 0:
            raise ProtocolError(f"TRUNCATE: negative relation count {n}")
        msg = Truncate(tuple(r.u32() for _ in range(n)), cascade=bool(flags & 1), restart_identity=bool(flags & 2))
    elif tag == "Y":
        type_oid = r.u32()
        msg = Skipped("Y", {"type_oid": type_oid, "namespace": r.cstr(), "name": r.cstr()})
    elif tag == "O":
        origin_lsn = r.u64()
        msg = Skipped("O", {"origin_lsn": origin_lsn, "name": r.cstr()})
    elif tag == "M":
        flags = r.u8()
        lsn = r.u64()
        prefix = r.cstr()
        length = r.i32()
        r.take(length)
        msg = Skipped("M", {"transactional": bool(flags & 1), "lsn": lsn, "prefix": prefix, "length": length})
    else:
        raise ProtocolError(f"unsupported pgoutput message {tag!r}")
    r.done(tag)
    return msg


@dataclass
class Transaction:
    """One decoded transaction with the positions the cut needs."""

    xid: int
    s: int  # BEGIN final_lsn: start of the commit record
    e: int  # COMMIT end_lsn: end of the commit record
    commit_time_us: int
    begin_row_lsn: int  # lsn column of the BEGIN row: the transaction's first record
    commit_row_lsn: int  # lsn column of the COMMIT row
    changes: list = field(default_factory=list)  # (row_lsn, relation name "schema.table", message)


class StreamDecoder:
    """Turns rows ``(lsn, xid, data)`` into :class:`Transaction` objects and checks the protocol's invariants.

    Invariants checked (a violation raises :class:`ProtocolError`):

    * BEGIN ... COMMIT bracket every change; the ``xid`` column of every row equals the BEGIN's xid;
    * COMMIT ``commit_lsn`` equals BEGIN ``final_lsn``, and COMMIT ``end_lsn`` is greater;
    * the ``lsn`` column of the COMMIT row equals COMMIT ``end_lsn``.

    ``e_order_violations`` counts transactions whose ``E`` is not greater than the previous one's; the caller
    decides whether that is fatal (the stress checker treats it as a finding).
    """

    def __init__(self) -> None:
        self.relations: dict[int, Relation] = {}
        self.current: Optional[Transaction] = None
        self.last_e: Optional[int] = None
        self.transactions = 0
        self.messages = 0
        self.skipped = 0
        self.e_order_violations = 0

    def feed(self, rows: Iterable[tuple]) -> Iterator[Transaction]:
        for lsn, xid, data in rows:
            txn = self.feed_row(lsn, xid, data)
            if txn is not None:
                yield txn

    def feed_row(self, lsn: Any, xid: Any, data: bytes) -> Optional[Transaction]:
        row_lsn = to_lsn(lsn)
        row_xid = int(str(xid))
        msg = decode(data)
        self.messages += 1
        cur = self.current
        if isinstance(msg, Begin):
            if cur is not None:
                raise ProtocolError("BEGIN inside a transaction")
            if msg.xid != row_xid:
                raise ProtocolError(f"BEGIN xid {msg.xid} differs from row xid {row_xid}")
            self.current = Transaction(xid=msg.xid, s=msg.final_lsn, e=0, commit_time_us=msg.commit_time_us,
                                       begin_row_lsn=row_lsn, commit_row_lsn=0)
            return None
        if cur is None:
            if isinstance(msg, Skipped) and msg.kind == "M":  # a non-transactional message stands alone
                self.skipped += 1
                return None
            raise ProtocolError(f"{msg.kind} outside a transaction")
        if row_xid != cur.xid:
            raise ProtocolError(f"row xid {row_xid} inside transaction {cur.xid}")
        if isinstance(msg, Commit):
            if msg.commit_lsn != cur.s:
                raise ProtocolError("COMMIT commit_lsn differs from BEGIN final_lsn")
            if msg.end_lsn <= msg.commit_lsn:
                raise ProtocolError("COMMIT end_lsn is not after commit_lsn")
            if row_lsn != msg.end_lsn:
                raise ProtocolError("lsn column of the COMMIT row differs from COMMIT end_lsn")
            cur.e = msg.end_lsn
            cur.commit_row_lsn = row_lsn
            if self.last_e is not None and cur.e <= self.last_e:
                self.e_order_violations += 1
            self.last_e = cur.e
            self.transactions += 1
            self.current = None
            return cur
        if isinstance(msg, Relation):
            self.relations[msg.rel_id] = msg
            return None
        if isinstance(msg, Skipped):
            self.skipped += 1
            return None
        if isinstance(msg, Truncate):
            names = ",".join(self._name(i) for i in msg.rel_ids)
            cur.changes.append((row_lsn, names, msg))
            return None
        cur.changes.append((row_lsn, self._name(msg.rel_id), msg))
        return None

    def _name(self, rel_id: int) -> str:
        rel = self.relations.get(rel_id)
        if rel is None:
            raise ProtocolError(f"change for unknown relation {rel_id}")
        return f"{rel.namespace}.{rel.name}"

    def column_names(self, qualified: str) -> tuple[str, ...]:
        for rel in self.relations.values():
            if f"{rel.namespace}.{rel.name}" == qualified:
                return tuple(c.name for c in rel.columns)
        raise KeyError(qualified)


SQL_GET_BINARY_CHANGES = (
    "SELECT lsn::text, xid::text, data FROM pg_logical_slot_get_binary_changes("
    "%s, NULL, %s, 'proto_version', '1', 'publication_names', %s)"
)
