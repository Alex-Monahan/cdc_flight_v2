"""The cut: where a Postgres snapshot lies in the logical replication stream.

Terms (docs/design/cut.md has the argument and the evidence):

* ``E``  -- the end of a transaction's commit record (an LSN).  The stream identifies a transaction by it
  (pgoutput COMMIT ``end_lsn``).
* ``S``  -- the start of the same commit record (pgoutput BEGIN ``final_lsn``).
* ``R0`` -- ``pg_last_wal_replay_lsn()`` read by a statement that has finished before the snapshot is taken.
* ``R1`` -- the same function read after the snapshot exists.
* ``E1`` -- the ``E`` of the first transaction in the stream with ``E > R1``.

A cut answers, for one source transaction, ``inside`` (the snapshot sees it), ``outside`` (it does not) or
``unknown``.  On a standby:

    E <= R0  => inside
    E >  E1  => outside
    otherwise unknown

and the transaction id narrows ``unknown``: below the snapshot's ``xmin`` => inside, at or above its ``xmax`` =>
outside.  On a primary the cut is the snapshot's ``xmin``, ``xmax`` and in-progress list, and the answer is never
``unknown``.

This module has two halves: pure logic (LSN and transaction-id arithmetic, the cut classes) and the few SQL
statements that take a cut.  The SQL half needs a psycopg connection in autocommit mode; nothing here opens a
connection or names a host or a port.  Every loop that waits has a deadline; single statements are bounded by the
``statement_timeout`` that :func:`set_statement_timeout` puts on the connection.
"""

from __future__ import annotations

import enum
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence

# --------------------------------------------------------------------------------------------------------------
# LSN arithmetic
# --------------------------------------------------------------------------------------------------------------

LSN_MAX = (1 << 64) - 1
_LSN_RE = re.compile(r"^([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})$")

#: Size of a WAL page header at the start of a segment (``SizeOfXLogLongPHD``).  A record that follows a page
#: boundary starts this many bytes (or 24, on other pages) after the boundary.
WAL_LONG_PAGE_HEADER = 40
WAL_SHORT_PAGE_HEADER = 24
WAL_BLOCK_SIZE_DEFAULT = 8192


def parse_lsn(text: str) -> int:
    """``'16/B374D848'`` -> 64-bit integer.  The two halves are 32-bit hexadecimal numbers."""
    if not isinstance(text, str):
        raise TypeError(f"LSN text expected, got {type(text).__name__}")
    m = _LSN_RE.match(text.strip())
    if m is None:
        raise ValueError(f"not an LSN: {text!r}")
    h, l = int(m.group(1), 16), int(m.group(2), 16)
    return (h << 32) | l


def format_lsn(value: int) -> str:
    """64-bit integer -> ``'16/B374D848'`` (the form Postgres prints)."""
    if not 0 <= value <= LSN_MAX:
        raise ValueError(f"LSN out of range: {value}")
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


def to_lsn(value: Any) -> int:
    """Accept an integer, ``X/Y`` text, or an object whose ``str()`` is ``X/Y`` (a driver's LSN type)."""
    if isinstance(value, bool):
        raise TypeError("LSN expected, got bool")
    if isinstance(value, int):
        if not 0 <= value <= LSN_MAX:
            raise ValueError(f"LSN out of range: {value}")
        return value
    if value is None:
        raise ValueError("LSN is NULL")
    return parse_lsn(str(value))


# --------------------------------------------------------------------------------------------------------------
# Transaction-id arithmetic
# --------------------------------------------------------------------------------------------------------------

XID_BITS = 32
XID_MODULUS = 1 << XID_BITS
XID_MASK = XID_MODULUS - 1
XID_HALF = 1 << (XID_BITS - 1)
FIRST_NORMAL_XID = 3
XID8_MAX = (1 << 64) - 1


def xid32(full_xid: int) -> int:
    """The 32-bit transaction id that the stream carries for a 64-bit one (``xid8``)."""
    if not 0 <= full_xid <= XID8_MAX:
        raise ValueError(f"xid8 out of range: {full_xid}")
    return full_xid & XID_MASK


def xid_epoch(full_xid: int) -> int:
    if not 0 <= full_xid <= XID8_MAX:
        raise ValueError(f"xid8 out of range: {full_xid}")
    return full_xid >> XID_BITS


def widen_xid(xid: int, near: int) -> int:
    """The 64-bit transaction id for the 32-bit ``xid`` that lies closest to the 64-bit id ``near``.

    Postgres never has two live transaction ids 2^31 or more apart, so a stream transaction and a snapshot taken
    around the same time are less than 2^31 apart and the result is the transaction's true 64-bit id.  ``near`` is
    normally the snapshot's ``xmax``.  Ids 0..2 are not transactions and are refused.
    """
    if not 0 <= xid <= XID_MASK:
        raise ValueError(f"32-bit xid out of range: {xid}")
    if xid < FIRST_NORMAL_XID:
        raise ValueError(f"not a normal transaction id: {xid}")
    if not 0 <= near <= XID8_MAX:
        raise ValueError(f"xid8 out of range: {near}")
    full = (near & ~XID_MASK) | xid
    if full > near and full - near >= XID_HALF and full >= XID_MODULUS:
        full -= XID_MODULUS
    elif near > full and near - full > XID_HALF:
        full += XID_MODULUS
    return full


@dataclass(frozen=True)
class SnapshotBounds:
    """``pg_current_snapshot()`` as 64-bit ids: ``xmin``, ``xmax`` and the in-progress list.

    On a standby the list is always empty (the standby keeps the running transactions in a part of the snapshot
    that this function does not print), so only ``xmin`` and ``xmax`` carry information there.
    """

    xmin: int
    xmax: int
    xip: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if not FIRST_NORMAL_XID <= self.xmin <= self.xmax <= XID8_MAX:
            raise ValueError(f"bad snapshot bounds: xmin {self.xmin}, xmax {self.xmax}")
        for x in self.xip:
            if not self.xmin <= x < self.xmax:
                raise ValueError(f"in-progress id {x} outside [{self.xmin}, {self.xmax})")

    @classmethod
    def parse(cls, text: Any) -> "SnapshotBounds":
        """Parse the text form ``xmin:xmax:xip,xip,...`` of ``pg_snapshot``."""
        parts = str(text).strip().split(":")
        if len(parts) != 3:
            raise ValueError(f"not a pg_snapshot: {text!r}")
        try:
            xmin, xmax = int(parts[0]), int(parts[1])
            xip = tuple(int(p) for p in parts[2].split(",")) if parts[2] else ()
        except ValueError:
            raise ValueError(f"not a pg_snapshot: {text!r}") from None
        return cls(xmin, xmax, xip)

    def __str__(self) -> str:
        return f"{self.xmin}:{self.xmax}:{','.join(str(x) for x in self.xip)}"


# --------------------------------------------------------------------------------------------------------------
# Verdicts and cuts
# --------------------------------------------------------------------------------------------------------------


class Verdict(enum.Enum):
    INSIDE = "inside"
    OUTSIDE = "outside"
    UNKNOWN = "unknown"


INSIDE, OUTSIDE, UNKNOWN = Verdict.INSIDE, Verdict.OUTSIDE, Verdict.UNKNOWN


class CutError(Exception):
    """A cut could not be taken, or a rule of use was broken."""


class CutDeadlineExceeded(CutError):
    """A wait ran into its deadline.  ``detail`` says what was last seen."""

    def __init__(self, message: str, detail: Optional[dict] = None):
        super().__init__(message)
        self.detail = detail or {}


@dataclass
class StandbyCut:
    """The cut of one snapshot taken on a hot standby.

    The cut is **open** until ``E1`` is known.  Feed the ``E`` of every stream transaction, in stream order, to
    :meth:`observe`; the first one greater than ``R1`` closes the cut.  While the cut is open, a transaction with
    ``E > R1`` is answered ``unknown`` unless its transaction id settles it.

    ``r_stmt`` is the replay position read *by the statement that took the snapshot*.  It is an upper bound only
    (it is a valid ``R1``); it must never be used as ``R0``.  It is kept for diagnosis and for the negative
    control of the stress test.
    """

    r0: int
    r1: int
    bounds: Optional[SnapshotBounds] = None
    way: str = "own"  # "own": reader's own REPEATABLE READ snapshot; "exported": pg_export_snapshot()
    snapshot_id: Optional[str] = None  # exported way: the name to pass to SET TRANSACTION SNAPSHOT
    r_stmt: Optional[int] = None
    wal_block_size: int = WAL_BLOCK_SIZE_DEFAULT
    e1: Optional[int] = None
    extra: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 0 <= self.r0 <= self.r1 <= LSN_MAX:
            raise CutError(f"R0 {format_lsn(self.r0)} must not be after R1 {format_lsn(self.r1)}")
        if self.e1 is not None and self.e1 <= self.r1:
            raise CutError("E1 must be greater than R1")

    # -- E1 -------------------------------------------------------------------------------------------------

    @property
    def is_open(self) -> bool:
        return self.e1 is None

    def observe(self, e: int) -> bool:
        """Tell the cut about the next stream transaction (its ``E``).  Returns True when this call closed it.

        Calls must be made in stream order.  Transactions at or before ``R1`` do not close the cut.
        """
        if self.e1 is None and e > self.r1:
            self.e1 = e
            return True
        return False

    # -- classification ---------------------------------------------------------------------------------------

    def classify(self, e: int, xid: Optional[int] = None, *, s: Optional[int] = None) -> Verdict:
        """Answer for the stream transaction whose commit record ends at ``e``.

        ``xid`` is the transaction's 32-bit id as the stream carries it (the top-level id); it narrows
        ``unknown`` when the cut has snapshot bounds.  ``s``, the start of the commit record, narrows it further
        (see :meth:`starts_after_r1`).  Both are optional and never change an ``inside`` or ``outside`` that the
        positions alone give.
        """
        if e <= self.r0:
            return INSIDE
        if self.e1 is not None and e > self.e1:
            return OUTSIDE
        if xid is not None and self.bounds is not None:
            full = widen_xid(xid, self.bounds.xmax)
            if full < self.bounds.xmin:
                return INSIDE
            if full >= self.bounds.xmax:
                return OUTSIDE
        if s is not None and self.starts_after_r1(s):
            return OUTSIDE
        return UNKNOWN

    def classify_by_position(self, e: int) -> Verdict:
        """The rule of design 7.2 without any narrowing."""
        return self.classify(e)

    def starts_after_r1(self, s: int) -> bool:
        """True when a commit record that starts at ``s`` cannot have been in replay when the snapshot was taken.

        When the snapshot was taken, the replay position was at most ``R1``, and the only record whose replay may
        have been under way is the one that starts at the replay position.  A record starts exactly at the
        replay position, except when that position is a WAL page boundary: then a page header (24 or 40 bytes)
        comes first.  So a commit record that starts after ``R1``, and not within one page header of an ``R1``
        that is a page boundary, was not in replay.
        """
        if s <= self.r1:
            return False
        if self.r1 % self.wal_block_size == 0 and s - self.r1 <= WAL_LONG_PAGE_HEADER:
            return False
        return True

    # -- rules of use -----------------------------------------------------------------------------------------

    def has_passed(self, applied_position: int) -> bool:
        """The stream has passed this snapshot: it has applied a transaction whose commit ends after ``R1``.

        The applied position is always the ``E`` of an applied transaction, and the stream applies in ``E``
        order, so the transaction at ``E1`` is applied when this is true.
        """
        return applied_position > self.r1

    def usable_after(self, p0: int) -> bool:
        """``R0 >= P0``: every transaction the stream delivered up to ``P0`` is inside this snapshot."""
        return self.r0 >= p0

    def unknown_window(self) -> tuple[int, Optional[int]]:
        """``(R0, E1]``: the positions whose transactions the position rule leaves unknown (E1 None = open)."""
        return self.r0, self.e1


@dataclass(frozen=True)
class PrimaryCut:
    """The cut of one snapshot taken on a primary: the snapshot's ``xmin``, ``xmax`` and in-progress list.

    It never answers ``unknown``.  The stream carries the top-level transaction id, and the in-progress list
    holds top-level ids only, so subtransactions need no special care.
    """

    bounds: SnapshotBounds
    extra: dict = field(default_factory=dict, compare=False)

    def classify(self, xid: int) -> Verdict:
        """Answer for the stream transaction with the 32-bit (top-level) id ``xid``."""
        return self.classify_full(widen_xid(xid, self.bounds.xmax))

    def classify_full(self, full_xid: int) -> Verdict:
        b = self.bounds
        if full_xid < b.xmin:
            return INSIDE
        if full_xid >= b.xmax:
            return OUTSIDE
        return OUTSIDE if full_xid in self._xip_set else INSIDE

    @property
    def _xip_set(self) -> frozenset:
        cached = self.extra.get("_xip_set")
        if cached is None:
            cached = frozenset(self.bounds.xip)
            self.extra["_xip_set"] = cached
        return cached

    def usable_after_xid(self, x: int) -> bool:
        """``xmin >= X``: every transaction with an id below ``X`` has ended and, if it committed, is inside.

        ``X`` is a 64-bit id assigned after the table's switch to ``backfilling`` was committed
        (:func:`next_xid_marker`); every transaction whose commit record the stream had delivered by then has a
        smaller id.
        """
        return self.bounds.xmin >= x

    def blocking_xids(self, x: int) -> tuple[int, ...]:
        """The in-progress ids below ``X`` that keep :meth:`usable_after_xid` false (for the report)."""
        return tuple(i for i in self.bounds.xip if i < x)


# --------------------------------------------------------------------------------------------------------------
# SQL: taking a cut
# --------------------------------------------------------------------------------------------------------------

SQL_REPLAY_LSN = "SELECT pg_last_wal_replay_lsn()::text"
SQL_BEGIN_RR = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
SQL_BEGIN_RC = "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY"
#: First statement of the reader's REPEATABLE READ transaction: it takes the snapshot, then reads R1.
SQL_OWN_FIRST = "SELECT pg_last_wal_replay_lsn()::text, pg_is_in_recovery(), pg_current_snapshot()::text"
#: Second statement of the exporting transaction: its statement snapshot is the one that is exported.
SQL_EXPORT = (
    "SELECT pg_export_snapshot(), pg_last_wal_replay_lsn()::text, pg_is_in_recovery(), "
    "pg_current_snapshot()::text"
)
SQL_R1 = "SELECT pg_last_wal_replay_lsn()::text, pg_is_in_recovery()"
SQL_VERSION_NUM = "SELECT current_setting('server_version_num')::int"
SQL_IMPORT = "SET TRANSACTION SNAPSHOT {literal}"
SQL_PRIMARY_FIRST = "SELECT pg_current_snapshot()::text, pg_is_in_recovery()"
SQL_NEXT_XID = "SELECT pg_current_xact_id()::text, pg_is_in_recovery()"
SQL_WAL_BLOCK_SIZE = "SELECT current_setting('wal_block_size')::int"


def _one(conn, sql: str) -> tuple:
    row = conn.execute(sql).fetchone()
    if row is None:
        raise CutError(f"no row from: {sql}")
    return row


def _require_autocommit(conn) -> None:
    if not getattr(conn, "autocommit", False):
        raise CutError("the connection must be in autocommit mode; this module issues BEGIN itself")


def set_statement_timeout(conn, seconds: float) -> None:
    """Bound every later statement on this connection (SPEC 4.5).  Session level; call once after connecting."""
    _require_autocommit(conn)
    ms = max(1, int(seconds * 1000))
    conn.execute(f"SET statement_timeout = {ms}")


def wal_block_size(conn) -> int:
    return int(_one(conn, SQL_WAL_BLOCK_SIZE)[0])


def _replay(text: Optional[str], what: str) -> int:
    if text is None:
        raise CutError(f"{what}: pg_last_wal_replay_lsn() is NULL; this server has never replayed WAL")
    return parse_lsn(text)


def take_standby_cut_own(conn, *, block_size: int = WAL_BLOCK_SIZE_DEFAULT) -> StandbyCut:
    """Take a cut with the reader's own snapshot on a hot standby.

    Statement 1 (autocommit) reads ``R0``.  Then a REPEATABLE READ transaction is opened; its first statement
    takes the snapshot and reads ``R1``.  The transaction is **left open**: the caller reads under the snapshot
    and ends the transaction.  Raises :class:`CutError` (transaction rolled back) if the server is not in
    recovery when ``R1`` is read: after a promotion the replay position no longer moves and says nothing.
    """
    _require_autocommit(conn)
    r0 = _replay(_one(conn, SQL_REPLAY_LSN)[0], "R0")
    conn.execute(SQL_BEGIN_RR)
    try:
        r1_text, in_recovery, snap = _one(conn, SQL_OWN_FIRST)
        if not in_recovery:
            raise CutError("not a standby: pg_is_in_recovery() is false; use take_primary_cut")
        r1 = _replay(r1_text, "R1")
        return StandbyCut(r0=r0, r1=r1, bounds=SnapshotBounds.parse(snap), way="own", r_stmt=r1,
                          wal_block_size=block_size)
    except BaseException:
        _rollback_quietly(conn)
        raise


#: The server defect that makes an exported snapshot unsafe on a standby (docs/design/cut.md, "The exported
#: snapshot on a standby").  The fix is on the stable branches since 2026-08-26 and in no release up to these.
EXPORT_FIX = ("postgres commit 74c9832ca610, 'Export subxip[] for snapshots taken during recovery' "
              "(REL_18_STABLE; back-patched through 14)")
_LAST_MINOR_WITHOUT_EXPORT_FIX = {16: 15, 17: 11, 18: 6}


def export_defect(server_version_num: int) -> bool:
    """True if a standby of this version can hand out a wrong exported snapshot.

    On such a server a snapshot exported on a standby loses its list of running transactions while the standby
    marks its snapshots as overflowed.  With ``wal_level = logical`` one running transaction on the primary
    whose first write happened inside a subtransaction (a savepoint, a PL/pgSQL exception block) is enough; so
    is any transaction with more than 64 subtransactions.  A session that imports such a snapshot sees
    transactions that were still running when the snapshot was exported, and can leave wrong hint bits that
    mislead every other session of the standby.  Versions older than 16 were not examined and count as
    affected.  The table assumes that the first minor release after those listed carries the fix; check it
    when that release appears.
    """
    major, minor = divmod(int(server_version_num), 10000)
    if major >= 19:
        return False
    last = _LAST_MINOR_WITHOUT_EXPORT_FIX.get(major)
    return last is None or minor <= last


def take_standby_cut_exported(conn, *, block_size: int = WAL_BLOCK_SIZE_DEFAULT,
                              accept_export_defect: bool = False) -> StandbyCut:
    """Take a cut with an exported snapshot on a hot standby.

    One READ COMMITTED transaction with three statements: ``R0``; ``pg_export_snapshot()`` (its statement
    snapshot is the exported one); ``R1``.  The transaction is **left open**: the snapshot can be imported
    (:func:`import_snapshot`) only while it lives.  The connection touches no table, so it holds no relation
    lock.

    Refused on a server with the export defect (:func:`export_defect`) unless ``accept_export_defect`` is set.
    Set it only where no transaction on the primary uses a subtransaction (tests).
    """
    _require_autocommit(conn)
    if not accept_export_defect:
        version = int(_one(conn, SQL_VERSION_NUM)[0])
        if export_defect(version):
            raise CutError("exported snapshots are not safe on a standby of server version %d: it lacks %s; "
                           "take the cut with take_standby_cut_own" % (version, EXPORT_FIX))
    conn.execute(SQL_BEGIN_RC)
    try:
        r0 = _replay(_one(conn, SQL_REPLAY_LSN)[0], "R0")
        snapshot_id, r_stmt_text, in_recovery_a, snap = _one(conn, SQL_EXPORT)
        r1_text, in_recovery_b = _one(conn, SQL_R1)
        if not (in_recovery_a and in_recovery_b):
            raise CutError("not a standby: pg_is_in_recovery() is false; use take_primary_cut")
        return StandbyCut(r0=r0, r1=_replay(r1_text, "R1"), bounds=SnapshotBounds.parse(snap), way="exported",
                          snapshot_id=snapshot_id, r_stmt=_replay(r_stmt_text, "in-statement position"),
                          wal_block_size=block_size)
    except BaseException:
        _rollback_quietly(conn)
        raise


def import_snapshot(conn, snapshot_id: str) -> None:
    """Open a REPEATABLE READ transaction on ``conn`` that uses the exported snapshot.  Left open."""
    _require_autocommit(conn)
    if not snapshot_id or any(c not in "0123456789ABCDEFabcdef-" for c in snapshot_id):
        raise CutError(f"not a snapshot name: {snapshot_id!r}")
    conn.execute(SQL_BEGIN_RR)
    try:
        conn.execute(SQL_IMPORT.format(literal="'" + snapshot_id + "'"))
    except BaseException:
        _rollback_quietly(conn)
        raise


def take_primary_cut(conn) -> PrimaryCut:
    """Take a cut on a primary: a REPEATABLE READ transaction whose first statement takes the snapshot and reads
    ``pg_current_snapshot()``.  The transaction is **left open**."""
    _require_autocommit(conn)
    conn.execute(SQL_BEGIN_RR)
    try:
        snap, in_recovery = _one(conn, SQL_PRIMARY_FIRST)
        if in_recovery:
            raise CutError("not a primary: pg_is_in_recovery() is true; use a standby cut")
        return PrimaryCut(SnapshotBounds.parse(snap))
    except BaseException:
        _rollback_quietly(conn)
        raise


def next_xid_marker(conn) -> int:
    """Assign one transaction id on the primary and return it as a 64-bit id (``X`` of the primary rule).

    Every transaction id assigned earlier is smaller.  Call it after the table's switch to ``backfilling`` is
    committed.  It costs one transaction id.
    """
    _require_autocommit(conn)
    text, in_recovery = _one(conn, SQL_NEXT_XID)
    if in_recovery:
        raise CutError("not a primary")
    return int(text)


def end_transaction(conn) -> None:
    """End the transaction a ``take_*`` call left open (read-only, so ROLLBACK and COMMIT are the same)."""
    conn.execute("ROLLBACK")


def _rollback_quietly(conn) -> None:
    try:
        conn.execute("ROLLBACK")
    except Exception:  # the connection may be gone; the original error is the one to report
        pass


# --------------------------------------------------------------------------------------------------------------
# Waits (each with a deadline)
# --------------------------------------------------------------------------------------------------------------


def wait_replay_reached(conn, target: int, *, deadline_s: float, poll_s: float = 0.05,
                        clock: Callable[[], float] = time.monotonic,
                        sleep: Callable[[float], None] = time.sleep) -> int:
    """Wait until the standby has replayed up to ``target`` (so that a cut taken next has ``R0 >= target``).

    Returns the replay position seen.  Raises :class:`CutDeadlineExceeded` after ``deadline_s`` seconds.
    """
    _require_autocommit(conn)
    end = clock() + deadline_s
    seen = None
    while True:
        seen = _replay(_one(conn, SQL_REPLAY_LSN)[0], "replay position")
        if seen >= target:
            return seen
        if clock() >= end:
            raise CutDeadlineExceeded(
                f"standby replay did not reach {format_lsn(target)} within {deadline_s:g} s "
                f"(last seen {format_lsn(seen)})",
                {"target": format_lsn(target), "seen": format_lsn(seen), "behind_bytes": target - seen},
            )
        sleep(poll_s)


def take_standby_cut_after(conn, p0: int, *, deadline_s: float, exported: bool = False, poll_s: float = 0.05,
                           block_size: int = WAL_BLOCK_SIZE_DEFAULT) -> StandbyCut:
    """Wait for replay to reach ``P0``, then take a cut; the cut returned has ``R0 >= P0``."""
    wait_replay_reached(conn, p0, deadline_s=deadline_s, poll_s=poll_s)
    cut = (take_standby_cut_exported if exported else take_standby_cut_own)(conn, block_size=block_size)
    if not cut.usable_after(p0):  # cannot happen: the replay position never moves back while the server runs
        _rollback_quietly(conn)
        raise CutError(f"R0 {format_lsn(cut.r0)} is before P0 {format_lsn(p0)}")
    return cut


def take_primary_cut_after(conn, x: int, *, deadline_s: float, poll_s: float = 0.1,
                           clock: Callable[[], float] = time.monotonic,
                           sleep: Callable[[float], None] = time.sleep) -> PrimaryCut:
    """Take cuts on a primary until one has ``xmin >= X``; return it with its transaction left open.

    Raises :class:`CutDeadlineExceeded` after ``deadline_s`` seconds; ``detail['blocking_xids']`` lists the
    transactions (64-bit ids) that were still in progress below ``X`` at the last attempt, for the operator.
    """
    _require_autocommit(conn)
    end = clock() + deadline_s
    while True:
        cut = take_primary_cut(conn)
        if cut.usable_after_xid(x):
            return cut
        end_transaction(conn)
        if clock() >= end:
            blocking = cut.blocking_xids(x)
            raise CutDeadlineExceeded(
                f"transactions older than id {x} were still open after {deadline_s:g} s: "
                f"{len(blocking)} in progress, oldest {cut.bounds.xmin}",
                {"x": x, "xmin": cut.bounds.xmin, "blocking_xids": list(blocking)},
            )
        sleep(poll_s)


def classify_all(cut: StandbyCut, transactions: Sequence[tuple[int, Optional[int]]]) -> list[Verdict]:
    """Classify ``(E, xid)`` pairs given in stream order, feeding each to :meth:`StandbyCut.observe` first."""
    out = []
    for e, xid in transactions:
        cut.observe(e)
        out.append(cut.classify(e, xid))
    return out
