"""The cut: where a Postgres snapshot lies in the logical replication stream.

Terms (docs/design/cut.md has the argument and the evidence):

* ``E``  -- the end of a transaction's commit record (an LSN).  The stream identifies a transaction by it
  (pgoutput COMMIT ``end_lsn``).
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

The start of the commit record (pgoutput BEGIN ``final_lsn``) would narrow ``unknown`` further, but stock
Debezium does not deliver it, so nothing in this module takes it.  The measurement of that narrowing lives in
the test code (``cdc_v2.testing.cutnarrow``), which the pipeline never imports.

Rules of use that this module enforces:

* **Settled.**  A standby that has just started can, for a while, hide a transaction whose commit record it
  replayed before the restart (docs/design/cut.md, "A standby that has just started").  A standby cut is given
  out only when the snapshot's ``xmax`` has reached the next transaction id recorded in the standby's control
  file.  That value is read once per connection (:func:`settle_floor`) and is bound to the connection; every
  function that takes a standby cut demands it.
* **Export defect.**  On the server versions that :func:`export_defect` names, an exported snapshot of a standby
  is given out only if it is quiet.
* **Timeline.**  A cut records the timeline it was taken on and the start time of its server, and is valid only
  there (``valid_on``).  The functions that give out a cut for use (``take_*_cut_after``,
  :func:`take_keyless_standby_cut`, :func:`seal_primary_cut`) read the cut's own node right after the cut was
  taken and keep that reading on the cut; a cut without it is valid nowhere.  On a standby the reading needs a
  replication connection to the same node (SQL shows a lower bound of the replay timeline only); on a primary
  it is plain SQL, and the cut is given out only when the WAL is flushed up to its insert position.  At the
  swap, after the stream has passed the cut, ``valid_on`` takes one reading, the stream's, made then; one made
  earlier proves nothing.
* **Passed.**  A table swaps only when the stream has passed its cut (``has_passed``): beyond ``R1`` on a
  standby, up to the WAL insert position read after the snapshot on a primary.

This module has two halves: pure logic (transaction-id arithmetic, the cut classes) and the few SQL statements
that take a cut.  The SQL half needs a psycopg connection in autocommit mode; nothing here opens a connection or
names a host or a port.  With the source package's connections that is ``cdc_v2.pg.PgConnection.raw``, and the
caller wraps the call in ``PgConnection.guard(deadline, what)``, which is that package's rule for every direct
use of ``raw``.  Positions are integers (``cdc_v2.lsn``).  Every loop that waits takes a
``cdc_v2.deadline.Deadline`` and ends with ``DeadlineExceeded``; single statements are bounded by the
``statement_timeout`` of the connection (:func:`set_statement_timeout`) or by the caller's guard.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Optional, Sequence, Union

from cdc_v2 import lsn
from cdc_v2.deadline import Deadline, DeadlineExceeded

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


class SnapshotNotQuiet(CutError):
    """One attempt to take a quiet snapshot met a running transaction; the snapshot was dropped.

    ``bounds`` is the dropped snapshot; its ``xmin`` is the oldest transaction the standby still saw running.
    """

    def __init__(self, message: str, bounds: Optional[SnapshotBounds] = None):
        super().__init__(message)
        self.bounds = bounds


class StandbyNotSettled(CutError):
    """A snapshot was dropped because the standby has not settled after its start.  Take another one later.

    The snapshot's ``xmax`` was below the settle floor, so the snapshot may hide a transaction whose commit
    record lies before ``R0``.  The next commit or abort that the standby replays (the heartbeat write is one)
    settles it.
    """

    def __init__(self, message: str, floor: int = 0, xmax: int = 0):
        super().__init__(message)
        self.floor, self.xmax = floor, xmax


@dataclass(frozen=True)
class NoQuietSnapshot:
    """The result of a wait for a quiet snapshot that ended without one.  Not an error.

    Normally a write transaction stayed open on the primary for the whole wait, which a source is free to do:
    ``attempts`` snapshots were taken and dropped, and ``oldest_running`` is the 64-bit id of the oldest
    transaction the standby still saw running at the last one.  With ``not_settled`` no cut could be given
    out at all within the wait, and no snapshot was judged for quiet: the standby did not settle
    (:class:`StandbyNotSettled`), or its timeline could not be proven (:class:`TimelineNotProven`);
    ``detail`` is what the wait last saw.  The caller goes on with the other way to read
    (docs/design/cut.md, "Tables without a key").
    """

    attempts: int
    oldest_running: Optional[int] = None
    not_settled: bool = False
    detail: Optional[str] = None

    def __bool__(self) -> bool:
        return False

    def __str__(self) -> str:
        if self.not_settled and self.detail:
            return "no quiet snapshot within the wait: no cut could be given out (%s)" % self.detail
        if self.not_settled:
            return ("no quiet snapshot within the wait: the standby had not settled after its start "
                    "(no commit or abort was replayed)")
        return ("no quiet snapshot within the wait: a transaction was running at each of %d attempts "
                "(oldest running id at the last one: %s)" % (self.attempts, self.oldest_running))


@dataclass(frozen=True)
class TimelineReading:
    """What ``IDENTIFY_SYSTEM`` said on one node: the timeline it is on and its position.

    On a standby the timeline is the one replay is on, which no SQL function shows.  ``valid_on`` takes any
    object with these two attributes; ``cdc_v2.pg.slot.timeline_history`` returns one.
    """

    timeline: int
    position: int


def _reading(reading: Any) -> Optional[tuple[int, int]]:
    """``(timeline, position)`` of a reading, or None if it is no reading (then the answer is no)."""
    if reading is None:
        return None
    tl, pos = getattr(reading, "timeline", None), getattr(reading, "position", None)
    if tl is None or pos is None or isinstance(tl, bool) or isinstance(pos, bool):
        return None
    return int(tl), int(pos)


@dataclass
class StandbyCut:
    """The cut of one snapshot taken on a hot standby.

    The cut is **open** until ``E1`` is known.  Feed the ``E`` of every stream transaction, in stream order, to
    :meth:`observe`; the first one greater than ``R1`` closes the cut.  While the cut is open, a transaction with
    ``E > R1`` is answered ``unknown`` unless its transaction id settles it.

    ``r_stmt`` is the replay position read *by the statement that took the snapshot*.  It is an upper bound only
    (it is a valid ``R1``); it must never be used as ``R0``.  It is kept for diagnosis and for the negative
    control of the stress test.

    ``timeline`` is a lower bound of the timeline the cut was taken on (see :func:`timeline` for what a standby
    can tell) and ``server_start`` is the start time of the server it was taken on (:func:`server_start`), both
    read by the statement that reads ``R0``.  ``reader`` and ``reader_start`` are what the same node said right
    after the cut was taken: ``IDENTIFY_SYSTEM`` and the start time, read on a replication connection by the
    function that gave the cut out (:func:`take_standby_cut_after`, :func:`take_keyless_standby_cut`).
    :meth:`valid_on` needs all four; a cut made without them (by hand, or by one of the one-attempt functions)
    is valid nowhere.
    """

    r0: int
    r1: int
    bounds: Optional[SnapshotBounds] = None
    way: str = "own"  # "own": reader's own REPEATABLE READ snapshot; "exported": pg_export_snapshot()
    snapshot_id: Optional[str] = None  # exported way: the name to pass to SET TRANSACTION SNAPSHOT
    r_stmt: Optional[int] = None
    timeline: Optional[int] = None
    e1: Optional[int] = None
    extra: dict = field(default_factory=dict)
    server_start: Optional[int] = None
    reader: Optional[TimelineReading] = None
    reader_start: Optional[int] = None

    def __post_init__(self) -> None:
        if not 0 <= self.r0 <= self.r1 <= lsn.MAX:
            raise CutError(f"R0 {lsn.format(self.r0)} must not be after R1 {lsn.format(self.r1)}")
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

    def classify(self, e: int, xid: Optional[int] = None) -> Verdict:
        """Answer for the stream transaction whose commit record ends at ``e``.

        ``xid`` is the transaction's 32-bit id as the stream carries it (the top-level id); it narrows
        ``unknown`` when the cut has snapshot bounds.  It is optional and never changes an ``inside`` or
        ``outside`` that the positions alone give.
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
        return UNKNOWN

    def classify_by_position(self, e: int) -> Verdict:
        """The rule of design 7.2 without any narrowing."""
        return self.classify(e)

    # -- rules of use -----------------------------------------------------------------------------------------

    @property
    def is_quiet(self) -> bool:
        """No transaction was running, as far as the standby knew, when the snapshot was taken (``xmin == xmax``).

        The cut of a quiet snapshot is exact by transaction id alone: an id below ``xmax`` is inside, any other
        is outside.  A quiet snapshot is also the only kind that is safe to export on a server with the export
        defect (:func:`export_defect`), because it has no list of running transactions to lose.
        """
        return self.bounds is not None and self.bounds.xmin == self.bounds.xmax

    def is_settled(self, floor: int) -> bool:
        """The snapshot's ``xmax`` has reached the settle floor (the ``value`` of :class:`SettleFloor`).

        Only a settled cut may be used: ``E <= R0 => inside`` holds for it.  A cut without bounds is not settled.
        """
        return self.bounds is not None and self.bounds.xmax >= floor

    def has_passed(self, applied_position: int) -> bool:
        """The stream has passed this snapshot: it has applied a transaction whose commit ends after ``R1``.

        The applied position is always the ``E`` of an applied transaction, and the stream applies in ``E``
        order, so the transaction at ``E1`` is applied when this is true.
        """
        return applied_position > self.r1

    def usable_after(self, p0: int) -> bool:
        """``R0 >= P0``: every transaction the stream delivered up to ``P0`` is inside this snapshot."""
        return self.r0 >= p0

    @property
    def sealed(self) -> bool:
        """The reading of the cut's own node, made right after the cut, shows the timeline it was taken on:
        the node was then on the recorded timeline, at or beyond ``R1``, in the server life of the cut."""
        r = _reading(self.reader)
        if r is None or self.timeline is None or self.server_start is None or self.reader_start is None:
            return False
        if isinstance(self.reader_start, bool) or int(self.reader_start) != self.server_start:
            return False
        return r[0] == self.timeline and r[1] >= self.r1

    def valid_on(self, stream: Any) -> bool:
        """The cut belongs to the timeline it was taken on.  Ask **at the swap**, after :meth:`has_passed`.

        ``stream`` is what ``IDENTIFY_SYSTEM`` says on the node the stream reads from: an object with
        ``timeline`` and ``position`` (``cdc_v2.pg.slot.timeline_history`` returns one; a
        :class:`TimelineReading` is one).  It is read at the swap, not earlier.  Nothing else is asked of the
        caller: the reading of the cut's own node is on the cut (:attr:`sealed`), made by the function that
        gave the cut out.

        True only if the cut is sealed and the stream's node is on the recorded timeline at a position beyond
        ``R1``.  If it is false at the swap (a promotion of the stream's node, a reading that could not be
        made: pass ``None``), the cut is abandoned, with every answer it gave and everything read under its
        snapshot, and the table's backfill starts again.  The stream lane also abandons every open cut when
        its slot check has a finding.

        Why this is enough.  The recorded timeline is the one in the control file (:func:`timeline`), which
        is at or below the timeline replay was on when the cut was taken; and while a server runs, the
        timeline its replay is on never goes back.  So, within one server life: recorded <= timeline of the
        cut <= timeline read later.  The reading on the cut was made later in the same server life (its start
        time is equal), and if the outer two are equal the cut was taken on that timeline: the snapshot holds
        that timeline's WAL up to ``R1`` at most.  That is a fact about the cut, and it stays true when the
        reader's node restarts or is promoted afterwards.  The stream's node is on the same timeline and has
        its WAL beyond ``R1``, and one timeline has one content.  (That what the stream has applied is the
        history of its node is not shown here: it is what the slot check of the stream lane shows, core
        design section 8.)

        Why the stream's reading is made at the swap.  One made while the cut is open says nothing about what
        the stream delivers afterwards: the stream's node can still leave the timeline before it reaches
        ``R1`` (a reader that is ahead of the fork has then read a dead branch).

        Why the reader's reading is made by the taker.  It must be made after the cut and in the cut's server
        life: after a restart a standby replays again from its last restart point and can be on an earlier
        timeline than before, or follow another history.  A caller could make it too early or too late; the
        taker makes it at once, and a restart before it ends the cut's transaction anyway.
        """
        s = _reading(stream)
        if s is None or not self.sealed:
            return False
        return s[0] == self.timeline and s[1] > self.r1

    def unknown_window(self) -> tuple[int, Optional[int]]:
        """``(R0, E1]``: the positions whose transactions the position rule leaves unknown (E1 None = open)."""
        return self.r0, self.e1


@dataclass(frozen=True)
class PrimaryCut:
    """The cut of one snapshot taken on a primary: the snapshot's ``xmin``, ``xmax`` and in-progress list.

    It never answers ``unknown``.  The stream carries the top-level transaction id, and the in-progress list
    holds top-level ids only, so subtransactions need no special care.

    ``timeline`` is the timeline the snapshot was taken on (exact on a primary).  ``insert_lsn`` is the WAL
    insert position read by the statement that took the snapshot, after it was taken: the commit record of
    every transaction the snapshot sees ends at or before it.  ``server_start`` is the start time of the
    server (:func:`server_start`).  A cut made by hand without them classifies, but has not passed and is valid
    nowhere.  ``reader`` and ``reader_start`` are what the same server said once it had flushed its WAL up to
    ``insert_lsn``: its timeline, its flush position and its start time (:func:`seal_primary_cut`, which
    :func:`take_primary_cut_after` calls).  A cut without them is valid nowhere.
    """

    bounds: SnapshotBounds
    extra: dict = field(default_factory=dict, compare=False)
    timeline: Optional[int] = None
    insert_lsn: Optional[int] = None
    server_start: Optional[int] = None
    reader: Optional[TimelineReading] = None
    reader_start: Optional[int] = None

    def __post_init__(self) -> None:
        if self.insert_lsn is not None and not 0 <= self.insert_lsn <= lsn.MAX:
            raise CutError(f"not a WAL position: {self.insert_lsn!r}")

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

    def sees_all(self, full_xids) -> bool:
        """The exact rule: every one of these (64-bit) ids of committed transactions is inside."""
        return all(self.classify_full(x) is INSIDE for x in full_xids)

    def not_seen(self, full_xids) -> tuple[int, ...]:
        """Those of the (64-bit) ids that are not inside (for the report)."""
        return tuple(sorted(x for x in full_xids if self.classify_full(x) is not INSIDE))

    def has_passed(self, applied_position: int) -> bool:
        """The stream has passed this snapshot: it has applied a transaction whose commit record ends at or
        after the insert position read after the snapshot was taken.

        Every transaction the snapshot sees has then been delivered, and its commit record is on disk: the
        slot decodes flushed WAL only, while a snapshot can see a transaction whose commit record is not
        flushed yet (``synchronous_commit = off``).  A cut without an insert position has not passed.
        """
        return self.insert_lsn is not None and applied_position >= self.insert_lsn

    @property
    def sealed(self) -> bool:
        """The server the cut was taken on, in the same server life and on the cut's timeline, had flushed its
        WAL up to the cut's insert position: everything the snapshot saw is on disk."""
        r = _reading(self.reader)
        if (r is None or self.timeline is None or self.insert_lsn is None or self.server_start is None
                or self.reader_start is None):
            return False
        if isinstance(self.reader_start, bool) or int(self.reader_start) != self.server_start:
            return False
        return r[0] == self.timeline and r[1] >= self.insert_lsn

    def valid_on(self, stream: Any) -> bool:
        """As :meth:`StandbyCut.valid_on`: asked at the swap after :meth:`has_passed`, with the stream's reading
        made then (on a primary the stream's node and the reader's node are normally the same one).

        True only if the cut is sealed and the stream's node is on the cut's timeline with WAL up to the
        cut's insert position.  The seal is the fact about the cut here (:attr:`sealed`): a snapshot can see a
        transaction whose commit record is not flushed (``synchronous_commit = off``).  A crash loses that
        record, and the server then writes other records at the same positions of the same timeline: a
        transaction applied after the restart would say "passed" for a snapshot that saw a transaction which
        no longer exists.  :func:`seal_primary_cut` therefore waits, in the cut's own server life, until the
        flush position has reached the insert position, and no cut is given out for use before that.
        """
        s = _reading(stream)
        if s is None or not self.sealed:
            return False
        return s[0] == self.timeline and s[1] >= self.insert_lsn


@dataclass(frozen=True)
class GapMark:
    """What the stream on a primary knows, at one stream position, about transactions it has delivered and a
    snapshot may not see yet.  Made by :meth:`GapTracker.mark`."""

    gap: frozenset                      # 64-bit ids of delivered transactions that no probe snapshot has seen
    unknown_below: Optional[int] = None  # transactions delivered before the tracker existed have ids below this

    def admits(self, cut: "PrimaryCut") -> bool:
        """True if the snapshot sees every transaction the stream had delivered when the mark was made.

        The snapshot must have been taken after the mark was made.
        """
        if self.unknown_below is not None and cut.bounds.xmin < self.unknown_below:
            return False
        return cut.sees_all(self.gap)

    def blocking(self, cut: "PrimaryCut") -> tuple[int, ...]:
        """The transaction ids that keep :meth:`admits` false (for the report)."""
        ids = set(cut.not_seen(self.gap))
        if self.unknown_below is not None:
            ids.update(x for x in cut.bounds.xip if x < self.unknown_below)
        return tuple(sorted(ids))


class GapTracker:
    """On a primary: which delivered transactions may a new snapshot still not see?

    A commit record is written, and the slot can deliver the transaction, before the transaction becomes visible
    to snapshots (docs/design/cut.md, "The cut on a primary").  A snapshot that sees a committed transaction
    proves that every later snapshot sees it too.  So the tracker keeps only the delivered transactions that no
    probe snapshot has seen so far; normally that set is empty.

    * :meth:`delivered` for every transaction the stream delivers, with its **64-bit** id;
    * :meth:`probe` with a snapshot of the primary (:func:`probe_snapshot`), once per applied group (that keeps
      the set small; without probes it holds every delivered id);
    * :meth:`mark` at a table's switch.  A snapshot taken after that is usable for the table exactly if
      :meth:`GapMark.admits` it.

    **The ids.**  The stream carries the 32-bit id of a transaction's ``BEGIN``.  :meth:`widen` makes the
    64-bit id from it with the ``xmax`` of the latest probe as the reference, and that is the only reference
    the stream may use: a probe is at most one group old, so the transaction and the reference are far less than
    2^31 ids apart and the result is exact.  A reference kept from an earlier run, or read from the destination,
    can be 2^31 ids old on a busy source and would give an id of the wrong epoch; the tracker would then wait
    for a transaction that does not exist, or miss the one that does.  Before the first probe the reference is
    ``unknown_below``.  :meth:`delivered` refuses an id that lies 2^31 or more from the reference, which is what
    a 32-bit id passed by mistake looks like once the source has used 2^32 ids.

    ``unknown_below``: a tracker that starts while the stream is already running (an engine start) does not
    know the ids delivered before.  Give it an id assigned after the start (:func:`next_xid_marker`); every
    earlier delivered transaction has a smaller id.  Until a probe has ``xmin`` at or above it, marks also
    demand ``xmin >= unknown_below``, which is the simple rule :meth:`PrimaryCut.usable_after_xid`.
    """

    def __init__(self, unknown_below: Optional[int] = None):
        self._gap: set[int] = set()
        self._unknown_below = unknown_below
        self._reference: Optional[int] = unknown_below

    @property
    def reference(self) -> Optional[int]:
        """The 64-bit id that :meth:`widen` measures from: the ``xmax`` of the latest probe."""
        return self._reference

    def widen(self, xid: int) -> int:
        """The 64-bit id of the stream transaction whose ``BEGIN`` carries the 32-bit id ``xid``."""
        if self._reference is None:
            raise CutError("no probe yet: the tracker has no reference to widen a 32-bit transaction id with")
        return widen_xid(xid, self._reference)

    def delivered(self, full_xid: int) -> None:
        """The stream has delivered the transaction with this 64-bit id (:meth:`widen`)."""
        full_xid = int(full_xid)
        if not FIRST_NORMAL_XID <= full_xid <= XID8_MAX:
            raise CutError(f"not a 64-bit transaction id: {full_xid}")
        if self._reference is not None and abs(full_xid - self._reference) >= XID_HALF:
            raise CutError(f"transaction id {full_xid} is 2^31 or more from the latest probe ({self._reference}): "
                           "a 32-bit id was passed, or the reference is stale; use widen()")
        self._gap.add(full_xid)

    def delivered_xid32(self, xid: int) -> int:
        """:meth:`widen` and :meth:`delivered` in one call; returns the 64-bit id."""
        full = self.widen(xid)
        self.delivered(full)
        return full

    def probe(self, cut: "PrimaryCut") -> None:
        """Forget every delivered transaction this snapshot sees, and make its ``xmax`` the reference.

        Call it only with snapshots taken before the snapshots that will later be judged with a mark (snapshots
        of the stream's own connection, in order)."""
        self._gap = {x for x in self._gap if cut.classify_full(x) is not INSIDE}
        if self._unknown_below is not None and cut.bounds.xmin >= self._unknown_below:
            self._unknown_below = None
        self._reference = cut.bounds.xmax

    def mark(self) -> GapMark:
        return GapMark(frozenset(self._gap), self._unknown_below)

    def __len__(self) -> int:
        return len(self._gap)


# --------------------------------------------------------------------------------------------------------------
# SQL: taking a cut
# --------------------------------------------------------------------------------------------------------------

SQL_REPLAY_LSN = "SELECT pg_last_wal_replay_lsn()::text"
#: The start time of the server in microseconds since 1970: one value per server life, the same in every session.
_SQL_SERVER_START = "(extract(epoch from pg_postmaster_start_time()) * 1000000)::bigint"
SQL_SERVER_START = "SELECT " + _SQL_SERVER_START
#: ``R0``, and with it the timeline of the checkpoint that the control file names (see :func:`timeline`) and
#: the start time of the server.
SQL_R0 = ("SELECT pg_last_wal_replay_lsn()::text, (SELECT timeline_id FROM pg_control_checkpoint()), "
          + _SQL_SERVER_START)
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
_SQL_INSERT_WALFILE = ("CASE WHEN pg_is_in_recovery() THEN NULL "
                       "ELSE pg_walfile_name(pg_current_wal_insert_lsn()) END")
_SQL_INSERT_LSN = "CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_insert_lsn()::text END"
#: A primary's snapshot; then, read while the statement runs and so after the snapshot was taken, the WAL
#: insert position and the WAL file it lies in (the name starts with the timeline); and the server's start time.
SQL_PRIMARY_FIRST = ("SELECT pg_current_snapshot()::text, pg_is_in_recovery(), " + _SQL_INSERT_LSN + ", "
                     + _SQL_INSERT_WALFILE + ", " + _SQL_SERVER_START)
SQL_NEXT_XID = "SELECT pg_current_xact_id()::text, pg_is_in_recovery()"
#: The next transaction id of the checkpoint that the standby's control file names (text ``epoch:xid``).
SQL_SETTLE_FLOOR = "SELECT next_xid, pg_is_in_recovery() FROM pg_control_checkpoint()"
SQL_TIMELINE = "SELECT (SELECT timeline_id FROM pg_control_checkpoint()), " + _SQL_INSERT_WALFILE
#: A replication command, for a ``replication=database`` connection: system id, timeline, position, database.
#: On a standby the timeline is the one replay is on, and the position is at or beyond the replay position.
SQL_IDENTIFY = "IDENTIFY_SYSTEM"
#: A primary's flush position, the WAL file it lies in (the name starts with the timeline) and its start time.
SQL_PRIMARY_FLUSH = ("SELECT pg_is_in_recovery(), "
                     "CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_flush_lsn()::text END, "
                     "CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_walfile_name(pg_current_wal_flush_lsn()) END, "
                     + _SQL_SERVER_START)


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


def _replay(text: Optional[str], what: str) -> int:
    if text is None:
        raise CutError(f"{what}: pg_last_wal_replay_lsn() is NULL; this server has never replayed WAL")
    return lsn.parse(text)


def parse_epoch_xid(text: Any) -> int:
    """``'2:17'`` (epoch, 32-bit id; the form of ``pg_control_checkpoint().next_xid``) -> 64-bit id."""
    parts = str(text).strip().split(":")
    try:
        if len(parts) != 2:
            raise ValueError
        epoch, xid = int(parts[0]), int(parts[1])
    except ValueError:
        raise ValueError(f"not an epoch:xid value: {text!r}") from None
    if not (0 <= epoch <= XID_MASK and 0 <= xid <= XID_MASK):
        raise ValueError(f"not an epoch:xid value: {text!r}")
    return (epoch << XID_BITS) | xid


def _walfile_timeline(name: Any) -> int:
    text = str(name)
    try:
        if len(text) < 8:
            raise ValueError
        return int(text[:8], 16)
    except ValueError:
        raise CutError(f"not a WAL file name: {name!r}") from None


def timeline(conn) -> int:
    """The timeline of the server behind ``conn``, as far as SQL can tell.

    On a primary it is exact: the timeline of the WAL insert position.  On a standby SQL does not show the
    timeline that replay is on; the value is the timeline of the checkpoint named in the control file, which is
    the replay timeline or, between a timeline switch and the standby's next restart point, the one before it.
    It never runs ahead.  A standby cut records this value, read before its snapshot is taken.  Compared with
    an exact timeline of the same node read later in the same server life (:func:`read_node`, which the
    takers call right after the cut), the lag can only keep a good cut from being given out until the
    standby's next restart point.  Compared with a value read before the cut, in another server life, or on
    another node only, it could say yes to a cut taken after a switch; the takers therefore make the reading
    themselves.  The exact timeline of a standby comes from ``IDENTIFY_SYSTEM``, not from this function.
    """
    _require_autocommit(conn)
    control, walfile = _one(conn, SQL_TIMELINE)
    return _walfile_timeline(walfile) if walfile is not None else int(control)


def server_start(conn) -> int:
    """The start time of the server behind ``conn``, in microseconds since 1970: it names one server life.

    A cut records it (the statement that reads ``R0``, or the one that takes a primary's snapshot, reads it),
    and the reading made right after the cut must show the same value (:func:`read_node`,
    :func:`seal_primary_cut`).
    """
    return int(_one(conn, SQL_SERVER_START)[0])


def read_node(replication) -> tuple[TimelineReading, int]:
    """``IDENTIFY_SYSTEM`` and then the server's start time, on a replication connection.

    ``replication`` is a psycopg connection in autocommit mode that was opened with ``replication=database``
    (with the source package: ``connect(target, deadline, replication=True).raw``).  The reading is exact
    where SQL is not: on a standby ``IDENTIFY_SYSTEM`` names the timeline replay is on.  No SQL function of
    Postgres 16 to 18 does: ``pg_last_wal_replay_lsn()`` drops the timeline, ``pg_control_checkpoint()`` has
    the one of the last restart point, ``pg_stat_wal_receiver`` the one of the receiver (and no row without a
    receiver), and ``pg_walfile_name()`` is refused during recovery.  The start time is plain SQL, which such
    a connection also accepts when it is sent without parameters.
    """
    _require_autocommit(replication)
    try:
        row = _one(replication, SQL_IDENTIFY)
    except CutError:
        raise
    except Exception as exc:
        if getattr(exc, "sqlstate", None) == "42601":   # a syntax error: this is not a walsender
            raise CutError("the reading of the cut's node needs a replication connection "
                           "(replication=database): IDENTIFY_SYSTEM was refused") from exc
        raise
    try:
        reading = TimelineReading(int(row[1]), lsn.parse(row[2]))
    except (IndexError, TypeError, ValueError):
        raise CutError(f"not an answer to IDENTIFY_SYSTEM: {row!r}") from None
    return reading, int(_one(replication, SQL_SERVER_START)[0])


class TimelineNotProven(CutError):
    """A standby cut was dropped because the reading made after it names another timeline than the standby's
    control file: the standby has followed a timeline switch and has had no restart point since.  Take
    another one later."""


def _seal_standby(cut: StandbyCut, replication) -> None:
    """Read the cut's node right after the cut and keep the reading on the cut."""
    reading, started = read_node(replication)
    if cut.server_start is None or started != cut.server_start:
        raise CutError("the replication connection is not to the server the cut was taken on: its start time "
                       "is another one")
    if reading.position < cut.r1:     # cannot happen in one server life: the position never moves back
        raise CutError("the replication connection is not to the node the cut was taken on: it is at %s, "
                       "before R1 %s" % (lsn.format(reading.position), lsn.format(cut.r1)))
    cut.reader, cut.reader_start = reading, started
    if not cut.sealed:
        raise TimelineNotProven(
            "the timeline of the cut is not proven: the standby replays timeline %d and its last restart point "
            "is on timeline %s (after a timeline switch, until the standby's next restart point)"
            % (reading.timeline, cut.timeline))


_READ_ON_THE_STANDBY = object()     # what settle_floor() hands to SettleFloor; nothing else has it by accident


class SettleFloor:
    """The settle floor of one standby, bound to the connection it was read on.  Made by :func:`settle_floor`.

    ``value`` is a 64-bit transaction id.  The floor of one standby says nothing about another, and nothing
    about the same standby after a restart, so the functions that take a cut accept the floor only together
    with the connection it was read on.

    A floor is read, not made: the constructor refuses every caller but :func:`settle_floor`, and a floor
    cannot be changed afterwards.  This keeps a made-up floor (``0``, or another standby's value) out of
    production code by accident; it does not stop code that reaches for the private names on purpose.  Tests
    that need a made-up floor get it from ``cdc_v2.testing.cutnarrow.made_up_settle_floor``.
    """

    __slots__ = ("_conn", "_value")

    def __init__(self, conn, value: int, _read_by: Any = None):
        if _read_by is not _READ_ON_THE_STANDBY:
            raise CutError("a settle floor is read, not made: call settle_floor(conn) on the connection that "
                           "takes the cuts")
        if conn is None:
            raise CutError("a settle floor belongs to a connection")
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= XID8_MAX:
            raise CutError(f"not a settle floor: {value!r}")
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_value", value)

    def __setattr__(self, name: str, value: Any) -> None:
        raise CutError("a settle floor cannot be changed; read a new one with settle_floor(conn)")

    def __delattr__(self, name: str) -> None:
        raise CutError("a settle floor cannot be changed; read a new one with settle_floor(conn)")

    @property
    def value(self) -> int:
        return self._value

    def for_connection(self, conn) -> int:
        """The value, if ``conn`` is the connection the floor was read on; :class:`CutError` otherwise."""
        if conn is not self._conn:
            raise CutError("this settle floor was read on another connection; read settle_floor(conn) once on "
                           "every connection that takes cuts")
        return self._value

    def __repr__(self) -> str:
        return f"SettleFloor({self._value})"


def settle_floor(conn) -> SettleFloor:
    """Read the settle floor of the standby behind ``conn``.

    It is the next transaction id of the checkpoint named in the control file.  Every transaction that the
    standby may hide after its start has a smaller id (docs/design/cut.md, "A standby that has just started").
    Read it **once per connection, before the first cut**: a restart of the standby ends the connection, the
    value only grows while the standby runs, and the ``xmax`` of its snapshots never moves back.  So once one
    cut taken on this connection is settled against this value, every later cut on it is settled too.
    """
    _require_autocommit(conn)
    text, in_recovery = _one(conn, SQL_SETTLE_FLOOR)
    if not in_recovery:
        raise CutError("not a standby: pg_is_in_recovery() is false")
    return SettleFloor(conn, parse_epoch_xid(text), _READ_ON_THE_STANDBY)


def _floor_value(conn, floor: Any) -> int:
    if not isinstance(floor, SettleFloor):
        raise CutError("a standby cut needs the settle floor of its connection: pass settle_floor(conn), "
                       f"not {type(floor).__name__}")
    return floor.for_connection(conn)


def _check_settled(bounds: SnapshotBounds, floor: int) -> None:
    if bounds.xmax < floor:
        raise StandbyNotSettled(
            f"the standby has not settled after its start: snapshot xmax {bounds.xmax} is below the next "
            f"transaction id {floor} of its control file; take another snapshot after the next replayed commit",
            floor, bounds.xmax)


def take_standby_cut_own(conn, settle_floor: SettleFloor, *, accept_unsettled: bool = False) -> StandbyCut:
    """Take a cut with the reader's own snapshot on a hot standby.  One attempt.

    ``settle_floor`` is what :func:`settle_floor` returned for this connection.  A snapshot of a standby that
    has not settled is dropped and :class:`StandbyNotSettled` is raised.  ``accept_unsettled`` is for tests
    that show what such a cut does; with it the cut is returned and :meth:`StandbyCut.is_settled` says what it
    is.

    Statement 1 (autocommit) reads ``R0``.  Then a REPEATABLE READ transaction is opened; its first statement
    takes the snapshot and reads ``R1``.  The transaction is **left open**: the caller reads under the snapshot
    and ends the transaction.  Raises :class:`CutError` (transaction rolled back) if the server is not in
    recovery when ``R1`` is read: after a promotion the replay position no longer moves and says nothing.
    """
    _require_autocommit(conn)
    floor = _floor_value(conn, settle_floor)
    r0_text, control_timeline, started = _one(conn, SQL_R0)
    r0 = _replay(r0_text, "R0")
    conn.execute(SQL_BEGIN_RR)
    try:
        r1_text, in_recovery, snap = _one(conn, SQL_OWN_FIRST)
        if not in_recovery:
            raise CutError("not a standby: pg_is_in_recovery() is false; use take_primary_cut")
        r1 = _replay(r1_text, "R1")
        bounds = SnapshotBounds.parse(snap)
        if not accept_unsettled:
            _check_settled(bounds, floor)
        return StandbyCut(r0=r0, r1=r1, bounds=bounds, way="own", r_stmt=r1, timeline=int(control_timeline),
                          server_start=int(started))
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

    This function is the one place that knows the versions; everything else asks it.

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


def server_version_num(conn) -> int:
    """``server_version_num`` of the server behind ``conn`` (the argument of :func:`export_defect`)."""
    return int(_one(conn, SQL_VERSION_NUM)[0])


def take_standby_cut_exported(conn, settle_floor: SettleFloor, *, quiet_only: bool = False,
                              accept_export_defect: bool = False,
                              accept_unsettled: bool = False) -> StandbyCut:
    """Take a cut with an exported snapshot on a hot standby.  One attempt.

    One READ COMMITTED transaction with three statements: ``R0``; ``pg_export_snapshot()`` (its statement
    snapshot is the exported one); ``R1``.  The transaction is **left open**: the snapshot can be imported
    (:func:`import_snapshot`) only while it lives.  The connection touches no table, so it holds no relation
    lock.

    With ``quiet_only`` the snapshot is kept only if it is quiet (:attr:`StandbyCut.is_quiet`); otherwise the
    transaction is rolled back and :class:`SnapshotNotQuiet` is raised.  A quiet snapshot is safe to export on
    every server version, and its cut is exact.  :func:`take_standby_cut_after` repeats the attempt within a
    deadline.

    Without ``quiet_only`` the call is refused on a server with the export defect (:func:`export_defect`).
    ``accept_export_defect`` and ``accept_unsettled`` are for tests that show the defects.

    ``settle_floor``: as for :func:`take_standby_cut_own`.
    """
    _require_autocommit(conn)
    floor = _floor_value(conn, settle_floor)
    if not (accept_export_defect or quiet_only):
        version = server_version_num(conn)
        if export_defect(version):
            raise CutError("exported snapshots are not safe on a standby of server version %d: it lacks %s; "
                           "take the cut with take_standby_cut_own, or with quiet_only" % (version, EXPORT_FIX))
    conn.execute(SQL_BEGIN_RC)
    try:
        r0_text, control_timeline, started = _one(conn, SQL_R0)
        r0 = _replay(r0_text, "R0")
        snapshot_id, r_stmt_text, in_recovery_a, snap = _one(conn, SQL_EXPORT)
        r1_text, in_recovery_b = _one(conn, SQL_R1)
        if not (in_recovery_a and in_recovery_b):
            raise CutError("not a standby: pg_is_in_recovery() is false; use take_primary_cut")
        bounds = SnapshotBounds.parse(snap)
        if not accept_unsettled:
            _check_settled(bounds, floor)
        if quiet_only and bounds.xmin != bounds.xmax:
            raise SnapshotNotQuiet("a transaction was running when the snapshot %s was taken; take another one"
                                   % snap, bounds)
        return StandbyCut(r0=r0, r1=_replay(r1_text, "R1"), bounds=bounds, way="exported",
                          snapshot_id=snapshot_id, r_stmt=_replay(r_stmt_text, "in-statement position"),
                          timeline=int(control_timeline), server_start=int(started))
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


def _primary_cut(conn) -> PrimaryCut:
    snap, in_recovery, insert_text, walfile, started = _one(conn, SQL_PRIMARY_FIRST)
    if in_recovery or walfile is None or insert_text is None:
        raise CutError("not a primary: pg_is_in_recovery() is true; use a standby cut")
    return PrimaryCut(SnapshotBounds.parse(snap), timeline=_walfile_timeline(walfile),
                      insert_lsn=lsn.parse(insert_text), server_start=int(started))


def take_primary_cut(conn) -> PrimaryCut:
    """Take a cut on a primary: a REPEATABLE READ transaction whose first statement takes the snapshot and reads
    ``pg_current_snapshot()`` and, after it, the WAL insert position (:meth:`PrimaryCut.has_passed`).  The
    transaction is **left open**."""
    _require_autocommit(conn)
    conn.execute(SQL_BEGIN_RR)
    try:
        return _primary_cut(conn)
    except BaseException:
        _rollback_quietly(conn)
        raise


def probe_snapshot(conn) -> PrimaryCut:
    """One statement on a primary connection: the current snapshot, for :meth:`GapTracker.probe`.

    No transaction is opened and nothing is left open.  The stream lane calls it once per group on its own
    primary connection.
    """
    _require_autocommit(conn)
    return _primary_cut(conn)


def next_xid_marker(conn) -> int:
    """Assign one transaction id on the primary and return it as a 64-bit id (``X`` of the primary rule).

    Every transaction id assigned earlier is smaller.  Two callers: the stream lane after an engine start, for
    ``GapTracker(unknown_below=...)``; and a backfill that uses the simple rule ``xmin >= X``, after the table's
    switch to ``backfilling`` is committed.  It costs one transaction id.
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

#: The declared wait for a quiet snapshot (docs/design/cut.md, "Tables without a key").
QUIET_WAIT_DEFAULT_S = 60.0


def wait_replay_reached(conn, target: int, deadline: Deadline, *, poll_s: float = 0.05) -> int:
    """Wait until the standby has replayed up to ``target`` (so that a cut taken next has ``R0 >= target``).

    Returns the replay position seen.  Raises ``DeadlineExceeded`` when the deadline passes first; its text
    says how far replay had come.
    """
    _require_autocommit(conn)
    while True:
        seen = _replay(_one(conn, SQL_REPLAY_LSN)[0], "replay position")
        if seen >= target:
            return seen
        deadline.sleep(poll_s, "standby replay at %s, waited for %s (%d bytes behind)" % (
            lsn.format(seen), lsn.format(target), target - seen))


def take_standby_cut_after(conn, replication, p0: int, deadline: Deadline, settle_floor: SettleFloor, *,
                           exported: bool = False, quiet_only: bool = False, poll_s: float = 0.05,
                           report: Optional[Callable[[NoQuietSnapshot], None]] = None,
                           ) -> Union[StandbyCut, NoQuietSnapshot]:
    """Wait for replay to reach ``P0``, then take a cut that may be used: ``R0 >= P0``, settled and sealed.

    The cut's transaction is left open.  ``settle_floor`` is what :func:`settle_floor` returned for this
    connection.  One deadline covers all waits; ``DeadlineExceeded`` says what was last seen (replay behind
    ``P0``, a standby that did not settle, or a timeline that could not be proven).

    ``replication`` is a second connection **to the same standby**, opened with ``replication=database``
    (:func:`read_node` says why SQL does not do).  Right after each cut the node's ``IDENTIFY_SYSTEM`` and
    start time are read on it and kept on the cut (``cut.reader``, ``cut.reader_start``); the caller reads
    nothing about the reader's node for :meth:`StandbyCut.valid_on`.  A start time other than the cut's is
    a :class:`CutError` (the two connections are not to one server).  If the reading names another timeline
    than the cut recorded (the standby followed a timeline switch and has had no restart point since), the
    cut is dropped and another one is taken, until the deadline.

    ``exported`` takes an exported snapshot; on a server with the export defect that is refused
    (:class:`CutError`) unless ``quiet_only`` is set.  With ``quiet_only`` a snapshot that is not quiet is
    dropped and another one is taken; if none is quiet when the deadline passes, the call **returns**
    :class:`NoQuietSnapshot` (which is false in a condition) instead of raising: an open write transaction on
    the primary is no error, and the caller goes on with one reader and its own snapshot.  Without
    ``quiet_only`` the result is always a :class:`StandbyCut`.

    ``report``, if given, is called after every snapshot that was dropped because it was not quiet, with a
    :class:`NoQuietSnapshot` that says how many were dropped so far and which transaction (64-bit id) was the
    oldest still running.  The caller logs it at its own rate, so that the operator sees during the wait which
    transaction is waited for.
    """
    if quiet_only and not exported:
        raise ValueError("quiet_only is a rule for exported snapshots")
    _floor_value(conn, settle_floor)
    _require_autocommit(replication)
    wait_replay_reached(conn, p0, deadline, poll_s=poll_s)
    attempts = 0
    while True:
        attempts += 1
        try:
            if exported:
                cut = take_standby_cut_exported(conn, settle_floor, quiet_only=quiet_only)
            else:
                cut = take_standby_cut_own(conn, settle_floor)
        except StandbyNotSettled as exc:
            deadline.sleep(poll_s, "the standby has not settled: snapshot xmax %d stays below %d; no commit or "
                                   "abort was replayed (is the heartbeat write running?)" % (exc.xmax, exc.floor))
            continue
        except SnapshotNotQuiet as exc:
            state = NoQuietSnapshot(attempts, exc.bounds.xmin if exc.bounds is not None else None)
            if report is not None:
                report(state)
            try:
                deadline.sleep(poll_s, "no quiet snapshot")
            except DeadlineExceeded:
                return state
            continue
        try:
            if not cut.usable_after(p0):  # cannot happen: the replay position never moves back while it runs
                raise CutError(f"R0 {lsn.format(cut.r0)} is before P0 {lsn.format(p0)}")
            _seal_standby(cut, replication)
            return cut
        except TimelineNotProven as exc:
            _rollback_quietly(conn)
            deadline.sleep(max(poll_s, 0.5), str(exc))
        except BaseException:
            _rollback_quietly(conn)
            raise


def take_keyless_standby_cut(conn, replication, p0: int, deadline: Deadline, settle_floor: SettleFloor, *,
                             quiet_wait_s: float = QUIET_WAIT_DEFAULT_S, poll_s: float = 0.05,
                             report: Optional[Callable[[NoQuietSnapshot], None]] = None) -> StandbyCut:
    """The cut for a table without a key on a standby (docs/design/cut.md, "Tables without a key").

    ``replication``: as for :func:`take_standby_cut_after`; the cut that is returned is sealed in every case.

    * A server without the export defect: an exported snapshot; ``cut.way == "exported"``.
    * A server with it: a quiet exported snapshot, waited for at most ``quiet_wait_s`` after replay has reached
      ``P0``; ``cut.way == "exported"``.
    * If none came: this connection's own snapshot; ``cut.way == "own"``, this connection is the one reader, and
      ``cut.extra["quiet_wait"]`` holds the :class:`NoQuietSnapshot` for the log: it says whether a transaction
      was running throughout (and which one at the end) or the standby had not settled within the wait.

    ``report``: as for :func:`take_standby_cut_after`; it is called during the quiet wait.

    With ``way == "exported"`` any number of readers may import ``cut.snapshot_id``.  The transaction on
    ``conn`` is left open in every case.  ``deadline`` covers everything; the quiet wait never extends it.  As
    everywhere in this module a wait ends at the deadline, and one attempt is made before the first wait.
    """
    _require_autocommit(conn)
    _floor_value(conn, settle_floor)
    if not export_defect(server_version_num(conn)):
        return take_standby_cut_after(conn, replication, p0, deadline, settle_floor, exported=True, poll_s=poll_s)
    wait_replay_reached(conn, p0, deadline, poll_s=poll_s)
    try:
        got = take_standby_cut_after(conn, replication, p0, deadline.within(quiet_wait_s), settle_floor,
                                     exported=True, quiet_only=True, poll_s=poll_s, report=report)
    except DeadlineExceeded as exc:   # no cut at all within the quiet wait: the own way waits on, to the full deadline
        got = NoQuietSnapshot(0, None, not_settled=True, detail=exc.what)
    if isinstance(got, StandbyCut):
        return got
    cut = take_standby_cut_after(conn, replication, p0, deadline, settle_floor, poll_s=poll_s)
    cut.extra["quiet_wait"] = got
    return cut


def seal_primary_cut(conn, cut: PrimaryCut, deadline: Deadline, *, poll_s: float = 0.05) -> PrimaryCut:
    """Wait until the primary has flushed its WAL up to the cut's insert position; return the cut with that
    reading on it (``reader``, ``reader_start``).  Only such a cut can be valid (:meth:`PrimaryCut.valid_on`).

    ``conn`` is the connection the cut was taken on; its transaction stays open.  The reading is plain SQL and
    exact: a primary writes on one timeline for its whole server life, the one in the name of its current WAL
    file, and ``pg_current_wal_flush_lsn()`` is the position ``IDENTIFY_SYSTEM`` would give.  The start time
    must be the cut's (else :class:`CutError`: another server, or another life of it).

    The wait is short where anything commits: a commit with ``synchronous_commit`` on flushes at once (the
    heartbeat write is one), and the WAL writer flushes an asynchronous commit within three of its cycles.
    ``DeadlineExceeded`` says how far the flush position had come.  :func:`take_primary_cut_after` calls
    this; a caller of :func:`take_primary_cut` calls it once it has decided to use the cut.
    """
    _require_autocommit(conn)
    if cut.insert_lsn is None or cut.timeline is None or cut.server_start is None:
        raise CutError("a primary cut without an insert position, a timeline or a start time cannot be sealed")
    while True:
        in_recovery, flush_text, walfile, started = _one(conn, SQL_PRIMARY_FLUSH)
        if in_recovery or flush_text is None or walfile is None:
            raise CutError("not a primary: pg_is_in_recovery() is true")
        if int(started) != cut.server_start:
            raise CutError("this connection is not to the server the cut was taken on: its start time is "
                           "another one")
        flushed, now_on = lsn.parse(flush_text), _walfile_timeline(walfile)
        if now_on != cut.timeline:    # cannot happen: a running primary does not change its timeline
            raise CutError("the primary is on timeline %d, the cut was taken on timeline %d" % (now_on, cut.timeline))
        if flushed >= cut.insert_lsn:
            return replace(cut, reader=TimelineReading(now_on, flushed), reader_start=int(started))
        deadline.sleep(poll_s, "the primary has flushed its WAL up to %s; the cut's insert position is %s "
                               "(nothing has committed since; is the heartbeat write running?)"
                       % (lsn.format(flushed), lsn.format(cut.insert_lsn)))


def take_primary_cut_after(conn, x: int, deadline: Deadline, *, poll_s: float = 0.1) -> PrimaryCut:
    """Take cuts on a primary until one has ``xmin >= X``; seal it (:func:`seal_primary_cut`) and return it
    with its transaction left open.

    Raises ``DeadlineExceeded`` when the deadline passes first; its text names the transactions (64-bit ids)
    that were still in progress below ``X`` at the last attempt, for the operator, or says how far the flush
    position had come.  No transaction is left open then.
    """
    _require_autocommit(conn)
    while True:
        cut = take_primary_cut(conn)
        if cut.usable_after_xid(x):
            try:
                return seal_primary_cut(conn, cut, deadline, poll_s=min(poll_s, 0.05))
            except BaseException:
                _rollback_quietly(conn)
                raise
        end_transaction(conn)
        blocking = cut.blocking_xids(x)
        deadline.sleep(poll_s, "transactions older than id %d are still open: %d in progress, oldest %d (%s%s)" % (
            x, len(blocking), cut.bounds.xmin, ", ".join(str(i) for i in blocking[:8]),
            ", ..." if len(blocking) > 8 else ""))


def classify_all(cut: StandbyCut, transactions: Sequence[tuple[int, Optional[int]]]) -> list[Verdict]:
    """Classify ``(E, xid)`` pairs given in stream order, feeding each to :meth:`StandbyCut.observe` first."""
    out = []
    for e, xid in transactions:
        cut.observe(e)
        out.append(cut.classify(e, xid))
    return out
