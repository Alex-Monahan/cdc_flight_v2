"""From frames to transactions to groups (task S section 4; design sections 2, 3, 6.5).

- `Assembler` turns `BEGIN … END` frames into transactions (`OpenTxn`), checks the `END` event count against the
  records received (in total and per table), and decides **once, by position**, in `already_applied`, whether a
  transaction is discarded as a whole (design 2).
- `Group` holds one or more whole transactions in memory, per destination table, cut into **segments** at each
  TRUNCATE of that table (design 6.5), with the routing of each table as it stood when the group began (C10). Its
  ROWS batches stay in the engine's transport form until `build` concatenates the consecutive batches of one table,
  plan and segment and transforms them once (plan C3, "concatenate before the transform"; the transform still runs
  before anything is staged, spilled or sent).
- `build` returns the change sets of package A's local form (C3a), per table and segment, in commit order.

Nothing here touches the destination. No source value is ever logged (C0): messages carry tables, positions and
counts only.
"""
from __future__ import annotations

import collections
import logging
from dataclasses import dataclass, field
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc

from cdc_v2.stream import frames as F

log = logging.getLogger("cdc_v2.stream")

#: The name Debezium gives the data collection of a logical decoding message: it ends in an object hash that
#: changes, so it is matched by prefix (docs/design/engine.md 9).
MESSAGE_COLLECTION_PREFIX = "io.debezium.connector.postgresql.LogicalDecodingMessageMonitor$LogicalDecodingMessageId"


class ProtocolError(Exception):
    """The frames break the protocol in a way that belongs to no table: an END count that differs from the records
    received, a record outside a transaction, a BEGIN inside one, an END without a position (task S section 7).
    Nothing of the group is applied; the stream component restarts its engine. The message holds positions,
    counts and table names only."""


class TableFailure(Exception):
    """A failure that belongs to one table (task S section 7): `ConversionError`, `PlanMismatch`,
    `ApplyInvariantError`, a statement the destination refuses. `kind` is a short identifier for the alert."""

    def __init__(self, key, kind: str, message: str = ""):
        self.key = tuple(key)
        self.kind = kind
        super().__init__("table %s.%s: %s%s" % (key[0], key[1], kind, (" (%s)" % message) if message else ""))


def already_applied(commit_position: int, applied: int) -> bool:
    """Design 2, the one place where a transaction is discarded: a transaction whose commit end is at or below the
    applied position was applied (Debezium re-sends whole transactions after a restart); it is discarded as a
    whole. A greater one is applied as a whole."""
    return commit_position <= applied


# ------------------------------------------------------------------------------------------------ transactions

@dataclass
class Record:
    kind: str                       # rows | truncate
    key: tuple                      # (schema, table)
    version: int = 0
    batch: Optional[pa.RecordBatch] = None
    nbytes: int = 0


class OpenTxn:
    """One source transaction while it arrives: its records in stream order (until they are handed on to staging),
    and the counts the END is checked against."""

    def __init__(self, begin: F.Begin, arrived_at: float):
        self.begin = begin
        self.arrived_at = arrived_at
        self.records: list = []
        self.nbytes = 0                     # bytes of the records still held here
        self.total_bytes = 0
        self.counts = collections.Counter()  # (schema, table) -> rows + truncates received
        self.messages = 0                   # transactional MESSAGE frames
        self.rows = 0
        self.staged = False                 # its records go to staging (a transaction larger than a group)
        self.first_seq: Optional[int] = None

    def add(self, rec: Record, n_events: int):
        self.records.append(rec)
        self.nbytes += rec.nbytes
        self.total_bytes += rec.nbytes
        self.counts[rec.key] += n_events

    def take(self) -> list:
        """Hand the records held so far on (to staging); the counts stay for the END check."""
        out, self.records, self.nbytes = self.records, [], 0
        return out

    @property
    def xid(self) -> Optional[int]:
        v = self.begin.positions.get("txId") if self.begin.positions else None
        return int(v) if isinstance(v, int) and not isinstance(v, bool) else None


@dataclass(frozen=True)
class EndedTxn:
    txn: OpenTxn
    end: F.End
    keep: bool                          # False: discarded by position (already applied)

    @property
    def position(self) -> int:
        return self.end.ack_position


def _batch_bytes(batch: pa.RecordBatch) -> int:
    try:
        return int(batch.nbytes)
    except Exception:                   # noqa: BLE001 - a size is only a bound
        return sum(int(b.size) for c in batch.columns for b in c.buffers() if b is not None)


class Assembler:
    """BEGIN … END → `EndedTxn`. `threshold` starts at the applied position and becomes the commit position of each
    transaction that is kept, so a transaction is discarded only by `already_applied`."""

    def __init__(self, applied: int, clock):
        self.threshold = applied
        self.clock = clock
        self.open: Optional[OpenTxn] = None
        self.ended = 0
        self.discarded = 0

    @property
    def in_transaction(self) -> bool:
        return self.open is not None

    def feed(self, frame) -> Optional[EndedTxn]:
        if isinstance(frame, F.Begin):
            if self.open is not None:
                raise ProtocolError("BEGIN (seq %d) inside an open transaction" % frame.seq)
            self.open = OpenTxn(frame, self.clock())
            return None
        if isinstance(frame, F.Rows):
            t = self._need("ROWS")
            n = frame.batch.num_rows
            key = (frame.schema, frame.table)
            if n and t.first_seq is None:
                t.first_seq = int(frame.batch.column("seq")[0].as_py())
            t.rows += n
            t.add(Record("rows", key, frame.version, frame.batch, _batch_bytes(frame.batch)), n)
            return None
        if isinstance(frame, F.Truncate):
            t = self._need("TRUNCATE")
            t.add(Record("truncate", (frame.schema, frame.table)), 1)
            return None
        if isinstance(frame, F.Message):
            if frame.transactional:
                self._need("MESSAGE").messages += 1      # counted by the END; consumed in P4 (SPEC 7.4)
            return None
        if isinstance(frame, F.End):
            t = self._need("END")
            if frame.ack_position is None:
                raise ProtocolError("END (seq %d) without a commit position" % frame.seq)
            self._check_counts(t, frame)
            self.open = None
            self.ended += 1
            keep = not already_applied(frame.ack_position, self.threshold)
            if keep:
                self.threshold = frame.ack_position
            else:
                self.discarded += 1
            return EndedTxn(t, frame, keep)
        return None                                          # HELLO, SCHEMA, HEARTBEAT, BATCH_END: the caller's

    def _need(self, what) -> OpenTxn:
        if self.open is None:
            raise ProtocolError("%s outside a transaction" % what)
        return self.open

    @staticmethod
    def _check_counts(t: OpenTxn, end: F.End):
        """docs/design/engine.md 9: event_count = ROWS rows + TRUNCATE frames + transactional MESSAGE frames,
        and so per data collection. Collections whose name is one of our "schema.table" texts are compared one by
        one; message collections by prefix; whatever is left (a name Debezium prints in another way) in total."""
        ours = sum(t.counts.values()) + t.messages
        if end.event_count != ours:
            raise ProtocolError("END at %s: event count %d, records received %d" % (end.ack_position,
                                                                                   end.event_count, ours))
        if not end.collections:
            return
        received = collections.Counter()
        for (schema, table), n in t.counts.items():
            received["%s.%s" % (schema, table)] += n
        stated = collections.Counter()
        msgs = 0
        for name, n in end.collections:
            if name.startswith(MESSAGE_COLLECTION_PREFIX):
                msgs += n
            else:
                stated[name] += n
        if msgs != t.messages:
            raise ProtocolError("END at %s: %d messages stated, %d received" % (end.ack_position, msgs, t.messages))
        rest_stated = rest_received = 0
        for name in set(stated) | set(received):
            if name in stated and name in received:
                if stated[name] != received[name]:
                    raise ProtocolError("END at %s: %d events stated for one table, %d received"
                                        % (end.ack_position, stated[name], received[name]))
            else:
                rest_stated += stated.get(name, 0)
                rest_received += received.get(name, 0)
        if rest_stated != rest_received:
            raise ProtocolError("END at %s: %d events of unmatched collections, %d received"
                                % (end.ack_position, rest_stated, rest_received))


# ------------------------------------------------------------------------------------------------ plans

@dataclass
class PlanEntry:
    """What one (schema, table, version) of the engine means for the stream."""
    plan: object = None                 # transform.TablePlan, or None
    changed: bool = False               # the plan's hash differs from the one the destination table was built for
    failure: Optional[str] = None       # the table cannot be applied: a short kind (quarantine at the next group)


# ------------------------------------------------------------------------------------------------ groups

@dataclass
class TxnInfo:
    xid: Optional[int]
    commit_lsn: int
    commit_ms: int
    rows: int


@dataclass
class TableAcc:
    """One table's part of a group: segments of raw batches, cut at each TRUNCATE of the table."""
    key: tuple
    route: object                                   # stream.state.Route at the group's start
    segments: list = field(default_factory=lambda: [[]])   # [[(txn, plan, batch)], ...]; segment i > 0 follows a truncate
    cuts: list = field(default_factory=list)        # the txn of the truncate before segment i + 1
    leave_at: Optional[int] = None                  # first txn whose changes are not applied (schema change, C10)
    leave_kind: Optional[str] = None
    rows: int = 0

    @property
    def truncates(self) -> int:
        return len(self.segments) - 1

    @property
    def has_work(self) -> bool:
        return self.truncates > 0 or any(self.segments)


class Group:
    """Whole transactions held in memory until the group is committed (design 3)."""

    def __init__(self, applied: int, routes: dict, heartbeat_key: Optional[tuple], first_at: float):
        self.prev = applied                          # the position the group is built on
        self.routes = routes                         # routing at the group's start: {(schema, table): Route}
        self.heartbeat_key = heartbeat_key
        self.first_at = first_at
        self.txns: list = []
        self.tables: dict = {}
        self.nbytes = 0
        self.rows = 0
        self.dropped = collections.Counter()         # rows of tables that are not replicated or routed `skip`
        self.unknown = set()                         # tables not in the routing map
        self.failures = {}                           # (schema, table) -> kind: to quarantine in this group
        self.large = None                            # staging.StagedTxn of a transaction larger than a group

    @property
    def end(self) -> int:
        return self.txns[-1].commit_lsn if self.txns else self.prev

    @property
    def empty(self) -> bool:
        return not self.txns

    @property
    def has_work(self) -> bool:
        """Something to apply or to hand to a backfill (otherwise the group only moves the position: an idle
        step)."""
        return bool(self.failures) or self.large is not None or any(t.has_work or t.leave_at is not None
                                                                   for t in self.tables.values())

    def _acc(self, key) -> TableAcc:
        acc = self.tables.get(key)
        if acc is None:
            acc = self.tables[key] = TableAcc(key, self.routes.get(key))
        return acc

    def add(self, ended: EndedTxn, records: list, plans: dict):
        """Add one whole transaction that is kept. `records` are its records still in memory (all of them unless
        it was staged). `plans` maps (schema, table, version) to a PlanEntry."""
        t = ended.txn
        idx = len(self.txns)
        self.txns.append(TxnInfo(t.xid, ended.end.ack_position, ended.end.commit_time_ms, t.rows))
        self.rows += t.rows
        for rec in records:
            self.add_record(idx, rec, plans)

    def add_record(self, idx: int, rec: Record, plans: dict):
        key = rec.key
        route = self.routes.get(key)
        if route is None or not (route.visible or route.to_backfill) or key in self.failures:
            if rec.kind == "rows":
                self.dropped[key] += rec.batch.num_rows
                if route is None and key != self.heartbeat_key:
                    self.unknown.add(key)
            return
        acc = self._acc(key)
        if acc.leave_at is not None:
            if rec.kind == "rows":
                self.dropped[key] += rec.batch.num_rows
            return
        if rec.kind == "truncate":
            acc.segments.append([])
            acc.cuts.append(idx)
            return
        entry = plans.get((key[0], key[1], rec.version))
        if entry is None or entry.plan is None:
            if entry is not None and entry.failure:
                self.failures[key] = entry.failure
                self.tables.pop(key, None)
            else:
                acc.leave_at, acc.leave_kind = idx, "no_plan"
                self._cut_back(acc, idx)
            self.dropped[key] += rec.batch.num_rows
            return
        if entry.changed:
            # C10: from the first transaction that carries the new version, the table's changes are not applied
            acc.leave_at, acc.leave_kind = idx, "schema_changed"
            # what this transaction did to the table under the old version is taken out again
            self._cut_back(acc, idx)
            self.dropped[key] += rec.batch.num_rows
            return
        acc.segments[-1].append((idx, entry.plan, rec.batch))
        acc.rows += rec.batch.num_rows
        self.nbytes += rec.nbytes

    def _cut_back(self, acc: TableAcc, idx: int):
        """Keep only what transactions before `idx` did to the table."""
        keep = 1 + sum(1 for c in acc.cuts if c < idx)
        acc.cuts = acc.cuts[:keep - 1]
        acc.segments = [[x for x in seg if x[0] < idx] for seg in acc.segments[:keep]]
        acc.rows = sum(b.num_rows for seg in acc.segments for _t, _p, b in seg)

    def transactions_table(self) -> pa.Table:
        """C3a `transactions`: per `txn` the transaction id, the commit position and the commit time."""
        return pa.table({
            "xid": pa.array([t.xid for t in self.txns], pa.uint64()),
            "commit_lsn": pa.array([t.commit_lsn for t in self.txns], pa.uint64()),
            "commit_ts": pa.array([t.commit_ms * 1000 for t in self.txns], pa.timestamp("us", tz="UTC")),
        })


# ------------------------------------------------------------------------------------------------ building

@dataclass
class Segment:
    """One segment of one table: a TRUNCATE before it (when `truncate_before`), then the change set (or none)."""
    truncate_before: bool
    rows: Optional[pa.Table]                         # C3a local form rows (with `txn`), or None when empty
    plan: object = None


@dataclass
class TableWork:
    key: tuple
    route: object
    plan: object
    segments: list
    ambiguous: int = 0                               # rows whose zero interval is ambiguous (plan C3)
    leave_kind: Optional[str] = None                 # the table leaves the visible path in this group (C10)


def build(group: Group, transformer, ambiguous_fn) -> dict:
    """The change sets of a group, per table: {(schema, table): TableWork}. Concatenates the consecutive batches
    of one table, plan and segment (in memory only, in stream order) and transforms them once; then adds `txn`.
    Raises `TableFailure` for a table whose values cannot be converted, `transform.WideningNeeded` is turned into
    the table leaving the visible path (C10, P3) here."""
    from cdc_v2 import transform as T
    out = {}
    for key, acc in group.tables.items():
        work = TableWork(key, acc.route, None, [], leave_kind=acc.leave_kind)
        try:
            for i, seg in enumerate(acc.segments):
                rows, plan, amb = _segment_rows(key, seg, transformer, ambiguous_fn)
                work.ambiguous += amb
                work.plan = work.plan or plan
                work.segments.append(Segment(i > 0, rows, plan))
        except T.WideningNeeded:
            work = TableWork(key, acc.route, None, [], leave_kind="widening_needed")
        except (T.ConversionError, T.PlanMismatch) as e:
            raise TableFailure(key, type(e).__name__, str(e)) from None
        out[key] = work
    return out


def _segment_rows(key, seg: list, transformer, ambiguous_fn):
    """[(txn, plan, raw batch)] -> (Arrow table in destination types with `txn`, plan, ambiguous count)."""
    if not seg:
        return None, None, 0
    parts, plan, amb = [], None, 0
    run, run_plan = [], None

    def flush():
        nonlocal amb
        if not run:
            return
        raw = _concat(run)
        txn = _txn_column(run)
        if run_plan.ambiguous:
            mask = ambiguous_fn(run_plan, raw)
            if mask is not None:
                amb += int(pc.sum(mask).as_py() or 0)
        conv = transformer.stream(run_plan, raw)
        parts.append(pa.Table.from_batches([conv]).append_column("txn", txn))

    for txn, p, batch in seg:
        if run and (p is not run_plan or batch.schema != run[0][1].schema):
            flush()
            run = []
        run_plan = p
        plan = p
        run.append((txn, batch))
    flush()
    if len(parts) == 1:
        return parts[0], plan, amb
    return pa.concat_tables(parts, promote_options="default"), plan, amb


def _concat(run) -> pa.RecordBatch:
    """One record batch of the run (Arrow copies the buffers; no value passes through Python)."""
    if len(run) == 1:
        return run[0][1]
    return pa.concat_batches([b for _t, b in run])


def _txn_column(run) -> pa.Array:
    """The `txn` column of a run: the group's transaction index of each row, built by Arrow."""
    parts = [pa.repeat(pa.scalar(t, pa.int64()), b.num_rows) for t, b in run]
    return parts[0] if len(parts) == 1 else pa.concat_arrays(parts)


__all__ = ["ProtocolError", "TableFailure", "already_applied", "Record", "OpenTxn", "EndedTxn", "Assembler",
           "PlanEntry", "TxnInfo", "TableAcc", "Group", "Segment", "TableWork", "build",
           "MESSAGE_COLLECTION_PREFIX"]
