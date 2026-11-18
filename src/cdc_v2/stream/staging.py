"""Staging in `_cdc_stage` (design 6.1 and 6.4; task S section 6).

Two users:

- **a transaction larger than a group** (`StagedTxn`): its records go to staging in chunks while they arrive, so
  memory and local disk stay bounded. At its END one destination transaction applies everything through package
  A's relation form (C3a), drops the staging tables, resets the progress row, moves the position, fences and
  commits (`commit.commit_group` with `job.large`). Nothing of an unfinished source transaction is ever visible;
  the applied position does not move before that commit, and no later transaction overtakes it.
- **a table whose plan did not fit the spill area** (`ApplySpillFull`, task S section 10): its change sets of one
  group are staged the same way and applied in the relation form in the group's transaction.

Every chunk is a stream-lane transaction (fenced by package D) that moves the staging progress row
`_cdc.stream_stage` from (engine instance, source transaction, k chunks) to k + 1 with `execute_one`, so a chunk
whose commit outcome was unknown is never added twice, and a deposed run meets the fence at its next chunk. Every
start of the stream component drops the staging tables and resets the row (`stream.state.start`). Crash point
`stream.chunk_committed` after each chunk's commit.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc

from cdc_v2 import apply as A
from cdc_v2.crashpoints import crash_point, declare
from cdc_v2.deadline import DeadlineExceeded
from cdc_v2.md import errors as md_errors
from cdc_v2.md.connection import clock_sql
from cdc_v2.stream import state as st

log = logging.getLogger("cdc_v2.stream")

CP_CHUNK = declare("stream.chunk_committed", "a staging chunk is committed (reached only for a transaction larger "
                   "than a group, or a table staged after ApplySpillFull)")

_REG = "cdc_v2_stage_chunk"


class StageLost(Exception):
    """The staging progress row is not what this stream wrote: another writer moved it (a deposed run cannot, the
    fence stops it), or a start reset it. The stream component starts again; the slot sends the transaction again."""


@dataclass
class StageTable:
    name: str
    plan: object
    columns: tuple                       # change-set columns in the staging table, after seg/commit_lsn/xid/commit_ms
    rows_per_seg: dict = field(default_factory=dict)   # global segment -> rows staged


def _dest_type(plan, col: str) -> str:
    if col == "seq":
        return "BIGINT"
    if col == "op":
        return "VARCHAR"
    if col[0] == "u":
        return "BOOLEAN"
    return plan.columns[int(col[1:])].dest_type


def _columns_for(plan) -> tuple:
    """Every column a change set of this plan can carry (C2 ROWS sets): seq, op, a<i>, then b<i> (no key) or k<i>
    (key columns), u<i> (unchanged possible)."""
    cols = ["seq", "op"] + ["a%d" % c.position for c in plan.columns]
    if plan.has_key:
        cols += ["k%d" % p for p in plan.key_positions]
    else:
        cols += ["b%d" % c.position for c in plan.columns]
    cols += ["u%d" % c.position for c in plan.columns if c.unchanged_possible]
    return tuple(cols)


def _q(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


class Stage:
    """Rows of one source transaction (or of one group) in `_cdc_stage`, and the progress row that counts them."""

    def __init__(self, slot: str, engine_id: str, txn_seq: int):
        self.slot = slot
        self.engine_id = engine_id
        self.txn_seq = txn_seq
        self.chunks = 0                  # chunks committed (as the progress row says)
        self.rows = 0
        self.tables: dict = {}           # (schema, table) -> StageTable

    # -------------------------------------------------------------------------------------------- writing

    def write_chunk(self, lane, parts: list, deadline, wait, attempts: int, pause):
        """Write one chunk: `parts` are (key, plan, global segment, Arrow table of change-set rows, optional
        per-row commit_lsn/xid/commit_ms arrays). One stream-lane transaction, run again after `Conflict`,
        `ConnectionLost` or a deadline, and after `CommitUnknown` once the progress row has been read."""
        for i in range(1, attempts + 1):
            try:
                self._chunk_tx(lane, parts, deadline)
                break
            except md_errors.CommitUnknown:
                wait(2.5, "staging chunk: commit outcome unknown")
                if self._landed(lane, deadline):
                    break
            except (md_errors.Conflict, md_errors.ConnectionLost, DeadlineExceeded) as e:
                if i == attempts:
                    raise
                wait(pause(i + 1), "staging chunk run again after %s" % type(e).__name__)
        else:                                                        # pragma: no cover - the loop breaks or raises
            raise StageLost("staging chunk not written")
        self.chunks += 1
        for key, plan, seg, table, _extra in parts:
            t = self.tables[key]
            t.rows_per_seg[seg] = t.rows_per_seg.get(seg, 0) + table.num_rows
            self.rows += table.num_rows
        crash_point(CP_CHUNK)

    def _progress(self, tx):
        return tx.fetchone("SELECT engine_id, txn_seq, chunks FROM _cdc.stream_stage WHERE slot_name = ?",
                           [self.slot])

    def _landed(self, lane, deadline) -> bool:
        """After CommitUnknown: did the chunk land? Read in a new transaction (the run-again rule of C4)."""
        row = lane.conn.fetchone("SELECT engine_id, txn_seq, chunks FROM _cdc.stream_stage WHERE slot_name = ?",
                                 [self.slot])
        return self._state(row) == "landed"

    def _state(self, row) -> str:
        if row is None or row[0] != self.engine_id:
            raise StageLost("the staging progress row belongs to another engine instance")
        chunks, seq = int(row[2]), row[1]
        if chunks == self.chunks and (seq == self.txn_seq or (chunks == 0 and seq is None)):
            return "expected"
        if chunks == self.chunks + 1 and seq == self.txn_seq:
            return "landed"
        raise StageLost("the staging progress row holds %d chunks, this stream wrote %d" % (chunks, self.chunks))

    def _chunk_tx(self, lane, parts, deadline):
        with lane.transaction(deadline) as tx:
            if self._state(self._progress(tx)) == "landed":
                return                                                # an earlier run of this chunk committed late
            n = 0
            for key, plan, seg, table, extra in parts:
                t = self._table(tx, key, plan)
                n += self._insert(tx, t, seg, table, extra)
            tx.execute_one("UPDATE _cdc.stream_stage SET txn_seq = ?, chunks = chunks + 1, rows = rows + ?, "
                           "changed_at = " + clock_sql + " WHERE slot_name = ? AND engine_id = ? AND chunks = ?",
                           [self.txn_seq, n, self.slot, self.engine_id, self.chunks])

    def _table(self, tx, key, plan) -> StageTable:
        t = self.tables.get(key)
        if t is None:
            name = st.stage_table_name(key[0], key[1], plan.hash)
            cols = _columns_for(plan)
            t = self.tables[key] = StageTable(name, plan, cols)
        ddl = ", ".join(["seg BIGINT", "commit_lsn UBIGINT", "xid UBIGINT", "commit_ms BIGINT"]
                        + ["%s %s" % (c, _dest_type(plan, c)) for c in t.columns])
        tx.execute("CREATE TABLE IF NOT EXISTS %s.%s (%s)" % (st.STAGE_SCHEMA, _q(t.name), ddl))
        return t

    def _insert(self, tx, t: StageTable, seg: int, table: pa.Table, extra) -> int:
        if table.num_rows == 0:
            return 0
        cols = [c for c in t.columns if c in table.column_names]
        data = table.select(cols)
        names = list(cols)
        if extra is not None:
            for name, arr in zip(("commit_lsn", "xid", "commit_ms"), extra):
                data = data.append_column(name, arr)
                names.append(name)
        tx.register(_REG, data)
        n = tx.execute("INSERT INTO %s.%s (seg, %s) SELECT ?, %s FROM %s" % (
            st.STAGE_SCHEMA, _q(t.name), ", ".join(names), ", ".join(names), _REG), [seg])
        if n != table.num_rows:
            raise tx.fail(StageLost("staging insert wrote %r of %d rows" % (n, table.num_rows)))
        return n

    # -------------------------------------------------------------------------------------------- reading

    def change_set(self, key, seg: int, commit_lsn: Optional[int], xid: Optional[int],
                   commit_ms: Optional[int]) -> Optional[A.RelationChangeSet]:
        """The relation form (C3a) of one table's segment: order (commit_lsn, seq). For an oversized transaction the
        rows carry no commit position yet; it is the END's, given here."""
        t = self.tables.get(key)
        if t is None or not t.rows_per_seg.get(seg):
            return None
        sql = ("SELECT %s, coalesce(commit_lsn, CAST(? AS UBIGINT)) AS commit_lsn, coalesce(xid, CAST(? AS UBIGINT)) "
               "AS xid, make_timestamp(coalesce(commit_ms, CAST(? AS BIGINT)) * 1000) AS commit_ts FROM %s.%s "
               "WHERE seg = ?" % (", ".join(t.columns), st.STAGE_SCHEMA, _q(t.name)))
        from cdc_v2.stream.commit import apply_meta
        u_present = [int(c[1:]) for c in t.columns if c[0] == "u"]
        return A.RelationChangeSet(sql, (commit_lsn or 0, xid or 0, commit_ms or 0, seg), apply_meta(t.plan),
                                   u_present=u_present)

    def drop(self, tx) -> int:
        """In the applying transaction: drop the staging tables and reset the progress row (k chunks -> 0)."""
        for t in self.tables.values():
            tx.execute("DROP TABLE IF EXISTS %s.%s" % (st.STAGE_SCHEMA, _q(t.name)))
        tx.execute_one("UPDATE _cdc.stream_stage SET txn_seq = NULL, chunks = 0, rows = 0, changed_at = " + clock_sql
                       + " WHERE slot_name = ? AND engine_id = ? AND chunks = ?",
                       [self.slot, self.engine_id, self.chunks])
        return len(self.tables) + 1


class StagedTxn(Stage):
    """A transaction larger than a group (design 6.4). Its records are handed over in chunks; per table the
    segments are numbered across chunks (a TRUNCATE of the table starts the next one)."""

    def __init__(self, slot, engine_id, txn_seq):
        super().__init__(slot, engine_id, txn_seq)
        self.segs: dict = {}            # (schema, table) -> current global segment
        self.left: dict = {}            # (schema, table) -> why the table leaves the visible path (C10)
        self.failures: dict = {}        # (schema, table) -> kind (quarantine)
        self.ambiguous: dict = {}
        self.end = None                 # the END frame, once it arrived

    def apply(self, tx, routes: dict, job, apply_settings, seams) -> int:
        """In the group's transaction: every staged table, segment by segment (a TRUNCATE between), through the
        relation form; then drop the staging tables and reset the progress row."""
        from cdc_v2.stream.commit import _apply_one
        n = 0
        end = self.end
        xid = end.positions.get("txId") if end.positions else None
        for key in sorted(set(self.tables) | set(self.segs)):
            r = routes.get(key)
            if r is None or not r.visible or key in self.left or key in job.quarantine:
                continue
            if job.confirmed.get(key, "same") != "same":
                continue
            target = A.Target(key[0], key[1])
            for seg in range(self.segs.get(key, 0) + 1):
                if seg > 0:
                    A.truncate(tx, target)
                    n += 1
                cs = self.change_set(key, seg, end.ack_position, xid if isinstance(xid, int) else None,
                                     end.commit_time_ms)
                if cs is not None:
                    n += _apply_one(tx, key, target, cs, apply_settings)
        return n + self.drop(tx)


def chunk_parts_from_works(works: dict, segs: dict, transactions: Optional[pa.Table] = None) -> list:
    """(key, plan, global segment, rows, extra) of a set of `group.TableWork` (the segments of one chunk or one
    group), with the global segment numbers continuing `segs` ((schema, table) -> current segment; updated)."""
    parts = []
    for key, work in sorted(works.items()):
        base = segs.get(key, 0)
        for i, seg in enumerate(work.segments):
            g = base + i
            if seg.rows is not None and seg.rows.num_rows:
                rows = seg.rows
                extra = None
                if transactions is not None:
                    idx = rows.column("txn")
                    extra = tuple(pc.take(transactions.column(c), idx) for c in ("commit_lsn", "xid"))
                    ms = pc.take(transactions.column("commit_ts"), idx).cast(pa.int64())
                    extra = extra + (pc.divide(ms, 1000),)
                parts.append((key, seg.plan or work.plan, g, rows.drop_columns(["txn"]), extra))
        segs[key] = base + len(work.segments) - 1
    return parts


__all__ = ["Stage", "StagedTxn", "StageLost", "StageTable", "CP_CHUNK", "chunk_parts_from_works"]
