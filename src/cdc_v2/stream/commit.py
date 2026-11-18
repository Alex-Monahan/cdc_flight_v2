"""Commit and acknowledge (design section 3; task S section 5): one destination transaction per group.

The eight steps, each with its place in this module:

| # | Step | Where | Crash point |
|---|---|---|---|
| 1 | the group's records are staged (in memory, transformed; an oversized transaction in `_cdc_stage`) | `commit_group`, first line | `stream.staged` |
| 2 | `BEGIN` on the stream connection | `commit_group`, `lane.transaction` | `stream.begun` |
| 3 | apply every segment of every table, in commit order; then the seams of packages T and B | `_apply` | `stream.applied` |
| 4 | the applied position moves from `prev` to the group's end, with a condition on the value read in this transaction | `cdc_v2.stream.state.move_position` | `stream.position_written` |
| 5 | fence statement of lane `stream` | package D (`md.Lane`) | `stream.fenced` |
| 6 | `COMMIT` | package D | `stream.commit_sent` (run with `kill_later`), `stream.committed` |
| 7 | tell the engine "durable up to the group's end" | `Acknowledger.durable` (the one call of `engine.durable`) | `stream.ack_sent` |
| 8 | the engine marks the newest END at or below it | package E | `engine.acked` |

`commit_group` is one run of the group, from the start: it reads the applied position inside the transaction and
checks that it is the position the group was built on (the run-again rule of C4). `run_group` runs it again after
`Conflict`, `ConnectionLost` or a deadline (at most `apply.max_group_attempts` times), counts a failure of one table
(`apply.max_table_attempts`, then the quarantine exit), and settles `CommitUnknown` by reading the position in a new
transaction after the wait of the lane's document: equal to the group's end, the group is done; equal to the
previous position, the group runs again; anything else is a stop (`PositionDiverged`). `FenceLost` and
`LaneMisuse` are never retried.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from cdc_v2 import alerts, apply as A
from cdc_v2.crashpoints import crash_point, declare
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.md import errors as md_errors
from cdc_v2.md.schema import LATE_COMMIT_WAIT_S
from cdc_v2.stream import state as st
from cdc_v2.stream.group import TableFailure

log = logging.getLogger("cdc_v2.stream")

CP_STAGED = declare("stream.staged", "design 3 step 1: the group's records are staged (in memory, transformed; an "
                    "oversized transaction in _cdc_stage); nothing sent yet")
CP_BEGUN = declare("stream.begun", "design 3 step 2: the group's transaction is open on the stream lane")
CP_APPLIED = declare("stream.applied", "design 3 step 3: every segment of the group is applied, the seams of T and "
                     "B have run; the position is not written yet")
CP_POSITION_WRITTEN = declare("stream.position_written", "design 3 step 4: the applied position is moved in the "
                              "group's transaction; fence and COMMIT follow")
CP_ACK_SENT = declare("stream.ack_sent", "design 3 step 7: DURABLE for the group's end was handed to the engine")


class PositionDiverged(Exception):
    """The applied position is neither the position the group was built on nor the group's end: something else
    moved it. The group is not applied; the stream stops with an alert and starts again from what is stored."""

    def __init__(self, prev, end, found):
        self.prev, self.end, self.found = prev, end, found
        super().__init__("applied position is %s; the group was built on %d and ends at %d" % (found, prev, end))


class _PositionMoved(Exception):
    def __init__(self, found):
        self.found = found
        super().__init__("applied position %s" % (found,))


class GroupFailed(Exception):
    """A group could not be committed within `apply.max_group_attempts` runs (transport errors); the stream
    component starts again."""


# ------------------------------------------------------------------------------------------------ acknowledgement

class Acknowledger:
    """Step 7 has one call site: `durable`. It is called only with a position whose commit the stream saw return
    (`how="committed"`), or that it read back as the stored applied position (`how="read back"`, after the attach
    decision and after `CommitUnknown`). It never sends a position below the last one sent, and never one above the
    last position seen committed or read back."""

    def __init__(self, engine):
        self.engine = engine
        self.seen = None                    # the newest position seen committed or read back
        self.sent = None

    def durable(self, position: int, how: str):
        if how not in ("committed", "read back"):
            raise ValueError("durable is sent only for a position seen committed or read back")
        if self.seen is None or position > self.seen:
            self.seen = position
        if self.sent is not None and position <= self.sent:
            return
        self.engine.durable(position)        # the one call of engine.durable in the pipeline
        self.sent = position
        crash_point(CP_ACK_SENT)


# ------------------------------------------------------------------------------------------------ one group

def apply_meta(plan) -> A.TableMeta:
    """Package A's TableMeta from a transform plan (docs/design/transform.md, contract note 3: `plan.table_meta()`
    is the one adapter)."""
    return A.TableMeta(tuple(A.Column(c.position, c.name, c.type, key=c.key, unchanged_possible=c.unchanged_possible)
                             for c in plan.table_meta().columns))


@dataclass
class GroupJob:
    """Everything one run of a group needs. Built once per group; every run reads the position again."""
    slot: str
    engine_id: str
    run_id: str
    prev: int
    end: int
    kind: str                                   # group | idle | large
    facts: st.GroupFacts
    works: dict                                 # (schema, table) -> group.TableWork (local change sets)
    transactions: object                        # C3a `transactions` (Arrow), or None
    routes: dict                                # routing at the group's start
    large: object = None                        # staging.StagedTxn of an oversized transaction
    quarantine: dict = field(default_factory=dict)    # (schema, table) -> kind: leaves the stream in this group
    confirmed: dict = field(default_factory=dict)     # tables.confirm: (schema, table) -> same | changed
    alerts: list = field(default_factory=list)  # (kind, severity, fields, key) to record in this transaction
    ambiguous: dict = field(default_factory=dict)     # (schema, table) -> rows with an ambiguous zero interval
    first_of_life: bool = False                 # the first group of an engine life (slot and stream -> streaming)
    staged_tables: dict = field(default_factory=dict)  # (schema, table) -> relation change sets (spill fallback)
    spill_stage: object = None                  # staging.Stage of those tables, dropped in the group's transaction
    rows: int = 0

    @property
    def deadline_rows(self) -> int:
        return self.rows


@dataclass
class GroupResult:
    row: st.StreamRow
    changes: dict                               # routing changes, {(schema, table): Route}
    statements: int = 0


def commit_group(lane, job: GroupJob, deadline: Deadline, apply_settings: A.ApplySettings, seams) -> GroupResult:
    """One run of design 3, steps 1 to 6. Raises `_PositionMoved` when the stored position is not `job.prev`."""
    crash_point(CP_STAGED)
    stats = {"statements": 0}
    with lane.transaction(deadline) as tx:
        crash_point(CP_BEGUN)
        row = st.read_row(tx, job.slot)
        if row is None or row.applied_lsn != job.prev:
            raise _PositionMoved(None if row is None else row.applied_lsn)
        changes = _apply(tx, job, apply_settings, seams, stats)
        crash_point(CP_APPLIED)
        st.move_position(tx, row, job.facts, job.engine_id)
        crash_point(CP_POSITION_WRITTEN)
    new_row = st.StreamRow(row.slot_name, "applied", row.slot_state if not job.first_of_life else "streaming",
                           job.end, job.facts.commit_ms if job.facts.commit_ms is not None else row.applied_commit_ms,
                           job.facts.xid if job.facts.xid is not None else row.applied_xid, row.applied_lsn,
                           job.engine_id, row.groups + 1, row.txns + job.facts.txns, row.rows + job.facts.rows, 0,
                           row.attempts_at)
    return GroupResult(new_row, changes, stats["statements"])


def _apply(tx, job: GroupJob, apply_settings, seams, stats) -> dict:
    """Step 3: the quarantines decided before this run, every table's segments in commit order, the oversized
    transaction from staging, the tables that leave the visible path, the counters and alerts; then the seams."""
    routes = dict(job.routes)
    for key, kind in sorted(job.quarantine.items()):
        r = routes.get(key)
        if r is not None and r.routing != "skip":
            routes[key] = st.quarantine(tx, r, kind, job.prev)
    backfill_sets = {}
    for key in sorted(job.works):
        work = job.works[key]
        r = routes.get(key)
        if key in job.quarantine or r is None:
            continue
        if job.confirmed.get(key, "same") != "same":
            continue                                # C10: the catalog gate took the table out of this group
        if r.visible:
            target = A.Target(key[0], key[1])
            staged = job.staged_tables.get(key)
            for i, seg in enumerate(work.segments):
                if seg.truncate_before:
                    A.truncate(tx, target)
                    stats["statements"] += 1
                if staged is not None:
                    cs = staged[i]
                elif seg.rows is not None and seg.rows.num_rows:
                    cs = A.LocalChangeSet(seg.rows, job.transactions, apply_meta(seg.plan))
                else:
                    cs = None
                if cs is not None:
                    stats["statements"] += _apply_one(tx, key, target, cs, apply_settings)
        if r.to_backfill:
            backfill_sets[key] = work
        if work.leave_kind is not None and r.routing != "skip":
            routes[key] = _leave(tx, r, work.leave_kind, job.prev, seams)
    if job.spill_stage is not None:
        stats["statements"] += job.spill_stage.drop(tx)
    if job.large is not None:
        stats["statements"] += job.large.apply(tx, routes, job, apply_settings, seams)
        for key, kind in job.large.left.items():
            r = routes.get(key)
            if r is not None and r.routing != "skip":
                routes[key] = _leave(tx, r, kind, job.prev, seams)
    st.add_interval_ambiguous(tx, job.ambiguous)
    for kind, severity, fields, key in job.alerts:
        alerts.emit(tx, kind, severity, fields, key=key)
    if job.first_of_life:
        st.slot_state(tx, job.slot, "verified", "streaming", "first group of an engine life",
                      detail={"position": job.end})
    seams.tables.on_group(tx, job.end)
    changes = seams.backfill.on_group(tx, job.end, backfill_sets, routes) or {}
    out = {k: v for k, v in routes.items() if job.routes.get(k) is not v}
    out.update(changes)
    return out


def _apply_one(tx, key, target, cs, apply_settings) -> int:
    try:
        counts = A.apply_group(tx, target, cs, mode="stream", settings=apply_settings)
    except A.ApplyInvariantError as e:
        raise TableFailure(key, "apply_invariant", str(e)) from None
    except md_errors.StatementError as e:
        raise TableFailure(key, "refused_%s" % e.error_class.lower()[:40]) from None
    except A.ChangeSetError as e:
        raise TableFailure(key, "change_set", str(e)) from None
    except A.ApplySpillFull as e:
        e.table_key = key                   # not a failure of the table: the caller stages it (task S section 10)
        raise
    return counts.statements


def _leave(tx, route, kind, position, seams):
    """C10: a table whose schema changed (or whose values need their wide form, P3) leaves the visible path in
    this group, with a backfill request after a gap that is enacted at once."""
    rid = st.request_backfill(tx, route.schema, route.table, kind, gap=True, requested_lsn=position)
    alerts.emit(tx, "table_left_stream", "warning", {"schema": route.schema[:128], "table": route.table[:128],
                                                     "cause": kind, "position": position}, key=("schema", "table"))
    return seams.backfill_enact(tx, route, rid, position)


# ------------------------------------------------------------------------------------------------ run again

@dataclass
class RunPolicy:
    max_group_attempts: int
    max_table_attempts: int
    pause: Callable[[int], float]           # attempt -> seconds
    late_commit_wait_s: float = LATE_COMMIT_WAIT_S


class TableFailures:
    """Failures of one table at one applied position (task S section 7), kept by the stream context so that they
    survive a restart of the stream component inside one process."""

    def __init__(self):
        self.counts = {}

    def add(self, key, position) -> int:
        k = (tuple(key), position)
        self.counts[k] = self.counts.get(k, 0) + 1
        return self.counts[k]

    def get(self, key, position) -> int:
        return self.counts.get((tuple(key), position), 0)


def run_group(lane, job: GroupJob, policy: RunPolicy, *, deadline_for: Callable[[int], Deadline],
              apply_settings, seams, ack: Acknowledger, failures: TableFailures, wait: Callable,
              on_spill: Callable, read_position: Callable) -> Optional[GroupResult]:
    """Run one group until it is committed. Returns the result, or None when the group turned out to be committed
    already (a late commit; the position was read back). `wait(seconds, reason)` is a declared wait of the
    supervisor; `on_spill(job, key)` stages a table whose plan did not fit the spill area; `read_position()` reads
    the applied position in a new transaction."""
    attempt = 0
    while True:
        attempt += 1
        try:
            res = commit_group(lane, job, deadline_for(job.rows), apply_settings, seams)
        except TableFailure as e:
            n = failures.add(e.key, job.prev)
            log.warning("table failure", extra={"cdc": {"schema": e.key[0], "table": e.key[1], "kind": e.kind,
                                                        "attempt": n, "position": job.prev}})
            if n >= policy.max_table_attempts:
                job.quarantine[e.key] = e.kind
                job.works.pop(e.key, None)
            attempt -= 1                            # a table failure is counted per table, not per group
            continue
        except A.ApplySpillFull as e:
            on_spill(job, getattr(e, "table_key", None))
            attempt -= 1
            continue
        except _PositionMoved as e:
            if e.found == job.end:
                ack.durable(job.end, "read back")   # a late commit of an earlier run of this group landed
                return None
            raise PositionDiverged(job.prev, job.end, e.found) from None
        except md_errors.CommitUnknown:
            found = _settle(read_position, policy, wait)
            if found == job.end:
                ack.durable(job.end, "read back")
                return None
            if found != job.prev:
                raise PositionDiverged(job.prev, job.end, found) from None
            continue                                # nothing landed: run the group again from the start
        except (md_errors.Conflict, md_errors.ConnectionLost, DeadlineExceeded) as e:
            if attempt >= policy.max_group_attempts:
                raise GroupFailed("group at %d: %d runs, last %s" % (job.prev, attempt, type(e).__name__)) from None
            wait(policy.pause(attempt + 1), "group at %d run again after %s" % (job.prev, type(e).__name__))
            continue
        ack.durable(job.end, "committed")
        return res


def _settle(read_position, policy, wait) -> Optional[int]:
    """After CommitUnknown: wait for a late commit to land, then read the position in a new transaction (destination
    section 8.5). The read is tried again on transport errors, inside the same bounded attempts."""
    wait(policy.late_commit_wait_s, "commit outcome unknown: waiting for a late commit")
    last = None
    for i in range(policy.max_group_attempts):
        try:
            return read_position()
        except (md_errors.Conflict, md_errors.ConnectionLost, DeadlineExceeded) as e:
            last = e
            wait(policy.pause(i + 2), "commit outcome unknown: reading the position again")
    raise GroupFailed("could not read the applied position after an unknown commit outcome (%s)"
                      % type(last).__name__)


__all__ = ["Acknowledger", "GroupJob", "GroupResult", "commit_group", "run_group", "RunPolicy", "TableFailures",
           "PositionDiverged", "GroupFailed", "apply_meta"]
