"""The stream's state in `_cdc` (task S section 3; design sections 2, 3, 9; contracts C8, C10).

Tables (migration list ``stream``):

- `_cdc.stream_state`: one row per slot name. `applied_lsn` is the applied position (design 2). The row is the
  **batch commit and acknowledge machine** (`BATCH`, column `state`) and the **slot and stream machine**
  (`SLOT_STREAM`, column `slot_state`); next to them the commit time and transaction id of the last applied source
  transaction, the last position sent as durable, the engine instance, counters, and the attempt counter of SPEC
  4.3 (`attempts`, keyed by `attempts_at`, the applied position at which the starts were counted).
- `_cdc.stream_tables`: the routing map of contract C10, one row per replicated table name: the source identity
  (schema, name, OID), the hash of the transform plan the destination table was built for, the routing
  (`streaming`, `backfilling` with the flag `apply_visible`, `skip`), and the **table machine** (`TABLE`).
- `_cdc.backfill_requests`: every request for a backfill, whoever asks (`REQUEST`); enacted inside a stream group.
- `_cdc.stream_stage`: the staging progress row of a transaction larger than a group (design 6.4), one per slot.
- schema `_cdc_stage`: the staging tables themselves (one per destination table and plan, made on demand).

`applied_position` is the one reader of the applied position (C8). `move_position` is the batch move of design 3
step 4, `reposition` the only function that moves the position backwards (C10). Every write runs inside a stream
lane transaction (`md.Transaction`); the machines are created on the stream lane only.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Iterable, Optional

from cdc_v2 import alerts, state
from cdc_v2.crashpoints import declare
from cdc_v2.md import errors as md_errors
from cdc_v2.md import schema as md_schema
from cdc_v2.md.connection import Connection, clock_sql
from cdc_v2.md.lane import Transaction

log = logging.getLogger("cdc_v2.stream")

STAGE_SCHEMA = "_cdc_stage"

MIGRATIONS = md_schema.register_migrations("stream", (
    md_schema.Step("stream_state", (
        "CREATE TABLE _cdc.stream_state (slot_name VARCHAR, state VARCHAR, slot_state VARCHAR, "
        "applied_lsn UBIGINT, applied_commit_ms BIGINT, applied_xid UBIGINT, durable_lsn UBIGINT, "
        "engine_id VARCHAR, groups UBIGINT, txns UBIGINT, rows UBIGINT, attempts BIGINT, attempts_at UBIGINT, "
        "created_at TIMESTAMPTZ, moved_at TIMESTAMPTZ)",
    )),
    md_schema.Step("stream_tables", (
        "CREATE TABLE _cdc.stream_tables (schema_name VARCHAR, table_name VARCHAR, oid BIGINT, plan_hash VARCHAR, "
        "state VARCHAR, routing VARCHAR, apply_visible BOOLEAN, backfill_id VARCHAR, reason VARCHAR, "
        "has_key BOOLEAN, interval_ambiguous UBIGINT, created_at TIMESTAMPTZ, changed_at TIMESTAMPTZ)",
    )),
    md_schema.Step("backfill_requests", (
        "CREATE TABLE _cdc.backfill_requests (request_id VARCHAR, schema_name VARCHAR, table_name VARCHAR, "
        "state VARCHAR, gap BOOLEAN, cause VARCHAR, requested_lsn UBIGINT, p0 UBIGINT, epoch BIGINT, "
        "lane VARCHAR, requested_at TIMESTAMPTZ, enacted_at TIMESTAMPTZ)",
    )),
    md_schema.Step("stream_stage", (
        "CREATE TABLE _cdc.stream_stage (slot_name VARCHAR, engine_id VARCHAR, txn_seq BIGINT, chunks BIGINT, "
        "rows UBIGINT, changed_at TIMESTAMPTZ)",
        "CREATE SCHEMA IF NOT EXISTS _cdc_stage",
    )),
))

# ------------------------------------------------------------------------------------------------ the machines

#: Design 9, "batch commit / ack": one row per slot. A group moves the applied position forward (`applied` ->
#: `applied`, with a condition on the position it read); a slot finding or the last line of SPEC 4.3 sets it in
#: any direction (`-> repositioned`). Forward-only is checked by `move_position` before the transition.
BATCH = state.declare(state.Machine(
    name="batch",
    states=("applied", "repositioned"),
    transitions=(("applied", "applied"), ("applied", "repositioned"), ("repositioned", "applied"),
                 ("repositioned", "repositioned")),
    table="_cdc.stream_state", key_columns=("slot_name",), state_column="state", creators=("stream",),
    initial=("applied",),
    description="Batch commit and acknowledge (design 9): the applied position of one slot; moved forward by a "
                "committed group, set by a reposition."))

#: Design 9, "slot and stream". Every start of the stream component begins at `unverified`; the attach decision
#: makes it `verified` (or `broken`, a finding); the first group of an engine life `streaming`; the marker rule of
#: SPEC 4.6 `stalled` until the engine is rebuilt. `broken -> rebuilding -> verified` is package V's enactment.
SLOT_STREAM = state.declare(state.Machine(
    name="slot_stream",
    states=("unverified", "verified", "streaming", "stalled", "broken", "rebuilding"),
    transitions=(("unverified", "unverified"), ("unverified", "verified"), ("unverified", "broken"),
                 ("verified", "streaming"), ("verified", "unverified"), ("streaming", "stalled"),
                 ("streaming", "unverified"), ("stalled", "streaming"), ("stalled", "unverified"),
                 ("broken", "rebuilding"), ("broken", "unverified"), ("rebuilding", "verified"),
                 ("rebuilding", "unverified")),
    table="_cdc.stream_state", key_columns=("slot_name",), state_column="slot_state", creators=("stream",),
    initial=("unverified",),
    description="Slot and stream (design 9): whether the slot our engine holds is verified, streaming, stalled "
                "(SPEC 4.6) or broken (a finding of design 8)."))

#: Design 9, "table replication", with the states the stream enacts in P3. Package B extends this declaration
#: (it is the one place the machine is declared).
TABLE = state.declare(state.Machine(
    name="table",
    states=("streaming", "backfilling", "quarantined"),
    transitions=(("streaming", "backfilling"), ("backfilling", "streaming"), ("streaming", "quarantined"),
                 ("backfilling", "quarantined"), ("quarantined", "backfilling")),
    table="_cdc.stream_tables", key_columns=("schema_name", "table_name"), state_column="state",
    creators=("stream",), initial=("streaming", "backfilling"),
    description="Table replication (design 9): how one replicated table name is fed; its routing (C10) is "
                "changed in the same stream-lane transaction."))

#: A backfill request (C10): written by whoever asks, in any lane; enacted inside a stream group (package B). The
#: stream's stand-in leaves it pending.
REQUEST = state.declare(state.Machine(
    name="backfill_request",
    states=("pending", "enacted"),
    transitions=(("pending", "enacted"),),
    table="_cdc.backfill_requests", key_columns=("request_id",), state_column="state",
    creators=state.FRESH_IDS, initial=("pending",),
    description="A request for a backfill of one table (C10); enacted inside a stream-lane group, P0 = its end."))

# ------------------------------------------------------------------------------------------------ crash points

CP_SLOT_CREATED = declare("stream.slot_created", "first start: the slot was created, its row not written yet "
                          "(reached only at the very first start of a slot)", once=True)
CP_FIRST_ROW = declare("stream.first_row", "first start: the row of the slot with its consistent point is "
                       "committed (reached only at the very first start of a slot)", once=True)
CP_STARTED = declare("stream.started", "start of the stream component committed: staging truncated, progress "
                     "row reset, attempt counter raised")
CP_REPOSITIONED = declare("stream.repositioned", "a reposition is committed (reached only on a slot finding or "
                          "the last line of SPEC 4.3)")
CP_QUARANTINED = declare("stream.quarantined", "a table left the stream: quarantine committed (reached only when "
                         "a table fails max_table_attempts times or names itself in a FATAL)")


# ------------------------------------------------------------------------------------------------ values

class StreamStateError(Exception):
    """The stream's state is not what a step requires (no row for the slot, a position that would move
    backwards outside `reposition`). The message holds names and positions only."""


@dataclass(frozen=True)
class StreamRow:
    slot_name: str
    state: str
    slot_state: str
    applied_lsn: int
    applied_commit_ms: Optional[int]
    applied_xid: Optional[int]
    durable_lsn: Optional[int]
    engine_id: Optional[str]
    groups: int
    txns: int
    rows: int
    attempts: int
    attempts_at: Optional[int]

    @property
    def attempts_here(self) -> int:
        """The starts counted at the current applied position (SPEC 4.3)."""
        return self.attempts if self.attempts_at == self.applied_lsn else 0


_ROW_SQL = ("SELECT slot_name, state, slot_state, applied_lsn, applied_commit_ms, applied_xid, durable_lsn, "
            "engine_id, groups, txns, rows, attempts, attempts_at FROM _cdc.stream_state WHERE slot_name = ?")


def _int(v):
    return None if v is None else int(v)


def _row(r) -> Optional[StreamRow]:
    if r is None:
        return None
    return StreamRow(r[0], r[1], r[2], int(r[3]), _int(r[4]), _int(r[5]), _int(r[6]), r[7], int(r[8] or 0),
                     int(r[9] or 0), int(r[10] or 0), int(r[11] or 0), _int(r[12]))


def _fetchone(conn, sql, params):
    """One row from an md.Transaction, an md.Connection, or a plain DuckDB connection (the oracle's)."""
    if isinstance(conn, (Transaction, Connection)):
        return conn.fetchone(sql, params)
    return conn.execute(sql, params).fetchone()


def _fetchall(conn, sql, params=None):
    if isinstance(conn, (Transaction, Connection)):
        return conn.fetchall(sql, params)
    return conn.execute(sql, params or []).fetchall()


def _missing_table(e) -> bool:
    if isinstance(e, md_errors.StatementError):
        return e.error_class == "CatalogException"
    return type(e).__name__ == "CatalogException"


def applied_position(conn_or_tx, slot_name: str) -> Optional[int]:
    """The applied position of `slot_name` (contract C8), or None when the slot has no row yet (or the stream's
    tables do not exist yet). The one reader of `_cdc.stream_state.applied_lsn`: the oracle and every package read
    the position through it. Takes an `md.Transaction`, an `md.Connection` or a plain DuckDB connection."""
    try:
        r = _fetchone(conn_or_tx, "SELECT applied_lsn FROM _cdc.stream_state WHERE slot_name = ?", [slot_name])
    except Exception as e:                                    # noqa: BLE001 - a missing table is "no row yet"
        if _missing_table(e):
            return None
        raise
    return None if r is None or r[0] is None else int(r[0])


def read_row(conn_or_tx, slot_name: str) -> Optional[StreamRow]:
    """The whole row of `slot_name`, or None."""
    return _row(_fetchone(conn_or_tx, _ROW_SQL, [slot_name]))


# ------------------------------------------------------------------------------------------------ first start

def first_row(tx, slot_name: str, consistent_point: int, cause: str) -> StreamRow:
    """Create the row of a slot at its consistent point (or, after a crash between "create the slot" and this
    step, at the slot's `confirmed_flush_lsn`). The order of the first start (task S section 3):

    1. create the slot (crash point `stream.slot_created`);
    2. this transaction inserts the row and the staging progress row (crash point `stream.first_row`).

    A crash between 1 and 2 leaves a slot and no row. The next start finds the slot, takes its
    `confirmed_flush_lsn` (still its consistent point: no engine confirms anything before the row exists, because
    the attach step reads the row first) and goes on with 2. No table can be `streaming` before the row exists
    (`register_table` refuses), so nothing is lost before that position."""
    _check_position(consistent_point)
    state.create(tx, BATCH, slot_name, "applied", cause, values={
        "slot_state": "unverified", "applied_lsn": consistent_point, "applied_commit_ms": None, "applied_xid": None,
        "durable_lsn": None, "engine_id": None, "groups": 0, "txns": 0, "rows": 0, "attempts": 0,
        "attempts_at": consistent_point, "created_at": state.NOW, "moved_at": state.NOW},
        detail={"applied_lsn": consistent_point})
    n = tx.execute("INSERT INTO _cdc.stream_stage (slot_name, engine_id, txn_seq, chunks, rows, changed_at) "
                   "SELECT ?, NULL, NULL, 0, 0, " + clock_sql + " WHERE NOT EXISTS (SELECT 1 FROM _cdc.stream_stage "
                   "WHERE slot_name = ?)", [slot_name, slot_name])
    if n not in (0, 1):
        raise tx.fail(StreamStateError("stream_stage: %r rows inserted" % (n,)))
    return read_row(tx, slot_name)


def _check_position(p):
    if isinstance(p, bool) or not isinstance(p, int) or not 0 <= p < (1 << 64):
        raise StreamStateError("not a position: %r" % (p,))


# ------------------------------------------------------------------------------------------------ every start

def stage_tables(tx) -> list:
    """The staging tables that exist now, by name."""
    return [r[0] for r in tx.fetchall(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = ? AND database_name = current_database() "
        "ORDER BY table_name", [STAGE_SCHEMA])]


def start(tx, slot_name: str, engine_id: str) -> StreamRow:
    """Every start of the stream component, before its engine starts (task S sections 6 and 7): drop the staging
    tables, reset the staging progress row to the new engine instance, raise the attempt counter at the applied
    position, and set the slot and stream machine to `unverified`. Returns the row as it will be after the commit.
    Crash point `stream.started` after the commit (the caller's)."""
    row = read_row(tx, slot_name)
    if row is None:
        raise tx.fail(StreamStateError("slot %s has no row in _cdc.stream_state" % slot_name))
    for name in stage_tables(tx):
        tx.execute("DROP TABLE %s.%s" % (STAGE_SCHEMA, _quote(name)))
    tx.execute_one("UPDATE _cdc.stream_stage SET engine_id = ?, txn_seq = NULL, chunks = 0, rows = 0, "
                   "changed_at = " + clock_sql + " WHERE slot_name = ?", [engine_id, slot_name])
    attempts = row.attempts_here + 1
    state.transition(tx, SLOT_STREAM, slot_name, row.slot_state, "unverified", "stream component start",
                     values={"engine_id": engine_id, "attempts": attempts, "attempts_at": row.applied_lsn},
                     detail={"applied_lsn": row.applied_lsn, "attempts": attempts})
    return StreamRow(row.slot_name, row.state, "unverified", row.applied_lsn, row.applied_commit_ms, row.applied_xid,
                     row.durable_lsn, engine_id, row.groups, row.txns, row.rows, attempts, row.applied_lsn)


def slot_state(tx, slot_name: str, from_state: str, to_state: str, cause: str, detail=None, engine_id=None):
    """A transition of the slot and stream machine (attach decision, first group, marker rule)."""
    state.transition(tx, SLOT_STREAM, slot_name, from_state, to_state, cause, detail=detail,
                     values={"engine_id": engine_id} if from_state == to_state else None)


# ------------------------------------------------------------------------------------------------ the batch move

@dataclass(frozen=True)
class GroupFacts:
    """What the row of `_cdc.transitions` of a committed group records (task S section 10): previous and new
    applied position, the first and last commit position of its source transactions and their count, the rows,
    the run. Epoch, lane and the order of commit (`seq`) are columns of that row."""
    prev: int
    new: int
    first_commit: Optional[int]
    last_commit: Optional[int]
    txns: int
    rows: int
    commit_ms: Optional[int]
    xid: Optional[int]
    run_id: str = ""
    kind: str = "group"                  # group | idle | large

    def detail(self) -> dict:
        d = {"prev": self.prev, "new": self.new, "txns": self.txns, "rows": self.rows, "kind": self.kind}
        if self.first_commit is not None:
            d["first_commit"] = self.first_commit
        if self.last_commit is not None:
            d["last_commit"] = self.last_commit
        if self.run_id:
            d["run_id"] = self.run_id
        return d


def move_position(tx, row: StreamRow, facts: GroupFacts, engine_id: str):
    """Design 3 step 4: the position moves from `facts.prev` to `facts.new` in the group's own transaction, with
    the facts of the group, as one conditional UPDATE (`expect` on the position read in this transaction; a count
    of 0 is `Conflict`) and one INSERT into `_cdc.transitions`. `row` is the row read in this transaction. The
    position only moves forward here; `reposition` is the other way."""
    if row.applied_lsn != facts.prev:
        raise tx.fail(StreamStateError("the group was built on %d, the row holds %d" % (facts.prev, row.applied_lsn)))
    if not facts.new > facts.prev:
        raise tx.fail(StreamStateError("a group moves the applied position forward only (%d -> %d)"
                                       % (facts.prev, facts.new)))
    values = {"applied_lsn": facts.new, "durable_lsn": row.applied_lsn, "engine_id": engine_id,
              "groups": row.groups + 1, "txns": row.txns + facts.txns, "rows": row.rows + facts.rows,
              "moved_at": state.NOW}
    if facts.commit_ms is not None:
        values["applied_commit_ms"] = facts.commit_ms
    if facts.xid is not None:
        values["applied_xid"] = facts.xid
    if row.attempts:
        values["attempts"] = 0          # SPEC 4.3: the first group that moves the position resets the counter
    state.transition(tx, BATCH, row.slot_name, row.state, "applied", facts.kind,
                     values=values, expect={"applied_lsn": facts.prev}, detail=facts.detail())


# ------------------------------------------------------------------------------------------------ routing map

ROUTINGS = ("streaming", "backfilling", "skip")


@dataclass(frozen=True)
class Route:
    """One row of the routing map (C10)."""
    schema: str
    table: str
    oid: Optional[int]
    plan_hash: Optional[str]
    state: str
    routing: str                        # streaming | backfilling | skip
    apply_visible: bool                 # backfilling without a gap: also applied to the visible table
    backfill_id: Optional[str]
    reason: Optional[str]
    has_key: Optional[bool]

    @property
    def key(self):
        return (self.schema, self.table)

    @property
    def visible(self) -> bool:
        """Changes of this table are applied to its visible table."""
        return self.routing == "streaming" or (self.routing == "backfilling" and self.apply_visible)

    @property
    def to_backfill(self) -> bool:
        """Changes of this table are handed to its backfill (pending log)."""
        return self.routing == "backfilling"


_ROUTES_SQL = ("SELECT schema_name, table_name, oid, plan_hash, state, routing, apply_visible, backfill_id, reason, "
               "has_key FROM _cdc.stream_tables")


def load_routing(conn_or_tx) -> dict:
    """The routing map: {(schema, table): Route}. Empty when the table does not exist yet."""
    try:
        rows = _fetchall(conn_or_tx, _ROUTES_SQL)
    except Exception as e:                                     # noqa: BLE001
        if _missing_table(e):
            return {}
        raise
    return {(r[0], r[1]): Route(r[0], r[1], _int(r[2]), r[3], r[4], r[5], bool(r[6]), r[7], r[8],
                                None if r[9] is None else bool(r[9])) for r in rows}


def _quote(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


def target_sql(schema: str, table: str) -> str:
    return "%s.%s" % (_quote(schema), _quote(table))


def not_streamable(source_table, plan) -> Optional[str]:
    """Why a table is not streamed in P3 (C10), as a short identifier for its alert, or None."""
    if getattr(source_table, "name_problem", None):
        return "name_problem"
    if getattr(source_table, "replica_identity_problem", None):
        return "identity_problem"
    if plan is not None and plan.ambiguous_key:
        return "ambiguous_key"
    if plan is not None and not plan.has_key and plan.ambiguous:
        return "keyless_ambiguous_interval"
    if source_table.schema.lower().startswith("_cdc"):
        return "reserved_schema"
    return None


def register_table(tx, source_table, plan, *, create=True) -> Route:
    """Make a table replicated: create its destination schema and table from the transform plan (C0: the same
    schema and table name, quoted) and its row in the routing map, in the stream lane transaction `tx`. A table
    that cannot be streamed (C10) gets routing `skip` and one alert that names the reason. The stream row of the
    slot must exist (no table is `streaming` before it; first start, task S section 3). Until package B exists this
    is for tables that were created empty before the slot (task S section 9)."""
    if not tx.fetchall("SELECT 1 FROM _cdc.stream_state LIMIT 1"):
        raise tx.fail(StreamStateError("no slot row in _cdc.stream_state: a table cannot stream before it"))
    why = not_streamable(source_table, plan)
    if create:
        tx.execute("CREATE SCHEMA IF NOT EXISTS %s" % _quote(source_table.schema))
        tx.execute("CREATE TABLE IF NOT EXISTS %s (%s)" % (target_sql(source_table.schema, source_table.name),
                                                          plan.column_ddl()))
    routing = "skip" if why else "streaming"
    state.create(tx, TABLE, (source_table.schema, source_table.name), "streaming", "registered", values={
        "oid": source_table.oid, "plan_hash": plan.hash, "routing": routing, "apply_visible": False,
        "backfill_id": None, "reason": why, "has_key": bool(plan.has_key), "interval_ambiguous": 0,
        "created_at": state.NOW, "changed_at": state.NOW})
    if why:
        alerts.emit(tx, "table_not_streamed", "warning", {"schema": source_table.schema[:128],
                                                           "table": source_table.name[:128], "reason": why},
                    key=("schema", "table"))
    return Route(source_table.schema, source_table.name, source_table.oid, plan.hash, "streaming", routing, False,
                 None, why, bool(plan.has_key))


def request_backfill(tx, schema: str, table: str, cause: str, *, gap: bool, requested_lsn: Optional[int] = None) -> str:
    """A backfill request (C10): a row in `_cdc.backfill_requests`, in any lane. Returns its id."""
    import uuid
    rid = uuid.uuid4().hex
    state.create(tx, REQUEST, rid, "pending", cause[:200], values={
        "schema_name": schema, "table_name": table, "gap": bool(gap), "cause": cause[:200],
        "requested_lsn": requested_lsn, "p0": None, "epoch": tx.epoch, "lane": tx.lane,
        "requested_at": state.NOW, "enacted_at": None},
        detail={"gap": 1 if gap else 0})
    return rid


def pending_requests(conn_or_tx) -> list:
    """Pending requests: [(request_id, schema, table, gap, cause)] in the order they were made."""
    return [(r[0], r[1], r[2], bool(r[3]), r[4]) for r in _fetchall(
        conn_or_tx, "SELECT request_id, schema_name, table_name, gap, cause FROM _cdc.backfill_requests "
        "WHERE state = 'pending' ORDER BY requested_at, request_id")]


def set_routing(tx, route: Route, to_state: str, routing: str, cause: str, *, apply_visible=False, backfill_id=None,
                reason=None, detail=None) -> Route:
    """Change the table machine and the routing of one table in the stream lane transaction `tx`. A move to the
    same state still changes the routing column; the machine refuses a self-transition, so the routing alone is
    then changed with a condition on its old value."""
    vals = {"routing": routing, "apply_visible": bool(apply_visible), "backfill_id": backfill_id,
            "changed_at": state.NOW}
    if reason is not None:
        vals["reason"] = reason
    if route.state != to_state:
        state.transition(tx, TABLE, route.key, route.state, to_state, cause, values=vals, detail=detail)
    elif route.routing != routing:
        tx.execute_one("UPDATE _cdc.stream_tables SET routing = ?, apply_visible = ?, backfill_id = ?, "
                       "reason = coalesce(?, reason), changed_at = " + clock_sql + " WHERE schema_name = ? AND "
                       "table_name = ? AND routing = ?", [routing, bool(apply_visible), backfill_id, reason,
                                                           route.schema, route.table, route.routing])
    return Route(route.schema, route.table, route.oid, route.plan_hash, to_state, routing, bool(apply_visible),
                 backfill_id, reason if reason is not None else route.reason, route.has_key)


def quarantine(tx, route: Route, kind: str, position: int, *, attempts: int = 0) -> Route:
    """The quarantine exit (design 3, C10): the table leaves the stream (state `quarantined`, routing `skip`), a
    backfill request after a gap is recorded, and an alert, all in `tx`. The stored rows and the slow retries of
    the full quarantine (SPEC 4.0) are P4. Crash point `stream.quarantined` after the commit (the caller's)."""
    new = set_routing(tx, route, "quarantined", "skip", "quarantine: %s" % kind, reason=kind,
                      detail={"position": position, "attempts": attempts})
    request_backfill(tx, route.schema, route.table, "quarantine: %s" % kind, gap=True, requested_lsn=position)
    alerts.emit(tx, "table_quarantined", "critical", {"schema": route.schema[:128], "table": route.table[:128],
                                                      "cause": kind, "position": position, "attempts": attempts},
                key=("schema", "table"))
    return new


def add_interval_ambiguous(tx, counts: dict):
    """Count the rows whose zero interval is ambiguous (plan C3), per table, in the group's transaction."""
    for (schema, table), n in sorted(counts.items()):
        if n:
            tx.execute("UPDATE _cdc.stream_tables SET interval_ambiguous = interval_ambiguous + ? "
                       "WHERE schema_name = ? AND table_name = ?", [int(n), schema, table])


# ------------------------------------------------------------------------------------------------ reposition

def reposition(tx, position: int, tables: Iterable, cause: str, *, slot: Optional[str] = None,
               enact=None) -> StreamRow:
    """Set the applied position to `position` (in any direction), request a backfill after a gap of every table
    in `tables` ((schema, table) pairs) with P0 = `position`, and alert, all in the stream lane transaction `tx`
    (contract C10; design 8). The only function that moves the applied position backwards. The position is
    written with a condition on the value read in `tx`, so a late commit of an earlier copy never moves it back;
    a reposition to the same position relies on nothing. `enact(tx, route, request_id, position)` enacts each
    request (package B; the stream's stand-in by default). Every reposition must be followed by a new engine (C10:
    an engine keeps the highest DURABLE of its life). Crash point `stream.repositioned` after the commit."""
    _check_position(position)
    if slot is None:
        rows = tx.fetchall("SELECT slot_name FROM _cdc.stream_state")
        if len(rows) != 1:
            raise tx.fail(StreamStateError("reposition without a slot name needs exactly one slot row"))
        slot = rows[0][0]
    row = read_row(tx, slot)
    if row is None:
        raise tx.fail(StreamStateError("slot %s has no row" % slot))
    tables = sorted(set(tuple(t) for t in tables))
    state.transition(tx, BATCH, slot, row.state, "repositioned", cause[:200],
                     values={"applied_lsn": position, "moved_at": state.NOW, "attempts": 0,
                             "attempts_at": position},
                     expect={"applied_lsn": row.applied_lsn},
                     detail={"prev": row.applied_lsn, "new": position, "tables": len(tables)})
    routes = load_routing(tx)
    if enact is None:
        from cdc_v2.stream import seams
        enact = seams.current().backfill_enact
    for key in tables:
        rid = request_backfill(tx, key[0], key[1], "reposition: %s" % cause, gap=True, requested_lsn=position)
        r = routes.get(key)
        if r is not None:
            enact(tx, r, rid, position)
    alerts.emit(tx, "stream_repositioned", "critical", {"slot": slot, "from_position": row.applied_lsn,
                                                        "to_position": position, "tables": len(tables),
                                                        "cause": cause[:128]}, key=("slot",))
    return read_row(tx, slot)


def stage_table_name(schema: str, table: str, plan_hash: str) -> str:
    """The staging table of one destination table and plan: a fixed-length name that holds no source name."""
    return "t_" + hashlib.sha256(json.dumps([schema, table, plan_hash]).encode()).hexdigest()[:24]


__all__ = ["BATCH", "SLOT_STREAM", "TABLE", "REQUEST", "MIGRATIONS", "STAGE_SCHEMA", "StreamStateError", "StreamRow",
           "applied_position", "read_row", "first_row", "start", "slot_state", "GroupFacts", "move_position", "Route",
           "load_routing", "register_table", "request_backfill", "pending_requests", "set_routing", "quarantine",
           "add_interval_ambiguous", "reposition", "not_streamable", "target_sql", "stage_table_name"]
