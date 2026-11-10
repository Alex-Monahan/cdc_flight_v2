"""The source-side heartbeat (SPEC 4.4, 4.6, R8) and the standby lag read (H-044).

Decision: the heartbeat is an UPDATE of one row of a small table that is **in our publication**
(`cdc_v2.heartbeat`, one row per slot name). Why a table row and not a logical message:

- The row travels as an ordinary change of a published table, so the engine frames it like any other transaction:
  BEGIN, ROWS, END with an `ack_position` that may be acknowledged. An idle slot advances, and a slot behind a busy
  but unpublished workload advances too (package E measured that Debezium's own heartbeat records do not).
- The stream recognises its own write by the row's `marker` (a value unique to the write) and by the transaction
  id, which `beat` returns together with a bracket around the commit position on the primary. A black-holed
  replication connection is noticed because a marker that was written does not come back (SPEC 4.6).
- A logical message would also be framed (`MESSAGE`), but it needs `pg_logical_emit_message` on the primary and
  Debezium's message prefix configuration, and transactional messages have had restart defects in Debezium
  (docs/research/debezium.md §2.6). The runtime role needs no table privilege beyond UPDATE on this one table.

The write is idempotent and harmless when two runs do it: each sets the row of its slot to a new marker; the
later write wins and each run recognises only its own marker. `lock_timeout` bounds a wait on the row.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from cdc_v2 import lsn as lsnmod
from cdc_v2.deadline import Deadline
from cdc_v2.pg import errors
from cdc_v2.pg.conn import PgConnection
from cdc_v2.pg.sql import ident, qualified

SCHEMA = "cdc_v2"
TABLE = "heartbeat"


def setup_sql(runtime_role: str, *, schema: str = SCHEMA, table: str = TABLE) -> list[str]:
    """Statements for the setup role: the heartbeat table and the runtime role's privileges on it. The table has a
    primary key, so it needs no REPLICA IDENTITY change. It is added to the publication by `publication.ensure`."""
    t = qualified(schema, table)
    return [
        "CREATE SCHEMA IF NOT EXISTS %s" % ident(schema),
        "CREATE TABLE IF NOT EXISTS %s (slot text PRIMARY KEY, marker uuid NOT NULL, n bigint NOT NULL, "
        "beat_at timestamptz NOT NULL)" % t,
        "GRANT USAGE ON SCHEMA %s TO %s" % (ident(schema), ident(runtime_role)),
        "GRANT SELECT, INSERT, UPDATE ON %s TO %s" % (t, ident(runtime_role)),
    ]


@dataclass(frozen=True)
class HeartbeatWrite:
    slot: str
    marker: str           # uuid text, unique to this write; the stream finds it in the heartbeat row
    n: int                # the row's counter after this write
    xid: int              # the writing transaction's id (64-bit, pg_current_xact_id)
    lower: int            # pg_current_wal_insert_lsn() inside the transaction: lower ≤ start of the commit record
    upper: int            # pg_current_wal_insert_lsn() after COMMIT returned: end of the commit record ≤ upper


_BEAT_SQL = """
WITH up AS (
  INSERT INTO {t} AS h (slot, marker, n, beat_at) VALUES (%(slot)s, %(marker)s, 1, now())
  ON CONFLICT (slot) DO UPDATE SET marker = excluded.marker, n = h.n + 1, beat_at = excluded.beat_at
  RETURNING h.n)
SELECT up.n, pg_current_xact_id()::text, pg_current_wal_insert_lsn()::text FROM up
"""


def beat(primary: PgConnection, slot: str, *, deadline: Deadline, schema: str = SCHEMA,
         table: str = TABLE) -> HeartbeatWrite:
    """One heartbeat write on the primary (its own connection, autocommit). Returns what the stream needs to
    recognise the write when it arrives: the marker, the transaction id and a bracket around its commit record:
    lower ≤ commit start < commit end ≤ upper (the commit end is the END frame's position)."""
    marker = str(uuid.uuid4())
    sql = _BEAT_SQL.format(t=qualified(schema, table))
    with primary.transaction(deadline, what="heartbeat.beat"):
        row = primary.execute(sql, {"slot": slot, "marker": marker}, deadline=deadline, what="heartbeat.beat")
        if not row:
            raise errors.SourceError("heartbeat.beat: no row written")
        n, xid, lower = row[0]
    upper = primary.scalar("SELECT pg_current_wal_insert_lsn()::text", deadline=deadline, what="heartbeat.position")
    return HeartbeatWrite(slot=slot, marker=marker, n=int(n), xid=int(xid), lower=lsnmod.parse(lower),
                          upper=lsnmod.parse(upper))


@dataclass(frozen=True)
class StandbyLag:
    bytes_behind: int | None          # primary insert position − standby replay position (≥ 0; None if unknown)
    replay_position: int | None
    receive_position: int | None      # None when streaming has not started (H-041)
    primary_position: int
    seconds_since_replayed_commit: float | None   # now() − pg_last_xact_replay_timestamp() on the standby
    replay_lag_s: float | None        # pg_stat_replication.replay_lag on the primary, if this role may see it
    replay_paused: str | None         # pg_get_wal_replay_pause_state()
    recovery_min_apply_delay_ms: int


def standby_lag(primary: PgConnection, standby: PgConnection, *, deadline: Deadline) -> StandbyLag:
    """How far a standby is behind its primary. Bytes: the primary's position is read first, then the standby's
    replay position, so the difference is never larger than the true lag at the second read. Time: the age of the
    last replayed commit (meaningful while the primary writes, which the heartbeat guarantees every 10 s), and the
    primary's `replay_lag` where the role may read it (it needs pg_read_all_stats or the walreceiver's own role;
    the runtime role usually sees NULL)."""
    p_pos, lag_s = primary.execute(
        "SELECT pg_current_wal_insert_lsn()::text, "
        "(SELECT extract(epoch FROM max(replay_lag))::float8 FROM pg_stat_replication)",
        deadline=deadline, what="heartbeat.primary_position")[0]
    row = standby.execute(
        "SELECT pg_last_wal_replay_lsn()::text, pg_last_wal_receive_lsn()::text, "
        "extract(epoch FROM now() - pg_last_xact_replay_timestamp())::float8, "
        "CASE WHEN pg_is_in_recovery() THEN pg_get_wal_replay_pause_state() END, "
        "(SELECT setting::int FROM pg_settings WHERE name = 'recovery_min_apply_delay')",
        deadline=deadline, what="heartbeat.standby_position")[0]
    replay, recv, age, paused, delay = row
    pp = lsnmod.parse(p_pos)
    rp = lsnmod.parse(replay) if replay else None
    return StandbyLag(bytes_behind=max(0, pp - rp) if rp is not None else None, replay_position=rp,
                      receive_position=lsnmod.parse(recv) if recv else None, primary_position=pp,
                      seconds_since_replayed_commit=age, replay_lag_s=lag_s, replay_paused=paused,
                      recovery_min_apply_delay_ms=int(delay or 0))
