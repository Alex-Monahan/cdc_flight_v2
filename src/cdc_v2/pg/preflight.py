"""Pre-flight checks on one node, as the role that will use it (the runtime role): what the pipeline needs to read
there before it relies on that node. Read-only; one statement after a privilege probe.

`check(conn, deadline) -> Preflight` with `problems` (empty when the node is usable) and the facts it read:

- `pg_control_checkpoint()` is callable and gives `next_xid` (the backfill reads it once per connection on the server
  it reads from: a standby that has just started can hide a committed transaction until it replays its next commit,
  and the cut is used only when the snapshot's `xmax` is at or above that value). If the role may not call it, the
  fix is `GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_checkpoint() TO <runtime role>` on the primary (it
  replicates to the standbys). The same for `pg_control_system()` (the system identifier of the slot facts).
- the role has the REPLICATION attribute (slot functions, replication connections);
- `wal_level = logical`;
- on a standby: `hot_standby_feedback = on`, a `primary_slot_name`, no `recovery_min_apply_delay`, replay not
  paused (H-042, H-044).
"""
from __future__ import annotations

from dataclasses import dataclass

from cdc_v2.deadline import Deadline
from cdc_v2.pg.conn import PgConnection


@dataclass(frozen=True)
class Preflight:
    in_recovery: bool
    control_checkpoint_callable: bool
    checkpoint_next_xid: int | None       # 64-bit: epoch << 32 | xid, from pg_control_checkpoint().next_xid
    control_system_callable: bool
    replication_role: bool
    wal_level: str
    hot_standby_feedback: bool
    primary_slot_name: str | None
    recovery_min_apply_delay_ms: int
    replay_paused: str | None
    problems: tuple[str, ...]


_PRIV_SQL = """
SELECT has_function_privilege('pg_catalog.pg_control_checkpoint()', 'EXECUTE'),
       has_function_privilege('pg_catalog.pg_control_system()', 'EXECUTE'),
       (SELECT rolreplication FROM pg_roles WHERE rolname = current_user)
"""

_FACTS_SQL = """
SELECT pg_is_in_recovery(),
       {ckpt} AS next_xid,
       current_setting('wal_level'),
       current_setting('hot_standby_feedback') = 'on',
       nullif(current_setting('primary_slot_name'), ''),
       (SELECT setting::int FROM pg_settings WHERE name = 'recovery_min_apply_delay'),
       CASE WHEN pg_is_in_recovery() THEN pg_get_wal_replay_pause_state() END
"""


def parse_full_xid(text: str | None) -> int | None:
    """'epoch:xid' (pg_control_checkpoint().next_xid) or a plain number -> a 64-bit transaction id."""
    if text is None:
        return None
    if ":" in text:
        epoch, xid = text.split(":", 1)
        return (int(epoch) << 32) | int(xid)
    return int(text)


def check(conn: PgConnection, *, deadline: Deadline) -> Preflight:
    ckpt_ok, sys_ok, repl = conn.execute(_PRIV_SQL, deadline=deadline, what="preflight.privileges")[0]
    sql = _FACTS_SQL.format(ckpt="(SELECT next_xid FROM pg_control_checkpoint())" if ckpt_ok else "NULL::text")
    rec, next_xid, wal_level, hsf, psn, delay, paused = conn.execute(sql, deadline=deadline,
                                                                    what="preflight.facts")[0]
    problems = []
    if not ckpt_ok:
        problems.append("pg_control_checkpoint() is not callable by this role: GRANT EXECUTE ON FUNCTION "
                        "pg_catalog.pg_control_checkpoint() TO <runtime role> (on the primary)")
    if not sys_ok:
        problems.append("pg_control_system() is not callable by this role: GRANT EXECUTE ON FUNCTION "
                        "pg_catalog.pg_control_system() TO <runtime role> (on the primary)")
    if not repl:
        problems.append("the role lacks the REPLICATION attribute")
    if wal_level != "logical":
        problems.append("wal_level is %s, not logical" % wal_level)
    if rec:
        if not hsf:
            problems.append("hot_standby_feedback is off on the standby (H-042)")
        if not psn:
            problems.append("the standby has no primary_slot_name (H-042)")
        if delay:
            problems.append("recovery_min_apply_delay is %d ms on the standby (H-044)" % delay)
        if paused and paused != "not paused":
            problems.append("replay is %s on the standby (H-044)" % paused)
    return Preflight(in_recovery=bool(rec), control_checkpoint_callable=bool(ckpt_ok),
                     checkpoint_next_xid=parse_full_xid(next_xid), control_system_callable=bool(sys_ok),
                     replication_role=bool(repl), wal_level=wal_level, hot_standby_feedback=bool(hsf),
                     primary_slot_name=psn, recovery_min_apply_delay_ms=int(delay or 0), replay_paused=paused,
                     problems=tuple(problems))
