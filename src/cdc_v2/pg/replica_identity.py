"""Replica identity (H-014). A table without a primary key needs `REPLICA IDENTITY FULL` before it joins the
publication, also when it has `REPLICA IDENTITY USING INDEX` (Debezium gives such a table no key, and the engine
would carry an incomplete old image). In the wrong order, the source's own UPDATE and DELETE on that table fail
("cannot update table … because it does not have a replica identity and publishes updates").
"""
from __future__ import annotations

from cdc_v2.crashpoints import crash_point, declare
from cdc_v2.deadline import Deadline
from cdc_v2.pg import catalog, errors
from cdc_v2.pg.conn import PgConnection
from cdc_v2.pg.sql import qualified

declare("pg.replica_identity_set", "ALTER TABLE … REPLICA IDENTITY FULL committed on the primary")


def problem(table: catalog.SourceTable | catalog.TableFacts) -> str | None:
    """Why the table cannot be published as it is, or None (see catalog.identity_problem)."""
    return table.replica_identity_problem


def set_full(primary: PgConnection, table, deadline: Deadline, *, lock_timeout_ms: int | None = None) -> bool:
    """`ALTER TABLE … REPLICA IDENTITY FULL` with a `lock_timeout` (the statement needs a short ACCESS EXCLUSIVE
    lock; on timeout `errors.LockTimeout`). `table` is a SourceTable or (schema, name). Needs ownership (setup role).
    Returns False when the table already had FULL (nothing was changed)."""
    schema, name = (table.schema, table.name) if hasattr(table, "schema") else tuple(table)
    if primary.in_recovery(deadline):
        raise errors.SourceError("replica_identity.set_full needs the primary")
    lt = int(lock_timeout_ms if lock_timeout_ms is not None else primary.node.lock_timeout_ms)
    with primary.transaction(deadline, what="replica_identity.set_full"):
        cur = primary.scalar("SELECT c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                             "WHERE n.nspname = %s AND c.relname = %s", (schema, name), deadline=deadline,
                             what="replica_identity.read")
        if cur is None:
            raise errors.SourceError("replica_identity.set_full: no table %s.%s" % (schema, name), "42P01")
        if cur == "f":
            return False
        primary.execute("SET LOCAL lock_timeout = %d" % lt, deadline=deadline, what="replica_identity.lock_timeout")
        primary.execute("ALTER TABLE %s REPLICA IDENTITY FULL" % qualified(schema, name), deadline=deadline,
                        what="replica_identity.set_full")
    crash_point("pg.replica_identity_set")
    return True
