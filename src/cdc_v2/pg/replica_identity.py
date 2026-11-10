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
    For a partitioned table, every existing leaf partition is set too (the leaves are what is published; ALTER TABLE
    on the parent does not reach them). A partition attached later needs its own call (P4 discovery).
    Returns False when every target already had FULL (nothing was changed)."""
    schema, name = (table.schema, table.name) if hasattr(table, "schema") else tuple(table)
    if primary.in_recovery(deadline):
        raise errors.SourceError("replica_identity.set_full needs the primary")
    lt = int(lock_timeout_ms if lock_timeout_ms is not None else primary.node.lock_timeout_ms)
    from cdc_v2.pg.publication import leaves          # a partitioned table: its leaves are what is published
    targets = [(schema, name)] + leaves(primary, schema, name, deadline=deadline)
    changed = False
    with primary.transaction(deadline, what="replica_identity.set_full"):
        primary.execute("SET LOCAL lock_timeout = %d" % lt, deadline=deadline, what="replica_identity.lock_timeout")
        for s, n in targets:
            cur = primary.scalar("SELECT c.relreplident FROM pg_class c JOIN pg_namespace ns "
                                 "ON ns.oid = c.relnamespace WHERE ns.nspname = %s AND c.relname = %s", (s, n),
                                 deadline=deadline, what="replica_identity.read")
            if cur is None:
                raise errors.SourceError("replica_identity.set_full: no table %s.%s" % (s, n), "42P01")
            if cur == "f":
                continue
            primary.execute("ALTER TABLE %s REPLICA IDENTITY FULL" % qualified(s, n), deadline=deadline,
                            what="replica_identity.set_full")
            changed = True
    if changed:
        crash_point("pg.replica_identity_set")
    return changed
