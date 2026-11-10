"""Our publication on the primary (C7, H-014, H-021, H-023, H-030, H-034).

`ensure` creates the publication if it is missing and adds tables. It only ever adds: nothing on the source is
fenced by the leader epoch, so a deposed run that repeats the call must be harmless. `remove` is separate and
explicit; nothing calls it automatically.

The publication publishes insert, update, delete and truncate, has no row filter and no column list, and
`publish_via_partition_root = false` (H-021: leaves are published under their own names, a truncate of a partition
is delivered, and the combined-table mode of SPEC 7.3 is the applier's job). Adding a table needs ownership of it,
so these calls run as the setup role (docs/design/source.md, role split).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from cdc_v2.crashpoints import crash_point, declare
from cdc_v2.deadline import Deadline
from cdc_v2.pg import catalog, errors
from cdc_v2.pg.conn import PgConnection
from cdc_v2.pg.sql import ident, qualified

declare("pg.publication_changed", "our publication was created, or tables were added to or removed from it")

DEFAULT = "cdc_v2_pub"
PUBLISH = ("insert", "update", "delete", "truncate")


class NotPublishable(errors.SourceError):
    """Tables that must not be added as they are. `problems` maps (schema, name) to the reason."""

    def __init__(self, message: str, problems: dict):
        super().__init__(message)
        self.problems = problems


@dataclass(frozen=True)
class PublicationTable:
    oid: int
    schema: str
    name: str
    columns: tuple[str, ...] | None       # a column list (we never set one; H-023)
    row_filter: str | None                # a row filter (we never set one; H-023)


@dataclass(frozen=True)
class PublicationState:
    """What exists. A publication that was dropped and recreated has another `oid` (H-034): compare it with the oid
    recorded when the slot was created; changes between the two DDLs are not in the stream."""
    name: str
    exists: bool
    oid: int | None = None
    owner: str | None = None
    all_tables: bool = False
    publish: tuple[str, ...] = ()
    via_root: bool = False
    generated_columns: str | None = None
    tables: tuple[PublicationTable, ...] = ()

    @property
    def problems(self) -> list[str]:
        """Ways in which the publication differs from what we create (empty when it is as ours)."""
        if not self.exists:
            return ["missing"]
        out = []
        if set(self.publish) != set(PUBLISH):
            out.append("publishes %s" % ",".join(self.publish))
        if self.via_root:
            out.append("publish_via_partition_root is on")
        for t in self.tables:
            if t.columns is not None:
                out.append("column list on %s.%s" % (t.schema, t.name))
            if t.row_filter is not None:
                out.append("row filter on %s.%s" % (t.schema, t.name))
        return out

    def has(self, schema: str, name: str) -> bool:
        return any((t.schema, t.name) == (schema, name) for t in self.tables)


_STATE_SQL = """
SELECT to_jsonb(p), pg_get_userbyid(p.pubowner)::text,
       (SELECT coalesce(json_agg(json_build_object('oid', c.oid, 'schema', n.nspname, 'name', c.relname,
                                                   'columns', (SELECT array_agg(a.attname::text ORDER BY k.i)
                                                               FROM unnest(pr.prattrs::int2[]) WITH ORDINALITY k(n, i)
                                                               JOIN pg_attribute a ON a.attrelid = c.oid
                                                                                  AND a.attnum = k.n),
                                                   'has_columns', pr.prattrs IS NOT NULL,
                                                   'row_filter', pg_get_expr(pr.prqual, pr.prrelid))
                                 ORDER BY n.nspname, c.relname), '[]'::json)
        FROM pg_publication_rel pr JOIN pg_class c ON c.oid = pr.prrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace WHERE pr.prpubid = p.oid)
FROM pg_publication p WHERE p.pubname = %s
"""


def state(conn: PgConnection, *, publication: str = DEFAULT, deadline: Deadline) -> PublicationState:
    rows = conn.execute(_STATE_SQL, (publication,), deadline=deadline, what="publication.state")
    if not rows:
        return PublicationState(name=publication, exists=False)
    p, owner, tabs = rows[0]
    gc = p.get("pubgencols")
    return PublicationState(
        name=publication, exists=True, oid=int(p["oid"]), owner=owner, all_tables=bool(p["puballtables"]),
        publish=tuple(op for op in PUBLISH if p.get("pub" + op)), via_root=bool(p.get("pubviaroot")),
        generated_columns=gc if isinstance(gc, str) else None,
        tables=tuple(PublicationTable(oid=int(t["oid"]), schema=t["schema"], name=t["name"],
                                      columns=tuple(t["columns"] or ()) if t["has_columns"] else None,
                                      row_filter=t["row_filter"]) for t in tabs))


def check(tables: Iterable[catalog.SourceTable]) -> dict[tuple[str, str], str]:
    """Why each table cannot be added as it is (H-014, H-030). Empty when all can."""
    out = {}
    for t in tables:
        if t.persistence != "permanent":
            out[t.qualified] = "%s table: no WAL for its rows, cannot be published" % t.persistence
            continue
        p = t.replica_identity_problem
        if p:
            out[t.qualified] = p
    return out


@dataclass(frozen=True)
class EnsureResult:
    created: bool                                # the publication was created by this call
    added: tuple[tuple[str, str], ...]           # tables this call added
    present: tuple[tuple[str, str], ...]         # tables that were in it already
    next_xid: int                                # X of design 7.5 (see `ensure`)
    state: PublicationState


def ensure(primary: PgConnection, tables: Iterable, *, publication: str = DEFAULT,
           deadline: Deadline) -> EnsureResult:
    """Create the publication if it is missing and add the tables that are not in it. Only ever adds.

    `tables` are SourceTable values or (schema, name) pairs; they are read again here, and a table that cannot be
    published as it is (no primary key and not REPLICA IDENTITY FULL, also with USING INDEX; NOTHING; an identity
    index that is not the key; unlogged or temporary) raises `NotPublishable` before anything is added. Fix it with
    `replica_identity.set_full` first; the order matters (H-014: the source's own updates fail otherwise).

    Returns `next_xid` = `pg_snapshot_xmax(pg_current_snapshot())` read after the change committed: the first
    transaction id not yet assigned. Every transaction with a smaller id has started before the change; a table's
    first snapshot needs xmin ≥ next_xid (design 7.5). Racing callers are harmless: a table that another caller added
    meanwhile is counted as present.
    """
    pairs = sorted({(t.schema, t.name) if isinstance(t, catalog.SourceTable) else tuple(t) for t in tables})
    if primary.in_recovery(deadline):
        raise errors.SourceError("publication.ensure needs the primary")
    created = False
    for attempt in range(3):
        st = state(primary, publication=publication, deadline=deadline)
        if not st.exists:
            try:
                primary.execute("CREATE PUBLICATION %s WITH (publish = 'insert, update, delete, truncate', "
                                "publish_via_partition_root = false)" % ident(publication), deadline=deadline,
                                what="publication.create")
                created = True
                crash_point("pg.publication_changed")
            except errors.SourceError as e:
                if e.sqlstate != "42710":           # created by someone else meanwhile
                    raise
            continue
        missing = [p for p in pairs if not st.has(*p) and not st.all_tables]
        present = [p for p in pairs if p not in missing]
        if not missing:
            return EnsureResult(created, (), tuple(present), _next_xid(primary, deadline), st)
        found = {t.qualified: t for t in catalog.tables(primary, missing, publication=publication,
                                                         deadline=deadline)}
        problems = {p: "no such table" for p in missing if p not in found}
        problems.update(check(found.values()))
        if problems:
            raise NotPublishable("tables cannot be published as they are: %s"
                                 % "; ".join("%s.%s: %s" % (k[0], k[1], v) for k, v in sorted(problems.items())),
                                 problems)
        try:
            primary.execute("ALTER PUBLICATION %s ADD TABLE %s" % (ident(publication),
                                                                  ", ".join(qualified(*p) for p in missing)),
                            deadline=deadline, what="publication.add")
        except errors.SourceError as e:
            if e.sqlstate in ("42710", "42704") and attempt < 2:     # added (or publication dropped) meanwhile
                continue
            raise
        crash_point("pg.publication_changed")
        st = state(primary, publication=publication, deadline=deadline)
        return EnsureResult(created, tuple(missing), tuple(present), _next_xid(primary, deadline), st)
    raise errors.SourceError("publication.ensure: the publication kept changing under us")


def _next_xid(primary: PgConnection, deadline: Deadline) -> int:
    return int(primary.scalar("SELECT pg_snapshot_xmax(pg_current_snapshot())::text", deadline=deadline,
                              what="publication.next_xid"))


def remove(primary: PgConnection, tables: Iterable, *, publication: str = DEFAULT, deadline: Deadline) -> int:
    """Remove tables from the publication. Explicit only: no automatic path calls it. Returns how many."""
    pairs = sorted({(t.schema, t.name) if isinstance(t, catalog.SourceTable) else tuple(t) for t in tables})
    st = state(primary, publication=publication, deadline=deadline)
    present = [p for p in pairs if st.has(*p)]
    if present:
        primary.execute("ALTER PUBLICATION %s DROP TABLE %s" % (ident(publication),
                                                               ", ".join(qualified(*p) for p in present)),
                        deadline=deadline, what="publication.remove")
        crash_point("pg.publication_changed")
    return len(present)
