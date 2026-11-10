"""The source catalog (C7): `tables` (one immutable value per table), `observe` (the catalog fence of H-008 with its
WAL bracket) and `diff` (what changed between two reads).

`tables` reads the full description of each table, including formatted type names, in one REPEATABLE READ
transaction. `observe` reads only raw catalog columns (no `format_type`, no casts to `regclass`), because those
functions use the latest catalog state instead of the transaction's snapshot; that keeps every fact of an
observation at one replay position.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Callable, Iterable, Mapping

from cdc_v2 import lsn as lsnmod
from cdc_v2.deadline import Deadline
from cdc_v2.pg import errors
from cdc_v2.pg.conn import PgConnection

SYSTEM_SCHEMAS = ("pg_catalog", "information_schema")
RELKINDS = {"r": "ordinary", "p": "partitioned"}
PERSISTENCE = {"p": "permanent", "u": "unlogged", "t": "temporary"}
REPLIDENT = {"d": "default", "n": "nothing", "f": "full", "i": "index"}
GENERATED = {"": "", "s": "stored", "v": "virtual"}
IDENTITY = {"": "", "a": "always", "d": "by default"}
STORAGE = {"p": "plain", "e": "external", "m": "main", "x": "extended"}
TYPTYPE = {"b": "base", "c": "composite", "d": "domain", "e": "enum", "p": "pseudo", "r": "range",
           "m": "multirange"}


# ----------------------------------------------------------------------------------------------------- values

@dataclass(frozen=True)
class TypeInfo:
    """A type as far as the catalog describes it. `kind` is base, array, domain, enum, composite, range,
    multirange or pseudo. For a domain, `base` is the ultimate base type (domains over domains are followed)."""
    oid: int
    schema: str
    name: str
    kind: str
    base: "TypeInfo | None" = None
    element: "TypeInfo | None" = None
    enum_labels: tuple[str, ...] | None = None
    fields: tuple[tuple[str, "TypeInfo"], ...] | None = None
    range_subtype: "TypeInfo | None" = None
    typlen: int = -1


@dataclass(frozen=True)
class Column:
    attnum: int
    name: str
    type_oid: int
    typmod: int
    type_name: str                 # format_type(atttypid, atttypmod): the full name with modifiers
    type: TypeInfo
    ndims: int                     # array dimensions as declared (attndims; 0 when not declared)
    not_null: bool
    has_default: bool
    generated: str                 # '' | 'stored' | 'virtual'
    identity: str                  # '' | 'always' | 'by default'
    storage: str                   # plain | external | main | extended
    toastable: bool                # a value can be TOASTed, so the stream may send it as "unchanged"


@dataclass(frozen=True)
class SourceTable:
    oid: int
    schema: str
    name: str
    relfilenode: int
    relkind: str                   # 'ordinary' | 'partitioned'
    is_partition: bool
    partition_root: tuple[str, str] | None
    persistence: str               # 'permanent' | 'unlogged' | 'temporary'
    rls_enabled: bool
    rls_forced: bool
    rls_applies_to_reader: bool    # row_security_active(): policies filter what the reading role sees (H-039)
    columns: tuple[Column, ...]
    max_attnum: int                # highest attnum including dropped columns: gaps in `columns` are dropped ones
    key: tuple[str, ...]           # rule K: the primary key columns in key order when the table has a key, else ()
    key_attnums: tuple[int, ...]   # the same as attnums; () for every table without a key (rule K)
    key_source: str | None         # 'primary key' when the table has a key (rule K), else None
    has_primary_key: bool
    replica_identity: str          # default | index | full | nothing
    replica_identity_index: str | None
    replica_identity_index_is_key: bool
    published: bool                # in our publication (directly or through a partition ancestor)
    publication_columns: tuple[str, ...] | None   # column list of our publication for this table (H-023)
    publication_row_filter: str | None
    primary_key_deferrable: bool = False   # the primary key is DEFERRABLE (pg_index.indimmediate is false)

    @property
    def qualified(self) -> tuple[str, str]:
        return (self.schema, self.name)

    @property
    def has_key(self) -> bool:
        """Rule K (core design section 6): the table has a key, its primary key. False for every other table."""
        return has_key(self.has_primary_key, self.primary_key_deferrable, self.replica_identity,
                       self.replica_identity_index_is_key)

    @property
    def name_problem(self) -> str | None:
        """The name rule: a double quote in the schema or table name means the table is not streamed (stock
        Debezium 3.7.1 stops with FATAL or drops the records of such names; docs/design/engine.md 4a, H-051). The
        message never holds the name."""
        return name_problem(self.schema, self.name)

    @property
    def replica_identity_problem(self) -> str | None:
        """Why the table cannot be published as it is, or None. A table without a primary key, and a table whose
        primary key is deferrable, needs REPLICA IDENTITY FULL (also with USING INDEX: such a table has no key by
        rule K); a table with an immediate primary key needs DEFAULT, FULL, or an index identity that is the primary
        key's index (H-014, H-015)."""
        return identity_problem(self.has_primary_key, self.replica_identity, self.replica_identity_index_is_key,
                                pk_deferrable=self.primary_key_deferrable)

    def hash(self) -> str:
        """Stable hash of everything in this value."""
        return _hash(asdict(self))

    def shape_hash(self) -> str:
        """Hash of what a destination table is built for: OID, names, columns (attnum, name, type), the key facts
        (rule K's answer, key columns, deferrability of the primary key) and replica identity. Not relfilenode, row
        security or publication membership."""
        return _hash({"oid": self.oid, "schema": self.schema, "name": self.name,
                      "columns": [(c.attnum, c.name, c.type_oid, c.typmod, c.type_name) for c in self.columns],
                      "key": self.key_attnums, "has_key": self.has_key,
                      "pk_deferrable": self.primary_key_deferrable, "replica_identity": self.replica_identity})


def name_problem(schema: str, name: str) -> str | None:
    """See SourceTable.name_problem."""
    where = [w for w, n in (("schema", schema), ("table", name)) if '"' in n]
    if not where:
        return None
    return "%s name contains a double quote: stock Debezium cannot stream it" % " and ".join(where)


def has_key(has_pk: bool, pk_deferrable: bool, replident: str, index_is_pk: bool) -> bool:
    """Rule K (core design section 6): a table has a key when it has a primary key that is not deferrable and its
    replica identity is DEFAULT, FULL, or USING INDEX on the primary key's own index. Every other table that can be
    published is a table without a key and needs REPLICA IDENTITY FULL."""
    return bool(has_pk and not pk_deferrable
                and (replident in ("default", "full") or (replident == "index" and index_is_pk)))


def identity_problem(has_pk: bool, replident: str, index_is_pk: bool, *, pk_deferrable: bool = False) -> str | None:
    if replident == "full":
        return None
    if not has_pk:
        return "no primary key: needs REPLICA IDENTITY FULL (replica identity is %s)" % replident
    if pk_deferrable:
        return "primary key is deferrable: needs REPLICA IDENTITY FULL (replica identity is %s)" % replident
    if replident == "default" or (replident == "index" and index_is_pk):
        return None
    if replident == "nothing":
        return "replica identity NOTHING: needs DEFAULT or FULL"
    return "replica identity index is not the primary key: needs DEFAULT or FULL"


def _hash(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=list)
                          .encode()).hexdigest()


# ----------------------------------------------------------------------------------------------------- tables

def _name_filter(include) -> tuple[list[str] | None, list[str] | None, Callable | None]:
    if include is None:
        return None, None, None
    if callable(include):
        return None, None, include
    pairs = [tuple(p) for p in include]
    for p in pairs:
        if len(p) != 2 or not all(isinstance(x, str) for x in p):
            raise ValueError("include must be (schema, name) pairs or a callable")
    return [p[0] for p in pairs], [p[1] for p in pairs], None


_TABLES_SQL = """
SELECT c.oid, n.nspname, c.relname, c.relfilenode, c.relkind, c.relispartition, c.relpersistence,
       c.relrowsecurity, c.relforcerowsecurity, row_security_active(c.oid) AS rls_active, c.relreplident,
       CASE WHEN c.relispartition THEN (SELECT array_agg(a.relid::oid ORDER BY a.ord)
                                        FROM pg_partition_ancestors(c.oid) WITH ORDINALITY a(relid, ord)
                                        WHERE a.relid::oid <> c.oid) END
         AS ancestors,
       (SELECT max(attnum) FROM pg_attribute WHERE attrelid = c.oid AND attnum > 0) AS max_attnum
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\\_toast%%'
  AND n.nspname NOT LIKE 'pg\\_temp\\_%%'
  AND (%(schemas)s::text[] IS NULL OR (n.nspname, c.relname) IN (SELECT * FROM unnest(%(schemas)s::text[],
                                                                                        %(names)s::text[])))
ORDER BY n.nspname, c.relname
"""

_COLUMNS_SQL = """
SELECT a.attrelid, a.attnum, a.attname, a.atttypid, a.atttypmod, format_type(a.atttypid, a.atttypmod),
       a.attndims, a.attnotnull, a.atthasdef, a.attgenerated, a.attidentity, a.attstorage
FROM pg_attribute a
WHERE a.attrelid = ANY(%s) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attrelid, a.attnum
"""

_KEYS_SQL = """
SELECT i.indrelid, i.indexrelid, i.indisprimary, i.indisreplident, ic.relname,
       ARRAY(SELECT i.indkey[k] FROM generate_series(0, i.indnkeyatts - 1) k),       -- key columns, not INCLUDE
       i.indimmediate
FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
WHERE i.indrelid = ANY(%s) AND (i.indisprimary OR i.indisreplident)
"""

_PUB_SQL = """
SELECT pr.prrelid, pr.prattrs::int2[], pg_get_expr(pr.prqual, pr.prrelid), p.puballtables
FROM pg_publication p LEFT JOIN pg_publication_rel pr ON pr.prpubid = p.oid
WHERE p.pubname = %s
"""

_TYPES_SQL = """
SELECT t.oid, n.nspname, t.typname, t.typtype, t.typbasetype, t.typelem, t.typrelid, t.typlen,
       (t.typsubscript = 'pg_catalog.array_subscript_handler'::regproc) AS is_array,
       (SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder) FROM pg_enum e WHERE e.enumtypid = t.oid),
       (SELECT r.rngsubtype FROM pg_range r WHERE r.rngtypid = t.oid),
       (SELECT r.rngsubtype FROM pg_range r WHERE r.rngmultitypid = t.oid)
FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE t.oid = ANY(%s)
"""

_FIELDS_SQL = """
SELECT attrelid, attname, atttypid FROM pg_attribute
WHERE attrelid = ANY(%s) AND attnum > 0 AND NOT attisdropped ORDER BY attrelid, attnum
"""


def tables(conn: PgConnection, include=None, *, publication: str = "cdc_v2_pub",
           deadline: Deadline) -> list[SourceTable]:
    """Every ordinary or partitioned table outside the system schemas, or those named by `include` ((schema, name)
    pairs, or a callable(schema, name) -> bool). Read in one REPEATABLE READ transaction (a recovery conflict on a
    standby raises `errors.RecoveryConflict`: read again)."""
    schemas, names, pred = _name_filter(include)
    with conn.transaction(deadline, isolation="repeatable read", read_only=True, what="catalog.tables"):
        rows = conn.execute(_TABLES_SQL, {"schemas": schemas, "names": names}, deadline=deadline,
                            what="catalog.tables")
        if pred is not None:
            rows = [r for r in rows if pred(r[1], r[2])]
        oids = [r[0] for r in rows]
        if not oids:
            return []
        cols = conn.execute(_COLUMNS_SQL, (oids,), deadline=deadline, what="catalog.columns")
        keys = conn.execute(_KEYS_SQL, (oids,), deadline=deadline, what="catalog.keys")
        pub = conn.execute(_PUB_SQL, (publication,), deadline=deadline, what="catalog.publication")
        types = _load_types(conn, {c[3] for c in cols}, deadline)
        ancestors = {a for r in rows for a in (r[11] or [])}
        names_by_oid = {r[0]: (r[1], r[2]) for r in rows}
        missing = [a for a in ancestors if a not in names_by_oid]
        if missing:
            for oid, nsp, rel in conn.execute(
                    "SELECT c.oid, n.nspname, c.relname FROM pg_class c JOIN pg_namespace n "
                    "ON n.oid = c.relnamespace WHERE c.oid = ANY(%s)", (missing,), deadline=deadline,
                    what="catalog.ancestors"):
                names_by_oid[oid] = (nsp, rel)
    return _build_tables(rows, cols, keys, pub, types, names_by_oid)


def _load_types(conn: PgConnection, oids: set[int], deadline: Deadline) -> dict[int, TypeInfo]:
    raw: dict[int, tuple] = {}
    fields: dict[int, list[tuple[str, int]]] = {}
    todo = set(oids)
    for _ in range(16):                      # nesting depth of domains, arrays, composites and ranges
        todo -= set(raw)
        if not todo:
            break
        rows = conn.execute(_TYPES_SQL, (sorted(todo),), deadline=deadline, what="catalog.types")
        relids = []
        nxt = set()
        for r in rows:
            raw[r[0]] = r
            for dep in (r[4], r[5] if r[8] else 0, r[10], r[11]):
                if dep:
                    nxt.add(dep)
            if r[3] == "c" and r[6]:
                relids.append(r[6])
        if relids:
            for relid, attname, typid in conn.execute(_FIELDS_SQL, (relids,), deadline=deadline,
                                                      what="catalog.fields"):
                fields.setdefault(relid, []).append((attname, typid))
                nxt.add(typid)
        todo = nxt
    built: dict[int, TypeInfo] = {}

    def build(oid: int, depth: int = 0) -> TypeInfo:
        if oid in built:
            return built[oid]
        r = raw.get(oid)
        if r is None or depth > 32:
            return TypeInfo(oid=oid, schema="?", name="?", kind="unknown")
        oid_, nsp, name, typtype, basetype, elem, relid, typlen, is_array, labels, rng_sub, mrng_sub = r
        kind = "array" if (typtype == "b" and is_array) else TYPTYPE.get(typtype, typtype)
        base = element = rsub = None
        flds = None
        if kind == "domain" and basetype:
            b = build(basetype, depth + 1)
            base = b.base if b.kind == "domain" and b.base is not None else b
        if kind == "array" and elem:
            element = build(elem, depth + 1)
        if kind == "composite" and relid:
            flds = tuple((n, build(t, depth + 1)) for n, t in fields.get(relid, []))
        if kind in ("range", "multirange") and (rng_sub or mrng_sub):
            rsub = build(rng_sub or mrng_sub, depth + 1)
        ti = TypeInfo(oid=oid_, schema=nsp, name=name, kind=kind, base=base, element=element,
                      enum_labels=tuple(labels) if labels is not None else None, fields=flds, range_subtype=rsub,
                      typlen=typlen)
        built[oid] = ti
        return ti

    return {o: build(o) for o in raw}


def _build_tables(rows, cols, keys, pub, types, names_by_oid) -> list[SourceTable]:
    cols_by: dict[int, list] = {}
    for c in cols:
        cols_by.setdefault(c[0], []).append(c)
    keys_by: dict[int, list] = {}
    for k in keys:
        keys_by.setdefault(k[0], []).append(k)
    all_tables = bool(pub) and bool(pub[0][3])
    pub_by = {p[0]: p for p in pub if p[0] is not None}
    out = []
    for (oid, nsp, rel, filenode, relkind, ispart, persistence, rls, rlsforce, rlsactive, replident, ancestors,
         max_attnum) in rows:
        columns = []
        by_attnum = {}
        for (_, attnum, attname, typid, typmod, typname, ndims, notnull, hasdef, gen, ident_, storage) in \
                cols_by.get(oid, []):
            ti = types.get(typid) or TypeInfo(oid=typid, schema="?", name="?", kind="unknown")
            col = Column(attnum=attnum, name=attname, type_oid=typid, typmod=typmod, type_name=typname, type=ti,
                         ndims=ndims, not_null=notnull, has_default=hasdef, generated=GENERATED.get(gen, gen),
                         identity=IDENTITY.get(ident_, ident_), storage=STORAGE.get(storage, storage),
                         toastable=(ti.typlen == -1 and storage != "p"))
            columns.append(col)
            by_attnum[attnum] = col
        pk = next((k for k in keys_by.get(oid, []) if k[2]), None)
        ri = next((k for k in keys_by.get(oid, []) if k[3]), None)
        pk_deferrable = bool(pk is not None and not pk[6])
        ri_is_pk = bool(ri and pk and ri[1] == pk[1])
        keyed = has_key(pk is not None, pk_deferrable, REPLIDENT.get(replident, replident), ri_is_pk)
        key_attnums = tuple(int(a) for a in pk[5]) if keyed else ()
        p = pub_by.get(oid)
        via_ancestor = next((pub_by[a] for a in (ancestors or []) if a in pub_by), None)
        prow = p or via_ancestor
        pcols = None
        if p is not None and p[1] is not None:
            pcols = tuple(by_attnum[a].name if a in by_attnum else "#%d" % a for a in p[1])
        root = names_by_oid.get(ancestors[-1]) if ancestors else None
        out.append(SourceTable(
            oid=oid, schema=nsp, name=rel, relfilenode=filenode, relkind=RELKINDS[relkind], is_partition=ispart,
            partition_root=root, persistence=PERSISTENCE.get(persistence, persistence), rls_enabled=rls,
            rls_forced=rlsforce, rls_applies_to_reader=bool(rlsactive), columns=tuple(columns),
            max_attnum=max_attnum or 0,
            key=tuple(by_attnum[a].name for a in key_attnums if a in by_attnum), key_attnums=key_attnums,
            key_source="primary key" if keyed else None,
            has_primary_key=pk is not None, replica_identity=REPLIDENT.get(replident, replident),
            replica_identity_index=ri[4] if ri else None,
            replica_identity_index_is_key=ri_is_pk,
            published=all_tables or prow is not None, publication_columns=pcols,
            publication_row_filter=p[2] if p is not None else None, primary_key_deferrable=pk_deferrable))
    return out


# ----------------------------------------------------------------------------------------------------- observe

@dataclass(frozen=True)
class ColumnFacts:
    attnum: int
    name: str
    type_oid: int
    typmod: int
    dropped: bool
    generated: str
    has_default: bool


@dataclass(frozen=True)
class TableFacts:
    """The raw catalog facts of one table in an observation (H-008)."""
    oid: int
    schema: str
    name: str
    relfilenode: int
    replica_identity: str
    relkind: str
    persistence: str
    is_partition: bool
    all_columns: tuple[ColumnFacts, ...]          # in attnum order, dropped ones included (flag `dropped`)
    pk_attnums: tuple[int, ...]
    replica_identity_index_attnums: tuple[int, ...] | None
    replica_identity_index_is_key: bool
    published: bool                                # in our publication, directly or through a partition ancestor
    publication_has_column_list: bool
    publication_has_row_filter: bool
    pk_deferrable: bool = False                    # the primary key is DEFERRABLE (pg_index.indimmediate is false)

    @property
    def columns(self) -> tuple[ColumnFacts, ...]:
        return tuple(c for c in self.all_columns if not c.dropped)

    @property
    def has_key(self) -> bool:
        """Rule K, as SourceTable.has_key."""
        return has_key(bool(self.pk_attnums), self.pk_deferrable, self.replica_identity,
                       self.replica_identity_index_is_key)

    @property
    def key_attnums(self) -> tuple[int, ...]:
        """Rule K: the primary key's attnums when the table has a key, else ()."""
        return self.pk_attnums if self.has_key else ()

    @property
    def identity(self) -> tuple:
        """What a destination table is built for (package T): OID; per live column attnum, name, type, modifier;
        the key columns (rule K); the replica identity, the attnums of the identity index (USING INDEX) and the
        deferrability of the primary key, which decide what old image the stream carries. Not relfilenode."""
        return (self.oid, tuple((c.attnum, c.name, c.type_oid, c.typmod) for c in self.columns), self.key_attnums,
                self.replica_identity, self.replica_identity_index_attnums, self.pk_deferrable)

    @property
    def replica_identity_problem(self) -> str | None:
        return identity_problem(bool(self.pk_attnums), self.replica_identity, self.replica_identity_index_is_key,
                                pk_deferrable=self.pk_deferrable)


@dataclass(frozen=True)
class PublicationFacts:
    oid: int
    name: str
    all_tables: bool
    publish: tuple[str, ...]          # subset of insert, update, delete, truncate
    via_root: bool
    generated_columns: str | None     # 18: 'n' none / 's' stored; None before 18


@dataclass(frozen=True)
class CatalogObservation:
    """The catalog state at one replay position in [lower, upper] (standby). On a primary only the upper bound holds
    (no commit inserted after `upper` is visible); whether a transaction is in the observation is `visible(xid)` —
    see docs/design/source.md."""
    lower: int                                          # b
    upper: int                                          # a
    in_recovery: bool
    snapshot: str                                       # pg_current_snapshot() of the observing transaction
    tables: Mapping[int, TableFacts]                    # by OID
    by_name: Mapping[tuple[str, str], TableFacts]
    absent: tuple[tuple[str, str], ...]                 # requested names that do not exist
    publication: PublicationFacts | None
    elapsed_s: float = field(default=0.0, compare=False)

    @property
    def in_progress(self) -> tuple[int, ...]:
        parts = self.snapshot.split(":")
        return tuple(int(x) for x in parts[2].split(",") if x) if len(parts) == 3 else ()

    @property
    def xmin(self) -> int:
        return int(self.snapshot.split(":")[0])

    @property
    def xmax(self) -> int:
        return int(self.snapshot.split(":")[1])

    def visible(self, xid: int) -> bool:
        """Whether the observing snapshot sees the (64-bit) transaction `xid` as finished: below xmax and not in the
        in-progress list. On a primary this, not the bracket, says whether a DDL whose commit record lies before
        `lower` is in the observation (it may not be: a commit record is written before the transaction becomes
        visible). Whether it committed or aborted is not part of the answer."""
        return xid < self.xmax and xid not in self.in_progress


_OBSERVE_SQL = """
WITH pub AS (SELECT oid FROM pg_publication WHERE pubname = %(pub)s),
rels AS (
  SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE (n.nspname, c.relname) IN (SELECT * FROM unnest(%(schemas)s::text[], %(names)s::text[]))
  UNION
  SELECT pr.prrelid FROM pg_publication_rel pr WHERE pr.prpubid IN (SELECT oid FROM pub)
),
f AS (
SELECT c.oid, n.nspname, c.relname, c.relfilenode, c.relreplident, c.relkind, c.relpersistence, c.relispartition,
       a.attnums, a.attnames, a.atttypids, a.atttypmods, a.attdropped, a.attgenerated, a.atthasdef,
       (SELECT ARRAY(SELECT i.indkey[k] FROM generate_series(0, i.indnkeyatts - 1) k)
          FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) AS pk,
       (SELECT ARRAY(SELECT i.indkey[k] FROM generate_series(0, i.indnkeyatts - 1) k)
          FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident) AS ri,
       (SELECT i.indisprimary FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident) AS ri_is_pk,
       (pr.prrelid IS NOT NULL OR EXISTS (
          SELECT 1 FROM pg_publication_rel pa
          WHERE pa.prpubid IN (SELECT oid FROM pub)
            AND pa.prrelid IN (SELECT relid FROM pg_partition_ancestors(c.oid)))) AS published,
       pr.prattrs IS NOT NULL AS has_cols, pr.prqual IS NOT NULL AS has_filter,
       (SELECT NOT i.indimmediate FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) AS pk_deferrable
FROM rels JOIN pg_class c ON c.oid = rels.oid JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_publication_rel pr ON pr.prrelid = c.oid AND pr.prpubid IN (SELECT oid FROM pub)
LEFT JOIN LATERAL (
  SELECT array_agg(attnum ORDER BY attnum) AS attnums, array_agg(attname::text ORDER BY attnum) AS attnames,
         array_agg(atttypid ORDER BY attnum) AS atttypids, array_agg(atttypmod ORDER BY attnum) AS atttypmods,
         array_agg(attisdropped ORDER BY attnum) AS attdropped,
         array_agg(attgenerated::text ORDER BY attnum) AS attgenerated,
         array_agg(atthasdef ORDER BY attnum) AS atthasdef
  FROM pg_attribute WHERE attrelid = c.oid AND attnum > 0) a ON true
WHERE c.relkind IN ('r', 'p')
),
hdr AS MATERIALIZED (
SELECT pg_is_in_recovery() AS rec,
       CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_insert_lsn() END::text AS a,
       pg_current_snapshot()::text AS snap,
       (SELECT to_jsonb(p) FROM pg_publication p WHERE p.pubname = %(pub)s) AS pubjson
)
SELECT hdr.rec, hdr.a, hdr.snap, hdr.pubjson, f.* FROM hdr LEFT JOIN f ON true
"""



def observe(node: PgConnection, names: Iterable[tuple[str, str]] = (), *, publication: str = "cdc_v2_pub",
            deadline: Deadline) -> CatalogObservation:
    """One observation of the catalog fence (H-008): the facts of every table in `names` and every table in our
    publication, with the bracket (b, a). On a standby b and a are pg_last_wal_replay_lsn(), on a primary
    pg_current_wal_insert_lsn(): b is read by a statement that has finished before the facts are read; the facts,
    the publication, pg_current_snapshot() and a are read by one statement, so they share that statement's snapshot
    (the equivalent of one REPEATABLE READ transaction), and a is evaluated after the snapshot was taken.

    Both statements travel in one pipeline: one network round trip. A recovery conflict raises
    `errors.RecoveryConflict` (read again); a failed read is never an answer."""
    import time as _time
    t0 = _time.monotonic()
    pairs = sorted({(s, n) for s, n in names})
    schemas = [p[0] for p in pairs]
    rel = [p[1] for p in pairs]
    where = ("CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_insert_lsn() "
             "END::text")
    try:
        rec_b, b, facts, pubrow, rec_a, a, snap = _observe_pipeline(node, publication, schemas, rel, where,
                                                                    deadline)
    except BaseException:
        _rollback_quietly(node)
        raise
    if rec_b != rec_a or b is None or a is None:
        raise errors.SourceError("catalog.observe: the node changed role during the observation (promotion)")
    obs = _build_observation(lsnmod.parse(b), lsnmod.parse(a), rec_a, snap, facts, pubrow[0] if pubrow else None,
                             pairs)
    object.__setattr__(obs, "elapsed_s", _time.monotonic() - t0)
    return obs


def _rollback_quietly(node: PgConnection) -> None:
    import psycopg
    try:
        if not node.closed and node.raw.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            node.execute("ROLLBACK", deadline=Deadline.after(node.node.cancel_grace_s), what="rollback")
    except Exception:           # noqa: BLE001 - the original error is what the caller needs
        pass


def _observe_pipeline(node, publication, schemas, rel, where, deadline):
    with node.guard(deadline, "catalog.observe"):
        raw = node.raw
        with raw.pipeline() as p:
            c_b = raw.execute("SELECT pg_is_in_recovery(), " + where, prepare=False)
            c_facts = raw.execute(_OBSERVE_SQL, {"pub": publication, "schemas": schemas, "names": rel},
                                  prepare=False)
            p.sync()
            rec_b, b = c_b.fetchone()
            rows = c_facts.fetchall()
    rec_a, a, snap, pub = rows[0][:4]
    facts = [r[4:] for r in rows if r[4] is not None]
    return rec_b, b, facts, (pub,) if pub is not None else None, rec_a, a, snap


def _build_observation(b, a, in_recovery, snap, facts, pub, requested) -> CatalogObservation:
    tables_ = {}
    for (oid, nsp, rel, filenode, replident, relkind, persistence, ispart, attnums, attnames, typids, typmods,
         dropped, gens, hasdefs, pk, ri, ri_is_pk, published, has_cols, has_filter, pk_def) in facts:
        cols = tuple(ColumnFacts(attnum=attnums[i], name=attnames[i], type_oid=typids[i], typmod=typmods[i],
                                 dropped=dropped[i], generated=GENERATED.get(gens[i], gens[i]),
                                 has_default=hasdefs[i]) for i in range(len(attnums or [])))
        tables_[oid] = TableFacts(oid=oid, schema=nsp, name=rel, relfilenode=filenode,
                                  replica_identity=REPLIDENT.get(replident, replident),
                                  relkind=RELKINDS.get(relkind, relkind),
                                  persistence=PERSISTENCE.get(persistence, persistence), is_partition=ispart,
                                  all_columns=cols, pk_attnums=tuple(pk or ()),
                                  replica_identity_index_attnums=tuple(ri) if ri is not None else None,
                                  replica_identity_index_is_key=bool(ri_is_pk), published=published,
                                  publication_has_column_list=has_cols, publication_has_row_filter=has_filter,
                                  pk_deferrable=bool(pk_def))
    by_name = {(t.schema, t.name): t for t in tables_.values()}
    pubfacts = None
    if pub:
        ops = tuple(op for op, col in (("insert", "pubinsert"), ("update", "pubupdate"), ("delete", "pubdelete"),
                                       ("truncate", "pubtruncate")) if pub.get(col))
        gc = pub.get("pubgencols")
        pubfacts = PublicationFacts(oid=int(pub["oid"]), name=pub["pubname"], all_tables=bool(pub["puballtables"]),
                                    publish=ops, via_root=bool(pub.get("pubviaroot")),
                                    generated_columns=gc if isinstance(gc, str) else
                                    ("s" if gc is True else ("n" if gc is False else None)))
    absent = tuple(p for p in requested if p not in by_name)
    return CatalogObservation(lower=b, upper=a, in_recovery=bool(in_recovery), snapshot=snap, tables=tables_,
                              by_name=by_name, absent=absent, publication=pubfacts)


# ----------------------------------------------------------------------------------------------------- diff

@dataclass(frozen=True)
class TableChange:
    oid: int
    schema: str
    name: str
    columns_added: tuple[tuple[int, str], ...] = ()
    columns_dropped: tuple[tuple[int, str], ...] = ()
    columns_renamed: tuple[tuple[int, str, str], ...] = ()               # attnum, old name, new name
    columns_retyped: tuple[tuple[int, str, tuple[int, int], tuple[int, int]], ...] = ()   # (type oid, typmod)
    rewritten: bool = False                                               # relfilenode changed
    key_changed: bool = False
    replica_identity_changed: tuple[str, str] | None = None
    persistence_changed: tuple[str, str] | None = None
    publication_changed: tuple[bool, bool] | None = None

    @property
    def columns_changed(self) -> bool:
        return bool(self.columns_added or self.columns_dropped or self.columns_renamed or self.columns_retyped)


@dataclass(frozen=True)
class CatalogDiff:
    appeared: tuple = ()          # tables whose OID and name are new
    disappeared: tuple = ()       # tables whose OID is gone and whose name is gone too
    recreated: tuple = ()         # (old, new): the same name, another OID, the old OID gone (drop and recreate)
    renamed: tuple = ()           # (old, new): the same OID, another schema or name
    changed: tuple = ()           # TableChange per OID present on both sides

    @property
    def empty(self) -> bool:
        return not (self.appeared or self.disappeared or self.recreated or self.renamed or self.changed)


def _as_map(x) -> dict:
    if isinstance(x, CatalogObservation):
        return dict(x.tables)
    if isinstance(x, Mapping):
        return dict(x)
    return {t.oid: t for t in x}


def _live_cols(t) -> dict[int, tuple]:
    return {c.attnum: (c.name, c.type_oid, c.typmod) for c in t.columns}


def _published(t) -> bool:
    return bool(getattr(t, "published", False))


def _pk_deferrable(t) -> bool:
    return bool(getattr(t, "primary_key_deferrable", getattr(t, "pk_deferrable", False)))


def diff(old, new) -> CatalogDiff:
    """What changed between two reads: two CatalogObservations, two lists of SourceTable, or mappings OID -> value.
    Column identity is the attnum: a rename keeps it, a drop and add allocates a new one (H-008)."""
    o, n = _as_map(old), _as_map(new)
    o_names = {(t.schema, t.name): t for t in o.values()}
    n_names = {(t.schema, t.name): t for t in n.values()}
    appeared, disappeared, recreated, renamed, changed = [], [], [], [], []
    for oid, t in n.items():
        if oid in o:
            continue
        prev = o_names.get((t.schema, t.name))
        if prev is not None and prev.oid not in n:
            recreated.append((prev, t))
        else:
            appeared.append(t)
    recreated_old = {p.oid for p, _ in recreated}
    for oid, t in o.items():
        if oid not in n and oid not in recreated_old:
            disappeared.append(t)
    for oid in sorted(set(o) & set(n)):
        a, b = o[oid], n[oid]
        if (a.schema, a.name) != (b.schema, b.name):
            renamed.append((a, b))
        ca, cb = _live_cols(a), _live_cols(b)
        added = tuple((k, cb[k][0]) for k in sorted(set(cb) - set(ca)))
        dropped = tuple((k, ca[k][0]) for k in sorted(set(ca) - set(cb)))
        ren = tuple((k, ca[k][0], cb[k][0]) for k in sorted(set(ca) & set(cb)) if ca[k][0] != cb[k][0])
        rety = tuple((k, cb[k][0], ca[k][1:], cb[k][1:]) for k in sorted(set(ca) & set(cb)) if ca[k][1:] != cb[k][1:])
        ch = TableChange(
            oid=oid, schema=b.schema, name=b.name, columns_added=added, columns_dropped=dropped, columns_renamed=ren,
            columns_retyped=rety, rewritten=a.relfilenode != b.relfilenode,
            key_changed=(tuple(a.key_attnums) != tuple(b.key_attnums)
                         or _pk_deferrable(a) != _pk_deferrable(b)),
            replica_identity_changed=(a.replica_identity, b.replica_identity)
            if a.replica_identity != b.replica_identity else None,
            persistence_changed=(a.persistence, b.persistence) if a.persistence != b.persistence else None,
            publication_changed=(_published(a), _published(b)) if _published(a) != _published(b) else None)
        if ch != TableChange(oid=oid, schema=b.schema, name=b.name):
            changed.append(ch)
    key = lambda t: (t.schema, t.name, t.oid)              # noqa: E731
    return CatalogDiff(appeared=tuple(sorted(appeared, key=key)), disappeared=tuple(sorted(disappeared, key=key)),
                       recreated=tuple(sorted(recreated, key=lambda p: key(p[1]))),
                       renamed=tuple(sorted(renamed, key=lambda p: key(p[1]))), changed=tuple(changed))
