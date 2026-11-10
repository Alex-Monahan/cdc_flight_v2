"""Read table and column lists from both sides, inside the comparison's snapshot or transaction."""
from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from typing import Optional

from .canonical import MdType, PgColumnType, parse_md_type

PG_SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "pg_toast")
# Destination objects that are not replicated data: the operations schema and the staging schemas
# (docs/design/core.md sections 4 and 6.1), and DuckDB's own catalog schemas.
MD_IGNORED_SCHEMAS = ("_cdc", "_cdc_stage*", "information_schema", "pg_catalog")


@dataclass
class SourceColumn:
    name: str
    type: PgColumnType
    full_type: str


@dataclass
class SourceTable:
    schema: str
    name: str
    oid: int
    kind: str                                   # 'r' table, 'p' partitioned table
    columns: list = field(default_factory=list)
    key: list = field(default_factory=list)     # primary key column names in key order; [] if none
    blocks: int = 0
    est_rows: float = -1

    @property
    def qualified(self) -> str:
        return "%s.%s" % (self.schema, self.name)


@dataclass
class DestColumn:
    name: str
    type: MdType


@dataclass
class DestTable:
    schema: str
    name: str
    columns: list = field(default_factory=list)

    @property
    def qualified(self) -> str:
        return "%s.%s" % (self.schema, self.name)


SOURCE_SQL = """
SELECT n.nspname, c.relname, c.oid::bigint, c.relkind::text, a.attname, a.attnum,
       format_type(a.atttypid, NULL), CASE WHEN t.typcategory = 'A' THEN format_type(t.typelem, NULL) END,
       format_type(a.atttypid, a.atttypmod),
       CASE WHEN c.relkind = 'r' THEN pg_relation_size(c.oid) / current_setting('block_size')::int ELSE 0 END,
       c.reltuples::float8
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_type t ON t.oid = a.atttypid
WHERE (c.relkind = 'p' OR (c.relkind = 'r' AND NOT c.relispartition))
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND n.nspname NOT LIKE 'pg\\_temp\\_%'
  AND n.nspname NOT LIKE 'pg\\_toast\\_temp\\_%'
ORDER BY 1, 2, a.attnum
"""

SOURCE_KEYS_SQL = """
SELECT k.conrelid::bigint, a.attname
FROM pg_constraint k
CROSS JOIN LATERAL unnest(k.conkey) WITH ORDINALITY AS u(attnum, pos)
JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = u.attnum
WHERE k.contype = 'p'
ORDER BY k.conrelid, u.pos
"""

DEST_SQL = """
SELECT c.schema_name, c.table_name, c.column_name, c.column_index, c.data_type
FROM duckdb_columns() c
JOIN duckdb_tables() t ON t.database_name = c.database_name AND t.schema_name = c.schema_name
                       AND t.table_name = c.table_name
WHERE c.database_name = current_database()
ORDER BY 1, 2, 4
"""


def matches(name: str, patterns) -> bool:
    return any(fnmatch.fnmatchcase(name, p) for p in patterns)


def read_source(cur, exclude_schemas=()) -> dict:
    """{(schema, table): SourceTable} for every user table (system schemas and exclude_schemas left out)."""
    tables: dict = {}
    for (schema, rel, oid, kind, att, _num, base, elem, full, blocks, reltuples) in cur.execute(SOURCE_SQL).fetchall():
        if matches(schema, exclude_schemas):
            continue
        t = tables.get((schema, rel))
        if t is None:
            t = tables[(schema, rel)] = SourceTable(schema, rel, oid, kind, blocks=int(blocks or 0),
                                                    est_rows=float(reltuples))
        t.columns.append(SourceColumn(att, PgColumnType(base, elem), full))
    by_oid = {t.oid: t for t in tables.values()}
    for oid, att in cur.execute(SOURCE_KEYS_SQL).fetchall():
        if oid in by_oid:
            by_oid[oid].key.append(att)
    return tables


def read_dest(md_conn, ignore_schemas=MD_IGNORED_SCHEMAS) -> dict:
    tables: dict = {}
    for schema, table, col, _idx, dtype in md_conn.execute(DEST_SQL).fetchall():
        if matches(schema, ignore_schemas):
            continue
        t = tables.setdefault((schema, table), DestTable(schema, table))
        t.columns.append(DestColumn(col, parse_md_type(dtype)))
    return tables


def server_version_num(cur) -> int:
    return int(cur.execute("SHOW server_version_num").fetchone()[0])


def find_column(columns, name) -> Optional[object]:
    for c in columns:
        if c.name == name:
            return c
    return None
