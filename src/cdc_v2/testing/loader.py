"""A simple direct loader from a Postgres source database to a MotherDuck test database. Tests only.

It exists so that oracle tests have a destination that holds the same data as the source without any pipeline. It
is deliberately plain and separate from the oracle's canonical forms: each column is exported from Postgres as text
(or as a JSON array of texts for arrays), written to a local newline-delimited JSON file, and read on the destination
with `read_json` and one cast per column. All rows of a table arrive in one INSERT.

Type mapping (test-only; the pipeline's mapping is a P4 deliverable): see `md_type()`. Unconstrained `numeric` is
`UNION(n DECIMAL(38,10), s VARCHAR)` by default (`numeric_mode="union"`), or `DECIMAL(38,10)` (`"decimal"`).
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Iterable, Optional

from cdc_v2.deadline import Deadline
from cdc_v2.oracle import mdexec
from cdc_v2.oracle.catalog import SourceColumn, SourceTable, read_source
from cdc_v2.oracle.canonical import quote_ident

UNION_NUMERIC = "UNION(n DECIMAL(38,10), s VARCHAR)"
TMP = Path(os.environ.get("CDC_V2_CACHE", Path.home() / ".cache" / "cdc_flight_v2")) / "tmp"

SCALAR = {
    "smallint": "SMALLINT", "integer": "INTEGER", "bigint": "BIGINT", "boolean": "BOOLEAN",
    "real": "FLOAT", "double precision": "DOUBLE", "text": "VARCHAR", "character varying": "VARCHAR",
    "character": "VARCHAR", "bpchar": "VARCHAR", "bytea": "BLOB", "date": "DATE", "time without time zone": "TIME",
    "timestamp without time zone": "TIMESTAMP", "timestamp with time zone": "TIMESTAMPTZ", "interval": "INTERVAL",
    "uuid": "UUID", "json": "JSON", "jsonb": "JSON",
}


def _numeric_type(full: str, numeric_mode: str) -> str:
    m = re.match(r"numeric\((\d+),(\d+)\)", full.replace(" ", ""))
    if m and int(m.group(1)) <= 38:
        return "DECIMAL(%s,%s)" % (m.group(1), m.group(2))
    return UNION_NUMERIC if numeric_mode == "union" else "DECIMAL(38,10)"


def md_type(col: SourceColumn, numeric_mode: str = "union") -> str:
    t = col.type
    base = t.elem if t.is_array else t.base
    full = col.full_type[:-2] if t.is_array and col.full_type.endswith("[]") else col.full_type
    md = _numeric_type(full, numeric_mode) if base == "numeric" else SCALAR.get(base, "VARCHAR")
    return md + "[]" if t.is_array else md


def _pg_export(expr: str, base: str, md: str) -> str:
    """Postgres SQL: a value as text that the matching `_md_import` reads back. NULL stays NULL."""
    return "(CASE WHEN %s IS NULL THEN NULL ELSE %s END)" % (expr, _pg_export_value(expr, base, md))


def _pg_export_value(expr: str, base: str, md: str) -> str:
    if base == "boolean":
        return "(CASE WHEN %s THEN 'true' ELSE 'false' END)" % expr
    if base == "bytea":
        return "encode(%s, 'hex')" % expr
    if base == "interval":
        return ("(extract(year from {e}) * 12 + extract(month from {e}))::bigint::text || '|' || "
                "extract(day from {e})::bigint::text || '|' || ((extract(hour from {e}) * 60 + extract(minute from "
                "{e})) * 60000000 + extract(microseconds from {e}))::bigint::text").format(e=expr)
    if base == "numeric" and md.startswith("UNION"):
        return ("(CASE WHEN {e} IN ('NaN', 'Infinity', '-Infinity') OR scale({e}) > 10 OR abs({e}) >= 1e28 "
                "THEN 's:' ELSE 'n:' END || {e}::text)").format(e=expr)
    return "%s::text" % expr


def _md_import(x: str, base: str, md: str) -> str:
    """DuckDB SQL: read a value exported by `_pg_export`. NULL stays a real NULL of the column type."""
    return "(CASE WHEN %s IS NULL THEN NULL ELSE %s END)" % (x, _md_import_value(x, base, md))


def _md_import_value(x: str, base: str, md: str) -> str:
    if base == "bytea":
        return "unhex(%s)" % x
    if base == "interval":
        return ("(to_months(CAST(split_part({x}, '|', 1) AS INTEGER)) + to_days(CAST(split_part({x}, '|', 2) AS "
                "INTEGER)) + to_microseconds(CAST(split_part({x}, '|', 3) AS BIGINT)))").format(x=x)
    if base == "numeric" and md.startswith("UNION"):
        return ("(CASE WHEN {x} LIKE 'n:%' THEN CAST(union_value(n := CAST(substr({x}, 3) AS DECIMAL(38,10))) AS {u}) "
                "ELSE CAST(union_value(s := substr({x}, 3)) AS {u}) END)").format(x=x, u=md)
    return "CAST(%s AS %s)" % (x, md)


def _export_column(col: SourceColumn, md: str) -> str:
    q = quote_ident(col.name)
    t = col.type
    if not t.is_array:
        return _pg_export(q, t.base, md)
    elem = _pg_export("_e", t.elem, md[:-2])
    return ("(CASE WHEN {q} IS NULL THEN NULL ELSE array_to_json(ARRAY(SELECT {e} FROM unnest({q}) WITH ORDINALITY "
            "AS _u(_e, _i) ORDER BY _i))::text END)".format(e=elem, q=q))


def _import_column(name: str, col: SourceColumn, md: str) -> str:
    x = quote_ident(name)
    t = col.type
    if not t.is_array:
        return _md_import(x, t.base, md)
    return "(CASE WHEN %s IS NULL THEN NULL ELSE list_transform(from_json(%s, '[\"VARCHAR\"]'), lambda _v: %s) END)" % (
        x, x, _md_import("_v", t.elem, md[:-2]))


def load_table(pg_conn, md_conn, table: SourceTable, numeric_mode: str = "union", dest_schema: Optional[str] = None,
               dest_name: Optional[str] = None, tmp_dir: Optional[str] = None,
               statement_seconds: float = 600.0) -> int:
    """Create the destination table (replacing it) and copy every row. Returns the row count."""
    schema, name = dest_schema or table.schema, dest_name or table.name
    types = [md_type(c, numeric_mode) for c in table.columns]
    d = Deadline.after(statement_seconds)
    mdexec.execute(md_conn, "CREATE SCHEMA IF NOT EXISTS %s" % quote_ident(schema), deadline=d, fetch=None)
    mdexec.execute(md_conn, "CREATE OR REPLACE TABLE %s.%s (%s)" % (
        quote_ident(schema), quote_ident(name),
        ", ".join("%s %s" % (quote_ident(c.name), t) for c, t in zip(table.columns, types))), deadline=d, fetch=None)
    keys = ["c%d" % i for i in range(len(table.columns))]
    exports = ", ".join(_export_column(c, t) for c, t in zip(table.columns, types))
    tmp = Path(tmp_dir) if tmp_dir else TMP
    tmp.mkdir(parents=True, exist_ok=True)
    fd, path = tempfile.mkstemp(suffix=".ndjson", dir=str(tmp))
    n = 0
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f, pg_conn.cursor(name="cdc_v2_loader") as cur:
            cur.execute("SELECT %s FROM %s.%s" % (exports, quote_ident(table.schema), quote_ident(table.name)))
            while True:
                rows = cur.fetchmany(5000)
                if not rows:
                    break
                for r in rows:
                    f.write(json.dumps(dict(zip(keys, r)), ensure_ascii=False) + "\n")
                    n += 1
        if n:
            cols = "{%s}" % ", ".join("'%s': 'VARCHAR'" % k for k in keys)
            imports = ", ".join(_import_column(k, c, t) for k, c, t in zip(keys, table.columns, types))
            mdexec.execute(md_conn, "INSERT INTO %s.%s SELECT %s FROM read_json(?, format='newline_delimited', "
                           "columns=%s)" % (quote_ident(schema), quote_ident(name), imports, cols), [path],
                           deadline=Deadline.after(statement_seconds), fetch=None, what="loader insert")
    finally:
        Path(path).unlink(missing_ok=True)
    return n


def load_database(pg_params: dict, md_conn, tables: Optional[Iterable[str]] = None, numeric_mode: str = "union",
                  tmp_dir: Optional[str] = None) -> dict:
    """Copy every user table (or the named 'schema.table's) of a source database. Returns {table: rows}."""
    import psycopg
    out = {}
    params = dict(pg_params)
    params["autocommit"] = False
    with psycopg.connect(**params) as pg:
        pg.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        pg.execute("SET TimeZone = 'UTC'")
        pg.execute("SET DateStyle = 'ISO, YMD'")
        pg.execute("SET extra_float_digits = 1")
        found = read_source(pg)
        wanted = set(tables) if tables is not None else None
        for key, t in sorted(found.items()):
            if wanted is not None and "%s.%s" % key not in wanted:
                continue
            out["%s.%s" % key] = load_table(pg, md_conn, t, numeric_mode, tmp_dir=tmp_dir)
        pg.commit()
    return out
