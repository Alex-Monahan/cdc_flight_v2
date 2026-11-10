"""Catalog values (`cdc_v2.pg.catalog.SourceTable`) built by hand for the hermetic transform tests."""
from __future__ import annotations

from cdc_v2.pg.catalog import Column, SourceTable, TypeInfo

# name -> (oid, typlen, format_type name without modifiers)
PG = {
    "bool": (16, 1, "boolean"), "int2": (21, 2, "smallint"), "int4": (23, 4, "integer"), "int8": (20, 8, "bigint"),
    "float4": (700, 4, "real"), "float8": (701, 8, "double precision"), "numeric": (1700, -1, "numeric"),
    "text": (25, -1, "text"), "varchar": (1043, -1, "character varying"), "bpchar": (1042, -1, "character"),
    "name": (19, 64, "name"), "char": (18, 1, "\"char\""), "bytea": (17, -1, "bytea"), "date": (1082, 4, "date"),
    "timestamp": (1114, 8, "timestamp without time zone"), "timestamptz": (1184, 8, "timestamp with time zone"),
    "time": (1083, 8, "time without time zone"), "timetz": (1266, 12, "time with time zone"),
    "interval": (1186, 16, "interval"), "uuid": (2950, 16, "uuid"), "json": (114, -1, "json"),
    "jsonb": (3802, -1, "jsonb"), "money": (790, 8, "money"), "oid": (26, 4, "oid"), "inet": (869, -1, "inet"),
    "point": (600, 16, "point"), "xml": (142, -1, "xml"), "varbit": (1562, -1, "bit varying"),
}


def pgtype(name: str) -> TypeInfo:
    oid, typlen, _f = PG[name]
    return TypeInfo(oid=oid, schema="pg_catalog", name=name, kind="base", typlen=typlen)


def numeric_typmod(p: int, s: int) -> int:
    return ((p << 16) | (s & 0x7FF)) + 4


def col(attnum, name, typ, typmod=-1, type_name=None, toastable=None):
    """typ: a pg_catalog name, or a TypeInfo."""
    ti = pgtype(typ) if isinstance(typ, str) else typ
    tn = type_name or (PG[typ][2] if isinstance(typ, str) else ti.name)
    if toastable is None:
        toastable = ti.typlen == -1 and not (ti.kind == "domain" and ti.base and ti.base.typlen != -1)
    return Column(attnum=attnum, name=name, type_oid=ti.oid, typmod=typmod, type_name=tn, type=ti, ndims=0,
                  not_null=False, has_default=False, generated="", identity="", storage="x" if toastable else "p",
                  toastable=toastable)


def table(columns, key=(), schema="public", name="t", oid=16384, replica_identity="default"):
    """columns: Column values; key: attnums of the key."""
    return SourceTable(oid=oid, schema=schema, name=name, relfilenode=oid, relkind="ordinary", is_partition=False,
                       partition_root=None, persistence="permanent", rls_enabled=False, rls_forced=False,
                       rls_applies_to_reader=False, columns=tuple(columns),
                       max_attnum=max((c.attnum for c in columns), default=0),
                       key=tuple(c.name for c in columns if c.attnum in key), key_attnums=tuple(key),
                       key_source="primary key" if key else None, has_primary_key=bool(key),
                       replica_identity=replica_identity, replica_identity_index=None,
                       replica_identity_index_is_key=False, published=True, publication_columns=None,
                       publication_row_filter=None)


def one(typ, typmod=-1, name="c", key=False, **kw):
    """A table with one column of the given type (position 0)."""
    return table([col(1, name, typ, typmod, **kw)], key=(1,) if key else ())


def domain(base: str, name="d") -> TypeInfo:
    b = pgtype(base)
    return TypeInfo(oid=90000, schema="public", name=name, kind="domain", base=b, typlen=b.typlen)


def array(elem: str) -> TypeInfo:
    e = pgtype(elem)
    return TypeInfo(oid=91000, schema="pg_catalog", name="_" + elem, kind="array", element=e, typlen=-1)


def enum(labels=("a", "b")) -> TypeInfo:
    return TypeInfo(oid=92000, schema="public", name="mood", kind="enum", enum_labels=tuple(labels), typlen=4)


def composite() -> TypeInfo:
    return TypeInfo(oid=93000, schema="public", name="pair", kind="composite", fields=(("a", pgtype("int4")),),
                    typlen=-1)


def citext() -> TypeInfo:
    return TypeInfo(oid=94000, schema="public", name="citext", kind="base", typlen=-1)
