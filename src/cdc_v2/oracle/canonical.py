"""Canonical forms: what "equal" means for one value, per Postgres type family.

The oracle compares a Postgres value with a MotherDuck value by turning each into a canonical text on its own server,
with SQL that this module builds for both dialects. Two values are equal when their canonical texts are equal.
The forms are defined by the oracle alone and never by the pipeline's conversion code (there must be no shared code,
so that a pipeline bug cannot hide itself). `docs/oracle.md` lists every form as a table.

A type family is one `TypeRule` in `RULES`. A rule knows:

* which Postgres types it covers (base names as `format_type(oid, NULL)` prints them),
* `pg(expr, ctx)`: SQL that turns a non-NULL Postgres value into its canonical text,
* `md(expr, md_type)`: SQL that turns a non-NULL MotherDuck value of the given destination type into the same text,
  or `None` when the rule does not accept that destination type (the value then gets a marker that never matches),
* `py(value)`: the reference implementation in Python. Unit tests check both SQL builders against it.

Adding a type means adding one `TypeRule` to `RULES`. Arrays of any registered scalar type need no entry: they are
built from the element's rule. A destination `UNION(...)` column needs no entry either: each member is canonicalised
by the source family's rule for that member's type. A Postgres type without a rule falls back to `FALLBACK`
(Postgres text output compared with the destination's text), and the comparison result lists the fallback.

NULL is not a value here: callers wrap every column with `token()`, which encodes NULL and the length of the text,
so that concatenated columns cannot run into each other and NULL never equals the empty string.
"""
from __future__ import annotations

import datetime as _dt
import decimal
import math
import re
import struct
import uuid as _uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

PG, MD = "pg", "md"

# ------------------------------------------------------------------------------------------------ destination types


@dataclass(frozen=True)
class MdType:
    """A parsed DuckDB type string, e.g. 'DECIMAL(38,10)', 'INTEGER[]', 'UNION(n DECIMAL(38,10), s VARCHAR)'."""
    text: str
    base: str                                   # 'DECIMAL', 'LIST', 'UNION', 'VARCHAR', 'TIMESTAMP WITH TIME ZONE', ...
    args: tuple = ()                            # DECIMAL (p, s), VARCHAR (n) ...
    elem: Optional["MdType"] = None             # LIST element
    members: tuple = ()                         # UNION ((name, MdType), ...)

    @property
    def scale(self) -> int:
        return int(self.args[1]) if self.base == "DECIMAL" and len(self.args) == 2 else 0


def _split_top(s: str, sep: str = ",") -> list[str]:
    out, depth, cur, quote = [], 0, [], False
    for ch in s:
        if ch == '"':
            quote = not quote
        elif not quote and ch == "(":
            depth += 1
        elif not quote and ch == ")":
            depth -= 1
        if ch == sep and depth == 0 and not quote:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


def parse_md_type(text: str) -> MdType:
    t = " ".join(text.strip().split())
    if t.endswith("[]"):
        return MdType(t, "LIST", elem=parse_md_type(t[:-2]))
    m = re.fullmatch(r"([A-Za-z_ ]+?)\s*\((.*)\)", t)
    if m:
        base = m.group(1).strip().upper()
        inner = m.group(2)
        if base == "UNION":
            members = []
            for part in _split_top(inner):
                if part.startswith('"'):
                    end = part.index('"', 1)
                    name, rest = part[1:end], part[end + 1:]
                else:
                    name, _, rest = part.partition(" ")
                members.append((name, parse_md_type(rest)))
            return MdType(t, "UNION", members=tuple(members))
        if base in ("STRUCT", "MAP"):
            return MdType(t, base, args=tuple(_split_top(inner)))
        return MdType(t, base, args=tuple(a.strip() for a in inner.split(",")))
    up = t.upper()
    aliases = {"INT": "INTEGER", "INT4": "INTEGER", "INT8": "BIGINT", "INT2": "SMALLINT", "LONG": "BIGINT",
               "FLOAT4": "FLOAT", "REAL": "FLOAT", "FLOAT8": "DOUBLE", "STRING": "VARCHAR", "TEXT": "VARCHAR",
               "BYTEA": "BLOB", "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE", "BOOL": "BOOLEAN", "NUMERIC": "DECIMAL"}
    return MdType(t, aliases.get(up, up))


INT_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
             "UHUGEINT"}
TEXT_TYPES = {"VARCHAR", "JSON"}

# ------------------------------------------------------------------------------------------------ SQL helpers


def pg_text(x: str) -> str:
    return "CAST(%s AS text)" % x


def md_text(x: str) -> str:
    return "CAST(%s AS VARCHAR)" % x


def _trim_zeros(s: str) -> str:
    """SQL (both dialects): drop trailing zeros after a decimal point, and the point if nothing follows."""
    return "(CASE WHEN strpos({s}, '.') > 0 THEN rtrim(rtrim({s}, '0'), '.') ELSE {s} END)".format(s=s)


def _md_float(x: str) -> str:
    """SQL (DuckDB): a DOUBLE value -> canonical float text (IEEE-754 bits as 16 hex digits; NaN, ±Infinity by
    name). `CAST(DOUBLE AS BIT)` gives the bits; the BIGINT of them, in hex, is the same as Postgres's float8send."""
    return ("(CASE WHEN isnan({x}) THEN 'NaN' WHEN isinf({x}) THEN (CASE WHEN {x} > 0 THEN 'Infinity' ELSE "
            "'-Infinity' END) ELSE lower(lpad(hex(CAST(CAST({x} AS BIT) AS BIGINT)), 16, '0')) END)").format(x=x)


def _pg_float(x: str) -> str:
    """SQL (Postgres): a float8 value -> canonical float text (see _md_float)."""
    return ("(CASE WHEN {x} = 'NaN' THEN 'NaN' WHEN {x} = 'Infinity' THEN 'Infinity' WHEN {x} = '-Infinity' THEN "
            "'-Infinity' ELSE encode(float8send({x}), 'hex') END)").format(x=x)


def _never(md_type: MdType, x: str) -> str:
    """A text that never equals a canonical form: the destination type is not accepted for this family."""
    return "('!%s:' || %s)" % (md_type.text.replace("'", ""), md_text(x))


# ------------------------------------------------------------------------------------------------ Python references

EPOCH_DATE = _dt.date(1970, 1, 1)
EPOCH_TS = _dt.datetime(1970, 1, 1)
US = _dt.timedelta(microseconds=1)


def py_int(v) -> str:
    return str(int(v))


def py_bool(v) -> str:
    return "t" if v else "f"


def py_numeric(v) -> str:
    d = decimal.Decimal(v) if not isinstance(v, decimal.Decimal) else v
    if d.is_nan():
        return "NaN"
    if d.is_infinite():
        return "-Infinity" if d < 0 else "Infinity"
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


def _special_float(v: float) -> Optional[str]:
    if math.isnan(v):
        return "NaN"
    if math.isinf(v):
        return "Infinity" if v > 0 else "-Infinity"
    return None


def py_float8(v) -> str:
    v = float(v)
    return _special_float(v) or struct.pack(">d", v).hex()


def show_float(canon: str) -> str:
    """Readable form of a canonical float text, for samples: '3ff8000000000000' -> '1.5 (3ff8000000000000)'."""
    if not isinstance(canon, str) or len(canon) != 16:
        return canon
    try:
        return "%r (%s)" % (struct.unpack(">d", bytes.fromhex(canon))[0], canon)
    except ValueError:
        return canon


def float4_value(v: float) -> float:
    """The float32 value nearest to v, as a Python float (what a REAL column stores)."""
    return struct.unpack("f", struct.pack("f", v))[0]


def float4_repr(v: float) -> str:
    """Shortest decimal text that reads back as the same float32 (what Postgres prints for a real; DuckDB's FLOAT
    text is not always this shortest form)."""
    f = float4_value(v)
    for p in range(1, 10):
        s = "%.*g" % (p, f)
        if float4_value(float(s)) == f:
            return s
    return repr(f)


def py_float4(v) -> str:
    """A real is canonicalised as its exact double-precision value (float32 -> float64 is exact on both servers)."""
    return py_float8(float4_value(float(v)))


def py_text(v) -> str:
    return str(v)


def py_char(v) -> str:
    return str(v).rstrip(" ")


def py_bytea(v) -> str:
    return bytes(v).hex()


def _py_inf(v) -> Optional[str]:
    if isinstance(v, str) and v in ("infinity", "-infinity"):
        return v
    return None


def py_date(v) -> str:
    return _py_inf(v) or str((v - EPOCH_DATE).days)


def py_timestamp(v) -> str:
    if _py_inf(v):
        return v
    if v.tzinfo is not None:
        v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return str((v - EPOCH_TS) // US)


def py_time(v) -> str:
    if isinstance(v, _dt.timedelta):          # Postgres 24:00:00
        return str(v // US)
    return str(((v.hour * 60 + v.minute) * 60 + v.second) * 1_000_000 + v.microsecond)


def py_interval(v) -> str:
    """v: (months, days, microseconds) or a timedelta (no months), or 'infinity' / '-infinity'."""
    if _py_inf(v):
        return v
    if isinstance(v, _dt.timedelta):
        return "0:%d:%d" % (v.days, (v - _dt.timedelta(days=v.days)) // US)
    months, days, micros = v
    return "%d:%d:%d" % (months, days, micros)


def py_uuid(v) -> str:
    return str(_uuid.UUID(str(v)))


def py_json(v) -> str:
    return v if isinstance(v, str) else __import__("json").dumps(v)


# ------------------------------------------------------------------------------------------------ rules


@dataclass(frozen=True)
class Ctx:
    """Facts about the source server that a rule may need."""
    pg_version_num: int = 180000


@dataclass(frozen=True)
class TypeRule:
    family: str
    pg_types: tuple
    pg: Callable[[str, Ctx], str]
    md: Callable[[str, MdType], Optional[str]]
    py: Callable[[Any], str]
    form: str                                   # one line for docs/oracle.md
    md_accepts: str = ""                        # one line for docs/oracle.md
    show: Optional[Callable[[str], str]] = None  # readable form of a canonical text, for samples

    def md_or_never(self, x: str, t: MdType) -> str:
        if t.base == "UNION":
            parts = " ".join("WHEN '%s' THEN %s" % (name.replace("'", "''"),
                                                     self.md_or_never("union_extract(%s, '%s')" % (x, name), mt))
                             for name, mt in t.members)
            return "(CASE union_tag(%s) %s END)" % (x, parts)
        out = self.md(x, t)
        return out if out is not None else _never(t, x)


def _md_int(x, t):
    if t.base in INT_TYPES or (t.base == "DECIMAL" and t.scale == 0):
        return md_text(x)
    if t.base == "VARCHAR":
        return x
    return None


def _md_numeric(x, t):
    if t.base in INT_TYPES:
        return md_text(x)
    if t.base == "DECIMAL":
        return _trim_zeros(md_text(x))
    if t.base == "VARCHAR":
        return "(CASE WHEN %s = '-0' THEN '0' ELSE %s END)" % (_trim_zeros(x), _trim_zeros(x))
    return None


def _md_float8(x, t):
    if t.base == "DOUBLE":
        return _md_float(x)
    if t.base in INT_TYPES or t.base in ("DECIMAL", "FLOAT"):
        return _md_float("CAST(%s AS DOUBLE)" % x)
    return None


def _md_float4(x, t):
    if t.base == "FLOAT":
        return _md_float("CAST(%s AS DOUBLE)" % x)
    if t.base == "DOUBLE":
        # accepted only when the double holds exactly a float32 value
        return "(CASE WHEN isnan({x}) OR CAST(CAST({x} AS FLOAT) AS DOUBLE) = {x} THEN {c} ELSE {n} END)".format(
            x=x, c=_md_float(x), n=_never(t, x))
    if t.base in INT_TYPES or t.base == "DECIMAL":
        return _md_float("CAST(CAST(%s AS FLOAT) AS DOUBLE)" % x)
    return None


def _md_bool(x, t):
    return "(CASE WHEN %s THEN 't' ELSE 'f' END)" % x if t.base == "BOOLEAN" else None


def _md_text(x, t):
    return md_text(x) if t.base in TEXT_TYPES else None


def _md_char(x, t):
    return "rtrim(%s, ' ')" % md_text(x) if t.base in TEXT_TYPES else None


def _md_bytea(x, t):
    return "lower(hex(%s))" % x if t.base == "BLOB" else None


def _inf_case(finite_sql: str, x: str, epoch_literal: str) -> str:
    return ("(CASE WHEN isfinite({x}) THEN {f} WHEN {x} > {e} THEN 'infinity' ELSE '-infinity' END)"
            .format(x=x, f=finite_sql, e=epoch_literal))


def _pg_date(x, ctx):
    return _inf_case(pg_text("(%s - DATE '1970-01-01')" % x), x, "DATE '1970-01-01'")


def _md_date(x, t):
    if t.base != "DATE":
        return None
    return _inf_case(md_text("(%s - DATE '1970-01-01')" % x), x, "DATE '1970-01-01'")


def _pg_epoch_us(x):
    return pg_text("CAST(extract(epoch from %s) * 1000000 AS bigint)" % x)


def _pg_ts(x, ctx):
    return _inf_case(_pg_epoch_us(x), x, "'epoch'")


def _md_ts(x, t):
    if t.base != "TIMESTAMP":
        return None
    return _inf_case(md_text("epoch_us(%s)" % x), x, "TIMESTAMP '1970-01-01'")


def _md_tstz(x, t):
    if t.base != "TIMESTAMP WITH TIME ZONE":
        return None
    return _inf_case(md_text("epoch_us(%s)" % x), x, "TIMESTAMPTZ '1970-01-01 00:00:00+00'")


def _md_time(x, t):
    return md_text("epoch_us(%s)" % x) if t.base == "TIME" else None


def _interval_parts(part: Callable[[str], str], cast: Callable[[str], str]) -> str:
    """months, days and microseconds of an interval, from a dialect's field extractor part('year') etc."""
    months = cast("CAST(%s * 12 + %s AS bigint)" % (part("year"), part("month")))
    days = cast("CAST(%s AS bigint)" % part("day"))
    micros = cast("CAST((%s * 60 + %s) * 60000000 + %s AS bigint)" % (part("hour"), part("minute"),
                                                                       part("microseconds")))
    return "(%s || ':' || %s || ':' || %s)" % (months, days, micros)


def _pg_interval(x, ctx):
    finite = _interval_parts(lambda p: "extract(%s from %s)" % (p, x), pg_text)
    if ctx.pg_version_num >= 170000:            # infinite intervals exist from Postgres 17 on
        return "(CASE WHEN isfinite({x}) THEN {f} WHEN {x} > INTERVAL '0' THEN 'infinity' ELSE '-infinity' END)" \
            .format(x=x, f=finite)
    return finite


def _md_interval(x, t):
    if t.base != "INTERVAL":
        return None
    return _interval_parts(lambda p: "datepart('%s', %s)" % (p, x), md_text)


def _md_uuid(x, t):
    if t.base == "UUID":
        return md_text(x)
    if t.base == "VARCHAR":
        return "lower(%s)" % x
    return None


RULES: list[TypeRule] = [
    TypeRule("integer", ("smallint", "integer", "bigint"),
             lambda x, c: pg_text(x), _md_int, py_int,
             "decimal digits with an optional minus sign: `-42`",
             "integer types, `DECIMAL(p,0)`, `VARCHAR` (text must already be canonical)"),
    TypeRule("boolean", ("boolean",),
             lambda x, c: "(CASE WHEN %s THEN 't' ELSE 'f' END)" % x, _md_bool, py_bool,
             "`t` or `f`", "`BOOLEAN`"),
    TypeRule("numeric", ("numeric",),
             lambda x, c: pg_text("trim_scale(%s)" % x), _md_numeric, py_numeric,
             "plain decimal, trailing fractional zeros removed (`1.50` -> `1.5`, `100.00` -> `100`); `NaN`, "
             "`Infinity`, `-Infinity`. Value equality: the display scale of a Postgres value is not compared",
             "integer types, `DECIMAL(p,s)`, `VARCHAR` (for the text member of `UNION(DECIMAL, VARCHAR)`)"),
    TypeRule("double precision", ("double precision",),
             lambda x, c: _pg_float(x), _md_float8, py_float8,
             "the IEEE-754 bits as 16 lowercase hex digits (`1.5` -> `3ff8000000000000`, `-0` -> "
             "`8000000000000000`); `NaN` (any payload), `Infinity`, `-Infinity`",
             "`DOUBLE`; integer types, `DECIMAL`, `FLOAT` (converted to `DOUBLE`)", show_float),
    TypeRule("real", ("real",),
             lambda x, c: _pg_float("CAST(%s AS float8)" % x), _md_float4, py_float4,
             "the value converted exactly to double precision, then as double precision "
             "(`0.1::real` -> `3fb99999a0000000`)",
             "`FLOAT`; `DOUBLE` only when the value is exactly a float32 value; integer types, `DECIMAL`", show_float),
    TypeRule("text", ("text", "character varying", "name", "citext"),
             lambda x, c: pg_text(x), _md_text, py_text,
             "the text itself (an empty string is not NULL)", "`VARCHAR`, `JSON`"),
    TypeRule("character", ("character", "bpchar"),
             lambda x, c: pg_text(x), _md_char, py_char,
             "the text without trailing spaces (Postgres `char(n)` semantics)", "`VARCHAR` (trailing spaces removed)"),
    TypeRule("bytea", ("bytea",),
             lambda x, c: "encode(%s, 'hex')" % x, _md_bytea, py_bytea,
             "lowercase hex of the bytes, no prefix", "`BLOB`"),
    TypeRule("date", ("date",),
             _pg_date, _md_date, py_date,
             "days since 1970-01-01 (`19724`); `infinity`, `-infinity`", "`DATE`"),
    TypeRule("timestamp", ("timestamp without time zone",),
             _pg_ts, _md_ts, py_timestamp,
             "microseconds since 1970-01-01 00:00:00, read as wall-clock time; `infinity`, `-infinity`",
             "`TIMESTAMP`"),
    TypeRule("timestamptz", ("timestamp with time zone",),
             _pg_ts, _md_tstz, py_timestamp,
             "microseconds since 1970-01-01 00:00:00 UTC (the instant; the session time zone plays no part); "
             "`infinity`, `-infinity`", "`TIMESTAMP WITH TIME ZONE`"),
    TypeRule("time", ("time without time zone",),
             lambda x, c: _pg_epoch_us(x), _md_time, py_time,
             "microseconds since midnight (`24:00:00` -> `86400000000`)", "`TIME`"),
    TypeRule("interval", ("interval",),
             _pg_interval, _md_interval, py_interval,
             "`<months>:<days>:<microseconds>`, each field signed and kept apart (`1 mon` is not `30 days`); "
             "`infinity`, `-infinity` (Postgres 17+)", "`INTERVAL`"),
    TypeRule("uuid", ("uuid",),
             lambda x, c: pg_text(x), _md_uuid, py_uuid,
             "lowercase hyphenated form", "`UUID`, `VARCHAR` (lowercased)"),
    TypeRule("json", ("json",),
             lambda x, c: pg_text(x), _md_text, py_json,
             "the exact text Postgres stores (whitespace, key order and duplicate keys included)",
             "`JSON`, `VARCHAR`"),
    TypeRule("jsonb", ("jsonb",),
             lambda x, c: pg_text(x), _md_text, py_json,
             "Postgres's jsonb output text (keys sorted and de-duplicated by Postgres)", "`JSON`, `VARCHAR`"),
]

FALLBACK = TypeRule("fallback (text)", (),
                    lambda x, c: pg_text(x), lambda x, t: md_text(x), py_text,
                    "Postgres text output, compared with the destination value cast to `VARCHAR`",
                    "any type (cast to `VARCHAR`)")

_BY_PG = {name: rule for rule in RULES for name in rule.pg_types}


def rule_for(pg_base: str) -> TypeRule:
    return _BY_PG.get(pg_base, FALLBACK)


# ------------------------------------------------------------------------------------------------ columns and arrays


@dataclass(frozen=True)
class PgColumnType:
    """A source column type: base name without modifiers, and for arrays the element's base name."""
    base: str                       # e.g. 'numeric', 'integer[]'
    elem: Optional[str] = None      # e.g. 'integer' for 'integer[]'

    @property
    def is_array(self) -> bool:
        return self.elem is not None

    @property
    def rule(self) -> TypeRule:
        return rule_for(self.elem if self.is_array else self.base)

    @property
    def family(self) -> str:
        return ("%s[]" % self.rule.family) if self.is_array else self.rule.family


def _token_pg(x: str, null_test: Optional[str] = None) -> str:
    """'N' when null_test (default: x) is NULL, else 'V<bytes of x>:<x>'."""
    return "(CASE WHEN {n} IS NULL THEN 'N' ELSE 'V' || CAST(octet_length({x}) AS text) || ':' || {x} END)".format(
        x=x, n=null_test or x)


def _token_md(x: str, null_test: Optional[str] = None) -> str:
    return "(CASE WHEN {n} IS NULL THEN 'N' ELSE 'V' || CAST(strlen({x}) AS VARCHAR) || ':' || {x} END)".format(
        x=x, n=null_test or x)


def pg_canon(col_sql: str, t: PgColumnType, ctx: Ctx = Ctx()) -> str:
    """SQL (Postgres): canonical text of a non-NULL value of column type t (NULL stays NULL)."""
    rule = t.rule
    if not t.is_array:
        return "(CASE WHEN %s IS NULL THEN NULL ELSE coalesce(%s, '!null-pg') END)" % (col_sql, rule.pg(col_sql, ctx))
    e = "_e"
    elem = "coalesce(%s, '!null-pg')" % rule.pg(e, ctx)
    body = ("coalesce(array_dims({c}), '') || '{{' || array_to_string(ARRAY(SELECT {tok} FROM unnest({c}) "
            "WITH ORDINALITY AS _u({e}, _i) ORDER BY _i), ',') || '}}'").format(c=col_sql, tok=_token_pg(elem, e), e=e)
    return "(CASE WHEN %s IS NULL THEN NULL ELSE %s END)" % (col_sql, body)


def md_canon(col_sql: str, t: PgColumnType, md_type: MdType, depth: int = 0) -> str:
    """SQL (DuckDB): canonical text of a value of destination type md_type, for a source column of type t."""
    if not t.is_array:
        return "(CASE WHEN %s IS NULL THEN NULL ELSE coalesce(%s, '!null-md') END)" % (
            col_sql, t.rule.md_or_never(col_sql, md_type))
    if md_type.base != "LIST":
        return "(CASE WHEN %s IS NULL THEN NULL ELSE %s END)" % (col_sql, _never(md_type, col_sql))
    # depth of nesting: INTEGER[][] -> 2. Postgres arrays are rectangular with lower bound 1 here.
    dims, leaf, flat = [], md_type, col_sql
    probe = col_sql
    while leaf.base == "LIST":
        dims.append("'[1:' || CAST(len(%s) AS VARCHAR) || ']'" % probe)
        probe = "%s[1]" % probe
        leaf = leaf.elem
    for _ in range(len(dims) - 1):
        flat = "flatten(%s)" % flat
    v = "_x%d" % depth
    elem = "coalesce(%s, '!null-md')" % t.rule.md_or_never(v, leaf)
    dims_sql = " || ".join(dims)
    body = ("(CASE WHEN len({c}) = 0 THEN '' ELSE {dims} END) || '{{' || array_to_string(list_transform({flat}, "
            "lambda {v}: {tok}), ',') || '}}'").format(c=col_sql, dims=dims_sql, flat=flat, v=v, tok=_token_md(elem, v))
    return "(CASE WHEN %s IS NULL THEN NULL ELSE %s END)" % (col_sql, body)


def py_canon(value, t: PgColumnType) -> Optional[str]:
    """Python reference of pg_canon / md_canon. Arrays: nested Python lists (rectangular, lower bound 1)."""
    if value is None:
        return None
    if not t.is_array:
        return t.rule.py(value)
    dims, probe = [], value
    while isinstance(probe, list):
        dims.append(len(probe))
        probe = probe[0] if probe else None
    flat = value
    for _ in range(len(dims) - 1):
        flat = [x for sub in flat for x in sub]
    toks = []
    for v in flat:
        if v is None:
            toks.append("N")
        else:
            c = t.rule.py(v)
            toks.append("V%d:%s" % (len(c.encode()), c))
    dims_s = "" if not value else "".join("[1:%d]" % d for d in dims)
    return dims_s + "{" + ",".join(toks) + "}"


def token_pg(canon_sql: str) -> str:
    """SQL (Postgres): column token from a canonical text expression (NULL -> 'N', else 'V<bytes>:<text>')."""
    return _token_pg(canon_sql)


def token_md(canon_sql: str) -> str:
    return _token_md(canon_sql)


def py_token(canon: Optional[str]) -> str:
    return "N" if canon is None else "V%d:%s" % (len(canon.encode()), canon)


def hash60_pg(text_sql: str) -> str:
    """SQL (Postgres): the first 60 bits of md5(text) as a non-negative bigint."""
    return "CAST(CAST('x' || substr(md5(%s), 1, 15) AS bit(60)) AS bigint)" % text_sql


def hash60_md(text_sql: str) -> str:
    return "CAST('0x' || substr(md5(%s), 1, 15) AS BIGINT)" % text_sql


def py_hash60(text: str) -> int:
    import hashlib
    return int(hashlib.md5(text.encode()).hexdigest()[:15], 16)


def quote_ident(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


@dataclass
class RuleDoc:
    family: str
    pg_types: str
    form: str
    md_accepts: str
    examples: list = field(default_factory=list)


def rule_table() -> list[RuleDoc]:
    """The rows of the table in docs/oracle.md (a unit test keeps the document in step with RULES)."""
    return [RuleDoc(r.family, ", ".join(r.pg_types) or "any other", r.form, r.md_accepts) for r in RULES + [FALLBACK]]
