"""Type families: the destination type of a source column, and the conversion of each input form into it.

A *family* groups source types that are mapped the same way (docs/type_mapping.md has the table). For each family
and each direction (`stream`: the engine's transport form, docs/design/engine.md section 3; `backfill`: the text that
Postgres prints under the read sessions' settings, docs/design/source.md section 1) this module gives a `Conv`:

* `pass`  - the input Arrow array already has the destination layout; it is handed on untouched (no copy);
* `view`  - the input's buffers are reinterpreted as the destination type (no copy), after a range check;
* `sql`   - a DuckDB expression over the input column, evaluated in the transformer's local connection.

SQL templates use `{x}` for the quoted input column, `{p}` for the prefix of this column's intermediate names and
`{c}` for the column's name in error markers. Intermediate values are computed in layers (one SELECT per layer), so
that a value parsed once is used many times; intermediates use TRY_CAST and the final expression validates them,
so that an intermediate never fails on a row the final CASE does not take. Two markers are raised with `error()`:
`cdc_v2:misfit:<c>` (a value that the native form cannot hold: the caller widens) and `cdc_v2:bad:<c>` (an input
that breaks the contract). Neither contains a value.
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Optional

import pyarrow as pa

# ------------------------------------------------------------------------------------------------ constants

INT32_MAX, INT32_MIN = 2147483647, -2147483648
INT64_MAX, INT64_MIN = 9223372036854775807, -9223372036854775808
PG_EPOCH_DAYS = 10957                     # 2000-01-01 in days since 1970-01-01
PG_EPOCH_US = 946684800000000             # 2000-01-01 in microseconds since 1970-01-01
DAY_US = 86400000000
#: Largest |microseconds| of an interval that Arrow's month_day_nano can carry (nanoseconds in int64). DuckDB's
#: Arrow export multiplies by 1,000 without a check and wraps silently above it (measured on DuckDB 1.5.6).
ARROW_INTERVAL_US_MAX = INT64_MAX // 1000
#: DuckDB TIMETZ (dtime_tz_t): bits = micros << 24 | (MAX_OFFSET - offset_seconds); offsets up to +-15:59:59.
TIMETZ_MAX_OFFSET = 16 * 3600 - 1
#: Scale of the native DECIMAL(38, s) of an unconstrained numeric (SPEC 2.4). docs/type_mapping.md says why.
UNCONSTRAINED_NUMERIC_SCALE = 10

TIMETZ_ARROW = pa.opaque(pa.binary(8), type_name="time_tz", vendor_name="DuckDB")
UUID_ARROW = pa.uuid()
BOOL8_ARROW = pa.bool8()

#: The Arrow type of every physical transport type (C2) by transport form name.
FORM_ARROW = {
    "bool": pa.bool_(), "int16": pa.int16(), "int32": pa.int32(), "int64": pa.int64(), "float32": pa.float32(),
    "float64": pa.float64(), "numeric_text": pa.string(), "money_text": pa.string(), "bytes": pa.binary(),
    "date_pg_days": pa.int32(), "time_us": pa.int64(), "timestamp_pg_us": pa.int64(),
    "timestamptz_pg_us": pa.int64(), "interval_text": pa.string(), "array_text": pa.string(), "text": pa.string(),
}

# Built-in type OIDs whose transport form is not text (TransportConverter.formFor, docs/design/engine.md 3).
_FORM_BY_PG_NAME = {
    "bool": "bool", "int2": "int16", "int4": "int32", "int8": "int64", "oid": "int64", "float4": "float32",
    "float8": "float64", "numeric": "numeric_text", "money": "money_text", "bytea": "bytes", "date": "date_pg_days",
    "time": "time_us", "timestamp": "timestamp_pg_us", "timestamptz": "timestamptz_pg_us",
    "interval": "interval_text",
}


# ------------------------------------------------------------------------------------------------ type spec


@dataclass(frozen=True)
class TypeSpec:
    """What the plan needs to know about one column's type."""
    family: str                    # boolean, integer, real, double, numeric, text, bytea, date, timestamp,
    #                                timestamptz, time, timetz, interval, uuid, json, enum, fallback
    base_type: str                 # the type that decides the mapping (a domain's base, pg_catalog name)
    native_type: str               # DuckDB type of the native form
    wide_type: Optional[str]       # DuckDB type of the wide form, or None when the family has one form
    stream_forms: tuple            # transport forms the engine may send for it
    numeric: Optional[tuple] = None   # numeric: (width, scale, general) - general: values may not fit
    note: str = ""


def union_of(native: str) -> str:
    return "UNION(n %s, t VARCHAR)" % native


def numeric_params(typmod: int) -> tuple:
    """(width, scale, general) of the native DECIMAL for a numeric with this typmod (-1: unconstrained).

    `general` is True when a value of the source type need not fit (unconstrained, precision above 38, or a scale
    outside what one DECIMAL holds); the fit is then decided per value by its digits. Otherwise every finite value
    of the source type fits and only NaN (and infinities, which a constrained numeric cannot hold) does not."""
    if typmod < 4:
        return 38, UNCONSTRAINED_NUMERIC_SCALE, True
    p = ((typmod - 4) >> 16) & 0xFFFF
    s = (((typmod - 4) & 0x7FF) ^ 1024) - 1024          # NUMERIC_TYPMOD_SCALE (signed since Postgres 15)
    if p <= 38:
        if 0 <= s < p:
            return p, s, False
        if s >= p and s + 1 <= 38:
            # |v| < 10^(p-s) <= 1: all digits after the point. DECIMAL(s+1, s), not DECIMAL(s, s): DuckDB prints
            # DECIMAL(s, s) without the leading zero ('.00123'), which no reader of the destination expects.
            return s + 1, s, False
        if s < 0 and p - s <= 38:
            return p - s, 0, False
    return 38, min(max(s, 0), 37), True


def resolve(column) -> TypeSpec:
    """The TypeSpec of a catalog column (`cdc_v2.pg.catalog.Column`). Never fails: an unknown type is text."""
    t = column.type
    typmod = column.typmod
    if t.kind == "domain":
        # a domain is planned as its base type; the domain's own typmod (pg_type.typtypmod) is not in the catalog
        # value, so a numeric domain is planned as an unconstrained numeric (lossless either way)
        t = t.base if t.base is not None else t
        typmod = -1
    if t.kind == "array":
        return TypeSpec("fallback", "_" + (t.element.name if t.element else "?"), "VARCHAR", None,
                        ("array_text", "text"), note="array: text, P4")
    if t.kind == "enum":
        return TypeSpec("enum", t.name, "VARCHAR", None, ("text",), note="enum: VARCHAR (H-033)")
    if t.kind != "base":
        return TypeSpec("fallback", t.name, "VARCHAR", None, ("text", "array_text"), note="%s: text, P4" % t.kind)
    name = t.name if t.schema == "pg_catalog" else None
    if t.name == "citext":
        return TypeSpec("text", "citext", "VARCHAR", None, ("text",))
    if name == "bool":
        return TypeSpec("boolean", name, "BOOLEAN", None, ("bool",))
    if name in ("int2", "int4", "int8"):
        native = {"int2": "SMALLINT", "int4": "INTEGER", "int8": "BIGINT"}[name]
        return TypeSpec("integer", name, native, None, (_FORM_BY_PG_NAME[name],))
    if name == "float4":
        return TypeSpec("real", name, "FLOAT", None, ("float32",))
    if name == "float8":
        return TypeSpec("double", name, "DOUBLE", None, ("float64",))
    if name == "numeric":
        w, s, general = numeric_params(typmod)
        native = "DECIMAL(%d,%d)" % (w, s)
        return TypeSpec("numeric", name, native, union_of(native), ("numeric_text",), numeric=(w, s, general))
    if name in ("text", "varchar", "bpchar", "name", "char"):
        return TypeSpec("text", name, "VARCHAR", None, ("text",))
    if name == "bytea":
        return TypeSpec("bytea", name, "BLOB", None, ("bytes",))
    if name == "date":
        return TypeSpec("date", name, "DATE", None, ("date_pg_days",))
    if name == "timestamp":
        return TypeSpec("timestamp", name, "TIMESTAMP", union_of("TIMESTAMP"), ("timestamp_pg_us",))
    if name == "timestamptz":
        return TypeSpec("timestamptz", name, "TIMESTAMP WITH TIME ZONE", union_of("TIMESTAMP WITH TIME ZONE"),
                        ("timestamptz_pg_us",))
    if name == "time":
        return TypeSpec("time", name, "TIME", None, ("time_us",))
    if name == "timetz":
        return TypeSpec("timetz", name, "TIME WITH TIME ZONE", None, ("text",))
    if name == "interval":
        return TypeSpec("interval", name, "INTERVAL", union_of("INTERVAL"), ("interval_text",))
    if name == "uuid":
        return TypeSpec("uuid", name, "UUID", None, ("text",))
    if name in ("json", "jsonb"):
        return TypeSpec("json", name, "VARCHAR", None, ("text",), note="json text kept as printed")
    form = _FORM_BY_PG_NAME.get(name) if name else None
    return TypeSpec("fallback", t.name, "VARCHAR", None, (form,) if form else ("text",),
                    note="text, P4")


# ------------------------------------------------------------------------------------------------ Arrow types


def arrow_type(duck_type: str) -> pa.DataType:
    """The Arrow type in which the transform delivers a value of this destination type (DuckDB's own lossless
    Arrow mapping; `arrow_lossless_conversion` is on in the transformer's connection)."""
    t = duck_type
    if t.startswith("UNION("):
        native = t[len("UNION(n "):-len(", t VARCHAR)")]
        return pa.sparse_union([pa.field("n", arrow_type(native)), pa.field("t", pa.string())], type_codes=[0, 1])
    if t.startswith("DECIMAL("):
        w, s = t[len("DECIMAL("):-1].split(",")
        return pa.decimal128(int(w), int(s))
    return {
        "BOOLEAN": pa.bool_(), "SMALLINT": pa.int16(), "INTEGER": pa.int32(), "BIGINT": pa.int64(),
        "FLOAT": pa.float32(), "DOUBLE": pa.float64(), "VARCHAR": pa.string(), "BLOB": pa.binary(),
        "DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"), "TIMESTAMP WITH TIME ZONE": pa.timestamp("us", "UTC"),
        "TIME": pa.time64("us"), "TIME WITH TIME ZONE": TIMETZ_ARROW, "INTERVAL": pa.month_day_nano_interval(),
        "UUID": UUID_ARROW,
    }[t]


# ------------------------------------------------------------------------------------------------ converters


@dataclass(frozen=True)
class Conv:
    kind: str                           # 'pass' | 'view' | 'sql'
    in_type: pa.DataType
    out_type: pa.DataType
    layers: tuple = ()                  # sql: ((name, template), ...) per layer, evaluated in order
    final: str = ""                     # sql: the value
    misfit: Optional[str] = None        # sql predicate: a non-NULL input whose value the native form cannot hold
    sql_type: Optional[pa.DataType] = None   # the Arrow type DuckDB returns, when `post` turns it into out_type
    post: Optional[str] = None          # 'time64' | 'timetz': reinterpret DuckDB's integer result
    view_range: Optional[tuple] = None  # view: (min, max) the input must lie in
    fast: Optional[str] = None          # stream: an exact pyarrow path for the common values ('date', 'ts', 'decimal')


BAD = "error('cdc_v2:bad:{c}')"
MISFIT = "error('cdc_v2:misfit:{c}')"


def _wrap_null(final: str) -> str:
    return "CASE WHEN {x} IS NULL THEN NULL ELSE %s END" % final


def _native_or_wide(spec: TypeSpec, form: str, fits: str, native_value: str, wide_text: str, layers: tuple,
                    in_type, specials: tuple = (), ok: str = "TRUE") -> Conv:
    """A family with two forms. Order of the CASE: NULL; `specials` ((condition, native value), e.g. infinities
    that the native type holds); not `ok` (the input is not well formed: contract broken); fits (the native value);
    otherwise a misfit: in the native form the misfit marker, in the wide form the text member."""
    any_special = " OR ".join("(%s)" % c for c, _v in specials) or "FALSE"
    if form == "native":
        whens = " ".join("WHEN %s THEN %s" % (c, v) for c, v in specials)
        final = ("CASE WHEN {x} IS NULL THEN NULL %s WHEN NOT (%s) THEN %s WHEN %s THEN %s ELSE %s END"
                 % (whens, ok, BAD, fits, native_value, MISFIT))
        misfit = "({x} IS NOT NULL AND NOT (%s) AND (%s) AND NOT (%s))" % (any_special, ok, fits)
        return Conv("sql", in_type, arrow_type(spec.native_type), layers, final, misfit=misfit)
    u = spec.wide_type
    whens = " ".join("WHEN %s THEN CAST(union_value(n := %s) AS %s)" % (c, v, u) for c, v in specials)
    final = ("CASE WHEN {x} IS NULL THEN NULL %s WHEN NOT (%s) THEN CAST(%s AS %s) "
             "WHEN %s THEN CAST(union_value(n := %s) AS %s) ELSE CAST(union_value(t := %s) AS %s) END"
             % (whens, ok, BAD, u, fits, native_value, u, wide_text, u))
    return Conv("sql", in_type, arrow_type(u), layers, final)


# ---- shared SQL pieces -------------------------------------------------------------------------------------------

def _days_from_civil_layers(y: str, m: str, d: str, out: str) -> tuple:
    """Layers that compute days since 1970-01-01 of the proleptic Gregorian date (y, m, d), astronomical year
    numbering, into `{p}<out>` (H. Hinnant, "chrono-compatible low-level date algorithms"; DuckDB's `//` truncates
    like C, which is what the algorithm assumes)."""
    return (
        (("%s_y2" % out, "({y} - CASE WHEN {m} <= 2 THEN 1 ELSE 0 END)".format(y=y, m=m)),
         ("%s_mp" % out, "(({m} + 9) % 12)".format(m=m))),
        (("%s_era" % out, "((CASE WHEN {p}%s_y2 >= 0 THEN {p}%s_y2 ELSE {p}%s_y2 - 399 END) // 400)"
          % (out, out, out)),),
        (("%s_yoe" % out, "({p}%s_y2 - {p}%s_era * 400)" % (out, out)),
         ("%s_doy" % out, "((153 * {p}%s_mp + 2) // 5 + %s - 1)" % (out, d))),
        (("%s_doe" % out, "({p}%s_yoe * 365 + {p}%s_yoe // 4 - {p}%s_yoe // 100 + {p}%s_doy)"
          % (out, out, out, out)),),
        ((out, "({p}%s_era * 146097 + {p}%s_doe - 719468)" % (out, out)),),
    )


def _merge_layers(*groups) -> tuple:
    """Merge layer lists so that layer k of every group is evaluated in layer k of the result."""
    depth = max((len(g) for g in groups), default=0)
    return tuple(tuple(item for g in groups if k < len(g) for item in g[k]) for k in range(depth))


def _shift(layers: tuple, by: int) -> tuple:
    return tuple(() for _ in range(by)) + tuple(layers)


def _valid_date(y: str, m: str, d: str) -> str:
    """y is the astronomical year; m and d must form a real date."""
    leap = "(CASE WHEN ({y} % 4 = 0 AND {y} % 100 <> 0) OR {y} % 400 = 0 THEN 1 ELSE 0 END)".format(y=y)
    dim = ("(CASE WHEN {m} = 2 THEN 28 + {leap} ELSE 30 + (({m} + CASE WHEN {m} >= 8 THEN 1 ELSE 0 END) % 2) END)"
           .format(m=m, leap=leap))
    return ("({m} BETWEEN 1 AND 12 AND {d} BETWEEN 1 AND {dim})".format(m=m, d=d, dim=dim))


def _date_text_layers(src: str, pre: str) -> tuple:
    """Layers that parse 'YYYY-MM-DD' (src, an SQL expression or intermediate name, without ' BC') into
    {p}<pre>y, {p}<pre>m, {p}<pre>d and a format check {p}<pre>ok."""
    return (
        ((pre + "y", "TRY_CAST(split_part(%s, '-', 1) AS BIGINT)" % src),
         (pre + "m", "TRY_CAST(split_part(%s, '-', 2) AS BIGINT)" % src),
         (pre + "d", "TRY_CAST(split_part(%s, '-', 3) AS BIGINT)" % src),
         (pre + "ok", "(strlen(%s) - strlen(replace(%s, '-', '')) = 2 AND strlen(split_part(%s, '-', 1)) >= 4)"
          % (src, src, src))),
    )


def _clock_layers(src: str, pre: str) -> tuple:
    """Layers that parse 'HH:MM:SS[.ffffff]' (src) into {p}<pre>h, mi, s, f (microseconds of the fraction) and
    {p}<pre>ok (format)."""
    return (
        ((pre + "h", "TRY_CAST(split_part(%s, ':', 1) AS BIGINT)" % src),
         (pre + "mi", "TRY_CAST(split_part(%s, ':', 2) AS BIGINT)" % src),
         (pre + "sec", "split_part(%s, ':', 3)" % src),
         (pre + "ok0", "(strlen(%s) - strlen(replace(%s, ':', '')) = 2 AND strlen(split_part(%s, ':', 1)) = 2 "
                       "AND strlen(split_part(%s, ':', 2)) = 2)" % (src, src, src, src))),
        ((pre + "s", "TRY_CAST(split_part({p}%ssec, '.', 1) AS BIGINT)" % pre),
         (pre + "fr", "split_part({p}%ssec, '.', 2)" % pre),
         (pre + "ok1", "(strlen(split_part({p}%ssec, '.', 1)) = 2 AND (strpos({p}%ssec, '.') = 0 OR "
                       "strlen(split_part({p}%ssec, '.', 2)) BETWEEN 1 AND 6))" % (pre, pre, pre))),
        ((pre + "f", "(CASE WHEN {p}%sfr = '' THEN 0 ELSE TRY_CAST(rpad({p}%sfr, 6, '0') AS BIGINT) END)"
          % (pre, pre)),),
    )


def _clock_us(pre: str) -> str:
    return "((({p}%sh * 60 + {p}%smi) * 60 + {p}%ss) * 1000000 + {p}%sf)" % (pre, pre, pre, pre)


def _clock_ok(pre: str, allow_24: bool) -> str:
    base = ("{p}%sok0 AND {p}%sok1 AND {p}%sh IS NOT NULL AND {p}%smi BETWEEN 0 AND 59 AND {p}%ss BETWEEN 0 AND 59 "
            "AND {p}%sf IS NOT NULL" % (pre, pre, pre, pre, pre, pre))
    if allow_24:
        return base + (" AND ({p}%sh BETWEEN 0 AND 23 OR ({p}%sh = 24 AND {p}%smi = 0 AND {p}%ss = 0 AND {p}%sf = 0))"
                       % (pre, pre, pre, pre, pre))
    return base + " AND {p}%sh BETWEEN 0 AND 23" % pre


def _civil_text(days: str) -> tuple:
    """(layers, sql) that print days since 1970-01-01 as Postgres prints a date ('YYYY-MM-DD', ' BC' returned
    separately as the second sql). Inverse of _days_from_civil_layers (Hinnant's civil_from_days)."""
    layers = (
        (("cz", "(%s + 719468)" % days),),
        (("cera", "((CASE WHEN {p}cz >= 0 THEN {p}cz ELSE {p}cz - 146096 END) // 146097)"),),
        (("cdoe", "({p}cz - {p}cera * 146097)"),),
        (("cyoe", "(({p}cdoe - {p}cdoe // 1460 + {p}cdoe // 36524 - {p}cdoe // 146096) // 365)"),),
        (("cdoy", "({p}cdoe - (365 * {p}cyoe + {p}cyoe // 4 - {p}cyoe // 100))"),),
        (("cmp", "((5 * {p}cdoy + 2) // 153)"),),
        (("cd", "({p}cdoy - (153 * {p}cmp + 2) // 5 + 1)"),
         ("cm", "(CASE WHEN {p}cmp < 10 THEN {p}cmp + 3 ELSE {p}cmp - 9 END)")),
        (("cy", "({p}cyoe + {p}cera * 400 + CASE WHEN {p}cm <= 2 THEN 1 ELSE 0 END)"),),
    )
    # lpad cuts a longer text down to the length (DuckDB 1.5.6): years of five or more digits are printed as they are
    text = ("(CASE WHEN (CASE WHEN {p}cy > 0 THEN {p}cy ELSE 1 - {p}cy END) >= 1000 "
            "THEN CAST(CASE WHEN {p}cy > 0 THEN {p}cy ELSE 1 - {p}cy END AS VARCHAR) "
            "ELSE lpad(CAST(CASE WHEN {p}cy > 0 THEN {p}cy ELSE 1 - {p}cy END AS VARCHAR), 4, '0') END || '-' || "
            "lpad(CAST({p}cm AS VARCHAR), 2, '0') || '-' || lpad(CAST({p}cd AS VARCHAR), 2, '0'))")
    bc = "(CASE WHEN {p}cy > 0 THEN '' ELSE ' BC' END)"
    return layers, text, bc


def _frac_text(frac_us: str) -> str:
    """'.ffffff' with trailing zeros removed, or '' (Postgres AppendSeconds)."""
    return ("(CASE WHEN {f} = 0 THEN '' ELSE '.' || rtrim(lpad(CAST({f} AS VARCHAR), 6, '0'), '0') END)"
            .format(f=frac_us))


# ---- per family ----------------------------------------------------------------------------------------------


def conv(spec: TypeSpec, direction: str, form: str, in_type: pa.DataType) -> Conv:
    """The converter of one column. `form` is 'native' or 'wide'; `in_type` the input Arrow type (stream: the
    transport form's physical type)."""
    f = spec.family
    out = arrow_type(spec.wide_type if form == "wide" else spec.native_type)
    stream = direction == "stream"
    if f in ("text", "json", "enum"):
        return Conv("pass", pa.string(), out)
    if f == "fallback":
        return _fallback(spec, direction, in_type)
    if f in ("boolean", "integer", "real", "double"):
        if stream:
            return Conv("pass", out, out)
        return _scalar_text(spec, out)
    if f == "numeric":
        c = _numeric(spec, form)
        w, sc, general = spec.numeric
        if stream and form == "native" and not general and w <= 18:
            c = dataclasses.replace(c, fast="decimal")
        return c
    if f == "bytea":
        if stream:
            return Conv("pass", pa.binary(), out)
        return Conv("sql", pa.string(), out, (),
                    _wrap_null("CASE WHEN starts_with({x}, '\\x') AND strlen({x}) %% 2 = 0 THEN unhex(substr({x}, 3)) "
                               "ELSE %s END" % BAD))
    if f == "date":
        return dataclasses.replace(_date_stream(out), fast="date") if stream else _date_backfill(out)
    if f in ("timestamp", "timestamptz"):
        if stream:
            c = _ts_stream(spec, form)
            return dataclasses.replace(c, fast="ts") if form == "native" else c
        return _ts_backfill(spec, form)
    if f == "time":
        if stream:
            return Conv("view", pa.int64(), out, view_range=(0, DAY_US))
        layers = _clock_layers("{x}", "t")
        return Conv("sql", pa.string(), out, layers,
                    _wrap_null("CASE WHEN %s THEN %s ELSE %s END" % (_clock_ok("t", True), _clock_us("t"), BAD)),
                    sql_type=pa.int64(), post="time64")
    if f == "timetz":
        return _timetz(out)
    if f == "interval":
        return _interval_stream(spec, form) if stream else _interval_backfill(spec, form)
    if f == "uuid":
        return Conv("sql", pa.string(), out, (), _wrap_null("CAST({x} AS UUID)"))
    raise AssertionError("no converter for family %s" % f)            # pragma: no cover


def _fallback(spec: TypeSpec, direction: str, in_type) -> Conv:
    out = pa.string()
    if direction == "backfill" or pa.types.is_string(in_type):
        if direction == "stream" and spec.base_type == "money":
            return _money_stream()
        return Conv("pass", pa.string(), out)
    if pa.types.is_integer(in_type):              # oid: int64 in the stream, its decimal text in Postgres
        return Conv("sql", in_type, out, (), _wrap_null("CAST({x} AS VARCHAR)"))
    raise AssertionError("fallback with transport type %s" % in_type)  # pragma: no cover


def _money_stream() -> Conv:
    """money_text (plain decimal, two digits after the point) -> Postgres money text under lc_monetary=C
    ('$1,234.50', '-$0.01')."""
    layers = (
        (("neg", "starts_with({x}, '-')"),
         ("ip", "split_part(ltrim({x}, '-'), '.', 1)"),
         ("fp", "split_part({x}, '.', 2)")),
        (("iv", "TRY_CAST({p}ip AS BIGINT)"),),
    )
    final = _wrap_null("CASE WHEN {p}iv IS NOT NULL AND strlen({p}fp) <= 2 THEN "
                       "(CASE WHEN {p}neg THEN '-' ELSE '' END) || '$' || format('{{:,}}', {p}iv) || '.' || "
                       "rpad({p}fp, 2, '0') ELSE %s END" % BAD)
    return Conv("sql", pa.string(), pa.string(), layers, final)


def _scalar_text(spec: TypeSpec, out) -> Conv:
    f = spec.family
    if f == "boolean":
        return Conv("sql", pa.string(), out, (),
                    _wrap_null("CASE WHEN {x} = 't' THEN TRUE WHEN {x} = 'f' THEN FALSE ELSE %s END" % BAD))
    if f == "integer":
        # Postgres prints integers canonically; a value whose text does not print back the same was not printed
        # by Postgres (DuckDB's cast would accept '1.5' and round it)
        return Conv("sql", pa.string(), out, ((("v", "TRY_CAST({x} AS %s)" % spec.native_type),),),
                    _wrap_null("CASE WHEN CAST({p}v AS VARCHAR) = {x} THEN {p}v ELSE %s END" % BAD))
    # floats: parsed to binary once (correctly rounded; NaN, Infinity, -Infinity, -0 kept)
    return Conv("sql", pa.string(), out, (), _wrap_null("CAST({x} AS %s)" % spec.native_type))


def _numeric(spec: TypeSpec, form: str) -> Conv:
    """numeric text (Postgres numeric_out and BigDecimal.toPlainString give the same plain text) -> DECIMAL.
    Never through a cast that could round: the fit is decided by the digits first."""
    w, s, general = spec.numeric
    special = "{x} IN ('NaN', 'Infinity', '-Infinity')"
    # digits before and after the point, counted from the text (signs and the point excluded)
    layers = (
        (("dot", "strpos({x}, '.')"), ("len", "strlen({x})"), ("neg", "starts_with({x}, '-')")),
        (("il", "((CASE WHEN {p}dot = 0 THEN {p}len ELSE {p}dot - 1 END) - CASE WHEN {p}neg THEN 1 ELSE 0 END)"),
         ("fl", "(CASE WHEN {p}dot = 0 THEN 0 ELSE {p}len - {p}dot END)")),
    )
    if general:
        # cheap test first; the exact one (leading zeros of the integer part, trailing zeros of the fraction) only
        # for the texts that the cheap one does not settle
        fits = ("(NOT %s AND (CASE WHEN {p}il <= %d AND {p}fl <= %d THEN TRUE ELSE "
                "strlen(ltrim(split_part(ltrim({x}, '-'), '.', 1), '0')) <= %d AND "
                "strlen(rtrim(split_part({x}, '.', 2), '0')) <= %d END))" % (special, w - s, s, w - s, s))
    else:
        fits = "(NOT %s)" % special
    native = "CAST({x} AS DECIMAL(%d,%d))" % (w, s)
    if w > 18 and s <= 17:
        # DuckDB parses text into a DECIMAL wider than 18 digits about 50 times slower than into DECIMAL(18, s);
        # a text whose digits fit DECIMAL(18, s) goes that way and is widened exactly
        native = ("(CASE WHEN {p}il <= %d AND {p}fl <= %d THEN CAST(CAST({x} AS DECIMAL(18,%d)) AS DECIMAL(%d,%d)) "
                  "ELSE %s END)" % (18 - s, s, s, w, s, native))
    return _native_or_wide(spec, form, fits, native, "{x}", layers, pa.string())


def _date_stream(out) -> Conv:
    final = _wrap_null("CASE WHEN {x} = %d THEN DATE 'infinity' WHEN {x} = %d THEN DATE '-infinity' "
                       "ELSE CAST(DATE '2000-01-01' + {x} AS DATE) END" % (INT32_MAX, INT32_MIN))
    return Conv("sql", pa.int32(), out, (), final)


def _date_backfill(out) -> Conv:
    """'YYYY-MM-DD[ BC]', 'infinity', '-infinity'. Never through DuckDB's date cast (it reads '0044-03-15 BC' as
    44 AD): the date is taken apart and counted here."""
    pre = (
        (("bc", "ends_with({x}, ' BC')"),
         ("core", "(CASE WHEN ends_with({x}, ' BC') THEN left({x}, strlen({x}) - 3) ELSE {x} END)")),
    )
    parse = _date_text_layers("{p}core", "")
    astro = ((("ay", "(CASE WHEN {p}bc THEN 1 - {p}y ELSE {p}y END)"),),)
    days = _days_from_civil_layers("{p}ay", "{p}m", "{p}d", "days")
    layers = pre + parse + astro + days
    ok = "({p}ok AND {p}y >= 1 AND %s)" % _valid_date("{p}ay", "{p}m", "{p}d")
    final = _wrap_null("CASE WHEN {x} = 'infinity' THEN DATE 'infinity' WHEN {x} = '-infinity' THEN DATE '-infinity' "
                       "WHEN %s THEN CAST(DATE '1970-01-01' + CAST({p}days AS INTEGER) AS DATE) ELSE %s END"
                       % (ok, BAD))
    return Conv("sql", pa.string(), out, layers, final)


def _make_ts(spec: TypeSpec, us: str) -> str:
    if spec.family == "timestamptz":
        return "make_timestamptz(CAST(%s AS BIGINT))" % us
    return "make_timestamp(CAST(%s AS BIGINT))" % us


def _ts_specials(spec: TypeSpec, pos_cond: str, neg_cond: str) -> tuple:
    t = "TIMESTAMPTZ" if spec.family == "timestamptz" else "TIMESTAMP"
    return ((pos_cond, "%s 'infinity'" % t), (neg_cond, "%s '-infinity'" % t))


def _ts_fits(us: str) -> str:
    # DuckDB holds the finite microseconds strictly between its infinities (+-INT64_MAX)
    return "(%s > %d AND %s < %d)" % (us, -INT64_MAX, us, INT64_MAX)


def _ts_stream(spec: TypeSpec, form: str) -> Conv:
    layers = ((("us", "(CAST({x} AS HUGEINT) + %d)" % PG_EPOCH_US),),)
    specials = _ts_specials(spec, "{x} = %d" % INT64_MAX, "{x} = %d" % INT64_MIN)
    native = _make_ts(spec, "{p}us")
    if form == "wide":
        txt_layers, text = _ts_text(spec, "{p}us")
        layers = _merge_layers(layers, _shift(txt_layers, 1))
    else:
        text = "NULL"
    return _native_or_wide(spec, form, _ts_fits("{p}us"), native, text, layers, pa.int64(), specials)


def _ts_text(spec: TypeSpec, us: str) -> tuple:
    """(layers, sql): microseconds since 1970 (HUGEINT) printed as Postgres prints the timestamp under the read
    sessions' settings ('294276-12-31 23:59:59.999999', '+00' for timestamptz, ' BC' last)."""
    head = (
        (("tdays", "(CASE WHEN %s >= 0 THEN %s // %d ELSE -((-%s - 1) // %d) - 1 END)" % (us, us, DAY_US, us,
                                                                                        DAY_US)),),
        (("trem", "(%s - {p}tdays * %d)" % (us, DAY_US)),),
    )
    civ_layers, dtext, bc = _civil_text("{p}tdays")
    layers = _merge_layers(head, _shift(civ_layers, 1))
    clock = ("lpad(CAST({p}trem // 3600000000 AS VARCHAR), 2, '0') || ':' || "
             "lpad(CAST(({p}trem // 60000000) %% 60 AS VARCHAR), 2, '0') || ':' || "
             "lpad(CAST(({p}trem // 1000000) %% 60 AS VARCHAR), 2, '0') || %s" % _frac_text("({p}trem % 1000000)"))
    tz = " || '+00'" if spec.family == "timestamptz" else ""
    return layers, "(%s || ' ' || %s%s || %s)" % (dtext, clock, tz, bc)


def _ts_backfill(spec: TypeSpec, form: str) -> Conv:
    """'YYYY-MM-DD HH:MM:SS[.ffffff][+00][ BC]', 'infinity', '-infinity' (TimeZone=UTC, DateStyle=ISO). Taken
    apart here, never through DuckDB's timestamp cast (BC, years above 9999)."""
    tz = spec.family == "timestamptz"
    strip_tz = ("(CASE WHEN ends_with({p}c0, '+00') THEN left({p}c0, strlen({p}c0) - 3) ELSE NULL END)" if tz
                else "{p}c0")
    pre = (
        (("bc", "ends_with({x}, ' BC')"),
         ("c0", "(CASE WHEN ends_with({x}, ' BC') THEN left({x}, strlen({x}) - 3) ELSE {x} END)")),
        (("core", strip_tz),),
        (("dpart", "split_part({p}core, ' ', 1)"), ("tpart", "split_part({p}core, ' ', 2)"),
         ("ok_sp", "(strlen({p}core) - strlen(replace({p}core, ' ', '')) = 1)")),
    )
    parse = _merge_layers(_date_text_layers("{p}dpart", ""), _clock_layers("{p}tpart", "t"))
    astro = ((("ay", "(CASE WHEN {p}bc THEN 1 - {p}y ELSE {p}y END)"),),)
    days = _days_from_civil_layers("{p}ay", "{p}m", "{p}d", "days")
    us_layer = ((("us", "(CAST({p}days AS HUGEINT) * %d + %s)" % (DAY_US, _clock_us("t"))),),)
    layers = pre + parse + astro + days + us_layer
    ok = ("({p}core IS NOT NULL AND {p}ok_sp AND {p}ok AND {p}y >= 1 AND %s AND %s)"
          % (_valid_date("{p}ay", "{p}m", "{p}d"), _clock_ok("t", False)))
    specials = _ts_specials(spec, "{x} = 'infinity'", "{x} = '-infinity'")
    native = _make_ts(spec, "{p}us")
    return _native_or_wide(spec, form, _ts_fits("{p}us"), native, "{x}", layers, pa.string(), specials, ok)


def _timetz(out) -> Conv:
    """'HH:MM:SS[.ffffff]+HH[:MM[:SS]]' (the same text in both directions) -> DuckDB TIMETZ bits, computed here
    from the parts (DuckDB has no TIMETZ constructor from parts; no text of a value passes a DuckDB cast). Note: DuckDB
    1.5.6 *prints* an offset of -3630 s as '-01:30' (it drops a zero minute before seconds); the stored offset is
    right (date_part('timezone', z) = -3630), the text is not."""
    layers = (
        (("sp", "(CASE WHEN strpos({x}, '+') > 0 THEN strpos({x}, '+') ELSE strpos({x}, '-') END)"),),
        (("tpart", "left({x}, {p}sp - 1)"), ("opart", "substr({x}, {p}sp + 1)"),
         ("osign", "(CASE WHEN substr({x}, {p}sp, 1) = '-' THEN -1 ELSE 1 END)")),
        _clock_layers("{p}tpart", "t")[0] + (
            ("oh", "TRY_CAST(split_part({p}opart, ':', 1) AS BIGINT)"),
            ("om", "(CASE WHEN split_part({p}opart, ':', 2) = '' THEN 0 "
                   "ELSE TRY_CAST(split_part({p}opart, ':', 2) AS BIGINT) END)"),
            ("os", "(CASE WHEN split_part({p}opart, ':', 3) = '' THEN 0 "
                   "ELSE TRY_CAST(split_part({p}opart, ':', 3) AS BIGINT) END)"),
            ("ook", "(strlen({p}opart) IN (2, 5, 8) AND strlen(split_part({p}opart, ':', 1)) = 2)")),
        _clock_layers("{p}tpart", "t")[1] + (
            ("off", "({p}osign * ({p}oh * 3600 + {p}om * 60 + {p}os))"),),
        _clock_layers("{p}tpart", "t")[2],
    )
    ok = ("({p}sp > 0 AND {p}ook AND {p}oh BETWEEN 0 AND 15 AND {p}om BETWEEN 0 AND 59 AND {p}os BETWEEN 0 AND 59 "
          "AND %s)" % _clock_ok("t", True))
    bits = "((CAST(%s AS UBIGINT) << 24) | CAST(%d - {p}off AS UBIGINT))" % (_clock_us("t"), TIMETZ_MAX_OFFSET)
    final = _wrap_null("CASE WHEN %s THEN %s ELSE CAST(%s AS UBIGINT) END" % (ok, bits, BAD))
    return Conv("sql", pa.string(), out, layers, final, sql_type=pa.uint64(), post="timetz")


def _interval_value(m: str, d: str, u: str) -> str:
    return ("(to_months(CAST(%s AS INTEGER)) + to_days(CAST(%s AS INTEGER)) + to_microseconds(CAST(%s AS BIGINT)))"
            % (m, d, u))


def _interval_fits(m: str, d: str, u: str) -> str:
    return ("(%s BETWEEN %d AND %d AND %s BETWEEN %d AND %d AND %s BETWEEN %d AND %d)"
            % (m, INT32_MIN, INT32_MAX, d, INT32_MIN, INT32_MAX, u, -ARROW_INTERVAL_US_MAX, ARROW_INTERVAL_US_MAX))


def _interval_iso(m: str, d: str, u: str) -> tuple:
    """(layers, sql): Postgres's IntervalStyle=iso_8601 text of (months, days, microseconds) (EncodeInterval:
    every field truncated toward zero, so each carries its own sign)."""
    layers = (
        (("iy", "(%s // 12)" % m), ("imo", "(%s %% 12)" % m), ("ih", "(%s // 3600000000)" % u)),
        (("irem", "(%s - {p}ih * 3600000000)" % u),),
        (("imi", "({p}irem // 60000000)"),),
        (("isec", "(({p}irem - {p}imi * 60000000) // 1000000)"),
         ("ifs", "(({p}irem - {p}imi * 60000000) % 1000000)")),
    )

    def part(v, c):
        return "(CASE WHEN %s <> 0 THEN CAST(%s AS VARCHAR) || '%s' ELSE '' END)" % (v, v, c)

    timepart = ("(CASE WHEN {p}ih <> 0 OR {p}imi <> 0 OR {p}isec <> 0 OR {p}ifs <> 0 THEN 'T' ELSE '' END) || %s || %s"
                " || (CASE WHEN {p}isec <> 0 OR {p}ifs <> 0 THEN (CASE WHEN {p}isec < 0 OR {p}ifs < 0 THEN '-' "
                "ELSE '' END) || CAST(abs({p}isec) AS VARCHAR) || %s || 'S' ELSE '' END)"
                % (part("{p}ih", "H"), part("{p}imi", "M"), _frac_text("abs({p}ifs)")))
    text = ("(CASE WHEN %s = 0 AND %s = 0 AND %s = 0 THEN 'PT0S' ELSE 'P' || %s || %s || %s || %s END)"
            % (m, d, u, part("{p}iy", "Y"), part("{p}imo", "M"), part(d, "D"), timepart))
    return layers, text


def _interval_stream(spec: TypeSpec, form: str) -> Conv:
    """'M months D days U microseconds' (docs/design/engine.md 3)."""
    layers = (
        (("m", "TRY_CAST(split_part({x}, ' ', 1) AS HUGEINT)"),
         ("d", "TRY_CAST(split_part({x}, ' ', 3) AS HUGEINT)"),
         ("u", "TRY_CAST(split_part({x}, ' ', 5) AS HUGEINT)"),
         ("ok", "(split_part({x}, ' ', 2) = 'months' AND split_part({x}, ' ', 4) = 'days' AND "
                "split_part({x}, ' ', 6) = 'microseconds' AND split_part({x}, ' ', 7) = '')")),
    )
    ok = "({p}ok AND {p}m IS NOT NULL AND {p}d IS NOT NULL AND {p}u IS NOT NULL)"
    return _interval_common(spec, form, layers, ok, None)


ISO_INTERVAL = ("^P(?:(-?[0-9]+)Y)?(?:(-?[0-9]+)M)?(?:(-?[0-9]+)D)?"
                "(?:T(?:(-?[0-9]+)H)?(?:(-?[0-9]+)M)?(?:(-?)([0-9]+)(?:\\.([0-9]{{1,6}}))?S)?)?$")


def _interval_backfill(spec: TypeSpec, form: str) -> Conv:
    """Postgres IntervalStyle=iso_8601 text ('P1Y2M3DT4H5M6.5S', 'PT0S', 'P-1Y-2MT-4H-6.5S'), and 'infinity' /
    '-infinity' (Postgres 17+), which only the wide form holds."""
    g = "regexp_extract({x}, '%s', ['y', 'mo', 'd', 'h', 'mi', 'sg', 's', 'f'])" % ISO_INTERVAL
    layers = (
        (("g", g),),
        (("ok", "({p}g['y'] || {p}g['mo'] || {p}g['d'] || {p}g['h'] || {p}g['mi'] || {p}g['s']) <> ''"),
         ("y", "COALESCE(TRY_CAST(NULLIF({p}g['y'], '') AS HUGEINT), 0)"),
         ("mo", "COALESCE(TRY_CAST(NULLIF({p}g['mo'], '') AS HUGEINT), 0)"),
         ("d", "COALESCE(TRY_CAST(NULLIF({p}g['d'], '') AS HUGEINT), 0)"),
         ("h", "COALESCE(TRY_CAST(NULLIF({p}g['h'], '') AS HUGEINT), 0)"),
         ("mi", "COALESCE(TRY_CAST(NULLIF({p}g['mi'], '') AS HUGEINT), 0)"),
         ("s", "COALESCE(TRY_CAST(NULLIF({p}g['s'], '') AS HUGEINT), 0)"),
         ("f", "(CASE WHEN {p}g['f'] = '' THEN 0 ELSE TRY_CAST(rpad({p}g['f'], 6, '0') AS HUGEINT) END)"),
         ("sg", "(CASE WHEN {p}g['sg'] = '-' THEN -1 ELSE 1 END)")),
        (("m", "({p}y * 12 + {p}mo)"),
         ("u", "({p}h * 3600000000 + {p}mi * 60000000 + {p}sg * ({p}s * 1000000 + {p}f))")),
    )
    return _interval_common(spec, form, layers, "{p}ok", ("{x} = 'infinity'", "{x} = '-infinity'"))


def _interval_common(spec: TypeSpec, form: str, layers: tuple, ok: str, infinities) -> Conv:
    m, d, u = "{p}m", "{p}d", "{p}u"
    fits = _interval_fits(m, d, u)
    native = _interval_value(m, d, u)
    inf = "({x} IN ('infinity', '-infinity'))" if infinities else "FALSE"
    if form == "native":
        final = _wrap_null("CASE WHEN %s THEN %s WHEN NOT (%s) THEN %s WHEN %s THEN %s ELSE %s END"
                           % (inf, MISFIT, ok, BAD, fits, native, MISFIT))
        misfit = "({x} IS NOT NULL AND (%s OR ((%s) AND NOT (%s))))" % (inf, ok, fits)
        return Conv("sql", pa.string(), arrow_type(spec.native_type), layers, final, misfit=misfit)
    w = spec.wide_type
    if infinities:
        text = "{x}"
        all_layers = layers
    else:
        iso_layers, text = _interval_iso(m, d, u)
        all_layers = layers + iso_layers
    final = _wrap_null("CASE WHEN %s THEN CAST(union_value(t := {x}) AS %s) WHEN NOT (%s) THEN %s "
                       "WHEN %s THEN CAST(union_value(n := %s) AS %s) ELSE CAST(union_value(t := %s) AS %s) END"
                       % (inf, w, ok, "CAST(%s AS %s)" % (BAD, w), fits, native, w, text, w))
    return Conv("sql", pa.string(), arrow_type(w), all_layers, final)


def widen_expression(spec: TypeSpec, column_sql: str) -> str:
    """SQL that carries a value of the native form into the wide form (for `ALTER TABLE ... ALTER COLUMN ... TYPE
    <wide> USING <this>`): the value becomes the native member; NULL stays a real NULL (a plain CAST of a typed
    NULL to a UNION gives a non-NULL union whose member is NULL, measured on DuckDB 1.5.6)."""
    if spec.wide_type is None:
        raise ValueError("family %s has one form" % spec.family)
    return ("CASE WHEN {c} IS NULL THEN NULL ELSE CAST(union_value(n := {c}) AS {w}) END"
            .format(c=column_sql, w=spec.wide_type))
