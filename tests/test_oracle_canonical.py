"""Canonical forms of the oracle (T0, hermetic). The DuckDB side runs in a local in-memory DuckDB and is checked
against the Python reference; the Postgres side is checked against the same reference in tests/t1."""
import datetime as dt
import decimal
import math
import re
import struct
import uuid
from pathlib import Path

import duckdb
import pytest
from hypothesis import given, settings, strategies as st

from cdc_v2.oracle import canonical as C

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    return c


def md_value(con, mdtype, value, pg_base, elem=None):
    con.execute("CREATE OR REPLACE TABLE x (c %s)" % mdtype)
    con.execute("INSERT INTO x VALUES (?)", [value])
    t = C.PgColumnType(pg_base, elem)
    return con.execute("SELECT %s FROM x" % C.md_canon("c", t, C.parse_md_type(mdtype))).fetchone()[0]


# --------------------------------------------------------------------------------------------- type strings

@pytest.mark.parametrize("text,base,extra", [
    ("INTEGER", "INTEGER", None), ("DECIMAL(38,10)", "DECIMAL", ("38", "10")), ("INTEGER[]", "LIST", "INTEGER"),
    ("INTEGER[][]", "LIST", "INTEGER[]"), ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", None),
    ("TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE", None), ("VARCHAR(20)", "VARCHAR", ("20",)),
])
def test_parse_md_type(text, base, extra):
    t = C.parse_md_type(text)
    assert t.base == base
    if base == "LIST":
        assert t.elem.text == extra
    elif extra:
        assert t.args == extra


def test_parse_union_type():
    t = C.parse_md_type("UNION(n DECIMAL(38,10), s VARCHAR)")
    assert t.base == "UNION"
    assert [(n, m.base) for n, m in t.members] == [("n", "DECIMAL"), ("s", "VARCHAR")]
    assert t.members[0][1].scale == 10
    q = C.parse_md_type('UNION("a b" INTEGER, "c" DOUBLE[])')
    assert [(n, m.base) for n, m in q.members] == [("a b", "INTEGER"), ("c", "LIST")]


# --------------------------------------------------------------------------------------------- values per family

CASES = [
    ("integer", "INTEGER", [0, -5, 2147483647, -2147483648]),
    ("bigint", "BIGINT", [9223372036854775807, -9223372036854775808]),
    ("smallint", "SMALLINT", [-32768, 32767]),
    ("boolean", "BOOLEAN", [True, False]),
    ("numeric", "DECIMAL(38,10)", [decimal.Decimal("1.5"), decimal.Decimal("-0.25"), decimal.Decimal("100"),
                                   decimal.Decimal("0"), decimal.Decimal("0.0000000001")]),
    ("numeric", "DECIMAL(12,4)", [decimal.Decimal("12345678.9999"), decimal.Decimal("-0.0001")]),
    ("numeric", "VARCHAR", ["1.50", "NaN", "Infinity", "-Infinity", "-0.00", "100", "0.000"]),
    ("double precision", "DOUBLE", [0.1, 1e15, 1e16, 1.5e-5, -0.0, 0.0, float("nan"), float("inf"), float("-inf"),
                                    5e-324, 1.7976931348623157e308, 123456789012345678.0, 100.0, -2.5]),
    ("real", "FLOAT", [0.1, 1e30, -0.0, 16777216.0, 3.4028235e38, 1e-45, float("nan"), float("-inf")]),
    ("text", "VARCHAR", ["", "héllo", "a'b", "tab\there", "line\nbreak"]),
    ("character", "VARCHAR", ["ab  ", "x", ""]),
    ("bytea", "BLOB", [b"\xab\x00", b"", b"\x00\xff"]),
    ("date", "DATE", [dt.date(2024, 1, 2), dt.date(1, 1, 1), dt.date(1969, 12, 31), dt.date(9999, 12, 31)]),
    ("timestamp without time zone", "TIMESTAMP", [dt.datetime(2024, 1, 1, 0, 0, 0, 1),
                                                  dt.datetime(1960, 5, 5, 1, 2, 3), dt.datetime(1, 1, 1)]),
    ("timestamp with time zone", "TIMESTAMPTZ", [dt.datetime(2024, 1, 1, 0, 0, 0, 1, tzinfo=dt.timezone.utc),
                                                 dt.datetime(1969, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)]),
    ("time without time zone", "TIME", [dt.time(1, 2, 3, 4), dt.time(0, 0), dt.time(23, 59, 59, 999999)]),
    ("uuid", "UUID", [uuid.UUID("6F9619FF-8B86-D011-B42D-00C04FC964FF")]),
    ("uuid", "VARCHAR", ["6F9619FF-8B86-D011-B42D-00C04FC964FF"]),
    ("json", "JSON", ['{"b": 1,  "a":2}', "[]", "null"]),
    ("jsonb", "VARCHAR", ['{"a": 1, "b": [1, 2.50]}']),
]


@pytest.mark.parametrize("base,mdtype,values", CASES, ids=["%s-%s" % (c[0], c[1]) for c in CASES])
def test_md_matches_reference(con, base, mdtype, values):
    for v in values:
        assert md_value(con, mdtype, v, base) == C.py_canon(v, C.PgColumnType(base)), repr(v)


def test_float_forms(con):
    assert md_value(con, "DOUBLE", 1.5, "double precision") == "3ff8000000000000"
    assert md_value(con, "DOUBLE", -0.0, "double precision") == "8000000000000000"
    assert md_value(con, "DOUBLE", 0.0, "double precision") == "0000000000000000"
    assert md_value(con, "DOUBLE", float("nan"), "double precision") == "NaN"
    assert md_value(con, "DOUBLE", float("-inf"), "double precision") == "-Infinity"
    assert md_value(con, "FLOAT", 0.1, "real") == "3fb99999a0000000"
    assert md_value(con, "FLOAT", 1048576.25, "real") == C.py_float4(1048576.25)
    # a REAL stored in a DOUBLE column is accepted only when it is exactly a float32 value
    assert md_value(con, "DOUBLE", C.float4_value(0.1), "real") == "3fb99999a0000000"
    assert md_value(con, "DOUBLE", 0.1, "real").startswith("!DOUBLE")
    assert C.show_float("3ff8000000000000") == "1.5 (3ff8000000000000)"


def test_interval(con):
    t = C.PgColumnType("interval")
    for m, d, us in [(26, 3, 90067000009), (-26, -3, -90067000009), (1, -3, 5), (0, 0, 0)]:
        sql = "SELECT %s FROM (SELECT to_months(%d) + to_days(%d) + to_microseconds(%d) AS c)" % (
            C.md_canon("c", t, C.parse_md_type("INTERVAL")), m, d, us)
        assert con.execute(sql).fetchone()[0] == C.py_canon((m, d, us), t) == "%d:%d:%d" % (m, d, us)
    # one month is not thirty days
    assert C.py_canon((1, 0, 0), t) != C.py_canon((0, 30, 0), t)


def test_infinite_temporal(con):
    for typ, base in [("DATE", "date"), ("TIMESTAMP", "timestamp without time zone"),
                      ("TIMESTAMPTZ", "timestamp with time zone")]:
        for v in ("infinity", "-infinity"):
            got = con.execute("SELECT %s FROM (SELECT CAST('%s' AS %s) AS c)" % (
                C.md_canon("c", C.PgColumnType(base), C.parse_md_type(typ)), v, typ)).fetchone()[0]
            assert got == v


def test_arrays(con):
    t = C.PgColumnType("integer[]", "integer")
    assert md_value(con, "INTEGER[]", [1, None, 3], "integer[]", "integer") == C.py_canon([1, None, 3], t) \
        == "[1:3]{V1:1,N,V1:3}"
    assert md_value(con, "INTEGER[]", [], "integer[]", "integer") == "{}" == C.py_canon([], t)
    assert md_value(con, "INTEGER[][]", [[1, 2], [3, None]], "integer[]", "integer") == \
        C.py_canon([[1, 2], [3, None]], t) == "[1:2][1:2]{V1:1,V1:2,V1:3,N}"
    td = C.PgColumnType("double precision[]", "double precision")
    # (the Python client turns NaN inside a list parameter into NULL, so the list is built in SQL)
    got = con.execute("SELECT %s FROM (SELECT ['-0'::DOUBLE, 'nan'::DOUBLE, NULL] AS c)" % C.md_canon(
        "c", td, C.parse_md_type("DOUBLE[]"))).fetchone()[0]
    assert got == C.py_canon([-0.0, float("nan"), None], td) == "[1:3]{V16:8000000000000000,V3:NaN,N}"
    tt = C.PgColumnType("text[]", "text")
    assert md_value(con, "VARCHAR[]", ["", None], "text[]", "text") == "[1:2]{V0:,N}"
    # a list where an array of another type was expected never matches
    assert md_value(con, "VARCHAR", "x", "integer[]", "integer").startswith("!VARCHAR")


def test_union_members(con):
    con.execute("CREATE OR REPLACE TABLE u (c UNION(n DECIMAL(38,10), s VARCHAR))")
    con.execute("INSERT INTO u VALUES (1.5::DECIMAL(38,10)), ('NaN'), ('1.230'), (NULL)")
    t = C.PgColumnType("numeric")
    mt = C.parse_md_type("UNION(n DECIMAL(38,10), s VARCHAR)")
    got = [r[0] for r in con.execute("SELECT %s FROM u ORDER BY rowid" % C.md_canon("c", t, mt)).fetchall()]
    assert got == ["1.5", "NaN", "1.23", None]
    # a union whose active member holds NULL is not NULL in DuckDB (IS NULL is false): the oracle reports it
    con.execute("CREATE OR REPLACE TABLE u2 AS SELECT union_value(s := NULL::VARCHAR)::UNION(n DECIMAL(38,10), "
                "s VARCHAR) AS c")
    assert con.execute("SELECT %s FROM u2" % C.md_canon("c", t, mt)).fetchone()[0] == "!null"


def test_unaccepted_destination_type_never_matches(con):
    got = md_value(con, "TIMESTAMP", dt.datetime(2024, 1, 1), "integer")
    assert got.startswith("!TIMESTAMP:")
    assert md_value(con, "VARCHAR", "t", "boolean").startswith("!VARCHAR:")


def test_fallback_rule_for_unregistered_types():
    assert C.rule_for("money") is C.FALLBACK
    assert C.PgColumnType("inet").rule is C.FALLBACK
    assert C.rule_for("integer").family == "integer"


# --------------------------------------------------------------------------------------------- tokens and hashes

def test_tokens_separate_null_empty_and_boundaries():
    assert C.py_token(None) == "N"
    assert C.py_token("") == "V0:"
    assert C.py_token(None) != C.py_token("") != C.py_token("N")
    assert C.py_token("a") + C.py_token("bc") != C.py_token("ab") + C.py_token("c")
    assert C.py_token("é") == "V2:é"                     # bytes, not characters


def test_token_and_hash_sql_match_python(con):
    for v in [None, "", "abc", "héllo", "N", "V1:x"]:
        tok = con.execute("SELECT %s FROM (SELECT CAST(? AS VARCHAR) AS v)" % C.token_md("v"), [v]).fetchone()[0]
        assert tok == C.py_token(v)
        h = con.execute("SELECT %s FROM (SELECT CAST(? AS VARCHAR) AS v)" % C.hash60_md("v"), [tok]).fetchone()[0]
        assert h == C.py_hash60(tok)
        assert 0 <= h < 2 ** 60


# --------------------------------------------------------------------------------------------- properties

@settings(max_examples=300, deadline=None)
@given(st.floats(allow_nan=True, allow_infinity=True))
def test_double_property(con, v):
    assert md_value(con, "DOUBLE", v, "double precision") == C.py_float8(v)


@settings(max_examples=300, deadline=None)
@given(st.floats(width=32, allow_nan=True, allow_infinity=True))
def test_real_property(con, v):
    assert md_value(con, "FLOAT", v, "real") == C.py_float4(v)


@settings(max_examples=200, deadline=None)
@given(st.decimals(min_value=-10 ** 27, max_value=10 ** 27, places=10, allow_nan=False, allow_infinity=False))
def test_numeric_property(con, v):
    assert md_value(con, "DECIMAL(38,10)", v, "numeric") == C.py_numeric(v)


@settings(max_examples=200, deadline=None)
@given(st.text())
def test_text_property(con, v):
    v = v.replace("\x00", "")
    assert md_value(con, "VARCHAR", v, "text") == v


@settings(max_examples=200, deadline=None)
@given(st.datetimes(min_value=dt.datetime(1, 1, 1), max_value=dt.datetime(9999, 12, 31, 23, 59, 59, 999999)))
def test_timestamp_property(con, v):
    assert md_value(con, "TIMESTAMP", v, "timestamp without time zone") == C.py_timestamp(v)


def float_sample(n=20000, seed=5):
    """Random bit patterns plus dyadic rationals (the values where two shortest texts can tie)."""
    import random
    r = random.Random(seed)
    vals = [struct.unpack("d", struct.pack("Q", r.getrandbits(64)))[0] for _ in range(n)]
    vals += [r.randint(-2 ** 40, 2 ** 40) / (2 ** r.randint(0, 12)) for _ in range(n)]
    return [v for v in vals if not math.isnan(v)]


def test_float_forms_in_bulk(con):
    vals = float_sample()
    t8, t4 = C.PgColumnType("double precision"), C.PgColumnType("real")
    sql = ("SELECT x, %s, %s FROM (SELECT x, CASE WHEN x = 0 OR abs(x) BETWEEN 1e-37 AND 3.4e38 THEN CAST(x AS FLOAT) "
           "END AS f FROM (SELECT unnest(?::DOUBLE[]) AS x))") % (
        C.md_canon("x", t8, C.parse_md_type("DOUBLE")), C.md_canon("f", t4, C.parse_md_type("FLOAT")))
    bad = [(x, d, f) for x, d, f in con.execute(sql, [vals]).fetchall()
           if d != C.py_float8(x) or f != (C.py_float4(x) if x == 0 or 1e-37 <= abs(x) <= 3.4e38 else None)]
    assert not bad, bad[:5]


def test_duckdb_float_text_is_not_always_shortest(con):
    """Why floats are compared by their bits: DuckDB's FLOAT text is not the shortest float32 text, and Postgres
    prints some doubles with 17 digits where Python and DuckDB print 16 (rounding ties); see docs/oracle.md."""
    assert con.execute("SELECT CAST(CAST(1048576.25 AS FLOAT) AS VARCHAR)").fetchone()[0] == "1048576.25"
    assert C.float4_repr(1048576.25) == "1048576.2"


def test_float4_repr_is_shortest():
    assert C.float4_repr(0.1) == "0.1"
    assert C.float4_repr(16777216.0) in ("1.677722e+07", "16777216")
    for v in (0.1, 1.1, 3.4028235e38, 1e-45):
        f = C.float4_value(v)
        assert struct.unpack("f", struct.pack("f", float(C.float4_repr(v))))[0] == f
    assert math.isnan(C.float4_value(float("nan")))


# --------------------------------------------------------------------------------------------- documentation

def test_docs_list_every_family():
    doc = (ROOT / "docs" / "oracle.md").read_text()
    for row in C.rule_table():
        assert re.search(r"^\| %s \|" % re.escape(row.family), doc, re.M), "docs/oracle.md lacks %s" % row.family
