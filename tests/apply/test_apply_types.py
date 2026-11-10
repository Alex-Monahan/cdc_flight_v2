"""Package A, tier T0: destination types (cdc_v2.apply.types), TableMeta, and the DuckDB behaviours the statements
are built on (docs/design/apply.md section 5). The same facts are checked on MotherDuck in test_apply_md.py."""
from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from cdc_v2.apply import ChangeSetError, Column, LocalChangeSet, RelationChangeSet, TableMeta, Target
from cdc_v2.apply.types import affected, exact, needs_exact, parse, rewrites_row, sentinel

NZ = "CAST('-0' AS DOUBLE)"
BITS = "CAST(CAST({} AS BIT) AS BIGINT)"


def con():
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    return c


@pytest.mark.parametrize("text,sql", [
    ("INTEGER", "INTEGER"), ("int8", "BIGINT"), ("DECIMAL(38, 10)", "DECIMAL(38, 10)"),
    ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE"), ("timestamptz", "TIMESTAMP WITH TIME ZONE"),
    ("TIMETZ", "TIME WITH TIME ZONE"), ("INTEGER[]", "INTEGER[]"), ("DOUBLE[3]", "DOUBLE[3]"),
    ("INTEGER[][]", "INTEGER[][]"), ('STRUCT(a DOUBLE, "b c" VARCHAR)', 'STRUCT("a" DOUBLE, "b c" VARCHAR)'),
    ("UNION(n DECIMAL(38,10), s VARCHAR)", 'UNION("n" DECIMAL(38, 10), "s" VARCHAR)'),
    ("MAP(VARCHAR, DOUBLE[])", "MAP(VARCHAR, DOUBLE[])"), ('STRUCT("x""y" INT)', 'STRUCT("x""y" INTEGER)'),
])
def test_parse_round_trip(text, sql):
    t = parse(text)
    assert t.sql() == sql
    assert parse(t.sql()) == t
    c = con()
    c.execute("SELECT CAST(NULL AS %s)" % t.sql())          # DuckDB accepts the rendered type


def test_classification():
    assert needs_exact(parse("DOUBLE")) and needs_exact(parse("INTERVAL")) and needs_exact(parse("FLOAT[]"))
    assert needs_exact(parse("UNION(d DOUBLE, i BIGINT)")) and needs_exact(parse("MAP(VARCHAR, INTERVAL)"))
    for t in ("BIGINT", "VARCHAR", "DECIMAL(18,3)", "TIMESTAMP WITH TIME ZONE", "UUID", "BLOB", "TIME WITH TIME ZONE",
              "INTEGER[]", "STRUCT(a VARCHAR)", "UNION(n DECIMAL(38,10), s VARCHAR)", "DATE", "JSON"):
        assert not needs_exact(parse(t)), t
    assert affected(parse("DOUBLE")) and affected(parse("STRUCT(a STRUCT(i INTERVAL))"))
    assert not affected(parse("DOUBLE[]")) and not affected(parse("UNION(d DOUBLE, i INT)"))
    assert rewrites_row(parse("INTEGER[]")) and rewrites_row(parse("UNION(a INT, b VARCHAR)"))
    assert rewrites_row(parse("STRUCT(l INTEGER[])")) and not rewrites_row(parse("STRUCT(a DOUBLE)"))


# values that DuckDB's `=` merges and the source keeps apart, per type: (type, a, b)
MERGED = [
    ("DOUBLE", NZ, "0.0::DOUBLE"),
    ("FLOAT", "CAST('-0' AS FLOAT)", "0.0::FLOAT"),
    ("INTERVAL", "INTERVAL '1 month'", "INTERVAL '30 days'"),
    ("INTERVAL", "INTERVAL '30 days'", "INTERVAL '720 hours'"),
    ("INTERVAL", "INTERVAL '1 day'", "INTERVAL '24 hours'"),
    ("INTERVAL", "INTERVAL '1 month -30 days'", "INTERVAL '0 seconds'"),
    ("DOUBLE[]", "[%s]" % NZ, "[0.0::DOUBLE]"),
    ("STRUCT(a DOUBLE)", "{'a': %s}" % NZ, "{'a': 0.0::DOUBLE}"),
    ("UNION(d DOUBLE, i BIGINT)", "union_value(d := %s)" % NZ, "union_value(d := 0.0::DOUBLE)"),
    ("MAP(VARCHAR, DOUBLE)", "MAP {'k': %s}" % NZ, "MAP {'k': 0.0::DOUBLE}"),
    ("STRUCT(i INTERVAL)", "{'i': INTERVAL '1 month'}", "{'i': INTERVAL '30 days'}"),
]


@pytest.mark.parametrize("typ,a,b", MERGED)
def test_exact_keeps_apart_what_equality_merges(typ, a, b):
    t = parse(typ)
    c = con()
    x, y = "CAST(%s AS %s)" % (a, typ), "CAST(%s AS %s)" % (b, typ)
    assert c.execute("SELECT %s = %s" % (x, y)).fetchone()[0] is True            # DuckDB merges them
    assert c.execute("SELECT %s = %s" % (exact(x, t), exact(y, t))).fetchone()[0] is False
    assert c.execute("SELECT count(DISTINCT e) FROM (SELECT %s AS e UNION ALL SELECT %s)"
                     % (exact(x, t), exact(y, t))).fetchone()[0] == 2


def test_exact_nan_is_one_value_and_null_stays_null():
    c = con()
    for typ, nan, other in [("DOUBLE", "'nan'::DOUBLE", "(-('nan'::DOUBLE))"), ("FLOAT", "'nan'::FLOAT", "(-('nan'::FLOAT))")]:
        t = parse(typ)
        bits = c.execute("SELECT %s, %s" % (BITS.format(nan), BITS.format(other))).fetchone()
        assert bits[0] != bits[1]                                   # two different NaNs ...
        assert c.execute("SELECT %s = %s" % (exact(nan, t), exact(other, t))).fetchone()[0] is True   # ... one value
        assert c.execute("SELECT %s IS NULL" % exact("CAST(NULL AS %s)" % typ, t)).fetchone()[0] is True
    t = parse("UNION(d DOUBLE, i BIGINT)")
    assert c.execute("SELECT %s IS NULL" % exact("CAST(NULL AS UNION(d DOUBLE, i BIGINT))", t)).fetchone()[0]
    # members compare by member and value: 0 as BIGINT is not 0 as DOUBLE
    assert c.execute("SELECT %s = %s" % (exact("union_value(i := 0)::UNION(d DOUBLE, i BIGINT)", t),
                                         exact("union_value(d := 0.0::DOUBLE)::UNION(d DOUBLE, i BIGINT)", t))).fetchone()[0] is False


@pytest.mark.parametrize("typ,old,new", [
    ("DOUBLE", NZ, "0.0::DOUBLE"), ("DOUBLE", "0.0::DOUBLE", NZ), ("FLOAT", "CAST('-0' AS FLOAT)", "0.0::FLOAT"),
    ("INTERVAL", "INTERVAL '1 month'", "INTERVAL '30 days'"), ("INTERVAL", "INTERVAL '24 hours'", "INTERVAL '1 day'"),
    ("STRUCT(a DOUBLE, b VARCHAR)", "{'a': %s, 'b': 'x'}" % NZ, "{'a': 0.0::DOUBLE, 'b': 'x'}"),
    ("STRUCT(i INTERVAL)", "{'i': INTERVAL '1 month'}", "{'i': INTERVAL '30 days'}"),
])
def test_update_skips_equal_value_and_sentinel_cures_it(typ, old, new):
    """Finding 1 of the oracle, and its cure: a plain UPDATE keeps the old representation, also through a NULL;
    writing the sentinel first stores the new value."""
    t = parse(typ)
    c = con()
    probe = lambda: c.execute("SELECT %s FROM t WHERE k = 1" % exact("x", t)).fetchone()[0]
    want = c.execute("SELECT %s" % exact("CAST(%s AS %s)" % (new, typ), t)).fetchone()[0]
    c.execute("CREATE TABLE t (k INT, x %s)" % typ)
    c.execute("INSERT INTO t VALUES (1, %s)" % old)
    c.execute("UPDATE t SET x = %s WHERE k = 1" % new)
    assert probe() != want                                          # skipped
    c.execute("UPDATE t SET x = NULL WHERE k = 1")
    c.execute("UPDATE t SET x = %s WHERE k = 1" % new)
    assert probe() != want                                          # the NULL keeps the old data slot
    c.execute("UPDATE t SET x = %s WHERE k = 1" % sentinel("CAST(%s AS %s)" % (new, typ), t))
    c.execute("UPDATE t SET x = %s WHERE k = 1" % new)
    assert probe() == want


@pytest.mark.parametrize("typ,val", [("INTEGER[]", "[2]"), ("UNION(i INT, s VARCHAR)", "union_value(i := 2)"),
                                     ("MAP(INT, INT)", "MAP {1: 2}")])
def test_update_of_list_union_map_rewrites_the_row(typ, val):
    c = con()
    c.execute("CREATE TABLE t (k INT, x %s)" % typ)
    c.execute("INSERT INTO t VALUES (1, NULL), (2, NULL)")
    before = c.execute("SELECT rowid FROM t WHERE k = 1").fetchone()[0]
    c.execute("UPDATE t SET x = %s WHERE k = 1" % val)
    assert c.execute("SELECT rowid FROM t WHERE k = 1").fetchone()[0] != before       # delete + insert


def test_aggregates_normalise_negative_zero_in_nested_values():
    """Why the plan never aggregates a value (docs/design/apply.md 5)."""
    c = con()
    c.execute("CREATE TABLE s AS SELECT 1 AS g, [%s] AS l, {'a': %s} AS st, %s AS d" % (NZ, NZ, NZ))
    assert c.execute("SELECT %s FROM s" % BITS.format("l[1]")).fetchone()[0] < 0
    assert c.execute("SELECT %s FROM (SELECT any_value(l) AS l FROM s)" % BITS.format("l[1]")).fetchone()[0] == 0
    assert c.execute("SELECT %s FROM (SELECT arg_max(st, g) AS st FROM s)" % BITS.format("st.a")).fetchone()[0] == 0
    assert c.execute("SELECT %s FROM (SELECT any_value(d) AS d FROM s)" % BITS.format("d")).fetchone()[0] < 0


def test_arrow_round_trip_needs_lossless_conversion():
    c = con()
    sql = "SELECT '12:00:00+01'::TIMETZ AS z, gen_random_uuid() AS u, union_value(s := NULL::VARCHAR)::UNION(n INT, s VARCHAR) AS n"
    plain = c.execute(sql).to_arrow_table()
    c.execute("SET arrow_lossless_conversion = true")
    lossless = c.execute(sql).to_arrow_table()
    d = con()
    d.register("p", plain)
    d.register("l", lossless)
    assert d.execute("SELECT typeof(z), typeof(u) FROM p").fetchone() == ("TIME", "VARCHAR")
    assert d.execute("SELECT typeof(z), typeof(u), CAST(z AS VARCHAR) FROM l").fetchone() == (
        "TIME WITH TIME ZONE", "UUID", "12:00:00+01")
    assert d.execute("SELECT n IS NULL FROM l").fetchone()[0] is True       # a NULL member arrives as NULL


def test_meta_rules():
    with pytest.raises(ValueError):
        TableMeta((Column(0, "a", "INTEGER", key=True), Column(1, "A", "INTEGER")))
    with pytest.raises(ValueError):
        TableMeta((Column(0, "a", "INTEGER"), Column(0, "b", "INTEGER")))
    with pytest.raises(ValueError):
        Column(0, "", "INTEGER")
    names = ['a"b', "with space", "select", "ä€", " ", "a.b", "x0", "a1", "rowid", "tab\tname", "'q'"]
    m = TableMeta(tuple(Column(i, n, "INTEGER", key=(i == 0)) for i, n in enumerate(names)))
    assert m.has_key and [c.name for c in m.columns] == names
    assert not TableMeta((Column(3, "rowid", "INTEGER"),)).has_key
    assert str(Target("s", "t")) == "s.t" and Target('a"b', "c").sql() == '"a""b"."c"'


def test_change_set_shape_is_checked():
    meta = TableMeta((Column(0, "id", "INTEGER", key=True), Column(1, "v", "VARCHAR", unchanged_possible=True)))
    with pytest.raises(ChangeSetError):
        LocalChangeSet(pa.table({"seq": [0], "op": ["c"], "a0": [1]}), None, meta)
    with pytest.raises(ChangeSetError):
        RelationChangeSet("SELECT 1", (), meta, scratch_prefix="main")
