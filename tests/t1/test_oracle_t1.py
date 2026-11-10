"""The oracle against real servers (T1): equal data is reported equal, and every deliberate mutation of the
destination is reported, with the right kind of difference.

Source: a private database on the shared cluster with every supported type family (cdc_v2.testing.datasets).
Destination: a private MotherDuck database loaded by the direct test loader. Each mutation runs inside a
destination transaction; the oracle reads inside that transaction and the mutation is rolled back afterwards.
"""
import datetime as dt
import decimal
import uuid

import pytest

from cdc_v2.oracle import OracleConfig, PgColumnType, compare, py_canon
from cdc_v2.oracle.canonical import Ctx, pg_canon
from cdc_v2.testing import datasets, loader

pytestmark = [pytest.mark.t1, pytest.mark.timeout(900)]

CFG = dict(chunk_rows=500, source_workers=3, min_blocks_per_range=2)
RESULTS = []


@pytest.fixture(scope="module")
def loaded(make_source_db, make_dest_db, evidence):
    src = make_source_db()
    datasets.create_types_dataset(src)
    dst = make_dest_db()
    counts = loader.load_database(src.params(), dst.conn)
    yield src, dst, counts
    evidence("p3/oracle").write_json("mutations", {
        "what": "each mutation of the destination, applied inside a transaction and compared by the oracle",
        "dataset": counts, "config": CFG, "mutations": RESULTS,
        "all_detected": all(r["detected"] == r["expected_difference"] for r in RESULTS)})


def run(src, dst, statements=(), **cfg):
    conn = dst.connect()
    try:
        conn.execute("BEGIN TRANSACTION")
        for s in statements:
            conn.execute(s)
        return compare(src.source(), dst.destination(conn=conn, in_transaction=True), OracleConfig(**dict(CFG, **cfg)))
    finally:
        try:
            conn.execute("ROLLBACK")
        finally:
            conn.close()


def table(result, name):
    return next(t for t in result.tables if t.source == name)


def record(name, statements, expected, result, detail):
    RESULTS.append({"mutation": name, "statements": list(statements), "expected_difference": expected,
                    "detected": not result.equal, "detail": detail, "seconds": result.seconds})


def test_equal(loaded, evidence):
    src, dst, counts = loaded
    r = compare(src.source(), dst.destination(), OracleConfig(**CFG))
    assert r.equal, r.summary()
    assert {t.source for t in r.tables} == set(counts)
    assert all(t.source_rows == counts[t.source] == t.destination_rows for t in r.tables)
    big = table(r, "other.big")
    assert big.buckets == 6 and big.key == ["l_orderkey", "l_linenumber"]
    assert table(r, "public.dups").key == []
    evidence("p3/oracle").write_json("equal", r.to_dict())


# ------------------------------------------------------------------------------------------------ rows

ROW_MUTATIONS = [
    ("missing_row", ["DELETE FROM public.types_k WHERE id = 5"], "public.types_k", (1, 0, 0)),
    ("extra_row", ["INSERT INTO public.types_k (id) VALUES (100)"], "public.types_k", (0, 1, 0)),
    ("duplicate_key_row", ["INSERT INTO public.types_k SELECT * FROM public.types_k WHERE id = 2"],
     "public.types_k", (0, 1, 0)),
    ("all_null_row_missing", ["DELETE FROM public.types_k WHERE id = 1"], "public.types_k", (1, 0, 0)),
    ("keyless_one_identical_row_less",
     ["DELETE FROM public.dups WHERE rowid = (SELECT min(rowid) FROM public.dups WHERE a = 1)"], "public.dups",
     (1, 0, 0)),
    ("keyless_one_identical_row_more", ["INSERT INTO public.dups SELECT * FROM public.dups WHERE a = 1 LIMIT 1"],
     "public.dups", (0, 1, 0)),
    ("keyless_identical_null_row_more", ["INSERT INTO public.dups SELECT * FROM public.dups WHERE a = 2 LIMIT 1"],
     "public.dups", (0, 1, 0)),
    ("keyless_empty_string_becomes_null", ["UPDATE public.dups SET b = NULL WHERE a IS NULL"], "public.dups",
     (1, 1, 0)),
    ("big_table_row_missing", ["DELETE FROM other.big WHERE l_orderkey = 1234"], "other.big", (1, 0, 0)),
    ("big_table_last_digit", ["UPDATE other.big SET l_extendedprice = l_extendedprice + 0.01 WHERE l_orderkey = 77"],
     "other.big", (0, 0, 1)),
]


@pytest.mark.parametrize("name,stmts,tname,counts", ROW_MUTATIONS, ids=[m[0] for m in ROW_MUTATIONS])
def test_row_mutation_detected(loaded, name, stmts, tname, counts):
    src, dst, _ = loaded
    r = run(src, dst, stmts)
    t = table(r, tname)
    got = (t.missing_rows, t.extra_rows, t.different_rows)
    record(name, stmts, True, r, {"table": tname, "missing_extra_different": got, "sample": t.sample})
    assert not r.equal and not t.equal
    assert got == counts, t.sample
    assert [x for x in r.tables if not x.equal] == [t]


# ------------------------------------------------------------------------------------------------ values

VALUE_MUTATIONS = [
    # (name, column, new value expression, id); the row is replaced, see replace_row()
    ("smallint", "c_smallint", "c_smallint + 1", 2),
    ("integer", "c_int", "c_int + 1", 2),
    ("bigint_boundary", "c_bigint", "c_bigint + 1", 3),
    ("boolean", "c_bool", "NOT c_bool", 2),
    ("boolean_null", "c_bool", "false", 1),
    ("numeric_last_digit", "c_num", "c_num + 0.0001", 2),
    ("numeric_union_decimal_member", "c_num_u", "1.51::DECIMAL(38,10)", 2),
    ("numeric_nan_to_infinity", "c_num_u", "'Infinity'", 3),
    ("numeric_nan_to_null", "c_num_u", "NULL", 3),
    ("numeric_wide_last_digit", "c_num_u", "'123456789012345678901234567890.123456788'", 6),
    ("real_one_ulp", "c_real", "nextafter(c_real, 1::FLOAT)", 2),
    ("real_negative_zero", "c_real", "0", 3),
    ("real_nan", "c_real", "1", 4),
    ("double_one_ulp", "c_double", "nextafter(c_double, 1)", 2),
    ("double_negative_zero_vs_zero", "c_double", "0.0", 3),
    ("double_nan_to_number", "c_double", "1.0", 4),
    ("double_number_to_nan", "c_double", "'nan'::DOUBLE", 2),
    ("double_infinity_sign", "c_double", "'-inf'::DOUBLE", 5),
    ("text_empty_to_null", "c_text", "NULL", 3),
    ("text_null_to_empty", "c_text", "''", 1),
    ("text_one_char", "c_text", "'hellO'", 2),
    ("text_unicode", "c_text", "'héllo world ✓'", 4),
    ("varchar", "c_varchar", "'w'", 2),
    ("char", "c_char", "'ac'", 2),
    ("bytea_one_byte", "c_bytea", "'\\xAB\\x01'::BLOB", 2),
    ("bytea_empty_to_null", "c_bytea", "NULL", 3),
    ("date_one_day", "c_date", "c_date + 1", 2),
    ("date_infinity", "c_date", "DATE '9999-12-31'", 3),
    ("time_one_microsecond", "c_time", "c_time + INTERVAL 1 MICROSECOND", 2),
    ("time_24h", "c_time", "TIME '23:59:59.999999'", 3),
    ("timestamp_one_microsecond", "c_ts", "c_ts + INTERVAL 1 MICROSECOND", 2),
    ("timestamp_infinity", "c_ts", "TIMESTAMP '294000-01-01'", 3),
    ("timestamptz_one_microsecond", "c_tstz", "c_tstz + INTERVAL 1 MICROSECOND", 2),
    ("interval_month_vs_30_days", "c_interval", "INTERVAL 30 DAY", 3),
    ("interval_one_microsecond", "c_interval", "c_interval + INTERVAL 1 MICROSECOND", 2),
    ("uuid", "c_uuid", "'6f9619ff-8b86-d011-b42d-00c04fc964fe'", 2),
    ("json_whitespace", "c_json", "'{\"b\": 1, \"a\":2}'", 2),
    ("jsonb_value", "c_jsonb", "'{\"a\": [1, 2.5], \"b\": 1}'", 2),
    ("int_array_element", "c_int_arr", "[1, 0, 3]", 2),
    ("int_array_null_element_dropped", "c_int_arr", "[1, 3]", 2),
    ("text_array_empty_to_null", "c_text_arr", "['x', NULL, NULL]", 2),
    ("numeric_array_element", "c_num_arr",
     "[1.6::DECIMAL(38,10)::UNION(n DECIMAL(38,10), s VARCHAR), c_num_arr[2]]", 2),
    ("double_array_negative_zero", "c_double_arr", "[0.0, 1e300]", 2),
    ("timestamptz_array_one_microsecond", "c_tstz_arr", "[c_tstz_arr[1] + INTERVAL 1 MICROSECOND]", 2),
    ("empty_array_to_null", "c_int_arr", "NULL", 3),
]


def replace_row(column, expr, row, table="public.types_k"):
    """Statements that replace row `id = row` by a copy with one changed column.

    An UPDATE is not used: DuckDB skips an UPDATE whose new value compares equal to the old one (-0 and 0, NaN and
    NaN, '1 mon' and '30 days') and then keeps the old representation (evidence: duckdb_equal_update.json).
    The original row is the one with the smallest rowid (rows inserted by the open transaction come after it)."""
    return ["INSERT INTO %s SELECT * REPLACE (%s AS %s) FROM %s WHERE id = %d" % (table, expr, column, table, row),
            "DELETE FROM %s WHERE rowid = (SELECT min(rowid) FROM %s WHERE id = %d)" % (table, table, row)]


@pytest.mark.parametrize("name,column,expr,row", VALUE_MUTATIONS, ids=[m[0] for m in VALUE_MUTATIONS])
def test_value_mutation_detected(loaded, name, column, expr, row):
    src, dst, _ = loaded
    stmts = replace_row(column, expr, row)
    r = run(src, dst, stmts)
    t = table(r, "public.types_k")
    record(name, stmts, True, r, {"table": t.source, "missing_extra_different":
                                  (t.missing_rows, t.extra_rows, t.different_rows), "sample": t.sample["different"]})
    assert not r.equal
    assert (t.missing_rows, t.extra_rows, t.different_rows) == (0, 0, 1), t.sample
    diff = t.sample["different"][0]
    assert diff["key"] == {"id": str(row)}
    assert list(diff["columns"]) == [column], diff


def test_char_trailing_spaces_are_not_a_difference(loaded):
    src, dst, _ = loaded
    stmts = replace_row("c_char", "'ab    '", 2)
    r = run(src, dst, stmts)
    record("char_trailing_spaces", stmts, False, r, {"note": "char(n) ignores trailing spaces"})
    assert r.equal, r.summary()


def test_numeric_display_scale_is_not_a_difference(loaded):
    """1.50 and 1.5 are the same numeric value; the destination cannot keep a per-value display scale."""
    src, dst, _ = loaded
    stmts = replace_row("c_num_u", "'1.5000'::VARCHAR", 2)
    r = run(src, dst, stmts)
    record("numeric_display_scale", stmts, False, r, {"note": "value equality for numeric"})
    assert r.equal, r.summary()


# ------------------------------------------------------------------------------------------------ schema

def test_missing_column(loaded):
    src, dst, _ = loaded
    stmts = ["ALTER TABLE public.types_k DROP COLUMN c_text"]
    r = run(src, dst, stmts)
    t = table(r, "public.types_k")
    record("missing_column", stmts, True, r, {"missing_columns": t.missing_columns})
    assert not r.equal and t.missing_columns == ["c_text"] and "c_text" not in t.compared_columns


def test_extra_column(loaded):
    src, dst, _ = loaded
    stmts = ["ALTER TABLE public.types_k ADD COLUMN zz INTEGER"]
    r = run(src, dst, stmts)
    t = table(r, "public.types_k")
    record("extra_column", stmts, True, r, {"extra_columns": t.extra_columns})
    assert not r.equal and t.extra_columns == ["zz"]


def test_missing_table(loaded):
    src, dst, _ = loaded
    stmts = ["DROP TABLE public.dups"]
    r = run(src, dst, stmts)
    record("missing_table", stmts, True, r, {"missing_tables": r.missing_tables})
    assert not r.equal and r.missing_tables == ["public.dups"]
    assert table(r, "public.dups").missing_on_destination and table(r, "public.dups").source_rows == 7


def test_extra_table(loaded):
    src, dst, _ = loaded
    stmts = ["CREATE TABLE public.extra_t (a INTEGER)"]
    r = run(src, dst, stmts)
    record("extra_table", stmts, True, r, {"extra_tables": r.extra_tables})
    assert not r.equal and r.extra_tables == ["public.extra_t"]


def test_unaccepted_destination_type(loaded):
    src, dst, _ = loaded
    stmts = ["ALTER TABLE public.types_k ALTER COLUMN c_bool TYPE INTEGER"]
    r = run(src, dst, stmts)
    t = table(r, "public.types_k")
    record("boolean_column_became_integer", stmts, True, r, {"different": t.different_rows})
    assert not r.equal and t.different_rows >= 1


def test_operations_and_staging_schemas_are_ignored(loaded):
    src, dst, _ = loaded
    stmts = ["CREATE SCHEMA _cdc", "CREATE TABLE _cdc.stream_state (slot VARCHAR, applied_lsn VARCHAR)",
             "CREATE SCHEMA _cdc_stage_7", "CREATE TABLE _cdc_stage_7.t (a INTEGER)"]
    r = run(src, dst, stmts)
    record("cdc_and_staging_schemas", stmts, False, r, {"extra_tables": r.extra_tables})
    assert r.equal, r.summary()


def test_committed_mutation_and_restore(loaded):
    """The default path (the oracle opens its own destination transaction) sees committed changes."""
    src, dst, _ = loaded
    c = dst.conn
    c.execute("CREATE TABLE public.saved AS SELECT * FROM public.types_k WHERE id = 4")
    c.execute("DELETE FROM public.types_k WHERE id = 4")
    try:
        r = compare(src.source(), dst.destination(), OracleConfig(**CFG))
        assert not r.equal and table(r, "public.types_k").missing_rows == 1
        assert r.extra_tables == ["public.saved"]
    finally:
        c.execute("INSERT INTO public.types_k SELECT * FROM public.saved")
        c.execute("DROP TABLE public.saved")
    assert compare(src.source(), dst.destination(), OracleConfig(**CFG)).equal


def test_table_subset_and_mapping(loaded):
    src, dst, _ = loaded
    r = compare(src.source(), dst.destination(), OracleConfig(tables=["other.big"], **CFG))
    assert r.equal and [t.source for t in r.tables] == ["other.big"]
    r = compare(src.source(), dst.destination(), OracleConfig(
        tables=["public.dups"], table_map=lambda s, t: (s, "types_k"), **CFG))
    assert not r.equal                                   # dups compared against types_k: schemas differ


def test_compare_reading_from_the_replica(loaded, shared_pg):
    src, dst, _ = loaded
    shared_pg.wait_replica_caught_up()
    r = compare(src.source(node="replica"), dst.destination(), OracleConfig(**CFG))
    assert r.equal and r.source_in_recovery is True


# ------------------------------------------------------------------------------------------------ canonical forms

PG_VALUES = [
    ("integer", "'-2147483648'", -2147483648), ("bigint", "9223372036854775807", 9223372036854775807),
    ("boolean", "true", True), ("numeric", "1.50", decimal.Decimal("1.50")), ("numeric", "'NaN'", decimal.Decimal("NaN")),
    ("numeric", "'-Infinity'", decimal.Decimal("-Infinity")), ("numeric", "'-0.000'", decimal.Decimal("0")),
    ("numeric(10,3)", "100.000", decimal.Decimal("100")),
    ("double precision", "1e15", 1e15), ("double precision", "'-0'", -0.0), ("double precision", "'NaN'", float("nan")),
    ("double precision", "5e-324", 5e-324), ("double precision", "1.7976931348623157e308", 1.7976931348623157e308),
    ("double precision", "0.1", 0.1), ("real", "0.1", 0.1), ("real", "16777216", 16777216.0), ("real", "'-0'", -0.0),
    ("real", "3.4028235e38", 3.4028235e38), ("real", "1e-45", 1e-45), ("real", "'Infinity'", float("inf")),
    ("text", "''", ""), ("varchar(5)", "'héllo'", "héllo"), ("char(5)", "'ab'", "ab"),
    ("bytea", "'\\x00ff'", b"\x00\xff"), ("date", "'2024-01-02'", dt.date(2024, 1, 2)),
    ("date", "'infinity'", "infinity"), ("date", "'0001-01-01'", dt.date(1, 1, 1)),
    ("time", "'24:00:00'", dt.timedelta(hours=24)), ("time", "'01:02:03.000004'", dt.time(1, 2, 3, 4)),
    ("timestamp", "'2024-01-01 00:00:00.000001'", dt.datetime(2024, 1, 1, 0, 0, 0, 1)),
    ("timestamp", "'-infinity'", "-infinity"),
    ("timestamptz", "'2000-02-29 23:59:59.999999+05:30'",
     dt.datetime(2000, 2, 29, 23, 59, 59, 999999, tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30)))),
    ("interval", "'1 year 2 mons 3 days 04:05:06.000007'", (14, 3, 14706000007)),
    ("interval", "'-2 years -3 mons +4 days -05:06:07'", (-27, 4, -18367000000)),
    ("interval", "'infinity'", "infinity"),
    ("uuid", "'6F9619FF-8B86-D011-B42D-00C04FC964FF'", uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")),
    ("json", "'{\"b\": 1,  \"a\":2}'", '{"b": 1,  "a":2}'), ("jsonb", "'{\"b\": 1,  \"a\":2}'", '{"a": 2, "b": 1}'),
    ("integer[]", "'{1,NULL,3}'", [1, None, 3]), ("integer[]", "'{}'", []),
    ("integer[]", "'{{1,2},{3,4}}'", [[1, 2], [3, 4]]), ("text[]", "'{\"\",NULL}'", ["", None]),
    ("double precision[]", "'{-0,NaN}'", [-0.0, float("nan")]),
]


def test_postgres_canonical_forms_match_the_reference(source_db, evidence):
    out = []
    with source_db.connect() as c:
        c.execute("SET extra_float_digits = 1")
        ver = int(c.execute("SHOW server_version_num").fetchone()[0])
        for typ, literal, value in PG_VALUES:
            base, elem = c.execute(
                "SELECT format_type(t.oid, NULL), CASE WHEN t.typcategory = 'A' THEN format_type(t.typelem, NULL) END "
                "FROM pg_type t WHERE t.oid = %s::regtype", (typ,)).fetchone()
            t = PgColumnType(base, elem)
            # OFFSET 0 keeps the value from becoming a constant: Postgres folds constant CASE arms at planning
            # time (for example `'infinity'::date - DATE '1970-01-01'`), which never happens for table columns
            got = c.execute("SELECT %s FROM (SELECT %s::%s AS v OFFSET 0) s" % (
                pg_canon("v", t, Ctx(ver)), literal, typ)).fetchone()[0]
            want = py_canon(value, t)
            out.append({"type": typ, "literal": literal, "canonical": got, "reference": want, "match": got == want})
    evidence("p3/oracle").write_json("canonical_postgres", {"values": out,
                                                            "all_match": all(o["match"] for o in out)})
    assert all(o["match"] for o in out), [o for o in out if not o["match"]]


# ------------------------------------------------------------------------------------------------ a DuckDB finding

EQUAL_UPDATE_CASES = [
    # (label, column type, old value, new value, probe: representation of x)
    ("double -0 to 0", "DOUBLE", "'-0'::DOUBLE", "0.0", "signbit(x)"),
    ("real -0 to 0", "FLOAT", "'-0'::FLOAT", "0.0", "signbit(x)"),
    ("interval 1 month to 30 days", "INTERVAL", "to_months(1)", "INTERVAL 30 DAY",
     "datepart('month', x) || ':' || datepart('day', x)"),
]


def test_duckdb_update_of_an_equal_comparing_value(dest_db, evidence):
    """Records what DuckDB does when an UPDATE writes a value that compares equal to the stored one but has a
    different representation. Seen on 2026-11-10, locally and on MotherDuck: the UPDATE is skipped and the old
    representation stays (in a table loaded in bulk on MotherDuck one -0 update was applied, so it depends on the
    storage state). The pipeline must not rely on UPDATE for such values; DELETE + INSERT always writes the new
    representation (asserted)."""
    import duckdb
    out = []
    for where, conn in (("local", duckdb.connect()), ("motherduck", dest_db.conn)):
        for i, (label, typ, old, new, probe) in enumerate(EQUAL_UPDATE_CASES):
            t = "eq_%s_%d" % (where, i)
            conn.execute("CREATE TABLE %s (id INTEGER, x %s)" % (t, typ))
            conn.execute("INSERT INTO %s VALUES (1, %s), (2, %s)" % (t, old, old))
            before = conn.execute("SELECT %s FROM %s WHERE id = 1" % (probe, t)).fetchone()[0]
            conn.execute("UPDATE %s SET x = %s WHERE id = 1" % (t, new))
            after_update = conn.execute("SELECT %s FROM %s WHERE id = 1" % (probe, t)).fetchone()[0]
            conn.execute("DELETE FROM %s WHERE id = 2" % t)
            conn.execute("INSERT INTO %s VALUES (2, %s)" % (t, new))
            after_replace = conn.execute("SELECT %s FROM %s WHERE id = 2" % (probe, t)).fetchone()[0]
            out.append({"where": where, "case": label, "before": str(before), "after_update": str(after_update),
                        "update_changed_representation": after_update != before,
                        "after_delete_insert": str(after_replace),
                        "delete_insert_changed_representation": after_replace != before})
    evidence("p3/oracle").write_json("duckdb_equal_update", {"duckdb": duckdb.__version__, "cases": out})
    assert all(o["delete_insert_changed_representation"] for o in out), out
