"""Plans: every baseline family, the fallback, names and collisions, hash, forms and widening, PlanMismatch (T0)."""
import duckdb
import pyarrow as pa
import pytest

from cdc_v2 import transform as T
from cdc_v2.oracle import canonical as C
from cdc_v2.stream import frames as Fr
from tables import array, citext, col, composite, domain, enum, numeric_typmod, one, pgtype, table

V18 = 180006

FAMILIES = [
    ("bool", -1, "boolean", "BOOLEAN", None),
    ("int2", -1, "integer", "SMALLINT", None),
    ("int4", -1, "integer", "INTEGER", None),
    ("int8", -1, "integer", "BIGINT", None),
    ("float4", -1, "real", "FLOAT", None),
    ("float8", -1, "double", "DOUBLE", None),
    ("numeric", -1, "numeric", "DECIMAL(38,10)", "UNION(n DECIMAL(38,10), t VARCHAR)"),
    ("numeric", numeric_typmod(15, 2), "numeric", "DECIMAL(15,2)", "UNION(n DECIMAL(15,2), t VARCHAR)"),
    ("numeric", numeric_typmod(5, -2), "numeric", "DECIMAL(7,0)", "UNION(n DECIMAL(7,0), t VARCHAR)"),
    ("numeric", numeric_typmod(3, 5), "numeric", "DECIMAL(6,5)", "UNION(n DECIMAL(6,5), t VARCHAR)"),
    ("numeric", numeric_typmod(50, 20), "numeric", "DECIMAL(38,20)", "UNION(n DECIMAL(38,20), t VARCHAR)"),
    ("text", -1, "text", "VARCHAR", None),
    ("varchar", 14, "text", "VARCHAR", None),
    ("bpchar", 9, "text", "VARCHAR", None),
    ("name", -1, "text", "VARCHAR", None),
    ("char", -1, "text", "VARCHAR", None),
    ("bytea", -1, "bytea", "BLOB", None),
    ("date", -1, "date", "DATE", None),
    ("timestamp", -1, "timestamp", "TIMESTAMP", "UNION(n TIMESTAMP, t VARCHAR)"),
    ("timestamptz", -1, "timestamptz", "TIMESTAMP WITH TIME ZONE", "UNION(n TIMESTAMP WITH TIME ZONE, t VARCHAR)"),
    ("time", -1, "time", "TIME", None),
    ("timetz", -1, "timetz", "TIME WITH TIME ZONE", None),
    ("interval", -1, "interval", "INTERVAL", "UNION(n INTERVAL, t VARCHAR)"),
    ("uuid", -1, "uuid", "UUID", None),
    ("json", -1, "json", "VARCHAR", None),
    ("jsonb", -1, "json", "VARCHAR", None),
    ("money", -1, "fallback", "VARCHAR", None),
    ("oid", -1, "fallback", "VARCHAR", None),
    ("inet", -1, "fallback", "VARCHAR", None),
    ("point", -1, "fallback", "VARCHAR", None),
    ("xml", -1, "fallback", "VARCHAR", None),
    ("varbit", -1, "fallback", "VARCHAR", None),
]


@pytest.mark.parametrize("typ,typmod,family,native,wide", FAMILIES)
def test_plan_for_every_baseline_family_and_the_fallback(typ, typmod, family, native, wide):
    p = T.plan(one(typ, typmod), server_version_num=V18)
    c = p.columns[0]
    assert (c.family, c.native_type, c.wide_type, c.form, c.dest_type) == (family, native, wide, "native", native)
    assert c.dest_name == "c" and c.source_name == "c"
    if wide:
        w = p.widened(0)
        assert w.columns[0].form == "wide" and w.columns[0].dest_type == wide and w.hash != p.hash
        assert w.widened(0) is w
    else:
        with pytest.raises(ValueError):
            p.widened(0)
    # the destination type is one DuckDB accepts, and its Arrow type is what DuckDB exports
    con = duckdb.connect()
    con.execute("SET arrow_lossless_conversion = true")
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE TABLE x (%s)" % p.column_ddl())
    assert con.execute("SELECT * FROM x").to_arrow_table().schema.field(0).type in (
        T.arrow_type(native), pa.bool8())


def test_domains_arrays_enums_composites_and_extension_types_get_a_plan():
    t = table([col(1, "d", domain("int4"), type_name="d"), col(2, "dn", domain("numeric"), type_name="dn"),
               col(3, "a", array("int4"), type_name="integer[]"), col(4, "e", enum(), type_name="mood"),
               col(5, "r", composite(), type_name="pair"), col(6, "ci", citext(), type_name="citext")])
    p = T.plan(t, server_version_num=V18)
    got = [(c.family, c.dest_type, c.stream_forms) for c in p.columns]
    assert got == [("integer", "INTEGER", ("int32",)),
                   ("numeric", "DECIMAL(38,10)", ("numeric_text",)),      # a domain's typmod is not in the catalog
                   ("fallback", "VARCHAR", ("array_text", "text")),
                   ("enum", "VARCHAR", ("text",)),
                   ("fallback", "VARCHAR", ("text", "array_text")),
                   ("text", "VARCHAR", ("text",))]


def test_key_meta_create_columns_and_hash():
    t = table([col(1, "id", "int8"), col(3, "v", "text"), col(4, "n", "numeric", numeric_typmod(15, 2))], key=(1,))
    p = T.plan(t, server_version_num=V18)
    assert p.has_key and p.key_positions == (0,) and [c.key for c in p.columns] == [True, False, False]
    assert [c.attnum for c in p.columns] == [1, 3, 4]
    meta = p.table_meta()
    assert meta.has_key and [(m.position, m.name, m.type, m.key, m.unchanged_possible) for m in meta.columns] == [
        (0, "id", "BIGINT", True, False), (1, "v", "VARCHAR", False, True), (2, "n", "DECIMAL(15,2)", False, True)]
    assert p.create_columns() == (('"id"', "BIGINT"), ('"v"', "VARCHAR"), ('"n"', "DECIMAL(15,2)"))
    # stable: the same input gives the same hash; the minor version does not count, the major does
    assert T.plan(t, server_version_num=V18).hash == p.hash == T.plan(t, server_version_num=180002).hash
    assert T.plan(t, server_version_num=170004).hash != p.hash
    assert hash(p) == hash(T.plan(t, server_version_num=V18))
    with pytest.raises(Exception):
        p.schema = "other"                                  # frozen


def test_ambiguous_positions_depend_on_the_server_version():
    t = table([col(1, "id", "int4"), col(2, "iv", "interval"), col(3, "ia", array("interval"), type_name="x")],
              key=(1,))
    assert T.plan(t, server_version_num=170000).ambiguous == (1,)
    assert T.plan(t, server_version_num=180006).ambiguous == (1,)
    assert T.plan(t, server_version_num=160006).ambiguous == ()


NAMES = ['"quoted"', "dot.ted", "with space", "select", "ünïcødé", "x" * 63, "seq", "op", "a1", "b0", "u0", "k0",
         "", " ", "\"", "Ä"]


def test_names_are_kept_exactly_and_work_in_duckdb():
    names = [n for n in NAMES if n]
    t = table([col(i + 1, n, "int4") for i, n in enumerate(names)])
    p = T.plan(t, server_version_num=V18)
    assert [c.dest_name for c in p.columns] == names and p.collisions == ()
    con = duckdb.connect()
    con.execute("CREATE TABLE x (%s)" % p.column_ddl())
    assert [r[0] for r in con.execute("SELECT column_name FROM duckdb_columns() WHERE table_name = 'x' "
                                      "ORDER BY column_index").fetchall()] == names


def test_collisions_are_reported_with_deterministic_names():
    names = ["A", "a", "b", "a_1", "K", "k", "ss", "ß", "SS"]
    dn, groups = T.dest_names(names)
    assert groups == ((0, 1), (4, 5), (6, 7, 8))
    assert dn == ("A", "a_1_", "b", "a_1", "K", "k_5", "ss", "ß_7", "SS_8")
    t = table([col(i + 1, n, "int4") for i, n in enumerate(names)])
    p = T.plan(t, server_version_num=V18)
    assert p.collisions == groups and tuple(c.dest_name for c in p.columns) == dn
    con = duckdb.connect()
    con.execute("CREATE TABLE x (%s)" % p.column_ddl())          # the destination names fit into one table


@pytest.mark.parametrize("pair", [("A", "a"), ("Ab", "aB"), ("Z_\u00e9", "z_\u00e9")])
def test_every_name_pair_duckdb_refuses_is_a_reported_collision(pair):
    """Probed pairs; the plan's folding (Unicode casefold) is a superset: 'É'/'é', the Kelvin sign and 'ß'/'ss'
    are reported although DuckDB 1.5.6 holds them apart (test below)."""
    con = duckdb.connect()
    with pytest.raises(duckdb.Error):
        con.execute('CREATE TABLE x ("%s" INT, "%s" INT)' % pair)
    assert T.dest_names(list(pair))[1] == ((0, 1),)


@pytest.mark.parametrize("pair", [("\u00c9", "\u00e9"), ("\u212a", "k"), ("\u00df", "ss")])
def test_collisions_are_a_superset_of_what_duckdb_refuses(pair):
    con = duckdb.connect()
    con.execute('CREATE TABLE x ("%s" INT, "%s" INT)' % pair)          # DuckDB holds these apart
    assert T.dest_names(list(pair))[1] == ((0, 1),)                     # reported anyway (conservative)


def _frame(p, change=None):
    change = change or {}
    cols = []
    for c in p.columns:
        d = dict(position=c.position, name=c.source_name, connect_type="", semantic_name="", form=c.stream_forms[0],
                 physical=0, source_type=c.base_type, length=-1, scale=-1, nullable=True, key=c.key,
                 unchanged=c.unchanged_possible)
        d.update(change.get(c.position, {}))
        cols.append(Fr.Column(**d))
    return Fr.Schema(p.schema, p.table, 1, change.get("has_key", p.has_key), change.get("keys", p.key_positions),
                     tuple(cols))


def test_plan_mismatch_names_each_difference():
    t = table([col(1, "id", "int4"), col(2, "v", "numeric")], key=(1,))
    p = T.plan(t, server_version_num=V18)
    assert T.plan(t, _frame(p), server_version_num=V18).hash == p.hash        # the frame changes nothing
    with pytest.raises(T.PlanMismatch) as e:
        T.plan(t, _frame(p, {1: {"name": "w", "form": "text"}}), server_version_num=V18)
    assert {(d[0], d[1]) for d in e.value.differences} == {(1, "name"), (1, "transport form")}
    with pytest.raises(T.PlanMismatch) as e:
        T.plan(t, _frame(p, {0: {"key": False}, "has_key": False, "keys": ()}), server_version_num=V18)
    assert {d[1] for d in e.value.differences} == {"key", "has key", "key positions"}
    f = _frame(p)
    short = Fr.Schema(f.schema, f.table, 1, f.has_key, f.key_positions, f.columns[:1])
    with pytest.raises(T.PlanMismatch) as e:
        T.plan(t, short, server_version_num=V18)
    assert e.value.differences[0][1] == "column count"


# ---------------------------------------------------------------------------------------- widening expressions

WIDEN_VALUES = {
    ("numeric", -1): ["1.5", "-0.0000000001", "9999999999999999999999999999.9999999999", None],
    ("numeric", numeric_typmod(15, 2)): ["1.50", "-9999999999999.99", None],
    ("timestamp", -1): ["TIMESTAMP '2020-01-01 00:00:00.000001'", "TIMESTAMP 'infinity'",
                        "TIMESTAMP '-infinity'", "make_timestamp(CAST(-210866803200000000 AS BIGINT))", None],
    ("timestamptz", -1): ["TIMESTAMPTZ '2020-01-01 00:00:00.000001+00'", "TIMESTAMPTZ '-infinity'", None],
    ("interval", -1): ["INTERVAL '1 month'", "INTERVAL '30 days'", "to_months(-5) + to_days(3) + to_microseconds(-7)",
                       None],
}
PG_BASE = {"numeric": "numeric", "timestamp": "timestamp without time zone",
           "timestamptz": "timestamp with time zone", "interval": "interval"}


@pytest.mark.parametrize("typ,typmod", list(WIDEN_VALUES))
def test_widening_expression_changes_no_value_under_the_oracle(typ, typmod):
    p = T.plan(one(typ, typmod), server_version_num=V18)
    c = p.columns[0]
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE TABLE x (id INTEGER, %s)" % p.column_ddl())
    for i, v in enumerate(WIDEN_VALUES[(typ, typmod)]):
        lit = "NULL" if v is None else (v if not typ == "numeric" else "'%s'" % v)
        con.execute("INSERT INTO x VALUES (%d, CAST(%s AS %s))" % (i, lit, c.native_type))
    pt = C.PgColumnType(PG_BASE[typ])

    def canon(dtype):
        return con.execute("SELECT id, %s FROM x ORDER BY id" % C.md_canon('"c"', pt, C.parse_md_type(dtype))
                           ).fetchall()

    before = canon(c.native_type)
    con.execute('ALTER TABLE x ALTER COLUMN "c" TYPE %s USING %s' % (c.wide_type, p.widen_sql(0)))
    assert con.execute("SELECT data_type FROM duckdb_columns() WHERE column_name = 'c'").fetchone()[0] \
        .replace(" ", "") == c.wide_type.replace(" ", "")
    assert canon(c.wide_type) == before
    nulls = con.execute('SELECT id FROM x WHERE "c" IS NULL').fetchall()
    assert nulls == [(len(WIDEN_VALUES[(typ, typmod)]) - 1,)]                # a real NULL, not a NULL member
