"""Oracle building blocks that need no server (T0): the multiset row difference, ctid ranges, positions and the wait
for an applied position (against a local DuckDB), and the safety checks on connections."""
import duckdb
import pytest

from cdc_v2.oracle import canonical as C
from cdc_v2.oracle import comparison as K
from cdc_v2.oracle import position as P
from cdc_v2.oracle.catalog import DestTable, SourceColumn, SourceTable


def plan(key=("id",)):
    cols = [("id", C.PgColumnType("integer"), C.parse_md_type("INTEGER")),
            ("v", C.PgColumnType("text"), C.parse_md_type("VARCHAR"))]
    return K.Plan(SourceTable("public", "t", 1, "r"), DestTable("public", "t"), cols, list(key), 1)


def rows(*tuples):
    """(id, v) canonical tuples -> rows (hash, id, v) as the row pass returns them."""
    return [(C.py_hash60(C.py_token(a) + C.py_token(b)), a, b) for a, b in tuples]


def diff(p, src, dst, limit=10):
    r = K.TableResult("public.t", "public.t")
    K._row_diff(p, src, dst, r, limit)
    return r


def test_row_diff_equal():
    r = diff(plan(), rows(("1", "a"), ("2", "b")), rows(("2", "b"), ("1", "a")))
    assert (r.missing_rows, r.extra_rows, r.different_rows) == (0, 0, 0)


def test_row_diff_missing_extra_different():
    r = diff(plan(), rows(("1", "a"), ("2", "b"), ("3", "c")), rows(("1", "a"), ("2", "B"), ("4", "d")))
    assert (r.missing_rows, r.extra_rows, r.different_rows) == (1, 1, 1)
    assert r.sample["missing"] == [{"id": "3", "v": "c"}]
    assert r.sample["extra"] == [{"id": "4", "v": "d"}]
    assert r.sample["different"] == [{"key": {"id": "2"}, "columns": {"v": {"source": "b", "destination": "B"}}}]


def test_row_diff_duplicate_key_on_destination_is_extra():
    r = diff(plan(), rows(("1", "a")), rows(("1", "a"), ("1", "a")))
    assert (r.missing_rows, r.extra_rows, r.different_rows) == (0, 1, 0)


def test_row_diff_keyless_is_a_multiset():
    p = plan(key=())
    r = diff(p, rows(("1", "x"), ("1", "x"), ("1", "x"), ("2", None)), rows(("1", "x"), ("1", "x"), ("2", None)))
    assert (r.missing_rows, r.extra_rows, r.different_rows) == (1, 0, 0)
    r = diff(p, rows(("1", "x")), rows(("1", "x"), ("1", "x")))
    assert (r.missing_rows, r.extra_rows) == (0, 1)
    # NULL and the empty string are different values
    r = diff(p, rows(("1", None)), rows(("1", "")))
    assert (r.missing_rows, r.extra_rows) == (1, 1)


def test_row_diff_sample_is_bounded():
    src = rows(*[(str(i), "a") for i in range(50)])
    r = diff(plan(), src, [], limit=3)
    assert r.missing_rows == 50 and len(r.sample["missing"]) == 3


def test_ctid_ranges():
    assert K.ctid_ranges(10, 4, 256) == [""]
    rs = K.ctid_ranges(10_000, 4, 256)
    assert len(rs) == 8
    assert rs[0] == " WHERE ctid >= '(0,0)'::tid AND ctid < '(1250,0)'::tid"
    assert rs[-1] == " WHERE ctid >= '(8750,0)'::tid"          # open-ended: covers pages added later
    assert K.ctid_ranges(10_000, 1, 256) == [""]


def test_checksum_sql_shapes():
    p = plan()
    canon, row, key = K._md_exprs(p)
    inner = K._inner(C.hash60_md, row, key, 7, '"public"."t"', "", False)
    sql = K.checksum_sql(inner)
    assert "GROUP BY" in sql and "% 7" in sql
    keyless = K._inner(C.hash60_md, row, None, 7, '"public"."t"', "", True)
    assert keyless.count("md5(") == 1                       # the row hash is computed once per row


def test_compare_on_local_duckdb_tables_only():
    """The destination half of the SQL runs on a real DuckDB."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t (id INTEGER, v VARCHAR)")
    con.execute("INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, '')")
    p = K.Plan(SourceTable("main", "t", 1, "r"), DestTable("main", "t"),
               [("id", C.PgColumnType("integer"), C.parse_md_type("INTEGER")),
                ("v", C.PgColumnType("text"), C.parse_md_type("VARCHAR"))], ["id"], 2)
    canon, row, key = K._md_exprs(p)
    got = con.execute(K.checksum_sql(K._inner(C.hash60_md, row, key, 2, '"main"."t"', "", False))).fetchall()
    assert sum(n for _, n, _ in got) == 3
    expect = {}
    for i, v in [("1", "a"), ("2", None), ("3", "")]:
        b = C.py_hash60(C.py_token(i)) % 2
        n, s = expect.get(b, (0, 0))
        expect[b] = (n + 1, s + C.py_hash60(C.py_token(i) + C.py_token(v)))
    assert {b: (n, int(s)) for b, n, s in got} == expect
    picked = con.execute(K.rows_sql(K._inner(C.hash60_md, row, key, 2, '"main"."t"', "", True), canon, p.columns,
                                    [0, 1])).fetchall()
    assert sorted((r[1], r[2]) for r in picked) == [("1", "a"), ("2", None), ("3", "")]


# --------------------------------------------------------------------------------------------- positions

@pytest.mark.parametrize("text,value", [("0/0", 0), ("0/3000EE8", 0x3000EE8), ("16/B374D848", (0x16 << 32) + 0xB374D848)])
def test_lsn_parse_format(text, value):
    assert P.parse_lsn(text) == value
    assert P.format_lsn(value) == text.upper() if text != "0/0" else "0/0"
    assert P.parse_lsn(value) == value


def test_lsn_rejects_garbage():
    with pytest.raises(ValueError):
        P.parse_lsn("not an lsn")
    with pytest.raises(ValueError):
        P.parse_lsn(True)


def test_wait_for_applied_reads_stream_state():
    con = duckdb.connect()
    assert P.applied_lsn_from_stream_state(con) is None           # no _cdc schema yet
    con.execute("CREATE SCHEMA _cdc")
    con.execute("CREATE TABLE _cdc.stream_state (slot VARCHAR, applied_lsn VARCHAR)")
    assert P.applied_lsn_from_stream_state(con) is None
    con.execute("INSERT INTO _cdc.stream_state VALUES ('s1', '0/100'), ('s2', '0/80')")
    assert P.applied_lsn_from_stream_state(con) == 0x100

    calls = []

    def sleep(_):
        calls.append(1)
        if len(calls) == 3:
            con.execute("UPDATE _cdc.stream_state SET applied_lsn = '0/200' WHERE slot = 's1'")

    assert P.wait_for_applied(con, "0/180", timeout=60, poll=0.0, sleep=sleep) == 0x200
    assert len(calls) == 3


def test_wait_for_applied_times_out_and_lookup_is_replaceable():
    now = [0.0]

    def clock():
        return now[0]

    def sleep(dt):
        now[0] += 1.0

    with pytest.raises(P.PositionTimeout):
        P.wait_for_applied(None, "0/10", timeout=5, lookup=lambda c: 0x5, sleep=sleep, clock=clock)
    assert P.wait_for_applied(None, 16, lookup=lambda c: 16) == 16


def test_integer_applied_lsn_column():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA _cdc")
    con.execute("CREATE TABLE _cdc.stream_state (applied_lsn UBIGINT)")
    con.execute("INSERT INTO _cdc.stream_state VALUES (12345)")
    assert P.applied_lsn_from_stream_state(con) == 12345


# --------------------------------------------------------------------------------------------- safety

def test_destination_name_must_be_project_prefixed():
    with pytest.raises(K.OracleError):
        K.Destination("somebody_elses_db")
    K.Destination("cdc_v2_t_abc")                                  # no connection is made here


def test_source_port_must_be_named_and_not_5432():
    with pytest.raises(K.OracleError):
        K.Source(host="localhost", dbname="x")
    with pytest.raises(K.OracleError):
        K.Source(host="localhost", port=5432, dbname="x")
    K.Source(host="localhost", port=55432, dbname="x")


def test_plan_reports_schema_differences():
    src = SourceTable("public", "t", 1, "r", columns=[
        SourceColumn("id", C.PgColumnType("integer"), "integer"), SourceColumn("gone", C.PgColumnType("text"), "text"),
        SourceColumn("m", C.PgColumnType("money"), "money")], key=["id"], est_rows=1000)
    from cdc_v2.oracle.catalog import DestColumn
    dst = DestTable("public", "t", [DestColumn("id", C.parse_md_type("INTEGER")),
                                    DestColumn("m", C.parse_md_type("VARCHAR")),
                                    DestColumn("new", C.parse_md_type("INTEGER"))])
    r = K.TableResult("public.t", "public.t")
    p = K._plan(src, dst, K.OracleConfig(chunk_rows=100), r)
    assert r.missing_columns == ["gone"] and r.extra_columns == ["new"]
    assert r.fallback_columns == ["m (money)"]
    assert p.key == ["id"] and p.buckets == 10
