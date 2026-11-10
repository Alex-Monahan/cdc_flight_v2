"""Oracle building blocks that need no server (T0): the multiset row difference, ctid ranges, positions and the wait
for an applied position (against a local DuckDB), and the safety checks on connections."""
import duckdb
import pytest

from cdc_v2.oracle import canonical as C
from cdc_v2.oracle import comparison as K
from cdc_v2.deadline import Deadline, DeadlineExceeded
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


def test_diff_counts_with_capped_values_is_truncated_but_exact():
    from collections import Counter
    p = plan()
    sv = {h: (a, b) for h, a, b in rows(("1", "a"))}
    missing = Counter({h: 1 for h in sv})
    missing[12345] = 3                                      # values of this hash were not kept
    r = K.TableResult("public.t", "public.t")
    K._diff_counts(p, missing, sv, Counter(), {}, r, 10)
    assert r.missing_rows == 4 and r.truncated and len(r.sample["missing"]) == 1


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

def test_as_position():
    assert P.as_position("16/B374D848") == (0x16 << 32) + 0xB374D848
    assert P.as_position(12345) == 12345 and P.as_position(None) is None
    for bad in ("not an lsn", True):
        with pytest.raises(ValueError):
            P.as_position(bad)


def stream_state(con, kind="UBIGINT"):
    con.execute("CREATE SCHEMA _cdc")
    con.execute("CREATE TABLE _cdc.stream_state (slot_name VARCHAR, applied_lsn %s)" % kind)


def test_stand_in_reads_the_row_of_the_named_slot():
    con = duckdb.connect()
    assert P.stand_in_applied_position(con, "s1") is None          # no _cdc schema yet
    stream_state(con)
    assert P.stand_in_applied_position(con, "s1") is None          # no row yet
    con.execute("INSERT INTO _cdc.stream_state VALUES ('s1', 256), ('s2', 4096)")
    assert P.stand_in_applied_position(con, "s1") == 256           # not the maximum over all slots
    con2 = duckdb.connect()
    stream_state(con2, "VARCHAR")
    con2.execute("INSERT INTO _cdc.stream_state VALUES ('s1', '0/100')")
    assert P.stand_in_applied_position(con2, "s1") == 0x100


def test_wait_for_applied_returns_when_reached():
    con = duckdb.connect()
    stream_state(con)
    con.execute("INSERT INTO _cdc.stream_state VALUES ('s1', 256)")
    calls = []

    def reader(c, slot):
        calls.append(slot)
        if len(calls) == 3:
            c.execute("UPDATE _cdc.stream_state SET applied_lsn = 512 WHERE slot_name = 's1'")
        return P.stand_in_applied_position(c, slot)

    assert P.wait_for_applied(con, 384, Deadline.after(30), "s1", applied_position=reader, poll=0.0) == 512
    assert calls == ["s1"] * 3


def test_wait_for_applied_raises_deadline_exceeded():
    with pytest.raises(DeadlineExceeded):
        P.wait_for_applied(None, 0x10, Deadline.after(0.2), "s1", applied_position=lambda c, s: 0x5, poll=0.05)


# --------------------------------------------------------------------------------------------- the comparison API

class _Fail(Exception):
    pass


def test_a_position_or_unpositioned_must_be_asked_for():
    src, dst = K.Source(host="localhost", port=55432, dbname="x"), K.Destination("cdc_v2_t_x")
    with pytest.raises(K.OracleError, match="position"):
        K.compare(src, dst)
    with pytest.raises(K.OracleError, match="slot_name"):
        K.compare(src, dst, position=5)
    with pytest.raises(K.OracleError, match="not both"):
        K.compare(src, dst, position=5, unpositioned=True)


def test_an_empty_table_list_is_refused():
    src, dst = K.Source(host="localhost", port=55432, dbname="x"), K.Destination("cdc_v2_t_x")
    with pytest.raises(K.OracleError, match="empty"):
        K.compare(src, dst, K.OracleConfig(tables=[]), unpositioned=True)


def test_recovery_conflicts_restart_the_comparison(monkeypatch):
    calls = []

    class Conflict(Exception):
        sqlstate = "40001"

    def once(*a):
        calls.append(a[-1])
        if len(calls) < 3:
            raise Conflict()
        return "result"

    monkeypatch.setattr(K, "_compare_once", once)
    src, dst = K.Source(host="localhost", port=55432, dbname="x"), K.Destination("cdc_v2_t_x")
    assert K.compare(src, dst, unpositioned=True) == "result" and calls == [1, 2, 3]
    calls.clear()
    with pytest.raises(Conflict):
        K.compare(src, dst, K.OracleConfig(conflict_retries=1), unpositioned=True)
    assert calls == [1, 2]
    monkeypatch.setattr(K, "_compare_once", lambda *a: (_ for _ in ()).throw(_Fail()))
    with pytest.raises(_Fail):                                       # other errors are not retried
        K.compare(src, dst, unpositioned=True)


def test_destination_statements_have_deadlines():
    from cdc_v2.oracle import mdexec
    con = duckdb.connect()
    with pytest.raises(DeadlineExceeded):
        mdexec.execute(con, "SELECT count(*) FROM range(100000000000) a", deadline=Deadline.after(0.3))
    assert mdexec.execute(con, "SELECT 42", deadline=Deadline.after(5), fetch="one") == (42,)


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
