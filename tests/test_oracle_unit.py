"""Oracle building blocks that need no server (T0): the multiset row difference, ctid ranges, positions and the wait
for an applied position (against a local DuckDB), and the safety checks on connections."""
import concurrent.futures as futures
import threading
import time

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


# ----------------------------------------------------------------- the source snapshot on a standby (export defect)

POSITION_SQL = ("SELECT (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() "
                "ELSE pg_current_wal_insert_lsn() END)::text")
BEGIN_RR = "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY"
SNAPSHOT_SQL = "SELECT pg_current_snapshot()::text"
EXPORT_SQL = "SELECT pg_export_snapshot()"
SETTLE_SQL = "SELECT next_xid, pg_is_in_recovery() FROM pg_control_checkpoint()"


@pytest.fixture
def clock(monkeypatch):
    """The clock of cdc_v2.deadline, in the hands of the test (as in tests/test_deadline.py)."""
    from cdc_v2 import deadline as D
    state = {"now": 0.0}

    def sleep(seconds):
        state["now"] += seconds

    monkeypatch.setattr(D, "_clock", lambda: state["now"])
    monkeypatch.setattr(D, "_sleep", sleep)
    return state


class _FakePg:
    """A Postgres connection that answers the statements of the oracle's source reader and logs them."""

    def __init__(self, in_recovery, version, snapshots=(), floor="0:3"):
        self.in_recovery, self.version, self.floor = in_recovery, version, floor
        self.autocommit = True                   # as Source.connect() opens them
        self.snapshots = iter(snapshots)
        self.log = []
        self.inside = 0
        self.overlaps = 0
        self.lock = threading.Lock()

    def execute(self, sql, params=None):
        self.log.append(sql)
        rows = []
        if sql == "SELECT pg_is_in_recovery()":
            rows = [(self.in_recovery,)]
        elif sql == "SHOW server_version_num":
            rows = [(str(self.version),)]
        elif sql == POSITION_SQL:
            rows = [("0/3000",)]
        elif sql == SNAPSHOT_SQL:
            rows = [(next(self.snapshots),)]
        elif sql == EXPORT_SQL:
            rows = [("00000003-0000001B-1",)]
        elif sql == SETTLE_SQL:
            rows = [(self.floor, self.in_recovery)]
        elif sql.startswith("SELECT 'scan"):
            with self.lock:                      # a scan: count sessions that use this connection at once
                self.inside += 1
                self.overlaps += self.inside > 1
            time.sleep(0.005)
            with self.lock:
                self.inside -= 1
            rows = [(sql,)]

        class Cur:
            def fetchone(self_inner):
                return rows[0] if rows else None

            def fetchall(self_inner):
                return rows
        return Cur()

    def close(self):
        pass


class _FakeSource(K.Source):
    def __init__(self, coord, make_worker):
        self.params = {"port": 55601}
        self.coord, self.make_worker, self.opened = coord, make_worker, []

    def connect(self):
        c = self.coord if not self.opened else self.make_worker()
        self.opened.append(c)
        return c


def _reader(in_recovery, version, snapshots, quiet_wait_s=5.0, floor="0:3", settle_wait_s=60.0):
    coord = _FakePg(in_recovery, version, snapshots, floor)
    src = _FakeSource(coord, lambda: _FakePg(in_recovery, version))
    snap = K._SourceSnapshot(src, 4, 60_000, 0x2000, Deadline.after(60), lambda: Deadline.after(quiet_wait_s),
                             lambda: Deadline.after(settle_wait_s))
    return snap, src, coord


def test_which_way_the_oracle_reads_the_source_follows_recovery_version_and_quiet():
    """On a primary and on a server with the fix the snapshot is exported as before.  On a standby with the export
    defect only a quiet snapshot is exported; one that is not quiet is dropped until the wait is over, and then
    the coordinator reads alone."""
    from cdc_v2.pg import cut
    cases = [(False, 180006, False, False, K.EXPORTED),        # a primary: as before, whatever the snapshot
             (False, 180006, True, True, K.EXPORTED),
             (True, 180007, False, False, K.EXPORTED),         # a standby of a version with the fix
             (True, 190000, False, False, K.EXPORTED),
             (True, 160016, False, True, K.EXPORTED),
             (True, 170012, False, False, K.EXPORTED),
             (True, 180006, True, False, K.QUIET_EXPORTED),    # the defect: a quiet snapshot is exported
             (True, 180006, True, True, K.QUIET_EXPORTED),
             (True, 180006, False, False, None),               # not quiet: drop it and take another
             (True, 170011, False, False, None),
             (True, 180006, False, True, K.ONE_CONNECTION),    # not quiet when the wait is over: one connection
             (True, 160015, False, True, K.ONE_CONNECTION)]
    for in_recovery, version, quiet, over, want in cases:
        assert K.snapshot_way(in_recovery, version, quiet, over) == want, (in_recovery, version, quiet, over)
    for version in list(range(160000, 160030)) + list(range(170000, 170020)) + list(range(180000, 180015)) + [
            150010, 190000, 200001]:                           # the versions come from the library, the one place
        assert (K.snapshot_way(True, version, False, True) == K.ONE_CONNECTION) == cut.export_defect(version)
    assert K.OracleConfig().quiet_wait_s == 5.0


def test_on_a_standby_with_the_defect_the_oracle_exports_only_a_quiet_snapshot(clock):
    snap, src, coord = _reader(True, 180006, ["700:705:", "700:705:", "705:705:"])
    assert snap.way == K.QUIET_EXPORTED and snap.snapshot and len(src.opened) == 5
    assert coord.log.count(BEGIN_RR) == 3 and coord.log.count("ROLLBACK") == 2      # two snapshots were dropped
    assert coord.log.count(EXPORT_SQL) == 1
    assert coord.log[-2:] == [SNAPSHOT_SQL, EXPORT_SQL]         # the quiet snapshot is the one exported
    for i, sql in enumerate(coord.log):                         # the position is read before every snapshot
        if sql == BEGIN_RR:
            assert coord.log[i - 1] == POSITION_SQL and coord.log[i + 1] == SNAPSHOT_SQL
    for w in src.opened[1:]:
        assert w.log[-1] == "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"
    assert clock["now"] == pytest.approx(2 * K.QUIET_POLL_S)


def test_on_a_standby_with_the_defect_and_no_quiet_snapshot_the_oracle_reads_on_one_connection(clock):
    snap, src, coord = _reader(True, 180006, ["700:705:"] * 1000, quiet_wait_s=1.0)
    assert snap.way == K.ONE_CONNECTION and snap.snapshot is None
    assert src.opened == [coord] and snap.workers == [coord]   # no worker connection, nothing imported
    assert EXPORT_SQL not in coord.log and not any("SET TRANSACTION SNAPSHOT" in sql for sql in coord.log)
    assert coord.log[-3:] == [POSITION_SQL, BEGIN_RR, SNAPSHOT_SQL]   # the last snapshot stays open: it is read
    assert clock["now"] == pytest.approx(1.0)
    # scan jobs of several threads share the one connection in turn
    with futures.ThreadPoolExecutor(max_workers=4) as pool:
        got = list(pool.map(snap.run, ["SELECT 'scan %d'" % i for i in range(16)]))
    assert [g[0][0] for g in got] == ["SELECT 'scan %d'" % i for i in range(16)] and coord.overlaps == 0
    snap.close()
    assert coord.log.count("ROLLBACK") == coord.log.count(BEGIN_RR)   # closed once, nothing left open


def test_on_a_primary_the_oracle_reader_is_unchanged(clock):
    for version in (180006, 160015, 180007):
        snap, src, coord = _reader(False, version, [])
        assert snap.way == K.EXPORTED and len(src.opened) == 5
        assert SNAPSHOT_SQL not in coord.log and SETTLE_SQL not in coord.log      # no extra statement
        assert coord.log[-3:] == [POSITION_SQL, BEGIN_RR, EXPORT_SQL]
        assert clock["now"] == 0.0


def test_on_a_standby_with_the_fix_the_oracle_exports_a_settled_snapshot(clock):
    snap, src, coord = _reader(True, 180007, ["700:705:"])
    assert snap.way == K.EXPORTED and len(src.opened) == 5            # not quiet, but the server has the fix
    assert coord.log.index(SETTLE_SQL) < coord.log.index(BEGIN_RR)     # the floor is read before the snapshot
    assert coord.log[-4:] == [POSITION_SQL, BEGIN_RR, SNAPSHOT_SQL, EXPORT_SQL]
    assert clock["now"] == 0.0


def test_on_a_standby_the_oracle_uses_only_a_settled_snapshot(clock):
    """A standby that has just started can hide a commit that lies before its replay position, until its
    snapshots' xmax reaches the next transaction id of its control file (docs/design/cut.md section 8).  The
    oracle drops such snapshots, on every server version, and refuses the comparison after its settle wait."""
    for version, settled_way in ((180006, K.QUIET_EXPORTED), (180007, K.EXPORTED)):
        clock["now"] = 0.0
        snap, src, coord = _reader(True, version, ["700:700:", "700:700:", "702:702:"], floor="0:701")
        assert snap.way == settled_way and snap.snapshot      # the quiet but unsettled snapshots were dropped
        assert coord.log.count(BEGIN_RR) == 3 and coord.log.count("ROLLBACK") == 2
        assert coord.log.count(SETTLE_SQL) == 1 and coord.log.index(SETTLE_SQL) < coord.log.index(BEGIN_RR)
        assert clock["now"] == pytest.approx(2 * K.SETTLE_POLL_S)
    # a standby that never settles: refused after the wait, never compared, nothing left open
    clock["now"] = 0.0
    coord = _FakePg(True, 180006, ["700:700:"] * 1000, floor="0:701")
    src = _FakeSource(coord, lambda: _FakePg(True, 180006))
    with pytest.raises(K.PositionNotReached, match="has not settled") as info:
        K._SourceSnapshot(src, 4, 60_000, 0x2000, Deadline.after(60), lambda: Deadline.after(5.0),
                          lambda: Deadline.after(2.0))
    assert "700" in str(info.value) and "701" in str(info.value)
    assert clock["now"] == pytest.approx(2.0) and src.opened == [coord]
    # every snapshot was dropped, and the reader closed once more on the refusal: nothing is left open
    assert coord.log.count("ROLLBACK") == coord.log.count(BEGIN_RR) + 1 and coord.log[-1] == "ROLLBACK"
    assert EXPORT_SQL not in coord.log
    assert K.OracleConfig().settle_wait_s == 60.0
