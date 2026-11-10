"""Hermetic tests of cdc_v2.pg.cut: transaction-id arithmetic, classification, and the SQL half against a fake
connection.  Positions and deadlines come from the shared modules (cdc_v2.lsn, cdc_v2.deadline)."""
import inspect

import pytest
from hypothesis import given, strategies as st

from cdc_v2 import deadline as D
from cdc_v2 import lsn as L
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.pg import cut as C
from cdc_v2.pg.cut import INSIDE, OUTSIDE, UNKNOWN, PrimaryCut, SnapshotBounds, StandbyCut
from cdc_v2.testing import cutnarrow as N
from cdc_v2.testing import pgoutput as P

lsns = st.integers(min_value=0, max_value=L.MAX)
WALFILE_TL1 = "000000010000000000000003"


@pytest.fixture
def clock(monkeypatch):
    """The clock of cdc_v2.deadline, in the hands of the test (as in tests/test_deadline.py)."""
    state = {"now": 0.0}

    def sleep(seconds):
        state["now"] += seconds

    monkeypatch.setattr(D, "_clock", lambda: state["now"])
    monkeypatch.setattr(D, "_sleep", sleep)
    return state


# -- the shared modules (contract C0) ------------------------------------------------------------------------------

def test_the_library_has_no_position_or_deadline_code_of_its_own():
    assert C.lsn is L and C.Deadline is Deadline and C.DeadlineExceeded is DeadlineExceeded
    for old in ("parse_lsn", "format_lsn", "to_lsn", "LSN_MAX", "CutDeadlineExceeded"):
        assert not hasattr(C, old), old
    for fn in (C.wait_replay_reached, C.take_standby_cut_after, C.take_keyless_standby_cut,
               C.take_primary_cut_after):
        params = inspect.signature(fn).parameters
        assert params["deadline"].default is inspect.Parameter.empty, fn.__name__
        assert not {"deadline_s", "clock", "sleep"} & set(params), fn.__name__


@given(lsns)
def test_the_test_decoder_takes_positions_as_integers_or_text(v):
    assert P.to_lsn(v) == v and P.to_lsn(L.format(v)) == v


def test_the_test_decoder_rejects_what_is_not_a_position():
    for bad in (None, -1, L.MAX + 1):
        with pytest.raises(ValueError):
            P.to_lsn(bad)
    with pytest.raises(TypeError):
        P.to_lsn(True)

    class DriverLsn:  # a driver type whose str() is the text form
        def __str__(self):
            return "1/2"
    assert P.to_lsn(DriverLsn()) == (1 << 32) | 2


# -- transaction ids ---------------------------------------------------------------------------------------------

def test_widen_xid_same_epoch_and_across_the_boundary():
    e3 = 3 << 32
    assert C.widen_xid(100, e3 + 200) == e3 + 100                 # a little older, same epoch
    assert C.widen_xid(300, e3 + 200) == e3 + 300                 # a little newer, same epoch
    assert C.widen_xid(0xFFFFFFF0, e3 + 10) == e3 - 16            # older, previous epoch
    assert C.widen_xid(10, e3 - 16) == e3 + 10                    # newer, next epoch
    assert C.widen_xid(0xFFFFFFF0, 10) == 0xFFFFFFF0              # epoch 0 has no previous epoch
    assert C.xid32(e3 + 100) == 100 and C.xid_epoch(e3 + 100) == 3


@pytest.mark.parametrize("bad", [0, 1, 2, -1, 1 << 32])
def test_widen_xid_rejects(bad):
    with pytest.raises(ValueError):
        C.widen_xid(bad, 1000)


@given(st.integers(min_value=3, max_value=C.XID8_MAX - C.XID_HALF),
       st.integers(min_value=-(C.XID_HALF - 1), max_value=C.XID_HALF - 1))
def test_widen_xid_recovers_any_id_within_half_the_circle(near, delta):
    full = near + delta
    if full < 3 or C.xid32(full) < 3:
        return
    assert C.widen_xid(C.xid32(full), near) == full


def test_snapshot_bounds_parse():
    b = SnapshotBounds.parse("100:105:101,103")
    assert (b.xmin, b.xmax, b.xip) == (100, 105, (101, 103)) and str(b) == "100:105:101,103"
    b = SnapshotBounds.parse("4294967400:4294967400:")
    assert b.xmin == b.xmax == (1 << 32) + 104 and b.xip == ()
    for bad in ("", "1:2", "a:b:", "10:5:", "100:105:99", "100:105:105"):
        with pytest.raises(ValueError):
            SnapshotBounds.parse(bad)


# -- the standby cut ---------------------------------------------------------------------------------------------

def test_standby_rule_of_design_7_2():
    cut = StandbyCut(r0=1000, r1=1100)
    assert cut.is_open
    assert cut.classify(900) is INSIDE
    assert cut.classify(1000) is INSIDE            # E <= R0, boundary included
    assert cut.classify(1001) is UNKNOWN
    assert cut.classify(1100) is UNKNOWN           # E == R1 is unknown, not inside
    assert cut.classify(1200) is UNKNOWN           # open cut: could be E1 itself
    assert cut.observe(1050) is False and cut.is_open      # at or before R1: does not close
    assert cut.observe(1100) is False and cut.is_open
    assert cut.observe(1200) is True and cut.e1 == 1200
    assert cut.observe(1300) is False and cut.e1 == 1200   # only the first one counts
    assert cut.classify(1200) is UNKNOWN           # the transaction at E1
    assert cut.classify(1201) is OUTSIDE
    assert cut.unknown_window() == (1000, 1200)


def test_standby_cut_validates():
    with pytest.raises(C.CutError):
        StandbyCut(r0=11, r1=10)
    with pytest.raises(C.CutError):
        StandbyCut(r0=10, r1=20, e1=20)


def test_has_passed_and_usable_after():
    cut = StandbyCut(r0=1000, r1=1100)
    assert not cut.has_passed(1100) and cut.has_passed(1101)
    assert cut.usable_after(1000) and cut.usable_after(999) and not cut.usable_after(1001)


def test_xid_narrowing_never_overrides_positions_and_handles_the_epoch():
    e1 = 1 << 32
    b = SnapshotBounds(xmin=e1 + 10, xmax=e1 + 20)
    cut = StandbyCut(r0=1000, r1=1100, bounds=b, e1=1200)
    assert cut.classify(1050, 9) is INSIDE          # below xmin (same epoch)
    assert cut.classify(1050, 0xFFFFFFF0) is INSIDE  # below xmin, id from the previous epoch
    assert cut.classify(1050, 10) is UNKNOWN        # xmin itself is not settled
    assert cut.classify(1050, 19) is UNKNOWN
    assert cut.classify(1050, 20) is OUTSIDE        # at xmax
    assert cut.classify(1200, 25) is OUTSIDE        # the E1 transaction settled by its id
    assert cut.classify(900, 25) is INSIDE          # positions win: E <= R0
    assert cut.classify(1300, 5) is OUTSIDE         # positions win: E > E1
    # epoch boundary inside the window: xmax just after the wrap, a transaction from just before it
    b2 = SnapshotBounds(xmin=e1 - 5, xmax=e1 + 5)
    cut2 = StandbyCut(r0=1000, r1=1100, bounds=b2)
    assert cut2.classify(1050, C.xid32(e1 - 6)) is INSIDE
    assert cut2.classify(1050, C.xid32(e1 - 5)) is UNKNOWN
    assert cut2.classify(1050, 4) is UNKNOWN
    assert cut2.classify(1050, 5) is OUTSIDE


def test_the_library_cannot_narrow_by_the_start_of_the_commit_record():
    """Stock Debezium does not deliver the start of the commit record (docs/design/engine.md section 5), so no
    production signature takes it: the narrowing cannot be reached by passing one more argument."""
    cut = StandbyCut(r0=8192 * 3 + 100, r1=8192 * 3 + 500)
    assert list(inspect.signature(StandbyCut.classify).parameters) == ["self", "e", "xid"]
    assert not hasattr(cut, "starts_after_r1") and not hasattr(C, "wal_block_size")
    with pytest.raises(TypeError):
        cut.classify(8192 * 3 + 900, s=8192 * 3 + 600)
    assert cut.classify(8192 * 3 + 900) is UNKNOWN
    assert "testing" in N.__name__ and N.__doc__.startswith("Test only")


def test_the_test_only_narrowing_uses_the_start_of_the_commit_record():
    cut = StandbyCut(r0=8192 * 3 + 100, r1=8192 * 3 + 500)
    assert not N.starts_after_r1(cut, 8192 * 3 + 500)  # the record that starts at R1 may be in replay
    assert N.starts_after_r1(cut, 8192 * 3 + 501)
    assert N.classify_with_start(cut, 8192 * 3 + 900, None, 8192 * 3 + 600) is OUTSIDE
    assert N.classify_with_start(cut, 8192 * 3 + 900, None, 8192 * 3 + 500) is UNKNOWN
    assert N.classify_with_start(cut, 8192 * 3 + 50, None, 8192 * 3 + 2) is INSIDE     # never changes an answer
    on_page = StandbyCut(r0=8192 * 4, r1=8192 * 4)   # R1 on a page boundary: a page header comes first
    assert not N.starts_after_r1(on_page, 8192 * 4 + 24)
    assert not N.starts_after_r1(on_page, 8192 * 4 + 40)
    assert N.starts_after_r1(on_page, 8192 * 4 + 41)
    assert N.starts_after_r1(StandbyCut(r0=1000, r1=1000), 1024, wal_block_size=1000) is False
    assert N.wal_block_size(FakeConn({N.SQL_WAL_BLOCK_SIZE: (8192,)})) == 8192


@given(lsns, lsns, lsns, lsns)
def test_classification_is_monotone_in_e(a, b, e1, e):
    r0, r1 = sorted((a, b))
    cut = StandbyCut(r0=r0, r1=r1, e1=e1 if e1 > r1 else None)
    order = {INSIDE: 0, UNKNOWN: 1, OUTSIDE: 2}
    v = cut.classify(e)
    if e < L.MAX:
        assert order[cut.classify(e + 1)] >= order[v]
    assert (v is INSIDE) == (e <= r0)
    assert (v is OUTSIDE) == (cut.e1 is not None and e > cut.e1)


def test_classify_all_feeds_the_stream_in_order():
    cut = StandbyCut(r0=10, r1=20)
    verdicts = C.classify_all(cut, [(5, None), (15, None), (25, None), (30, None)])
    assert verdicts == [INSIDE, UNKNOWN, UNKNOWN, OUTSIDE] and cut.e1 == 25


# -- the timeline ------------------------------------------------------------------------------------------------

def test_a_cut_is_valid_only_on_the_timeline_it_was_taken_on():
    cut = StandbyCut(r0=1000, r1=1100, timeline=3)
    assert cut.valid_on(3)
    assert not cut.valid_on(4)                     # after a promotion: the cut is abandoned
    assert not cut.valid_on(2)
    assert not cut.valid_on(None)                  # the caller does not know the stream's timeline: abandoned
    assert not StandbyCut(r0=1000, r1=1100).valid_on(3)     # a cut that recorded no timeline is valid nowhere
    pc = PrimaryCut(SnapshotBounds(100, 100), timeline=3)
    assert pc.valid_on(3) and not pc.valid_on(4) and not pc.valid_on(None)
    assert not PrimaryCut(SnapshotBounds(100, 100)).valid_on(3)
    assert pc == PrimaryCut(SnapshotBounds(100, 100), timeline=3) != PrimaryCut(SnapshotBounds(100, 100), timeline=4)


# -- the primary cut ---------------------------------------------------------------------------------------------

def test_primary_cut_is_exact():
    cut = PrimaryCut(SnapshotBounds(xmin=100, xmax=110, xip=(100, 104)))
    assert cut.classify(99) is INSIDE
    assert cut.classify(100) is OUTSIDE     # in progress
    assert cut.classify(101) is INSIDE      # between xmin and xmax, not in progress: finished
    assert cut.classify(104) is OUTSIDE
    assert cut.classify(109) is INSIDE
    assert cut.classify(110) is OUTSIDE     # at xmax: started later
    assert not cut.usable_after_xid(101) and cut.usable_after_xid(100)
    assert cut.blocking_xids(105) == (100, 104) and cut.blocking_xids(101) == (100,)


def test_primary_cut_across_the_epoch():
    e2 = 2 << 32
    cut = PrimaryCut(SnapshotBounds(xmin=e2 - 3, xmax=e2 + 4, xip=(e2 - 3, e2 + 1)))
    assert cut.classify(C.xid32(e2 - 4)) is INSIDE
    assert cut.classify(C.xid32(e2 - 3)) is OUTSIDE
    assert cut.classify(C.xid32(e2 - 1)) is INSIDE
    assert cut.classify(3) is INSIDE        # e2 + 3: ids 0..2 of the new epoch are skipped by Postgres
    assert cut.classify(4) is OUTSIDE


# -- the SQL half, against a fake connection ---------------------------------------------------------------------

class FakeConn:
    """Answers the statements of cut.py from a script; records what was executed."""

    def __init__(self, answers, autocommit=True):
        self.answers = dict(answers)
        self.autocommit = autocommit
        self.log = []

    def execute(self, sql, params=None):
        self.log.append(sql)
        val = self.answers.get(sql)
        if callable(val):
            val = val()
        if isinstance(val, Exception):
            raise val

        class Cur:
            def fetchone(self_inner):
                return val
        return Cur()


def standby(answers, floor="0:3"):
    """A fake standby connection and its settle floor, read the way the pipeline reads it; the log starts empty."""
    conn = FakeConn({C.SQL_SETTLE_FLOOR: (floor, True), **answers})
    bound = C.settle_floor(conn)
    assert conn.log == [C.SQL_SETTLE_FLOOR]
    conn.log.clear()
    return conn, bound


def export_answers(snapshot, version=180007, timeline=1):
    replay = iter(["0/1000", "0/1100", "0/1200"] * 50)
    answers = {C.SQL_R0: lambda: (next(replay), timeline), C.SQL_R1: lambda: (next(replay), True),
               C.SQL_EXPORT: lambda: ("00000003-0000001B-1", next(replay), True,
                                      snapshot() if callable(snapshot) else snapshot)}
    if version is not None:
        answers[C.SQL_VERSION_NUM] = (version,)
    return answers


def test_take_standby_cut_own_statement_order():
    replay = iter(["0/1000", "0/1100"])
    conn, floor = standby({C.SQL_R0: lambda: (next(replay), 7),
                           C.SQL_OWN_FIRST: lambda: (next(replay), True, "700:705:")})
    cut = C.take_standby_cut_own(conn, floor)
    assert conn.log == [C.SQL_R0, C.SQL_BEGIN_RR, C.SQL_OWN_FIRST]   # R0 first, in autocommit
    assert (cut.r0, cut.r1, cut.r_stmt, cut.way) == (0x1000, 0x1100, 0x1100, "own")
    assert cut.bounds == SnapshotBounds(700, 705)
    assert cut.timeline == 7 and cut.valid_on(7) and not cut.valid_on(8)   # read with R0, before the snapshot


def test_take_standby_cut_exported_statement_order():
    conn, floor = standby(export_answers("700:705:", timeline=2))
    cut = C.take_standby_cut_exported(conn, floor)
    assert conn.log == [C.SQL_VERSION_NUM, C.SQL_BEGIN_RC, C.SQL_R0, C.SQL_EXPORT, C.SQL_R1]
    assert (cut.r0, cut.r_stmt, cut.r1) == (0x1000, 0x1100, 0x1200)
    assert cut.snapshot_id == "00000003-0000001B-1" and cut.way == "exported" and cut.timeline == 2


def test_export_defect_is_the_one_place_that_knows_the_versions():
    """Releases up to 16.15, 17.11 and 18.6 export a wrong snapshot on a standby (docs/design/cut.md)."""
    for version, bad in ((150010, True), (160015, True), (160016, False), (170011, True), (170012, False),
                         (180000, True), (180006, True), (180007, False), (190000, False)):
        assert C.export_defect(version) is bad, version
    assert C.server_version_num(FakeConn({C.SQL_VERSION_NUM: (160004,)})) == 160004


def test_exported_cut_is_refused_on_a_server_with_the_export_defect():
    conn, floor = standby({C.SQL_VERSION_NUM: (180006,)})
    with pytest.raises(C.CutError, match="74c9832ca610"):
        C.take_standby_cut_exported(conn, floor)
    assert conn.log == [C.SQL_VERSION_NUM]                      # nothing was exported
    conn, floor = standby(export_answers("700:705:", version=None))
    assert C.take_standby_cut_exported(conn, floor, accept_export_defect=True).way == "exported"
    assert C.SQL_VERSION_NUM not in conn.log                    # the test switch does not ask


def test_quiet_only_export_keeps_a_quiet_snapshot_and_drops_any_other():
    """A snapshot with xmin == xmax has no running transaction to lose, so it is safe on a defect server too."""
    conn, floor = standby(export_answers("705:705:", version=180006))
    cut = C.take_standby_cut_exported(conn, floor, quiet_only=True)
    assert cut.is_quiet and cut.way == "exported" and "ROLLBACK" not in conn.log
    assert C.SQL_VERSION_NUM not in conn.log                    # the version does not matter for a quiet snapshot
    assert cut.classify(0x1150, 704) is INSIDE and cut.classify(0x1150, 705) is OUTSIDE   # exact by id alone
    conn, floor = standby(export_answers("700:705:", version=180006))
    with pytest.raises(C.SnapshotNotQuiet) as info:
        C.take_standby_cut_exported(conn, floor, quiet_only=True)
    assert conn.log[-1] == "ROLLBACK"                           # the export is gone with its transaction
    assert info.value.bounds == SnapshotBounds(700, 705)        # xmin: the oldest transaction still running
    assert not C.StandbyCut(r0=1, r1=2, bounds=SnapshotBounds(700, 705)).is_quiet
    assert not C.StandbyCut(r0=1, r1=2).is_quiet                # no bounds known: not quiet


def test_standby_cut_refused_after_promotion_and_rolled_back():
    conn, floor = standby({C.SQL_R0: ("0/1000", 1), C.SQL_OWN_FIRST: ("0/1000", False, "700:705:")})
    with pytest.raises(C.CutError, match="not a standby"):
        C.take_standby_cut_own(conn, floor)
    assert conn.log[-1] == "ROLLBACK"
    conn, floor = standby({C.SQL_R0: (None, 1)})
    with pytest.raises(C.CutError):
        C.take_standby_cut_own(conn, floor)


def test_autocommit_is_required():
    conn = FakeConn({}, autocommit=False)
    with pytest.raises(C.CutError, match="autocommit"):
        C.take_standby_cut_own(conn, C.SettleFloor(conn, 3))
    with pytest.raises(C.CutError, match="autocommit"):
        C.settle_floor(conn)
    with pytest.raises(C.CutError, match="autocommit"):
        C.probe_snapshot(conn)
    assert conn.log == []


def test_import_snapshot_rejects_odd_names():
    conn = FakeConn({})
    with pytest.raises(C.CutError):
        C.import_snapshot(conn, "x'; DROP TABLE t; --")
    assert conn.log == []
    C.import_snapshot(conn, "00000003-0000001B-1")
    assert conn.log == [C.SQL_BEGIN_RR, "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"]


def test_wait_replay_reached_has_a_deadline(clock):
    conn = FakeConn({C.SQL_REPLAY_LSN: ("0/1000",)})
    with pytest.raises(DeadlineExceeded) as err:
        C.wait_replay_reached(conn, 0x2000, Deadline.after(1.0), poll_s=0.25)
    assert "0/1000" in err.value.what and "0/2000" in err.value.what and "4096 bytes behind" in err.value.what
    assert clock["now"] == pytest.approx(1.0)
    assert C.wait_replay_reached(conn, 0x1000, Deadline.after(1.0)) == 0x1000


def test_take_primary_cut_after_reports_blockers_at_the_deadline(clock):
    conn = FakeConn({C.SQL_PRIMARY_FIRST: ("100:110:100,104", False, WALFILE_TL1)})
    with pytest.raises(DeadlineExceeded) as err:
        C.take_primary_cut_after(conn, 105, Deadline.after(0.5), poll_s=0.25)
    assert "100, 104" in err.value.what and clock["now"] == pytest.approx(0.5)
    assert conn.log.count(C.SQL_BEGIN_RR) == conn.log.count("ROLLBACK")      # no transaction left open
    cut = C.take_primary_cut_after(conn, 100, Deadline.after(0.5))
    assert cut.bounds.xmin == 100 and cut.timeline == 1


def test_primary_cut_refused_on_a_standby():
    conn = FakeConn({C.SQL_PRIMARY_FIRST: ("100:100:", True, None)})
    with pytest.raises(C.CutError):
        C.take_primary_cut(conn)
    assert conn.log[-1] == "ROLLBACK"


def test_probe_snapshot_is_one_statement_and_leaves_nothing_open():
    conn = FakeConn({C.SQL_PRIMARY_FIRST: ("100:110:100,104", False, "0000000500000000000000A1")})
    cut = C.probe_snapshot(conn)
    assert conn.log == [C.SQL_PRIMARY_FIRST]                    # no BEGIN: nothing to end
    assert cut.bounds == SnapshotBounds(100, 110, (100, 104)) and cut.timeline == 5
    conn = FakeConn({C.SQL_PRIMARY_FIRST: ("100:100:", True, None)})
    with pytest.raises(C.CutError, match="not a primary"):
        C.probe_snapshot(conn)
    assert conn.log == [C.SQL_PRIMARY_FIRST]


def test_timeline_of_a_primary_is_exact_and_of_a_standby_is_the_control_file():
    assert C.timeline(FakeConn({C.SQL_TIMELINE: (3, "0000000400000000000000A1")})) == 4   # primary: insert position
    assert C.timeline(FakeConn({C.SQL_TIMELINE: (3, None)})) == 3                         # standby: control file
    with pytest.raises(C.CutError, match="WAL file name"):
        C.timeline(FakeConn({C.SQL_TIMELINE: (3, "xyz")}))


# -- the exact rule on a primary: the gap tracker ----------------------------------------------------------------

def test_gap_tracker_keeps_only_what_no_probe_has_seen():
    t = C.GapTracker()
    for x in (101, 104, 107):
        t.delivered(x)
    assert len(t) == 3
    # 104 has written its commit record (the stream delivered it) and is still listed as in progress: the gap
    t.probe(PrimaryCut(SnapshotBounds(xmin=104, xmax=108, xip=(104,))))
    assert len(t) == 1
    mark = t.mark()
    assert mark.gap == frozenset({104}) and mark.unknown_below is None
    later_still_in_gap = PrimaryCut(SnapshotBounds(xmin=104, xmax=120, xip=(104, 111)))
    assert not mark.admits(later_still_in_gap) and mark.blocking(later_still_in_gap) == (104,)
    # an unrelated open transaction does not block: the exact rule waits only for the gap itself
    later = PrimaryCut(SnapshotBounds(xmin=111, xmax=120, xip=(111,)))
    assert mark.admits(later) and mark.blocking(later) == ()
    # the mark is frozen: what the tracker learns afterwards does not change it
    t.delivered(130)
    t.probe(later)
    assert mark.gap == frozenset({104}) and len(t) == 1


def test_gap_tracker_delivered_id_at_or_above_xmax_stays():
    t = C.GapTracker()
    t.delivered(110)
    quiet = PrimaryCut(SnapshotBounds(xmin=110, xmax=110))
    t.probe(quiet)                       # 110 is not below xmax: this snapshot does not see it
    assert len(t) == 1 and not t.mark().admits(quiet)
    assert t.mark().admits(PrimaryCut(SnapshotBounds(xmin=111, xmax=111)))


def test_gap_tracker_blind_start_demands_xmin_until_a_probe_clears_it():
    t = C.GapTracker(unknown_below=200)
    mark = t.mark()
    blocked = PrimaryCut(SnapshotBounds(xmin=150, xmax=260, xip=(150, 230)))
    assert not mark.admits(blocked) and mark.blocking(blocked) == (150,)
    assert mark.admits(PrimaryCut(SnapshotBounds(xmin=230, xmax=260, xip=(230,))))
    t.probe(blocked)
    assert t.mark().unknown_below == 200
    t.probe(PrimaryCut(SnapshotBounds(xmin=200, xmax=260, xip=(230,))))
    assert t.mark().unknown_below is None and t.mark().admits(blocked)


def test_gap_tracker_widens_the_stream_id_with_the_latest_probe():
    """The stream carries 32-bit ids; the tracker needs 64-bit ones.  The reference is the xmax of the latest
    probe, which is at most one group old."""
    e3 = 3 << 32
    t = C.GapTracker()
    assert t.reference is None
    with pytest.raises(C.CutError, match="no probe yet"):
        t.widen(15)
    t.probe(PrimaryCut(SnapshotBounds(xmin=e3 + 10, xmax=e3 + 20)))
    assert t.reference == e3 + 20
    assert t.widen(15) == e3 + 15 and t.widen(25) == e3 + 25
    assert t.widen(0xFFFFFFF0) == e3 - 16                       # a transaction from before the wrap
    assert t.delivered_xid32(25) == e3 + 25 and t.mark().gap == frozenset({e3 + 25})
    t.probe(PrimaryCut(SnapshotBounds(xmin=e3 + 40, xmax=e3 + 40)))
    assert t.reference == e3 + 40 and len(t) == 0               # the reference moves with every probe
    blind = C.GapTracker(unknown_below=e3 + 7)                   # an engine start: the marker is the reference
    assert blind.reference == e3 + 7 and blind.delivered_xid32(9) == e3 + 9


def test_gap_tracker_refuses_a_32_bit_id_and_a_stale_reference():
    e3 = 3 << 32
    t = C.GapTracker()
    t.probe(PrimaryCut(SnapshotBounds(xmin=e3 + 10, xmax=e3 + 20)))
    with pytest.raises(C.CutError, match="2\\^31 or more"):
        t.delivered(15)                                         # the 32-bit id, passed by mistake
    for bad in (0, 2, -1, C.XID8_MAX + 1):
        with pytest.raises(C.CutError):
            t.delivered(bad)
    assert len(t) == 0
    # why the reference must be fresh: against one that is 2^31 ids old, the id widens to the wrong epoch
    stale = e3 + 20
    true = stale + C.XID_HALF + 5
    assert C.widen_xid(C.xid32(true), stale) != true
    assert C.widen_xid(C.xid32(true), true + 100) == true       # against a probe of its own time it is exact


class _ModelPrimary:
    """Ids are assigned in order; a transaction writes its commit record (delivered) and later ends (visible)."""

    def __init__(self):
        self.next = 100
        self.running: list[int] = []      # has an id, has not ended
        self.recorded: list[int] = []     # commit record written, not ended yet: the gap
        self.ended: set[int] = set()
        self.latest_completed = 99

    def snapshot(self) -> PrimaryCut:
        xmax = self.latest_completed + 1
        xip = tuple(sorted(x for x in self.running if x < xmax))
        return PrimaryCut(SnapshotBounds(xmin=xip[0] if xip else xmax, xmax=xmax, xip=xip))


@given(st.lists(st.tuples(st.sampled_from(["start", "record", "end", "probe", "mark", "abort"]),
                          st.integers(0, 7)), max_size=40),
       st.booleans())
def test_gap_tracker_is_exact(events, blind):
    """Against a model of the primary: a mark admits a later snapshot exactly if the snapshot sees every
    transaction that had been delivered when the mark was made."""
    p = _ModelPrimary()
    delivered_before_start: set[int] = set()
    if blind:                           # the stream ran before the tracker existed
        for _ in range(3):
            p.running.append(p.next)
            p.next += 1
        p.recorded = [p.running[0], p.running[2]]
        delivered_before_start = set(p.recorded)
        marker = p.next                 # an id assigned after the start
        p.next += 1
        p.ended.add(marker)
        p.latest_completed = max(p.latest_completed, marker)
        tracker = C.GapTracker(unknown_below=marker)
    else:
        tracker = C.GapTracker()
        tracker.probe(p.snapshot())     # the stream probes before it delivers: the tracker has its reference
    delivered = set(delivered_before_start)
    marks = []                          # (mark, ids delivered when it was made)
    for kind, pick in events:
        if kind == "start":
            p.running.append(p.next)
            p.next += 1
        elif kind == "record":
            open_ = [x for x in p.running if x not in p.recorded]
            if open_:
                x = open_[pick % len(open_)]
                p.recorded.append(x)
                delivered.add(x)
                assert tracker.delivered_xid32(C.xid32(x)) == x   # as the stream does it
        elif kind == "end" and p.recorded:
            x = p.recorded.pop(pick % len(p.recorded))
            p.running.remove(x)
            p.ended.add(x)
            p.latest_completed = max(p.latest_completed, x)
        elif kind == "abort":
            open_ = [x for x in p.running if x not in p.recorded]
            if open_:
                x = open_[pick % len(open_)]
                p.running.remove(x)
                p.latest_completed = max(p.latest_completed, x)
        elif kind == "probe":
            tracker.probe(p.snapshot())
        elif kind == "mark":
            marks.append((tracker.mark(), set(delivered)))
        cut = p.snapshot()
        for mark, had in marks:
            sees_all = all(x in p.ended for x in had)
            if mark.admits(cut):
                assert sees_all                                  # never admits a snapshot that misses one
            elif mark.unknown_below is None:
                assert not sees_all, (mark, cut)                 # exact: refuses only when one is missed
            assert set(mark.blocking(cut)) or mark.admits(cut)   # a refusal names what it waits for


# -- the settle floor of a standby that has just started -------------------------------------------------------

def test_parse_epoch_xid():
    assert C.parse_epoch_xid("0:26838533") == 26838533
    assert C.parse_epoch_xid("2:17") == (2 << 32) | 17
    for bad in ("17", "0:1:2", "a:b", "-1:5", "0:4294967296", ""):
        with pytest.raises(ValueError):
            C.parse_epoch_xid(bad)


def test_settle_floor_reads_the_control_file_and_refuses_a_primary():
    conn = FakeConn({C.SQL_SETTLE_FLOOR: ("1:9", True)})
    floor = C.settle_floor(conn)
    assert isinstance(floor, C.SettleFloor) and floor.value == (1 << 32) | 9
    assert floor.for_connection(conn) == floor.value
    with pytest.raises(C.CutError, match="not a standby"):
        C.settle_floor(FakeConn({C.SQL_SETTLE_FLOOR: ("1:9", False)}))
    for bad in (None, True, -1, "9", 1.5):
        with pytest.raises(C.CutError):
            C.SettleFloor(conn, bad)
    with pytest.raises(C.CutError):
        C.SettleFloor(None, 9)


TAKERS = (C.take_standby_cut_own, C.take_standby_cut_exported, C.take_standby_cut_after,
          C.take_keyless_standby_cut)


def test_no_function_takes_a_standby_cut_without_a_settle_floor():
    """The floor is not optional: no default, and nothing but the floor of this connection is accepted."""
    for fn in TAKERS:
        assert inspect.signature(fn).parameters["settle_floor"].default is inspect.Parameter.empty, fn.__name__
    conn, floor = standby({})
    calls = (lambda f: C.take_standby_cut_own(conn, f),
             lambda f: C.take_standby_cut_exported(conn, f),
             lambda f: C.take_standby_cut_exported(conn, f, quiet_only=True, accept_export_defect=True),
             lambda f: C.take_standby_cut_after(conn, 0x1000, Deadline.after(1), f),
             lambda f: C.take_keyless_standby_cut(conn, 0x1000, Deadline.after(1), f))
    for call in calls:
        for bad in (None, 701, floor.value, "0:701", False):
            with pytest.raises(C.CutError, match="settle floor"):
                call(bad)
    with pytest.raises(TypeError):
        C.take_standby_cut_own(conn)                                       # the floor has no default
    with pytest.raises(TypeError):
        C.take_standby_cut_after(conn, 0x1000, Deadline.after(1))
    assert conn.log == []                                                  # nothing reached the server


def test_a_settle_floor_read_on_one_standby_is_refused_on_another():
    conn_a, floor_a = standby({}, floor="0:900")
    conn_b, floor_b = standby({C.SQL_R0: ("0/1000", 1), C.SQL_OWN_FIRST: ("0/1100", True, "700:705:")},
                              floor="0:100")
    for fn in (lambda: C.take_standby_cut_own(conn_b, floor_a),
               lambda: C.take_standby_cut_exported(conn_b, floor_a, accept_export_defect=True),
               lambda: C.take_standby_cut_after(conn_b, 0x1000, Deadline.after(1), floor_a),
               lambda: C.take_keyless_standby_cut(conn_b, 0x1000, Deadline.after(1), floor_a)):
        with pytest.raises(C.CutError, match="another connection"):
            fn()
    assert conn_b.log == []
    assert C.take_standby_cut_own(conn_b, floor_b).bounds.xmax == 705     # its own floor is accepted
    with pytest.raises(AttributeError):
        floor_a.value = 1                                                  # and cannot be changed afterwards


def test_cut_below_the_settle_floor_is_dropped():
    """The measured case (evidence/p3/cut/standby_start.json): after the restart the snapshot is
    26838532:26838532: and the control file says next id 26838533; the hidden transaction is 26838532."""
    def conn_with(snapshot):
        replay = iter(["0/1000", "0/1100"])
        return standby({C.SQL_R0: lambda: (next(replay), 1),
                        C.SQL_OWN_FIRST: lambda: (next(replay), True, snapshot)}, floor="0:26838533")
    conn, floor = conn_with("26838532:26838532:")
    assert floor.value == 26838533
    with pytest.raises(C.StandbyNotSettled) as info:
        C.take_standby_cut_own(conn, floor)
    assert conn.log[-1] == "ROLLBACK" and (info.value.floor, info.value.xmax) == (floor.value, 26838532)
    conn, floor = conn_with("26838534:26838534:")
    assert C.take_standby_cut_own(conn, floor).is_settled(floor.value)
    conn, floor = conn_with("26838532:26838532:")
    cut = C.take_standby_cut_own(conn, floor, accept_unsettled=True)       # the test switch: the cut is given out
    assert not cut.is_settled(floor.value) and cut.is_settled(26838532) and "ROLLBACK" not in conn.log
    assert not C.StandbyCut(r0=1, r1=2).is_settled(3)                      # no bounds: never settled
    # the exported way tests the floor before it tests for a quiet snapshot
    conn, floor = standby(export_answers("700:705:", version=180006), floor="0:706")
    with pytest.raises(C.StandbyNotSettled):
        C.take_standby_cut_exported(conn, floor, quiet_only=True)
    assert conn.log[-1] == "ROLLBACK"
    conn, floor = standby(export_answers("700:705:", version=180006), floor="0:706")
    cut = C.take_standby_cut_exported(conn, floor, accept_export_defect=True, accept_unsettled=True)
    assert not cut.is_settled(floor.value)


def own_answers(snapshots, replay="0/2000", timeline=1):
    return {C.SQL_REPLAY_LSN: (replay,), C.SQL_R0: (replay, timeline),
            C.SQL_OWN_FIRST: lambda: (replay, True, next(snapshots))}


def test_take_standby_cut_after_waits_for_replay_and_for_the_floor(clock):
    conn, floor = standby(own_answers(iter(["700:700:", "700:700:", "702:702:"])), floor="0:701")
    cut = C.take_standby_cut_after(conn, 0x1800, Deadline.after(5), floor)
    assert isinstance(cut, StandbyCut) and cut.bounds.xmax == 702
    assert cut.usable_after(0x1800) and cut.is_settled(floor.value)
    assert conn.log.count("ROLLBACK") == 2                                 # two snapshots were dropped
    conn, floor = standby(own_answers(iter(["700:700:"] * 1000)), floor="0:701")
    clock["now"] = 0.0
    with pytest.raises(DeadlineExceeded) as info:
        C.take_standby_cut_after(conn, 0x1800, Deadline.after(1), floor)
    assert "700 stays below 701" in info.value.what and "heartbeat" in info.value.what
    assert conn.log[-1] == "ROLLBACK" and clock["now"] == pytest.approx(1.0)   # nothing is left open
    conn, floor = standby(own_answers(iter(["702:702:"]), replay="0/1000"), floor="0:701")
    with pytest.raises(DeadlineExceeded) as info:                          # replay never reaches P0
        C.take_standby_cut_after(conn, 0x1800, Deadline.after(1), floor)
    assert "bytes behind" in info.value.what and C.SQL_BEGIN_RR not in conn.log
    with pytest.raises(ValueError, match="quiet_only"):
        C.take_standby_cut_after(conn, 0x1800, Deadline.after(1), floor, quiet_only=True)


# -- tables without a key: a quiet exported snapshot within a wait, else one reader --------------------------------

def keyless_answers(snapshots, version, replay="0/2000"):
    """A standby whose replay position stands at ``replay`` and whose snapshots come from the list, in order."""
    snapshots = iter(snapshots)
    return {C.SQL_VERSION_NUM: (version,), C.SQL_REPLAY_LSN: (replay,), C.SQL_R0: (replay, 1),
            C.SQL_R1: (replay, True),
            C.SQL_EXPORT: lambda: ("00000003-0000001B-1", replay, True, next(snapshots)),
            C.SQL_OWN_FIRST: lambda: (replay, True, next(snapshots))}


def test_a_wait_for_a_quiet_snapshot_ends_with_a_result_not_with_an_error(clock):
    conn, floor = standby(keyless_answers(["700:705:"] * 100, version=180006))
    got = C.take_standby_cut_after(conn, 0x1800, Deadline.after(1.0), floor, exported=True, quiet_only=True,
                                   poll_s=0.25)
    assert isinstance(got, C.NoQuietSnapshot) and not isinstance(got, (StandbyCut, Exception))
    assert not got                                                         # false in a condition
    assert (got.attempts, got.oldest_running) == (4, 700)
    assert "no quiet snapshot within the deadline" in str(got) and "700" in str(got)
    assert clock["now"] == pytest.approx(1.0)
    assert conn.log[-1] == "ROLLBACK" and conn.log.count(C.SQL_BEGIN_RC) == conn.log.count("ROLLBACK") == 4
    assert C.SQL_VERSION_NUM not in conn.log
    # the same wait, when the open transaction ends in time
    conn, floor = standby(keyless_answers(["700:705:", "700:705:", "705:705:"], version=180006))
    got = C.take_standby_cut_after(conn, 0x1800, Deadline.after(1.0), floor, exported=True, quiet_only=True,
                                   poll_s=0.25)
    assert got and isinstance(got, StandbyCut) and got.is_quiet and got.way == "exported"
    # a standby that does not settle is an error also here: no kind of cut may be taken from it
    conn, floor = standby(keyless_answers(["700:700:"] * 100, version=180006), floor="0:701")
    with pytest.raises(DeadlineExceeded):
        C.take_standby_cut_after(conn, 0x1800, Deadline.after(1.0), floor, exported=True, quiet_only=True)


def test_keyless_cut_on_a_server_with_the_fix_is_a_plain_exported_snapshot(clock):
    conn, floor = standby(keyless_answers(["700:705:"], version=180007))
    cut = C.take_keyless_standby_cut(conn, 0x1800, Deadline.after(300), floor)
    assert cut.way == "exported" and not cut.is_quiet and cut.snapshot_id and "quiet_wait" not in cut.extra
    assert clock["now"] == 0.0 and C.SQL_OWN_FIRST not in conn.log


def test_keyless_cut_on_a_server_with_the_defect_waits_for_a_quiet_snapshot(clock):
    conn, floor = standby(keyless_answers(["700:705:", "700:705:", "705:705:"], version=180006))
    cut = C.take_keyless_standby_cut(conn, 0x1800, Deadline.after(300), floor, poll_s=0.25)
    assert cut.way == "exported" and cut.is_quiet and "quiet_wait" not in cut.extra
    assert clock["now"] == pytest.approx(0.5) and C.SQL_OWN_FIRST not in conn.log


def test_keyless_cut_falls_back_to_one_reader_when_no_quiet_snapshot_comes(clock):
    assert C.QUIET_WAIT_DEFAULT_S == 60.0
    assert inspect.signature(C.take_keyless_standby_cut).parameters["quiet_wait_s"].default == 60.0
    conn, floor = standby(keyless_answers(["700:705:"] * 1000, version=180006))
    cut = C.take_keyless_standby_cut(conn, 0x1800, Deadline.after(300), floor, poll_s=0.25)
    assert cut.way == "own" and cut.snapshot_id is None and cut.bounds == SnapshotBounds(700, 705)
    waited = cut.extra["quiet_wait"]
    assert isinstance(waited, C.NoQuietSnapshot) and waited.oldest_running == 700 and waited.attempts == 240
    assert clock["now"] == pytest.approx(60.0)                             # the declared wait, and no more
    assert conn.log[-3:] == [C.SQL_R0, C.SQL_BEGIN_RR, C.SQL_OWN_FIRST]     # the reader's transaction is open
    # the quiet wait never extends the caller's deadline
    clock["now"] = 0.0
    conn, floor = standby(keyless_answers(["700:705:"] * 1000, version=180006))
    cut = C.take_keyless_standby_cut(conn, 0x1800, Deadline.after(2.0), floor, quiet_wait_s=60.0, poll_s=0.25)
    assert cut.way == "own" and clock["now"] == pytest.approx(2.0)
    # a standby that has not settled: the quiet wait ends, the reader's own way waits on, to the full deadline
    clock["now"] = 0.0
    conn, floor = standby(keyless_answers(["700:700:"] * 100000, version=180006), floor="0:701")
    with pytest.raises(DeadlineExceeded) as info:
        C.take_keyless_standby_cut(conn, 0x1800, Deadline.after(5.0), floor, quiet_wait_s=1.0, poll_s=0.25)
    assert "not settled" in info.value.what and clock["now"] == pytest.approx(5.0)
