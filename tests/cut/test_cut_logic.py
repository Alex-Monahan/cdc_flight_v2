"""Hermetic tests of the pure half of cdc_v2.pg.cut: LSN and transaction-id arithmetic, classification."""
import pytest
from hypothesis import given, strategies as st

from cdc_v2.pg import cut as C
from cdc_v2.pg.cut import INSIDE, OUTSIDE, UNKNOWN, PrimaryCut, SnapshotBounds, StandbyCut

lsns = st.integers(min_value=0, max_value=C.LSN_MAX)


# -- LSN -------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("text,value", [
    ("0/0", 0),
    ("0/1", 1),
    ("0/FFFFFFFF", 0xFFFFFFFF),
    ("1/0", 1 << 32),
    ("16/B374D848", (0x16 << 32) | 0xB374D848),
    ("FFFFFFFF/FFFFFFFF", C.LSN_MAX),
    ("a/b", (0xA << 32) | 0xB),
])
def test_parse_lsn(text, value):
    assert C.parse_lsn(text) == value
    assert C.parse_lsn(C.format_lsn(value)) == value


@pytest.mark.parametrize("bad", ["", "0", "/", "0/", "/0", "1/2/3", "G/0", "0/G", "100000000/0", "0/100000000",
                                 "-1/0", "0/-1", "+1/0", "0x1/0"])
def test_parse_lsn_rejects(bad):
    with pytest.raises(ValueError):
        C.parse_lsn(bad)


def test_lsn_text_order_is_not_numeric_order():
    # the reason positions are compared as integers, never as text
    assert "9/0" > "10/0" and C.parse_lsn("9/0") < C.parse_lsn("10/0")
    assert C.parse_lsn("0/FFFFFFFF") + 1 == C.parse_lsn("1/0")  # carry across the 32-bit half


@given(lsns)
def test_lsn_round_trip(v):
    assert C.parse_lsn(C.format_lsn(v)) == v
    assert C.to_lsn(v) == v and C.to_lsn(C.format_lsn(v)) == v


def test_to_lsn_rejects():
    for bad in (None, -1, C.LSN_MAX + 1):
        with pytest.raises(ValueError):
            C.to_lsn(bad)
    with pytest.raises(TypeError):
        C.to_lsn(True)

    class DriverLsn:  # a driver type whose str() is the text form
        def __str__(self):
            return "1/2"
    assert C.to_lsn(DriverLsn()) == (1 << 32) | 2


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


def test_sharper_form_uses_the_start_of_the_commit_record():
    cut = StandbyCut(r0=8192 * 3 + 100, r1=8192 * 3 + 500)
    assert not cut.starts_after_r1(8192 * 3 + 500)  # the record that starts at R1 may be in replay
    assert cut.starts_after_r1(8192 * 3 + 501)
    assert cut.classify(8192 * 3 + 900, s=8192 * 3 + 600) is OUTSIDE
    assert cut.classify(8192 * 3 + 900, s=8192 * 3 + 500) is UNKNOWN
    on_page = StandbyCut(r0=8192 * 4, r1=8192 * 4)   # R1 on a page boundary: a page header comes first
    assert not on_page.starts_after_r1(8192 * 4 + 24)
    assert not on_page.starts_after_r1(8192 * 4 + 40)
    assert on_page.starts_after_r1(8192 * 4 + 41)


@given(lsns, lsns, lsns, lsns)
def test_classification_is_monotone_in_e(a, b, e1, e):
    r0, r1 = sorted((a, b))
    cut = StandbyCut(r0=r0, r1=r1, e1=e1 if e1 > r1 else None)
    order = {INSIDE: 0, UNKNOWN: 1, OUTSIDE: 2}
    v = cut.classify(e)
    if e < C.LSN_MAX:
        assert order[cut.classify(e + 1)] >= order[v]
    assert (v is INSIDE) == (e <= r0)
    assert (v is OUTSIDE) == (cut.e1 is not None and e > cut.e1)


def test_classify_all_feeds_the_stream_in_order():
    cut = StandbyCut(r0=10, r1=20)
    verdicts = C.classify_all(cut, [(5, None), (15, None), (25, None), (30, None)])
    assert verdicts == [INSIDE, UNKNOWN, UNKNOWN, OUTSIDE] and cut.e1 == 25


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


def test_take_standby_cut_own_statement_order():
    replay = iter(["0/1000", "0/1100"])
    conn = FakeConn({
        C.SQL_REPLAY_LSN: lambda: (next(replay),),
        C.SQL_OWN_FIRST: lambda: (next(replay), True, "700:705:"),
    })
    cut = C.take_standby_cut_own(conn)
    assert conn.log == [C.SQL_REPLAY_LSN, C.SQL_BEGIN_RR, C.SQL_OWN_FIRST]   # R0 first, in autocommit
    assert (cut.r0, cut.r1, cut.r_stmt, cut.way) == (0x1000, 0x1100, 0x1100, "own")
    assert cut.bounds == SnapshotBounds(700, 705)


def test_take_standby_cut_exported_statement_order():
    replay = iter(["0/1000", "0/1100", "0/1200"])
    conn = FakeConn({
        C.SQL_VERSION_NUM: (180007,),
        C.SQL_REPLAY_LSN: lambda: (next(replay),),
        C.SQL_EXPORT: lambda: ("00000003-0000001B-1", next(replay), True, "700:705:"),
        C.SQL_R1: lambda: (next(replay), True),
    })
    cut = C.take_standby_cut_exported(conn)
    assert conn.log == [C.SQL_VERSION_NUM, C.SQL_BEGIN_RC, C.SQL_REPLAY_LSN, C.SQL_EXPORT, C.SQL_R1]
    assert (cut.r0, cut.r_stmt, cut.r1) == (0x1000, 0x1100, 0x1200)
    assert cut.snapshot_id == "00000003-0000001B-1" and cut.way == "exported"


def test_exported_cut_is_refused_on_a_server_with_the_export_defect():
    """Releases up to 16.15, 17.11 and 18.6 export a wrong snapshot on a standby (docs/design/cut.md)."""
    for version, bad in ((150010, True), (160015, True), (160016, False), (170011, True), (170012, False),
                         (180000, True), (180006, True), (180007, False), (190000, False)):
        assert C.export_defect(version) is bad, version
    conn = FakeConn({C.SQL_VERSION_NUM: (180006,)})
    with pytest.raises(C.CutError, match="74c9832ca610"):
        C.take_standby_cut_exported(conn)
    assert conn.log == [C.SQL_VERSION_NUM]                      # nothing was exported
    replay = iter(["0/1000", "0/1100", "0/1200"])
    conn = FakeConn({C.SQL_REPLAY_LSN: lambda: (next(replay),), C.SQL_R1: lambda: (next(replay), True),
                     C.SQL_EXPORT: lambda: ("00000003-0000001B-1", next(replay), True, "700:705:")})
    assert C.take_standby_cut_exported(conn, accept_export_defect=True).way == "exported"
    assert C.SQL_VERSION_NUM not in conn.log


def test_quiet_only_export_keeps_a_quiet_snapshot_and_drops_any_other():
    """A snapshot with xmin == xmax has no running transaction to lose, so it is safe on a defect server too."""
    def conn_with(snapshot):
        replay = iter(["0/1000", "0/1100", "0/1200"])
        return FakeConn({C.SQL_VERSION_NUM: (180006,), C.SQL_REPLAY_LSN: lambda: (next(replay),),
                         C.SQL_R1: lambda: (next(replay), True),
                         C.SQL_EXPORT: lambda: ("00000003-0000001B-1", next(replay), True, snapshot)})
    conn = conn_with("705:705:")
    cut = C.take_standby_cut_exported(conn, quiet_only=True)
    assert cut.is_quiet and cut.way == "exported" and "ROLLBACK" not in conn.log
    assert C.SQL_VERSION_NUM not in conn.log                    # the version does not matter for a quiet snapshot
    assert cut.classify(0x1150, 704) is INSIDE and cut.classify(0x1150, 705) is OUTSIDE   # exact by id alone
    conn = conn_with("700:705:")
    with pytest.raises(C.SnapshotNotQuiet):
        C.take_standby_cut_exported(conn, quiet_only=True)
    assert conn.log[-1] == "ROLLBACK"                           # the export is gone with its transaction
    assert not C.StandbyCut(r0=1, r1=2, bounds=SnapshotBounds(700, 705)).is_quiet
    assert not C.StandbyCut(r0=1, r1=2).is_quiet                # no bounds known: not quiet


def test_standby_cut_refused_after_promotion_and_rolled_back():
    conn = FakeConn({C.SQL_REPLAY_LSN: ("0/1000",), C.SQL_OWN_FIRST: ("0/1000", False, "700:705:")})
    with pytest.raises(C.CutError):
        C.take_standby_cut_own(conn)
    assert conn.log[-1] == "ROLLBACK"
    conn = FakeConn({C.SQL_REPLAY_LSN: (None,)})
    with pytest.raises(C.CutError):
        C.take_standby_cut_own(conn)


def test_autocommit_is_required():
    with pytest.raises(C.CutError):
        C.take_standby_cut_own(FakeConn({}, autocommit=False))


def test_import_snapshot_rejects_odd_names():
    conn = FakeConn({})
    with pytest.raises(C.CutError):
        C.import_snapshot(conn, "x'; DROP TABLE t; --")
    assert conn.log == []
    C.import_snapshot(conn, "00000003-0000001B-1")
    assert conn.log == [C.SQL_BEGIN_RR, "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"]


def test_wait_replay_reached_has_a_deadline():
    now = [0.0]
    conn = FakeConn({C.SQL_REPLAY_LSN: ("0/1000",)})
    with pytest.raises(C.CutDeadlineExceeded) as err:
        C.wait_replay_reached(conn, 0x2000, deadline_s=1.0, poll_s=0.25, clock=lambda: now[0],
                              sleep=lambda s: now.__setitem__(0, now[0] + s))
    assert err.value.detail["behind_bytes"] == 0x1000 and now[0] == pytest.approx(1.0)
    assert C.wait_replay_reached(conn, 0x1000, deadline_s=1.0) == 0x1000


def test_take_primary_cut_after_reports_blockers_at_the_deadline():
    now = [0.0]
    conn = FakeConn({C.SQL_PRIMARY_FIRST: ("100:110:100,104", False)})
    with pytest.raises(C.CutDeadlineExceeded) as err:
        C.take_primary_cut_after(conn, 105, deadline_s=0.5, poll_s=0.25, clock=lambda: now[0],
                                 sleep=lambda s: now.__setitem__(0, now[0] + s))
    assert err.value.detail["blocking_xids"] == [100, 104]
    assert conn.log.count(C.SQL_BEGIN_RR) == conn.log.count("ROLLBACK")      # no transaction left open
    cut = C.take_primary_cut_after(conn, 100, deadline_s=0.5)
    assert cut.bounds.xmin == 100


def test_primary_cut_refused_on_a_standby():
    conn = FakeConn({C.SQL_PRIMARY_FIRST: ("100:100:", True)})
    with pytest.raises(C.CutError):
        C.take_primary_cut(conn)
    assert conn.log[-1] == "ROLLBACK"


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
                tracker.delivered(x)
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
    assert C.settle_floor(conn) == (1 << 32) | 9
    with pytest.raises(C.CutError):
        C.settle_floor(FakeConn({C.SQL_SETTLE_FLOOR: ("1:9", False)}))


def test_cut_below_the_settle_floor_is_dropped():
    """The measured case (evidence/p3/cut/standby_start.json): after the restart the snapshot is
    26838532:26838532: and the control file says next id 26838533; the hidden transaction is 26838532."""
    def conn_with(snapshot):
        replay = iter(["0/1000", "0/1100"])
        return FakeConn({C.SQL_REPLAY_LSN: lambda: (next(replay),),
                         C.SQL_OWN_FIRST: lambda: (next(replay), True, snapshot)})
    floor = 26838533
    conn = conn_with("26838532:26838532:")
    with pytest.raises(C.StandbyNotSettled) as info:
        C.take_standby_cut_own(conn, settle_floor=floor)
    assert conn.log[-1] == "ROLLBACK" and (info.value.floor, info.value.xmax) == (floor, 26838532)
    cut = C.take_standby_cut_own(conn_with("26838534:26838534:"), settle_floor=floor)
    assert cut.is_settled(floor)
    cut = C.take_standby_cut_own(conn_with("26838532:26838532:"))        # no floor given: the caller tests
    assert not cut.is_settled(floor) and cut.is_settled(26838532)
    assert not C.StandbyCut(r0=1, r1=2).is_settled(3)                     # no bounds: never settled
    # the exported way tests the floor before it tests for a quiet snapshot
    replay = iter(["0/1000", "0/1100", "0/1200"])
    conn = FakeConn({C.SQL_REPLAY_LSN: lambda: (next(replay),), C.SQL_R1: lambda: (next(replay), True),
                     C.SQL_EXPORT: lambda: ("00000003-0000001B-1", next(replay), True, "700:705:")})
    with pytest.raises(C.StandbyNotSettled):
        C.take_standby_cut_exported(conn, quiet_only=True, settle_floor=706)
    assert conn.log[-1] == "ROLLBACK"


def test_take_standby_cut_after_waits_for_replay_and_for_the_floor():
    now = [0.0]
    snaps = iter(["700:700:", "700:700:", "702:702:"])

    def conn_for(snapshots, replay="0/2000"):
        return FakeConn({C.SQL_REPLAY_LSN: (replay,), C.SQL_OWN_FIRST: lambda: (replay, True, next(snapshots))})

    def sleep(seconds):
        now[0] += seconds
    conn = conn_for(snaps)
    cut = C.take_standby_cut_after(conn, 0x1800, deadline_s=5, settle_floor=701, clock=lambda: now[0], sleep=sleep)
    assert cut.bounds.xmax == 702 and cut.usable_after(0x1800) and cut.is_settled(701)
    assert conn.log.count("ROLLBACK") == 2                                 # two snapshots were dropped
    now[0] = 0.0
    conn = conn_for(iter(["700:700:"] * 1000))
    with pytest.raises(C.CutDeadlineExceeded) as info:
        C.take_standby_cut_after(conn, 0x1800, deadline_s=1, settle_floor=701, clock=lambda: now[0], sleep=sleep)
    assert info.value.detail == {"settle_floor": 701, "xmax": 700}
    assert conn.log[-1] == "ROLLBACK"                                      # nothing is left open
    with pytest.raises(TypeError):
        C.take_standby_cut_after(conn_for(iter(["702:702:"])), 0x1800, deadline_s=1)   # the floor has no default
