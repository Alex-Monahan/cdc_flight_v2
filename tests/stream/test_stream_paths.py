"""The paths of the stream that the main walk does not take, each with its crash points: the first start (slot,
then row), the quarantine exit of a table that fails again and again (SPEC 4.0), and the last line of SPEC 4.3
(the slot moved past a place that fails again and again, the position repositioned, every table backfilled)."""
import collections

import pytest

from cdc_v2 import crashpoints
from cdc_v2.stream import state as st
from cdc_v2.testing import destination as td

from fakes import K, N, col, default_catalog, ins, table
from rig import Rig

PATH_POINTS = ["stream.slot_created", "stream.first_row", "stream.quarantined", "stream.slot_advanced",
               "stream.repositioned"]


def _arm(point, log, hit=1):
    crashpoints.reconfigure({crashpoints.ENV_POINT: point or "", crashpoints.ENV_HIT: str(hit),
                             crashpoints.ENV_ACTION: "raise", crashpoints.ENV_LOG: str(log)})


def _fired(log):
    return [h["point"] for h in crashpoints.read_log(log) if h["fire"]]


def _routes(rig):
    conn = rig.dest.connect("check")
    try:
        return {k: (r.state, r.routing) for k, r in st.load_routing(conn).items()}
    finally:
        conn.close()


def _alerts(rig):
    return collections.Counter(r[0] for r in rig.dest.run("SELECT kind FROM _cdc.alerts"))


# --------------------------------------------------------------------------------------------- first start

@pytest.mark.parametrize("point", [None, "stream.slot_created", "stream.first_row"])
def test_first_start_creates_the_slot_then_the_row(tmp_path, point):
    log = tmp_path / "hits.log"
    try:
        with td.local_destination() as dest:
            rig = Rig(dest, tmp_path, first_start=False)
            rig.slot.exists = False
            _arm(point, log)
            with rig.running() as (comp, sup):
                rig.wait_applied(rig.slot.consistent_point, sup=sup)
            crashpoints.reconfigure({})
            if point:
                assert _fired(log) == [point]
            assert rig.source.created == 1                 # a crash after the creation finds the slot: no second one
            rows = rig.dest.run("SELECT slot_name, applied_lsn, state FROM _cdc.stream_state")
            assert rows == [("cdc_v2_slot", rig.slot.consistent_point, "applied")]
    finally:
        crashpoints.reconfigure({})


def test_no_table_streams_before_the_row(dest, tmp_path):
    from cdc_v2 import transform as T
    from cdc_v2.stream.state import StreamStateError
    rig = Rig(dest, tmp_path, first_start=False)
    lane = rig.lane()
    with pytest.raises(StreamStateError):
        with lane.transaction() as tx:
            src = rig.slot.catalog[K]
            st.register_table(tx, src, T.plan(src, server_version_num=180006))


# --------------------------------------------------------------------------------------------- quarantine

def _quarantine_run(tmp_path, point):
    log = tmp_path / ("q_%s.log" % point)
    try:
        with td.local_destination() as dest:
            rig = Rig(dest, tmp_path)
            # the destination holds a row the source never had: the stream's insert at that key is no source
            # history (ApplyInvariantError), a failure of table k that no retry cures
            dest.run('INSERT INTO "public"."k" VALUES (1, \'stray\')')
            s = rig.slot
            s.txn([("rows", K, [ins(1, "a")]), ("rows", N, [ins(1, "x")])])
            s.txn([("rows", N, [ins(2, "y")])])
            last = s.txn([("rows", K, [ins(3, "c")]), ("rows", N, [ins(3, "z")])])
            _arm(point, log)
            with rig.running() as (comp, sup):
                rig.wait_applied(last.commit_lsn, sup=sup)
            crashpoints.reconfigure({})
            keyed, keyless = rig.dest_state()
            rig.requests = rig.dest.run("SELECT schema_name, table_name, state, gap FROM _cdc.backfill_requests")
            return rig, _fired(log), keyed, keyless, _routes(rig), _alerts(rig)
    finally:
        crashpoints.reconfigure({})


@pytest.mark.parametrize("point", [None, "stream.quarantined"])
def test_a_table_that_always_fails_leaves_the_stream_after_its_attempts(tmp_path, point):
    rig, fired, keyed, keyless, routes, alerts = _quarantine_run(tmp_path, point)
    if point:
        assert fired == [point]
    assert routes[K] == ("quarantined", "skip")
    assert routes[N] == ("streaming", "streaming")
    assert keyed == {1: "stray"}                             # nothing of k was applied after it failed
    assert keyless == collections.Counter({(1, "x"): 1, (2, "y"): 1, (3, "z"): 1})   # the others went on
    assert alerts["table_quarantined"] == 1
    assert rig.requests == [("public", "k", "pending", True)]
    assert rig.slot.confirmed == rig.slot.txns[-1].commit_lsn  # the slot keeps advancing


# --------------------------------------------------------------------------------------------- last line

def _last_line_run(tmp_path, point, culprit=False):
    log = tmp_path / ("l_%s.log" % point)
    try:
        with td.local_destination() as dest:
            cat = default_catalog()
            iv = ("public", "iv")
            if culprit:
                cat[iv] = table("public", "iv", [col(1, "id", "int8"), col(2, "d", "interval")], key=(1,),
                                oid=16410)
            rig = Rig(dest, tmp_path, catalog=cat, tables=(K, N, iv) if culprit else (K, N),
                      stream=dict(max_attempts_at_position=2))
            s = rig.slot
            s.txn([("rows", K, [ins(1, "a")]), ("rows", N, [ins(1, "x")])])
            bad = s.txn([("rows", K, [ins(2, "b")])], fatal={"failing": {"lsn": s.lsn + 0x100 - 0x40, "txId": 1},
                                                              "culprit": iv if culprit else None})
            bad.fatal["failing"]["lsn"] = bad.commit_lsn - 0x40
            last = s.txn([("rows", K, [ins(3, "c")]), ("rows", N, [ins(3, "z")])])
            _arm(point, log)
            with rig.running() as (comp, sup):
                rig.wait_applied(last.commit_lsn, timeout=60, sup=sup)
                if point == "stream.slot_advanced":
                    pass
            crashpoints.reconfigure({})
            keyed, keyless = rig.dest_state()
            rig.row = rig.dest.run("SELECT applied_lsn, state FROM _cdc.stream_state")[0]
            return rig, _fired(log), keyed, keyless, _routes(rig), _alerts(rig)
    finally:
        crashpoints.reconfigure({})


@pytest.mark.parametrize("point", [None, "stream.repositioned"])
def test_a_failure_of_no_table_at_one_position_ends_in_the_last_line(tmp_path, point):
    rig, fired, keyed, keyless, routes, alerts = _last_line_run(tmp_path, point)
    if point:
        assert fired == [point]
    assert rig.source.advanced, "the slot was moved past the failing place"
    # every table is backfilled after the gap (the stand-in: routing skip, the requests pending)
    assert routes[K] == ("backfilling", "skip") and routes[N] == ("backfilling", "skip")
    assert alerts["stream_repositioned"] == 1
    assert keyed == {1: "a"} and keyless == collections.Counter({(1, "x"): 1})
    assert rig.row == (rig.source.advanced[0], "repositioned")


def test_a_crash_between_the_slot_advance_and_the_reposition_is_a_slot_finding(tmp_path):
    """With the minimal attach step a slot that is ahead stops the stream with an alert (package V enacts it as
    the same reposition)."""
    log = tmp_path / "adv.log"
    try:
        with td.local_destination() as dest:
            rig = Rig(dest, tmp_path, stream=dict(max_attempts_at_position=2))
            s = rig.slot
            s.txn([("rows", K, [ins(1, "a")])])
            bad = s.txn([("rows", K, [ins(2, "b")])], fatal={"failing": {"lsn": 0, "txId": 1}})
            bad.fatal["failing"]["lsn"] = bad.commit_lsn - 0x40
            _arm("stream.slot_advanced", log)
            from cdc_v2.deadline import Deadline
            with rig.running() as (comp, sup):
                d = Deadline.after(30)
                while not _alerts(rig)["slot_finding"]:
                    d.sleep(0.1, "waiting for the slot finding")
            crashpoints.reconfigure({})
            assert _fired(log) == ["stream.slot_advanced"]
            row = rig.dest.run("SELECT slot_state FROM _cdc.stream_state")[0]
            assert row == ("broken",)
    finally:
        crashpoints.reconfigure({})


def test_a_table_whose_value_the_driver_cannot_parse_is_found_by_probing_and_quarantined(tmp_path):
    rig, fired, keyed, keyless, routes, alerts = _last_line_run(tmp_path, None, culprit=True)
    assert not rig.source.advanced, "probing found the table: no last line"
    assert routes[("public", "iv")] == ("quarantined", "skip")
    assert routes[K] == ("streaming", "streaming")
    assert keyed == {1: "a", 2: "b", 3: "c"}                # the failing transaction's other rows were applied
    assert alerts["table_quarantined"] == 1
