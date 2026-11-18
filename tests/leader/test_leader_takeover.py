"""Tier T0: the guard decision and the take-over of the bootstrap on a local DuckDB database in this process.

The bootstrap's code runs here unchanged (it is loaded from `flights/cdc_v2/main.py`); only its exit is replaced
(`fake_finish`) and, on a local database, the server's interrupt by package D's local stand-in.
"""
import threading
import time
import uuid

import pytest

from cdc_v2 import leader, md  # noqa: F401 - leader registers its migrations (_cdc.contenders)
from cdc_v2.testing import destination as td

from leader_helpers import FLIGHT, OTHER_FLIGHT, Exited, Hooks, fake_finish, make_stale, new_boot

pytestmark = pytest.mark.timeout(120)


@pytest.fixture
def nofinish(boot, monkeypatch):
    monkeypatch.setattr(boot, "finish", fake_finish)
    return boot


def raw_set(dest, sql, params=None):
    dest.run(sql, params)


# ---------------------------------------------------------------------------------------------- the guard
def test_the_guard_creates_the_leader_tables_on_an_empty_database(boot):
    with td.local_destination(leader_tables=False) as dest:
        b = new_boot(boot, dest.database)
        rec = b.guard()
        assert (rec.epoch, rec.state, rec.age_us) == (0, "released", None)
        assert boot.decide(rec, FLIGHT, 1.0) == ("take_over", "initial")
        assert dest.run(boot.CHECK_LEADER_TABLES_SQL)[0] == (1, 18, 18, 1)
        b.db.close()


def _record(boot, **kw):
    row = dict(epoch=3, state="leading", flight_id=FLIGHT, run_number=7, run_id="r", hb_epoch=3, phase="streaming",
               age_us=1_000_000)
    row.update(kw)
    return boot.Record([row[k] for k in ("epoch", "state", "flight_id", "run_number", "run_id", "hb_epoch", "phase",
                                         "age_us")])


@pytest.mark.parametrize("case,fields,expect", [
    ("initial", dict(epoch=0, state="released", age_us=None, hb_epoch=0), ("take_over", "initial")),
    ("released", dict(state="released"), ("take_over", "released")),
    ("healthy same Flight", dict(), ("exit_same", "healthy")),
    ("healthy other Flight", dict(flight_id=OTHER_FLIGHT), ("exit_other", "healthy")),
    ("stale", dict(age_us=45_000_000), ("take_over", "stale")),
    ("exactly at the limit", dict(age_us=30_000_000), ("take_over", "stale")),
    ("just inside the limit", dict(age_us=29_999_999), ("exit_same", "healthy")),
    ("heartbeat of an older epoch", dict(hb_epoch=2), ("take_over", "stale")),
    ("never beaten", dict(age_us=None), ("take_over", "stale")),
    ("a state no run writes", dict(state="zombie"), ("damaged", "state")),
])
def test_the_guard_decision_for_every_state_of_the_record(boot, case, fields, expect):
    assert boot.decide(_record(boot, **fields), FLIGHT, 30.0) == expect, case


def test_the_guard_reads_the_age_by_the_destination_clock(boot):
    """The age is computed inside the statement that reads the record (design 4.1); here on local DuckDB."""
    with td.local_destination() as dest:
        raw_set(dest, "UPDATE _cdc.leader SET epoch = 4, state = 'leading', flight_id = ?", [FLIGHT])
        raw_set(dest, "UPDATE _cdc.heartbeat SET epoch = 4, beat_at = uuid_extract_timestamp(uuidv7()) "
                      "- INTERVAL 40 SECOND")
        b = new_boot(boot, dest.database)
        rec = b.guard()
        assert 39_000_000 <= rec.age_us <= 45_000_000
        assert boot.decide(rec, FLIGHT, 30.0) == ("take_over", "stale")
        assert boot.decide(rec, FLIGHT, 60.0) == ("exit_same", "healthy")
        b.db.close()


def test_a_healthy_leader_ends_the_newcomer_by_the_guard_rule(nofinish):
    boot = nofinish
    with td.local_destination() as dest:
        raw_set(dest, "UPDATE _cdc.leader SET epoch = 2, state = 'leading', flight_id = ?, run_number = 5", [FLIGHT])
        raw_set(dest, "UPDATE _cdc.heartbeat SET epoch = 2, beat_at = uuid_extract_timestamp(uuidv7())")
        for flight, code in ((FLIGHT, boot.EXIT_OK), (OTHER_FLIGHT, boot.EXIT_CONTENDING)):
            b = new_boot(boot, dest.database, flight_id=flight)
            with pytest.raises(Exited) as e:
                b.run()
            assert e.value.code == code
            b.db.close()
        # nothing changed: the record is the leader's, no fence row moved, no connection was registered
        assert dest.run("SELECT epoch, state, run_number FROM _cdc.leader")[0] == (2, "leading", 5)
        assert dest.run("SELECT count(*) FROM _cdc.fence WHERE epoch <> 0 OR n <> 0")[0] == (0,)
        assert dest.run("SELECT count(*) FROM _cdc.connections")[0] == (0,)


# ---------------------------------------------------------------------------------------------- take-over
def _take_over_from_threads(boot, dest, n):
    barrier = threading.Barrier(n)
    out = [None] * n
    boots = [new_boot(boot, dest.database, run_number=i + 10) for i in range(n)]

    def one(i):
        b = boots[i]
        try:
            rec = b.guard()
            barrier.wait(10)
            out[i] = ("won", b.take_over(rec), b.s.run_id, dict(b.report))
        except boot.AnotherRunLeads:
            out[i] = ("lost", None, b.s.run_id, dict(b.report))
        except Exception as e:                       # noqa: BLE001
            out[i] = ("error", repr(e), b.s.run_id, dict(b.report))
        finally:
            b.db.close()
    threads = [threading.Thread(target=one, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    return out


@pytest.mark.parametrize("reps", [pytest.param(8, id="8x"), pytest.param(60, id="60x", marks=pytest.mark.l_full)])
def test_simultaneous_take_overs_have_exactly_one_winner_every_time(nofinish, reps):
    boot = nofinish
    with td.local_destination() as dest:
        for rep in range(reps):
            make_stale(dest)
            before = dest.run("SELECT epoch, n FROM _cdc.fence WHERE lane = 'stream'")[0]
            out = _take_over_from_threads(boot, dest, 5)
            winners = [o for o in out if o[0] == "won"]
            assert len(winners) == 1, (rep, out)
            assert all(o[0] in ("won", "lost") for o in out), out
            epoch = winners[0][1]
            assert epoch == before[0] + 1
            row = dest.run("SELECT epoch, state, run_id FROM _cdc.leader")[0]
            assert row == (epoch, "leading", winners[0][2])
            assert dest.run("SELECT DISTINCT epoch FROM _cdc.fence")[0] == (epoch,)
            assert dest.run("SELECT n FROM _cdc.fence WHERE lane = 'stream'")[0] == (before[1] + 1,)
            assert dest.run("SELECT epoch, phase FROM _cdc.heartbeat")[0] == (epoch, "starting")


def test_the_take_over_writes_the_leader_machines_row_when_the_table_exists(nofinish):
    boot = nofinish
    with td.local_destination() as dest:
        b = new_boot(boot, dest.database)
        assert b.take_over(b.guard()) == 1
        assert b.facts["transition_row"] is False             # nobody migrated: no _cdc.transitions yet
        b.db.close()
        conn = dest.connect("m")
        md.migrate(md.Lane(conn, "control", 1))
        raw_set(dest, "UPDATE _cdc.heartbeat SET beat_at = uuid_extract_timestamp(uuidv7()) - INTERVAL 1 HOUR")
        b = new_boot(boot, dest.database, run_number=2)
        assert b.take_over(b.guard()) == 2
        assert b.facts["transition_row"] is True and b.facts["reason"] == "stale"
        rows = dest.run("SELECT machine, key, from_state, to_state, epoch, lane, seq, cause FROM _cdc.transitions "
                        "WHERE machine = 'leader'")
        n = dest.run("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0][0]
        assert rows == [("leader", "[]", "leading", "leading", 2, "control", n, "take-over (stale)")]
        b.db.close()


def test_a_take_over_meets_an_old_transaction_at_its_fence_and_interrupts_it(nofinish):
    """The old run stands between its fence statement and COMMIT (it holds the stream fence row); the take-over
    meets a conflict, interrupts the connections of earlier epochs from `_cdc.connections` and wins. The old run's
    COMMIT then fails, and its next transaction gets `FenceLost`."""
    boot = nofinish
    with td.local_destination() as dest:
        epoch = dest.lead()
        make_stale(dest)
        dest.run("CREATE TABLE t (v BIGINT)")
        lane = dest.lane("stream", epoch)
        result = {}

        def take_over():
            b = new_boot(boot, dest.database, run_number=9)
            result["epoch"] = b.take_over(b.guard())
            result["report"] = dict(b.report)
            b.db.close()

        with Hooks() as hooks:
            hooks.at("stream.fenced", take_over)
            with pytest.raises((md.CommitUnknown, md.ConnectionLost)):
                with lane.transaction() as tx:
                    tx.execute("INSERT INTO t VALUES (1)")
        assert result["epoch"] == epoch + 1
        assert result["report"]["conflicts"] >= 1 and result["report"]["interrupted"] >= 1, result
        assert dest.run("SELECT count(*) FROM t")[0] == (0,)
        with pytest.raises(md.FenceLost):
            with lane.transaction() as tx:
                tx.execute("INSERT INTO t VALUES (2)")
        assert dest.run("SELECT count(*) FROM t")[0] == (0,)


def test_without_the_interrupt_the_take_over_does_not_win_against_a_held_fence_row(nofinish):
    """The negative control: the same old transaction, a take-over that does not interrupt: it gives up after
    leader.takeover_timeout (here 1.5 s) with exit 4, and the old run commits."""
    boot = nofinish
    with td.local_destination() as dest:
        epoch = dest.lead()
        make_stale(dest)
        dest.run("CREATE TABLE t (v BIGINT)")
        lane = dest.lane("stream", epoch)
        result = {}

        def take_over():
            b = new_boot(boot, dest.database, interrupt=False, run_number=9, CDC_V2_TAKEOVER_INTERRUPT="0",
                         CDC_V2_TAKEOVER_TIMEOUT_S="1.5")
            try:
                b.take_over(b.guard())
            except Exited as e:
                result["code"] = e.code
                result["fields"] = e.fields
            b.db.close()

        with Hooks() as hooks:
            hooks.at("stream.fenced", take_over)
            with lane.transaction() as tx:
                tx.execute("INSERT INTO t VALUES (1)")
        assert result["code"] == boot.EXIT_NO_TAKEOVER and result["fields"]["conflicts"] >= 1
        assert result["fields"]["interrupted"] == 0
        assert dest.run("SELECT count(*) FROM t")[0] == (1,)
        assert dest.run("SELECT epoch FROM _cdc.leader")[0] == (epoch,)


def test_a_take_over_that_met_no_conflict_interrupts_the_old_runs_open_transaction_after_winning(nofinish):
    """Destination finding 2: an old transaction that holds only data rows does not stop the take-over, and the
    take-over then interrupts nothing; `after_win` interrupts once more, so the new leader's lanes are free."""
    boot = nofinish
    with td.local_destination() as dest:
        epoch = dest.lead()
        make_stale(dest)
        dest.run("CREATE TABLE t (k BIGINT, v BIGINT)")
        dest.run("INSERT INTO t VALUES (1, 0)")
        lane = dest.lane("stream", epoch)
        result = {}

        def take_over():
            b = new_boot(boot, dest.database, run_number=9)
            result["epoch"] = b.take_over(b.guard())
            result["before"] = dict(b.report)
            b.after_win()
            result["after"] = dict(b.facts)
            b.db.close()

        block = lane.transaction()
        tx = block.__enter__()
        tx.execute("UPDATE t SET v = v + 1 WHERE k = 1")           # holds a data row only
        take_over()
        with pytest.raises((md.ConnectionLost, md.FenceLost, md.CommitUnknown, md.Conflict)):
            block.__exit__(None, None, None)
        assert result["before"]["conflicts"] == 0 and result["before"]["interrupted"] == 0
        assert result["after"]["interrupted_after"] >= 1
        new = dest.lane("stream", result["epoch"], role="new")
        with new.transaction() as tx:
            tx.execute("UPDATE t SET v = v + 10 WHERE k = 1")
        assert dest.run("SELECT v FROM t")[0] == (10,)


class _CommitThenLose:
    """COMMIT reaches the destination and its answer is lost (``lands``), or it never arrives."""

    def __init__(self, boot, db, lands):
        self.boot, self.db, self.lands, self.calls = boot, db, lands, 0
        self.orig = db.commit

    def __call__(self, deadline):
        self.calls += 1
        if self.calls == 1:
            if self.lands:
                self.orig(deadline)
            else:
                self.db.rollback()
            raise self.boot.Unknown("the answer of COMMIT was lost")
        return self.orig(deadline)


@pytest.mark.parametrize("lands", [True, False])
def test_a_take_over_whose_commit_outcome_is_unknown_decides_by_run_id(nofinish, lands):
    boot = nofinish
    with td.local_destination() as dest:
        b = new_boot(boot, dest.database)
        b.db.commit = _CommitThenLose(boot, b.db, lands)
        rec = b.guard()
        assert b.take_over(rec) == 1
        assert b.report["unknown"] == 1
        assert b.report["attempts"] == (1 if lands else 2)
        assert dest.run("SELECT epoch, run_id FROM _cdc.leader")[0] == (1, b.s.run_id)
        assert dest.run("SELECT n FROM _cdc.fence WHERE lane = 'stream'")[0] == (1,)    # one take-over, not two
        b.db.close()


def test_an_unknown_outcome_never_takes_a_rivals_take_over_for_its_own(nofinish):
    """The first COMMIT is lost and did not land; meanwhile a rival takes over from the same epoch. The read
    after the wait finds the rival's run id at the next epoch: this run does not lead and exits by the guard."""
    boot = nofinish
    with td.local_destination() as dest:
        b = new_boot(boot, dest.database)
        rival = new_boot(boot, dest.database, run_number=2)

        def first(deadline):
            b.db.rollback()
            rival.take_over(rival.guard())
            raise boot.Unknown("lost")
        b.db.commit = first
        with pytest.raises(boot.AnotherRunLeads):
            b.take_over(b.guard())
        assert dest.run("SELECT epoch, run_id FROM _cdc.leader")[0] == (1, rival.s.run_id)
        b.db.close()
        rival.db.close()


def test_a_take_over_whose_old_leader_comes_back_healthy_exits(nofinish):
    """"Until it wins, or a healthy heartbeat appears": a conflict, and the record shows the old leader healthy
    again at the same epoch: the newcomer leaves by the guard rule."""
    boot = nofinish
    with td.local_destination() as dest:
        raw_set(dest, "UPDATE _cdc.leader SET epoch = 1, state = 'leading', flight_id = ?", [FLIGHT])
        raw_set(dest, "UPDATE _cdc.fence SET epoch = 1")
        raw_set(dest, "UPDATE _cdc.heartbeat SET epoch = 1, beat_at = uuid_extract_timestamp(uuidv7()) "
                      "- INTERVAL 1 HOUR")
        b = new_boot(boot, dest.database)
        rec = b.guard()
        assert boot.decide(rec, FLIGHT, b.s.stale_age_s)[0] == "take_over"
        raw = dest.raw()
        raw.execute("BEGIN")
        raw.execute("UPDATE _cdc.heartbeat SET beat_at = uuid_extract_timestamp(uuidv7()) WHERE epoch = 1")
        orig = b._reread

        def reread(seen, deadline):
            if raw is not None:
                raw.execute("COMMIT")                           # the old leader's beat lands
            return orig(seen, deadline)
        b._reread = reread
        with pytest.raises(boot.AnotherRunLeads, match="healthy again"):
            b.take_over(rec)
        assert dest.run("SELECT epoch FROM _cdc.leader")[0] == (1,)
        raw.close()
        b.db.close()


def test_a_damaged_record_is_refused(nofinish):
    boot = nofinish
    with td.local_destination() as dest:
        raw_set(dest, "UPDATE _cdc.leader SET state = 'zombie', epoch = 3")
        b = new_boot(boot, dest.database)
        with pytest.raises(Exited) as e:
            b.run()
        assert e.value.code == boot.EXIT_DAMAGED
        b.db.close()


def test_a_newcomer_of_another_flight_leaves_a_notice_and_changes_nothing_else(nofinish):
    boot = nofinish
    with td.local_destination() as dest:
        epoch = dest.lead()
        raw_set(dest, "UPDATE _cdc.leader SET flight_id = ?", [FLIGHT])
        raw_set(dest, "UPDATE _cdc.heartbeat SET beat_at = uuid_extract_timestamp(uuidv7())")
        before = dest.run("SELECT * FROM _cdc.fence ORDER BY lane")
        b = new_boot(boot, dest.database, flight_id=OTHER_FLIGHT, run_number=4)
        with pytest.raises(Exited) as e:
            b.run()
        assert e.value.code == boot.EXIT_CONTENDING and e.value.fields["noticed"] is True
        notes = dest.run("SELECT flight_id, run_number, leader_epoch, leader_flight_id FROM _cdc.contenders")
        assert notes == [(OTHER_FLIGHT, 4, epoch, FLIGHT)]
        assert dest.run("SELECT * FROM _cdc.fence ORDER BY lane") == before
        assert dest.run("SELECT epoch, state FROM _cdc.leader")[0] == (epoch, "leading")
        b.db.close()
