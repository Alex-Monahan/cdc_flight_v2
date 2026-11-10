"""The transfers observer on MotherDuck (T1, SPEC 1.3).

The workload runs transfer transactions on the source. A stand-in applier copies them to the destination: once as
whole transactions (correct: the observer must see only the constant sum, and the oracle equality), once with each
source transaction split into two destination commits (the bug: the observer must see a different sum).
"""
import pytest

from cdc_v2.oracle import OracleConfig, compare
from cdc_v2.testing import loader
from cdc_v2.testing.md import STAND_IN_SLOT, write_stand_in_position
from cdc_v2.testing.observer import TransfersObserver, apply_transfers
from cdc_v2.testing.workload import Features, Workload, WorkloadConfig, read_log

pytestmark = [pytest.mark.t1, pytest.mark.timeout(600)]


def transfers_of_round(log, round_):
    return [e["statements"][1:-1] for e in read_log(log)
            if e["type"] == "txn" and e["round"] == round_ and e["end"] == "commit"]


def test_observer_catches_a_split_transaction(source_db, dest_db, tmp_path, evidence):
    log = tmp_path / "replay.jsonl"
    cfg = WorkloadConfig(seed=23, writers=2, txns_per_round=5, features=Features.only(transfers=True))
    wl = Workload(cfg, connect=source_db.connect, log_path=log)
    wl.setup()
    loader.load_database(source_db.params(), dest_db.conn)          # initial balances on the destination
    tables, total = wl.transfer_table_names, wl.transfer_sum

    # 1. whole transactions: no violation, and the destination equals the source afterwards
    wl.run(1)
    q1 = wl.quiesce()
    good = transfers_of_round(log, 0)
    obs = TransfersObserver(dest_db.connect, tables, total).start()
    apply_transfers(dest_db.conn, good)
    rep_good = obs.stop()
    write_stand_in_position(dest_db.conn, q1)
    eq = compare(source_db.source(), dest_db.destination(), OracleConfig(), position=q1, slot_name=STAND_IN_SLOT)

    # 2. every source transaction split into two destination commits, one second apart
    wl.run(1)
    q2 = wl.quiesce()
    bad = transfers_of_round(log, 1)
    obs = TransfersObserver(dest_db.connect, tables, total).start()
    apply_transfers(dest_db.conn, bad, split=True, pause=1.0)
    rep_bad = obs.stop()
    write_stand_in_position(dest_db.conn, q2)
    eq_after = compare(source_db.source(), dest_db.destination(), OracleConfig(), position=q2,
                       slot_name=STAND_IN_SLOT)
    wl.close()

    evidence("p3/oracle").write_json("observer", {
        "what": "transfers observer on MotherDuck: whole destination transactions vs. split ones",
        "workload": cfg.to_dict(), "expected_sum": total,
        "whole_transactions": {"transfers": len(good), "report": rep_good.__dict__, "oracle_equal_after": eq.equal},
        "split_transactions": {"transfers": len(bad), "report": rep_bad.__dict__,
                               "oracle_equal_after": eq_after.equal},
        "note": "after the split run the final state is still equal: only the observer sees the split"})
    assert good and bad
    assert rep_good.ok and rep_good.observations >= 5, rep_good
    assert eq.equal, eq.summary()
    assert rep_bad.violations > 0, rep_bad
    assert eq_after.equal, eq_after.summary()
