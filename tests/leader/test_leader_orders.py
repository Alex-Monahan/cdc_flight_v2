"""Tier T0 (a local DuckDB file, after a CHECKPOINT) and marker `md` (MotherDuck): the heartbeat, the release and
a second take-over against the take-over, in every order (`order_probe.py` says how)."""
import os

import duckdb
import pytest

from cdc_v2.testing import destination as td

import order_probe
from conftest import ROOT

EVIDENCE = os.path.join(ROOT, "evidence", "p3", "leadership")


def _check(cases):
    for c in cases:
        assert c["violations"] == 0, c
        if c["contestant"] == "takeover":
            assert c["take_overs_committed"] + c["a_committed"] == c["rounds"], c     # exactly one of the two
        if c["order"] == "a_commits_first":
            assert c["a_committed"] == c["rounds"], c
            assert c["take_overs_committed"] == (0 if c["contestant"] == "takeover" else c["rounds"]), c
        if c["order"] == "stale":
            assert c["take_overs_committed"] == c["rounds"] and c["a_committed"] == 0, c


def test_every_order_on_a_local_file_after_a_checkpoint(boot, tmp_path):
    path = str(tmp_path / "orders.duckdb")
    setup = duckdb.connect(path)
    for sql in boot.LEADER_TABLE_STATEMENTS:
        setup.execute(sql)
    setup.close()
    cases = order_probe.run(boot, lambda: duckdb.connect(path), rounds=3)
    _check(cases)


@pytest.mark.md
def test_every_order_on_motherduck(boot):
    from cdc_v2.testing.harness_evidence import write_evidence
    with td.motherduck_destination("cdc_v2_t_l_") as dest:
        cases = order_probe.run(boot, dest.raw, rounds=5)
    write_evidence(os.path.join(EVIDENCE, "statement_orders.json"),
                   {"what": "heartbeat, release and a second take-over against the take-over, every order, on "
                            "MotherDuck (tests/leader/order_probe.py)", "cases": cases})
    _check(cases)
