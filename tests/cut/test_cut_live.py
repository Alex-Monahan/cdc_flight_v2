"""Live tests of the cut (marker ``localpg``: they need Postgres and are excluded from the default run).

    uv run pytest -m localpg tests/cut/test_cut_live.py

They use the cut worker's private cluster through tools/localpg: home ``~/.cache/cdc_flight_v2/pg-cut``, ports
55600-55607 (the environment variables CDC_V2_CUT_PG_HOME and CDC_V2_CUT_PG_PORT_BASE change that).  The cluster is
started if needed and stopped again if these tests started it.  The whole module takes about two minutes; the
stress run itself 15 seconds.
"""

import json
import os

import pytest

from cdc_v2.testing import cutprimary as CP
from cdc_v2.testing import cutstress as S

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(420)]

PG_BIN = os.environ.get("CDC_V2_PG_BIN", "/opt/homebrew/opt/postgresql@18/bin")
HOME = os.environ.get("CDC_V2_CUT_PG_HOME", S.HOME_DEFAULT)
PORT_BASE = int(os.environ.get("CDC_V2_CUT_PG_PORT_BASE", str(S.PORT_BASE_DEFAULT)))


def _running(cl: S.Cluster) -> bool:
    try:
        status = json.loads(cl.localpg("status", "--json", check=False) or "{}")
    except ValueError:
        return False
    return any(n.get("pid") for n in status.get("nodes", []))


@pytest.fixture(scope="module")
def cluster():
    if not os.access(os.path.join(PG_BIN, "postgres"), os.X_OK):
        pytest.skip("Postgres binaries not found in %s" % PG_BIN)
    cl = S.Cluster(HOME, PORT_BASE)
    was_running = _running(cl)
    cl.localpg("up")
    yield cl
    if not was_running:
        cl.localpg("down", check=False)


@pytest.fixture(scope="module")
def stress(cluster):
    """One short campaign: four writers on the primary, two cut takers on the standby, every cut checked."""
    cfg = S.RunConfig(label="live-test", seconds=15, writers=("plain", "async", "subxact", "abort"),
                      takers_own=2, takers_exported=0)
    return S.run(cluster, cfg)


def test_short_stress_run_has_no_violation(stress):
    assert stress["problems"] == []
    assert stress["violations"] == {}, stress["violation_examples"]
    assert stress["e_order_violations"] == 0
    assert stress["ok"]
    assert stress["slot_dropped"]
    assert stress["cuts_checked"] >= 1000
    assert stress["transactions_delivered"] >= 5000
    # the run must have exercised the rule: cuts with transactions on both sides and in between
    assert stress["unknown_set"]["base"]["max"] >= 1


def test_negative_control_wrong_rules_are_caught(stress):
    """The same cuts, judged by rules that are wrong on purpose.  The checker has to find violations."""
    wrong = stress["wrong_rules"]
    assert wrong["r1_inside"] > 0        # E <= R1 taken as inside
    assert wrong["xmax_inside"] > 0      # id below xmax taken as inside
    assert wrong["xmin_outside"] > 0     # id at or above xmin taken as outside


def test_position_read_by_the_snapshot_statement_is_not_r0(cluster):
    """Exported way (no subtransaction writers, so the server's export defect stays out of the picture): the
    position read by the statement that takes the snapshot, used as R0, is caught; the real rule holds."""
    cfg = S.RunConfig(label="live-test-exported", seconds=8, writers=("plain", "async", "abort"),
                      takers_own=0, takers_exported=2)
    r = S.run(cluster, cfg)
    assert r["problems"] == []
    assert r["violations"] == {}, r["violation_examples"]
    assert r["ok"]
    assert r["wrong_rules"]["stmt_as_r0"] > 0
    assert r["wrong_rules"]["r1_inside"] > 0


def test_export_defect_matches_the_library_guard(cluster):
    """The library refuses exported snapshots exactly on the servers that hand out wrong ones."""
    r = CP.export_defect(cluster)
    assert r["defect_shown"] == r["library_expects_defect"], r
    assert r["cases"]["no subtransaction"]["wrong"] == []
    assert r["slot_dropped"]


def test_stream_positions(cluster):
    """COMMIT.end_lsn is E, BEGIN.final_lsn is the start of the commit record, the stream is in E order."""
    r = CP.positions(cluster)
    assert r["ok"], r
    assert r["e_order_violations_primary"] == 0 and r["e_order_violations_replica"] == 0
    assert r["stream_order_equals_commit_order"]
    assert r["standby_slot_delivers_the_same_xid_S_E"]
