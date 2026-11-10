"""Live tests of the cut (marker ``localpg``: they need Postgres and are excluded from the default run).

    uv run pytest -m localpg tests/cut/test_cut_live.py

They use the cut worker's private cluster through tools/localpg: home ``~/.cache/cdc_flight_v2/pg-cut``, ports
55600-55607 (the environment variables CDC_V2_CUT_PG_HOME and CDC_V2_CUT_PG_PORT_BASE change that).  The cluster is
started if needed and stopped again if these tests started it.  The whole module takes about two minutes; the
stress run itself 15 seconds.

**Another major version.**  The Postgres binaries come from ``CDC_V2_PG_BIN``, the variable tools/localpg reads;
nothing here names a binary of its own.  A cluster home belongs to the major version that initialized it, so a run
on another version needs another home:

    CDC_V2_PG_BIN=<bin directory of 16> CDC_V2_CUT_PG_HOME=~/.cache/cdc_flight_v2/pg-cut-16 \
        uv run pytest -m localpg tests/cut/test_cut_live.py

No test names a version: what depends on the server is asked of ``cut.export_defect`` or read from the result.
"""

import json
import os

import pytest

from cdc_v2.pg import cut as C
from cdc_v2.testing import cutprimary as CP
from cdc_v2.testing import cutstress as S

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(420)]

#: As in tests/conftest.py: the variable of tools/localpg, and its default when the variable is not set.
PG_BIN = os.environ.get(S.PG_BIN_ENV, "/opt/homebrew/opt/postgresql@18/bin")
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


def test_every_cut_was_checked_with_the_complete_checker(stress):
    """A result says which steps judged its cuts (the missing-row step finds a row the snapshot lost) and on
    which server it ran, so that a result from before a step existed cannot be taken for a complete one."""
    assert stress["checker_steps"] == list(S.CHECKER_STEPS)
    assert "missing_rows" in stress["checker_steps"]
    assert stress["server_version_num"] >= 160000
    assert stress["binaries_from_" + S.PG_BIN_ENV] == bool(os.environ.get(S.PG_BIN_ENV))


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
    assert r["checker_steps"] == list(S.CHECKER_STEPS)
    assert r["wrong_rules"]["stmt_as_r0"] > 0
    assert r["wrong_rules"]["r1_inside"] > 0


def test_export_defect_matches_the_library_guard(cluster):
    """The library refuses exported snapshots exactly on the servers that hand out wrong ones."""
    r = CP.export_defect(cluster)
    assert r["library_expects_defect"] == C.export_defect(r["server_version_num"])
    assert r["defect_shown"] == r["library_expects_defect"], r
    assert r["cases"]["no subtransaction"]["wrong"] == []
    assert r["slot_dropped"]


def test_keyless_table_gets_a_quiet_exported_snapshot_or_one_reader(cluster):
    """The rule for tables without a key.  While a write transaction is open on the primary the wait for a quiet
    exported snapshot ends with a result, not an error, and the entry point gives one reader its own snapshot;
    after the commit it gives a quiet exported snapshot that a second session imports.  On a server without the
    export defect it gives an exported snapshot both times."""
    r = CP.keyless(cluster, quiet_wait_s=1.0)
    assert r["ok"], r
    assert r["slot_dropped"]
    assert r["server_has_the_export_defect"] == C.export_defect(r["server_version_num"])
    first, second, third = (r[k] for k in ("while_a_transaction_is_open", "keyless_cut_while_open",
                                           "keyless_cut_after_the_commit"))
    assert first["quiet_only_wait_returned"] == "NoQuietSnapshot" and first["is_false_in_a_condition"]
    assert 0.9 <= first["seconds"] < 5 and first["attempts"] >= 2      # the declared wait, and not much more
    assert second["way"] == ("own" if r["server_has_the_export_defect"] else "exported")
    assert third["way"] == "exported" and third["a_reader_that_imports_it_sees_both"]


def test_stream_positions(cluster):
    """COMMIT.end_lsn is E, BEGIN.final_lsn is the start of the commit record, the stream is in E order."""
    r = CP.positions(cluster)
    assert r["ok"], r
    assert r["e_order_violations_primary"] == 0 and r["e_order_violations_replica"] == 0
    assert r["stream_order_equals_commit_order"]
    assert r["standby_slot_delivers_the_same_xid_S_E"]


def test_standby_that_has_just_started_is_refused_until_it_settles(cluster):
    """A commit waits for a synchronous standby across a checkpoint; the standby we read restarts from that
    checkpoint.  Its snapshot then hides the transaction although the commit record ends before R0.  The
    library's settle floor refuses every such cut and accepts a cut again after the next replayed commit."""
    r = CP.standby_start(cluster)
    assert "error" not in r, r
    assert r["ok"] and r["slot_dropped"], r
    assert not r["guard_accepts_a_wrong_cut"], r
    assert not r["guard_refuses_a_right_cut_at_the_end"], r
    assert r["before_the_restart"]["snapshot_reads"] == 1
    assert not r["before_the_restart"]["library_refuses_a_cut_taken_now"]
    if r["rule_violated"]:
        # negative control: without the floor the position rule alone gives a wrong answer (seen on every
        # release examined); the library must have refused the cut that shows it
        assert r["after_the_restart"]["library_refuses_a_cut_taken_now"], r
    else:
        # a server that no longer shows the hazard: say so instead of passing silently
        pytest.xfail("server %s did not hide the commit after the restart: the settle floor was not exercised"
                     % r["server_version_num"])


def test_a_cut_is_valid_only_on_the_timeline_it_was_taken_on(cluster):
    """A second standby is added, a cut is taken on it and left open, the standby is promoted.  The open cut
    still reads its old snapshot, so nothing in the data shows that it is void: the timeline does.  The cut is
    valid on the timeline of its source and on no other; the library refuses a new standby cut and a new settle
    floor on the promoted node; a primary cut there belongs to the new timeline."""
    r = CP.promotion(cluster)
    assert r["ok"], r
    cut, after = r["open_cut"], r["after_promotion"]
    assert cut["timeline"] == cut["timeline_of_the_source"]
    assert cut["valid_on_the_timeline_of_the_source"] and not cut["valid_without_a_timeline"]
    assert after["timeline"] > cut["timeline"]
    assert not after["the_open_cut_is_valid_on_the_new_timeline"]
    assert after["the_open_cut_still_reads"] == 1 and after["a_new_snapshot_reads"] == 2
    for key in ("library_on_a_new_cut", "library_on_a_new_exported_cut", "library_on_a_new_settle_floor"):
        assert r[key].startswith("refused"), r[key]
    promoted = r["primary_cut_on_the_promoted_node"]
    assert promoted["timeline"] == after["timeline"] and promoted["valid_on_the_new_timeline"]
    assert not promoted["valid_on_the_timeline_of_the_source"]
    assert CP.PROMO_NODE not in " ".join(r["replication_slots_left_on_the_primary"])


def test_primary_rules_under_a_held_back_synchronous_standby(cluster):
    """Cuts on the primary while commits wait for a standby whose replay is paused now and then: the stream
    delivers transactions that snapshots do not see yet.  The exact rule and the gap tracker hold; a later
    snapshot alone, and a later snapshot checked with pg_xact_status, are caught."""
    r = CP.primary_stress(cluster, "live-test-primary", 20, sync="remote_apply", pause_period=2)
    assert r["ok"], r["violations"]
    assert r["violations"] == {}
    assert r["cuts_checked"] >= 2000
    wrong = r["wrong_rules"]
    assert wrong["later_snapshot"] > 0
    assert wrong["status_check"] > 0
    assert wrong["passed_flush"] > 0
