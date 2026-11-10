"""Fix round 2 of package F (hermetic): a setup failure is a failure, suite 1.7 never passes with a part missing, and
a leased home is deleted only after `localpg down` succeeded. Each test fails on the code of 19b4a52."""
import subprocess
from pathlib import Path

import pytest

from cdc_v2.testing import pg as tpg
from cdc_v2.testing.fuzz import run as fr
from cdc_v2.testing.fuzz import suite17

REPO = Path(__file__).resolve().parents[2]


class SetupFails:
    name, tier, source_node, is_self_test, wl = "fake", "local", "replica", False, None

    def __init__(self, error):
        self.error = error

    def setup(self, workdir, seed):
        raise self.error

    def teardown(self):
        pass

    def run_counts(self):
        return {}


def _run(tmp_path, error):
    return fr.Run(SetupFails(error), 1, tmp_path / "w", fr.RunConfig(budget_s=1), REPO).execute()


def test_a_refusal_at_setup_is_a_failure_not_infrastructure(tmp_path):
    rec = _run(tmp_path, RuntimeError("publication check refused: table without a replica identity"))
    assert rec["outcome"] == "error" and "setup failed" in rec["reason"]


@pytest.mark.parametrize("cause", fr.SETUP_ENVIRONMENT_CAUSES)
def test_only_named_environment_causes_are_infrastructure(tmp_path, cause):
    rec = _run(tmp_path, RuntimeError("localpg: primary did not start: FATAL: %s: No space left on device" % cause))
    assert rec["outcome"] == "infrastructure" and cause in rec["reason"]


def _parts(**over):
    parts = {"crash_points": {"all_passed": True, "missing_points": []}, "soak": {"passed": True}}
    parts.update({g: {"passed": True} for g in suite17.GROUPS})
    parts.update(over)
    return {k: v for k, v in parts.items() if v is not None}


def test_suite_never_passes_with_a_part_missing():
    assert suite17.judge(_parts()) == (True, [])
    ok, failed = suite17.judge(_parts(soak=None))
    assert not ok and failed == ["soak: missing"]
    ok, failed = suite17.judge(_parts(network={"skipped": "the system supports none of [...]"}))
    assert not ok and "network: skipped, not declared not applicable" in failed
    ok, _ = suite17.judge(_parts(network={"skipped": "none", "not_applicable": "no TCP link to the source",
                                          "passed": True}))
    assert ok
    ok, _ = suite17.judge(_parts(spill={"outcome": "pass"}))             # no explicit pass
    assert not ok
    ok, _ = suite17.judge(_parts(crash_points={"all_passed": True, "missing_points": None}))
    assert not ok


def test_a_home_is_kept_when_localpg_down_refused(tmp_path, monkeypatch):
    home = tmp_path / "pg-t1-55999"
    home.mkdir()
    (home / ".localpg-home").write_text("marker")
    calls = []

    def fake_localpg(*args, **kw):
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, "", "replica: shared-memory segment 7 still has 1 attached")
    monkeypatch.setattr(tpg, "localpg", fake_localpg)

    class Lease:
        base = 55999
        released = False

        def release(self):
            self.released = True
    c = tpg.PrivateCluster.__new__(tpg.PrivateCluster)
    c.lease = Lease()
    with pytest.raises(tpg.HarnessError, match="home is kept"):
        c._cleanup(home)
    assert home.exists() and c.lease.released and calls[0][0] == "down"
