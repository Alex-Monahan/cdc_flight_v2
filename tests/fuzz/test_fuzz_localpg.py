"""Package F on local Postgres (opt-in, marker `localpg`; homes pg-f<n> of the fuzz pool, ports 55700-55795):

    tools/with-env uv run pytest -q -m localpg tests/fuzz/test_fuzz_localpg.py            # about 25 minutes
    tools/with-env uv run pytest -q -m localpg tests/fuzz/test_fuzz_localpg.py -k "not variant"   # about 6 minutes

The toy end to end under each fuzzer; every wrong variant found by the fuzzer the task names, and the correct toy
passing the same seeds; a small suite-1.7 on the toy; a campaign killed in the middle, cleaned and started again;
nothing left behind. Ledger lines go to a temporary directory, never to evidence/.
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from cdc_v2.testing import procs
from cdc_v2.testing.fuzz import ledger, pool
from cdc_v2.testing.fuzz.campaign import Campaign
from cdc_v2.testing.fuzz.variants import TABLE, run_variants

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(1800)]
REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module", autouse=True)
def nothing_left_behind():
    yield
    out = pool.clean(stop_clusters=True)
    for rec in out:
        assert not rec.get("skipped"), rec
    for n in range(pool.HOMES):
        p = pool.LOCK_DIR / ("pg-f%d.json" % n)
        if p.exists():
            r = json.loads(p.read_text())
            assert not r["databases"] and not r["procs"], r


@pytest.mark.parametrize("fuzzer,budget", [("workload", 15), ("fault", 25), ("stream", 15), ("leadership", None)])
def test_toy_end_to_end_under_each_fuzzer(tmp_path, fuzzer, budget):
    c = Campaign(fuzzer, "toy", seeds=[1], budget_s=budget, rounds=2, ledger_root=tmp_path, log=lambda *_: None)
    res = c.run()
    assert [r["outcome"] for r in res] == ["pass"], res[0]["reason"]
    lines = ledger.read_runs(tmp_path, fuzzer)
    assert len(lines) == 1 and lines[0]["self_test"] is True
    if fuzzer == "fault":
        assert sum(lines[0]["faults"].values()) >= 1
    if fuzzer == "stream":
        assert lines[0]["faults"]["stream_bytes"] >= 1 and lines[0]["stream_detected"] >= 1


def test_a_system_that_breaks_the_source_writes_fails_the_run(tmp_path):
    """H-014: the system takes the replica identity away from a published table; the workload's UPDATE and DELETE
    fail at the source with SQLSTATE 55000. That is a failure of the run, never `infrastructure`."""
    c = Campaign("workload", "toy", seeds=[1], budget_s=15, ledger_root=tmp_path, log=lambda *_: None,
                 options={"toy": {"sabotage_replica_identity": True}})
    res = c.run()
    assert res[0]["outcome"] == "error", res[0]
    assert "55000" in res[0]["reason"] and "allow-list" in res[0]["reason"]


@pytest.mark.parametrize("variant", sorted(TABLE))
def test_variant_is_found_and_the_correct_toy_passes(tmp_path, variant):
    doc = run_variants([variant], ledger_root=tmp_path, log=lambda *_: None)
    row = doc["variants"][0]
    assert row["found"], row
    assert row["correct_toy_passed"], row


def test_suite_17_small_on_the_toy(tmp_path):
    from cdc_v2.testing.fuzz.suite17 import run_suite
    from cdc_v2.testing.fuzz.system import make_system
    lease = pool.acquire(wait_s=60)
    pts = make_system("toy", lease).crash_points()
    lease.release()
    res = run_suite("toy", tmp_path / "out", REPO, seeds=(1,), soak_s=40, min_soak_faults=5, group_budget_s=16,
                    workers=1, points=[pts[0], "toyrep.commit_sent", "toyrep.committed"], log=lambda *_: None)
    assert res["parts"]["crash_points"]["all_passed"], res["parts"]["crash_points"]
    assert not res["parts"]["crash_points"]["missing_points"]
    assert res["all_passed"], json.dumps(res["parts"], default=str)[:2000]


def test_campaign_killed_cleaned_and_started_again(tmp_path):
    argv = [str(REPO / "tools" / "fuzz"), "--ledger-root", str(tmp_path), "run", "--fuzzer", "workload",
            "--system", "toy", "--seeds", "1..2", "--budget", "12"]
    p = procs.start(argv, name="campaign", log_path=str(tmp_path / "campaign.log"))
    time.sleep(14)                                         # inside the first run
    assert p.alive()
    p.kill_group()                     # the campaign (uv and python) only: its toy instances run in sessions of their own
    p.wait(10)
    leftover = [json.loads((pool.LOCK_DIR / ("pg-f%d.json" % n)).read_text()) for n in range(pool.HOMES)
                if (pool.LOCK_DIR / ("pg-f%d.json" % n)).exists()]
    assert any(r["procs"] or r["databases"] for r in leftover)
    cleaned = subprocess.run([str(REPO / "tools" / "fuzz"), "clean"], capture_output=True, text=True, timeout=300)
    assert cleaned.returncode == 0
    done = [json.loads(line) for line in cleaned.stdout.splitlines() if line.startswith("{")]
    assert any(d.get("killed") or d.get("databases") for d in done), done
    for n in range(pool.HOMES):
        f = pool.LOCK_DIR / ("pg-f%d.json" % n)
        if f.exists():
            r = json.loads(f.read_text())
            assert not r["procs"] and not r["databases"]
    assert ledger.held_seeds(tmp_path, "workload", "toy-correct", "replica") <= {1}
    again = subprocess.run(argv, capture_output=True, text=True, timeout=900)
    assert again.returncode == 0, again.stdout[-2000:] + again.stderr[-2000:]
    assert ledger.held_seeds(tmp_path, "workload", "toy-correct", "replica") == {1, 2}
