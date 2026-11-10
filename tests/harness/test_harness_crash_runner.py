"""The crash runner proven against a correct toy (always passes) and a deliberately wrong one (caught).

Set CDC_V2_HARNESS_EVIDENCE=<dir> to keep the ledgers and summaries there (evidence/p3/harness/).
"""
import json
import os
import subprocess

import pytest
import toy_adapter

from cdc_v2 import crashpoints
from cdc_v2.testing.crash import Config, CrashRunner

EVIDENCE = os.environ.get("CDC_V2_HARNESS_EVIDENCE")
SEEDS = (1, 2, 3)

crashpoints.declare("toy.never", "a point the toy never reaches (runner self-test)")


def out_paths(tmp_path, name):
    d = EVIDENCE or str(tmp_path)
    ledger = os.path.join(d, name + "_ledger.jsonl")
    if os.path.exists(ledger):
        os.unlink(ledger)
    return ledger, os.path.join(d, name + "_summary.json")


def runner(factory, tmp_path, name, **cfg):
    ledger, summary = out_paths(tmp_path, name)
    r = CrashRunner(factory, tmp_path / "work" / name, ledger_path=ledger, config=Config(workers=4, **cfg))
    return r, ledger, summary


def no_toy_process_left(tmp_path):
    out = subprocess.run(["ps", "-A", "-o", "command="], capture_output=True, text=True, timeout=10).stdout
    return [ln for ln in out.splitlines() if str(tmp_path) in ln]


def assert_ledger_clean(ledger, tmp_path):
    text = open(ledger, encoding="utf-8").read()
    assert text.strip()
    assert str(tmp_path) not in text and os.path.expanduser("~") not in text and "/Users/" not in text
    for line in text.splitlines():
        rec = json.loads(line)
        assert {"mode", "point", "seed", "outcome", "timings", "hits"} <= set(rec)


def test_correct_toy_passes_at_every_point_with_three_seeds(tmp_path):
    r, ledger, summary = runner(toy_adapter.CorrectToy, tmp_path, "toy_correct_points")
    recs = r.run_crash_points(seeds=SEEDS)
    s = r.summary()
    r.write_summary(summary)
    assert len(recs) == len(r.declared) * len(SEEDS)
    assert s["missing_points"] == [] and s["coverage"] == "7/7"
    assert s["all_passed"], s["failures"]
    for rec in recs:                                    # died by SIGKILL exactly at the armed point and hit
        fired = [h for h in rec["hits"] if h["tag"] == "a" and h["fire"]]
        assert fired == [{"tag": "a", "point": rec["point"], "hit": rec["hit"], "fire": True, "action": "kill"}]
        assert rec["exit"]["signal_name"] == "SIGKILL"
    assert {rec["hit"] for rec in recs if rec["point"] == "toy.schema_ready"} == {1}   # declared once=True
    assert_ledger_clean(ledger, tmp_path)
    assert no_toy_process_left(tmp_path) == []


def test_kill_while_the_commit_is_in_flight(tmp_path):
    """kill_later: SIGKILL from another thread while the toy goes on into COMMIT (unknown commit outcome)."""
    r, ledger, summary = runner(toy_adapter.CorrectToy, tmp_path, "toy_correct_commit_in_flight")
    recs = r.run_crash_points(points=["toy.commit_sent"], seeds=SEEDS + (4, 5, 6), action="kill_later", delay_ms=0)
    r.write_summary(summary)
    assert all(rec["outcome"] == "pass" and rec["exit"]["signal_name"] == "SIGKILL" for rec in recs), \
        [rec["reason"] for rec in recs]
    assert all(rec["action"] == "kill_later" for rec in recs)
    assert no_toy_process_left(tmp_path) == []


def test_wrong_toy_is_caught(tmp_path):
    r, ledger, summary = runner(toy_adapter.WrongToy, tmp_path, "toy_wrong_points")
    r.run_crash_points(seeds=SEEDS)
    s = r.summary()
    r.write_summary(summary)
    assert s["missing_points"] == []
    assert not s["all_passed"]
    caught = [f for f in s["failures"] if f["outcome"] == "diverged"]
    assert {f["point"] for f in caught} >= {"toy.data_committed"}
    assert s["by_point"]["toy.data_committed"] == {"diverged": 3}    # every seed
    assert all("duplicated" in f["reason"] for f in caught)
    assert {f["outcome"] for f in s["failures"]} == {"diverged"}      # nothing else went wrong
    assert_ledger_clean(ledger, tmp_path)
    assert no_toy_process_left(tmp_path) == []


def test_random_kills(tmp_path):
    r, ledger, summary = runner(toy_adapter.CorrectToy, tmp_path, "toy_correct_random_kills")
    recs = r.run_random_kills(seeds=SEEDS, kills=4)
    r.write_summary(summary)
    assert all(rec["outcome"] == "pass" for rec in recs), [rec["reason"] for rec in recs]
    kills = [k for rec in recs for k in rec["kills"]]
    assert len(kills) == 12 and all(k["exit"]["signal_name"] == "SIGKILL" for k in kills)
    assert len({k["after_s"] for k in kills}) == 12             # random times from the seed
    assert any(k["points_hit"] > 0 for k in kills)              # the toy was working when killed
    assert no_toy_process_left(tmp_path) == []


def test_freeze_and_continue_with_a_second_instance(tmp_path):
    r, ledger, summary = runner(toy_adapter.CorrectToy, tmp_path, "toy_correct_freeze")
    recs = r.run_freeze(seeds=SEEDS, second_instance=True)
    recs += r.run_freeze(seeds=(1,), second_instance=True,
                         at_points=["toy.data_written", "toy.position_written", "toy.committed"])
    r.write_summary(summary)
    assert all(rec["outcome"] == "pass" for rec in recs), [rec["reason"] for rec in recs]
    assert all(rec["second_instance"] and rec["frozen_s"] > 0.2 for rec in recs)
    at = [rec for rec in recs if rec["point"]]
    assert [rec["frozen_after_point"] for rec in at] == ["toy.data_written", "toy.position_written", "toy.committed"]
    assert no_toy_process_left(tmp_path) == []


def test_a_hung_instance_that_blocks_progress_is_caught(tmp_path):
    """The toy has no take-over: an instance that hangs inside its batch keeps the DuckDB file lock, so the second
    instance can never catch up. The runner must report that as a liveness failure (SPEC 4.5), not wait for ever."""
    r, ledger, summary = runner(toy_adapter.CorrectToy, tmp_path, "toy_correct_hang", catch_up_timeout=2.0)
    recs = r.run_freeze(seeds=(1,), at_points=["toy.data_written"], action="hang")
    r.write_summary(summary)
    assert [(rec["mode"], rec["outcome"]) for rec in recs] == [("hang", "no_catch_up")]
    assert recs[0]["frozen_after_point"] == "toy.data_written"
    with pytest.raises(ValueError):
        r.run_crash_points(action="stop")
    assert no_toy_process_left(tmp_path) == []


class NeverToy(toy_adapter.CorrectToy):
    def crash_points(self):
        return ["toy.never"]


def test_a_point_that_is_never_reached_is_a_failure(tmp_path):
    r, ledger, summary = runner(NeverToy, tmp_path, "toy_unreached", crash_timeout=2.0)
    recs = r.run_crash_points(seeds=(1,))
    s = r.summary()
    assert [rec["outcome"] for rec in recs] == ["not_reached"]
    assert s["missing_points"] == ["toy.never"] and not s["all_passed"]
    assert no_toy_process_left(tmp_path) == []


@pytest.mark.skipif(not os.environ.get("CDC_V2_HARNESS_LONG"), reason="long run: set CDC_V2_HARNESS_LONG=1")
def test_long_crash_campaign_on_the_toy(tmp_path):
    """Opt-in: 20 seeds per point, 20 random-kill and 20 freeze scenarios on the correct toy."""
    seeds = tuple(range(1, 21))
    r, ledger, summary = runner(toy_adapter.CorrectToy, tmp_path, "toy_correct_long")
    r.run_crash_points(seeds=seeds)
    r.run_random_kills(seeds=seeds, kills=6)
    r.run_freeze(seeds=seeds)
    s = r.summary()
    r.write_summary(summary)
    assert s["all_passed"] and not s["missing_points"], s["failures"]
