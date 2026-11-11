"""Package F, hermetic (default run): schedules and plans from seeds, the ledger and its restart rule on a scratch
git repository, the reducer, the stream fuzzer on the recorded corpus, the consumer loops on a local DuckDB file,
the writer-log check, and the toy's own parsing and frame path."""
import json
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

import duckdb
import pytest

from cdc_v2.stream import frames as F
from cdc_v2.testing import frames as FE
from cdc_v2.testing.fuzz import faultkinds as fk
from cdc_v2.testing.fuzz import ledger, observers as obs, reduce as red, stream_fuzzer as sf
from cdc_v2.testing.fuzz.leadership import check_writer_log
from cdc_v2.testing.fuzz.system import FAULT_KINDS, WORKLOAD_FEATURES
from cdc_v2.testing.workload import Features, Workload, WorkloadConfig

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "toy"))
import toy  # noqa: E402


class FakeSystem:
    """Enough of a System for planning faults (nothing is injected)."""

    def __init__(self, unsupported=()):
        self.unsupported = set(unsupported)

    def supports(self, kind):
        return kind not in self.unsupported

    def crash_points(self):
        return toy.points_for("correct")

    def fault_sites(self):
        return list(toy.SITES)

    def compared_tables(self):
        return ["public.w_k0", "public.w_nk"]


# ------------------------------------------------------------------------------------------------- schedules

def _plan_dump(seed, system=None, episodes=5):
    s = fk.Schedule(system or FakeSystem())
    rng = random.Random(seed)
    return json.dumps([[(o, d, f.kind, f.params) for o, d, f in s.plan(rng, 10.0)] for _ in range(episodes)],
                      sort_keys=True, default=str)


def test_fault_schedule_is_a_function_of_the_seed():
    assert _plan_dump(7) == _plan_dump(7)
    assert _plan_dump(7) != _plan_dump(8)


def test_every_fault_kind_has_a_weight_a_maker_and_a_proof():
    sysm = FakeSystem()
    assert set(fk.DEFAULT_WEIGHTS) == set(FAULT_KINDS)
    for kind in FAULT_KINDS:
        f, dur = fk.make_fault(kind, random.Random(1), sysm)
        assert f.kind == kind and dur >= 0
        assert len(FAULT_KINDS[kind]) == 3


def test_unsupported_kinds_are_not_scheduled_and_are_reported():
    s = fk.Schedule(FakeSystem(unsupported={"replica_promote", "spill_full"}))
    assert "replica_promote" in s.not_attacked and "spill_full" in s.not_attacked
    rng = random.Random(3)
    kinds = {f.kind for _ in range(200) for _o, _d, f in s.plan(rng, 10.0)}
    assert not kinds & {"replica_promote", "spill_full"}
    assert len(kinds) > 15                                 # the schedule mixes kinds; it is not one kind


def test_episodes_overlap_on_purpose():
    s = fk.Schedule(FakeSystem(), per_episode=(2, 3))
    rng = random.Random(11)
    overlaps = 0
    for _ in range(100):
        p = s.plan(rng, 10.0)
        overlaps += any(b[0] < a[0] + a[1] for a, b in zip(p, p[1:]))
    assert overlaps > 5


# ------------------------------------------------------------------------------------------------- workload shapes

F_NEW = Features.only(parent_child=True, ledger=True, large_marked=True, large_rollback=True)


def test_new_shapes_are_reproducible_and_appear():
    a = Workload(WorkloadConfig(seed=5, features=F_NEW, large_txn_rows=7, large_txn_prob=0.2)).plan(8)
    b = Workload(WorkloadConfig(seed=5, features=F_NEW, large_txn_rows=7, large_txn_prob=0.2)).plan(8)
    assert json.dumps(a) == json.dumps(b)
    text = json.dumps(a)
    for table in ("w_parent", "w_child", "w_debit", "w_credit", "w_bulk", "w_bulk_mark"):
        assert table in text
    shapes = {e.get("shape") for e in a if e["type"] == "txn"}
    assert {"family", "ledger", "large_marked", "large_rollback"} <= shapes


def test_new_shapes_off_by_default_keep_old_plans():
    old = Workload(WorkloadConfig(seed=42)).plan(5)
    assert "w_parent" not in json.dumps(old) and "w_bulk" not in json.dumps(old)
    assert set(WORKLOAD_FEATURES) >= {"parent_child", "ledger", "large_marked", "large_rollback"}


def test_rolled_back_bulk_never_enters_the_model():
    w = Workload(WorkloadConfig(seed=9, features=Features.only(large_rollback=True), large_txn_rows=5))
    entries = w.plan(3)
    assert all(e["end"] == "rollback" for e in entries if e["type"] == "txn")
    assert all(not b for b in w.model.batches.values())


def test_ledger_and_family_invariants_hold_in_every_planned_state():
    """Replaying the plan into a model database (DuckDB) keeps the invariants after every transaction."""
    w = Workload(WorkloadConfig(seed=13, features=Features.only(parent_child=True, ledger=True), writers=2))
    con = duckdb.connect()
    for s in w.setup_statements():
        if not s.startswith(("CREATE SCHEMA", "ALTER TABLE")):
            con.execute(s.replace('"public".', ""))
    for e in w.plan(6)[1:]:
        if e["type"] != "txn" or e["end"] != "commit":
            continue
        for s in e["statements"][1:-1]:
            con.execute(s.replace('"public".', ""))
        assert con.execute("SELECT (SELECT coalesce(sum(amount),0) FROM w_debit) = "
                           "(SELECT coalesce(sum(amount),0) FROM w_credit)").fetchone()[0]
        assert con.execute("SELECT count(*) FROM w_parent p WHERE n_children <> (SELECT count(*) FROM w_child c "
                           "WHERE c.parent_id = p.id)").fetchone()[0] == 0


# ------------------------------------------------------------------------------------------------- ledger

def _git(repo, *args):
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True).stdout.strip()


def _commit(repo, path, text, msg):
    p = repo / path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    _git(repo, "add", path)
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", msg)
    return _git(repo, "rev-parse", "HEAD")


def _run(fuzzer, seed, commit, clean=True, system="pipe", mode="replica", hours=0.5, faults=None, outcome="pass",
         self_test=False, rounds=0):
    return {"fuzzer": fuzzer, "seed": seed, "commit": commit, "clean": clean, "system": system, "tier": "local",
            "source_mode": mode, "duration_s": hours * 3600, "faults": faults or {}, "no_effect": {},
            "outcome": outcome, "self_test": self_test, "counts": {"rounds": rounds}}


@pytest.fixture
def scratch(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    c0 = _commit(repo, "src/cdc_v2/stream/a.py", "1", "stream code")
    c1 = _commit(repo, "docs/x.md", "doc", "a document only")
    c2 = _commit(repo, "tests/t.py", "t", "a test only")
    root = tmp_path / "evidence"
    for fz in ledger.FUZZERS:
        (root / fz).mkdir(parents=True)
    return repo, root, (c0, c1, c2)


def _write_runs(root, runs):
    for r in runs:
        with open(root / r["fuzzer"] / "runs.jsonl", "a") as f:
            f.write(json.dumps(r) + "\n")


def test_restart_commit_is_the_newest_commit_touching_an_affecting_path(scratch):
    repo, root, (c0, c1, c2) = scratch
    rc = ledger.restart_commit(ledger.Git(repo), "workload", [])
    assert rc["commit"] == c0 and "src/cdc_v2/stream/a.py" in rc["why"]
    _write_runs(root, [_run("workload", 1, c0), _run("workload", 2, c2), _run("workload", 3, c2, clean=False)])
    g = ledger.build(root, repo)["fuzzers"]["workload"]["groups"][0]
    assert g["all_runs"]["runs"] == 3 and g["counted"]["seeds"] == 2      # the dirty tree is not counted


def test_a_fix_to_correctness_code_restarts_the_counters(scratch):
    repo, root, (c0, c1, c2) = scratch
    _write_runs(root, [_run("workload", 1, c2), _run("workload", 2, c2)])
    c3 = _commit(repo, "src/cdc_v2/stream/a.py", "2", "fix in the stream")
    _write_runs(root, [_run("workload", 3, c3)])
    g = ledger.build(root, repo)["fuzzers"]["workload"]
    assert g["restart"]["commit"] == c3
    assert g["counted_total"]["seeds"] == 1
    # the leadership fuzzer does not care about a stream-only... it does: src/cdc_v2/stream is in its list
    assert ledger.restart_commit(ledger.Git(repo), "leadership", [])["commit"] == c3
    # an exemption takes the commit out, with its reason listed for review
    (root / "exempt.json").write_text(json.dumps({"exempt": [{"commit": c3, "reason": "comment only"}]}))
    d = ledger.build(root, repo)
    assert d["fuzzers"]["workload"]["restart"]["commit"] == c0
    assert d["fuzzers"]["workload"]["counted_total"]["seeds"] == 3
    assert d["exempt_commits"][0]["reason"] == "comment only"


def test_documents_and_tests_do_not_restart(scratch):
    repo, root, (c0, c1, c2) = scratch
    _commit(repo, "docs/y.md", "more", "doc")
    _commit(repo, "tests/fuzz/x.py", "x", "test")
    assert ledger.restart_commit(ledger.Git(repo), "fault", [])["commit"] == c0


def test_self_test_runs_never_count_and_minimums_are_judged(scratch):
    repo, root, (c0, c1, c2) = scratch
    kinds = {"kill_point": 60, "net_reset": 120}
    _write_runs(root, [_run("fault", s, c2, system="toy-correct", self_test=True, faults=kinds) for s in range(5)])
    _write_runs(root, [_run("fault", s, c2, faults={"kill_point": 20, "net_reset": 40}) for s in range(4)])
    _write_runs(root, [_run("leadership", 1, c2, rounds=600), _run("leadership", 2, c2, rounds=500)])
    d = ledger.build(root, repo)
    fault = d["fuzzers"]["fault"]
    toy_g = [g for g in fault["groups"] if g["self_test"]][0]
    assert "against_minimum" not in toy_g and toy_g["note"]
    tot = fault["against_minimum_total"]
    assert fault["counted_total"]["seeds"] == 4 and tot["seeds"]["met"] is False
    assert tot["faults"]["value"] == 240 and tot["per_kind"]["below"] == {"kill_point": 80}
    assert fault["bar_met"] is False and "planetscale/replica" in fault["combos"]["missing"]
    lead = d["fuzzers"]["leadership"]
    assert lead["counted_total"]["rounds"] == 1100 and lead["against_minimum_total"]["rounds"]["met"] is True
    assert d["fuzzers"]["value"]["note"]


@pytest.mark.parametrize("path", ["uv.lock", "jvm/debezium.lock.json", "pyproject.toml",
                                  "src/cdc_v2/testing/fuzz/run.py", "src/cdc_v2/oracle/comparison.py",
                                  "src/cdc_v2/testing/workload.py", "src/cdc_v2/crashpoints.py"])
def test_pins_and_the_instrument_restart_the_counters(scratch, path):
    repo, root, (c0, c1, c2) = scratch
    _write_runs(root, [_run("fault", 1, c2)])
    c3 = _commit(repo, path, "changed", "a pin or the instrument")
    d = ledger.build(root, repo)["fuzzers"]["fault"]
    assert d["restart"]["commit"] == c3 and d["counted_total"]["seeds"] == 0
    (root / "exempt.json").write_text(json.dumps({"exempt": [{"commit": c3, "reason": "the reason, read at gates"}]}))
    d2 = ledger.build(root, repo)
    assert d2["fuzzers"]["fault"]["counted_total"]["seeds"] == 1
    text = ledger.status_text(d2)
    assert c3 in text and "the reason, read at gates" in text              # every entry in full


def test_each_of_the_four_combinations_needs_a_quarter(scratch):
    repo, root, (c0, c1, c2) = scratch
    runs = [_run("workload", s, c2, hours=0.2) for s in range(700)]              # local replica: 700 seeds, 140 h
    runs += [_run("workload", 1000 + s, c2, mode="primary", hours=0.2) for s in range(300)]
    _write_runs(root, runs)
    w = ledger.build(root, repo)["fuzzers"]["workload"]
    assert w["against_minimum_total"]["seeds"]["met"] and w["against_minimum_total"]["hours"]["met"]
    per = w["combos"]["per_combination"]
    assert per["local/replica"]["seeds"]["met"] and per["local/primary"]["seeds"]["met"]
    assert per["planetscale/replica"]["seeds"]["value"] == 0 and not per["planetscale/replica"]["seeds"]["met"]
    assert w["combos"]["fraction"] == ledger.COMBO_FRACTION == 0.25 and w["bar_met"] is False
    assert "local/primary" in ledger.status_text(ledger.build(root, repo))


def test_infrastructure_is_an_allow_list():
    import psycopg
    from cdc_v2.testing.fuzz import run as fr
    w = fr.FaultWindow()
    ri = psycopg.errors.ObjectNotInPrerequisiteState("cannot update table because it does not have a replica identity")
    lost = psycopg.OperationalError("server closed the connection unexpectedly")
    quiet = [fr.source_error(ri, w), fr.source_error(lost, w)]
    assert not any(e["allowed"] for e in quiet) and quiet[0]["sqlstate"] == "55000"
    with pytest.raises(fr._Outcome) as o:
        fr.judge_source_errors(quiet)
    assert o.value.outcome == "error" and "55000" in o.value.reason
    w.enter()
    during = [fr.source_error(lost, w), fr.source_error(ri, w)]
    w.leave()
    assert during[0]["allowed"] and not during[1]["allowed"]        # only a lost connection, only during a fault
    with pytest.raises(fr._Outcome):
        fr.judge_source_errors(during)
    with pytest.raises(fr.Infrastructure):
        fr.judge_source_errors(during[:1])
    fr.judge_source_errors([])


def test_held_seeds_skip_infrastructure(tmp_path):
    root = tmp_path / "ev"
    (root / "workload").mkdir(parents=True)
    _write_runs(root, [_run("workload", 1, "c"), _run("workload", 2, "c", outcome="infrastructure"),
                       _run("workload", 3, "c", mode="primary")])
    assert ledger.held_seeds(root, "workload", "pipe", "replica") == {1}


# ------------------------------------------------------------------------------------------------- reducer

def test_ddmin_finds_the_two_entries_that_matter():
    calls = []

    def fails(sub):
        calls.append(len(sub))
        return 13 in sub and 57 in sub
    assert red.ddmin(list(range(100)), fails, budget=400) == [13, 57]
    assert len(calls) < 200


def test_ddmin_keeps_a_single_cause_and_respects_its_budget():
    assert red.ddmin(list(range(64)), lambda s: 5 in s) == [5]
    n = []
    red.ddmin(list(range(1000)), lambda s: (n.append(1) or True) and 3 in s and 999 in s, budget=10)
    assert len(n) <= 10


def test_regression_test_file_is_written(tmp_path, monkeypatch):
    monkeypatch.setattr(red, "REGRESSION_DIR", HERE.parent.parent / "tests" / "fuzz" / "regressions_tmp_unit")
    root = HERE.parent.parent / "evidence" / "robustness_unit_tmp"
    case = {"system": "toy:split", "source_mode": "replica", "seed": 4, "options": {}, "fuzzer": "workload",
            "outcome": "violation", "entries": [{"type": "txn", "round": 0, "writer": 0, "statements": ["BEGIN",
                                                                                                     "COMMIT"]}],
            "faults": []}
    try:
        meta = red.write_regression(root, "workload-toy-split-replica-s4-abc", case, {"reproduced": True}, True)
        test = (HERE.parent.parent / meta["regression_test"]).read_text()
        compile(test, "t.py", "exec")
        assert "xfail(strict=True" in test and "localpg" in test
        assert json.loads((root / "workload" / "failures" / "workload-toy-split-replica-s4-abc" /
                           "case.json").read_text())["seed"] == 4
    finally:
        import shutil
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(HERE.parent.parent / "tests" / "fuzz" / "regressions_tmp_unit", ignore_errors=True)


# ------------------------------------------------------------------------------------------------- stream fuzzer

def _corpus():
    c = sf.load_corpus()
    assert c, "the recorded corpus is missing (tests/fuzz/record_corpus.py)"
    return c


def test_stream_fuzzer_property_holds_on_the_engine_corpus():
    corpus = [x for x in _corpus() if x[0] == "engine_prefix"] + \
        [("synth%d" % s, FE.Encoder().encode_all(sf.synth_stream(s, 6))) for s in range(3)]
    res = sf.run_offline(range(1, 401), corpus)
    assert res["failures"] == 0, res["failing_cases"][:3]
    assert len(res["by_kind"]) >= 15
    det = sum(res["detected"].values())
    assert det > 150


def test_stream_fuzzer_on_the_full_recording_bounded():
    corpus = [x for x in _corpus() if x[0] == "engine_all_types"]
    res = sf.run_offline(range(1000, 1030), corpus)
    assert res["failures"] == 0, res["failing_cases"][:3]


def test_negative_controls_are_caught():
    corpus = [("synth%d" % s, FE.Encoder().encode_all(sf.synth_stream(s, 6))) for s in range(3)]
    trusting = sf.run_offline(range(1, 301), corpus, assembler_cls=sf.TrustingAssembler, decoder="lenient")
    assert trusting["failures"] > 10
    heartbeat = sf.run_offline(range(1, 60), corpus, kinds=["heartbeat_ahead"], assembler_cls=sf.TrustingAssembler)
    assert heartbeat["failures"] > 0                       # a heartbeat position must never be adopted


def test_new_life_replay_is_discarded_by_position():
    corpus = [("synth1", FE.Encoder().encode_all(sf.synth_stream(1, 8)))]
    res = sf.run_offline(range(1, 40), corpus, kinds=["new_life_replay"])
    assert res["failures"] == 0 and res["by_kind"] == {"new_life_replay": 39}


def test_live_pipe_stall_is_a_deadline_never_a_partial_frame():
    data = FE.Encoder().encode_all(sf.synth_stream(3, 4))
    r = sf.run_live(5, data, read_deadline_s=0.1)
    assert r["ok"], r


# ------------------------------------------------------------------------------------------------- consumer loops

def _ledger_db(path):
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE w_debit (id BIGINT, entry BIGINT, amount BIGINT)")
    con.execute("CREATE TABLE w_credit (id BIGINT, entry BIGINT, amount BIGINT)")
    return con


def _writer(con, split, n=40):
    cur = con.cursor()
    for i in range(n):
        if split:
            cur.execute("INSERT INTO w_debit VALUES (?, ?, 10)", [i, i])
            time.sleep(0.003)
            cur.execute("INSERT INTO w_credit VALUES (?, ?, 10)", [i, i])
        else:
            cur.execute("BEGIN")
            cur.execute("INSERT INTO w_debit VALUES (?, ?, 10)", [i, i])
            cur.execute("INSERT INTO w_credit VALUES (?, ?, 10)", [i, i])
            cur.execute("COMMIT")
        time.sleep(0.002)


@pytest.mark.parametrize("split", [False, True])
def test_consumer_loop_sees_a_split_writer_and_not_a_correct_one(tmp_path, split):
    con = _ledger_db(tmp_path / "d.duckdb")
    loop = obs.ConsumerLoop(obs.debit_credit("main"), lambda: con.cursor()).start()
    w = threading.Thread(target=_writer, args=(con, split))
    w.start()
    w.join()
    rep = loop.stop()
    assert rep.observations > 20 and rep.longest_gap_s < 1.0
    if split:
        assert rep.violations > 0 and rep.first_violations[0]["debits"] != rep.first_violations[0]["credits"]
    else:
        assert rep.violations == 0


def test_parent_child_and_large_marked_checks(tmp_path):
    con = duckdb.connect()
    con.execute("CREATE TABLE w_parent (id BIGINT, w INTEGER, n_children INTEGER)")
    con.execute("CREATE TABLE w_child (id BIGINT, parent_id BIGINT, w INTEGER, v VARCHAR)")
    con.execute("CREATE TABLE w_bulk (id BIGINT, batch BIGINT, v INTEGER)")
    con.execute("CREATE TABLE w_bulk_mark (batch BIGINT, n INTEGER)")
    pc, lm = obs.parent_child("main"), obs.large_marked("main")
    con.execute("INSERT INTO w_parent VALUES (1, 0, 2)")
    con.execute("INSERT INTO w_child VALUES (10, 1, 0, 'a'), (11, 1, 0, 'b')")
    assert pc.judge(con.execute(pc.sql).fetchone())[0]
    con.execute("INSERT INTO w_child VALUES (12, 1, 0, 'c'), (20, 2, 0, 'orphan')")
    ok, detail = pc.judge(con.execute(pc.sql).fetchone())
    assert not ok and detail["parents_with_wrong_count"] == 1 and detail["orphan_parent_ids"] == 1
    con.execute("INSERT INTO w_bulk SELECT range, 5, 0 FROM range(3)")
    con.execute("INSERT INTO w_bulk_mark VALUES (5, 3)")
    assert lm.judge(con.execute(lm.sql).fetchone())[0]
    con.execute("DELETE FROM w_bulk WHERE id = 0")
    ok, detail = lm.judge(con.execute(lm.sql).fetchone())
    assert not ok and detail["sample_batch_rows_marked"][0] == [5, 2, 3]


def test_versions_check_for_spec_3_2(tmp_path):
    con = duckdb.connect()
    con.execute("CREATE TABLE t (id INTEGER, v VARCHAR)")
    con.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
    v1 = tuple(int(x) for x in con.execute(obs.version_sql("t", "main")).fetchone())
    chk = obs.versions("t", {v1}, "main")
    loop = obs.ConsumerLoop(chk, lambda: con.cursor())
    assert loop.observe_once(con.cursor()) is True
    con.execute("DELETE FROM t WHERE id = 2")                 # a partial table
    assert loop.observe_once(con.cursor()) is False
    con.execute("DROP TABLE t")                               # a missing table is a violation here
    assert loop.observe_once(con.cursor()) is False
    assert loop.report.violations == 2 and loop.report.first_violations[1]["missing_table"]


# ------------------------------------------------------------------------------------------------- writer log

def test_writer_log_check():
    ok = [(1, 1, "a", 100, 0, "resync"), (2, 1, "a", 110, 5, "txn"), (3, 2, "b", 120, 6, "txn")]
    assert check_writer_log(ok) == []
    two = ok + [(4, 1, "a", 130, 7, "txn")]
    assert check_writer_log(two)[0]["two_writers"]
    dup = ok + [(4, 2, "b", 120, 6, "txn")]
    assert any("transaction_twice" in p or "position_not_rising" in p for p in check_writer_log(dup))
    two_epochs = ok + [(4, 3, "b", 130, 7, "txn")]
    assert any("run_with_two_epochs" in p for p in check_writer_log(two_epochs))


# ------------------------------------------------------------------------------------------------- the toy

def test_toy_parses_test_decoding_lines():
    ins = toy.parse_change("table public.t: INSERT: id[bigint]:1 a[integer]:5 b[text]:'it''s é ✓' "
                           "d[timestamp with time zone]:'2001-02-03 04:05:06.789+00' e[boolean]:true c[text]:null")
    assert ins["new"] == {"id": "1", "a": "5", "b": "it's é ✓", "d": "2001-02-03 04:05:06.789+00", "e": "true",
                          "c": None}
    up = toy.parse_change("table public.t: UPDATE: old-key: id[bigint]:1 new-tuple: id[bigint]:2 "
                          "big[text]:unchanged-toast-datum")
    assert up["old"] == {"id": "1"} and up["new"]["big"] is toy.UNCHANGED
    tr = toy.parse_change("table public.t, public.nk: TRUNCATE: (no-flags)")
    assert tr == {"kind": "T", "tables": ["public.t", "public.nk"]}
    de = toy.parse_change("table public.nk: DELETE: w[integer]:0 a[integer]:1")
    assert de["old"] == {"w": "0", "a": "1"}
    q = toy.parse_change('table "S x"."T": INSERT: "a b"[text]:\'x y\'')
    assert q["table"] == "S x.T" and q["new"] == {"a b": "x y"}


def _batch():
    meta = {"public.t": {"columns": [("id", "bigint"), ("v", "text")], "types": ["BIGINT", "VARCHAR"], "key": [0]},
            "public.nk": {"columns": [("a", "integer")], "types": ["INTEGER"], "key": []}}
    txns = [{"xid": 5, "commit_lsn": 1000, "changes": [
        {"kind": "I", "table": "public.t", "old": None, "new": {"id": "1", "v": "x"}},
        {"kind": "U", "table": "public.t", "old": {"id": "1"}, "new": {"id": "2", "v": toy.UNCHANGED}},
        {"kind": "T", "tables": ["public.nk"]}]},
        {"xid": 6, "commit_lsn": 1100, "changes": [
            {"kind": "D", "table": "public.nk", "old": {}, "new": None}]}]
    return meta, txns


def test_toy_frame_path_round_trip_and_detection():
    meta, txns = _batch()
    data = toy.encode_batch(txns, meta)
    got = toy.assemble(F.decode_all(data), strict=True)
    assert [t["commit_lsn"] for t in got] == [1000, 1100] and len(got[0]["items"]) == 3
    assert got[0]["items"][1][1]["k0"] == "1" and got[0]["items"][1][1]["u1"] is True
    # a cut inside the second transaction: the strict path hands on the first only, trust_frames applies a part
    cut = data[:len(data) - 30]
    with pytest.raises(F.EngineBroken):
        F.decode_all(cut)
    spans = sf.frame_spans(data)
    at_boundary = data[:spans[-2][0]]                          # the last END is missing
    assert [t["commit_lsn"] for t in toy.assemble(F.decode_all(at_boundary), strict=True)] == [1000]
    assert [t["commit_lsn"] for t in toy.assemble(toy.lenient_frames(at_boundary), strict=False)] == [1000, 1100]


def test_writer_log_epoch_belongs_to_one_run_and_takeover_raises_it():
    base = [(1, 1, "a", 0, 0, "takeover"), (2, 1, "a", 100, 0, "resync"), (3, 1, "a", 110, 5, "txn")]
    assert check_writer_log(base + [(4, 2, "b", 110, 0, "takeover"), (5, 2, "b", 120, 6, "txn")]) == []
    shared = base + [(4, 1, "b", 120, 6, "txn")]
    assert any("epoch_with_two_runs" in p for p in check_writer_log(shared))
    stale = base + [(4, 1, "c", 110, 0, "takeover")]
    assert any("takeover_without_new_epoch" in p for p in check_writer_log(stale))
    late = base + [(4, 2, "b", 110, 0, "takeover"), (5, 1, "a", 120, 6, "txn")]   # old leader after the take-over
    assert any(p.get("two_writers") for p in check_writer_log(late))


def test_a_reader_that_drops_the_tail_is_caught_unless_the_fault_shortened_the_stream():
    true = [{"ack": 1, "n": 1, "digest": "a"}, {"ack": 2, "n": 1, "digest": "b"}]
    short = {"txns": true[:1], "ends": ["clean"], "position": 1}
    assert sf.check_case(true, short, "flip_payload", {})["ok"] is False
    assert sf.check_case(true, short, "cut_at_boundary", {})["ok"] is True


def test_toy_points_per_variant():
    assert "toyrep.data_committed" in toy.points_for("position_apart")
    assert "toyrep.data_committed" not in toy.points_for("correct")
    assert "toyrep.acked_early" in toy.points_for("ack_early")
    assert "toyrep.released" not in toy.points_for("correct") and "toyrep.released" in toy.points_for("correct", True)
