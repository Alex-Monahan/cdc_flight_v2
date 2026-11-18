"""Tier T0: the bootstrap's copies equal the package's originals, and the bootstrap holds the take-over and
nothing else that commits.

The bootstrap (`flights/cdc_v2/main.py`) runs before the repository can be imported, so it carries copies: the
statements of `cdc_v2.md.schema` it uses, the take-over (D's test copy in `cdc_v2.testing.destination`), the row
of `_cdc.transitions` (`cdc_v2.state`), the contenders' notice, the leader machine, the crash points and their
environment contract, the leader timings and their order, the classification of client errors, and the
platform calls. Each is compared here, character for character or behaviour for behaviour.
"""
import ast
import json
import os
import re

import duckdb
import pytest

from cdc_v2 import crashpoints, leader, state
from cdc_v2.leader import points, schema as leader_schema
from cdc_v2.leader.settings import ENV, LeaderSettings, check_order
from cdc_v2.md import errors as md_errors
from cdc_v2.md import schema
from cdc_v2.platform import local as plocal
from cdc_v2.platform import motherduck as pmd
from cdc_v2.platform import normalize_state
from cdc_v2.testing import destination as td
from cdc_v2.testing import machines as tm

from conftest import BOOTSTRAP, ROOT


def test_the_bootstrap_copies_the_named_statements_exactly(boot):
    assert schema.compare_statements(boot.COPIES, required=tuple(boot.COPIES)) == []
    assert schema.compare_statements(list(boot.LEADER_TABLE_STATEMENTS)) == []
    # the copy holds the take-over's companions the task names: registration and heartbeat
    for name in ("register_connection", "register_fence", "heartbeat", "interrupt_earlier_connections",
                 "list_earlier_connections", "create_connections"):
        assert name in boot.COPIES
    assert boot.NO_CONNECTION == "00000000-0000-0000-0000-000000000000"


def test_the_comparison_finds_a_changed_copy(boot):
    changed = dict(boot.COPIES)
    changed["heartbeat"] = changed["heartbeat"].replace("epoch = ?", "epoch = ? ")
    assert schema.compare_statements(changed, required=tuple(changed))


def test_the_take_over_is_the_one_package_d_tested(boot):
    assert boot.TAKEOVER_LEADER_SQL == td.TAKEOVER_LEADER_SQL
    assert boot.TAKEOVER_FENCE_SQL == td.TAKEOVER_FENCE_SQL          # raises n, never puts it back (F0)
    assert boot.TAKEOVER_HEARTBEAT_SQL == td.TAKEOVER_HEARTBEAT_SQL
    assert "n = n + 1" in boot.TAKEOVER_FENCE_SQL and "n - 1" not in boot.TAKEOVER_FENCE_SQL


def test_the_transition_row_and_the_notice_are_the_packages(boot):
    assert boot.TRANSITION_SQL == state._LOG_SQL
    assert boot.NOTICE_SQL == leader_schema.NOTICE_SQL


def test_the_leader_machine_copy_equals_the_declaration(boot):
    assert tuple(boot.LEADER_STATES) == leader.LEADER.states
    assert set(boot.LEADER_TRANSITIONS) == set(leader.LEADER.transitions)
    assert state.get("leader") is leader.LEADER


def test_the_crash_points_of_the_bootstrap_are_declared_in_the_package(boot):
    assert tuple(boot.BOOT_POINTS) == points.BOOT_POINTS
    for name, description, once in boot.BOOT_POINTS:
        cp = crashpoints.get(name)
        assert cp is not None and cp.description == description and cp.once == once
    # every point the bootstrap calls is in its list, and every listed point is called
    text = open(BOOTSTRAP, encoding="utf-8").read()
    called = set(re.findall(r"\bcrash_point\(\s*[\"']([^\"']+)[\"']", text))
    assert called == {p[0] for p in boot.BOOT_POINTS}
    # and the pipeline's points are called with their literal names in the package
    src = os.path.join(ROOT, "src", "cdc_v2", "leader")
    calls = set()
    for name in os.listdir(src):
        if name.endswith(".py"):
            calls |= set(re.findall(r"\bcrash_point\(\s*[\"']([^\"']+)[\"']", open(os.path.join(src, name)).read()))
    assert calls == set(points.PIPELINE_POINTS)


@pytest.mark.parametrize("spec", ["", "1", "3", "17", "seed:0:3", "seed:1:3", "seed:18446744073709551615:7",
                                  "seed:42:1"])
def test_the_crash_point_environment_contract_is_implemented_twice_alike(boot, spec):
    assert boot.parse_hit(spec) == crashpoints.parse_hit(spec)
    assert boot.splitmix64(0) == crashpoints.splitmix64(0) == 0xE220A8397B1DCDAF
    assert boot.splitmix64(1) == crashpoints.splitmix64(1) == 0x910A2DEC89025CC1


@pytest.mark.parametrize("bad", ["0", "x", "seed:1:0", "seed:a:1"])
def test_a_wrong_hit_fails_in_both(boot, bad):
    with pytest.raises(ValueError):
        crashpoints.parse_hit(bad)
    with pytest.raises(ValueError):
        boot.parse_hit(bad)


def test_the_hit_log_lines_have_the_same_form(boot, tmp_path):
    log_a, log_b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    env = {"CDC_V2_CRASH_POINT": "leader.taken_over", "CDC_V2_CRASH_HIT": "2", "CDC_V2_CRASH_ACTION": "raise",
           "CDC_V2_CRASH_LOG": str(log_a), "CDC_V2_CRASH_TAG": "t1"}
    crash = boot._Crash(env)
    crash.hit("leader.taken_over")
    with pytest.raises(SystemExit):
        crash.hit("leader.taken_over")
    crashpoints.reconfigure(dict(env, CDC_V2_CRASH_LOG=str(log_b)))
    try:
        crashpoints.crash_point("leader.taken_over")
        with pytest.raises(crashpoints.CrashPointHit):
            crashpoints.crash_point("leader.taken_over")
    finally:
        crashpoints.reconfigure({})
    a = [json.loads(x) for x in log_a.read_text().splitlines()]
    b = [json.loads(x) for x in log_b.read_text().splitlines()]
    assert [list(x) for x in a] == [list(x) for x in b]            # same keys in the same order
    strip = lambda rows: [{k: v for k, v in r.items() if k not in ("t", "pid")} for r in rows]  # noqa: E731
    assert strip(a) == strip(b)


def test_the_leader_timings_are_one_table(boot):
    shared = {attr: (var, default) for attr, var, default, is_shared in boot.TIMINGS if is_shared}
    defaults = LeaderSettings()
    for attr, (var, default) in shared.items():
        assert ENV[attr] == var, attr
        assert getattr(defaults, attr) == default, attr
    # the bootstrap passes exactly these on to the pipeline, which reads them back
    env = {var: repr(v * 1.5 if attr != "max_runtime_s" else 99.0) for attr, (var, v) in shared.items()}
    env.update({"CDC_V2_HEARTBEAT_INTERVAL_S": "1.0", "CDC_V2_STALE_AGE_S": "4.0", "CDC_V2_HARD_EXIT_AGE_S": "8.0"})
    s = LeaderSettings.from_env(env)
    assert (s.heartbeat_interval_s, s.stale_age_s, s.hard_exit_age_s, s.max_runtime_s) == (1.0, 4.0, 8.0, 99.0)


@pytest.mark.parametrize("interval,stale,hard", [(10, 30, 60), (10.01, 30, 60), (10, 30, 30), (1, 3.1, 3.2),
                                                 (0, 30, 60), (1, 3, 60), (0.3, 1.0, 2.0), (5, 2, 1)])
def test_the_order_of_the_timings_is_checked_alike(boot, interval, stale, hard):
    assert boot.check_order(interval, stale, hard) == check_order(interval, stale, hard)
    ok = not check_order(interval, stale, hard)
    assert ok == (interval <= stale / 3 and stale < hard and interval > 0)
    if ok:
        LeaderSettings(heartbeat_interval_s=interval, stale_age_s=stale, hard_exit_age_s=hard)
    else:
        with pytest.raises(ValueError):
            LeaderSettings(heartbeat_interval_s=interval, stale_age_s=stale, hard_exit_age_s=hard)


def test_the_bootstrap_refuses_settings_out_of_order(boot):
    env = {"CDC_V2_DATABASE": "cdc_v2_t_l_x", "MOTHERDUCK_FLIGHT_ID": "f", "MOTHERDUCK_FLIGHT_RUN_NUMBER": "1",
           "CDC_V2_SOURCE_DIR": ROOT, "CDC_V2_HEARTBEAT_INTERVAL_S": "11"}
    with pytest.raises(ValueError, match="a third of the stale age"):
        boot.Settings(env)
    env["CDC_V2_HEARTBEAT_INTERVAL_S"] = "1"
    s = boot.Settings(env)
    assert s.target == "md:cdc_v2_t_l_x?attach_mode=single" and s.is_motherduck
    for bad in ({"CDC_V2_DATABASE": "md:x"}, {"CDC_V2_DATABASE": "x?y=1"}, {"MOTHERDUCK_FLIGHT_RUN_NUMBER": "a"},
                {"CDC_V2_SOURCE_DIR": "", "CDC_V2_COMMIT": "abc"}, {"CDC_V2_PIPELINE": "script:../x.py"},
                {"CDC_V2_STALE_AGE_S": "nan"}):
        with pytest.raises(ValueError):
            boot.Settings(dict(env, **bad))


def test_the_error_classification_is_the_layers(boot):
    assert boot.TRANSPORT_RE.pattern == md_errors.TRANSPORT_RE.pattern
    assert boot.CONFLICT_RE.pattern == md_errors.CONFLICT_RE.pattern
    assert boot._QUOTE.pattern == md_errors._QUOTE.pattern
    data = json.load(open(os.path.join(ROOT, "tests", "destination", "data", "misleading_texts.json")))
    texts = [t for t in _strings_of(data)]
    assert len(texts) > 50
    cases = [duckdb.TransactionException, duckdb.CatalogException, duckdb.ConversionException,
             duckdb.IOException, duckdb.ConnectionException, duckdb.InvalidInputException,
             duckdb.ConstraintException]
    for text in texts + ["Conflict on update!", "Connection Error: interrupted by user", "lease expired",
                         "Transaction conflict: attempting to update table", "x 'Conflict on update' y"]:
        for cls in cases:
            e = cls(text)
            assert boot.kind_of(e) == md_errors.kind_of(e), (cls.__name__, text[:60])
    for e in (ConnectionError("x"), TimeoutError("x"), ValueError("Connection Error")):
        assert boot.kind_of(e) == md_errors.kind_of(e)


def _strings_of(obj):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _strings_of(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _strings_of(v)


def test_the_platform_calls_are_the_packages(boot):
    for fid, n in (("8c9a4fbb-37f2-4a1e-9b1d-0d2e6b0f5c11", 1), ("8C9A4FBB37F24A1E9B1D0D2E6B0F5C11", 9001)):
        assert boot.run_state_sql(fid, n) == pmd.run_state_sql(fid, n)
        assert boot.cancel_sql(fid, n) == pmd.cancel_sql(fid, n)
    with pytest.raises(ValueError):
        boot.run_state_sql("1; DROP TABLE x", 1)
    with pytest.raises(ValueError):
        pmd.cancel_sql("x' OR '1", 1)
    for s in ("RUN_STATUS_RUNNING", "running", "CANCELLED", None, "PENDING"):
        assert boot.normalize_state(s) == normalize_state(s)
    for fid, n in (("8c9a4fbb-37f2-4a1e-9b1d-0d2e6b0f5c11", 3), ("a/b..c", 12)):
        assert boot.local_record_path("/x", fid, n) == plocal.record_path("/x", fid, n)


_READS = ("SELECT", "EXPLAIN", "TRANSACTION", "SET", "VARIABLE_SET")


def sql_that_writes(source):
    """``[(line, text)]`` of every string literal of ``source`` that the client's parser reads as SQL and that is
    not a read, a `SET` or a transaction statement (`BEGIN`, `COMMIT`, `ROLLBACK`)."""
    out = []
    for line, text in tm._strings(ast.parse(source)):
        try:
            stmts = duckdb.extract_statements(text)
        except Exception:                     # noqa: BLE001 - not SQL: a message, a name, a path
            continue
        if any(s.type.name not in _READS for s in stmts):
            out.append((line, text))
    return out


def test_the_bootstrap_holds_the_take_over_and_nothing_else_that_commits(boot):
    """D's static tests read `src/cdc_v2` only. Here: every SQL text of `main.py` that writes is the take-over,
    the leader machine's row or the notice of a contender (each compared above with the package), or a copy of a
    statement of `cdc_v2.md.schema` (compared above). Transaction statements are the take-over's and the
    registration's (`BEGIN`, `COMMIT`, `ROLLBACK`)."""
    source = open(BOOTSTRAP, encoding="utf-8").read()
    allowed = {boot.TAKEOVER_LEADER_SQL, boot.TAKEOVER_FENCE_SQL, boot.TAKEOVER_HEARTBEAT_SQL, boot.TRANSITION_SQL,
               boot.NOTICE_SQL} | set(boot.COPIES.values())
    writes = sql_that_writes(source)
    assert [(line, text[:80]) for line, text in writes if text not in allowed] == []
    assert {text for _l, text in writes} == allowed - {v for v in boot.COPIES.values()
                                                       if duckdb.extract_statements(v)[0].type.name in _READS}
    # each text that writes is used, not just carried
    for name in ("TAKEOVER_LEADER_SQL", "TAKEOVER_FENCE_SQL", "TAKEOVER_HEARTBEAT_SQL", "TRANSITION_SQL",
                 "NOTICE_SQL", "REGISTER_SQL", "REGISTER_FENCE_SQL", "HEARTBEAT_SQL"):
        assert source.count(name) >= 2, name
    # the take-over writes the leader row only with 'leading'; nothing in the bootstrap writes 'released'
    assert "state = 'released'" not in source
    # no local wall clock decides anything: time.time() is read for T0_WALL and the crash-point log line only
    uses = [n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Attribute) and n.attr == "time"
            and isinstance(n.value, ast.Name) and n.value.id == "time"]
    assert len(uses) == 2


def test_the_check_finds_a_planted_write():
    planted = ('X = "UPDATE _cdc.leader SET state = \'released\' WHERE epoch = ?"\n'
               'Y = "the outcome of COMMIT is unknown"\nZ = "SELECT 1"\nW = "COMMIT"\n'
               'V = "DELETE FROM _cdc.connections"\n')
    assert [line for line, _t in sql_that_writes(planted)] == [1, 5]


def test_pipeline_requirements_are_the_locked_versions():
    """The Flight's own requirements are the guard's (`duckdb` only, the 1,440 short runs a day); the pipeline's
    are installed by the bootstrap after the take-over, at the versions of `uv.lock`."""
    req = open(os.path.join(ROOT, "flights", "cdc_v2", "requirements.txt")).read().split()
    assert req == ["duckdb==1.5.6"]
    lock = open(os.path.join(ROOT, "uv.lock")).read()
    pins = [line.strip() for line in open(os.path.join(ROOT, "flights", "cdc_v2", "pipeline-requirements.txt"))
            if line.strip() and not line.startswith("#")]
    assert pins
    for pin in pins:
        name, version = re.match(r"^([a-z0-9_-]+)(?:\[[a-z]+\])?==(.+)$", pin).groups()
        assert re.search(r'name = "%s"\nversion = "%s"' % (re.escape(name), re.escape(version)), lock), pin


def test_the_bootstrap_fits_the_flight_source_limit():
    assert os.path.getsize(BOOTSTRAP) < 200 * 1024


def test_the_bootstrap_needs_only_the_standard_library_and_duckdb():
    tree = ast.parse(open(BOOTSTRAP, encoding="utf-8").read())
    mods = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            mods.add((node.module or "").split(".")[0])
    import sys as _sys
    std = set(_sys.stdlib_module_names)
    assert {m for m in mods if m not in std} == {"duckdb"}


def test_the_guard_reads_the_clock_inside_a_statement_on_a_table(boot):
    """Design 4.1 as built: the clock expression is MotherDuck's only in a statement on a MotherDuck table."""
    assert "FROM _cdc.leader" in boot.READ_RECORD_SQL and "uuid_extract_timestamp(uuidv7())" in boot.READ_RECORD_SQL
    assert "now()" not in open(BOOTSTRAP, encoding="utf-8").read()

