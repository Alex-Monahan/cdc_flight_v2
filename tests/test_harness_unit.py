"""T1 harness pieces that need no server (T0): the port allocator, the refusals that protect the shared cluster and
other MotherDuck databases, and the transfers observer against a local DuckDB file."""
import subprocess
import sys
import time

import duckdb
import pytest

from cdc_v2.testing import md as tmd
from cdc_v2.testing import pg as tpg
from cdc_v2.testing.observer import TransfersObserver, apply_transfers, sum_query


def no_home(base):
    return tpg.CACHE / "does-not-exist" / str(base)


def test_port_leases_are_exclusive(tmp_path):
    a = tpg.PortLease.acquire(lock_dir=tmp_path, start=61000, blocks=4, home_for=no_home)
    b = tpg.PortLease.acquire(lock_dir=tmp_path, start=61000, blocks=4, home_for=no_home)
    assert (a.base, b.base) == (61000, 61008)
    a.release()
    c = tpg.PortLease.acquire(lock_dir=tmp_path, start=61000, blocks=4, home_for=no_home)
    assert c.base == 61000
    b.release()
    c.release()


def test_port_lease_held_by_another_process(tmp_path):
    code = ("import sys, time; sys.path.insert(0, %r)\nfrom cdc_v2.testing import pg\n"
            "from pathlib import Path\nl = pg.PortLease.acquire(lock_dir=Path(%r), start=61100, blocks=2, "
            "home_for=lambda b: Path('/nonexistent'))\nprint(l.base, flush=True)\ntime.sleep(30)\n"
            % (str(tpg.REPO / "src"), str(tmp_path)))
    p = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "61100"
        mine = tpg.PortLease.acquire(lock_dir=tmp_path, start=61100, blocks=2, home_for=no_home)
        assert mine.base == 61108
        mine.release()
        with pytest.raises(tpg.HarnessError):
            held = tpg.PortLease.acquire(lock_dir=tmp_path, start=61100, blocks=1, home_for=no_home)
            held.release()
    finally:
        p.kill()
        p.wait()
    # the lock dies with the process
    again = tpg.PortLease.acquire(lock_dir=tmp_path, start=61100, blocks=1, home_for=no_home)
    assert again.base == 61100
    again.release()


@pytest.mark.parametrize("cmd", sorted(tpg.SHARED_FORBIDDEN))
def test_fixture_clusters_refuse_disruptive_commands(cmd, monkeypatch, tmp_path):
    with pytest.raises(tpg.HarnessError, match="refusing"):
        tpg.localpg(cmd, "replica")                                # the default home
    monkeypatch.setenv("CDC_V2_PG_HOME", str(tmp_path / "pg-x"))
    with pytest.raises(tpg.HarnessError, match="refusing"):
        tpg.localpg(cmd, "replica", home=tmp_path / "pg-x")        # the named fixture home


def test_leased_range_misses_every_fixed_range():
    lo = tpg.PRIVATE_BASE
    hi = tpg.PRIVATE_BASE + tpg.PRIVATE_BLOCKS * tpg.PRIVATE_STEP - 1
    assert (lo, hi, tpg.PRIVATE_STEP) == (55840, 56095, 8)
    expected = {(55432, 55439), (55440, 55459), (55460, 55467), (55500, 55509), (55600, 55607), (55620, 55823)}
    assert {(a, b) for a, b, _ in tpg.FIXED_RANGES} == expected
    for a, b, what in tpg.FIXED_RANGES:
        assert b < lo or a > hi, "leased range overlaps %s" % what
    assert tpg.private_home(55848).name == "pg-t1-55848"


def test_fixture_home_needs_a_named_home_or_the_gate_switch(monkeypatch, tmp_path):
    for v in ("CDC_V2_PG_HOME", "CDC_V2_PG_PORT_BASE", "CDC_V2_T1_SHARED"):
        monkeypatch.delenv(v, raising=False)
    with pytest.raises(tpg.HarnessError, match="CDC_V2_PG_HOME"):
        tpg.fixture_home()
    monkeypatch.setenv("CDC_V2_T1_SHARED", "1")
    assert tpg.fixture_home() == (tpg.SHARED_HOME, 55432)
    monkeypatch.delenv("CDC_V2_T1_SHARED")
    monkeypatch.setenv("CDC_V2_PG_HOME", str(tmp_path / "pg-o"))
    monkeypatch.setenv("CDC_V2_PG_PORT_BASE", "55520")
    assert tpg.fixture_home() == (tmp_path / "pg-o", 55520)
    monkeypatch.delenv("CDC_V2_PG_PORT_BASE")           # a home that is not built yet needs its port base ...
    with pytest.raises(tpg.HarnessError, match="CDC_V2_PG_PORT_BASE"):
        tpg.fixture_home()
    (tmp_path / "pg-o").mkdir()
    (tmp_path / "pg-o" / "state.json").write_text("{}")
    assert tpg.fixture_home() == (tmp_path / "pg-o", None)          # ... a built one knows it
    for overlapping in ("55425", "55432", "55439"):                 # never the ports of the default home
        monkeypatch.setenv("CDC_V2_PG_PORT_BASE", overlapping)
        with pytest.raises(tpg.HarnessError, match="default home"):
            tpg.fixture_home()
    monkeypatch.setenv("CDC_V2_PG_PORT_BASE", "55424")
    assert tpg.fixture_home() == (tmp_path / "pg-o", 55424)
    monkeypatch.setenv("CDC_V2_PG_HOME", str(tpg.SHARED_HOME))       # naming the default home is not enough
    with pytest.raises(tpg.HarnessError):
        tpg.fixture_home()


@pytest.mark.parametrize("name", ["cdc_v2_x", "my_db", "cdc_v2_t_", "cdc_v2_t_A", "cdc_v2_t_x;drop", ""])
def test_md_names_outside_the_test_prefix_are_refused(name, monkeypatch):
    monkeypatch.delenv("CDC_V2_MD_PREFIX", raising=False)
    with pytest.raises(tmd.DestinationError):
        tmd.check_name(name)


def test_md_prefix_from_the_environment(monkeypatch):
    monkeypatch.setenv("CDC_V2_MD_PREFIX", "cdc_v2_t_o_")
    n = tmd.new_name()
    assert n.startswith("cdc_v2_t_o_") and tmd.check_name(n) == n
    with pytest.raises(tmd.DestinationError):
        tmd.check_name("cdc_v2_t_s_abc")                           # another package's database
    for bad in ("cdc_v2_x_", "cdc_v2_t_o", "other_"):
        monkeypatch.setenv("CDC_V2_MD_PREFIX", bad)
        with pytest.raises(tmd.DestinationError):
            tmd.prefix()


def test_pg_test_database_names():
    assert tpg.new_db_name().startswith("cdc_v2_t_")


# --------------------------------------------------------------------------------------------- observer


@pytest.fixture
def ledger_db(tmp_path):
    path = str(tmp_path / "dest.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE SCHEMA public")
    for t in ("a", "b"):
        con.execute("CREATE TABLE public.%s (id INTEGER, balance BIGINT)" % t)
        con.execute("INSERT INTO public.%s SELECT range, 100 FROM range(4)" % t)
    return path, con


TRANSFERS = [['UPDATE "public"."a" SET balance = balance + (-10) WHERE id = 0',
              'UPDATE "public"."b" SET balance = balance + (10) WHERE id = 3']] * 5


def test_sum_query():
    assert sum_query(["x", "y"]) == ('SELECT (SELECT coalesce(sum("balance"), 0) FROM "public"."x") + '
                                     '(SELECT coalesce(sum("balance"), 0) FROM "public"."y")')


def test_observer_sees_no_violation_with_whole_transactions(ledger_db):
    path, con = ledger_db
    obs = TransfersObserver(lambda: duckdb.connect(path), ["a", "b"], 800).start()
    time.sleep(0.05)
    apply_transfers(con, TRANSFERS)
    time.sleep(0.05)
    rep = obs.stop()
    assert rep.ok and rep.violations == 0 and rep.observations > 0


def test_observer_catches_a_split_transaction(ledger_db):
    path, con = ledger_db
    obs = TransfersObserver(lambda: duckdb.connect(path), ["a", "b"], 800).start()
    apply_transfers(con, TRANSFERS, split=True, pause=0.05)
    rep = obs.stop()
    assert rep.violations > 0 and not rep.ok
    assert rep.first_violation["sum"] == 790


def test_observer_counts_unavailable_tables(tmp_path):
    path = str(tmp_path / "empty.duckdb")
    duckdb.connect(path).close()
    obs = TransfersObserver(lambda: duckdb.connect(path), ["missing"], 0)
    con = duckdb.connect(path)
    assert obs.observe_once(con) is None
    assert obs.report.unavailable == 1 and obs.report.violations == 0
