"""Tier T0: the small system under test of this package (`sut.py`) on a local DuckDB file, one process at a time.
The tests with two processes need MotherDuck (a local file can be opened for writing by one process only); they are
in `test_md_procs.py`."""
import duckdb

import sut
from cdc_v2 import crashpoints
from cdc_v2.testing import procs


def test_a_killed_run_and_its_successor_apply_every_event_once(tmp_path):
    database, source = str(tmp_path / "d.duckdb"), str(tmp_path / "source.jsonl")
    writer = sut.SourceWriter(source)
    writer.add_all(7)
    fleet = procs.Fleet()
    try:
        env = crashpoints.env_for("stream.commit_sent", hit=2, action="kill", log=str(tmp_path / "hits.jsonl"))
        a = sut.start(database, source, "a", env=env, log_path=str(tmp_path / "a.log"))
        fleet.add(a)
        assert a.wait(20).killed                                # died at its second COMMIT of the stream lane
        assert [h["point"] for h in crashpoints.read_log(str(tmp_path / "hits.jsonl")) if h.get("fire")] \
            == ["stream.commit_sent"]
        writer.add_all(2)
        b = sut.start(database, source, "b", log_path=str(tmp_path / "b.log"), exit_idle=0.2)
        fleet.add(b)
        assert b.wait(20).code == 0, b.log_text()[-800:]
    finally:
        fleet.kill_all(5)
    leader = sut.log_events(b, "leader")
    assert len(leader) == 1 and leader[0]["epoch"] == 2
    raw = duckdb.connect(database)
    try:
        result = sut.check(raw, source)
        assert result["ok"] and result["caught_up"], result
        assert result["lanes"]["stream"] == {"source": 9, "pos": 9, "n": 9, "rows": 9, "doubles": 0}
        by_tag = dict(raw.execute("SELECT tag, count(*) FROM sut.data WHERE lane = 'stream' GROUP BY tag").fetchall())
        assert by_tag == {"a": 3, "b": 6}                       # the killed commit (events 4 to 6) did not land
    finally:
        raw.close()


def test_the_check_sees_a_double_application_and_a_gap():
    raw = duckdb.connect(":memory:")
    try:
        raw.execute("CREATE SCHEMA sut")
        raw.execute("CREATE TABLE sut.position (lane VARCHAR, pos UBIGINT, n BIGINT)")
        raw.execute("CREATE TABLE sut.data (lane VARCHAR, seq UBIGINT, payload VARCHAR, epoch BIGINT, tag VARCHAR)")
        raw.execute("INSERT INTO sut.position VALUES ('stream', 2, 2), ('control', 2, 3), ('backfill.0', 1, 1)")
        raw.execute("INSERT INTO sut.data VALUES ('stream', 1, 'e-stream-1', 1, 'a'), ('stream', 2, 'e-stream-2', 1, 'a'), "
                    "('stream', 2, 'e-stream-2', 2, 'b'), ('control', 1, 'e-control-1', 1, 'a'), "
                    "('control', 3, 'e-control-3', 1, 'a')")
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            writer = sut.SourceWriter(d + "/source.jsonl")
            writer.add_all(3)
            result = sut.check(raw, writer.path)
    finally:
        raw.close()
    assert not result["ok"] and not result["caught_up"]
    text = " ".join(result["problems"])
    assert "stream: 1 events applied more than once" in text
    assert "control: counter 3 differs from position 2" in text and "control: data rows differ" in text
    assert "backfill.0: data rows differ" in text


# ------------------------------------------------------------------------- test support: MotherDuck test databases
def test_a_test_database_is_in_the_ledger_from_before_it_is_created_until_it_is_dropped(tmp_path, monkeypatch):
    """The md tests create and drop their databases through `cdc_v2.testing.md` (shared with the oracle's tests):
    one place, and a ledger entry before the database is created. A database that a killed test left behind is
    then found by its name; nothing lists the account. Here without MotherDuck: the statement is recorded."""
    import pytest
    from cdc_v2.testing import destination as td
    from cdc_v2.testing import md as tmd
    monkeypatch.setattr(tmd, "LEDGER", tmp_path / "ledger")
    monkeypatch.delenv("CDC_V2_MD_PREFIX", raising=False)
    sent = []

    def record(sql):
        assert sql.startswith("DROP") or sql.split()[-1] in td.leftover_databases("cdc_v2_t_"), "ledger first"
        sent.append(sql)
    monkeypatch.setattr(tmd, "_workspace_statement", record)
    assert td.leftover_databases("cdc_v2_t_d_") == []
    name = td.create_database("cdc_v2_t_d_0123abcd")
    assert name == "cdc_v2_t_d_0123abcd"
    td.create_database("cdc_v2_t_x_0123abcd")                   # another package's database is not listed as ours
    assert td.leftover_databases("cdc_v2_t_d_") == [name]
    td.drop_database(name)
    assert td.leftover_databases("cdc_v2_t_d_") == []
    assert sent == ["CREATE DATABASE cdc_v2_t_d_0123abcd", "CREATE DATABASE cdc_v2_t_x_0123abcd",
                    "DROP DATABASE IF EXISTS cdc_v2_t_d_0123abcd"]
    for bad in ("some_other_db", "cdc_v2_t_d_x; DROP DATABASE y", "cdc_v2_prod"):
        with pytest.raises(tmd.DestinationError):               # never a name outside the test prefix
            td.create_database(bad)
        with pytest.raises(tmd.DestinationError):
            td.drop_database(bad)
    for bad in ("", None):
        with pytest.raises(ValueError):                         # and never a name that this code did not choose
            td.create_database(bad)
    assert len(sent) == 3

    def fails(sql):
        raise RuntimeError("no answer")
    monkeypatch.setattr(tmd, "_workspace_statement", fails)
    with pytest.raises(RuntimeError):
        td.create_database("cdc_v2_t_d_4567abcd")
    assert td.leftover_databases("cdc_v2_t_d_") == ["cdc_v2_t_d_4567abcd"]      # it may exist: the entry stays
    with pytest.raises(RuntimeError):
        td.drop_database("cdc_v2_t_d_4567abcd")
    assert td.leftover_databases("cdc_v2_t_d_") == ["cdc_v2_t_d_4567abcd"]      # not dropped: the entry stays
