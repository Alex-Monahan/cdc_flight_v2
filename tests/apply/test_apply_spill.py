"""Package A, tier T0: the planning database's spill area (SPEC 5.4, fix round item 11). One spill directory with a
limit; a plan that needs more fails with ApplySpillFull, leaves no file behind, and the next call works. The
destination is package D's lane on a local DuckDB (apply_lane)."""
from __future__ import annotations

import duckdb
import pytest

from apply_harness import local_db, spill_dir
from cdc_v2.apply import (ApplySettings, ApplySettingsError, ApplySpillFull, Column, LocalChangeSet, TableMeta, Target,
                          apply_group, reduce_duplicates)

META = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "v", "VARCHAR", unchanged_possible=True)))


def _rows(n):
    c = duckdb.connect()
    t = c.execute("SELECT 0 AS txn, i AS seq, 'c' AS op, i AS a0, repeat('x', 200) || i AS a1, false AS u1, "
                  "CAST(NULL AS BIGINT) AS k0 FROM range(%d) t(i)" % n).to_arrow_table()
    c.close()
    return t


def _dest():
    conn = local_db()
    conn.execute("CREATE SCHEMA s")
    conn.execute("CREATE TABLE s.t (id BIGINT, v VARCHAR)")
    return conn


def test_a_spill_directory_is_required():
    with _dest() as db:
        with pytest.raises(ApplySettingsError):
            with db.transaction() as tx:
                apply_group(tx, Target("s", "t"), LocalChangeSet(_rows(3), None, META), settings=ApplySettings())


def test_a_full_spill_area_is_a_typed_error_and_leaves_nothing(tmp_path):
    small = ApplySettings(local_temp_directory=str(tmp_path), local_memory_limit="32MB",
                          local_max_temp_directory_size="1MB", local_threads=1)
    with _dest() as conn:
        with pytest.raises(ApplySpillFull) as e:
            with conn.transaction() as tx:
                apply_group(tx, Target("s", "t"), LocalChangeSet(_rows(300_000), None, META), settings=small)
        assert "x" * 20 not in str(e.value)                       # no value in the message
        leftover = [p for p in tmp_path.rglob("*") if p.is_file()]
        assert leftover == [], leftover
        assert conn.execute("SELECT count(*) FROM s.t").fetchone()[0] == 0
        # the next call (same thread, same settings) works
        with conn.transaction() as tx:
            counts = apply_group(tx, Target("s", "t"), LocalChangeSet(_rows(100), None, META), settings=small)
        assert counts.inserted == 100
        assert conn.execute("SELECT count(*) FROM s.t").fetchone()[0] == 100


# second fix round, item 6: reduce_duplicates plans in the same database, under the same limits

KEYV = TableMeta((Column(0, "id", "VARCHAR", key=True), Column(1, "v", "VARCHAR", unchanged_possible=True)))


def _key_rows(n):
    c = duckdb.connect()
    t = c.execute("SELECT 0 AS txn, i AS seq, 'c' AS op, repeat('k', 200) || i AS a0, 'v' AS a1, false AS u1, "
                  "CAST(NULL AS VARCHAR) AS k0 FROM range(%d) t(i)" % n).to_arrow_table()
    c.close()
    return t


def _dest_v():
    conn = local_db()
    conn.execute("CREATE SCHEMA s")
    conn.execute("CREATE TABLE s.t (id VARCHAR, v VARCHAR)")
    conn.execute("INSERT INTO s.t VALUES (repeat('k', 200) || 1, 'a'), (repeat('k', 200) || 1, 'b'), ('z', 'c')")
    return conn


def test_reduce_duplicates_needs_the_spill_directory():
    with _dest_v() as db:
        with pytest.raises(ApplySettingsError):
            with db.transaction() as tx:
                reduce_duplicates(tx, Target("s", "t"), LocalChangeSet(_key_rows(3), None, KEYV),
                                  settings=ApplySettings())


def test_reduce_duplicates_full_spill_area_is_a_typed_error(tmp_path):
    small = ApplySettings(local_temp_directory=str(tmp_path), local_memory_limit="32MB",
                          local_max_temp_directory_size="1MB", local_threads=1)
    with _dest_v() as conn:
        with pytest.raises(ApplySpillFull):
            with conn.transaction() as tx:
                reduce_duplicates(tx, Target("s", "t"), LocalChangeSet(_key_rows(400_000), None, KEYV),
                                  settings=small)
        assert [p for p in tmp_path.rglob("*") if p.is_file()] == []
        with conn.transaction() as tx:
            assert reduce_duplicates(tx, Target("s", "t"), LocalChangeSet(_key_rows(5), None, KEYV),
                                     settings=small) == 1
        assert conn.execute("SELECT count(*) FROM s.t").fetchone()[0] == 2


def test_planning_settings_stay_with_the_cursor():
    """Item 10, and item 8 of the fix round after D's merge (finding 21 of D's review): the planning database is
    shared by the calls of a thread; its cursors use SET SESSION, so a setting of one call is not a setting of the
    database, and a cursor of another call keeps the database's own values."""
    from cdc_v2.apply.backend import _planning_db
    small = ApplySettings(local_temp_directory=spill_dir())
    db_, _ = _planning_db(small.local_threads, small.local_memory_limit, small.local_temp_directory,
                          small.local_max_temp_directory_size)
    # the database's own time zone is not UTC, so that a global setting by apply would show (also on a machine
    # whose zone is UTC)
    before = db_.cursor().execute("SELECT current_setting('TimeZone')").fetchone()[0]
    db_.cursor().execute("SET GLOBAL TimeZone = 'America/Denver'")
    try:
        with _dest() as db:
            with db.transaction() as tx:
                apply_group(tx, Target("s", "t"), LocalChangeSet(_rows(3), None, META), settings=small)
            with db.transaction() as tx:
                reduce_duplicates(tx, Target("s", "t"), LocalChangeSet(_rows(3), None, META), settings=small)
        other = db_.cursor()
        assert other.execute("SELECT current_setting('arrow_lossless_conversion')").fetchone()[0] is False
        assert other.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "America/Denver"
    finally:
        db_.cursor().execute("SET GLOBAL TimeZone = '%s'" % before)
