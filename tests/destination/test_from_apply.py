"""Tier T0 (local DuckDB): the seven points that the review of package A (apply) added to contract C4.

Package A calls the lane transaction with `execute`, `fetchall`, `fetch_arrow` and `register`. The points:

1. `execute` returns the number of rows an INSERT, UPDATE or DELETE changed, else None.
2. `fetchall` returns a list.
3. `fetch_arrow` returns a `pyarrow.Table` that was read completely, never the client's lazy reader.
4. `register` takes a table or record batches under a checked name; the name is gone when the block ends.
5. Every connection of the layer has `TimeZone = 'UTC'` and `arrow_lossless_conversion = true`.
6. Nothing of a destination error text travels on: the text can hold a value of a row.
7. A scratch schema `_cdc_apply_<random>` lives inside one lane transaction and is dropped table by table.

`test_md_again.py` runs the tests that MotherDuck decides against MotherDuck as well, and writes what they
measured there to `evidence/p3/destination/from_apply.json`.
"""
import logging
import secrets
import traceback

import pyarrow as pa
import pytest

from cdc_v2 import alerts, md
from cdc_v2.deadline import Deadline
from cdc_v2.md import connection as mdc
from cdc_v2.md import schema

BUMP = "UPDATE t SET v = v + 1 WHERE k = 1"


@pytest.fixture
def measured():
    """``measured(key, value)``: what a test measured. Here it is dropped; the run against MotherDuck writes it
    down (`test_md_again.py`)."""
    return lambda key, value: None


def counter(dest):
    """A table with one counter row; returns a connection to look at it."""
    c = dest.connect("look")
    c.execute("CREATE TABLE t (k INTEGER, v BIGINT)")
    c.execute("INSERT INTO t VALUES (1, 0)")
    return c


def value(c):
    return c.fetchone("SELECT v FROM t WHERE k = 1")[0]


def gone(lane, name="incoming"):
    """True when a statement of the lane's connection can no longer read ``name``."""
    with pytest.raises(md.StatementError) as info:
        with lane.transaction() as tx:
            tx.fetchall("SELECT * FROM %s" % name)
    return info.value.error_class == "CatalogException"


def scratch_schemas(conn):
    return [r[0] for r in conn.fetchall(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE catalog_name = current_database() AND starts_with(schema_name, '_cdc_apply') ORDER BY 1")]


# ------------------------------------------------------------------------------------------- 1: the row count
#: statement, parameters, what `execute` returns
COUNTS = [
    ("CREATE TABLE c1 (k INTEGER, v BIGINT)", None, None),
    ("INSERT INTO c1 VALUES (1, 10), (2, 20), (3, 30)", None, 3),
    ("UPDATE c1 SET v = v + 1 WHERE k >= ?", [2], 2),
    ("UPDATE c1 SET v = 0 WHERE k = 99", None, 0),              # no row: 0, not None
    ("DELETE FROM c1 WHERE k = ?", [1], 1),
    ("INSERT INTO c1 SELECT k + 10, v FROM c1", None, 2),
    ("CREATE TABLE c2 AS SELECT * FROM c1", None, None),        # the client reports a count here; C4 does not
    ("ALTER TABLE c2 RENAME COLUMN v TO w", None, None),
    ("SELECT count(*) FROM c1", None, None),
    ('SELECT 5 AS "Count"', None, None),                        # the name of a result column decides nothing
    ("INSERT INTO c1 VALUES (7, 70) RETURNING k", None, None),  # its result is its rows: read them with fetchall
    ("CREATE SCHEMA c3", None, None),
    ("DROP SCHEMA c3", None, None),
    ("DROP TABLE c2", None, None),
]
AFTER_COUNTS = [(2, 21), (3, 31), (7, 70), (12, 21), (13, 31)]


def test_execute_returns_a_count_for_insert_update_and_delete_only(led):
    lane = led.lane("stream", led.epoch)
    look = led.connect("look")
    with lane.transaction() as tx:
        for sql, params, expected in COUNTS:
            got = tx.execute(sql, params)
            assert got == expected and type(got) is type(expected), sql
        assert tx.fetchall("SELECT k, v FROM c1 ORDER BY k") == AFTER_COUNTS
    assert look.fetchall("SELECT k, v FROM c1 ORDER BY k") == AFTER_COUNTS
    look.execute("DROP TABLE c1")
    for sql, params, expected in COUNTS:                        # the same rule outside a lane transaction
        got = look.execute(sql, params)
        assert got == expected and type(got) is type(expected), sql
    assert look.fetchall("SELECT k, v FROM c1 ORDER BY k") == AFTER_COUNTS


def test_merge_reports_no_count_and_truncate_counts_as_a_delete(led, measured):
    """Contract C4 names INSERT, UPDATE and DELETE. MERGE is none of them: `execute` returns None for it (a contract
    note). TRUNCATE is a DELETE to the client's parser and to the destination, so its rows are counted."""
    lane = led.lane("stream", led.epoch)
    merge = ("MERGE INTO m USING (SELECT 2 AS k, 5 AS v UNION ALL SELECT 9, 90) s ON m.k = s.k "
             "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT VALUES (s.k, s.v)")
    assert md.statement_kind(merge) == "MERGE_INTO" and md.statement_kind("TRUNCATE m") == "DELETE"
    with lane.transaction() as tx:
        tx.execute("CREATE TABLE m (k INTEGER, v BIGINT)")
        assert tx.execute("INSERT INTO m VALUES (1, 10), (2, 20)") == 2
        assert tx.execute(merge) is None
        rows = tx.fetchall("SELECT k, v FROM m ORDER BY k")
        truncated = tx.execute("TRUNCATE m")
        left = tx.fetchone("SELECT count(*) FROM m")[0]
    measured("merge_and_truncate", {"merge_returns": None, "rows_after_merge": [list(r) for r in rows],
                                    "truncate_returns": truncated, "rows_after_truncate": left})
    assert rows == [(1, 10), (2, 5), (9, 90)]                   # the MERGE did run
    assert truncated == 3 and left == 0


@pytest.mark.parametrize("sql, kind", [
    ("INSERT INTO t VALUES (1)", "INSERT"), ("insert into t select 1", "INSERT"),
    ("INSERT INTO t VALUES (1) RETURNING k", "INSERT"), ("INSERT OR REPLACE INTO t VALUES (1)", "INSERT"),
    ("UPDATE t SET v = 1", "UPDATE"), ("DELETE FROM t", "DELETE"), ("TRUNCATE t", "DELETE"),
    ("WITH x AS (SELECT 1 AS k) INSERT INTO t SELECT k FROM x", "INSERT"),
    ("SELECT 1", "SELECT"), ("WITH x AS (SELECT 1) SELECT * FROM x", "SELECT"), ("FROM t", "SELECT"),
    ("CREATE TABLE t (k INT)", "CREATE"), ("CREATE TABLE u AS SELECT 1", "CREATE"), ("CREATE SCHEMA s", "CREATE"),
    ("DROP TABLE t", "DROP"), ("DROP SCHEMA s", "DROP"), ("ALTER TABLE t RENAME COLUMN a TO b", "ALTER"),
    ("MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN DELETE", "MERGE_INTO"),
    ("EXPLAIN SELECT 1", "EXPLAIN"), ("SELEC 1", "UNPARSED"), ("", "UNPARSED"), ("SELECT 1; SELECT 2", "UNPARSED"),
    ("COMMIT", "UNPARSED"), (None, "UNPARSED"), (5, "UNPARSED"),
])
def test_the_kind_of_a_statement_comes_from_the_parser_of_the_client(sql, kind):
    """The "statement shape" of point 6: a name from a fixed list, found on this side, nothing is sent. Text that
    is not exactly one statement of a caller has no kind."""
    assert md.statement_kind(sql) == kind
    assert set(mdc.COUNTED) == {"INSERT", "UPDATE", "DELETE"}


# ------------------------------------------------------------------------------- 2 and 3: fetchall, fetch_arrow
def test_fetch_arrow_gives_a_table_that_the_same_transaction_can_register(led):
    """In DuckDB 1.5.6 `.arrow()` gives a lazy reader, and a reader that a connection produced cannot be read by a
    statement of the same connection (the client waits for itself). So `fetch_arrow` reads the result completely,
    and what it returns can be registered on the connection it came from."""
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("CREATE TABLE t (k INTEGER, v BIGINT)")
        assert tx.execute("INSERT INTO t SELECT i, i * 10 FROM range(5) r(i)") == 5
        got = tx.fetch_arrow("SELECT k, v FROM t WHERE k >= ? ORDER BY k", [1], Deadline.after(20.0))
        assert isinstance(got, pa.Table) and not isinstance(got, pa.RecordBatchReader)
        assert got.to_pylist() == [{"k": i, "v": i * 10} for i in range(1, 5)]
        tx.register("again", got, deadline=Deadline.after(20.0))
        rows = tx.fetchall("SELECT sum(v), count(*) FROM again WHERE k > ?", [1], Deadline.after(20.0))
        assert isinstance(rows, list) and rows == [(90, 3)]
        assert tx.execute("INSERT INTO t SELECT k + 100, v FROM again", None, Deadline.after(20.0)) == 4
        empty = tx.fetch_arrow("SELECT k, v FROM t WHERE k < 0")
        assert isinstance(empty, pa.Table) and empty.num_rows == 0 and empty.column_names == ["k", "v"]
        assert tx.fetchall("SELECT k FROM t WHERE k < 0") == []
        with pytest.raises(md.LaneMisuse, match="reader"):      # a reader is refused, whoever made it
            tx.register("lazy", got.to_reader())
    look = led.connect("look")
    assert look.fetchone("SELECT count(*), sum(v) FROM t") == (9, 200)


# ----------------------------------------------------------------------------------------------- 4: register
def test_register_takes_a_table_a_record_batch_or_a_list_of_batches(led):
    lane = led.lane("stream", led.epoch)
    table = pa.table({"k": [1, 2, 3], "s": ["a", "b", "c"]})
    one, two = table.to_batches(max_chunksize=2)
    read = "SELECT k, s FROM incoming ORDER BY k"
    with lane.transaction() as tx:
        tx.register("incoming", table)
        assert tx.fetchall(read) == [(1, "a"), (2, "b"), (3, "c")]
        tx.register("incoming", one)                            # the same name again: the new data
        assert tx.fetchall(read) == [(1, "a"), (2, "b")]
        tx.register("incoming", [one, two])
        assert tx.fetchall(read) == [(1, "a"), (2, "b"), (3, "c")]
        tx.register("incoming", (two,))
        assert tx.fetchall(read) == [(3, "c")]
        tx.register("nothing", table.slice(0, 0))               # no rows is data too
        assert tx.fetchall("SELECT count(*) FROM nothing") == [(0,)]
        refused = [table.to_reader(), None, [], (), {"k": [1]}, [one, "x"], [table], "incoming", 5,
                   [one, pa.record_batch({"z": [1]})]]
        for bad in refused:
            with pytest.raises(md.LaneMisuse):
                tx.register("refused", bad)
        assert tx.fetchall(read) == [(3, "c")]                  # a refusal changes nothing and fails nothing
        tx.execute("CREATE TABLE kept AS SELECT k FROM incoming")
    assert led.connect("look").fetchall("SELECT k FROM kept") == [(3,)]


BAD_NAMES = ["Incoming", "1abc", "a-b", "a b", "a;drop", "x" * 64, "", None, 5, 'a"b', "main.t", "abc\n", " abc",
             "äbc", b"abc", "select*"]
GOOD_NAMES = ["_x", "a" * 63, "cdc_v2_apply_keys", "a1_", "x"]


def test_a_registered_name_is_checked(led):
    """`^[a-z_][a-z0-9_]{0,62}$`, and nothing else: the name goes into the client's catalog as it is."""
    lane = led.lane("stream", led.epoch)
    data = pa.table({"k": [1]})
    with lane.transaction() as tx:
        for name in BAD_NAMES:
            with pytest.raises(md.LaneMisuse) as info:
                tx.register(name, data)
            if isinstance(name, str) and name.strip():
                assert name not in str(info.value)              # the refusal does not repeat what it was given
        for name in GOOD_NAMES:
            tx.register(name, data)
            assert tx.fetchall('SELECT k FROM "%s"' % name) == [(1,)]
        tx.execute("CREATE TABLE kept AS SELECT k FROM x")
    assert led.connect("look").fetchone("SELECT count(*) FROM kept") == (1,)
    for name in GOOD_NAMES:
        assert gone(lane, '"%s"' % name)


def test_a_registered_name_is_gone_when_the_block_ends_however_it_ends(led):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    data = pa.table({"k": [1, 2]})
    with lane.transaction() as tx:                              # a block that commits
        tx.register("incoming", data)
        tx.register("second", data)
        assert tx.fetchone("SELECT count(*) FROM incoming, second") == (4,)
    assert gone(lane) and gone(lane, "second")
    with pytest.raises(KeyError):                               # an exception of the caller
        with lane.transaction() as tx:
            tx.register("incoming", data)
            raise KeyError("boom")
    assert gone(lane)
    with pytest.raises(md.StatementError):                      # a statement that the destination refuses
        with lane.transaction() as tx:
            tx.register("incoming", data)
            tx.execute("SELECT * FROM no_such_table")
    assert gone(lane)
    with pytest.raises(md.Conflict):                            # the run-again rule
        with lane.transaction() as tx:
            tx.register("incoming", data)
            tx.execute_one("UPDATE t SET v = 1 WHERE k = 99")
    assert gone(lane)
    with pytest.raises(md.DeadlineExceeded):                    # a deadline that passed before a statement started
        with lane.transaction() as tx:
            tx.register("incoming", data)
            tx.execute(BUMP, deadline=Deadline.after(0.0))
    assert gone(lane)
    with pytest.raises(ValueError):                             # the caller gives the transaction up
        with lane.transaction() as tx:
            tx.register("incoming", data)
            raise tx.fail(ValueError("not this one"))
    assert gone(lane)
    assert led.take_over() == led.epoch + 1
    with pytest.raises(md.FenceLost):                           # another run leads
        with lane.transaction() as tx:
            tx.register("incoming", data)
            tx.execute(BUMP)
    assert gone(lane)
    assert lane.conn.generation == 1 and value(c) == 0          # one connection all the way; nothing was committed


def test_a_registered_name_is_gone_after_a_deadline_gave_the_connection_up(led, faults):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    data = pa.table({"k": [1, 2]})
    with lane.transaction() as tx:                              # registered; skip counts from here
        tx.execute("SELECT 1")
    faults.block_at("md.execute", skip=1, hits=1)               # BEGIN passes, the statement blocks
    with pytest.raises(md.DeadlineExceeded):
        with lane.transaction() as tx:
            tx.register("incoming", data)
            tx.execute(BUMP, deadline=Deadline.after(0.15))
    assert not lane.conn.usable
    faults.clear()
    assert gone(lane) and lane.conn.generation == 2             # a new connection: it never had the name
    with lane.transaction() as tx:                              # and the name can be registered again
        tx.register("incoming", data)
        assert tx.execute("UPDATE t SET v = v + (SELECT count(*) FROM incoming) WHERE k = 1") == 1
    assert value(c) == 2 and gone(lane)


# ---------------------------------------------------------------------------- 5: time zone and lossless Arrow
TYPED = "(u UUID, j JSON, tz TIMETZ, ts TIMESTAMPTZ, h HUGEINT)"
TYPED_ROW = ("'6f1c2a44-0b7e-4c1d-9a55-1f2e3d4c5b6a', '{\"a\": [1, 2]}', '12:34:56+02:00', "
             "'2026-01-02 03:04:05.123456+00', 170141183460469231731687303715884105727")
AS_TEXT = ("SELECT CAST(u AS VARCHAR), CAST(j AS VARCHAR), CAST(tz AS VARCHAR), epoch_us(ts), CAST(h AS VARCHAR) "
           "FROM %s ORDER BY 1 NULLS LAST")
TYPED_TEXT = ("6f1c2a44-0b7e-4c1d-9a55-1f2e3d4c5b6a", '{"a": [1, 2]}', "12:34:56+02", 1767323045123456,
              "170141183460469231731687303715884105727")


def test_arrow_data_keeps_uuid_json_and_time_zones(led, measured):
    """Without `arrow_lossless_conversion` a UUID and a JSON value arrive in Arrow as text and a TIMETZ as a time
    without its offset; written back through a registered table they would be other values. With it, and with
    the session time zone UTC, a table read with `fetch_arrow` goes back in unchanged."""
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        settings = tx.fetchone("SELECT current_setting('TimeZone'), current_setting('arrow_lossless_conversion')")
        assert settings == ("UTC", True)
        tx.execute("CREATE TABLE typed " + TYPED)
        tx.execute("CREATE TABLE back " + TYPED)
        assert tx.execute("INSERT INTO typed VALUES (%s), (NULL, NULL, NULL, NULL, NULL)" % TYPED_ROW) == 2
        got = tx.fetch_arrow("SELECT * FROM typed")
        types = [str(f.type) for f in got.schema]
        tx.register("incoming", got)
        seen_as = tx.fetchone("SELECT typeof(COLUMNS(*)) FROM incoming LIMIT 1")
        inserted = tx.execute("INSERT INTO back SELECT * FROM incoming")
        back, typed = tx.fetchall(AS_TEXT % "back"), tx.fetchall(AS_TEXT % "typed")
        # Arrow data that a caller built itself: a TIMESTAMPTZ column goes in without the `pytz` module
        stamps = pa.table({"ts": pa.array([1767323045123456, None], pa.timestamp("us", tz="UTC"))})
        tx.register("stamps", stamps)
        tx.execute("CREATE TABLE seen (ts TIMESTAMPTZ)")
        assert tx.execute("INSERT INTO seen SELECT ts FROM stamps") == 2
        assert tx.fetchall("SELECT epoch_us(ts) FROM seen ORDER BY 1 NULLS LAST") == [(1767323045123456,), (None,)]
    raw = led.raw()                                             # a connection without the setting, to compare
    try:
        plain = [str(f.type) for f in raw.execute("SELECT * FROM typed").to_arrow_table().schema]
    finally:
        raw.close()
    measured("arrow_types", {"with_the_setting": types, "without_the_setting": plain, "registered_as": list(seen_as)})
    assert types[0] == "extension<arrow.uuid>" and types[1] == "extension<arrow.json>"
    assert "time_tz" in types[2] and types[3] == "timestamp[us, tz=UTC]" and "hugeint" in types[4]
    assert seen_as == ("UUID", "JSON", "TIME WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", "HUGEINT")
    assert inserted == 2 and back == typed == [TYPED_TEXT, (None, None, None, None, None)]
    assert plain[:3] == ["string", "string", "time64[us]"]      # what the setting is for


def test_every_connection_of_the_layer_has_the_two_settings(led, faults):
    """Also a connection outside a lane, and the one that replaces a connection that was given up."""
    read = "SELECT current_setting('TimeZone'), current_setting('arrow_lossless_conversion')"
    c = counter(led)
    assert c.fetchone(read) == ("UTC", True)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        assert tx.fetchone(read) == ("UTC", True)
    faults.raise_at("md.fence", "ConnectionError", message="injected: connection reset", hits=1)
    with pytest.raises(md.ConnectionLost):
        with lane.transaction() as tx:
            tx.execute(BUMP)
    with lane.transaction() as tx:
        assert tx.fetchone(read) == ("UTC", True)
    assert lane.conn.generation == 2


def test_the_two_settings_belong_to_the_connections_of_the_layer_only(led, measured):
    """A plain `SET arrow_lossless_conversion` changes the value for every connection this process has to the
    database (measured, DuckDB 1.5.6), so any of them could change it back. The layer sets both for its own
    session: another connection keeps its values and cannot change those of the layer."""
    read = "SELECT current_setting('TimeZone'), current_setting('arrow_lossless_conversion')"
    c = led.connect("look")
    c.execute("CREATE TABLE u (u UUID)")
    c.execute("INSERT INTO u VALUES ('6f1c2a44-0b7e-4c1d-9a55-1f2e3d4c5b6a')")
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        assert tx.fetchone(read) == ("UTC", True)
    raw = led.raw()                                             # a connection that is not the layer's
    try:
        other = raw.execute(read).fetchone()[1]
        raw.execute("SET GLOBAL arrow_lossless_conversion = false")
        raw.execute("SET GLOBAL TimeZone = 'Asia/Tokyo'")
        try:
            with lane.transaction() as tx:
                mine = tx.fetchone(read)
                kind = str(tx.fetch_arrow("SELECT u FROM u").schema[0].type)
            seen = c.fetchone(read)
            later = led.connect("later").fetchone(read)         # a connection of the layer that opens after it
        finally:
            raw.execute("RESET GLOBAL TimeZone")
            raw.execute("RESET GLOBAL arrow_lossless_conversion")
    finally:
        raw.close()
    measured("settings_scope", {"other_connection_has_lossless": other, "lane_after_a_global_change": list(mine),
                                "arrow_type_of_uuid": kind, "new_connection_after_it": list(later)})
    assert other is False                                       # the layer changed nothing for others
    assert mine == seen == later == ("UTC", True) and kind == "extension<arrow.uuid>"


# ------------------------------------------------------------------------- 6: no value of a row in an error
MARKER = "zq-marker-7731"
NUMBER = 3999888777
NEEDLES = (MARKER, str(NUMBER))
#: statement, parameters, class of the client's error, kind of statement. The client's text for each of them
#: names the planted value: with quotation marks, without them (the number), or as part of the SQL line.
PLANTED = [
    ("INSERT INTO t SELECT 50, CAST(? AS BIGINT)", [MARKER], "ConversionException", "INSERT"),
    ("INSERT INTO t SELECT 51, CAST(? AS INTEGER)", [NUMBER], "ConversionException", "INSERT"),
    ("UPDATE t SET v = CAST(? AS BIGINT) WHERE k = 1", [MARKER], "ConversionException", "UPDATE"),
    ("SELECT CAST(? AS DATE)", [MARKER], "ConversionException", "SELECT"),
    ("INSERT INTO names VALUES (?)", [MARKER], "ConstraintException", "INSERT"),
    ("SELEC '%s'" % MARKER, None, "ParserException", "UNPARSED"),
    ('SELECT * FROM "%s"' % MARKER, None, "CatalogException", "SELECT"),
]


def found_in(texts):
    return sorted({n for n in NEEDLES for t in texts if n in t})


def texts_of(err):
    return [str(err), repr(err), repr(err.args), repr(vars(err)),
            "".join(traceback.format_exception(type(err), err, err.__traceback__))]


def cdc_tables(conn):
    """The names of the tables of `_cdc` and every row of them as text."""
    names = [r[0] for r in conn.fetchall(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = '_cdc' ORDER BY 1")]
    rows = []
    for name in names:
        rows.extend(repr(r) for r in conn.fetchall('SELECT CAST(COLUMNS(*) AS VARCHAR) FROM _cdc."%s"' % name))
    return names, "\n".join(rows)


def test_a_value_in_a_destination_error_never_travels_on(led, caplog, measured):
    """SPEC 8.3. DuckDB names the offending value in conversion and constraint errors, and the parser repeats the
    line of SQL. The layer raises `StatementError` with the class of the client's error and the kind of the
    statement, and nothing else: not in the exception, not in a log line, not in the alert a caller writes."""
    caplog.set_level(logging.DEBUG, logger="cdc_v2")
    c = counter(led)
    c.execute("CREATE TABLE names (name VARCHAR PRIMARY KEY)")
    c.execute("INSERT INTO names VALUES (?)", [MARKER])
    lane = led.lane("stream", led.epoch)
    raw = led.raw()
    premise = []
    try:
        for sql, params, error_class, kind in PLANTED:
            try:                                                # the premise: the client's own text names the value
                raw.execute(sql, params) if params else raw.execute(sql)
                name, text = None, ""
            except Exception as e:                              # noqa: BLE001
                name, text = type(e).__name__, str(e)
            premise.append([kind, name, bool(found_in([text]))])
            assert name == error_class and found_in([text]), (sql, name)
            reads = kind == "SELECT"
            with pytest.raises(md.StatementError) as in_lane:
                with lane.transaction() as tx:
                    tx.execute(BUMP)
                    tx.fetch_arrow(sql, params) if reads else tx.execute(sql, params)
            with pytest.raises(md.StatementError) as outside:
                c.fetchall(sql, params) if reads else c.execute(sql, params)
            for err, where in ((in_lane.value, "lane stream"), (outside.value, "connection look")):
                assert (err.where, err.error_class, err.statement) == (where, error_class, kind)
                assert str(err) == "%s: the destination refused a statement (%s in %s)" % (where, error_class, kind)
                assert err.__cause__ is None and err.__context__ is None        # the client's error is not attached
                assert found_in(texts_of(err)) == []
            assert lane.conn.generation == 1 and value(c) == 0                  # rolled back; no connection given up
            err = in_lane.value                                 # what a caller does with it: an alert
            alerts.emit_now(lane, "apply_failed", "warning", {"error_class": err.error_class,
                                                              "statement": err.statement, "error": str(err)})
    finally:
        raw.close()
    measured("planted_values", {"what": "kind of statement, class of the client's error, its text names the value",
                                "cases": premise})
    # the log capture: the lines of the layer and of the alerts
    assert len([r for r in caplog.records if r.getMessage() == "alert"]) == len(PLANTED)
    lines = [caplog.text]
    for r in caplog.records:
        lines.append("%s %r %s" % (r.getMessage(), getattr(r, "cdc", None), r.exc_text or ""))
        if r.exc_info:
            lines.append("".join(traceback.format_exception(*r.exc_info)))
    assert found_in(lines) == []
    # the alert records and every other table of _cdc
    names, text = cdc_tables(c)
    assert {"alerts", "transitions", "connections", "leader"} <= set(names)
    assert text.count("apply_failed") >= len(PLANTED) and "ConstraintException in INSERT" in text
    assert found_in([text]) == []
    assert c.fetchone("SELECT count(*) FROM names WHERE name = ?", [MARKER]) == (1,)    # the value is where it belongs


def test_a_value_in_local_data_that_cannot_be_registered_does_not_travel_on(led, monkeypatch):
    """`register` hands data to the client. What the client says when it refuses the data is not passed on."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)

    def refuse(self, name, data):
        def run():
            raise ValueError("cannot read the value %s" % MARKER)
        return run
    monkeypatch.setattr(mdc.Session, "view", refuse)
    with pytest.raises(md.StatementError) as info:
        with lane.transaction() as tx:
            tx.execute(BUMP)
            tx.register("incoming", pa.table({"k": [1]}))
    err = info.value
    assert (err.error_class, err.statement) == ("ValueError", "REGISTER")
    assert err.__cause__ is None and err.__context__ is None and found_in(texts_of(err)) == []
    monkeypatch.undo()
    assert value(c) == 0                                        # the transaction was rolled back


# ------------------------------------------------------------------------------------- 7: scratch schemas
def test_a_scratch_schema_of_apply_lives_inside_one_transaction(led):
    """What package A does on MotherDuck: a schema `_cdc_apply_<random>` with tables made by CREATE TABLE AS, read
    and written in the same lane transaction, then dropped table by table and at last the schema."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    name = "_cdc_apply_" + secrets.token_hex(6)
    with lane.transaction() as tx:
        assert tx.execute("CREATE SCHEMA %s" % name) is None
        assert tx.execute("CREATE TABLE %s.a AS SELECT ? AS k" % name, [1]) is None
        # a table with the name of a leader table is an ordinary table there
        assert tx.execute("CREATE TABLE %s.leader AS SELECT k + 1 AS k FROM %s.a" % (name, name)) is None
        assert tx.execute("INSERT INTO %s.leader VALUES (5)" % name) == 1
        assert tx.execute("DELETE FROM %s.leader WHERE k = 5" % name) == 1
        assert tx.fetch_arrow("SELECT k FROM %s.leader" % name).to_pylist() == [{"k": 2}]
        assert tx.execute("UPDATE t SET v = v + (SELECT sum(k) FROM %s.leader) WHERE k = 1" % name) == 1
        tx.execute("DROP TABLE %s.leader" % name)
        tx.execute("DROP TABLE %s.a" % name)
        tx.execute("DROP SCHEMA %s" % name)
    assert value(c) == 2 and scratch_schemas(c) == []
    with pytest.raises(KeyError):                               # a block that fails takes its scratch schema along
        with lane.transaction() as tx:
            tx.execute("CREATE SCHEMA %s" % name)
            tx.execute("CREATE TABLE %s.a AS SELECT 1 AS k" % name)
            tx.execute(BUMP)
            raise KeyError("boom")
    assert value(c) == 2 and scratch_schemas(c) == [] and lane.conn.generation == 1


def test_drop_schema_cascade_of_tables_made_in_the_same_transaction_fails_at_commit(led, measured):
    """Measured by package A on MotherDuck; local DuckDB 1.5.6 does the same. The DROP is accepted and COMMIT is
    refused. To the lane that is an error after COMMIT was sent: `CommitUnknown`, and the connection is given up.
    Nothing was committed. So a caller drops the tables one by one and then the schema (the test above)."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    name = "_cdc_apply_" + secrets.token_hex(6)
    with pytest.raises(md.DestinationError) as info:
        with lane.transaction() as tx:
            tx.execute(BUMP)
            tx.execute("CREATE SCHEMA %s" % name)
            tx.execute("CREATE TABLE %s.a AS SELECT 1 AS k" % name)
            tx.execute("CREATE TABLE %s.b AS SELECT 2 AS k" % name)
            tx.execute("DROP SCHEMA %s CASCADE" % name)         # accepted
    measured("drop_schema_cascade", {"error": type(info.value).__name__, "message": str(info.value).replace(
        "lane stream: ", ""), "committed_value": value(c), "schemas_left": len(scratch_schemas(c))})
    assert type(info.value) is md.CommitUnknown
    assert "TransactionException" in str(info.value)
    assert value(c) == 0 and scratch_schemas(c) == []           # nothing of the transaction is there
    assert not lane.conn.usable
    with lane.transaction() as tx:                              # the lane goes on with a new connection
        tx.execute(BUMP)
    assert value(c) == 1 and lane.conn.generation == 2


@pytest.mark.parametrize("sql", [
    "CREATE SCHEMA _cdc_apply_ab12", "DROP SCHEMA _cdc_apply_ab12", 'DROP SCHEMA "_cdc_apply_ab12"',
    "DROP SCHEMA IF EXISTS _cdc_apply_ab12", "DROP SCHEMA _cdc_apply_ab12 CASCADE",
    "DROP SCHEMA mydb._cdc_apply_ab12", "DROP TABLE _cdc_apply_ab12.leader", "DELETE FROM _cdc_apply_ab12.leader",
    "INSERT INTO _cdc_apply_ab12.heartbeat VALUES (1)", "TRUNCATE _cdc_apply_ab12.fence",
    "CREATE OR REPLACE TABLE _cdc_apply_ab12.fence AS SELECT 1", "ALTER TABLE _cdc_apply_ab12.leader ADD COLUMN x INT",
    'INSERT INTO mydb."_cdc_apply_ab12"."leader" VALUES (1)',
])
def test_a_statement_on_a_scratch_schema_is_not_taken_for_one_on_the_leader_tables(sql):
    assert mdc.check_sql(sql) is False


@pytest.mark.parametrize("sql", [
    "DROP SCHEMA _cdc", "DROP SCHEMA _cdc CASCADE", 'DROP SCHEMA "_cdc"', "DROP SCHEMA IF EXISTS _cdc CASCADE",
    "DROP SCHEMA mydb._cdc", "DELETE FROM _cdc.leader", "INSERT INTO _cdc.heartbeat VALUES (1)",
    "TRUNCATE _cdc.fence", "DROP TABLE _cdc.leader", "ALTER TABLE _cdc.fence ADD COLUMN x INT",
])
def test_the_leader_tables_stay_protected_next_to_scratch_schemas(sql):
    with pytest.raises(md.LaneMisuse):
        mdc.check_sql(sql)


def test_the_schema_check_and_the_migrations_leave_a_scratch_schema_alone(dest):
    """A scratch schema that a transaction of apply would have open (here it is committed, which never happens:
    the worst case). The check of the leader tables, their initialisation, the take-over and the migrations say
    nothing about it and do not touch it, also when its tables have the names of tables of `_cdc`."""
    name = "_cdc_apply_0123456789ab"
    raw = dest.raw()
    try:
        raw.execute("CREATE SCHEMA %s" % name)
        for table in ("leader", "fence", "schema_version", "alerts"):
            raw.execute("CREATE TABLE %s.%s (x INTEGER)" % (name, table))
            raw.execute("INSERT INTO %s.%s VALUES (1), (2)" % (name, table))
    finally:
        raw.close()
    c = dest.connect("look")
    schema.check_leader_tables(c)                               # raises when it has something to report
    assert md.ensure_leader_tables(c) == 0
    epoch = dest.lead(migrate=False)
    control = dest.lane("control", epoch)
    applied = md.migrate(control)
    assert applied and {p for p, _, _ in applied} >= {"state", "alerts"}
    assert md.migrate(control) == []                            # and a second look finds nothing to do
    schema.check_leader_tables(c)
    assert scratch_schemas(c) == [name]
    for table in ("leader", "fence", "schema_version", "alerts"):
        assert c.fetchall("SELECT x FROM %s.%s ORDER BY x" % (name, table)) == [(1,), (2,)]
    with control.transaction() as tx:                           # and the tables of _cdc are the real ones
        assert tx.fetchone("SELECT count(*) FROM _cdc.alerts") == (0,)
