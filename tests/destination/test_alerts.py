"""Tier T0 (local DuckDB): alert records (`cdc_v2.alerts`, SPEC 6.2 and 8.3)."""
import json
import logging

import pytest

from cdc_v2 import alerts, md, state
from cdc_v2.deadline import Deadline

COLUMNS = [("alert_id", "VARCHAR"), ("kind", "VARCHAR"), ("severity", "VARCHAR"), ("dedup_key", "VARCHAR"),
           ("first_at", "TIMESTAMP WITH TIME ZONE"), ("last_at", "TIMESTAMP WITH TIME ZONE"), ("count", "BIGINT"),
           ("state", "VARCHAR"), ("delivery_state", "VARCHAR"), ("delivery_attempts", "BIGINT"), ("fields", "JSON"),
           ("epoch", "BIGINT"), ("lane", "VARCHAR")]

ROW_SQL = ("SELECT alert_id, kind, severity, dedup_key, epoch_us(first_at), epoch_us(last_at), count, state, "
           "delivery_state, delivery_attempts, fields, epoch, lane FROM _cdc.alerts ORDER BY first_at, alert_id")


class Untouchable:
    def __getattr__(self, name):
        raise AssertionError("the database was touched (%s)" % name)


@pytest.fixture
def al(led):
    led.stream, led.look = led.lane("stream", led.epoch), led.connect("look")
    return led


def count(conn):
    return conn.fetchone("SELECT count(*) FROM _cdc.alerts")[0]


def test_the_table_carries_what_delivery_needs(al):
    """SPEC 6.2 later needs: kind, severity, a de-duplication key, first and last time, count, state, delivery
    state and attempts, fields as JSON. The shape must not change, so it is pinned here."""
    got = al.look.fetchall("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = "
                           "'_cdc' AND table_name = 'alerts' ORDER BY ordinal_position")
    assert [tuple(r) for r in got] == COLUMNS
    assert alerts.SEVERITIES == ("info", "warning", "critical")


def test_emit_writes_one_row_inside_the_callers_transaction(al, caplog):
    fields = {"table_id": 17, "applied_position": 2 ** 64 - 1, "lag_seconds": 412.5, "slot": "cdc_v2_slot"}
    with caplog.at_level(logging.WARNING, logger="cdc_v2.alerts"):
        with al.stream.transaction() as tx:
            before = tx.clock_us()
            alert_id = alerts.emit(tx, "slot_lost", "critical", fields, key=("table_id",))
            assert tx.fetchone("SELECT count(*) FROM _cdc.alerts")[0] == 1      # its own transaction sees it
            assert count(al.look) == 0                                          # nobody else, before the commit
    rows = al.look.fetchall(ROW_SQL)
    assert len(rows) == 1
    (aid, kind, severity, dkey, first_at, last_at, n, st, dstate, attempts, stored, epoch, lane) = rows[0]
    assert (aid, kind, severity, dkey) == (alert_id, "slot_lost", "critical", "slot_lost|table_id=17")
    assert (n, st, dstate, attempts, epoch, lane) == (1, "open", "pending", 0, al.epoch, "stream")
    assert first_at == last_at and before <= first_at <= al.look.clock_us()
    assert json.loads(stored) == fields
    # the fields are JSON the server can read, a position beyond 2^63 included
    assert al.look.fetchone("SELECT json_extract_string(fields, '$.slot'), "
                            "CAST(json_extract(fields, '$.applied_position') AS UBIGINT), "
                            "CAST(json_extract(fields, '$.lag_seconds') AS DOUBLE) FROM _cdc.alerts") == (
        "cdc_v2_slot", 2 ** 64 - 1, 412.5)
    assert len(alert_id) == 32 and state.read(al.look, alerts.ALERT, alert_id) == "open"
    assert [tuple(r) for r in al.look.fetchall(
        "SELECT machine, key, from_state, to_state, cause, epoch FROM _cdc.transitions")] == [
        ("alert", json.dumps([alert_id]), None, "open", "emitted", al.epoch)]
    # the alert is logged in the C0 form
    records = [r for r in caplog.records if r.name == "cdc_v2.alerts"]
    assert len(records) == 1 and records[0].cdc["kind"] == "slot_lost" and records[0].cdc["alert_id"] == alert_id


def test_an_alert_is_gone_when_its_transaction_rolls_back(al):
    with pytest.raises(RuntimeError, match="boom"):
        with al.stream.transaction() as tx:
            alerts.emit(tx, "apply_failed", "warning", {"table_id": 3})
            raise RuntimeError("boom")
    assert count(al.look) == 0
    assert al.look.fetchone("SELECT count(*) FROM _cdc.transitions")[0] == 0


def test_two_alerts_in_one_transaction_and_defaults(al):
    with al.stream.transaction() as tx:
        a = alerts.emit(tx, "lag_high", "info")
        b = alerts.emit(tx, "lag_high", "info", {}, key=())
    assert a != b
    assert [tuple(r) for r in al.look.fetchall("SELECT dedup_key, fields, count FROM _cdc.alerts")] == [
        ("lag_high", "{}", 1)] * 2


def test_emit_now_uses_a_transaction_of_its_own(al):
    before = al.look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0]
    control = al.lane("control", al.epoch)
    alert_id = alerts.emit_now(control, "lag_high", "warning", {"lag_seconds": 3.5})
    assert [tuple(r) for r in al.look.fetchall("SELECT alert_id, lane, state FROM _cdc.alerts")] == [
        (alert_id, "control", "open")]
    assert al.look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0] == before + 1


@pytest.mark.parametrize("site,skip", [("md.execute", 0), ("md.execute", 1), ("md.fence", 0)])
def test_emit_now_tries_again_after_a_lost_connection_and_records_once(al, faults, site, skip):
    """BEGIN, the insert and the fence statement each lose the connection once; the alert is recorded exactly
    once. (A connection lost at COMMIT is `CommitUnknown` and is not tried again: the next test.)"""
    faults.raise_at(site, "ConnectionError", message="injected: connection reset", skip=skip, hits=1)
    alert_id = alerts.emit_now(al.stream, "lag_high", "warning", {"lag_seconds": 3.5}, deadline=Deadline.after(10.0))
    assert len(faults.fired(site)) == 1
    assert [r[0] for r in al.look.fetchall("SELECT alert_id FROM _cdc.alerts")] == [alert_id]
    assert al.look.fetchone("SELECT count(*) FROM _cdc.transitions")[0] == 1


def test_emit_now_passes_on_what_it_cannot_decide(al, faults):
    stale = al.lane("control", al.epoch)
    al.take_over()
    with pytest.raises(md.FenceLost):
        alerts.emit_now(stale, "lag_high", "warning")
    assert count(al.look) == 0
    new = al.lane("stream", al.epoch + 1)
    faults.raise_at("md.commit", "ConnectionError", message="injected: connection reset", hits=1)
    with pytest.raises(md.CommitUnknown):                       # lost at COMMIT: recorded or not, not known
        alerts.emit_now(new, "lag_high", "warning")
    assert count(al.look) == 0                                  # here it did not land
    faults.raise_at("md.commit_reply", "ConnectionError", message="injected: connection reset", hits=1)
    with pytest.raises(md.CommitUnknown):                       # the answer to COMMIT was lost
        alerts.emit_now(new, "lag_high", "warning")
    assert count(al.look) == 1                                  # here it did land; emit_now did not write a second


def test_emit_now_gives_up_at_its_deadline(al, faults):
    faults.raise_at("md.fence", "ConnectionError", message="injected: connection reset")
    with pytest.raises(md.DeadlineExceeded):
        alerts.emit_now(al.stream, "lag_high", "warning", deadline=Deadline.after(0.6))
    assert len(faults.fired("md.fence")) >= 2 and count(al.look) == 0


def test_resolve(al):
    with al.stream.transaction() as tx:
        a = alerts.emit(tx, "table_failed", "critical", {"table_id": 1, "attempts": 3}, key=("table_id",))
        b = alerts.emit(tx, "table_failed", "critical", {"table_id": 1, "attempts": 4}, key="table_id")
        c = alerts.emit(tx, "table_failed", "critical", {"table_id": 2, "attempts": 1}, key=("table_id",))
    dkey = alerts.dedup_key("table_failed", {"table_id": 1}, ("table_id",))
    assert dkey == "table_failed|table_id=1"
    with al.stream.transaction() as tx:
        assert alerts.resolve(tx, dkey, "the table was copied again") == 2
        assert alerts.resolve(tx, dkey, "again") == 0
        assert alerts.resolve(tx, "table_failed|table_id=9", "nothing there") == 0
    assert {r[0]: r[1] for r in al.look.fetchall("SELECT alert_id, state FROM _cdc.alerts")} == {
        a: "resolved", b: "resolved", c: "open"}
    assert al.look.fetchone("SELECT count(*) FROM _cdc.transitions WHERE to_state = 'resolved' AND cause = "
                            "'the table was copied again'")[0] == 2
    with al.stream.transaction() as tx:                         # resolved is final
        with pytest.raises(state.IllegalTransition):
            state.transition(tx, alerts.ALERT, a, "resolved", "open", "back")


def test_dedup_key():
    f = {"table_id": 7, "slot": "s 1", "lag_seconds": 1.5}
    assert alerts.dedup_key("lag_high") == "lag_high"
    assert alerts.dedup_key("lag_high", f) == "lag_high"
    assert alerts.dedup_key("lag_high", f, ("slot", "table_id")) == 'lag_high|slot="s 1"|table_id=7'
    assert alerts.dedup_key("lag_high", f, "lag_seconds") == "lag_high|lag_seconds=1.5"
    with pytest.raises(alerts.AlertError, match="no field other"):
        alerts.dedup_key("lag_high", f, ("other",))
    for bad in ("Lag", "", "lag high", "1lag", None, 7, "x" * 65, "lag|x"):
        with pytest.raises(alerts.AlertError, match="kind"):
            alerts.dedup_key(bad)


SECRET = "row-value-do-not-leak"

BAD_VALUES = [
    True, False, None, [1, 2], (1,), {"a": 1}, {1}, SECRET.encode(), bytearray(b"x"), object(),
    SECRET + "x" * (alerts.MAX_STRING + 1 - len(SECRET)), SECRET + "\n", SECRET + "\x00", SECRET + "\t",
    float("nan"), float("inf"), float("-inf"), 2 ** 64, -(2 ** 63) - 1, 1 + 2j,
]


@pytest.mark.parametrize("value", BAD_VALUES, ids=lambda v: type(v).__name__)
def test_a_value_that_is_not_an_identifier_position_count_or_duration_is_refused(value):
    """SPEC 8.3: a row value can never reach an alert. Refused before the database is touched, and the value is
    not in the error text."""
    for call in (lambda: alerts.emit(Untouchable(), "apply_failed", "warning", {"ok": 1, "bad": value}),
                 lambda: alerts.emit_now(Untouchable(), "apply_failed", "warning", {"bad": value}),
                 lambda: alerts.dedup_key("apply_failed", {"bad": value}, ("bad",)),
                 lambda: alerts.check_fields({"bad": value})):
        with pytest.raises(alerts.AlertError) as info:
            call()
        assert "bad" in str(info.value) and "row-value" not in str(info.value)
        assert type(value).__name__ in str(info.value)


def test_values_at_the_limits_are_accepted():
    ok = {"a": 0, "b": -(2 ** 63), "c": 2 ** 64 - 1, "d": 0.0, "e": -1.5e300, "f": "", "g": "x" * alerts.MAX_STRING,
          "h": "public.orders", "i": "0/16B3748"}
    assert alerts.check_fields(ok) == ok and alerts.check_fields(ok) is not ok
    assert alerts.check_fields(None) == {} and alerts.check_fields({}) == {}
    many = {"f%d" % i: i for i in range(alerts.MAX_FIELDS)}
    assert alerts.check_fields(many) == many


@pytest.mark.parametrize("fields", [
    {"Bad": 1}, {"": 1}, {"has space": 1}, {"1x": 1}, {7: 1}, {None: 1}, {"x" * 41: 1}, {"a.b": 1},
    {"f%d" % i: i for i in range(alerts.MAX_FIELDS + 1)}, [("a", 1)], "a=1", 7,
])
def test_wrong_fields_are_refused(fields):
    with pytest.raises(alerts.AlertError):
        alerts.emit(Untouchable(), "apply_failed", "warning", fields)


@pytest.mark.parametrize("kind,severity", [
    ("apply_failed", "fatal"), ("apply_failed", "WARNING"), ("apply_failed", None), ("apply_failed", ""),
    ("Apply", "warning"), ("apply failed", "warning"), ("", "warning"), (None, "warning"),
])
def test_a_wrong_kind_or_severity_is_refused(kind, severity):
    with pytest.raises(alerts.AlertError):
        alerts.emit(Untouchable(), kind, severity)
    with pytest.raises(alerts.AlertError):
        alerts.emit_now(Untouchable(), kind, severity)


def test_nothing_is_written_when_an_alert_is_refused(al):
    with al.stream.transaction() as tx:
        with pytest.raises(alerts.AlertError):
            alerts.emit(tx, "apply_failed", "warning", {"value": [SECRET]})
        with pytest.raises(alerts.AlertError):
            alerts.emit(tx, "apply_failed", "warning", {"n": 1}, key=("missing",))
        alerts.emit(tx, "apply_failed", "warning", {"n": 1})       # the transaction is still usable
    assert count(al.look) == 1
    assert SECRET not in json.dumps(al.look.fetchall("SELECT kind, dedup_key, fields FROM _cdc.alerts"))


def test_an_alert_before_the_migrations_ran_fails_and_writes_nothing(dest):
    import duckdb
    epoch = dest.lead(migrate=False)
    with pytest.raises(duckdb.CatalogException):
        alerts.emit_now(dest.lane("control", epoch), "takeover", "info", {"epoch": epoch})
    look = dest.connect("look")
    assert look.fetchone("SELECT count(*) FROM information_schema.tables WHERE table_schema = '_cdc' AND "
                         "table_name = 'alerts'")[0] == 0
