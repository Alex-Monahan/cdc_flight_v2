"""Unit tests for cdc_v2.flightctl: the SQL it builds and how it parses results. No network.

The integration test (creates, runs and deletes a tiny Flight on MotherDuck) is opt-in:
    tools/with-env uv run pytest -m flight_integration tests/test_flightctl.py
"""
import json
import os
import subprocess
import sys
import uuid

import pytest

from cdc_v2 import flightctl as fc

FID = "206bf7dc-3ac6-42b7-80c1-cf80008e9fc7"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --- literals ---------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    (None, "NULL"), (True, "true"), (False, "false"), (0, "0"), (3600, "3600"),
    ("abc", "'abc'"), ("it's", "'it''s'"), ("", "''"), ("a\nb", "'a\nb'"),
    (["a", "b'c"], "['a', 'b''c']"), ([], "[]"),
    ({"K": "v", "Q": "it's"}, "MAP {'K': 'v', 'Q': 'it''s'}"), ({}, "MAP {}"),
    (uuid.UUID(FID), "'%s'::UUID" % FID),
])
def test_sql_literal(value, expected):
    assert fc.sql_literal(value) == expected


def test_sql_literal_rejects_nul_and_non_string_maps():
    with pytest.raises(fc.FlightCtlError):
        fc.sql_literal("a\x00b")
    with pytest.raises(fc.FlightCtlError):
        fc.sql_literal({"K": 1})
    with pytest.raises(fc.FlightCtlError):
        fc.sql_literal(1.5)


def test_sql_literal_round_trips_through_duckdb():
    duckdb = pytest.importorskip("duckdb")
    nasty = "x = 'y'; -- $$ \\ \" \n\t ünï"
    con = duckdb.connect()
    assert con.execute("SELECT " + fc.sql_literal(nasty)).fetchone()[0] == nasty
    m = con.execute("SELECT " + fc.sql_literal({"A": nasty, "B": ""})).fetchone()[0]
    assert dict(m) == {"A": nasty, "B": ""}
    assert con.execute("SELECT " + fc.sql_literal(["a'", "b"])).fetchone()[0] == ["a'", "b"]


# --- statements -------------------------------------------------------------------------------------------------

def test_create_minimal():
    sql = fc.build_create_sql("cdc_v2_x", "print('hi')")
    assert sql == "SELECT * FROM MD_CREATE_FLIGHT(name := 'cdc_v2_x', source_code := 'print(''hi'')')"


def test_create_full():
    sql = fc.build_create_sql("cdc_v2_x", "src", requirements_txt="duckdb==1.5.6\n", schedule_cron="* * * * *",
                              config={"A": "1"}, secret_names=["cdc_v2_s"], max_runtime_sec=600,
                              instance_type="F16", access_token_name="tok")
    assert sql == ("SELECT * FROM MD_CREATE_FLIGHT(name := 'cdc_v2_x', source_code := 'src', "
                   "requirements_txt := 'duckdb==1.5.6\n', schedule_cron := '* * * * *', config := MAP {'A': '1'}, "
                   "flight_secret_names := ['cdc_v2_s'], max_runtime_sec := 600, instance_type := 'F16', "
                   "access_token_name := 'tok')")


def test_create_empty_schedule_is_omitted():
    assert "schedule_cron" not in fc.build_create_sql("cdc_v2_x", "src", schedule_cron="")


@pytest.mark.parametrize("kwargs", [
    {"name": "other_flight"}, {"name": ""}, {"name": "cdc_v2_x", "instance_type": "F8"},
    {"name": "cdc_v2_x", "schedule_cron": "*/30 * * * * *"}, {"name": "cdc_v2_x", "source_code": "  \n"},
])
def test_create_refusals(kwargs):
    kwargs.setdefault("source_code", "src")
    with pytest.raises(fc.FlightCtlError):
        fc.build_create_sql(kwargs.pop("name"), kwargs.pop("source_code"), **kwargs)


def test_update_patch_only_given_fields():
    sql = fc.build_update_sql(FID, schedule_cron="", flight_secret_names=[], config={})
    assert sql == ("SELECT * FROM MD_UPDATE_FLIGHT(flight_id := '%s'::UUID, schedule_cron := '', config := MAP {}, "
                   "flight_secret_names := [])" % FID)


def test_update_refusals():
    with pytest.raises(fc.FlightCtlError):
        fc.build_update_sql(FID)
    with pytest.raises(fc.FlightCtlError):
        fc.build_update_sql(FID, name="not_ours")
    with pytest.raises(fc.FlightCtlError):
        fc.build_update_sql(FID, colour="red")
    with pytest.raises(fc.FlightCtlError):
        fc.build_update_sql("not-a-uuid", name="cdc_v2_x")


def test_run_and_runs_and_cancel():
    assert fc.build_run_sql(FID) == "SELECT * FROM MD_RUN_FLIGHT(flight_id := '%s'::UUID)" % FID
    assert fc.build_run_sql(FID, {"MODE": "x"}) == (
        "SELECT * FROM MD_RUN_FLIGHT(flight_id := '%s'::UUID, config := MAP {'MODE': 'x'})" % FID)
    assert fc.build_list_runs_sql(FID, 5) == (
        'SELECT * FROM MD_LIST_FLIGHT_RUNS(flight_id := \'%s\'::UUID, "LIMIT" := 5, "OFFSET" := 0)' % FID)
    assert fc.build_get_run_sql(FID, 7) == (
        "SELECT * FROM MD_GET_FLIGHT_RUN(flight_id := '%s'::UUID, run_number := 7)" % FID)
    assert fc.build_cancel_sql(FID, 7) == (
        "SELECT * FROM MD_CANCEL_FLIGHT_RUN(flight_id := '%s'::UUID, run_number := 7)" % FID)
    assert fc.build_delete_sql(FID) == "SELECT * FROM MD_DELETE_FLIGHT(flight_id := '%s'::UUID)" % FID


def test_logs_sql():
    assert fc.build_logs_sql(FID, 3) == (
        "SELECT * FROM MD_GET_FLIGHT_LOGS(flight_id := '%s'::UUID, run_number := 3) ORDER BY line_number" % FID)
    assert fc.build_logs_sql(FID, 3, limit=10, order="desc") == (
        'SELECT * FROM MD_GET_FLIGHT_LOGS(flight_id := \'%s\'::UUID, run_number := 3, "LIMIT" := 10, '
        '"ORDER" := \'desc\') ORDER BY line_number' % FID)
    with pytest.raises(fc.FlightCtlError):
        fc.build_logs_sql(FID, 3, offset=5)
    with pytest.raises(fc.FlightCtlError):
        fc.build_logs_sql(FID, 3, order="up")


def test_list_flights_is_filtered_to_project_names():
    sql = fc.build_list_flights_sql()
    assert "owner_only := true" in sql and "WHERE starts_with(flight_name, 'cdc_v2')" in sql
    assert "starts_with(flight_name, 'cdc_v2_spike')" in fc.build_list_flights_sql("cdc_v2_spike")
    with pytest.raises(fc.FlightCtlError):
        fc.build_list_flights_sql("")


def test_secret_statements():
    sql = fc.build_create_secret_sql("cdc_v2_s", {"KEY": "v'al"})
    assert sql == "CREATE OR REPLACE SECRET cdc_v2_s IN MOTHERDUCK (TYPE FLIGHTS, PARAMS MAP {'KEY': 'v''al'})"
    assert fc.build_drop_secret_sql("cdc_v2_s") == "DROP SECRET IF EXISTS cdc_v2_s FROM MOTHERDUCK"
    for bad in ("other", "cdc_v2-x", "cdc_v2 x; DROP"):
        with pytest.raises(fc.FlightCtlError):
            fc.build_create_secret_sql(bad, {"K": "v"})
    with pytest.raises(fc.FlightCtlError):
        fc.build_create_secret_sql("cdc_v2_s", {"bad key": "v"})
    with pytest.raises(fc.FlightCtlError):
        fc.build_create_secret_sql("cdc_v2_s", {})
    assert "starts_with(name, 'cdc_v2')" in fc.build_list_secrets_sql()


def test_wrap_json():
    assert fc.wrap_json("SELECT 1") == "SELECT to_json(_r) AS j FROM (SELECT 1) AS _r"


# --- parsing ----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("RUN_STATUS_RUNNING", "RUNNING"), ("RUNNING", "RUNNING"), ("run_status_failed", "FAILED"),
    ("SCHEDULE_STATUS_ACTIVE", "ACTIVE"), ("JOB_STATUS_ACTIVE", "ACTIVE"), (None, None),
])
def test_normalize_status(raw, expected):
    assert fc.normalize_status(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("2026-11-10 02:08:07.667+00", "2026-11-10T02:08:07.667+00:00"),
    ("2026-11-10 02:08:07+00", "2026-11-10T02:08:07+00:00"),
    ("2026-11-10 02:08:07.1+05:30", "2026-11-10T02:08:07.1+05:30"),
    ("not a time", "not a time"), (None, None), (5, 5),
])
def test_normalize_timestamp(raw, expected):
    assert fc.normalize_timestamp(raw) == expected


def test_parse_json_rows_redacts_users_and_adds_state():
    doc = json.dumps({"flight_id": FID, "flight_name": "cdc_v2_x", "owner_name": "someone",
                      "started_by_name": None, "started_by_id": "u-1", "status": "RUN_STATUS_SUCCEEDED",
                      "created_at": "2026-11-10 02:09:02.795+00", "config": {"A": "1"}, "exit_code": 0})
    (row,) = fc.parse_json_rows([doc])
    assert row["owner_name"] == "<redacted>" and row["started_by_id"] == "<redacted>"
    assert row["started_by_name"] is None
    assert row["state"] == "SUCCEEDED" and row["config"] == {"A": "1"}
    assert row["created_at"] == "2026-11-10T02:09:02.795+00:00"
    assert fc.parse_time(row["created_at"]).tzinfo is not None
    assert "someone" not in json.dumps(row)


def test_run_state_predicates():
    assert fc.run_is_active({"status": "RUN_STATUS_PENDING"}) and fc.run_is_active({"status": "RUNNING"})
    assert fc.run_is_terminal({"status": "CANCELLED"}) and not fc.run_is_terminal({"status": "RUNNING"})


def test_parse_json_rows_against_duckdb_rendering():
    """The JSON shape DuckDB produces for the column types of the Flight functions."""
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    sql = ("SELECT '%s'::UUID AS flight_id, TIMESTAMPTZ '2026-11-10 02:09:02.795+00' AS created_at, "
           "MAP {'K': 'v'} AS config, ['cdc_v2_s'] AS flight_secret_names, 7::UBIGINT AS run_number, "
           "NULL::INTEGER AS exit_code, 'RUN_STATUS_RUNNING' AS status" % FID)
    rows = fc.parse_json_rows(r[0] for r in con.execute(fc.wrap_json(sql)).fetchall())
    assert rows == [{"flight_id": FID, "created_at": "2026-11-10T02:09:02.795+00:00", "config": {"K": "v"},
                     "flight_secret_names": ["cdc_v2_s"], "run_number": 7, "exit_code": None,
                     "status": "RUN_STATUS_RUNNING", "state": "RUNNING"}]


# --- client against a fake connection ---------------------------------------------------------------------------

class FakeCon:
    def __init__(self, responses):
        self.responses, self.sql = responses, []

    def execute(self, sql):
        self.sql.append(sql)
        for needle, rows in self.responses:
            if needle in sql:
                self._rows = [(json.dumps(r),) for r in rows]
                return self
        self._rows = []
        return self

    def fetchall(self):
        return self._rows

    def close(self):
        pass


def test_resolve_refuses_foreign_flight_by_id():
    con = FakeCon([("MD_GET_FLIGHT(", [{"flight_id": FID, "flight_name": "someone_elses"}])])
    with pytest.raises(fc.FlightCtlError) as e:
        fc.FlightCtl(con).delete(FID)
    assert "someone_elses" not in str(e.value)
    assert not any("MD_DELETE_FLIGHT" in s for s in con.sql)


def test_deploy_updates_existing_by_name_and_never_clears_absent_schedule():
    con = FakeCon([("MD_LIST_FLIGHTS", [{"flight_id": FID, "flight_name": "cdc_v2_x", "schedule_cron": None}]),
                   ("MD_UPDATE_FLIGHT", [{"flight_id": FID, "flight_name": "cdc_v2_x", "current_version": 2}])])
    action, summary = fc.FlightCtl(con).deploy("cdc_v2_x", "src", schedule_cron="", secret_names=["cdc_v2_s"])
    assert action == "updated" and summary["current_version"] == 2
    upd = [s for s in con.sql if "MD_UPDATE_FLIGHT" in s][0]
    assert "schedule_cron" not in upd and "flight_secret_names := ['cdc_v2_s']" in upd


def test_deploy_creates_when_absent():
    con = FakeCon([("MD_CREATE_FLIGHT", [{"flight_id": FID, "flight_name": "cdc_v2_x", "current_version": 1}])])
    action, _ = fc.FlightCtl(con).deploy("cdc_v2_x", "src", instance_type="F16")
    assert action == "created"
    assert any("MD_CREATE_FLIGHT" in s and "instance_type := 'F16'" in s for s in con.sql)


def test_active_runs_and_cancel_by_id():
    con = FakeCon([("MD_LIST_FLIGHT_RUNS", [{"run_number": 3, "status": "RUNNING"},
                                            {"run_number": 2, "status": "RUN_STATUS_PENDING"},
                                            {"run_number": 1, "status": "SUCCEEDED"}])])
    ctl = fc.FlightCtl(con)
    assert [r["run_number"] for r in ctl.active_runs(FID)] == [3, 2]
    out = ctl.cancel_by_id(FID, 3)
    assert out["run_number"] == 3
    assert any("MD_CANCEL_FLIGHT_RUN(flight_id := '%s'::UUID, run_number := 3)" % FID in s for s in con.sql)


def test_secret_value_never_in_error():
    class Boom(FakeCon):
        def execute(self, sql):
            raise RuntimeError("Parser Error near " + sql)
    with pytest.raises(fc.FlightCtlError) as e:
        fc.FlightCtl(Boom([])).create_secret("cdc_v2_s", {"K": "very-secret-value"})
    assert "very-secret-value" not in str(e.value)


def test_cli_json_error_without_token():
    env = {k: v for k, v in os.environ.items() if k.lower() != "motherduck_token"}
    p = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "flightctl"), "--json", "list"],
                       capture_output=True, text=True, env=env)
    assert p.returncode == 1
    assert "error" in json.loads(p.stdout)


def test_cli_help_lists_subcommands():
    p = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "flightctl"), "--help"],
                       capture_output=True, text=True)
    for cmd in ("deploy", "update", "run", "runs", "cancel", "logs", "delete", "secret-set", "unschedule"):
        assert cmd in p.stdout


# --- opt-in integration ----------------------------------------------------------------------------------------

@pytest.mark.flight_integration
@pytest.mark.timeout(900)
def test_integration_create_run_logs_delete():
    if not (os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")):
        pytest.skip("needs a MotherDuck token (tools/with-env)")
    name = "cdc_v2_spike_it_%s" % uuid.uuid4().hex[:8]
    src = ('import os, sys\n\ndef main():\n    print("hello from", os.environ.get("CDC_V2_IT"))\n'
           '    sys.exit(int(os.environ.get("CDC_V2_IT_EXIT", "0")))\n\nif __name__ == "__main__":\n    main()\n')
    ctl = fc.FlightCtl()
    try:
        created = ctl.create(name, src, instance_type="F4", config={"CDC_V2_IT": "it-marker", "CDC_V2_IT_EXIT": "0"})
        assert created["flight_name"] == name and created["owner_name"] == "<redacted>"
        assert ctl.versions(name)[0]["instance_type"] == "F4"
        run = ctl.run(name)
        final = ctl.wait(name, run["run_number"], timeout=600)
        assert final["state"] == "SUCCEEDED" and final["exit_code"] == 0
        assert any("hello from it-marker" in (ln["line"] or "") for ln in ctl.logs(name, run["run_number"]))
        run2 = ctl.run(name, {"CDC_V2_IT_EXIT": "4"})
        final2 = ctl.wait(name, run2["run_number"], timeout=600)
        assert final2["state"] == "FAILED" and final2["exit_code"] == 4
        updated = ctl.update(name, schedule_cron="0 0 1 1 *")
        assert updated["schedule_cron"] == "0 0 1 1 *"
        assert ctl.unschedule(name)["schedule_cron"] is None
    finally:
        if ctl.find_flight(name):
            ctl.delete(name)
        assert ctl.find_flight(name) is None
        ctl.close()
