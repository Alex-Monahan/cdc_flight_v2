"""Argument handling of tools/oracle (T0): connections are built from a localpg home without printing secrets."""
import json

import pytest

from cdc_v2.oracle import cli
from cdc_v2.oracle.comparison import OracleError


PW_ADMIN, PW_RUNTIME = "made-up-" + "for-test-1", "made-up-" + "for-test-2"


@pytest.fixture
def home(tmp_path):
    (tmp_path / "secrets").mkdir()
    (tmp_path / "secrets" / "passwords.json").write_text(json.dumps(
        {"admin": {"user": "cdc_v2_admin", "password": PW_ADMIN},
         "runtime": {"user": "cdc_v2_runtime", "password": PW_RUNTIME}}))
    return tmp_path


def test_source_from_a_localpg_home(home):
    args = cli.build_parser().parse_args(["--source-port", "55433", "--source-db", "cdc_v2_t_x", "--dest-db",
                                          "cdc_v2_t_x", "--pg-home", str(home), "--source-role", "runtime"])
    s = cli.source_from_args(args)
    assert s.params["port"] == 55433 and s.params["sslmode"] == "verify-full"
    assert s.params["user"] == "cdc_v2_runtime" and s.params["sslrootcert"].endswith("tls/ca.crt")


def test_source_needs_port_and_db(home):
    args = cli.build_parser().parse_args(["--dest-db", "cdc_v2_t_x", "--pg-home", str(home)])
    with pytest.raises(OracleError):
        cli.source_from_args(args)


def test_conninfo_from_environment_must_name_a_port(monkeypatch):
    monkeypatch.setenv("CDC_V2_TEST_CONNINFO", "host=localhost dbname=x")
    args = cli.build_parser().parse_args(["--dest-db", "cdc_v2_t_x", "--source-conninfo-env", "CDC_V2_TEST_CONNINFO"])
    with pytest.raises(OracleError):
        cli.source_from_args(args)
    monkeypatch.setenv("CDC_V2_TEST_CONNINFO", "host=localhost port=55432 dbname=x")
    assert cli.source_from_args(args).params["conninfo"].endswith("dbname=x")


def test_errors_exit_2_without_echoing_the_password(home, capsys):
    rc = cli.main(["--source-port", "5432", "--source-db", "x", "--dest-db", "cdc_v2_t_x", "--pg-home", str(home)])
    err = capsys.readouterr().err
    assert rc == cli.EXIT_ERROR and "5432" in err and "made-up-for-test" not in err


def test_destination_outside_the_project_is_refused(home, capsys):
    rc = cli.main(["--source-port", "55432", "--source-db", "x", "--dest-db", "other_db", "--pg-home", str(home)])
    assert rc == cli.EXIT_ERROR and "cdc_v2_" in capsys.readouterr().err
