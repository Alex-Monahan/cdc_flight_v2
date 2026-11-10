"""Drive MotherDuck Flights through MotherDuck's Flight SQL functions.

Everything goes through SQL on a `duckdb.connect("md:")` connection (the token comes from the environment:
`motherduck_token` locally, `MOTHERDUCK_TOKEN` inside a Flight). Used by `tools/flightctl` and, later, by the
pipeline's leader guard ("cancel that run" from inside a Flight).

Functions used (https://motherduck.com/docs/sql-reference/motherduck-sql-reference/flights/):
MD_CREATE_FLIGHT, MD_UPDATE_FLIGHT, MD_DELETE_FLIGHT, MD_GET_FLIGHT, MD_LIST_FLIGHTS, MD_LIST_FLIGHT_VERSIONS,
MD_GET_FLIGHT_VERSION, MD_RUN_FLIGHT, MD_LIST_FLIGHT_RUNS, MD_GET_FLIGHT_RUN, MD_GET_FLIGHT_LOGS,
MD_CANCEL_FLIGHT_RUN; secrets with CREATE SECRET ... (TYPE FLIGHTS).

Table-function arguments must be constants, so values are rendered as SQL literals by `sql_literal`, which is
unit-tested. Secret values are never rendered into a statement that this module logs or returns.

Naming rule (PLAN §4): this module only creates, updates, runs, cancels or deletes Flights and secrets whose
names start with `cdc_v2`. Flights with other names are invisible to its callers: listings are filtered in SQL
and their names are never returned.
"""
from __future__ import annotations

import datetime as _dt
import os
import re
import time
import uuid
from typing import Any, Iterable, Mapping, Sequence

NAME_PREFIX = "cdc_v2"
INSTANCE_TYPES = ("F4", "F16", "F32")
# Documented definition limits (MD_CREATE_FLIGHT); the measured limits are in docs/spike/flight_platform.md.
SOURCE_LIMIT_BYTES = 200 * 1024
REQUIREMENTS_LIMIT_BYTES = 20 * 1024

RUN_PENDING = "PENDING"
RUN_RUNNING = "RUNNING"
RUN_SUCCEEDED = "SUCCEEDED"
RUN_FAILED = "FAILED"
RUN_CANCELLED = "CANCELLED"
ACTIVE_STATES = frozenset({RUN_PENDING, RUN_RUNNING})
TERMINAL_STATES = frozenset({RUN_SUCCEEDED, RUN_FAILED, RUN_CANCELLED})

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class FlightCtlError(Exception):
    """A refused or failed Flight operation. The message never contains a secret value."""


# ---------------------------------------------------------------------------------------------------------------
# SQL rendering (pure functions, unit-tested)
# ---------------------------------------------------------------------------------------------------------------

def sql_literal(value: Any) -> str:
    """Render a Python value as a DuckDB SQL literal.

    str -> '...' (quotes doubled), bool -> true/false, int -> digits, None -> NULL,
    list/tuple -> [..], dict -> MAP {k: v, ...}, uuid.UUID -> '...'::UUID.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, uuid.UUID):
        return "'%s'::UUID" % value
    if isinstance(value, str):
        if "\x00" in value:
            raise FlightCtlError("a NUL character cannot be passed in a SQL string")
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(sql_literal(v) for v in value) + "]"
    if isinstance(value, Mapping):
        if not value:
            return "MAP {}"
        items = []
        for k, v in value.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise FlightCtlError("config and secret maps hold strings only")
            items.append("%s: %s" % (sql_literal(k), sql_literal(v)))
        return "MAP {" + ", ".join(items) + "}"
    raise FlightCtlError("cannot render %s as a SQL literal" % type(value).__name__)


def _named_args(args: Sequence[tuple[str, Any]]) -> str:
    parts = []
    for name, value in args:
        key = '"%s"' % name if name.upper() in ("LIMIT", "OFFSET", "ORDER") else name
        parts.append("%s := %s" % (key, sql_literal(value)))
    return ", ".join(parts)


def _call(function: str, args: Sequence[tuple[str, Any]]) -> str:
    return "SELECT * FROM %s(%s)" % (function, _named_args(args))


def parse_flight_id(value: str | uuid.UUID) -> uuid.UUID:
    try:
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
    except ValueError as exc:
        raise FlightCtlError("not a Flight id (UUID): %r" % value) from exc


def check_name(name: str, what: str = "Flight") -> str:
    if not name or not name.startswith(NAME_PREFIX):
        raise FlightCtlError("%s name must start with %r (PLAN §4): %r" % (what, NAME_PREFIX, name))
    return name


def check_secret_name(name: str) -> str:
    check_name(name, "secret")
    if not _IDENT.match(name):
        raise FlightCtlError("secret name must be an identifier: %r" % name)
    return name


def _check_definition(source_code: str | None, requirements_txt: str | None, instance_type: str | None,
                      schedule_cron: str | None) -> None:
    if instance_type is not None and instance_type not in INSTANCE_TYPES:
        raise FlightCtlError("instance_type must be one of %s" % ", ".join(INSTANCE_TYPES))
    if schedule_cron not in (None, "") and len(schedule_cron.split()) != 5:
        raise FlightCtlError("schedule_cron must have 5 fields (UTC): %r" % schedule_cron)
    if source_code is not None and not source_code.strip():
        raise FlightCtlError("source_code is empty")


def build_create_sql(name: str, source_code: str, *, requirements_txt: str | None = None,
                     schedule_cron: str | None = None, config: Mapping[str, str] | None = None,
                     secret_names: Sequence[str] | None = None, max_runtime_sec: int | None = None,
                     instance_type: str | None = None, access_token_name: str | None = None) -> str:
    check_name(name)
    _check_definition(source_code, requirements_txt, instance_type, schedule_cron)
    args: list[tuple[str, Any]] = [("name", name), ("source_code", source_code)]
    for key, value in (("requirements_txt", requirements_txt), ("schedule_cron", schedule_cron or None),
                       ("config", config), ("flight_secret_names", list(secret_names) if secret_names is not None
                                            else None),
                       ("max_runtime_sec", max_runtime_sec), ("instance_type", instance_type),
                       ("access_token_name", access_token_name)):
        if value is not None:
            args.append((key, value))
    return _call("MD_CREATE_FLIGHT", args)


_UPDATE_FIELDS = ("name", "source_code", "requirements_txt", "schedule_cron", "config", "flight_secret_names",
                  "max_runtime_sec", "instance_type", "access_token_name")


def build_update_sql(flight_id: str | uuid.UUID, **fields: Any) -> str:
    """PATCH semantics: only the fields given (not None) are sent. schedule_cron='' clears the schedule."""
    fid = parse_flight_id(flight_id)
    unknown = set(fields) - set(_UPDATE_FIELDS)
    if unknown:
        raise FlightCtlError("unknown update field(s): %s" % ", ".join(sorted(unknown)))
    if fields.get("name") is not None:
        check_name(fields["name"])
    _check_definition(fields.get("source_code"), fields.get("requirements_txt"), fields.get("instance_type"),
                      fields.get("schedule_cron"))
    args: list[tuple[str, Any]] = [("flight_id", fid)]
    for key in _UPDATE_FIELDS:
        value = fields.get(key)
        if value is None:
            continue
        if key == "flight_secret_names":
            value = list(value)
        args.append((key, value))
    if len(args) == 1:
        raise FlightCtlError("nothing to update")
    return _call("MD_UPDATE_FLIGHT", args)


def build_run_sql(flight_id: str | uuid.UUID, config: Mapping[str, str] | None = None) -> str:
    args: list[tuple[str, Any]] = [("flight_id", parse_flight_id(flight_id))]
    if config:
        args.append(("config", config))
    return _call("MD_RUN_FLIGHT", args)


def build_list_runs_sql(flight_id: str | uuid.UUID, limit: int = 50, offset: int = 0) -> str:
    return _call("MD_LIST_FLIGHT_RUNS", [("flight_id", parse_flight_id(flight_id)), ("LIMIT", limit),
                                         ("OFFSET", offset)])


def build_get_run_sql(flight_id: str | uuid.UUID, run_number: int) -> str:
    return _call("MD_GET_FLIGHT_RUN", [("flight_id", parse_flight_id(flight_id)), ("run_number", int(run_number))])


def build_cancel_sql(flight_id: str | uuid.UUID, run_number: int) -> str:
    return _call("MD_CANCEL_FLIGHT_RUN", [("flight_id", parse_flight_id(flight_id)),
                                          ("run_number", int(run_number))])


def build_logs_sql(flight_id: str | uuid.UUID, run_number: int, limit: int | None = None, offset: int | None = None,
                   order: str | None = None) -> str:
    args: list[tuple[str, Any]] = [("flight_id", parse_flight_id(flight_id)), ("run_number", int(run_number))]
    if order is not None and order not in ("asc", "desc"):
        raise FlightCtlError("order must be 'asc' or 'desc'")
    if offset is not None and limit is None:
        raise FlightCtlError("offset requires limit")
    if limit is not None:
        args.append(("LIMIT", limit))
    if offset is not None:
        args.append(("OFFSET", offset))
    if order is not None:
        args.append(("ORDER", order))
    return _call("MD_GET_FLIGHT_LOGS", args) + " ORDER BY line_number"


def build_delete_sql(flight_id: str | uuid.UUID) -> str:
    return _call("MD_DELETE_FLIGHT", [("flight_id", parse_flight_id(flight_id))])


def build_get_flight_sql(flight_id: str | uuid.UUID) -> str:
    return _call("MD_GET_FLIGHT", [("flight_id", parse_flight_id(flight_id))])


def build_list_flights_sql(prefix: str = NAME_PREFIX, limit: int = 1000) -> str:
    """Only Flights whose name starts with `prefix` (never shorter than the project prefix) are returned."""
    if not prefix.startswith(NAME_PREFIX):
        raise FlightCtlError("listing is restricted to names starting with %r" % NAME_PREFIX)
    return (_call("MD_LIST_FLIGHTS", [("LIMIT", limit), ("owner_only", True)])
            + " WHERE starts_with(flight_name, %s) ORDER BY flight_name" % sql_literal(prefix))


def build_list_versions_sql(flight_id: str | uuid.UUID, limit: int = 50) -> str:
    return _call("MD_LIST_FLIGHT_VERSIONS", [("flight_id", parse_flight_id(flight_id)), ("LIMIT", limit)])


def build_get_version_sql(flight_id: str | uuid.UUID, version_number: int) -> str:
    return _call("MD_GET_FLIGHT_VERSION", [("flight_id", parse_flight_id(flight_id)),
                                           ("version_number", int(version_number))])


def build_create_secret_sql(name: str, params: Mapping[str, str]) -> str:
    """CREATE OR REPLACE SECRET <name> IN MOTHERDUCK (TYPE FLIGHTS, PARAMS MAP {...}).

    The statement contains the secret values: never log it or return it."""
    check_secret_name(name)
    if not params:
        raise FlightCtlError("a Flight secret needs at least one key")
    for key in params:
        if not _IDENT.match(key):
            raise FlightCtlError("secret key must be an identifier: %r" % key)
    return "CREATE OR REPLACE SECRET %s IN MOTHERDUCK (TYPE FLIGHTS, PARAMS %s)" % (name, sql_literal(dict(params)))


def build_drop_secret_sql(name: str) -> str:
    check_secret_name(name)
    return "DROP SECRET IF EXISTS %s FROM MOTHERDUCK" % name


def build_list_secrets_sql() -> str:
    return ("SELECT name, type, storage FROM duckdb_secrets() WHERE starts_with(name, %s) ORDER BY name"
            % sql_literal(NAME_PREFIX))


# ---------------------------------------------------------------------------------------------------------------
# Result parsing (pure functions, unit-tested)
# ---------------------------------------------------------------------------------------------------------------

def normalize_status(status: str | None) -> str | None:
    """'RUN_STATUS_RUNNING' and 'RUNNING' both become 'RUNNING' (newer clients drop the prefix)."""
    if status is None:
        return None
    s = str(status).upper()
    for prefix in ("RUN_STATUS_", "SCHEDULE_STATUS_", "JOB_STATUS_"):
        if s.startswith(prefix):
            return s[len(prefix):]
    return s


# Columns that carry a MotherDuck user name or user id: never returned (public repository, PLAN §4).
REDACTED_COLUMNS = frozenset({"owner_name", "started_by_name", "started_by_id", "username", "user_name"})
_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}(?:\.\d+)?)([+-]\d{2})(?::?(\d{2}))?$")


def wrap_json(sql: str) -> str:
    """Each result row as one JSON document: timestamps, UUIDs, maps and lists survive without pytz."""
    return "SELECT to_json(_r) AS j FROM (%s) AS _r" % sql


def normalize_timestamp(value: Any) -> Any:
    """'2026-11-10 02:08:07.667+00' (DuckDB's JSON rendering in UTC) -> '2026-11-10T02:08:07.667+00:00'."""
    if not isinstance(value, str):
        return value
    m = _TS.match(value)
    if not m:
        return value
    return "%sT%s%s:%s" % (m.group(1), m.group(2), m.group(3), m.group(4) or "00")


def clean_row(row: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if key in REDACTED_COLUMNS:
            out[key] = None if value is None else "<redacted>"
            continue
        out[key] = normalize_timestamp(value) if key.endswith("_at") else value
    if isinstance(out.get("status"), str):
        out["state"] = normalize_status(out["status"])
    return out


def parse_json_rows(docs: Iterable[str]) -> list[dict[str, Any]]:
    import json
    return [clean_row(json.loads(doc)) for doc in docs]



def parse_time(value: Any) -> _dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=_dt.timezone.utc)
    return _dt.datetime.fromisoformat(str(normalize_timestamp(value)))


def run_is_active(run: Mapping[str, Any]) -> bool:
    return normalize_status(run.get("status")) in ACTIVE_STATES


def run_is_terminal(run: Mapping[str, Any]) -> bool:
    return normalize_status(run.get("status")) in TERMINAL_STATES


# ---------------------------------------------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------------------------------------------

class FlightCtl:
    """Thin client over the Flight SQL functions. `con` may be passed for tests or reuse."""

    def __init__(self, con: Any = None, database: str = "md:"):
        self._con = con
        self._database = database

    @property
    def con(self) -> Any:
        if self._con is None:
            if not (os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")):
                raise FlightCtlError("no MotherDuck token in the environment (run through tools/with-env)")
            import duckdb  # imported lazily so the pure functions above need no duckdb
            self._con = duckdb.connect(self._database)
            self._con.execute("SET TimeZone = 'UTC'")
        return self._con

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def query(self, sql: str, *, quiet_sql: bool = False) -> list[dict[str, Any]]:
        """Run a statement; result rows come back as JSON from the server (no pytz/pyarrow needed)."""
        try:
            if sql.lstrip().upper().startswith("SELECT"):
                rows = self.con.execute(wrap_json(sql)).fetchall()
                return parse_json_rows(r[0] for r in rows)
            self.con.execute(sql)
            return []
        except FlightCtlError:
            raise
        except Exception as exc:  # duckdb raises several exception classes; keep the message, drop the SQL
            msg = str(exc)
            if quiet_sql:
                msg = "statement failed (%s)" % type(exc).__name__
            raise FlightCtlError(msg) from None

    # -- Flights ---------------------------------------------------------------------------------------------
    def list_flights(self, prefix: str = NAME_PREFIX) -> list[dict[str, Any]]:
        return self.query(build_list_flights_sql(prefix))

    def find_flight(self, name: str) -> dict[str, Any] | None:
        check_name(name)
        matches = [f for f in self.list_flights(name) if f["flight_name"] == name]
        if len(matches) > 1:
            raise FlightCtlError("%d Flights are named %r; use the id" % (len(matches), name))
        return matches[0] if matches else None

    def get_flight(self, flight_id: str | uuid.UUID) -> dict[str, Any]:
        rows = self.query(build_get_flight_sql(flight_id))
        if not rows:
            raise FlightCtlError("Flight not found: %s" % flight_id)
        return rows[0]

    def resolve(self, ref: str) -> dict[str, Any]:
        """A Flight by id or by name; refuses Flights whose name does not start with cdc_v2."""
        try:
            fid = parse_flight_id(ref)
        except FlightCtlError:
            flight = self.find_flight(ref)
            if flight is None:
                raise FlightCtlError("no Flight named %r" % ref) from None
            return flight
        flight = self.get_flight(fid)
        if not str(flight.get("flight_name", "")).startswith(NAME_PREFIX):
            raise FlightCtlError("Flight %s is not a %s* Flight; refusing to touch it" % (fid, NAME_PREFIX))
        return flight

    def create(self, name: str, source_code: str, **kwargs: Any) -> dict[str, Any]:
        return self.query(build_create_sql(name, source_code, **kwargs))[0]

    def update(self, ref: str, **fields: Any) -> dict[str, Any]:
        flight = self.resolve(ref)
        return self.query(build_update_sql(flight["flight_id"], **fields))[0]

    def deploy(self, name: str, source_code: str, **kwargs: Any) -> tuple[str, dict[str, Any]]:
        """Create the Flight, or update it in place if one with this name exists. Returns (action, summary)."""
        existing = self.find_flight(name)
        if existing is None:
            return "created", self.create(name, source_code, **kwargs)
        # PATCH: omitted fields (including the schedule) stay as they are.
        fields = dict(kwargs)
        fields["source_code"] = source_code
        if "secret_names" in fields:
            fields["flight_secret_names"] = fields.pop("secret_names")
        if fields.get("schedule_cron") == "" and not existing.get("schedule_cron"):
            fields.pop("schedule_cron")  # clearing an absent schedule is an error on the server
        return "updated", self.query(build_update_sql(existing["flight_id"], **fields))[0]

    def unschedule(self, ref: str) -> dict[str, Any]:
        flight = self.resolve(ref)
        if not flight.get("schedule_cron"):
            return flight  # clearing an absent schedule is an error on the server
        return self.query(build_update_sql(flight["flight_id"], schedule_cron=""))[0]

    def delete(self, ref: str) -> dict[str, Any]:
        flight = self.resolve(ref)
        self.query(build_delete_sql(flight["flight_id"]))
        return {"deleted": True, "flight_id": flight["flight_id"], "flight_name": flight["flight_name"]}

    def versions(self, ref: str, limit: int = 50) -> list[dict[str, Any]]:
        return self.query(build_list_versions_sql(self.resolve(ref)["flight_id"], limit))

    def version(self, ref: str, number: int) -> dict[str, Any]:
        return self.query(build_get_version_sql(self.resolve(ref)["flight_id"], number))[0]

    # -- Runs ------------------------------------------------------------------------------------------------
    def run(self, ref: str, config: Mapping[str, str] | None = None) -> dict[str, Any]:
        return self.query(build_run_sql(self.resolve(ref)["flight_id"], config))[0]

    def runs(self, ref: str, limit: int = 50) -> list[dict[str, Any]]:
        return self.query(build_list_runs_sql(self.resolve(ref)["flight_id"], limit))

    def get_run(self, ref: str, run_number: int) -> dict[str, Any]:
        return self.query(build_get_run_sql(self.resolve(ref)["flight_id"], run_number))[0]

    def cancel(self, ref: str, run_number: int) -> dict[str, Any]:
        flight = self.resolve(ref)
        return self.cancel_by_id(flight["flight_id"], run_number)

    def cancel_by_id(self, flight_id: str | uuid.UUID, run_number: int) -> dict[str, Any]:
        """For the leader guard: cancel a run of a known Flight id without a name lookup."""
        rows = self.query(build_cancel_sql(flight_id, run_number))
        return rows[0] if rows else {"cancelled": True, "flight_id": str(flight_id), "run_number": run_number}

    def active_runs(self, flight_id: str | uuid.UUID, limit: int = 50) -> list[dict[str, Any]]:
        return [r for r in self.query(build_list_runs_sql(flight_id, limit)) if run_is_active(r)]

    def logs(self, ref: str, run_number: int, limit: int | None = None, offset: int | None = None,
             order: str | None = None) -> list[dict[str, Any]]:
        rows = self.query(build_logs_sql(self.resolve(ref)["flight_id"], run_number, limit, offset, order))
        return sorted(rows, key=lambda r: (r.get("line_number") is None, r.get("line_number") or 0))

    def wait(self, ref: str, run_number: int, timeout: float = 600.0, poll: float = 2.0,
             on_change: Any = None) -> dict[str, Any]:
        """Poll until the run reaches a terminal state; returns the run with `observed_states` added."""
        flight = self.resolve(ref)
        deadline = time.monotonic() + timeout
        seen: list[dict[str, Any]] = []
        while True:
            run = self.query(build_get_run_sql(flight["flight_id"], run_number))[0]
            state = normalize_status(run.get("status"))
            if not seen or seen[-1]["state"] != state:
                seen.append({"state": state, "observed_at": _dt.datetime.now(_dt.timezone.utc).isoformat()})
                if on_change:
                    on_change(run)
            if state in TERMINAL_STATES:
                run["observed_states"] = seen
                return run
            if time.monotonic() > deadline:
                raise FlightCtlError("run %s did not finish within %.0f s (last state %s)" % (run_number, timeout,
                                                                                               state))
            time.sleep(poll)

    # -- Secrets ---------------------------------------------------------------------------------------------
    def create_secret(self, name: str, params: Mapping[str, str]) -> dict[str, Any]:
        self.query(build_create_secret_sql(name, params), quiet_sql=True)
        return {"secret": name, "keys": sorted(params)}

    def drop_secret(self, name: str) -> dict[str, Any]:
        self.query(build_drop_secret_sql(name))
        return {"secret": name, "dropped": True}

    def list_secrets(self) -> list[dict[str, Any]]:
        return self.query(build_list_secrets_sql())
