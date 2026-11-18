"""The one configuration file of a deployment (task S section 1; contract C0).

    cfg = config.load("config/deployment.toml", os.environ)     # -> Config, one frozen dataclass per package

`load` is the only code that reads the file. The format is TOML (read with the standard library), one table per
package's settings, named as the design names them (`apply.max_group_delay`, `stream.marker_timeout`,
`supervisor.restart_after`, `run.max_runtime`, `leader.takeover_timeout`, …). `docs/configuration.md` lists every
key with its default, its unit and what it serves.

Spellings (one each):

- a **duration** is a string of a number and a unit, `ms`, `s`, `m` or `h`, and the parts may follow each other:
  `"250ms"`, `"30s"`, `"7h30m"`. A plain number is refused.
- a **size** is a string of an integer and a unit, `B`, `KiB`, `MiB` or `GiB`: `"512MiB"`. A plain number is
  refused.
- a **count** is a TOML integer; a **switch** a TOML boolean; a **name** a TOML string.

No secret is in the file. A password is named by the environment variable that holds it (`password_env`); the
loader hands package P a callable that reads the variable when it connects, never the value. Values that must not
be committed (host, port, database, user of a source node) may be named the same way (`host_env`, `port_env`,
`dbname_env`, `user_env`). An unknown key, a wrong type, a value out of range, a missing variable, or an order that
the design demands and the file breaks is refused at load with `ConfigError`, whose message names the key and never
holds the value of a variable.
"""
from __future__ import annotations

import dataclasses
import os
import re
import tomllib
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from cdc_v2 import md, pg
from cdc_v2.apply import ApplySettings
from cdc_v2.stream.engine import EngineSettings
from cdc_v2.stream.settings import GroupSettings, StreamSettings
from cdc_v2.supervisor import SupervisorSettings
from cdc_v2.transform import TransformSettings


class ConfigError(ValueError):
    """The configuration file is refused. The message names the key; it never holds a secret."""


_DUR = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|h)")
_SIZE = re.compile(r"^(\d+)(B|KiB|MiB|GiB)$")
_UNITS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}
_SIZES = {"B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}
_ENV = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def duration(text, key) -> float:
    if not isinstance(text, str) or not text:
        raise ConfigError("%s: a duration is a string such as \"30s\" or \"7h30m\"" % key)
    pos, total = 0, 0.0
    for m in _DUR.finditer(text):
        if m.start() != pos:
            break
        total += float(m.group(1)) * _UNITS[m.group(2)]
        pos = m.end()
    if pos != len(text):
        raise ConfigError("%s: a duration is a number and a unit (ms, s, m, h), such as \"30s\" or \"7h30m\"" % key)
    return total


def size(text, key) -> int:
    m = _SIZE.match(text) if isinstance(text, str) else None
    if m is None:
        raise ConfigError("%s: a size is an integer and a unit (B, KiB, MiB, GiB), such as \"512MiB\"" % key)
    return int(m.group(1)) * _SIZES[m.group(2)]


def _count(v, key):
    if isinstance(v, bool) or not isinstance(v, int):
        raise ConfigError("%s: an integer" % key)
    return v


def _switch(v, key):
    if not isinstance(v, bool):
        raise ConfigError("%s: true or false" % key)
    return v


def _name(v, key):
    if not isinstance(v, str):
        raise ConfigError("%s: a string" % key)
    return v


def _names(v, key):
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        raise ConfigError("%s: a list of strings" % key)
    return tuple(v)


def _ms(v, key):
    return int(round(duration(v, key) * 1000))


KINDS = {"duration": duration, "size": size, "count": _count, "switch": _switch, "name": _name, "names": _names,
         "ms": _ms, "mib": lambda v, k: size(v, k) // (1 << 20), "sizestr": lambda v, k: "%dB" % size(v, k)}

# ------------------------------------------------------------------------------------------------ the stand-ins


@dataclass(frozen=True)
class RunSettings:
    """`run.*`: the pipeline process (package S) and the run's time limit (SPEC 4.2, PLAN §7)."""
    max_runtime_s: float = 7.5 * 3600            # 50 min on a Lite plan, 1 h during the soak
    work_dir: str = ""                           # local working directory of the run ('' = a new temporary one)
    spill_limit: int = 8 << 30                   # the run's one spill area: transform 1/4, apply's planning 3/4
    jvm_cache_dir: str = ""                      # JRE, jars and compiled classes ('' = ~/.cache/cdc_flight_v2)
    stop_timeout_s: float = 30.0                 # SIGTERM: the group in flight is finished within this

    def __post_init__(self):
        if not self.max_runtime_s > 0:
            raise ValueError("run.max_runtime must be > 0")
        if self.spill_limit < (64 << 20):
            raise ValueError("run.spill_limit must be at least 64MiB")
        if not self.stop_timeout_s > 0:
            raise ValueError("run.stop_timeout must be > 0")


@dataclass(frozen=True)
class LeaderSettings:
    """`leader.*`: the timings of design 4 (package L owns the protocol; this dataclass is the stand-in that the
    pipeline fills until L's own replaces it). Their order is checked: interval < stale age / 3, stale age <
    hard-exit age."""
    heartbeat_interval_s: float = 10.0
    stale_age_s: float = 30.0
    hard_exit_age_s: float = 60.0
    takeover_timeout_s: float = 45.0
    startup_timeout_s: float = 180.0

    def __post_init__(self):
        for f in dataclasses.fields(self):
            if not getattr(self, f.name) > 0:
                raise ValueError("leader.%s must be > 0" % f.name[:-2])
        if not self.heartbeat_interval_s < self.stale_age_s / 3:
            raise ValueError("leader.heartbeat_interval must be shorter than leader.stale_age / 3")
        if not self.stale_age_s < self.hard_exit_age_s:
            raise ValueError("leader.stale_age must be shorter than leader.hard_exit_age")


# ------------------------------------------------------------------------------------------------ the key tables

#: section -> {key: (field of the dataclass, kind)}. A field that no key fills is derived (`DERIVED`).
SECTIONS = {
    "destination": (md.Settings, {
        "database": ("database", "name"), "marker": ("marker", "name"),
        "connect_deadline": ("connect_deadline_s", "duration"), "connect_attempts": ("connect_attempts", "count"),
        "connect_retry_pause": ("connect_retry_pause_s", "duration"),
        "statement_deadline": ("statement_deadline_s", "duration"),
        "transaction_deadline": ("transaction_deadline_s", "duration"),
        "rollback_deadline": ("rollback_deadline_s", "duration"), "grace": ("grace_s", "duration"),
        "interrupt_deadline": ("interrupt_deadline_s", "duration"),
        "max_waiting_calls": ("max_waiting_calls", "count")}),
    "source": (pg.SourceSettings, {
        "publication": ("publication", "name"), "slot": ("slot", "name"),
        "heartbeat_schema": ("heartbeat_schema", "name"), "heartbeat_table": ("heartbeat_table", "name")}),
    "source.nodes.*": (pg.NodeSettings, {
        "host": ("host", "name"), "port": ("port", "count"), "dbname": ("dbname", "name"), "user": ("user", "name"),
        "tls": ("tls", "name"), "sslrootcert": ("sslrootcert", "name"),
        "application_name": ("application_name", "name"),
        "connect_timeout": ("connect_timeout_s", "duration"), "statement_timeout": ("statement_timeout_ms", "ms"),
        "lock_timeout": ("lock_timeout_ms", "ms"),
        "idle_in_transaction_session_timeout": ("idle_in_transaction_session_timeout_ms", "ms"),
        "keepalives_idle": ("keepalives_idle_s", "count"), "keepalives_interval": ("keepalives_interval_s", "count"),
        "keepalives_count": ("keepalives_count", "count"), "tcp_user_timeout": ("tcp_user_timeout_ms", "ms"),
        "cancel_grace": ("cancel_grace_s", "duration")}),
    "engine": (EngineSettings, {
        "heap": ("heap_mb", "mib"), "gc": ("gc", "name"), "active_processors": ("active_processors", "count"),
        "max_batch_size": ("max_batch_size", "count"), "max_queue_size": ("max_queue_size", "count"),
        "max_queue_bytes": ("max_queue_bytes", "size"), "poll_interval": ("poll_interval_ms", "ms"),
        "record_processing_threads": ("record_processing_threads", "count"),
        "heartbeat_interval": ("heartbeat_interval_ms", "ms"),
        "status_update_interval": ("status_update_interval_ms", "ms"),
        "message_prefixes": ("message_prefixes", "names"), "frame_max_rows": ("frame_max_rows", "count"),
        "frame_target_bytes": ("frame_target_bytes", "size"), "frame_max_bytes": ("frame_max_bytes", "size"),
        "max_value_bytes": ("max_value_bytes", "size"), "ack_max_pending": ("ack_max_pending", "count"),
        "stop_timeout": ("stop_timeout_ms", "ms"), "stat_interval": ("stat_interval_ms", "ms")}),
    "transform": (TransformSettings, {
        "threads": ("threads", "count"), "memory_limit": ("memory_limit", "sizestr"),
        "max_cached_queries": ("max_cached_queries", "count")}),
    "apply": (ApplySettings, {
        "statement_rows": ("statement_rows", "count"), "local_threads": ("local_threads", "count"),
        "local_memory_limit": ("local_memory_limit", "sizestr"), "check_new_keys": ("check_new_keys", "switch")}),
    "apply.group": (GroupSettings, {
        "max_group_delay": ("max_group_delay_s", "duration"), "max_group_bytes": ("max_group_bytes", "size"),
        "max_group_attempts": ("max_group_attempts", "count"), "retry_pause": ("retry_pause_s", "duration"),
        "retry_pause_max": ("retry_pause_max_s", "duration"), "max_table_attempts": ("max_table_attempts", "count"),
        "commit_timeout_base": ("commit_timeout_base_s", "duration"),
        "commit_timeout_per_million_rows": ("commit_timeout_per_million_rows_s", "duration"),
        "stage_chunk_bytes": ("stage_chunk_bytes", "size")}),
    "stream": (StreamSettings, {
        "node": ("node", "name"), "frame_silence": ("frame_silence_s", "duration"),
        "marker_timeout": ("marker_timeout_s", "duration"), "heartbeat_interval": ("heartbeat_interval_s", "duration"),
        "idle_position_interval": ("idle_position_interval_s", "duration"),
        "max_attempts_at_position": ("max_attempts_at_position", "count"),
        "max_key_restarts": ("max_key_restarts", "count"), "attach_timeout": ("attach_timeout_s", "duration"),
        "unreachable_wait": ("unreachable_wait_s", "duration"),
        "unreachable_pause": ("unreachable_pause_s", "duration"), "engine_stop": ("engine_stop_s", "duration"),
        "request_poll": ("request_poll_s", "duration"), "alert_flush": ("alert_flush_s", "duration"),
        "source_statement": ("source_statement_s", "duration")}),
    "supervisor": (SupervisorSettings, {
        "restart_after": ("restart_after_s", "duration"), "max_restarts": ("max_restarts", "count"),
        "restart_window": ("restart_window_s", "duration"), "join_timeout": ("join_timeout_s", "duration"),
        "check_interval": ("check_interval_s", "duration"), "max_waiting_calls": ("max_waiting_calls", "count")}),
    "run": (RunSettings, {
        "max_runtime": ("max_runtime_s", "duration"), "work_dir": ("work_dir", "name"),
        "spill_limit": ("spill_limit", "size"), "jvm_cache_dir": ("jvm_cache_dir", "name"),
        "stop_timeout": ("stop_timeout_s", "duration")}),
    "leader": (LeaderSettings, {
        "heartbeat_interval": ("heartbeat_interval_s", "duration"), "stale_age": ("stale_age_s", "duration"),
        "hard_exit_age": ("hard_exit_age_s", "duration"), "takeover_timeout": ("takeover_timeout_s", "duration"),
        "startup_timeout": ("startup_timeout_s", "duration")}),
}

#: Fields that no key fills, and where they come from (a test checks that every field is a key or listed here).
DERIVED = {
    ("source", "nodes"): "the tables [source.nodes.<name>]",
    ("source.nodes.*", "password"): "password_env: the environment variable that holds it (read when connecting)",
    ("engine", "host"): "the source node stream.node", ("engine", "port"): "the source node stream.node",
    ("engine", "dbname"): "the source node stream.node", ("engine", "user"): "the source node stream.node",
    ("engine", "password"): "the source node stream.node (read when the engine starts)",
    ("engine", "slot"): "source.slot", ("engine", "publication"): "source.publication",
    ("engine", "sslmode"): "the source node's tls", ("engine", "sslrootcert"): "the source node's sslrootcert",
    ("engine", "table_include"): "set by the stream at each engine start (tables left out)",
    ("engine", "keyless_tables"): "read from the catalog at each engine start (rule K)",
    ("engine", "extra"): "not configurable (protected properties, docs/design/engine.md 4)",
    ("transform", "temp_directory"): "run.work_dir/spill/transform", ("transform", "max_temp_directory_size"):
        "a quarter of run.spill_limit",
    ("apply", "local_temp_directory"): "run.work_dir/spill/apply", ("apply", "local_max_temp_directory_size"):
        "three quarters of run.spill_limit",
    ("stream", "slot"): "source.slot",
}

_NODE_ENV_KEYS = ("host", "port", "dbname", "user")


@dataclass(frozen=True)
class Config:
    destination: md.Settings
    source: pg.SourceSettings
    engine: EngineSettings                 # password empty: the pipeline reads it from the node when the engine starts
    transform: TransformSettings
    apply: ApplySettings
    group: GroupSettings
    stream: StreamSettings
    supervisor: SupervisorSettings
    run: RunSettings
    leader: LeaderSettings


def _section(doc: dict, name: str) -> dict:
    cur: Any = doc
    for part in name.split("."):
        cur = cur.get(part, {}) if isinstance(cur, dict) else {}
    if not isinstance(cur, dict):
        raise ConfigError("[%s] must be a table" % name)
    return cur


def _fill(section: str, table: dict, *, skip=(), extra: Optional[dict] = None) -> dict:
    """Keyword arguments of the section's dataclass from the file's table; refuses unknown keys and bad values."""
    cls, keys = SECTIONS[section]
    out = dict(extra or {})
    for k, v in table.items():
        if k in skip:
            continue
        if k not in keys:
            raise ConfigError("%s.%s: unknown key" % (section, k))
        field, kind = keys[k]
        out[field] = KINDS[kind](v, "%s.%s" % (section, k))
    return out


def _build(section: str, kwargs: dict):
    cls = SECTIONS[section][0]
    try:
        return cls(**kwargs)
    except (ValueError, TypeError) as e:
        raise ConfigError("[%s]: %s" % (section, _scrub(str(e)))) from None


def _scrub(text: str) -> str:
    return re.sub(r"'[^']*'|\"[^\"]*\"", "<value>", text)[:300]


def _env_value(environ: Mapping, var: str, key: str) -> str:
    if not isinstance(var, str) or not _ENV.match(var):
        raise ConfigError("%s: the name of an environment variable" % key)
    v = environ.get(var)
    if v is None or v == "":
        raise ConfigError("%s: the environment variable %s is not set" % (key, var))
    return v


def _node(name: str, table: dict, environ: Mapping) -> pg.NodeSettings:
    sec = "source.nodes.*"
    key0 = "source.nodes.%s" % name
    kwargs = {}
    plain = {}
    for k, v in table.items():
        if k.endswith("_env"):
            base = k[:-4]
            if base == "password":
                _env_value(environ, v, "%s.password_env" % key0)        # present now; read again when connecting
                kwargs["password"] = pg.Password(fn=lambda env=environ, var=v: env.get(var) or "")
                continue
            if base not in _NODE_ENV_KEYS:
                raise ConfigError("%s.%s: unknown key" % (key0, k))
            if base in table:
                raise ConfigError("%s.%s: give %s or %s, not both" % (key0, k, base, k))
            raw = _env_value(environ, v, "%s.%s" % (key0, k))
            if base == "port":
                if not raw.isdigit():
                    raise ConfigError("%s.%s: the variable %s does not hold a port number" % (key0, k, v))
                raw = int(raw)
            plain[base] = raw
        elif k == "password":
            raise ConfigError("%s.password: no secret in the file; name its variable with password_env" % key0)
    kwargs.update(_fill(sec, {k: v for k, v in table.items() if not k.endswith("_env")}))
    kwargs.update(plain)
    try:
        return pg.NodeSettings(**kwargs)
    except (ValueError, TypeError) as e:
        raise ConfigError("[%s]: %s" % (key0, _scrub(str(e)))) from None


def load(path, environ: Mapping = None) -> Config:
    """Read the configuration file at `path` and fill every package's settings. `environ` gives the variables that
    keys ending in `_env` name (default: the process environment)."""
    environ = os.environ if environ is None else environ
    try:
        with open(path, "rb") as f:
            doc = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError("the configuration file is not valid TOML (%s)" % _scrub(str(e))) from None
    return from_dict(doc, environ)


def from_dict(doc: dict, environ: Mapping) -> Config:
    known = {"destination", "source", "engine", "transform", "apply", "stream", "supervisor", "run", "leader"}
    for k in doc:
        if k not in known:
            raise ConfigError("[%s]: unknown table" % k)
    dest = _section(doc, "destination")
    if "database" not in dest:
        raise ConfigError("destination.database: required")
    destination = _build("destination", _fill("destination", dest))

    src = _section(doc, "source")
    nodes_t = src.get("nodes", {})
    if not isinstance(nodes_t, dict) or not nodes_t:
        raise ConfigError("source.nodes: at least the node 'primary'")
    nodes = {}
    for name, t in nodes_t.items():
        if not isinstance(t, dict):
            raise ConfigError("source.nodes.%s: must be a table" % name)
        nodes[name] = _node(name, t, environ)
    source = _build("source", _fill("source", src, skip=("nodes",), extra={"nodes": nodes}))

    stream_t = _section(doc, "stream")
    stream = _build("stream", _fill("stream", stream_t, extra={"slot": source.slot}))
    if stream.node not in source.nodes:
        if "node" in stream_t:
            raise ConfigError("stream.node: no source node of that name")
        stream = dataclasses.replace(stream, node="primary")

    run = _build("run", _fill("run", _section(doc, "run")))
    leader = _build("leader", _fill("leader", _section(doc, "leader")))
    supervisor = _build("supervisor", _fill("supervisor", _section(doc, "supervisor")))

    node = source.nodes[stream.node]
    eng = _fill("engine", _section(doc, "engine"))
    engine = _build("engine", dict(eng, host=node.host, port=node.port, dbname=node.dbname, user=node.user,
                                   password="", slot=source.slot, publication=source.publication,
                                   sslmode="verify-full" if node.tls == "verify-full" else "disable",
                                   sslrootcert=node.sslrootcert))

    work = run.work_dir or ""
    transform = _build("transform", _fill("transform", _section(doc, "transform"), extra={
        "temp_directory": os.path.join(work, "spill", "transform") if work else None,
        "max_temp_directory_size": "%dB" % (run.spill_limit // 4)}))
    apply_t = _section(doc, "apply")
    apply_s = _build("apply", _fill("apply", {k: v for k, v in apply_t.items()
                                              if k in SECTIONS["apply"][1]}, extra={
        "local_temp_directory": os.path.join(work, "spill", "apply") if work else None,
        "local_max_temp_directory_size": "%dB" % (run.spill_limit - run.spill_limit // 4)}))
    group = _build("apply.group", _fill("apply.group", {k: v for k, v in apply_t.items()
                                                        if k not in SECTIONS["apply"][1]}))
    # orders across packages that the design demands
    if stream.attach_timeout_s < leader.hard_exit_age_s + 15:
        raise ConfigError("stream.attach_timeout: at least leader.hard_exit_age + 15 s (design 8: the previous run "
                          "can take the slot back until its hard exit)")
    if stream.heartbeat_interval_s >= stream.marker_timeout_s:
        raise ConfigError("stream.heartbeat_interval: shorter than stream.marker_timeout")
    if supervisor.restart_after_s <= stream.frame_silence_s:
        raise ConfigError("supervisor.restart_after: longer than stream.frame_silence")
    if engine.heartbeat_interval_ms / 1000.0 >= stream.frame_silence_s:
        raise ConfigError("engine.heartbeat_interval: shorter than stream.frame_silence (its HEARTBEAT frames show "
                          "that the engine is alive)")
    return Config(destination, source, engine, transform, apply_s, group, stream, supervisor, run, leader)


def documented_keys() -> list:
    """Every key the file can hold, as `section.key` (a test compares it with docs/configuration.md)."""
    out = []
    for section, (_cls, keys) in SECTIONS.items():
        shown = "apply" if section == "apply.group" else section.replace(".*", ".<name>")
        out += ["%s.%s" % (shown, k) for k in keys]
    out += ["source.nodes.<name>.%s_env" % k for k in _NODE_ENV_KEYS + ("password",)]
    return out


__all__ = ["Config", "ConfigError", "load", "from_dict", "RunSettings", "LeaderSettings", "SECTIONS", "DERIVED",
           "documented_keys", "duration", "size", "render_document"]


# ------------------------------------------------------------------------------------------------ the document

#: What each key serves (SPEC item or design section). Unit and default come from the key tables and dataclasses.
SERVES = {
    "destination.database": "the MotherDuck database (C4; `cdc_v2_*` names in tests)",
    "destination.marker": "custom user agent of every connection (C4)",
    "destination.connect_deadline": "one connect attempt (SPEC 4.5; destination 4)",
    "destination.connect_attempts": "connect attempts inside the caller's deadline (destination 2.1)",
    "destination.connect_retry_pause": "pause between connect attempts (destination 2.1)",
    "destination.statement_deadline": "one statement, the fence and the registration (SPEC 4.5; destination 4)",
    "destination.transaction_deadline": "a lane block without its own deadline (destination 4; C11)",
    "destination.rollback_deadline": "the clean-up after a failed step (destination 3)",
    "destination.grace": "wait after an interrupt before a connection is given up (destination 4)",
    "destination.interrupt_deadline": "one call of the server's interrupt (destination 4)",
    "destination.max_waiting_calls": "calls a connection gave up that may still wait (destination 4)",
    "source.publication": "our publication (C7; SPEC 2.3)",
    "source.slot": "our replication slot (C7; design 8)",
    "source.heartbeat_schema": "schema of the published heartbeat table (SPEC 4.4; source 5)",
    "source.heartbeat_table": "the published heartbeat table (SPEC 4.4; source 5)",
    "source.nodes.<name>.host": "the node's host (prefer host_env: never committed, SPEC R23)",
    "source.nodes.<name>.port": "the node's port; always explicit (PLAN §9)",
    "source.nodes.<name>.dbname": "the source database",
    "source.nodes.<name>.user": "the runtime role (source 6)",
    "source.nodes.<name>.tls": "verify-full, or disable for a host on this machine only (C7)",
    "source.nodes.<name>.sslrootcert": "CA file for verify-full, or \"system\"",
    "source.nodes.<name>.application_name": "application_name of the node's SQL sessions (C7)",
    "source.nodes.<name>.connect_timeout": "libpq connect timeout (source 1)",
    "source.nodes.<name>.statement_timeout": "server-side bound behind the client deadlines (SPEC 4.5)",
    "source.nodes.<name>.lock_timeout": "server-side lock bound (SPEC 4.5)",
    "source.nodes.<name>.idle_in_transaction_session_timeout": "no source lock held across a network wait "
                                                               "(SPEC 4.5)",
    "source.nodes.<name>.keepalives_idle": "TCP keepalive idle seconds (SPEC 4.6)",
    "source.nodes.<name>.keepalives_interval": "TCP keepalive interval seconds (SPEC 4.6)",
    "source.nodes.<name>.keepalives_count": "TCP keepalive probes (SPEC 4.6)",
    "source.nodes.<name>.tcp_user_timeout": "bound of unacknowledged sent data, Linux (SPEC 4.6)",
    "source.nodes.<name>.cancel_grace": "grace after a cancel before the socket is shut (source 1)",
    "source.nodes.<name>.host_env": "the variable that holds the host",
    "source.nodes.<name>.port_env": "the variable that holds the port",
    "source.nodes.<name>.dbname_env": "the variable that holds the database name",
    "source.nodes.<name>.user_env": "the variable that holds the user name",
    "source.nodes.<name>.password_env": "the variable that holds the password (read when connecting; never in the "
                                        "file)",
    "engine.heap": "JVM heap cap (SPEC 5.4; engine 4)",
    "engine.gc": "JVM garbage collector, parallel or g1 (engine 4)",
    "engine.active_processors": "-XX:ActiveProcessorCount (the F16 Flight: 2)",
    "engine.max_batch_size": "Debezium max.batch.size (engine 4; SPEC 5.1)",
    "engine.max_queue_size": "Debezium max.queue.size (SPEC 5.4)",
    "engine.max_queue_bytes": "Debezium max.queue.size.in.bytes (SPEC 5.4)",
    "engine.poll_interval": "Debezium poll.interval.ms",
    "engine.record_processing_threads": "Debezium record.processing.threads",
    "engine.heartbeat_interval": "Debezium heartbeat.interval.ms: HELLO on an idle source, DURABLE taking effect "
                                 "(C2)",
    "engine.status_update_interval": "Debezium status.update.interval.ms",
    "engine.message_prefixes": "logical message prefixes delivered (SPEC 7.4)",
    "engine.frame_max_rows": "rows per ROWS frame (engine 2)",
    "engine.frame_target_bytes": "target size of a ROWS frame (engine 2)",
    "engine.frame_max_bytes": "largest frame the reader accepts (C2; SPEC 5.4)",
    "engine.max_value_bytes": "largest single value (engine 8; H-002)",
    "engine.ack_max_pending": "END records the engine holds for acknowledgement (engine 5)",
    "engine.stop_timeout": "STOP of the JVM (engine 7)",
    "engine.stat_interval": "the engine's statistics lines",
    "transform.threads": "threads of the transform's DuckDB (C3)",
    "transform.memory_limit": "memory limit of the transform's DuckDB (SPEC 5.4)",
    "transform.max_cached_queries": "compiled conversion statements kept (transform 3)",
    "apply.statement_rows": "rows per statement from local data (apply 3.2; design 6.1)",
    "apply.local_threads": "threads of apply's planning DuckDB (apply 2.7)",
    "apply.local_memory_limit": "memory limit of apply's planning DuckDB (SPEC 5.4)",
    "apply.check_new_keys": "stream mode checks that a new key is free (apply 2.5)",
    "apply.max_group_delay": "a group closes this long after its first record (design 3; SPEC 5.2)",
    "apply.max_group_bytes": "the memory one group may hold (design 6.1; SPEC 5.4)",
    "apply.max_group_attempts": "runs of one group after Conflict, ConnectionLost or a deadline (design 3; C10)",
    "apply.retry_pause": "first pause between runs of a group (destination 8.3)",
    "apply.retry_pause_max": "longest pause between runs of a group",
    "apply.max_table_attempts": "failures of one table before it leaves the stream (SPEC 4.0; C10)",
    "apply.commit_timeout_base": "commit deadline: base (design 3)",
    "apply.commit_timeout_per_million_rows": "commit deadline: per million rows (design 3; SPEC 5.1)",
    "apply.stage_chunk_bytes": "one staging chunk of a transaction larger than a group (design 6.4)",
    "stream.node": "the source node the engine decodes from (SPEC R17: a replica)",
    "stream.frame_silence": "no frame while reading: the engine is torn down (SPEC 4.6; engine 6)",
    "stream.marker_timeout": "a beat must come back within this (SPEC 4.6)",
    "stream.heartbeat_interval": "the source-side heartbeat write (SPEC 4.4)",
    "stream.idle_position_interval": "at most one commit per this when only heartbeats arrive (SPEC 4.4)",
    "stream.max_attempts_at_position": "starts at one position before the last line (SPEC 4.3)",
    "stream.max_key_restarts": "engine restarts for a key that differs from the catalog (rule K)",
    "stream.attach_timeout": "the attach step's declared wait (design 8)",
    "stream.unreachable_wait": "retries against an unreachable source or destination in one declared wait "
                               "(SPEC 4.7)",
    "stream.unreachable_pause": "first pause of those retries (doubles to 30 s)",
    "stream.engine_stop": "STOP of the engine at a planned stop (C12)",
    "stream.request_poll": "the stand-in of backfill.on_group reads requests this often (C10)",
    "stream.alert_flush": "alerts of other components wait at most this for a stream transaction (SPEC 6.2)",
    "stream.source_statement": "one source statement of the stream (SPEC 4.5)",
    "supervisor.restart_after": "a silent component is restarted (SPEC 4.5)",
    "supervisor.max_restarts": "restarts of one component in the window before the run ends (C11)",
    "supervisor.restart_window": "the window of max_restarts",
    "supervisor.join_timeout": "a restarted component's thread must return within this (C11)",
    "supervisor.check_interval": "how often the rules run",
    "supervisor.max_waiting_calls": "destination calls left waiting before the run ends (C11; destination 4)",
    "run.max_runtime": "planned hand-over before the platform's limit (SPEC 4.2; PLAN §7)",
    "run.work_dir": "local working directory of the run; the spill area is under it",
    "run.spill_limit": "the run's one spill area (SPEC 5.4): transform 1/4, apply 3/4",
    "run.jvm_cache_dir": "JRE, jars and engine classes (artifacts)",
    "run.stop_timeout": "SIGTERM: the group in flight is finished within this (C12)",
    "leader.heartbeat_interval": "leader heartbeat (SPEC 4.2; design 4.3)",
    "leader.stale_age": "a heartbeat older than this is not healthy (SPEC 4.2)",
    "leader.hard_exit_age": "no tick for this long: the bootstrap kills the run (SPEC 4.2; design 5)",
    "leader.takeover_timeout": "the take-over gives up after this (design 4.5)",
    "leader.startup_timeout": "start-up of the run (design 5)",
}

UNIT = {"duration": "duration", "ms": "duration", "size": "size", "mib": "size", "sizestr": "size", "count": "count",
        "switch": "switch", "name": "name", "names": "list of names"}


def _default(section, field):
    for f in dataclasses.fields(SECTIONS[section][0]):
        if f.name == field:
            return None if f.default is dataclasses.MISSING else f.default
    return None


def _show_default(kind, v):
    if v is None or v == "":
        return "–"
    if kind == "duration":
        return "%g s" % v
    if kind == "ms":
        return "%g ms" % v
    if kind == "size":
        return "%d MiB" % (v >> 20) if v % (1 << 20) == 0 else "%d B" % v
    if kind == "mib":
        return "%d MiB" % v
    if isinstance(v, tuple):
        return ", ".join("`%s`" % x for x in v)
    return str(v)


def render_document() -> str:
    """docs/configuration.md, generated from the key tables (`uv run python -m cdc_v2.config > docs/configuration.md`)."""
    lines = [
        "# Configuration",
        "",
        "Generated from `src/cdc_v2/config.py` (`uv run python -m cdc_v2.config > docs/configuration.md`); a test "
        "fails when the two differ, and when a settings field is neither a key here nor listed as derived. "
        "`config/example.toml` sets every key.",
        "",
        "One TOML file configures a deployment; `cdc_v2.config.load(path, environ)` reads it and fills the settings "
        "dataclass of every package. Spellings: a **duration** is a number and a unit (`ms`, `s`, `m`, `h`), and the "
        "parts may follow each other (`\"7h30m\"`); a **size** is an integer and a unit (`B`, `KiB`, `MiB`, `GiB`); a "
        "**count** is an integer; a **switch** is true or false. No secret is in the file: a password is named by the "
        "environment variable that holds it (`password_env`), and so may a node's host, port, database and user "
        "(`*_env`). An unknown key, a wrong type, a value out of range or a broken order is refused at load, with a "
        "message that names the key and never a variable's value.",
        "",
        "Orders checked: `leader.heartbeat_interval` < `leader.stale_age` / 3 and `leader.stale_age` < "
        "`leader.hard_exit_age`; `stream.attach_timeout` ≥ `leader.hard_exit_age` + 15 s; `stream.heartbeat_interval` "
        "< `stream.marker_timeout`; `engine.heartbeat_interval` < `stream.frame_silence` < "
        "`supervisor.restart_after`; `apply.retry_pause` ≤ `apply.retry_pause_max`; `apply.stage_chunk_bytes` ≤ "
        "`apply.max_group_bytes`.",
        "",
    ]
    for section, (cls, keys) in SECTIONS.items():
        shown = "apply" if section == "apply.group" else section.replace(".*", ".<name>")
        title = "## `[%s]`%s" % (shown, " (the stream's group settings)" if section == "apply.group" else "")
        lines += [title, "", "Fills `%s.%s`." % (cls.__module__, cls.__name__), "",
                  "| Key | Unit | Default | Serves |", "|---|---|---|---|"]
        for k, (field, kind) in keys.items():
            full = "%s.%s" % (shown, k)
            lines.append("| `%s` | %s | %s | %s |" % (full, UNIT[kind], _show_default(kind, _default(section, field)),
                                                     SERVES.get(full, "")))
        if section == "source.nodes.*":
            for k in _NODE_ENV_KEYS + ("password",):
                full = "source.nodes.<name>.%s_env" % k
                lines.append("| `%s` | name of a variable | – | %s |" % (full, SERVES.get(full, "")))
        derived = [(f, why) for (s, f), why in DERIVED.items() if s == section]
        if derived:
            lines += ["", "Derived, not keys: " + "; ".join("`%s` (%s)" % d for d in derived) + "."]
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    sys.stdout.write(render_document())
