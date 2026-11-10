"""Settings of the source side (package P): one frozen dataclass per node, and the fixed session settings.

No setting holds a password as a value. A node's password comes from a :class:`Password`, which reads it from the
environment or from a callable at the moment of connecting and never shows it in ``repr``, ``str`` or an error.
"""
from __future__ import annotations

import ipaddress
import os
import re
from dataclasses import dataclass, field, replace
from typing import Callable, Mapping

# The session settings of every connection (C3, C7). Every value that Postgres prints is then the same text on
# every connection, whatever the role or the database has set (`ALTER ROLE/DATABASE … SET`): these travel in the
# startup packet (source "client"), which outranks role and database settings, and `connect` reads them back and
# refuses a connection on which one differs. The text form of the backfill (package Y) is defined by this table.
#   name -> (value sent, value read back from pg_settings.setting)
FIXED_SESSION_SETTINGS: dict[str, tuple[str, str]] = {
    "TimeZone": ("UTC", "UTC"),
    "DateStyle": ("ISO, YMD", "ISO, YMD"),
    "IntervalStyle": ("iso_8601", "iso_8601"),
    "extra_float_digits": ("3", "3"),
    "bytea_output": ("hex", "hex"),
    "client_encoding": ("UTF8", "UTF8"),
    "lc_monetary": ("C", "C"),
    "standard_conforming_strings": ("on", "on"),
    "search_path": ("pg_catalog, pg_temp", "pg_catalog, pg_temp"),
    "row_security": ("off", "off"),
    "default_transaction_isolation": ("read committed", "read committed"),
}

NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")
_LOCAL_HOSTS = {"localhost"}


class Password:
    """Where a password comes from: an environment variable or a callable. The value is fetched only by
    :meth:`reveal`, at the moment of connecting, and is never kept on this object."""

    __slots__ = ("_env", "_fn")

    def __init__(self, *, env: str | None = None, fn: Callable[[], str] | None = None):
        if (env is None) == (fn is None):
            raise ValueError("a Password needs exactly one of env= or fn=")
        if env is not None and not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", env):
            raise ValueError("bad environment variable name for a password")
        object.__setattr__(self, "_env", env)
        object.__setattr__(self, "_fn", fn)

    def __setattr__(self, *_):
        raise AttributeError("Password is immutable")

    def reveal(self) -> str:
        value = os.environ.get(self._env) if self._env else self._fn()
        if not isinstance(value, str) or not value:
            raise PasswordMissing("the password for this node is not available (%s)"
                                  % ("environment variable %s is empty" % self._env if self._env else "callable"))
        return value

    def __repr__(self):
        return "Password(env=%r)" % self._env if self._env else "Password(fn=<callable>)"

    __str__ = __repr__

    def __eq__(self, other):
        return isinstance(other, Password) and (self._env, self._fn) == (other._env, other._fn)

    def __hash__(self):
        return hash((self._env, id(self._fn)))

    def __reduce__(self):
        raise TypeError("a Password cannot be pickled")


class PasswordMissing(RuntimeError):
    pass


def _is_local(host: str) -> bool:
    if host.startswith("/") or host in _LOCAL_HOSTS:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True)
class NodeSettings:
    """How to reach one Postgres server. ``port`` is always explicit: no code falls back to a default port.

    TLS: ``tls='verify-full'`` (the default) verifies the server certificate against ``sslrootcert`` (or the system
    store when it is ``'system'``). ``tls='disable'`` is allowed only for a host on this machine and only when it is
    set explicitly; there is no silent fall-back.

    Keepalives (client side, libpq): after ``keepalives_idle`` seconds without traffic the kernel sends a probe every
    ``keepalives_interval`` seconds and gives up after ``keepalives_count`` unanswered probes. ``tcp_user_timeout``
    (milliseconds; Linux only, ignored by the kernel elsewhere) bounds how long sent data may stay unacknowledged.
    Neither notices a server that is alive but does not answer, nor a black hole in a proxy whose kernel acknowledges
    the probes (docs/testing/harness.md §7): the statement deadlines do.
    """

    host: str = field(repr=False)
    port: int
    dbname: str
    user: str = field(repr=False)
    password: Password | None = field(default=None, compare=False)
    tls: str = "verify-full"
    sslrootcert: str | None = None
    application_name: str = "cdc_v2"
    connect_timeout_s: float = 10.0
    statement_timeout_ms: int = 30_000
    lock_timeout_ms: int = 5_000
    idle_in_transaction_session_timeout_ms: int = 60_000
    keepalives_idle_s: int = 30
    keepalives_interval_s: int = 10
    keepalives_count: int = 6
    tcp_user_timeout_ms: int = 120_000
    cancel_grace_s: float = 2.0

    def __post_init__(self):
        if not isinstance(self.port, int) or isinstance(self.port, bool) or not 1 <= self.port <= 65535:
            raise ValueError("port must be an explicit integer 1-65535")
        if not self.host or not self.dbname or not self.user:
            raise ValueError("host, dbname and user are required")
        if self.tls not in ("verify-full", "disable"):
            raise ValueError("tls must be 'verify-full' or 'disable'")
        if self.tls == "disable" and not _is_local(self.host):
            raise ValueError("tls='disable' is allowed only for a host on this machine")
        if not NAME_RE.match(self.application_name):
            raise ValueError("application_name must be a plain identifier of at most 63 characters")
        for name in ("statement_timeout_ms", "lock_timeout_ms", "idle_in_transaction_session_timeout_ms",
                     "keepalives_idle_s", "keepalives_interval_s", "keepalives_count", "tcp_user_timeout_ms"):
            v = getattr(self, name)
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise ValueError("%s must be an integer >= 0" % name)
        if not self.connect_timeout_s > 0 or not self.cancel_grace_s > 0:
            raise ValueError("connect_timeout_s and cancel_grace_s must be > 0")

    def with_(self, **kw) -> "NodeSettings":
        return replace(self, **kw)

    def conninfo(self, *, replication: bool = False, connect_timeout: int | None = None) -> dict:
        """Keyword arguments for psycopg.connect, without the password (added at connect time only)."""
        options = " ".join("-c %s=%s" % (k, _opt(v)) for k, v in self.session_settings().items())
        kw = dict(host=self.host, port=self.port, dbname=self.dbname, user=self.user,
                  application_name=self.application_name, options=options, gssencmode="disable",
                  connect_timeout=str(connect_timeout or max(2, int(round(self.connect_timeout_s)))),
                  keepalives="1", keepalives_idle=str(self.keepalives_idle_s),
                  keepalives_interval=str(self.keepalives_interval_s), keepalives_count=str(self.keepalives_count),
                  tcp_user_timeout=str(self.tcp_user_timeout_ms))
        if self.tls == "verify-full":
            kw.update(sslmode="verify-full", sslrootcert=self.sslrootcert or "system")
        else:
            kw.update(sslmode="disable")
        if replication:
            kw["replication"] = "database"
        return kw

    def session_settings(self) -> dict[str, str]:
        out = {k: v for k, (v, _) in FIXED_SESSION_SETTINGS.items()}
        out.update(statement_timeout=str(self.statement_timeout_ms), lock_timeout=str(self.lock_timeout_ms),
                   idle_in_transaction_session_timeout=str(self.idle_in_transaction_session_timeout_ms))
        return out

    def expected_settings(self) -> dict[str, str]:
        """What pg_settings.setting must show for each session setting (timeouts in milliseconds)."""
        out = {k: v for k, (_, v) in FIXED_SESSION_SETTINGS.items()}
        out.update(statement_timeout=str(self.statement_timeout_ms), lock_timeout=str(self.lock_timeout_ms),
                   idle_in_transaction_session_timeout=str(self.idle_in_transaction_session_timeout_ms))
        return out


def _opt(value: str) -> str:
    """Escape a value for the libpq `options` string: backslash escapes spaces and backslashes."""
    return value.replace("\\", "\\\\").replace(" ", "\\ ")


@dataclass(frozen=True)
class SourceSettings:
    """All nodes of one source by name ('primary', 'replica', or any configured name) plus the names of our objects
    on the source. Package S fills it from the configuration file."""

    nodes: Mapping[str, NodeSettings]
    publication: str = "cdc_v2_pub"
    slot: str = "cdc_v2_slot"
    heartbeat_schema: str = "cdc_v2"
    heartbeat_table: str = "heartbeat"

    def __post_init__(self):
        object.__setattr__(self, "nodes", dict(self.nodes))
        if "primary" not in self.nodes:
            raise ValueError("a source needs a node named 'primary'")
        for name in (self.publication, self.slot):
            if not re.match(r"^cdc_v2[a-z0-9_]{0,57}$", name):
                raise ValueError("our object names start with cdc_v2 and are lower-case identifiers: %r" % name)

    def node(self, target: str) -> NodeSettings:
        try:
            return self.nodes[target]
        except KeyError:
            raise KeyError("no source node named %r (known: %s)" % (target, ", ".join(sorted(self.nodes)))) from None


_configured: SourceSettings | None = None


def configure(settings: SourceSettings | None) -> None:
    """Set the process-wide source settings that `pg.connect('primary', …)` uses."""
    global _configured
    _configured = settings


def configured() -> SourceSettings:
    if _configured is None:
        raise RuntimeError("cdc_v2.pg is not configured (call cdc_v2.pg.configure(SourceSettings(...)))")
    return _configured
