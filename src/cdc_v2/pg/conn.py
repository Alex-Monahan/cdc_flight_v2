"""Connections to the source with deadlines (SPEC 4.5, C0, C7).

`connect(target, deadline, replication=False)` returns a :class:`PgConnection`. Every call that waits takes a
`cdc_v2.deadline.Deadline`:

- **connect**: libpq's `connect_timeout` plus a hard bound: the connection is opened on a helper thread, and the
  caller is released at the deadline plus `cancel_grace_s` whatever the network does; a connection that is opened
  late is closed by the helper.
- **each statement**: a watchdog thread fires at the deadline. It sends a cancel request (bounded by
  `cancel_grace_s`); if the statement has not returned `cancel_grace_s` after that, it shuts the socket down, which
  makes the blocked call fail at once. A statement that the watchdog reached raises
  `cdc_v2.deadline.DeadlineExceeded` and the connection is closed (a late cancel could otherwise hit the next
  statement). The caller is never blocked beyond deadline + 2 × `cancel_grace_s`.
- **close**: on a helper thread, bounded the same way.

The fixed session settings (`settings.FIXED_SESSION_SETTINGS` plus the three timeouts) travel in the startup packet
and are read back after connecting; a connection on which one differs is refused (`SessionSettingsMismatch`).
"""
from __future__ import annotations

import logging
import math
import os
import socket
import threading
from contextlib import contextmanager
from typing import Any, Iterator, Sequence

import psycopg

from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.faultsites import declare_site, fault_site
from cdc_v2.pg import errors
from cdc_v2.pg.settings import NodeSettings, SourceSettings, configured

log = logging.getLogger("cdc_v2.pg")

declare_site("pg.connect", "opening a connection to a source Postgres node")
declare_site("pg.execute", "one statement on a source Postgres node")


class SessionSettingsMismatch(errors.SourceError):
    """A fixed session setting did not take effect (something on the server overrides it)."""


def _check_deadline(deadline: Deadline) -> Deadline:
    if not isinstance(deadline, Deadline):
        raise TypeError("a Deadline is required (cdc_v2.deadline.Deadline), got %r" % (type(deadline).__name__,))
    return deadline


class PgConnection:
    """One connection to one node. Not thread-safe: one thread uses it at a time (the watchdog only cancels).

    `raw` is the underlying psycopg connection (autocommit on), for callers that need psycopg directly (COPY in the
    backfill); such callers wrap their call in :meth:`guard` to get the same deadline behaviour.
    """

    def __init__(self, raw: psycopg.Connection, node: NodeSettings, target: str, replication: bool):
        self.raw = raw
        self.node = node
        self.target = target
        self.replication = replication
        self.backend_pid: int = raw.info.backend_pid
        self.server_version_num: int = raw.info.server_version
        self._in_recovery: bool | None = None
        self._broken = False
        self._secrets = (node.host, node.user)

    # ----------------------------------------------------------------------------------------------- facts
    @property
    def closed(self) -> bool:
        return self._broken or self.raw.closed

    @property
    def major(self) -> int:
        return self.server_version_num // 10000

    def in_recovery(self, deadline: Deadline) -> bool:
        """pg_is_in_recovery(), read again on every call (a standby can be promoted)."""
        self._in_recovery = bool(self.scalar("SELECT pg_is_in_recovery()", deadline=deadline))
        return self._in_recovery

    # ----------------------------------------------------------------------------------------------- statements
    @contextmanager
    def guard(self, deadline: Deadline, what: str) -> Iterator[None]:
        """Run the block under the statement deadline rules of this module; map errors to the taxonomy."""
        _check_deadline(deadline)
        if self.closed:
            raise errors.ConnectionLost("%s: the connection is closed" % what)
        if deadline.expired:
            raise DeadlineExceeded("%s: deadline passed before the call" % what)
        lock = threading.Lock()
        state = {"done": False, "fired": False, "shut": False}
        grace = self.node.cancel_grace_s

        def shut():
            with lock:
                if state["done"]:
                    return
                state["shut"] = True
            self._shutdown_socket()

        def fire():
            with lock:
                if state["done"]:
                    return
                state["fired"] = True
            t2 = threading.Timer(grace, shut)
            t2.daemon = True
            t2.start()
            timers.append(t2)
            try:
                self.raw.cancel_safe(timeout=grace)
            except Exception:           # noqa: BLE001 - the socket shutdown that follows is the fallback
                pass

        timers: list[threading.Timer] = []
        t1 = threading.Timer(deadline.remaining(), fire)
        t1.daemon = True
        timers.append(t1)
        t1.start()
        try:
            try:
                yield
            finally:
                with lock:
                    state["done"] = True
                for t in timers:
                    t.cancel()
        except BaseException as exc:
            if state["fired"]:
                self._mark_broken()
                raise DeadlineExceeded("%s: deadline passed (cancelled%s)"
                                       % (what, ", socket shut down" if state["shut"] else "")) from None
            if isinstance(exc, (psycopg.Error, OSError, EOFError)):
                mapped = errors.classify(exc, in_recovery=self._in_recovery, what=what, secrets=self._secrets)
                if isinstance(mapped, errors.ConnectionLost) or self.raw.closed or self.raw.broken:
                    self._mark_broken()
                elif isinstance(mapped, errors.RecoveryConflict) and self.raw.info.transaction_status == \
                        psycopg.pq.TransactionStatus.UNKNOWN:
                    self._mark_broken()
                raise mapped from None
            raise
        if state["fired"]:              # the statement returned, but a cancel may still be on its way
            self._mark_broken()
            raise DeadlineExceeded("%s: deadline passed" % what)

    def execute(self, sql: str, params: Sequence[Any] | dict | None = None, *, deadline: Deadline,
                what: str = "statement") -> list[tuple]:
        """Run one statement; return its rows (empty list when it returns none)."""
        with self.guard(deadline, what):
            fault_site("pg.execute")
            cur = self.raw.execute(sql, params, prepare=False)
            return cur.fetchall() if cur.description else []

    def execute_dicts(self, sql: str, params=None, *, deadline: Deadline, what: str = "statement") -> list[dict]:
        with self.guard(deadline, what):
            fault_site("pg.execute")
            cur = self.raw.execute(sql, params, prepare=False)
            if not cur.description:
                return []
            names = [d.name for d in cur.description]
            return [dict(zip(names, r)) for r in cur.fetchall()]

    def scalar(self, sql: str, params=None, *, deadline: Deadline, what: str = "statement") -> Any:
        rows = self.execute(sql, params, deadline=deadline, what=what)
        return rows[0][0] if rows else None

    def simple(self, sql: str, *, deadline: Deadline, what: str = "command") -> list[tuple]:
        """Run a command with the simple query protocol (replication commands such as IDENTIFY_SYSTEM)."""
        with self.guard(deadline, what):
            fault_site("pg.execute")
            res = self.raw.pgconn.exec_(sql.encode())
            if res.status not in (psycopg.pq.ExecStatus.TUPLES_OK, psycopg.pq.ExecStatus.COMMAND_OK):
                err = psycopg.errors.lookup(res.error_field(psycopg.pq.DiagnosticField.SQLSTATE).decode()) \
                    if res.error_field(psycopg.pq.DiagnosticField.SQLSTATE) else psycopg.OperationalError
                raise err((res.error_message or b"").decode(errors="replace"))
            return [tuple(res.get_value(r, c) for c in range(res.nfields)) for r in range(res.ntuples)]

    @contextmanager
    def transaction(self, deadline: Deadline, *, isolation: str = "read committed", read_only: bool = False,
                    what: str = "transaction") -> Iterator["PgConnection"]:
        """BEGIN … COMMIT with the given isolation; ROLLBACK on error. Statements inside take their own deadlines;
        BEGIN and COMMIT use this one."""
        if isolation not in ("read committed", "repeatable read", "serializable"):
            raise ValueError("bad isolation level %r" % isolation)
        self.execute("BEGIN ISOLATION LEVEL %s%s" % (isolation.upper(), " READ ONLY" if read_only else ""),
                     deadline=deadline, what=what + " (begin)")
        try:
            yield self
        except BaseException:
            if not self.closed:
                try:
                    self.execute("ROLLBACK", deadline=Deadline.after(self.node.cancel_grace_s),
                                 what=what + " (rollback)")
                except (errors.SourceError, DeadlineExceeded):
                    self._mark_broken()
            raise
        self.execute("COMMIT", deadline=deadline, what=what + " (commit)")

    # ----------------------------------------------------------------------------------------------- closing
    def _shutdown_socket(self) -> None:
        try:
            fd = self.raw.pgconn.socket
        except Exception:           # noqa: BLE001 - already closed
            return
        try:
            s = socket.socket(fileno=os.dup(fd))
        except OSError:
            return
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            s.close()

    def _mark_broken(self) -> None:
        self._broken = True
        self.close(Deadline.after(self.node.cancel_grace_s))

    def close(self, deadline: Deadline | None = None) -> None:
        """Close; never blocks beyond the deadline (default: `cancel_grace_s`)."""
        deadline = deadline or Deadline.after(self.node.cancel_grace_s)
        if self.raw.closed:
            return
        t = threading.Thread(target=self._close_quietly, daemon=True, name="cdc_v2-pg-close")
        t.start()
        t.join(deadline.remaining())
        if t.is_alive():
            self._shutdown_socket()
            t.join(self.node.cancel_grace_s)

    def _close_quietly(self) -> None:
        try:
            self.raw.close()
        except Exception:           # noqa: BLE001
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __repr__(self):
        return "<PgConnection %s pid=%s%s>" % (self.target, self.backend_pid, " closed" if self.closed else "")


def _resolve(target, settings: SourceSettings | None) -> tuple[str, NodeSettings]:
    if isinstance(target, NodeSettings):
        return target.application_name, target
    return target, (settings or configured()).node(target)


def connect(target: str | NodeSettings, deadline: Deadline, *, replication: bool = False,
            settings: SourceSettings | None = None, application_name: str | None = None) -> PgConnection:
    """Open a connection to a node: 'primary', 'replica', a configured name, or a NodeSettings.

    `replication=True` opens a `replication=database` connection (replication commands and SQL).
    """
    _check_deadline(deadline)
    name, node = _resolve(target, settings)
    if application_name:
        node = node.with_(application_name=application_name)
    what = "connect to %s" % name
    if deadline.expired:
        raise DeadlineExceeded(what + ": deadline passed before the call")
    budget = deadline.remaining()
    kw = node.conninfo(replication=replication, connect_timeout=max(2, int(math.ceil(min(budget,
                                                                                         node.connect_timeout_s)))))
    password = node.password.reveal() if node.password is not None else None
    if password is not None:
        kw["password"] = password
    box: dict = {}
    lock = threading.Lock()

    def opener():
        try:
            fault_site("pg.connect")
            c = psycopg.connect(autocommit=True, **kw)
        except BaseException as e:      # noqa: BLE001 - handed to the caller
            with lock:
                box["error"] = e
            return
        with lock:
            if box.get("abandoned"):
                try:
                    c.close()
                except Exception:       # noqa: BLE001
                    pass
                return
            box["conn"] = c

    t = threading.Thread(target=opener, daemon=True, name="cdc_v2-pg-connect")
    t.start()
    t.join(deadline.remaining() + node.cancel_grace_s)
    with lock:
        if "conn" not in box and "error" not in box:
            box["abandoned"] = True
    secrets = (node.host, node.user) + ((password,) if password else ())
    del password
    kw.pop("password", None)
    if box.get("abandoned"):
        raise DeadlineExceeded(what + ": deadline passed while connecting")
    if "error" in box:
        mapped = errors.classify(box["error"], what=what, secrets=secrets)
        if isinstance(mapped, errors.SourceError) and "timeout expired" in str(mapped) and deadline.expired:
            raise DeadlineExceeded(what + ": deadline passed while connecting") from None
        raise mapped from None
    conn = PgConnection(box["conn"], node, name, replication)
    try:
        _verify_session(conn, deadline)
    except BaseException:
        conn.close()
        raise
    return conn


def _verify_session(conn: PgConnection, deadline: Deadline) -> None:
    expected = conn.node.expected_settings()
    names = list(expected)
    if conn.replication:
        sql = ("SELECT name, setting FROM pg_settings WHERE lower(name) = ANY('{%s}'::text[])"
               % ",".join(n.lower() for n in names))
        rows = [(a.decode(), b.decode()) for a, b in conn.simple(sql, deadline=deadline, what="session settings")]
    else:
        rows = conn.execute("SELECT name, setting FROM pg_settings WHERE lower(name) = ANY(%s)",
                            ([n.lower() for n in names],), deadline=deadline, what="session settings")
    seen = {k.lower(): v for k, v in rows}
    wrong = sorted(n for n in names if seen.get(n.lower()) != expected[n])
    if wrong:
        raise SessionSettingsMismatch("session settings did not take effect: %s"
                                      % ", ".join("%s=%r" % (n, seen.get(n.lower())) for n in wrong))
    conn._in_recovery = None
