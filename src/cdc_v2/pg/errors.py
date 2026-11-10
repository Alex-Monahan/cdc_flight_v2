"""Errors of the source side, as a small taxonomy that callers can act on.

| Class | Meaning | What the caller may do |
|---|---|---|
| `ConnectionLost` | the connection is gone (network, server restart, backend terminated) | connect again, run again |
| `cdc_v2.deadline.DeadlineExceeded` | the call passed its deadline; the statement was cancelled (C0) | the caller's rule |
| `RecoveryConflict` | a standby cancelled the statement or session to let replay go on | read again |
| `SerializationFailure` | a primary refused a serializable or repeatable read transaction | run again |
| `LockTimeout` | `lock_timeout` passed while waiting for a lock (55P03) | later, with backoff |
| `PermissionDenied` | missing privilege or failed authentication | alert; retry with backoff (SPEC 4.7) |
| `SourceError` | everything else | not retried blindly |

`RecoveryConflict` and `SerializationFailure` share the base `ReadAgain`. Messages never contain the password; the
host, port and user name of the node are replaced by placeholders, and the original exception is not chained
(`raise … from None`), so no text from the driver reaches a log unfiltered.
"""
from __future__ import annotations

import re

import psycopg

from cdc_v2.deadline import DeadlineExceeded


class SourceError(Exception):
    """Base of every source error except the deadline. `sqlstate` is the five-character code or None."""

    def __init__(self, message: str, sqlstate: str | None = None):
        super().__init__(message)
        self.sqlstate = sqlstate


class ConnectionLost(SourceError):
    pass


class ReadAgain(SourceError):
    pass


class RecoveryConflict(ReadAgain):
    pass


class SerializationFailure(ReadAgain):
    pass


class LockTimeout(SourceError):
    pass


class PermissionDenied(SourceError):
    pass


# SQLSTATE -> class. Class 08 (connection exception) is handled by prefix.
_BY_STATE = {
    "57P01": ConnectionLost,        # admin_shutdown: terminated by pg_terminate_backend or a shutdown
    "57P02": ConnectionLost,        # crash_shutdown
    "57P03": ConnectionLost,        # cannot_connect_now (starting up, shutting down, not yet consistent)
    "57P05": ConnectionLost,        # idle_session_timeout
    "25P03": ConnectionLost,        # idle_in_transaction_session_timeout (the session is closed)
    "55P03": LockTimeout,           # lock_not_available
    "42501": PermissionDenied,      # insufficient_privilege
    "28000": PermissionDenied,      # invalid_authorization_specification
    "28P01": PermissionDenied,      # invalid_password
    "57P04": RecoveryConflict,      # database_dropped (a standby replaying DROP DATABASE)
}
READ_AGAIN_STATES = ("40001", "40P01")
_AUTH_TEXT = ("password authentication failed", "pg_hba.conf rejects", "no pg_hba.conf entry",
              "permission denied", "role \"", "authentication failed")
_SERVER_AT = re.compile(r'connection failed: connection to server (at|on socket) "[^"]*"(, port \d+)? failed: ')
RECOVERY_CONFLICT_TEXT = "conflict with recovery"


def scrub(text: str, secrets: tuple[str, ...] = ()) -> str:
    """Replace every given secret-like value (password, host, user name, database name) in a message by a
    placeholder."""
    out = text or ""
    for s in sorted({s for s in secrets if s and len(s) >= 2}, key=len, reverse=True):
        out = out.replace(s, "<redacted>")
    return out


def classify(exc: BaseException, *, in_recovery: bool | None = None, deadline_fired: bool = False,
             what: str = "source call", secrets: tuple[str, ...] = ()) -> BaseException:
    """The taxonomy exception for a driver exception. Pure: used by the connection wrapper and tested without a
    server. `deadline_fired` says that our own deadline cancelled the call."""
    if isinstance(exc, (DeadlineExceeded, SourceError)):
        return exc
    if deadline_fired:
        return DeadlineExceeded("%s: deadline passed" % what)
    state = getattr(exc, "sqlstate", None)
    diag = getattr(exc, "diag", None)
    primary = getattr(diag, "message_primary", None) if diag is not None else None
    text = str(exc).strip()
    detail = primary or (text.splitlines()[0] if text else type(exc).__name__)
    # libpq connection failures name the server's address; keep only what the server said
    detail = _SERVER_AT.sub("connection failed: ", detail)
    msg = scrub("%s: %s" % (what, detail), secrets)
    if state == "57014":
        # query_canceled: a server-side statement_timeout is a deadline; a cancel by someone else is not
        if primary and "statement timeout" in primary:
            return DeadlineExceeded("%s: server statement_timeout" % what)
        return SourceError(msg, state)
    if state in READ_AGAIN_STATES:
        if in_recovery or (primary and RECOVERY_CONFLICT_TEXT in primary):
            return RecoveryConflict(msg, state)
        return SerializationFailure(msg, state)
    if state and state.startswith("08"):
        return ConnectionLost(msg, state)
    if state in _BY_STATE:
        return _BY_STATE[state](msg, state)
    if state is None and any(m in text for m in _AUTH_TEXT):
        return PermissionDenied(msg, None)      # libpq reports a refused login without an SQLSTATE
    if state is None and ("certificate" in text or "SSL error" in text):
        return SourceError(msg, None)           # a TLS configuration fault: running again does not help
    if state is None and isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError, OSError, EOFError)):
        return ConnectionLost(msg, None)
    return SourceError(msg, state)
