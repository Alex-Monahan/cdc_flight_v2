"""Errors of the destination layer (contract C4) and the mapping of the client library's exceptions onto them.

The mapping was found with real MotherDuck and with fault sites; the evidence is named in
`docs/design/destination.md`, section "Error mapping". The rules, in the order they are applied:

1. A deadline that passed decides first (`cdc_v2.md.connection`): before `COMMIT` was sent the caller gets
   `cdc_v2.deadline.DeadlineExceeded`, after it `CommitUnknown`.
2. A **transport** error: the classes `ConnectionException`, `IOException`, `HTTPException`, `InterruptException`
   of the client, Python's `ConnectionError`, `TimeoutError`, `OSError`, and every client error whose text holds
   one of the transport phrases below (MotherDuck reports a server interrupt, an expired lease and a lost
   server as different classes with the same phrases). Before `COMMIT`: `ConnectionLost`. After: `CommitUnknown`.
3. A **conflict**: a client error whose text says ``Conflict on ...`` or ``write-write conflict`` and that is not
   a transport error: `Conflict`.
4. Everything else is passed on unchanged before `COMMIT` was sent, and is `CommitUnknown` after.

Messages of these errors never repeat the client's text for a conflict (it can hold a key value, SPEC 8.3). For a
transport error they hold the first line of the client's text with tokens, host names, addresses and paths
replaced.
"""
import re

import duckdb

from cdc_v2.deadline import DeadlineExceeded

__all__ = ["DestinationError", "FenceLost", "CommitUnknown", "Conflict", "ConnectionLost", "LaneMisuse",
           "DeadlineExceeded", "kind_of", "safe_text"]


class DestinationError(Exception):
    """Base class of every error this layer raises itself (not of errors it passes on unchanged)."""


class FenceLost(DestinationError):
    """Another run leads. The transaction was rolled back. Stop every lane; the pipeline exits."""


class CommitUnknown(DestinationError):
    """The connection was lost, or the deadline passed, after COMMIT was sent. Read the state in a new transaction
    and continue from what it finds; never assume either outcome."""


class Conflict(DestinationError):
    """The transaction lost a write-write conflict (or `execute_one` changed no row) and was rolled back. Nothing
    was applied. Run it again from the start."""


class ConnectionLost(DestinationError):
    """The connection was lost before COMMIT was sent. Nothing was applied. Run it again from the start; the lane
    takes a new connection at its next transaction."""


class LaneMisuse(RuntimeError):
    """A programming error in the caller: a lane or a transaction object used in a way the contract forbids
    (nested, after its block, from another thread, a transaction statement in the caller's SQL)."""


TRANSPORT_CLASSES = (duckdb.ConnectionException, duckdb.IOException, duckdb.HTTPException, duckdb.InterruptException,
                     ConnectionError, TimeoutError)
# Phrases seen on MotherDuck (evidence/p3/destination/error_mapping.json) for: a server-side interrupt, an expired
# lease, a statement after the server ended the transaction, an unreachable server, a closed connection.
TRANSPORT_RE = re.compile(
    r"Connection Error|interrupted by user|lease expired|transaction already rolled back|Request failed|"
    r"Could not connect|UNAVAILABLE|DEADLINE_EXCEEDED|Connection already closed|No open result set|"
    r"connection (has been |was |is )?closed", re.IGNORECASE)
CONFLICT_RE = re.compile(r"\bconflict on \w+|write-write conflict", re.IGNORECASE)

_SCRUB = (
    (re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"), "<token>"),
    (re.compile(r"(?<![\w.~<>-])/(?:[^\s\"':]+/)+[^\s\"':]*"), "<path>"),
    (re.compile(r"(?i)\b[a-z0-9-]+(?:\.[a-z0-9-]+)+\.(?:com|dev|io|net|internal|local)\b(?::\d+)?"), "<host>"),
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b"), "<address>"),
    (re.compile(r"\[[0-9a-fA-F:]{3,}\](?::\d+)?"), "<address>"),
)


def safe_text(exc, limit=240):
    """Class name and first line of a transport error, with tokens, paths, host names and addresses replaced."""
    text = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
    for rx, repl in _SCRUB:
        text = rx.sub(repl, text)
    if len(text) > limit:
        text = text[:limit] + "..."
    return "%s: %s" % (type(exc).__name__, text) if text else type(exc).__name__


def kind_of(exc):
    """``'transport'``, ``'conflict'`` or ``'other'`` for an exception that a statement raised."""
    if isinstance(exc, DeadlineExceeded):
        return "other"
    if isinstance(exc, TRANSPORT_CLASSES):
        return "transport"
    text = str(exc)
    if isinstance(exc, (duckdb.Error, OSError)) and TRANSPORT_RE.search(text):
        return "transport"
    if isinstance(exc, OSError):
        return "transport"
    if isinstance(exc, duckdb.Error) and CONFLICT_RE.search(text):
        return "conflict"
    return "other"
