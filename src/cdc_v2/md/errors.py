"""Errors of the destination layer (contract C4) and the mapping of the client library's exceptions onto them.

The mapping was found with real MotherDuck and with fault sites; the evidence is named in
`docs/design/destination.md`, section "Error mapping". The rules, in the order they are applied:

1. A deadline that passed decides first (`cdc_v2.md.connection`): before `COMMIT` was sent the caller gets
   `cdc_v2.deadline.DeadlineExceeded`, after it `CommitUnknown`.
2. The **class** decides for three families, whatever the text says. An error about the data of a statement
   (the client's `DataError` and `IntegrityError` families: a conversion, a constraint) and an error of DuckDB's
   programming family (`ProgrammingError`: parser, syntax, binder, catalog, invalid input, invalid type) are
   rule 5: their text repeats values and names of the statement without quotation marks
   (``Catalog Error: Table with name stock_unavailable does not exist!``), so it decides nothing. The client's
   `ConnectionException` and `InterruptException`, and Python's `ConnectionError`, `TimeoutError` and `OSError`,
   are **transport** errors.
3. A **transport** error by its text: any other client error whose head holds one of the transport phrases
   below (MotherDuck reports a server interrupt, an expired lease and a lost server as `TransactionException`
   with these phrases). The gRPC codes `UNAVAILABLE` and `DEADLINE_EXCEEDED` count only in capitals and as
   whole words; the other phrases in any case, as whole words. An `IOException` or `HTTPException` is a
   transport error only by this rule: a file that does not exist is an `IOException` too, and running the
   statement again gives the same answer. Before `COMMIT`: `ConnectionLost`. After: `CommitUnknown`.
4. A **conflict**: any other client error whose head says ``Conflict on ...``, ``write-write conflict`` or
   ``Transaction conflict`` (DDL on a table that another transaction writes, or the other way round): `Conflict`.
5. Everything else that a statement raises is `StatementError` before `COMMIT` was sent, and `CommitUnknown`
   after. Inside a lane transaction the transaction was rolled back; a statement in autocommit (outside a
   lane transaction) was refused as a whole, nothing of it was applied.

Where the text decides (rules 3 and 4), only its **head** does: the first line up to the first quotation mark.
The client quotes the values it repeats (`duplicate key "..."`, `Could not convert string '...'`, `Casting value
"..."`), so a value of a row that holds one of the phrases decides nothing.

What these errors keep of the client's error (SPEC 8.3): the name of its class, the kind of the statement and,
for a lost connection, the transport phrase that decided (`label`, `StatementError`). All three come from
fixed lists. Nothing else of the client's text is repeated in a message or in a log line: the text can name a
value of a row, with quotation marks or without (`Type INT64 with value 3000000000 can't be cast ...`). The
client's exception is not attached either (no `__cause__`): a traceback in a log would print its whole text.
The one exception is a failed connect (`safe_text`): nothing was sent, so its text holds no value of a row,
and it says why the connect failed.
"""
import re

import duckdb

from cdc_v2.deadline import DeadlineExceeded

__all__ = ["DestinationError", "FenceLost", "CommitUnknown", "Conflict", "ConnectionLost", "StatementError",
           "LaneMisuse", "DeadlineExceeded", "kind_of", "safe_text", "label", "head", "refused"]


class DestinationError(Exception):
    """Base class of every error this layer raises itself."""


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


class StatementError(DestinationError):
    """The destination refused a statement, or the client library could not run it: a value that does not
    convert, a constraint, a name that does not exist, SQL text it cannot parse. Inside a lane transaction the
    transaction was rolled back; outside one nothing of the statement was applied. Running the same statement
    again gives the same answer.

    ``error_class`` is the name of the client's exception class. ``statement`` is the kind of the statement:
    the name DuckDB's parser gives it (``INSERT``, ``UPDATE``, ``DELETE``, ``SELECT``, ``CREATE``, ``DROP``,
    ``ALTER``, ...), ``UNPARSED`` for text the parser refused, ``BEGIN`` and ``FENCE`` for the lane's own
    statements, ``REGISTER`` for data handed to ``register``, ``INTERRUPT`` for the call that stops what an
    earlier connection left running on the server. The message holds these two and the place, and
    nothing of the client's text: that text can name a value of a row (SPEC 8.3), so it is not kept, not
    attached as a cause and not logged."""

    def __init__(self, where, error_class, statement):
        super().__init__("%s: the destination refused a statement (%s in %s)" % (where, error_class, statement))
        self.where = where
        self.error_class = error_class
        self.statement = statement


class LaneMisuse(RuntimeError):
    """A programming error in the caller: a lane or a transaction object used in a way the contract forbids
    (nested, after its block, from another thread, a transaction statement in the caller's SQL)."""


#: Transport errors by their class alone (rule 2).
TRANSPORT_CLASSES = (duckdb.ConnectionException, duckdb.InterruptException, ConnectionError, TimeoutError, OSError)
#: Transport errors only when a transport phrase decides (rule 3); `HTTPException` is a subclass.
IO_CLASSES = (duckdb.IOException,)
#: Errors about the data of a statement (a value that does not convert, a constraint). Their text quotes a value
#: of a row, so their text decides nothing: a value that holds one of the phrases below must not look like a lost
#: connection or a conflict, which a caller answers by running the transaction again.
DATA_CLASSES = (duckdb.DataError, duckdb.IntegrityError)
#: DuckDB's programming family: parser, syntax, binder, catalog, invalid input, invalid type. Their text repeats
#: names and values of the statement without quotation marks, so it decides nothing either (rule 2): always
#: `StatementError` (seeded from 168 texts of MotherDuck, `tests/destination/data/misleading_texts.json`).
STATEMENT_CLASSES = (duckdb.ProgrammingError,)
#: Phrases seen on MotherDuck (evidence/p3/destination/error_mapping.json) for: a server-side interrupt, an expired
#: lease, a statement after the server ended the transaction, an unreachable server, a closed connection; and the
#: reason phrases of HTTP 502, 503 and 504. Whole words; the gRPC codes in capitals only (``stock_unavailable``
#: and ``deadline_exceeded_log`` are names).
TRANSPORT_RE = re.compile(
    r"(?i:\bConnection Error\b|\binterrupted by user\b|\blease expired\b|\btransaction already rolled back\b|"
    r"\bRequest failed\b|\bCould not connect\b|\bConnection already closed\b|\bNo open result set\b|"
    r"\bconnection (?:has been |was |is )?closed\b|\bService Unavailable\b|\bBad Gateway\b|\bGateway Time-?out\b)|"
    r"\bUNAVAILABLE\b|\bDEADLINE_EXCEEDED\b")
#: ``Transaction conflict: attempting to insert into table "t" but it has been altered by a different
#: transaction`` (and ``... update table ...``): DDL on one lane while another lane writes the table. Running the
#: transaction again succeeds once the other one has committed (7 of 8 shapes of the second review).
CONFLICT_RE = re.compile(r"(?i:\bconflict on \w+|\bwrite-write conflict\b|\bTransaction conflict\b)")

_SCRUB = (
    (re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"), "<token>"),
    (re.compile(r"(?<![\w.~<>-])/(?:[^\s\"':]+/)+[^\s\"':]*"), "<path>"),
    (re.compile(r"(?i)\b[a-z0-9-]+(?:\.[a-z0-9-]+)+\.(?:com|dev|io|net|internal|local)\b(?::\d+)?"), "<host>"),
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b"), "<address>"),
    (re.compile(r"\[[0-9a-fA-F:]{3,}\](?::\d+)?"), "<address>"),
)


#: Where a quoted piece of a client text starts (the apostrophe of "can't" does not start one).
_QUOTE = re.compile(r"""(?<![A-Za-z])['"]""")


def _first_line(exc):
    text = str(exc).strip()
    return text.splitlines()[0] if text else ""


def head(exc):
    """The first line of a client text up to its first quotation mark: the client's own wording, without the
    values it quotes. ``(text, cut)``; ``cut`` says that something was left out."""
    line = _first_line(exc)
    m = _QUOTE.search(line)
    return (line, False) if m is None else (line[:m.start()].rstrip(), True)


def _scrubbed(name, text, limit):
    for rx, repl in _SCRUB:
        text = rx.sub(repl, text)
    if len(text) > limit:
        text = text[:limit] + "..."
    return "%s: %s" % (name, text) if text else name


def safe_text(exc, limit=240):
    """Class name and first line of a client error, with tokens, paths, host names and addresses replaced. The
    line can still name a value of a row: this is for a failed connect, and for tests and evidence about texts
    that are known. Every other message and every log line of the layer uses `label`."""
    return _scrubbed(type(exc).__name__, _first_line(exc), limit)


def _decided_by_text(exc):
    """True when the text of ``exc`` may decide its kind (rules 3 and 4): a client error that is neither about
    data nor of the programming family."""
    return isinstance(exc, duckdb.Error) and not isinstance(exc, DATA_CLASSES + STATEMENT_CLASSES)


def label(exc):
    """What a message or a log line of this layer says about a client error: the name of its class and, when
    one of the transport phrases is in the head of a text that may decide, that phrase. Both come from fixed
    lists; nothing else of the client's text is repeated."""
    name = type(exc).__name__
    if _decided_by_text(exc):
        m = TRANSPORT_RE.search(head(exc)[0])
        if m is not None:
            return "%s (%s)" % (name, m.group(0))
    return name


def refused(err, where, statement):
    """The error a caller gets for an exception of a statement that is neither a lost connection nor a
    conflict (rule 4): a `StatementError` that names the class and the kind of statement. An error of this
    layer, a deadline and anything that is not an `Exception` stay what they are."""
    if isinstance(err, (DestinationError, DeadlineExceeded)) or not isinstance(err, Exception):
        return err
    return StatementError(where, type(err).__name__, statement)


def kind_of(exc):
    """``'transport'``, ``'conflict'`` or ``'other'`` for an exception that a statement raised (the rules of the
    module text, in their order)."""
    if isinstance(exc, DeadlineExceeded):
        return "other"
    if isinstance(exc, DATA_CLASSES + STATEMENT_CLASSES):
        return "other"
    if isinstance(exc, TRANSPORT_CLASSES):
        return "transport"
    if not isinstance(exc, duckdb.Error):
        return "other"
    text = head(exc)[0]                          # a phrase inside a quoted value decides nothing
    if TRANSPORT_RE.search(text):
        return "transport"
    if isinstance(exc, IO_CLASSES):
        return "other"                           # a file that does not exist: the same answer again
    if CONFLICT_RE.search(text):
        return "conflict"
    return "other"
