"""Tier T0: how an exception of the client is sorted (`kind_of`) and what of its text goes into a message
(`label`; `safe_text` for a failed connect). The texts are the ones MotherDuck gave
(`evidence/p3/destination/error_mapping.json`)."""
import json
import os

import duckdb
import pytest

from cdc_v2 import md
from cdc_v2.deadline import DeadlineExceeded
from cdc_v2.md import errors

import review_cases

INTERRUPTED = "Connection Error: interrupted by user"
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
COMMIT_KEY = "TransactionContext Error: Failed to commit: PRIMARY KEY or UNIQUE constraint violation: duplicate key "


@pytest.mark.parametrize("exc, kind", [
    (duckdb.ConnectionException(INTERRUPTED), "transport"),
    (duckdb.IOException("Request failed"), "transport"),
    (duckdb.HTTPException("HTTP Error: 503 Service Unavailable"), "transport"),
    (duckdb.HTTPException("HTTP Error: Could not connect to the server"), "transport"),
    (duckdb.InterruptException("Interrupted!"), "transport"),
    # MotherDuck reports a server interrupt inside a transaction with this class, so the text decides
    (duckdb.TransactionException("TransactionContext Error: transaction already rolled back: " + INTERRUPTED), "transport"),
    (duckdb.TransactionException("Failed to commit: MotherDuck transaction statement failed: " + INTERRUPTED), "transport"),
    (duckdb.TransactionException("time-based lease expired"), "transport"),
    (duckdb.ConnectionException("Connection Error: Connection already closed!"), "transport"),
    (ConnectionResetError("reset"), "transport"),
    (TimeoutError("late"), "transport"),
    (OSError("anything"), "transport"),
    (duckdb.TransactionException("TransactionContext Error: Conflict on update!"), "conflict"),
    (duckdb.TransactionException("Conflict on tuple deletion!"), "conflict"),
    (duckdb.TransactionException("Catalog write-write conflict on create with \"t\""), "conflict"),
    (duckdb.ConstraintException("Duplicate key \"k: 1\" violates primary key constraint"), "other"),
    (duckdb.CatalogException("Table with name t does not exist"), "other"),
    # an error about the data quotes a value of a row; a value that holds a phrase must not decide (seeded: the
    # first two were sorted as a conflict and as a lost connection, which a caller answers by running again)
    (duckdb.ConversionException("Conversion Error: Could not convert string 'Conflict on update' to INT32"), "other"),
    (duckdb.ConversionException("Conversion Error: Could not convert string 'Connection Error' to INT32"), "other"),
    (duckdb.ConstraintException("Duplicate key \"k: write-write conflict\" violates primary key constraint"), "other"),
    (duckdb.OutOfRangeException("Value 'interrupted by user' is out of range"), "other"),
    # seeded (review): the same in an error that is not of the data classes. A constraint that COMMIT reports is
    # a TransactionException and quotes the key; a name is quoted too. Only the text before the first quotation
    # mark decides, so these were a conflict and a lost connection and are neither
    (duckdb.TransactionException(COMMIT_KEY + '"Conflict on update"'), "other"),
    (duckdb.TransactionException(COMMIT_KEY + '"k: Connection Error: interrupted by user"'), "other"),
    (duckdb.BinderException("Binder Error: Referenced column \"write-write conflict\" not found in FROM clause!"), "other"),
    (duckdb.CatalogException("Catalog Error: Table with name \"lease expired\" does not exist!"), "other"),
    (duckdb.InvalidInputException("Invalid Input Error: No value for 'Connection already closed'"), "other"),
    (duckdb.TransactionException("first line\nsecond line: Conflict on update!"), "other"),       # the first line only
    # the phrase before the first quotation mark still decides
    (duckdb.TransactionException("TransactionContext Error: Conflict on update! (key \"k: 1\")"), "conflict"),
    (duckdb.TransactionException("TransactionContext Error: transaction already rolled back: 'x'"), "transport"),
    # seeded (fix round, F3): an IO or HTTP error is a transport error only when a transport phrase decides; a
    # file that does not exist was ConnectionLost, and CommitUnknown in autocommit, five times out of five
    (duckdb.IOException("IO Error: no phrase at all, 'Conflict on update'"), "other"),
    (duckdb.IOException("IO Error: No files found that match the pattern"), "other"),
    (duckdb.HTTPException("HTTP Error: 404 Not Found"), "other"),
    # seeded (fix round, F1): DDL on one lane while another lane writes the table. Running it again succeeds
    (duckdb.TransactionException("TransactionContext Error: Transaction conflict: attempting to insert into table "
                                 '"t" but it has been altered by a different transaction'), "conflict"),
    (duckdb.TransactionException("TransactionContext Error: Transaction conflict: attempting to update table"),
     "conflict"),
    (duckdb.TransactionException("TransactionContext Error: Failed to commit: MotherDuck transaction statement "
                                 "failed: TransactionContext Error: Failed to commit: Transaction conflict:"),
     "conflict"),
    (duckdb.TransactionException('Transaction conflict "x" is only a quoted "Transaction conflict"'), "conflict"),
    (duckdb.TransactionException('TransactionContext Error: duplicate key "Transaction conflict"'), "other"),
    # seeded (fix round, F2): the gRPC codes count in capitals and as whole words only, the other phrases as
    # whole words; the programming family (parser, binder, catalog, invalid input) is decided by its class
    (duckdb.CatalogException("Catalog Error: Table with name stock_unavailable does not exist!"), "other"),
    (duckdb.CatalogException("Catalog Error: Collation with name connection error does not exist!"), "other"),
    (duckdb.CatalogException("Catalog Error: Pragma Function with name Conflict on update does not exist!"), "other"),
    (duckdb.InvalidInputException("Invalid Input Error: Connection Error"), "other"),
    (duckdb.InvalidInputException("Invalid Input Error: No open result set"), "other"),
    (duckdb.BinderException("Binder Error: lease expired"), "other"),
    (duckdb.ParserException("Parser Error: syntax error at or near UNAVAILABLE"), "other"),
    (duckdb.TransactionException("rpc error: code = UNAVAILABLE desc = closing"), "transport"),
    (duckdb.TransactionException("rpc error: code = DEADLINE_EXCEEDED"), "transport"),
    (duckdb.TransactionException("TransactionContext Error: stock_unavailable"), "other"),
    (duckdb.TransactionException("TransactionContext Error: STOCK_UNAVAILABLE"), "other"),
    (duckdb.TransactionException("TransactionContext Error: unavailable"), "other"),
    (duckdb.TransactionException("TransactionContext Error: deadline_exceeded_log"), "other"),
    (duckdb.TransactionException("TransactionContext Error: no_connection_errors_today"), "other"),
    (ValueError("Connection Error"), "other"),                # not an error of the client: its text decides nothing
    (DeadlineExceeded("a wait"), "other"),                    # a deadline is decided before `kind_of`, by the layer
])
def test_how_an_exception_is_sorted(exc, kind):
    assert errors.kind_of(exc) == kind


def test_a_message_never_holds_a_token_a_path_a_host_or_an_address():
    # built from pieces, so that this file holds nothing that looks like a secret
    token = "ey" + "J" + "abcdefghij" + "." + "klmnopqrstuv" + "." + "wxyzabcdefgh"
    path = "/" + "home" + "/someone/.cache/store/x.db"
    host = "api" + ".example-service" + ".com:443"
    v4, v6 = "192.0.2.7:443", "[2001:db8::1]:8443"
    text = "IO Error: cannot open %s as %s at %s (%s, %s)\nsecond line with %s" % (path, token, host, v4, v6, token)
    got = errors.safe_text(duckdb.IOException(text))
    for piece in (token, path, "someone", host, "example-service", "192.0.2.7", "2001:db8", "second line"):
        assert piece not in got, (piece, got)
    assert got == "IOException: IO Error: cannot open <path> as <token> at <host> (<address>, <address>)"


def test_a_message_keeps_what_a_reader_needs():
    got = errors.safe_text(duckdb.ConnectionException(INTERRUPTED))
    assert got == "ConnectionException: " + INTERRUPTED
    assert errors.safe_text(ConnectionResetError()) == "ConnectionResetError"
    long = errors.safe_text(duckdb.IOException("x" * 1000))
    assert len(long) < 270 and long.endswith("...")
    assert len(errors.safe_text(duckdb.IOException("x" * 1000), limit=20)) < 50


def test_a_value_that_holds_a_phrase_does_not_look_like_a_conflict_or_a_lost_connection(led):
    """Seeded regression: a value that cannot be converted is quoted in the client's error. With the phrase of a
    conflict in it the lane raised `Conflict`, with the phrase of a transport error `ConnectionLost`; a caller
    runs a transaction again after both, for ever. The error of the data is a `StatementError` that names the
    client's class; the transaction is rolled back and no connection is given up."""
    c = led.connect("look")
    c.execute("CREATE TABLE t (k INTEGER, v BIGINT)")
    lane = led.lane("stream", led.epoch)
    for phrase in ("Conflict on update", "write-write conflict", "Connection Error: interrupted by user"):
        with pytest.raises(md.StatementError) as info:
            with lane.transaction() as tx:
                tx.execute("INSERT INTO t VALUES (1, 1)")
                tx.execute("INSERT INTO t SELECT 2, CAST(? AS BIGINT)", [phrase])
        assert (info.value.error_class, info.value.statement) == ("ConversionException", "INSERT")
        assert phrase not in str(info.value)
        with pytest.raises(md.StatementError) as info:
            c.execute("INSERT INTO t SELECT 3, CAST(? AS BIGINT)", [phrase])
        assert (info.value.error_class, info.value.statement) == ("ConversionException", "INSERT")
        assert phrase not in str(info.value)
    assert c.fetchone("SELECT count(*) FROM t")[0] == 0           # rolled back
    assert lane.conn.generation == 1 and c.generation == 1        # no connection was given up for it
    with lane.transaction() as tx:
        tx.execute("INSERT INTO t VALUES (1, 1)")
    assert c.fetchone("SELECT count(*) FROM t")[0] == 1


# --------------------------------------------------------------------------------------------- the head of a text
@pytest.mark.parametrize("text, expected", [
    ("Conversion Error: Could not convert string 'abc' to INT32", ("Conversion Error: Could not convert string", True)),
    ('Constraint Error: Duplicate key "k: 1" violates primary key constraint', ("Constraint Error: Duplicate key", True)),
    ("Conversion Error: Type INT64 with value 3000000000 can't be cast", (
        "Conversion Error: Type INT64 with value 3000000000 can't be cast", False)),   # the apostrophe of a word
    ("first line\nsecond 'line'", ("first line", False)),
    ("  padded \n", ("padded", False)),
    ("'all of it'", ("", True)),
    ("", ("", False)),
])
def test_the_head_of_a_text_ends_at_the_first_quotation_mark(text, expected):
    assert errors.head(duckdb.Error(text)) == expected


def test_the_known_limit_of_the_head_rule():
    """Not every value in a client text is quoted. The programming family (parser, binder, catalog, invalid
    input), which repeats names and values of the statement without quotation marks most often, is decided by
    its class (fix round, F2). A text of another class that is not about data, with a value outside quotation
    marks that holds a whole phrase, is still sorted by that phrase; no such text was seen. Pinned here so that
    the limit is known (`docs/design/destination.md`, section "Error mapping"); the caller's answer to both
    kinds is to run the transaction again, which meets the same error, so it shows as a loop of retries that
    the caller's limit ends, and not as wrong data."""
    assert errors.kind_of(duckdb.CatalogException("Catalog Error: Table with name lease expired does not exist!")) \
        == "other"
    assert errors.label(duckdb.CatalogException("Catalog Error: Table with name x9 lease expired does not exist!")) \
        == "CatalogException"
    assert errors.kind_of(duckdb.TransactionException("TransactionContext Error: x9 lease expired")) == "transport"
    # the message of the layer still repeats only the phrase from the fixed list
    assert errors.label(duckdb.TransactionException("TransactionContext Error: x9 lease expired")) \
        == "TransactionException (lease expired)"


@pytest.mark.parametrize("exc, expected", [
    (duckdb.IOException("IO Error: Request failed for row 4111 with key 'k'"), "IOException (Request failed)"),
    (duckdb.ConnectionException(INTERRUPTED), "ConnectionException (Connection Error)"),
    (duckdb.TransactionException("time-based lease expired, value 17"), "TransactionException (lease expired)"),
    (duckdb.TransactionException(COMMIT_KEY + '"Request failed"'), "TransactionException"),
    (duckdb.ConversionException("Conversion Error: Could not convert string 'Connection Error' to INT32"),
     "ConversionException"),
    (duckdb.ConversionException("Conversion Error: Request failed 17"), "ConversionException"),     # a data class
    (duckdb.CatalogException("Catalog Error: Table with name Request failed 17 does not exist!"), "CatalogException"),
    (duckdb.IOException("IO Error: No files found that match the pattern 4111"), "IOException"),
    (duckdb.TransactionException("TransactionContext Error: Conflict on update!"), "TransactionException"),
    (ConnectionResetError("Request failed for row 4111"), "ConnectionResetError"),      # not a client error: no text
    (ValueError("anything 'at' all"), "ValueError"),
])
def test_a_label_is_a_class_name_and_at_most_a_phrase_from_the_list(exc, expected):
    got = errors.label(exc)
    assert got == expected
    for piece in ("4111", "17", "'k'", "row"):
        assert piece not in got


def test_refused_names_the_class_and_the_kind_of_statement_and_nothing_else():
    e = errors.refused(duckdb.ConversionException("Conversion Error: Could not convert string 'zq-1' to INT32"),
                       "lane stream", "INSERT")
    assert type(e) is md.StatementError and isinstance(e, md.DestinationError)
    assert str(e) == "lane stream: the destination refused a statement (ConversionException in INSERT)"
    assert (e.where, e.error_class, e.statement) == ("lane stream", "ConversionException", "INSERT")
    assert e.__cause__ is None and e.__context__ is None and "zq-1" not in repr(e) + repr(vars(e))
    e = errors.refused(ValueError("zq-1"), "connection look", "REGISTER")              # not an error of the client
    assert str(e) == "connection look: the destination refused a statement (ValueError in REGISTER)"
    for own in (md.Conflict("x"), md.ConnectionLost("x"), md.CommitUnknown("x"), md.FenceLost("x"),
                DeadlineExceeded("x"), e):
        assert errors.refused(own, "lane stream", "INSERT") is own      # an error of the layer stays what it is
    stop = KeyboardInterrupt()
    assert errors.refused(stop, "lane stream", "INSERT") is stop        # and so does what is not an Exception


def _texts(node):
    if isinstance(node, dict):
        for v in node.values():
            yield from _texts(v)
    elif isinstance(node, list):
        for v in node:
            yield from _texts(v)
    elif isinstance(node, str):
        yield node


def test_no_text_that_motherduck_gave_has_its_phrase_only_inside_a_quoted_value():
    """The head rule must not hide a real lost connection or a real conflict. Every text recorded from
    MotherDuck that holds a transport or a conflict phrase holds it before its first quotation mark."""
    with open(os.path.join(ROOT, "evidence", "p3", "destination", "error_mapping.json"), encoding="utf-8") as f:
        doc = json.load(f)
    both = (errors.TRANSPORT_RE, errors.CONFLICT_RE)
    with_phrase = [t for t in _texts(doc["results"]) if any(rx.search(t) for rx in both)]
    assert len(with_phrase) >= 5                                # the texts of the raw client alone are more
    for text in with_phrase:
        first = errors.head(duckdb.Error(text))[0]
        assert any(rx.search(first) for rx in both), text


# ------------------------------------------------------------------------- seeded from the second review (fix round)
def _review_texts():
    with open(review_cases.TEXTS, encoding="utf-8") as f:
        return json.load(f)


def test_every_misleading_text_that_motherduck_gave_is_refused_not_retried():
    """Seeded (fix round, F2): 168 statements of the second review (`e4_misleading.py`) repeat a planted text
    without quotation marks; 160 failed on MotherDuck and 48 of them were sorted as a lost connection or a
    conflict, which a caller runs again for ever. Every one is a refused statement, and its label is the class
    name alone."""
    doc = _review_texts()
    rows = doc["misleading"]
    assert len(rows) == 168 and sum(1 for r in rows if r["class"]) == 160
    assert sum(1 for r in rows if r["sorted_by_9e389cc"] in ("transport", "conflict")) == 48
    for r in rows:
        if r["class"] is None:
            continue
        exc = getattr(duckdb, r["class"])(r["head"])
        assert errors.kind_of(exc) == "other", r
        assert errors.label(exc) == r["class"], r


def test_every_transaction_conflict_text_that_motherduck_gave_is_a_conflict():
    """Seeded (fix round, F1): `Transaction conflict: attempting to insert into/update table ... altered by a
    different transaction` (`e1b_raw_texts.py` on MotherDuck), also as the answer of COMMIT."""
    rows = _review_texts()["transaction_conflicts"]
    assert len(rows) == 6 and all(r["sorted_by_9e389cc"] == "other" for r in rows)
    for r in rows:
        assert errors.kind_of(getattr(duckdb, r["class"])(r["head"])) == "conflict", r


def test_the_misleading_statements_on_local_duckdb_are_refused_not_retried():
    """The same 168 statements on local DuckDB 1.5.6 (the texts there are the client's own): none is sorted as
    a lost connection or a conflict."""
    import uuid
    raw = duckdb.connect(":memory:rv_%s" % uuid.uuid4().hex[:8])
    failed = 0
    try:
        for label, planted, setup, stmt in review_cases.misleading_statements():
            try:
                for sql in setup:
                    raw.execute(sql)
                raw.execute(stmt).fetchall()
            except duckdb.Error as e:
                failed += 1
                assert errors.kind_of(e) == "other", (label, planted, type(e).__name__)
                assert "(" not in errors.label(e), (label, planted)
    finally:
        raw.close()
    assert failed >= 150
