"""Tier T0: how an exception of the client is sorted (`kind_of`) and what of its text goes into a message
(`safe_text`). The texts are the ones MotherDuck gave (`evidence/p3/destination/error_mapping.json`)."""
import duckdb
import pytest

from cdc_v2.deadline import DeadlineExceeded
from cdc_v2.md import errors

INTERRUPTED = "Connection Error: interrupted by user"


@pytest.mark.parametrize("exc, kind", [
    (duckdb.ConnectionException(INTERRUPTED), "transport"),
    (duckdb.IOException("Request failed"), "transport"),
    (duckdb.HTTPException("503"), "transport"),
    (duckdb.InterruptException("Interrupted!"), "transport"),
    # MotherDuck reports a server interrupt inside a transaction with this class, so the text decides
    (duckdb.TransactionException("TransactionContext Error: transaction already rolled back: " + INTERRUPTED), "transport"),
    (duckdb.TransactionException("Failed to commit: MotherDuck transaction statement failed: " + INTERRUPTED), "transport"),
    (duckdb.TransactionException("time-based lease expired"), "transport"),
    (duckdb.InvalidInputException("Connection already closed"), "transport"),
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
    runs a transaction again after both, for ever. The error of the data is passed on unchanged."""
    c = led.connect("look")
    c.execute("CREATE TABLE t (k INTEGER, v BIGINT)")
    lane = led.lane("stream", led.epoch)
    for phrase in ("Conflict on update", "write-write conflict", "Connection Error: interrupted by user"):
        with pytest.raises(duckdb.ConversionException):
            with lane.transaction() as tx:
                tx.execute("INSERT INTO t VALUES (1, 1)")
                tx.execute("INSERT INTO t SELECT 2, CAST(? AS BIGINT)", [phrase])
        with pytest.raises(duckdb.ConversionException):
            c.execute("INSERT INTO t SELECT 3, CAST(? AS BIGINT)", [phrase])
    assert c.fetchone("SELECT count(*) FROM t")[0] == 0           # rolled back
    assert lane.conn.generation == 1 and c.generation == 1        # no connection was given up for it
    with lane.transaction() as tx:
        tx.execute("INSERT INTO t VALUES (1, 1)")
    assert c.fetchone("SELECT count(*) FROM t")[0] == 1
