"""Package A, tier T0: apply on package D's real lane (the fix round after D's merge). What D's lane does that the
stand-in did not, and what apply must keep to (docs/design/apply.md 10; docs/design/destination.md 13-15):

1. apply catches nothing of D's: `md.DestinationError` (StatementError, ConnectionLost, Conflict, FenceLost, ...),
   `DeadlineExceeded` and `md.LaneMisuse` reach the caller as the lane raised them, from every call apply makes;
2. a statement the destination refuses is D's `md.StatementError`, with the class and the kind of statement,
   never the value;
3. apply reads a count only from an INSERT, UPDATE or DELETE without RETURNING, and never reads a missing count
   as 0 (every apply test checks the first through `apply_lane.counted_statements`, see conftest.py);
4. registered names are `cdc_v2_apply_<step>`, registering one again replaces it, and they are gone when the
   block ends;
6. the lane's Arrow export is lossless (HUGEINT is an opaque value, not a number), calls after a failure and
   from another thread are refused.

The `*_case` functions run on any LaneDB; test_apply_md.py runs them on MotherDuck.
"""
from __future__ import annotations

import os
import threading
import time

import pyarrow as pa
import pytest

from apply_cases import R, all_cases
from apply_harness import SCHEMA, change_set, dest_canon, local_db, test_settings
from apply_lane import APPLY_NAME, counted_statements
from apply_sim import change_set_table, create_table, literal_rows_canon
from cdc_v2 import md
from cdc_v2.apply import (Column, LocalChangeSet, TableMeta, Target, apply_group, reduce_duplicates, repair_keys,
                          truncate)
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.oracle.canonical import quote_ident

_FULL = os.environ.get("CDC_V2_APPLY_FULL") == "1"


@pytest.fixture
def db():
    with local_db() as d:
        yield d


# ------------------------------------------------------------------ 1. nothing of D's is caught


class _Inject:
    """D's transaction, failing at call number `at` (counted over every call apply makes, across blocks) with
    `err`, the way the lane fails: the transaction is rolled back and the error raised."""

    def __init__(self, tx, at, err, calls):
        self.tx, self.at, self.err, self.calls = tx, at, err, calls

    def _step(self):
        self.calls[0] += 1
        if self.calls[0] == self.at:
            raise self.tx.fail(self.err)

    def execute(self, sql, params=None, deadline=None):
        self._step()
        return self.tx.execute(sql, params, deadline)

    def fetchall(self, sql, params=None, deadline=None):
        self._step()
        return self.tx.fetchall(sql, params, deadline)

    def fetch_arrow(self, sql, params=None, deadline=None):
        self._step()
        return self.tx.fetch_arrow(sql, params, deadline)

    def register(self, name, data, deadline=None):
        self._step()
        return self.tx.register(name, data, deadline)


#: One of each error that D's lane raises from a statement (CommitUnknown comes from COMMIT, which apply never
#: sends).
ERRORS = [lambda: md.StatementError("lane stream", "ConversionException", "INSERT"),
          lambda: DeadlineExceeded("lane stream: the deadline passed before a statement"),
          lambda: md.LaneMisuse("a transaction is used by the thread that opened it"),
          lambda: md.ConnectionLost("lane stream: connection lost during a statement: connection reset"),
          lambda: md.Conflict("lane stream: write-write conflict in a statement"),
          lambda: md.FenceLost("lane stream: epoch 1 no longer leads")]

#: Named cases that between them reach every kind of call apply makes: copies by row id (C0, K7, K8) and moves
#: (K2, K3), the catch-up lookup and unresolved keys, a column named rowid (R1, R2), and the keyless statements.
INJECT_CASES = ["round2_X1_form_change_then_move_onto_its_old_form", "catch_up_reports_a_key_it_cannot_resolve",
                "awkward_names", "keyless_with_a_column_named_rowid", "negative_zero_and_interval_form_partial_image"]
_BY_NAME = {c.name: c for c in all_cases()}


def _run(db, case, form, table, wrap=lambda tx: tx):
    target = Target(SCHEMA, table)
    create_table(db, SCHEMA, table, case.meta, case.start)
    css = [change_set(db, case.meta, recs, form, i) for i, recs in enumerate(case.sets)]
    settings = test_settings(**(case.settings or {}))
    for cs in css:
        with db.transaction() as tx:
            apply_group(wrap(tx), target, cs, mode=case.mode, settings=settings)
    return target


@pytest.mark.parametrize("form", ["local", "relation"])
@pytest.mark.parametrize("name", INJECT_CASES)
def test_d_errors_pass_through_unchanged_from_every_call(db, name, form):
    """Item 1: the error that the lane raises at any call of apply is the error apply's caller gets, the same
    object, and the lane's roll-back leaves the table as it was. (On the code before this round too: apply caught
    nothing then either; this confirms it on the real lane.)"""
    case = _BY_NAME[name]
    calls = [0]
    _run(db, case, form, "t_count", lambda tx: _Inject(tx, 0, None, calls))
    total = calls[0]
    assert total >= (2 if form == "local" else 10), total
    sites = range(1, total + 1) if (_FULL or form == "local") else sorted({1, 2, 3, total} | set(range(1, total, 7)))
    for at in sites:
        err = ERRORS[at % len(ERRORS)]()
        table = "t_%d" % at
        n = [0]
        with pytest.raises(type(err)) as got:
            _run(db, case, form, table, lambda tx: _Inject(tx, at, err, n))
        assert got.value is err, (at, got.value)
        if case.sets and len(case.sets) == 1:
            assert dest_canon(db, Target(SCHEMA, table), case.meta) == literal_rows_canon(
                list(case.meta.columns), case.start), "the lane's roll-back left the table as it was"


# ------------------------------------------------------------------ 2. a refused statement


PLANTED = 4242424242                       # does not fit the destination's SMALLINT column
TS = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "v", "BIGINT")))


def refused_value_case(db, table, form="local"):
    """A value that the destination column cannot hold (meta says BIGINT, the table has SMALLINT): the INSERT is
    refused, and apply's caller gets D's `StatementError` with the class of the client's error and the kind of
    statement, and nothing of the value. A further call on the failed transaction is the lane's `LaneMisuse`;
    leaving the block raises the error again; nothing was committed."""
    target = Target(SCHEMA, table)
    db.execute("CREATE SCHEMA IF NOT EXISTS %s" % quote_ident(SCHEMA))
    db.execute("CREATE TABLE %s (id BIGINT, v SMALLINT)" % target.sql())
    db.execute("INSERT INTO %s VALUES (1, 7)" % target.sql())
    cs = change_set(db, TS, R().c(1, {0: "2", 1: str(PLANTED)}).out, form)
    with pytest.raises(md.StatementError) as outer:
        with db.transaction() as tx:
            with pytest.raises(md.StatementError) as e:
                apply_group(tx, target, cs, settings=test_settings())
            with pytest.raises(md.LaneMisuse):
                apply_group(tx, target, cs, settings=test_settings())
    err = e.value
    assert outer.value is err
    assert err.statement == "INSERT" and err.error_class.endswith("Exception"), (err.statement, err.error_class)
    for text in (str(err), repr(err), " ".join(map(str, err.args)), err.where):
        assert str(PLANTED) not in text, text
    assert err.__cause__ is None and str(PLANTED) not in str(err.__context__ or "")
    assert db.execute("SELECT id, v FROM %s" % target.sql()).fetchall() == [(1, 7)]
    return {"error_class": err.error_class, "statement": err.statement}


@pytest.mark.parametrize("form", ["local", "relation"])
def test_a_refused_value_is_d_s_statement_error_without_the_value(db, form):
    refused_value_case(db, "t", form)


def test_a_deadline_of_the_block_passes_through(db):
    """The block's deadline passes while apply plans: the lane's `DeadlineExceeded` reaches the caller."""
    case = _BY_NAME["insert_update_delete"]
    create_table(db, SCHEMA, "t", case.meta, case.start)
    cs = change_set(db, case.meta, case.sets[0], "local")
    with pytest.raises(DeadlineExceeded):
        with db.transaction(Deadline.after(0.2)) as tx:
            time.sleep(0.3)
            apply_group(tx, Target(SCHEMA, "t"), cs, settings=test_settings())
    assert dest_canon(db, Target(SCHEMA, "t"), case.meta) == literal_rows_canon(list(case.meta.columns), case.start)


def test_a_call_from_another_thread_is_the_lane_s_lane_misuse(db):
    """The lane is used by the thread that opened the block only; apply (which starts no thread) passes the
    lane's refusal on."""
    case = _BY_NAME["insert_update_delete"]
    create_table(db, SCHEMA, "t", case.meta, case.start)
    cs = change_set(db, case.meta, case.sets[0], "local")
    got = []
    with db.transaction() as tx:
        def other():
            try:
                apply_group(tx, Target(SCHEMA, "t"), cs, settings=test_settings())
            except BaseException as e:                     # noqa: BLE001 - handed to the test thread
                got.append(e)
        th = threading.Thread(target=other)
        th.start()
        th.join(30)
    assert len(got) == 1 and isinstance(got[0], md.LaneMisuse), got
    assert dest_canon(db, Target(SCHEMA, "t"), case.meta) == literal_rows_canon(list(case.meta.columns), case.start)


# ------------------------------------------------------------------ 3. counts


class _Answers:
    def __init__(self, n):
        self.n = n

    def execute(self, sql, params=None, deadline=None):
        return self.n


def test_counting_hands_back_no_result_but_a_count():
    """`_Counting.execute` returns nothing, whatever the transaction answers (D's lane answers None for CREATE
    TABLE AS and RETURNING): nothing in apply can read it. `count` refuses a missing count."""
    from cdc_v2.apply.core import ApplyCounts, _Counting
    assert _Counting(_Answers(5), ApplyCounts()).execute("CREATE TABLE x AS SELECT 1") is None
    assert _Counting(_Answers(3), ApplyCounts()).count("DELETE FROM x") == 3
    with pytest.raises(RuntimeError):
        _Counting(_Answers(None), ApplyCounts()).count("DELETE FROM x")


class _NoCount:
    """D's transaction, except that it answers no count (None) for every statement: what a transaction outside
    contract C4 would do. apply must not read the missing count as 0."""

    def __init__(self, tx):
        self.tx = tx

    def execute(self, sql, params=None, deadline=None):
        self.tx.execute(sql, params, deadline)
        return None

    def __getattr__(self, name):
        return getattr(self.tx, name)


def test_a_missing_count_is_never_read_as_zero(db):
    """Item 3, seeded: with the old `... or 0` an insert-only change set without a check of its count (L2, or K6
    with `check_new_keys` off) reported 0 rows inserted and went on; now apply refuses."""
    with pytest.raises(RuntimeError, match="no row count"):
        _run(db, _BY_NAME["round2_insert_at_a_held_key_unchecked_with_the_switch_off"], "local", "t1", _NoCount)
    create_table(db, SCHEMA, "t2", TK, [])
    with pytest.raises(RuntimeError, match="no row count"):
        with db.transaction() as tx:
            apply_group(_NoCount(tx), Target(SCHEMA, "t2"),
                        LocalChangeSet(change_set_table(TK, R().kl(1, "c", a={0: "5", 1: "'q'"}).out), None, TK),
                        settings=test_settings())


SHAPES_COUNTED = {"K1", "K3", "K6", "K7", "K8", "L1", "L2", "RK1", "RK2", "RD", "T"}


def test_counts_are_read_from_insert_update_and_delete_only(db):
    """Item 3: every statement whose count apply reads is an INSERT, UPDATE or DELETE without RETURNING (the
    client's parser decides, as in D's lane), over cases that reach every counted shape, reduce_duplicates,
    repair_keys and truncate. (Every other apply test runs under the same check: conftest.py.)"""
    seen = []
    with counted_statements(seen):
        for i, name in enumerate(["round2_X1_form_change_then_move_onto_its_old_form", "insert_update_delete",
                                  "keyless_with_a_column_named_rowid", "keyless_update_one_of_three",
                                  "awkward_names"]):
            for form in ("local", "relation"):
                _run(db, _BY_NAME[name], form, "c%d_%s" % (i, form))
        case = _BY_NAME["insert_update_delete"]
        target = Target(SCHEMA, "c%d_local" % 1)
        with db.transaction() as tx:
            reduce_duplicates(tx, target, LocalChangeSet(change_set_table(case.meta, case.sets[0]), None, case.meta),
                              settings=test_settings())
            repair_keys(tx, target, case.meta, tx.fetch_arrow("SELECT * FROM %s" % target.sql()))
            truncate(tx, target)
    shapes = {s for s, _ in seen}
    assert {k for _, k in seen} <= {"INSERT", "UPDATE", "DELETE"}
    assert shapes <= SHAPES_COUNTED and {"K1", "K3", "K6", "K7", "K8", "L1", "L2", "RK1", "RK2", "RD", "T"} <= shapes, \
        shapes


# ------------------------------------------------------------------ 4. registered names


TN = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "v", "VARCHAR")))


def names_gone_case(db, table):
    """Names are `cdc_v2_apply_<step>`; a name registered again in one block (slices of one row, and a second
    change set) gets the new data; after the block no name is left."""
    target = Target(SCHEMA, table)
    create_table(db, SCHEMA, table, TN, [{0: str(i), 1: "'old'"} for i in range(1, 4)])
    first = R().d(1, {0: "1"}).d(1, {0: "2"}).c(1, {0: "11", 1: "'a'"}).c(1, {0: "12", 1: "'b'"}).out
    second = R().d(2, {0: "3"}).c(2, {0: "13", 1: "'c'"}).c(2, {0: "14", 1: "'d'"}).out
    settings = test_settings(statement_rows=1)
    with db.transaction(check=True) as tx:
        for recs in (first, second):
            apply_group(tx, target, LocalChangeSet(change_set_table(TN, recs), None, TN), settings=settings)
    assert all(APPLY_NAME.fullmatch(n) for n in tx.names), tx.names
    assert len(tx.names) > len(set(tx.names)), "a name was registered again in the block"
    assert dest_canon(db, target, TN) == literal_rows_canon(list(TN.columns), [
        {0: "11", 1: "'a'"}, {0: "12", 1: "'b'"}, {0: "13", 1: "'c'"}, {0: "14", 1: "'d'"}])
    gone = {}
    for name in sorted(set(tx.names)):
        with pytest.raises(md.StatementError) as e:
            with db.transaction() as t2:
                t2.fetchall("SELECT count(*) FROM %s" % name)
        gone[name] = e.value.error_class
    return {"names": sorted(set(tx.names)), "registrations": len(tx.names), "gone_after_the_block": gone}


def test_registered_names_are_replaced_and_gone_after_the_block(db):
    names_gone_case(db, "t")


# ------------------------------------------------------------------ 6. what the stand-in hid


TK = TableMeta((Column(0, "a", "BIGINT"), Column(1, "v", "VARCHAR")))


def keyless_relation_case(db, table):
    """A keyless change set in the relation form: its plan counts rows with `sum`, a HUGEINT, which the lane's
    lossless Arrow export hands out as an opaque 16-byte value (on the code before this round: `int()` of those
    bytes failed). The counts come back as numbers."""
    target = Target(SCHEMA, table)
    create_table(db, SCHEMA, table, TK, [{0: "1", 1: "'x'"}, {0: "1", 1: "'x'"}, {0: "2", 1: "'y'"}])
    recs = R().kl(1, "d", b={0: "1", 1: "'x'"}).kl(1, "c", a={0: "3", 1: "'z'"}).out
    with db.transaction() as tx:
        counts = apply_group(tx, target, change_set(db, TK, recs, "relation"), settings=test_settings())
    assert (counts.deleted, counts.inserted) == (1, 1), counts
    assert dest_canon(db, target, TK) == literal_rows_canon(list(TK.columns), [
        {0: "1", 1: "'x'"}, {0: "2", 1: "'y'"}, {0: "3", 1: "'z'"}])
    return {"deleted": counts.deleted, "inserted": counts.inserted}


def test_keyless_relation_form_counts_through_the_lossless_export(db):
    keyless_relation_case(db, "t")


def test_the_lane_exports_hugeint_as_an_opaque_value(db):
    """The fact behind the case above: through D's lane `sum` of a BIGINT is not a number in Arrow data."""
    with db.transaction() as tx:
        t = tx.fetch_arrow("SELECT sum(x) AS s, CAST(sum(x) AS BIGINT) AS b FROM range(3) t(x)")
    assert not pa.types.is_integer(t.schema.field("s").type) and t.schema.field("b").type == pa.int64()
    assert t.column("b").to_pylist() == [3]
