"""Package A, tier T0: the named cases (apply_cases.py) on local DuckDB, through package D's lane. Every case runs
in the local form; the relation form (the same plan, run inside the transaction) runs for every catch-up and error
case, or for all with CDC_V2_APPLY_FULL=1. All cases run in both forms on MotherDuck in test_apply_md.py."""
from __future__ import annotations

import os

import pytest

from apply_cases import all_cases
from apply_harness import local_db, run_case, test_settings

_ALL = all_cases()
_FULL = os.environ.get("CDC_V2_APPLY_FULL") == "1"
CASES = [(c, "local") for c in _ALL] + [(c, "relation") for i, c in enumerate(_ALL)
                                        if _FULL or c.mode == "catch_up" or c.error or "relation" in c.forms]


@pytest.fixture(scope="module")
def shared_db():
    # one local destination for the module (a new one with its leader tables costs about 20 ms), a table per case
    db = local_db(shared=True)
    yield db
    db.close()


_n = [0]


@pytest.mark.parametrize("case,form", CASES, ids=["%s-%s" % (c.name, f) for c, f in CASES])
def test_case(case, form, shared_db):
    _n[0] += 1
    run_case(case, shared_db, form, table="t%d" % _n[0])


_CUT = [c for c in _ALL if c.meta.has_key and c.mode == "stream" and not c.error and not c.settings]


@pytest.mark.parametrize("case", [c for i, c in enumerate(_CUT) if _FULL or i % 5 == 0], ids=lambda c: c.name)
def test_case_cut_at_every_boundary(case):
    """Fix round item 4: the case's records cut at every boundary that keeps a key change's d and c together, each
    piece applied in turn in one transaction, give the expected table (CDC_V2_APPLY_FULL=1: every keyed case)."""
    from apply_harness import SCHEMA, apply_sets, create_table, cut_all, dest_canon
    from apply_sim import literal_rows_canon
    from cdc_v2.apply import Target
    with local_db() as conn:
        create_table(conn, SCHEMA, "t", case.meta, case.start)
        pieces = cut_all([r for s in case.sets for r in s])
        apply_sets(conn, Target(SCHEMA, "t"), case.meta, pieces, one_tx=True)
        assert dest_canon(conn, Target(SCHEMA, "t"), case.meta) == literal_rows_canon(list(case.meta.columns),
                                                                                        case.expect)


def test_record_without_seq_is_refused():
    import pyarrow as pa
    from apply_cases import START, T, R, row
    from apply_sim import change_set_table, create_table
    from cdc_v2.apply import ChangeSetError, LocalChangeSet, Target, apply_group
    rows = change_set_table(T, R().c(1, row(10, "'n'")).c(1, row(11, "'m'")).out)
    rows = rows.set_column(rows.column_names.index("seq"), "seq", pa.array([0, None], type=pa.int64()))
    with local_db() as conn:
        create_table(conn, "s", "t", T, START)
        with pytest.raises(ChangeSetError):
            with conn.transaction() as tx:
                apply_group(tx, Target("s", "t"), LocalChangeSet(rows, None, T), settings=test_settings())


def test_unchanged_flag_is_honoured_even_if_meta_does_not_expect_it():
    """Review finding: a u<i> column that meta does not flag must still keep the value (never an invented NULL)."""
    from apply_cases import R
    from apply_sim import change_set_table, create_table, literal_rows_canon
    from apply_harness import dest_canon
    from cdc_v2.apply import Column, LocalChangeSet, TableMeta, Target, apply_group
    flagged = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "big", "VARCHAR", unchanged_possible=True)))
    plain = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "big", "VARCHAR")))
    rows = change_set_table(flagged, R().u(1, {0: "1", 1: None}, unchanged=[1]).out)
    with local_db() as conn:
        create_table(conn, "s", "t", plain, [{0: "1", 1: "'B'"}])
        with conn.transaction() as tx:
            apply_group(tx, Target("s", "t"), LocalChangeSet(rows, None, plain), settings=test_settings())
        assert dest_canon(conn, Target("s", "t"), plain) == literal_rows_canon(list(plain.columns),
                                                                               [{0: "1", 1: "'B'"}])
