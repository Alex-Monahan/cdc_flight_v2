"""Package A, tier T0: the named cases (apply_cases.py) on local DuckDB. Every case runs in the local form; the
relation form (the same plan, run inside the transaction) runs for every fourth case and for every catch-up and
error case, or for all with CDC_V2_APPLY_FULL=1. All cases run in both forms on MotherDuck in test_apply_md.py."""
from __future__ import annotations

import os

import pytest

from apply_cases import all_cases
from apply_harness import local_conn, run_case

_ALL = all_cases()
_FULL = os.environ.get("CDC_V2_APPLY_FULL") == "1"
CASES = [(c, "local") for c in _ALL] + [(c, "relation") for i, c in enumerate(_ALL)
                                        if _FULL or i % 4 == 0 or c.mode == "catch_up" or c.error]


@pytest.mark.parametrize("case,form", CASES, ids=["%s-%s" % (c.name, f) for c, f in CASES])
def test_case(case, form):
    run_case(case, local_conn(), form)


def test_record_without_seq_is_refused():
    import pyarrow as pa
    from apply_cases import START, T, R, row
    from apply_sim import change_set_table, create_table
    from apply_standin import StandInTx
    from cdc_v2.apply import ChangeSetError, LocalChangeSet, Target, apply_group
    rows = change_set_table(T, R().c(1, row(10, "'n'")).c(1, row(11, "'m'")).out)
    rows = rows.set_column(rows.column_names.index("seq"), "seq", pa.array([0, None], type=pa.int64()))
    conn = local_conn()
    create_table(conn, "s", "t", T, START)
    with pytest.raises(ChangeSetError):
        with StandInTx(conn) as tx:
            apply_group(tx, Target("s", "t"), LocalChangeSet(rows, None, T))


def test_unchanged_flag_is_honoured_even_if_meta_does_not_expect_it():
    """Review finding: a u<i> column that meta does not flag must still keep the value (never an invented NULL)."""
    from apply_cases import R
    from apply_sim import change_set_table, create_table, literal_rows_canon
    from apply_harness import dest_canon
    from apply_standin import StandInTx
    from cdc_v2.apply import Column, LocalChangeSet, TableMeta, Target, apply_group
    flagged = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "big", "VARCHAR", unchanged_possible=True)))
    plain = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "big", "VARCHAR")))
    rows = change_set_table(flagged, R().u(1, {0: "1", 1: None}, unchanged=[1]).out)
    conn = local_conn()
    create_table(conn, "s", "t", plain, [{0: "1", 1: "'B'"}])
    with StandInTx(conn) as tx:
        apply_group(tx, Target("s", "t"), LocalChangeSet(rows, None, plain))
    assert dest_canon(conn, Target("s", "t"), plain) == literal_rows_canon(list(plain.columns), [{0: "1", 1: "'B'"}])
