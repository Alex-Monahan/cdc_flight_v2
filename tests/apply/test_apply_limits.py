"""Package A, tier T0: what the caller must keep to (docs/design/apply.md 2.3 and 2.6), shown by the cases that
break when it does not (second fix round, items 3 and 4)."""
from __future__ import annotations

from apply_cases import R
from apply_harness import SCHEMA, apply_sets, dest_canon, key_canon, local_conn, repair
from apply_sim import create_table, literal_rows_canon
from cdc_v2.apply import Column, TableMeta, Target

TV = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "v", "VARCHAR"),
                Column(2, "big", "VARCHAR", unchanged_possible=True)))


def tv(i, v, big):
    return {0: str(i), 1: v, 2: big}


def _table(rows):
    conn = local_conn()
    create_table(conn, SCHEMA, "t", TV, rows)
    return conn, Target(SCHEMA, "t")


def test_only_the_immediate_repeat_of_a_call_is_safe():
    """Item 3: CS1, CS2, CS1 in catch-up gives a wrong table and reports no key. The repeat of CS1 takes `big` from
    the row that CS2 wrote at key 1 (rule 1 sees exactly one row there and no create of CS1 at it). Precondition 1
    of 2.6: a call is repeated only directly after itself, before any later change set is applied."""
    conn, target = _table([tv(1, "'a'", "'B1'")])
    cs1 = R().move(1, {0: "1"}, tv(2, "'a'", None), unchanged=[2]).out
    cs2 = R().c(2, tv(1, "'new'", "'Z'")).out
    calls = apply_sets(conn, target, TV, [cs1, cs2, cs1], mode="catch_up")
    assert all(key_canon(TV, c.unresolved) == set() for c in calls)
    got = dest_canon(conn, target, TV)
    assert got == literal_rows_canon(list(TV.columns), [tv(2, "'a'", "'Z'")])          # wrong, without a sign
    assert got != literal_rows_canon(list(TV.columns), [tv(2, "'a'", "'B1'"), tv(1, "'new'", "'Z'")])
    # the immediate repeat is safe
    conn, target = _table([tv(1, "'a'", "'B1'")])
    apply_sets(conn, target, TV, [cs1, cs1, cs2], mode="catch_up")
    assert dest_canon(conn, target, TV) == literal_rows_canon(list(TV.columns), [tv(2, "'a'", "'B1'"),
                                                                                tv(1, "'new'", "'Z'")])


def _late_repair(replace):
    """Call 1 reports key 1 (no row to take `big` from); call 2, later, writes key 1 whole; then the repair of
    call 1's key reads the source (now the row of call 2) and loads it."""
    conn, target = _table([])
    cs1 = R().u(1, tv(1, "'x'", None), unchanged=[2]).out
    cs2 = R().d(2, {0: "1"}).c(2, tv(1, "'y'", "'B2'")).out
    first, second = apply_sets(conn, target, TV, [cs1, cs2], mode="catch_up")
    assert len(key_canon(TV, first.unresolved)) == 1 and key_canon(TV, second.unresolved) == set()
    repair(conn, target, TV, [tv(1, "'y'", "'B2'")], replace=replace)
    return dest_canon(conn, target, TV)


def test_a_repair_replaces_the_row_of_its_key():
    """Item 4: a repair that runs after a later call has resolved its key. A plain INSERT leaves two rows; delete
    then insert, in the repair's transaction, leaves the source's row."""
    want = literal_rows_canon(list(TV.columns), [tv(1, "'y'", "'B2'")])
    assert _late_repair(replace=True) == want
    plain = _late_repair(replace=False)
    assert plain != want and sum(plain.values()) == 2
