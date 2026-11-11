"""Named cases for package A, shared by the local run (test_apply_cases.py) and the MotherDuck run
(test_apply_md.py). A case is a table, a start state, change sets of records, and the expected end state (or the
expected error / unresolved keys). Literals are DuckDB SQL; equality is the oracle's (apply_sim.canon)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from apply_sim import NZ, Rec
from cdc_v2.apply import Column, TableMeta

NZF = "CAST('-0' AS FLOAT)"


class R:
    """Record builder: seq numbers in order; txn given per record."""

    def __init__(self):
        self.seq = 0
        self.out: List[Rec] = []

    def _add(self, rec):
        self.out.append(rec)
        self.seq += 1
        return self

    def c(self, txn, vals, unchanged=(), k=None):
        a = dict(vals)
        for p in unchanged:
            a[p] = None
        return self._add(Rec(txn, self.seq, "c", a, {p: True for p in unchanged}, dict(k or {})))

    def u(self, txn, vals, unchanged=()):
        a = dict(vals)
        for p in unchanged:
            a[p] = None
        return self._add(Rec(txn, self.seq, "u", a, {p: True for p in unchanged}))

    def d(self, txn, key):
        return self._add(Rec(txn, self.seq, "d", dict(key)))

    def move(self, txn, old_key, vals, unchanged=()):
        """A key change: d(old) + c(new image, k=old)."""
        self.d(txn, old_key)
        return self.c(txn, vals, unchanged, k=old_key)

    def kl(self, txn, op, a=None, b=None, unchanged=()):
        """A record of a table without a key (b = old image)."""
        aa = dict(a or {})
        for p in unchanged:
            aa[p] = None
        return self._add(Rec(txn, self.seq, op, aa, {p: True for p in unchanged}, {}, dict(b or {})))


@dataclass
class Case:
    name: str
    meta: TableMeta
    start: List[Dict[int, Optional[str]]]
    sets: List[List[Rec]]
    expect: Optional[List[Dict[int, Optional[str]]]] = None
    mode: str = "stream"
    error: Optional[str] = None                         # 'invariant' | 'changeset'
    unresolved: Optional[List[tuple]] = None            # catch_up: expected unresolved keys (literals of key cols)
    one_tx: bool = False
    forms: tuple = ("local",)
    max_statements: Optional[int] = None                # per change set, local form
    twice: bool = False                                 # catch_up: apply again, the table must not change
    settings: Optional[dict] = None                     # ApplySettings fields other than the defaults of the tests


# the standard keyed table: id key, `big` can be unchanged (TOAST), d DOUBLE (not), iv INTERVAL (can be unchanged)
T = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "big", "VARCHAR", unchanged_possible=True),
               Column(2, "d", "DOUBLE"), Column(3, "iv", "INTERVAL", unchanged_possible=True)))


def row(i, big, d="1.5", iv="INTERVAL '1 day'"):
    return {0: str(i), 1: big, 2: d, 3: iv}


def K(i):
    return {0: str(i)}


START = [row(1, "'B1'"), row(2, "'B2'"), row(3, "'B3'")]


def keyed_cases() -> List[Case]:
    cs = []
    r = R().c(1, row(10, "'n'")).u(1, row(2, "'B2x'", "2.5")).d(1, K(3))
    cs.append(Case("insert_update_delete", T, START, [r.out],
                   [row(1, "'B1'"), row(2, "'B2x'", "2.5"), row(10, "'n'")], max_statements=2))
    # an insert-only change set: K6, plus K1 as the check that the new keys have no row (check_new_keys)
    r = R().c(1, row(10, "'n'")).c(1, row(11, "'m'"))
    cs.append(Case("insert_only_two_statements", T, START, [r.out], START + [row(10, "'n'"), row(11, "'m'")],
                   max_statements=2))
    # stream mode: the destination equals the source at the start, so a delete or update of a missing row is a
    # divergence; catch-up: the table was loaded separately, the row may be missing
    r = R().d(1, K(99))
    cs.append(Case("stream_delete_of_a_missing_row_raises", T, START, [r.out], error="invariant",
                   forms=("local", "relation")))
    cs.append(Case("catch_up_delete_of_a_missing_row", T, START, [r.out], START, mode="catch_up", unresolved=[]))
    r = R().u(1, row(2, None, "9.0", "INTERVAL '2 days'"), unchanged=[1])
    cs.append(Case("partial_update_from_destination", T, START, [r.out],
                   [row(1, "'B1'"), row(2, "'B2'", "9.0", "INTERVAL '2 days'"), row(3, "'B3'")]))
    r = R().c(1, row(10, "'n'")).u(1, row(10, None, "7.0"), unchanged=[1])
    cs.append(Case("insert_then_partial_update", T, START, [r.out], START + [row(10, "'n'", "7.0")]))
    r = (R().u(1, row(2, "'X'", "3.0", None), unchanged=[3]).u(1, row(2, None, "4.0", "INTERVAL '9 days'"), unchanged=[1])
         .u(2, row(2, None, "5.0", None), unchanged=[1, 3]))
    cs.append(Case("consecutive_partial_updates", T, START, [r.out],
                   [row(1, "'B1'"), row(2, "'X'", "5.0", "INTERVAL '9 days'"), row(3, "'B3'")]))
    r = R().move(1, K(1), row(5, None, "1.5", None), unchanged=[1, 3])
    cs.append(Case("key_change_takes_unchanged_from_destination", T, START, [r.out],
                   [row(5, "'B1'"), row(2, "'B2'"), row(3, "'B3'")]))
    r = R().move(1, K(1), row(5, None, "2.0"), unchanged=[1]).move(1, K(5), row(6, None, "3.0"), unchanged=[1])
    cs.append(Case("chain_a_b_c_one_transaction", T, START, [r.out], [row(6, "'B1'", "3.0"), row(2, "'B2'"), row(3, "'B3'")]))
    r1 = R().move(1, K(1), row(5, None, "2.0"), unchanged=[1])
    r2 = R().move(2, K(5), row(6, None, "3.0"), unchanged=[1])
    cs.append(Case("chain_across_two_change_sets", T, START, [r1.out, r2.out],
                   [row(6, "'B1'", "3.0"), row(2, "'B2'"), row(3, "'B3'")]))
    cs.append(Case("chain_across_two_change_sets_one_transaction", T, START, [r1.out, r2.out],
                   [row(6, "'B1'", "3.0"), row(2, "'B2'"), row(3, "'B3'")], one_tx=True))
    r = R().move(1, K(2), row(7, None), unchanged=[1]).move(1, K(1), row(2, None), unchanged=[1])
    cs.append(Case("workload_chain_b_new_then_a_b", T, START, [r.out], [row(7, "'B2'"), row(2, "'B1'"), row(3, "'B3'")]))
    r = (R().move(1, K(1), row(9, None), unchanged=[1]).move(1, K(2), row(1, None), unchanged=[1])
         .move(1, K(9), row(2, None), unchanged=[1]))
    cs.append(Case("swap_through_a_third_key", T, START, [r.out], [row(1, "'B2'"), row(2, "'B1'"), row(3, "'B3'")]))
    r = (R().move(1, K(1), row(9, None), unchanged=[1]).move(1, K(2), row(1, None), unchanged=[1])
         .move(1, K(3), row(2, None), unchanged=[1]).move(1, K(9), row(3, None), unchanged=[1]))
    cs.append(Case("rotation_through_a_free_key", T, START, [r.out], [row(1, "'B2'"), row(2, "'B3'"), row(3, "'B1'")]))
    r = R().move(1, K(1), row(5, None), unchanged=[1]).move(1, K(5), row(6, None), unchanged=[1]).d(1, K(2)).c(
        1, row(2, "'again'"))
    cs.append(Case("chain_and_key_reuse_in_one_transaction", T, START, [r.out],
                   [row(6, "'B1'"), row(2, "'again'"), row(3, "'B3'")]))
    # Not a keyed source history: an immediate key never has two holders (rule K, H-015). A direct swap (only a
    # deferrable key allows it, and such a table is keyless here) arrives as d(1) c(2, k=1) while 2 holds a row.
    # The first record of key 2 is a create, so the destination must have no row 2: stream mode finds one (K1).
    r = R().move(1, K(1), row(2, None), unchanged=[1]).move(1, K(2), row(1, None), unchanged=[1])
    cs.append(Case("direct_swap_is_not_a_keyed_history", T, START, [r.out], error="invariant"))
    r = (R().move(1, K(3), row(1, None, "0.5"), unchanged=[1]).move(1, K(1), row(2, None), unchanged=[1])
         .move(1, K(2), row(3, None), unchanged=[1]))
    cs.append(Case("direct_rotation_is_not_a_keyed_history", T, START, [r.out], error="invariant"))
    # the second review's counterexamples A1, A4 and SEG, now loud: each puts a create at a key that holds a row
    TV = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "v", "VARCHAR"),
                    Column(2, "big", "VARCHAR", unchanged_possible=True)))
    r = R().c(1, {0: "2", 1: "'new'", 2: "'Bn'"}).u(1, {0: "2", 1: "'new!'", 2: None}, unchanged=[2]).d(1, {0: "2"})
    cs.append(Case("review_A1_create_at_a_held_key", TV, [{0: "2", 1: "'old'", 2: "'Bo'"}], [r.out], error="invariant"))
    a4 = [{0: "1", 1: "'a'", 2: "'Ba'"}, {0: "2", 1: "'b'", 2: "'Bb'"}]
    r = (R().move(1, {0: "1"}, {0: "2", 1: "'a'", 2: None}, unchanged=[2]).u(1, {0: "2", 1: "'x'", 2: "'NEW'"})
         .move(1, {0: "2"}, {0: "1", 1: "'b'", 2: None}, unchanged=[2]))
    cs.append(Case("review_A4_create_at_a_held_key", TV, a4, [r.out], error="invariant"))
    r1 = R().move(1, {0: "1"}, {0: "2", 1: "'a'", 2: "'Ba'"})
    r2 = R().move(1, {0: "2"}, {0: "1", 1: "'b'", 2: "'Bb'"})
    cs.append(Case("review_SEG_create_at_a_held_key_across_a_cut", TV, a4, [r1.out, r2.out], error="invariant",
                   one_tx=True))
    r = R().c(1, row(10, "'a'")).d(1, K(10)).d(1, K(10))
    cs.append(Case("delete_twice_is_not_a_history", T, START, [r.out], error="invariant"))
    cs.append(Case("delete_twice_is_not_a_history_catch_up", T, START, [r.out], error="invariant", mode="catch_up"))
    r = R().d(1, K(1)).u(1, row(1, "'z'"))
    cs.append(Case("update_after_delete_is_not_a_history", T, START, [r.out], error="invariant",
                   forms=("local", "relation")))
    r = R().move(1, K(2), row(2, None, "8.0"), unchanged=[1])
    cs.append(Case("key_change_back_to_the_same_key", T, START, [r.out], [row(1, "'B1'"), row(2, "'B2'", "8.0"), row(3, "'B3'")]))
    r = R().d(1, K(2)).move(1, K(1), row(2, None), unchanged=[1])
    cs.append(Case("key_change_onto_a_deleted_key", T, START, [r.out], [row(2, "'B1'"), row(3, "'B3'")]))
    r1 = R().d(1, K(2))
    r2 = R().move(2, K(1), row(2, None), unchanged=[1])
    cs.append(Case("key_change_onto_a_key_deleted_in_the_previous_set", T, START, [r1.out, r2.out],
                   [row(2, "'B1'"), row(3, "'B3'")]))
    r = R().c(1, row(10, "'t'")).u(1, row(10, None, "2.0"), unchanged=[1]).d(1, K(10))
    cs.append(Case("insert_update_delete_in_one_transaction", T, START, [r.out], START))
    r = R().move(1, K(1), row(10, None), unchanged=[1]).d(1, K(10))
    cs.append(Case("move_then_delete_in_one_transaction", T, START, [r.out], [row(2, "'B2'"), row(3, "'B3'")]))
    r = R().u(1, row(42, "'full'", "1.0", "INTERVAL '1 hour'"))
    cs.append(Case("stream_full_update_of_a_missing_row_raises", T, START, [r.out], error="invariant"))
    cs.append(Case("catch_up_full_update_of_a_missing_row_upserts", T, START, [r.out],
                   START + [row(42, "'full'", "1.0", "INTERVAL '1 hour'")], mode="catch_up", unresolved=[]))
    r = R().u(1, row(42, None), unchanged=[1])
    cs.append(Case("stream_partial_update_of_a_missing_row_raises", T, START, [r.out], error="invariant"))
    r = R().move(1, K(42), row(43, None), unchanged=[1])
    cs.append(Case("stream_key_change_of_a_missing_row_raises", T, START, [r.out], error="invariant"))
    r1 = R().c(1, row(10, "'a'"))
    r2 = R().c(2, row(10, "'b'"))
    cs.append(Case("stream_insert_twice_raises", T, START, [r1.out + r2.out], error="invariant"))
    cs.append(Case("insert_twice_raises_in_catch_up", T, START, [r1.out + r2.out], error="invariant", mode="catch_up"))
    r = R().c(1, row(3, "'again'"))
    cs.append(Case("stream_insert_at_a_held_key_raises", T, START, [r.out], error="invariant",
                   forms=("local", "relation")))
    r = R().c(1, row(5, "'x'"))
    r.out[0].op = "t"
    cs.append(Case("truncate_record_is_refused", T, START, [r.out], error="changeset"))
    # catch-up
    r = R().u(1, row(1, None, "2.0"), unchanged=[1]).u(1, row(42, None, "2.0"), unchanged=[1]).c(1, row(50, "'z'"))
    cs.append(Case("catch_up_reports_a_key_it_cannot_resolve", T, START, [r.out],
                   [row(1, "'B1'", "2.0"), row(2, "'B2'"), row(3, "'B3'"), row(50, "'z'")], mode="catch_up",
                   unresolved=[("42",)], twice=True))
    r = R().move(1, K(7), row(1, None, "6.0"), unchanged=[1])
    cs.append(Case("catch_up_takes_the_value_under_the_new_key", T, START, [r.out],
                   [row(1, "'B1'", "6.0"), row(2, "'B2'"), row(3, "'B3'")], mode="catch_up", unresolved=[], twice=True))
    r = (R().move(1, K(1), row(5, None, "2.0"), unchanged=[1]).move(2, K(5), row(6, None, "3.0"), unchanged=[1])
         .u(3, row(6, None, "4.0"), unchanged=[1]))
    cs.append(Case("catch_up_chain_is_idempotent", T, START, [r.out], [row(6, "'B1'", "4.0"), row(2, "'B2'"), row(3, "'B3'")],
                   mode="catch_up", unresolved=[], twice=True))
    # c(10) d(10): the first record fixes p = 0, so nothing is unresolved
    r = R().c(1, row(10, "'t'")).u(1, row(10, None, "2.0"), unchanged=[1]).d(1, K(10))
    cs.append(Case("catch_up_insert_and_delete_leave_nothing_unresolved", T, START, [r.out], START, mode="catch_up",
                   unresolved=[], twice=True))
    # a chain 1 -> 5 -> 6 in one transaction resolves by rule 1 (root 1 is in the table and the log creates nothing
    # at 1)
    r = R().move(1, K(1), row(5, None, "2.0"), unchanged=[1]).move(1, K(5), row(6, None, "3.0"), unchanged=[1])
    cs.append(Case("catch_up_chain_in_one_transaction_resolves", T, START, [r.out],
                   [row(6, "'B1'", "3.0"), row(2, "'B2'"), row(3, "'B3'")], mode="catch_up", unresolved=[], twice=True))
    # A swap through a free key does not resolve by rule 1: the log creates rows at both roots. Why that rule is
    # needed: the table can hold the rows of a snapshot taken after the swap (start below), where row 1 holds B2;
    # taking key 2's unchanged value from row 2 would give B1 to the row that must hold B2. Both keys are reported.
    swap = (R().move(1, K(1), row(9, None), unchanged=[1]).move(1, K(2), row(1, None), unchanged=[1])
            .move(1, K(9), row(2, None), unchanged=[1]))
    cs.append(Case("catch_up_swap_through_a_free_key_after_the_swap", T, [row(1, "'B2'"), row(2, "'B1'"), row(3, "'B3'")],
                   [swap.out], [row(3, "'B3'")], mode="catch_up", unresolved=[("1",), ("2",)]))
    # a snapshot that already holds the row under its new key, and loses nothing when applied twice
    r = (R().move(1, K(1), row(5, None, "2.0"), unchanged=[1]).move(2, K(5), row(6, None, "3.0"), unchanged=[1])
         .u(3, row(6, None, "4.0"), unchanged=[1]))
    cs.append(Case("catch_up_snapshot_after_the_move", T, [row(6, "'B1'", "3.0"), row(2, "'B2'"), row(3, "'B3'")],
                   [r.out], [row(6, "'B1'", "4.0"), row(2, "'B2'"), row(3, "'B3'")], mode="catch_up", unresolved=[],
                   twice=True))
    return cs


# special values: -0 / 0 and equal intervals of another form, as complete and as partial images
S = TableMeta((Column(0, "id", "INTEGER", key=True), Column(1, "big", "VARCHAR", unchanged_possible=True),
               Column(2, "d", "DOUBLE"), Column(3, "f", "FLOAT"), Column(4, "iv", "INTERVAL", unchanged_possible=True),
               Column(5, "st", "STRUCT(a DOUBLE, b VARCHAR)", unchanged_possible=True),
               Column(6, "l", "DOUBLE[]", unchanged_possible=True),
               Column(7, "un", "UNION(d DOUBLE, i BIGINT)")))


def srow(i, big="'b'", d=NZ, f=NZF, iv="INTERVAL '1 month'", st="{'a': %s, 'b': 'x'}" % NZ, l="[%s]" % NZ,
         un="union_value(d := %s)" % NZ):
    return {0: str(i), 1: big, 2: d, 3: f, 4: iv, 5: st, 6: l, 7: un}


def special_cases() -> List[Case]:
    cs = []
    to_pos = dict(d="0.0::DOUBLE", f="0.0::FLOAT", iv="INTERVAL '30 days'", st="{'a': 0.0, 'b': 'x'}", l="[0.0::DOUBLE]",
                  un="union_value(d := 0.0::DOUBLE)")
    start = [srow(1), srow(2), srow(3, d="0.0::DOUBLE", f="0.0::FLOAT", iv="INTERVAL '30 days'",
                                    st="{'a': 0.0, 'b': 'x'}", l="[0.0::DOUBLE]", un="union_value(d := 0.0::DOUBLE)")]
    r = R().u(1, srow(1, **to_pos))
    cs.append(Case("negative_zero_and_interval_form_complete_image", S, start, [r.out], [srow(1, **to_pos), start[1], start[2]]))
    r = R().u(1, srow(2, big=None, **to_pos), unchanged=[1])
    cs.append(Case("negative_zero_and_interval_form_partial_image", S, start, [r.out], [start[0], srow(2, **to_pos), start[2]]))
    back = dict(d=NZ, f=NZF, iv="INTERVAL '720 hours'", st="{'a': %s, 'b': 'x'}" % NZ, l="[%s]" % NZ,
                un="union_value(d := %s)" % NZ)
    r = R().u(1, srow(3, big=None, **back), unchanged=[1])
    cs.append(Case("zero_to_negative_zero_partial_image", S, start, [r.out], [start[0], start[1], srow(3, **back)]))
    # a NULL that hides the old data slot: d set to NULL, then to 0 over a slot holding -0 (two change sets)
    r1 = R().u(1, srow(1, big=None, d="NULL", iv="NULL"), unchanged=[1])
    r2 = R().u(2, srow(1, big=None, d="0.0::DOUBLE", iv="INTERVAL '30 days'"), unchanged=[1])
    cs.append(Case("null_then_zero_over_a_negative_zero_slot", S, start, [r1.out, r2.out],
                   [srow(1, d="0.0::DOUBLE", iv="INTERVAL '30 days'"), start[1], start[2]]))
    cs.append(Case("null_then_zero_catch_up", S, start, [r1.out + r2.out],
                   [srow(1, d="0.0::DOUBLE", iv="INTERVAL '30 days'"), start[1], start[2]], mode="catch_up",
                   unresolved=[], twice=True))
    # -0 / 0 and interval form as keys, moved with an unchanged column (the copy path)
    KD = TableMeta((Column(0, "k", "DOUBLE", key=True), Column(1, "big", "VARCHAR", unchanged_possible=True)))
    r = R().move(1, {0: NZ}, {0: "0.0::DOUBLE", 1: None}, unchanged=[1])
    cs.append(Case("double_key_negative_zero_to_zero_with_unchanged", KD, [{0: NZ, 1: "'B'"}, {0: "1.0::DOUBLE", 1: "'C'"}],
                   [r.out], [{0: "0.0::DOUBLE", 1: "'B'"}, {0: "1.0::DOUBLE", 1: "'C'"}]))
    r = R().move(1, {0: NZ}, {0: "0.0::DOUBLE", 1: "'B2'"})
    cs.append(Case("double_key_negative_zero_to_zero_complete", KD, [{0: NZ, 1: "'B'"}], [r.out], [{0: "0.0::DOUBLE", 1: "'B2'"}]))
    KI = TableMeta((Column(0, "k", "INTERVAL", key=True), Column(1, "big", "VARCHAR", unchanged_possible=True)))
    r = R().move(1, {0: "INTERVAL '1 month'"}, {0: "INTERVAL '30 days'", 1: None}, unchanged=[1])
    cs.append(Case("interval_key_change_of_form_with_unchanged", KI, [{0: "INTERVAL '1 month'", 1: "'B'"},
                                                                     {0: "INTERVAL '720 hours'", 1: "'C'"}],
                   [r.out], [{0: "INTERVAL '30 days'", 1: "'B'"}, {0: "INTERVAL '720 hours'", 1: "'C'"}]))
    r = R().d(1, {0: "INTERVAL '720 hours'"})
    cs.append(Case("interval_key_delete_removes_only_its_form", KI, [{0: "INTERVAL '30 days'", 1: "'B'"},
                                                                    {0: "INTERVAL '720 hours'", 1: "'C'"}],
                   [r.out], [{0: "INTERVAL '30 days'", 1: "'B'"}]))
    # found by the review: a composite key whose DOUBLE part changes only in form while the other part changes
    KC = TableMeta((Column(0, "id", "INTEGER", key=True), Column(1, "d", "DOUBLE", key=True),
                    Column(2, "v", "VARCHAR", unchanged_possible=True)))
    r = R().move(1, {0: "1", 1: NZ}, {0: "2", 1: "0.0::DOUBLE", 2: None}, unchanged=[2])
    cs.append(Case("composite_key_with_a_double_part_changing_form", KC, [{0: "1", 1: NZ, 2: "'x'"}], [r.out],
                   [{0: "2", 1: "0.0::DOUBLE", 2: "'x'"}], forms=("local", "relation")))
    r1 = R().move(1, {0: "1", 1: NZ}, {0: "2", 1: "0.0::DOUBLE", 2: None}, unchanged=[2])
    r2 = R().u(2, {0: "2", 1: "0.0::DOUBLE", 2: "'y'"})
    cs.append(Case("composite_key_changed_form_then_updated", KC, [{0: "1", 1: NZ, 2: "'x'"}], [r1.out, r2.out],
                   [{0: "2", 1: "0.0::DOUBLE", 2: "'y'"}]))
    # found by the review: a DATE beyond the TIMESTAMP range (Postgres dates reach 5874897 AD)
    KF = TableMeta((Column(0, "k", "DATE", key=True), Column(1, "v", "VARCHAR", unchanged_possible=True)))
    far = [{0: "'300000-01-01'::DATE", 1: "'far'"}, {0: "'2020-01-01'::DATE", 1: "'near'"}]
    r = R().u(1, {0: "'2020-01-01'::DATE", 1: None}, unchanged=[1]).u(1, {0: "'300000-01-01'::DATE", 1: "'far2'"})
    cs.append(Case("date_key_beyond_the_timestamp_range", KF, far, [r.out],
                   [{0: "'300000-01-01'::DATE", 1: "'far2'"}, {0: "'2020-01-01'::DATE", 1: "'near'"}],
                   forms=("local", "relation")))
    # The second review's B: d(-0) c(0, k=-0) and d(5) c(-0, k=5). Not a source history: for Postgres -0 = 0, so
    # moving 5 onto -0 while 0 holds a row is a unique violation. Pinned: the copy path's count finds it.
    r = R().move(1, {0: NZ}, {0: "0.0::DOUBLE", 1: None}, unchanged=[1]).move(1, {0: "5.0::DOUBLE"},
                                                                              {0: NZ, 1: None}, unchanged=[1])
    cs.append(Case("review_B_two_forms_of_one_key_held_at_once", KD, [{0: NZ, 1: "'A'"}, {0: "5.0::DOUBLE", 1: "'B'"}],
                   [r.out], error="invariant"))
    r = R().d(1, {0: "0.0::DOUBLE"}).u(1, {0: NZ, 1: "'X'"})
    cs.append(Case("double_keys_negative_zero_and_zero_are_two_keys", KD, [{0: NZ, 1: "'B'"}, {0: "0.0::DOUBLE", 1: "'C'"}],
                   [r.out], [{0: NZ, 1: "'X'"}]))
    return cs


def type_key_cases() -> List[Case]:
    """Every key type of the simulator: insert, partial update, key change with an unchanged value, delete."""
    from apply_sim import SPECS
    cs = []
    for kind in ("int", "bigint", "decimal", "double", "varchar", "blob", "uuid", "date", "ts", "timetz", "interval"):
        spec = SPECS[kind]
        # the keys the change set ends with are distinct at the source (1 mon -30 days and 0 seconds are one key)
        k = spec.keys if kind != "interval" else spec.keys[:4] + spec.keys[5:]
        meta = TableMeta((Column(0, "k", spec.type, key=True), Column(1, "big", "VARCHAR", unchanged_possible=True)))
        start = [{0: k[0], 1: "'a'"}, {0: k[1], 1: "'b'"}, {0: k[2], 1: "'c'"}]
        r = (R().c(1, {0: k[3], 1: "'n'"}).u(1, {0: k[0], 1: None}, unchanged=[1]).move(1, {0: k[1]}, {0: k[4], 1: None}, unchanged=[1])
             .d(1, {0: k[2]}))
        cs.append(Case("key_type_" + kind, meta, start, [r.out], [{0: k[0], 1: "'a'"}, {0: k[4], 1: "'b'"}, {0: k[3], 1: "'n'"}]))
    # a composite key
    meta = TableMeta((Column(0, "a", "INTEGER", key=True), Column(5, "b", "VARCHAR", key=True),
                      Column(2, "big", "VARCHAR", unchanged_possible=True)))
    start = [{0: "1", 5: "'x'", 2: "'A'"}, {0: "1", 5: "'y'", 2: "'B'"}]
    r = R().d(1, {0: "1", 5: "'y'"}).move(1, {0: "1", 5: "'x'"}, {0: "1", 5: "'y'", 2: None}, unchanged=[2])
    cs.append(Case("composite_key_move_onto_deleted", meta, start, [r.out], [{0: "1", 5: "'y'", 2: "'A'"}]))
    # ±infinity keys written and then deleted or changed in one transaction (two change sets): DuckDB's join filter
    # pushdown misses them unless the join is on an expression (found by the property test, seed 2273038)
    for typ, lo, hi in [("DATE", "'-infinity'::DATE", "'infinity'::DATE"),
                        ("TIMESTAMP", "'-infinity'::TIMESTAMP", "'infinity'::TIMESTAMP"),
                        ("TIMESTAMPTZ", "'-infinity'::TIMESTAMPTZ", "'infinity'::TIMESTAMPTZ")]:
        inf = TableMeta((Column(0, "k", typ, key=True), Column(1, "big", "VARCHAR", unchanged_possible=True)))
        r1 = R().c(1, {0: lo, 1: "'a'"}).c(1, {0: hi, 1: "'b'"})
        r2 = R().d(2, {0: lo}).u(2, {0: hi, 1: None}, unchanged=[1]).move(2, {0: hi}, {0: lo, 1: None}, unchanged=[1])
        cs.append(Case("infinite_%s_keys_in_one_transaction" % typ.lower(), inf, [], [r1.out, r2.out],
                       [{0: lo, 1: "'b'"}], one_tx=True, forms=("local", "relation")))
    # UUID and JSON keys written and then deleted in one transaction: DuckDB pushes the join filter into the Arrow
    # scan, which cannot filter these extension types (found by the campaign, seeds 10002906, 10005444, 10005702)
    for typ, lo, hi in [("UUID", "'00000000-0000-0000-0000-000000000001'::UUID", "'ffffffff-0000-0000-0000-000000000002'::UUID"),
                        ("JSON", "'{\"a\":1}'::JSON", "'[]'::JSON")]:
        ux = TableMeta((Column(0, "k", typ, key=True), Column(1, "x", "TIMETZ", key=True),
                        Column(2, "big", "VARCHAR", unchanged_possible=True)))
        r1 = R().c(1, {0: lo, 1: "'12:00:00+01'::TIMETZ", 2: "'a'"}).c(1, {0: hi, 1: "'12:00:00+01'::TIMETZ", 2: "'b'"})
        r2 = R().d(2, {0: lo, 1: "'12:00:00+01'::TIMETZ"}).u(2, {0: hi, 1: "'12:00:00+01'::TIMETZ", 2: None}, unchanged=[2])
        cs.append(Case("%s_keys_in_one_transaction" % typ.lower(), ux, [], [r1.out, r2.out],
                       [{0: hi, 1: "'12:00:00+01'::TIMETZ", 2: "'b'"}], one_tx=True, forms=("local", "relation")))
    KJ = TableMeta((Column(0, "j", "JSON"), Column(1, "u", "UUID")))
    r1 = R().kl(1, "c", a={0: "'{\"a\":1}'::JSON", 1: "'00000000-0000-0000-0000-000000000001'::UUID"})
    r2 = R().kl(2, "d", b={0: "'{\"a\":1}'::JSON", 1: "'00000000-0000-0000-0000-000000000001'::UUID"})
    cs.append(Case("keyless_json_uuid_in_one_transaction", KJ, [], [r1.out, r2.out], [], one_tx=True,
                   forms=("local", "relation")))
    # TIME WITH TIME ZONE keys: equal instants with other offsets are other keys, and a key travels back intact
    TZ = TableMeta((Column(0, "k", "TIMETZ", key=True), Column(1, "big", "VARCHAR", unchanged_possible=True)))
    tz_start = [{0: "'11:00:00+00'::TIMETZ", 1: "'a'"}]
    r = R().c(1, {0: "'12:00:00+01'::TIMETZ", 1: "'t'"}).d(1, {0: "'12:00:00+01'::TIMETZ"})
    cs.append(Case("timetz_key_lookup_keeps_the_offset", TZ, tz_start, [r.out], tz_start, forms=("local", "relation")))
    r = R().u(1, {0: "'12:00:00+01'::TIMETZ", 1: None}, unchanged=[1])
    cs.append(Case("timetz_key_unresolved_comes_back_with_its_offset", TZ, tz_start, [r.out], tz_start,
                   mode="catch_up", unresolved=[("'12:00:00+01'::TIMETZ",)], forms=("local", "relation")))
    # awkward destination names, including rowid and our own column names
    names = ["rowid", 'a"b', "x0", "a1", "n", "t", "with space", "select", "ä€", " ", "a.b", "tab\tname", "Count"]
    cols = [Column(0, names[0], "BIGINT", key=True)] + [Column(i, n, "VARCHAR", unchanged_possible=(i % 2 == 1))
                                                         for i, n in enumerate(names[1:], start=1)]
    meta = TableMeta(tuple(cols))
    full = lambda key, tag: {0: str(key), **{i: "'%s%d'" % (tag, i) for i in range(1, len(names))}}
    unch = [i for i in range(1, len(names)) if i % 2 == 1]
    over = lambda base, vals: {**base, **vals}
    r = R().c(1, full(3, "c")).u(1, over(full(1, "u"), {i: None for i in unch}), unchanged=unch).move(
        1, {0: "2"}, over(full(4, "m"), {i: None for i in unch}), unchanged=unch)
    expect = [over(full(1, "u"), {i: "'s%d'" % i for i in unch}), over(full(4, "m"), {i: "'s%d'" % i for i in unch}),
              full(3, "c")]
    cs.append(Case("awkward_names", meta, [full(1, "s"), full(2, "s")], [r.out], expect))
    return cs


# tables without a key
KL = TableMeta((Column(0, "a", "INTEGER"), Column(1, "big", "VARCHAR", unchanged_possible=True),
                Column(2, "d", "DOUBLE"), Column(3, "iv", "INTERVAL"), Column(4, "un", "UNION(d DOUBLE, i BIGINT)"),
                Column(5, "l", "DOUBLE[]")))


def kv(a, big="'x'", d="1.0::DOUBLE", iv="INTERVAL '1 day'", un="union_value(i := 1)", l="[1.0::DOUBLE]"):
    return {0: a, 1: big, 2: d, 3: iv, 4: un, 5: l}


def keyless_cases() -> List[Case]:
    cs = []
    three = [kv("1"), kv("1"), kv("1"), kv("2")]
    r = R().kl(1, "d", b=kv("1"))
    cs.append(Case("keyless_delete_one_of_three", KL, three, [r.out], [kv("1"), kv("1"), kv("2")], max_statements=1))
    r = R().kl(1, "u", a=kv("9"), b=kv("1"))
    cs.append(Case("keyless_update_one_of_three", KL, three, [r.out], [kv("1"), kv("1"), kv("9"), kv("2")]))
    r = R().kl(1, "d", b=kv("1")).kl(1, "d", b=kv("1")).kl(1, "d", b=kv("1")).kl(1, "d", b=kv("1"))
    cs.append(Case("keyless_delete_more_than_present_raises", KL, three, [r.out], error="invariant"))
    r = R().kl(1, "d", b=kv("3"))
    cs.append(Case("keyless_delete_missing_row_raises_in_catch_up_too", KL, three, [r.out], error="invariant", mode="catch_up"))
    z = [kv("1", d="0.0::DOUBLE"), kv("1", d=NZ)]
    r = R().kl(1, "d", b=kv("1", d=NZ))
    cs.append(Case("keyless_negative_zero_image_never_deletes_zero", KL, z, [r.out], [kv("1", d="0.0::DOUBLE")]))
    r = R().kl(1, "u", a=kv("1", d="0.0::DOUBLE"), b=kv("1", d=NZ))
    cs.append(Case("keyless_negative_zero_to_zero_does_not_net_to_nothing", KL, [kv("1", d=NZ)], [r.out],
                   [kv("1", d="0.0::DOUBLE")]))
    iv = [kv("1", iv="INTERVAL '1 month'"), kv("1", iv="INTERVAL '30 days'")]
    r = R().kl(1, "d", b=kv("1", iv="INTERVAL '30 days'"))
    cs.append(Case("keyless_interval_forms_are_different_rows", KL, iv, [r.out], [kv("1", iv="INTERVAL '1 month'")]))
    r = R().kl(1, "d", b=kv("1", d="'nan'::DOUBLE"))
    cs.append(Case("keyless_nan_equals_nan", KL, [kv("1", d="(-('nan'::DOUBLE))")], [r.out], []))
    nul = [{0: None, 1: None, 2: None, 3: None, 4: None, 5: None}] * 2
    r = R().kl(1, "d", b=nul[0])
    cs.append(Case("keyless_null_equals_null", KL, nul, [r.out], nul[:1]))
    un = [kv("1", un="union_value(i := 0)"), kv("1", un="union_value(d := 0.0::DOUBLE)")]
    r = R().kl(1, "d", b=kv("1", un="union_value(d := 0.0::DOUBLE)"))
    cs.append(Case("keyless_union_members_differ", KL, un, [r.out], [kv("1", un="union_value(i := 0)")]))
    li = [kv("1", l="[%s, NULL]" % NZ), kv("1", l="[0.0::DOUBLE, NULL]")]
    r = R().kl(1, "d", b=kv("1", l="[0.0::DOUBLE, NULL]"))
    cs.append(Case("keyless_list_elements_compare_exactly", KL, li, [r.out], [kv("1", l="[%s, NULL]" % NZ)]))
    r = R().kl(1, "c", a=kv("5")).kl(1, "c", a=kv("5")).kl(2, "d", b=kv("5")).kl(2, "d", b=kv("1"))
    cs.append(Case("keyless_insert_then_delete_in_the_same_set", KL, three, [r.out], [kv("1"), kv("1"), kv("5"), kv("2")]))
    r1 = R().kl(1, "c", a=kv("5")).kl(1, "c", a=kv("5"))
    r2 = R().kl(2, "d", b=kv("5")).kl(2, "u", a=kv("6"), b=kv("5"))
    cs.append(Case("keyless_rows_inserted_by_the_same_transaction", KL, three, [r1.out, r2.out], three + [kv("6")], one_tx=True))
    r = R().kl(1, "u", a=kv("1", big=None, d="5.0::DOUBLE"), b=kv("1", big="'x'"), unchanged=[1])
    cs.append(Case("keyless_unchanged_value_comes_from_the_old_image", KL, three, [r.out],
                   [kv("1"), kv("1"), kv("1", d="5.0::DOUBLE"), kv("2")]))
    KLD = TableMeta((Column(0, "dt", "DATE"), Column(1, "v", "VARCHAR")))
    r = R().kl(1, "d", b={0: "'300000-01-01'::DATE", 1: "'a'"})
    cs.append(Case("keyless_date_beyond_the_timestamp_range", KLD, [{0: "'300000-01-01'::DATE", 1: "'a'"}] * 2,
                   [r.out], [{0: "'300000-01-01'::DATE", 1: "'a'"}]))
    RW = TableMeta((Column(0, "rowid", "INTEGER"), Column(1, "v", "VARCHAR")))
    r = R().kl(1, "d", b={0: "1", 1: "'a'"})
    cs.append(Case("keyless_with_a_column_named_rowid", RW, [{0: "1", 1: "'a'"}] * 3, [r.out], [{0: "1", 1: "'a'"}] * 2))
    return cs


def round2_cases() -> List[Case]:
    """The second fix round: X1 and its variants (a form-only key change next to a move onto its old form), the
    per-key count checks (report 4b), the new-key check inside the statements (item 9) and catch-up rule 1 for
    d(K) c(K, k=K) (item 5)."""
    cs = []
    both = ("local", "relation")
    # (k1 DOUBLE key, k2 INTEGER key, v, big unchanged-possible)
    KC = TableMeta((Column(0, "k1", "DOUBLE", key=True), Column(1, "k2", "INTEGER", key=True), Column(2, "v", "VARCHAR"),
                    Column(3, "big", "VARCHAR", unchanged_possible=True)))
    P0, Z, F5, F9 = "0.0::DOUBLE", NZ, "5.0::DOUBLE", "9.0::DOUBLE"

    def kc(k1, k2, v, big):
        return {0: k1, 1: k2, 2: v, 3: big}

    def key(k1, k2):
        return {0: k1, 1: k2}
    # X1: the form-only change first (-0,1 -> 0,2 is copied), then 5,5 moves onto the old form -0,1 (K3)
    r = (R().move(1, key(Z, "1"), kc(P0, "2", "'a2'", None), unchanged=[3])
         .move(1, key(F5, "5"), kc(Z, "1", "'b2'", None), unchanged=[3]))
    x1_start = [kc(Z, "1", "'a'", "'Ba'"), kc(F5, "5", "'b'", "'Bb'")]
    x1_end = [kc(P0, "2", "'a2'", "'Ba'"), kc(Z, "1", "'b2'", "'Bb'")]
    cs.append(Case("round2_X1_form_change_then_move_onto_its_old_form", KC, x1_start, [r.out], x1_end, forms=both))
    cs.append(Case("round2_X1_two_change_sets_one_transaction", KC, x1_start, [r.out[:2], r.out[2:]], x1_end,
                   one_tx=True, forms=both))
    # the form-only change second: 0,2 moves away (K3) first, then -0,1 is copied onto 0,2
    r = (R().move(1, key(P0, "2"), kc(F5, "5", "'b2'", None), unchanged=[3])
         .move(1, key(Z, "1"), kc(P0, "2", "'a2'", None), unchanged=[3]))
    cs.append(Case("round2_form_change_second_onto_a_moved_root", KC,
                   [kc(Z, "1", "'a'", "'Ba'"), kc(P0, "2", "'b'", "'Bb'")], [r.out],
                   [kc(P0, "2", "'a2'", "'Ba'"), kc(F5, "5", "'b2'", "'Bb'")], forms=both))
    # two form-only changes in a chain: 0,2 -> -0,3 is copied, then -0,1 -> 0,2 is copied onto its root
    r = (R().move(1, key(P0, "2"), kc(Z, "3", "'b2'", None), unchanged=[3])
         .move(1, key(Z, "1"), kc(P0, "2", "'a2'", None), unchanged=[3]))
    cs.append(Case("round2_two_form_changes_in_a_chain", KC, [kc(Z, "1", "'a'", "'Ba'"), kc(P0, "2", "'b'", "'Bb'")],
                   [r.out], [kc(P0, "2", "'a2'", "'Ba'"), kc(Z, "3", "'b2'", "'Bb'")], forms=both))
    # three keys in a ring through a temporary key: -0,1 -> 0,2 (copy), 0,2 -> 5,5 and 5,5 -> -0,1 (K3)
    r = (R().move(1, key(P0, "2"), kc(F9, "9", "'t'", None), unchanged=[3])
         .move(1, key(Z, "1"), kc(P0, "2", "'a2'", None), unchanged=[3])
         .move(1, key(F5, "5"), kc(Z, "1", "'c2'", None), unchanged=[3])
         .move(1, key(F9, "9"), kc(F5, "5", "'b2'", None), unchanged=[3]))
    cs.append(Case("round2_three_keys_in_a_ring_with_a_form_change", KC,
                   [kc(Z, "1", "'a'", "'Ba'"), kc(P0, "2", "'b'", "'Bb'"), kc(F5, "5", "'c'", "'Bc'")],
                   [r.out], [kc(P0, "2", "'a2'", "'Ba'"), kc(F5, "5", "'b2'", "'Bb'"), kc(Z, "1", "'c2'", "'Bc'")],
                   forms=both))
    # third round: the copy root was inserted by an earlier change set of the same transaction (not committed),
    # and K1 and K3 change other rows of that transaction between C0 and K7/K8; then X1 onto its old form
    r1 = R().c(1, kc(Z, "1", "'a'", "'Ba'")).c(1, kc("7.0::DOUBLE", "7", "'g'", "'Bg'"))
    r2 = (R().d(2, key("3.0::DOUBLE", "3"))
          .move(2, key("7.0::DOUBLE", "7"), kc("8.0::DOUBLE", "8", "'g2'", None), unchanged=[3])
          .move(2, key(Z, "1"), kc(P0, "2", "'a2'", None), unchanged=[3])
          .move(2, key(F5, "5"), kc(Z, "1", "'b2'", None), unchanged=[3]))
    cs.append(Case("round3_copy_root_inserted_earlier_in_the_transaction", KC,
                   [kc("3.0::DOUBLE", "3", "'c'", "'Bc'"), kc(F5, "5", "'b'", "'Bb'")], [r1.out, r2.out],
                   [kc(P0, "2", "'a2'", "'Ba'"), kc(Z, "1", "'b2'", "'Bb'"), kc("8.0::DOUBLE", "8", "'g2'", "'Bg'")],
                   one_tx=True, forms=both))
    # X1 with an INTERVAL key part: 1 month,1 -> 30 days,2 is copied, then 1 day,5 moves onto 1 month,1
    KI = TableMeta((Column(0, "k1", "INTERVAL", key=True), Column(1, "k2", "INTEGER", key=True),
                    Column(2, "v", "VARCHAR"), Column(3, "big", "VARCHAR", unchanged_possible=True)))
    M, D30, D1 = "INTERVAL '1 month'", "INTERVAL '30 days'", "INTERVAL '1 day'"
    r = (R().move(1, key(M, "1"), kc(D30, "2", "'a2'", None), unchanged=[3])
         .move(1, key(D1, "5"), kc(M, "1", "'b2'", None), unchanged=[3]))
    cs.append(Case("round2_X1_interval_key_part", KI, [kc(M, "1", "'a'", "'Ba'"), kc(D1, "5", "'b'", "'Bb'")], [r.out],
                   [kc(D30, "2", "'a2'", "'Ba'"), kc(M, "1", "'b2'", "'Bb'")], forms=both))
    # report 4b: the count checks counted sums. The destination holds key 1 twice and lacks key 3.
    TV = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "v", "VARCHAR"),
                    Column(2, "big", "VARCHAR", unchanged_possible=True)))

    def tv(i, v, big):
        return {0: str(i), 1: v, 2: big}
    wrong = [tv(1, "'a'", "'B1'"), tv(1, "'a'", "'B1'"), tv(2, "'b'", "'B2'")]
    r = R().move(1, {0: "1"}, tv(11, "'x'", None), unchanged=[2]).move(1, {0: "3"}, tv(13, "'y'", None), unchanged=[2])
    cs.append(Case("round2_4b_moves_of_a_doubled_and_a_missing_key_raise", TV, wrong, [r.out], error="invariant",
                   forms=both))
    r = R().d(1, {0: "1"}).d(1, {0: "3"})
    cs.append(Case("round2_4b_deletes_of_a_doubled_and_a_missing_key_raise", TV, wrong, [r.out], error="invariant",
                   forms=both))
    r = R().u(1, tv(1, "'x'", None), unchanged=[2]).u(1, tv(3, "'y'", None), unchanged=[2])
    cs.append(Case("round2_4b_updates_of_a_doubled_and_a_missing_key_raise", TV, wrong, [r.out], error="invariant",
                   forms=both))
    # a doubled root of a form-only key change
    KD = TableMeta((Column(0, "k", "DOUBLE", key=True), Column(1, "big", "VARCHAR", unchanged_possible=True)))
    r = R().move(1, {0: NZ}, {0: P0, 1: None}, unchanged=[1])
    cs.append(Case("round2_copy_of_a_doubled_root_raises", KD, [{0: NZ, 1: "'A'"}, {0: NZ, 1: "'A'"}], [r.out],
                   error="invariant", forms=both))
    # item 9: the new keys are checked inside K6 (an insert-only change set is one statement), K3 and K7 (a move
    # onto a key that must be free)
    r = R().c(1, tv(2, "'n'", "'N'"))
    cs.append(Case("round2_insert_at_a_held_key_raises", TV, [tv(2, "'b'", "'B2'")], [r.out], error="invariant",
                   forms=both))
    cs.append(Case("round2_insert_at_a_held_key_unchecked_with_the_switch_off", TV, [tv(2, "'b'", "'B2'")], [r.out],
                   [tv(2, "'b'", "'B2'"), tv(2, "'n'", "'N'")], settings={"check_new_keys": False}))
    r = R().move(1, {0: "1"}, tv(2, "'x'", None), unchanged=[2])
    cs.append(Case("round2_move_onto_a_held_free_key_raises", TV, [tv(1, "'a'", "'B1'"), tv(2, "'b'", "'B2'")],
                   [r.out], error="invariant", forms=both))
    cs.append(Case("round2_move_onto_a_held_free_key_unchecked_with_the_switch_off", TV,
                   [tv(1, "'a'", "'B1'"), tv(2, "'b'", "'B2'")], [r.out],
                   [tv(2, "'x'", "'B1'"), tv(2, "'b'", "'B2'")], settings={"check_new_keys": False}))
    r = R().move(1, {0: NZ}, {0: P0, 1: None}, unchanged=[1])
    cs.append(Case("round2_copy_onto_a_held_free_key_raises", KD, [{0: NZ, 1: "'A'"}, {0: P0, 1: "'X'"}], [r.out],
                   error="invariant", forms=both))
    r = R().c(1, tv(5, "'n'", "'N'")).c(1, tv(6, "'m'", "'M'"))
    cs.append(Case("round2_insert_only_is_one_statement", TV, [tv(1, "'a'", "'B1'")], [r.out],
                   [tv(1, "'a'", "'B1'"), tv(5, "'n'", "'N'"), tv(6, "'m'", "'M'")], max_statements=1))
    # item 5: catch-up, d(K) c(K, k=K) with an unchanged value: the destination row at K is the row before or after
    # the pair, and both hold the same value of the unchanged column (rule 1, docs/design/apply.md 2.3)
    r = R().move(1, {0: "1"}, tv(1, "'v2'", None), unchanged=[2])
    cs.append(Case("round2_catch_up_self_key_change_resolves", TV, [tv(1, "'a'", "'B1'")], [r.out],
                   [tv(1, "'v2'", "'B1'")], mode="catch_up", unresolved=[], twice=True, forms=both))
    cs.append(Case("round2_catch_up_self_key_change_loaded_after_the_pair_resolves", TV, [tv(1, "'v2'", "'B1'")],
                   [r.out], [tv(1, "'v2'", "'B1'")], mode="catch_up", unresolved=[], forms=both))
    # back to the same key through another one: 1 -> 2 -> 1
    r = R().move(1, {0: "1"}, tv(2, "'v2'", None), unchanged=[2]).move(1, {0: "2"}, tv(1, "'v3'", None), unchanged=[2])
    cs.append(Case("round2_catch_up_back_through_another_key_resolves", TV, [tv(1, "'a'", "'B1'")], [r.out],
                   [tv(1, "'v3'", "'B1'")], mode="catch_up", unresolved=[], forms=both))
    # a full update between two self key changes: complete, nothing to take from the destination
    r = (R().move(1, {0: "1"}, tv(1, "'v2'", None), unchanged=[2]).u(2, tv(1, "'v3'", "'NEW'"))
         .move(3, {0: "1"}, tv(1, "'v4'", None), unchanged=[2]))
    cs.append(Case("round2_catch_up_full_update_between_self_key_changes", TV, [tv(1, "'a'", "'B1'")], [r.out],
                   [tv(1, "'v4'", "'NEW'")], mode="catch_up", unresolved=[]))
    # two creates at the key that both take the value from the destination: rule 1 is kept to one create (open)
    r = R().move(1, {0: "1"}, tv(1, "'v2'", None), unchanged=[2]).move(2, {0: "1"}, tv(1, "'v3'", None), unchanged=[2])
    cs.append(Case("round2_catch_up_two_self_key_changes_stay_unresolved", TV, [tv(1, "'a'", "'B1'")], [r.out],
                   [], mode="catch_up", unresolved=[("1",)], forms=both))
    return cs


def all_cases() -> List[Case]:
    return keyed_cases() + special_cases() + type_key_cases() + keyless_cases() + round2_cases()

