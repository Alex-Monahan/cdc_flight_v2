"""Package A, tier T0: property-based tests on local DuckDB (bounded; CDC_V2_APPLY_EXAMPLES raises the count).

The reference is the simulated source table (apply_sim): it applies source statements one by one, emits the
records Debezium would send, and its final rows are what the destination must hold, compared by the oracle's
canonical forms. The same properties run on MotherDuck in test_apply_md.py, and for hours in apply_campaign.py."""
from __future__ import annotations

import os
import random

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from apply_controls import caught_after, everything_unresolved, last_record_decides, plain_values
from apply_harness import (catch_up_scenario, cut_scenario, local_conn, rounds_scenario, stream_scenario,
                           test_settings)
from apply_sim import KeyedSource, change_set_table, create_table, random_meta
from apply_standin import StandInTx
from cdc_v2.apply import LocalChangeSet, Target, apply_group
from cdc_v2.apply.statements import Shapes

N = int(os.environ.get("CDC_V2_APPLY_EXAMPLES", "3"))
SETTINGS = dict(max_examples=N, deadline=None, suppress_health_check=[HealthCheck.too_slow], derandomize=True)
SEEDS = st.integers(min_value=0, max_value=2 ** 31)
# The regressions found by earlier runs (seeds of a simulator that changed since) are named cases in
# apply_cases.py: infinite_*_keys_in_one_transaction, uuid/json_keys_in_one_transaction, timetz_key_*,
# catch_up_* ; the catch-up rounds below would fail on a stale unresolved row (round two takes values from it).


@settings(**SETTINGS)
@given(seed=SEEDS, form=st.sampled_from(["local", "local", "relation"]))
def test_keyed_stream_equals_source(seed, form):
    stream_scenario(local_conn(), seed, keyed=True, form=form)


@settings(**SETTINGS)
@given(seed=SEEDS, form=st.sampled_from(["local", "local", "relation"]))
def test_keyless_stream_equals_source(seed, form):
    stream_scenario(local_conn(), seed, keyed=False, form=form)


@settings(**SETTINGS)
@given(seed=SEEDS, form=st.sampled_from(["local", "local", "relation"]))
def test_catch_up_converges_and_is_idempotent(seed, form):
    catch_up_scenario(local_conn(), seed, form=form)


@settings(**SETTINGS)
@given(seed=SEEDS)
def test_cut_inside_transactions_equals_source(seed):
    """Item 4: a change set cut at every record boundary that keeps key-change pairs together, applied piece by
    piece in one transaction."""
    cut_scenario(local_conn(), seed)


@settings(**SETTINGS)
@given(seed=SEEDS, repeat=st.booleans())
def test_catch_up_rounds_with_repair_reads(seed, repeat):
    """Item 7 (b, c; a with repeat): rounds with repair reads, up to 6 snapshots, the precondition kept; a key is
    unresolved only if its row has a value no record of the round carries; the final table equals the source."""
    rounds_scenario(local_conn(), seed, repeat=repeat)


def test_negative_control_last_record_decides():
    """Item 12 (a): the keyed property catches "the last record per key decides"."""
    assert caught_after(lambda s: stream_scenario(local_conn(), s, keyed=True, apply_fn=last_record_decides),
                        0, 5) is not None


def test_negative_control_plain_group_by():
    """Item 12 (b): the keyless property catches a planner that compares raw values."""
    with plain_values():
        assert caught_after(lambda s: stream_scenario(local_conn(), s, keyed=False), 0, 5) is not None


def test_negative_control_everything_unresolved():
    """Item 12 (c): the rounds property catches a catch-up that reports every key unresolved (bound 7c)."""
    assert caught_after(lambda s: rounds_scenario(local_conn(), s, apply_fn=everything_unresolved), 0, 5) is not None


@pytest.mark.xfail(strict=True, reason="documented precondition of catch_up (docs/design/apply.md 2.6): a call that "
                   "ends before the newest snapshot of the table can take an unchanged value from a later row that "
                   "reused the key")
def test_catch_up_split_before_the_newest_snapshot():
    # Found by the property test (first as seed 99008). The source moves 1 -> 5 with `big` unchanged (txn 1), then
    # inserts a new row 1 (txn 2). The table was loaded under a snapshot after txn 2. A first call that applies txn
    # 1 alone sees no create at key 1 and takes 5's `big` from destination row 1, which is the new row.
    from apply_cases import R, T, row, K
    from apply_harness import SCHEMA, apply_sets, create_table, dest_canon
    from apply_sim import literal_rows_canon
    from cdc_v2.apply import Target
    conn = local_conn()
    create_table(conn, SCHEMA, "t", T, [row(1, "'NEW'", "9.0"), row(5, "'B1'")])
    r = R().move(1, K(1), row(5, None), unchanged=[1]).c(2, row(1, "'NEW'", "9.0"))
    first = [x for x in r.out if x.txn == 1]
    second = [x for x in r.out if x.txn == 2]
    apply_sets(conn, Target(SCHEMA, "t"), T, [first, second], mode="catch_up")
    assert dest_canon(conn, Target(SCHEMA, "t"), T) == literal_rows_canon(list(T.columns), [row(5, "'B1'"),
                                                                                             row(1, "'NEW'", "9.0")])


def test_statement_shapes_depend_on_columns_only():
    """SPEC 2.6: over many change sets with every operation and every pattern of unchanged columns, each shape
    name always has the same SQL text, and the number of shapes stays within the table's bound."""
    rng = random.Random(7)
    meta, kinds = random_meta(rng, True, 4, value_kinds=["varchar", "interval", "double", "struct", "intlist", "blob"],
                              key_kinds=["bigint"])
    seen = {}
    total_sets = 0
    for seed in range(int(os.environ.get("CDC_V2_APPLY_SHAPE_SEEDS", "4"))):
        r2 = random.Random(seed)
        src = KeyedSource(meta, r2, kinds, toast_rate=0.7)
        src.seed_rows(8)
        conn = local_conn()
        create_table(conn, "s", "t", meta, src.snapshot())
        for _ in range(3):
            recs = src.ops_txn(r2.randint(1, 6))
            with StandInTx(conn) as tx:
                counts = apply_group(tx, Target("s", "t"), LocalChangeSet(change_set_table(meta, recs), None, meta),
                                     settings=test_settings())
                total_sets += 1
                texts = [s for s in tx.sent if not s.startswith("SELECT")]
                for shape, sql in zip([x for x in counts.shapes if x not in ("plan", "lookup")], texts):
                    seen.setdefault(shape, set()).add(sql)
    sh = Shapes(meta, Target("s", "t"), list(meta.unchanged_columns))
    assert all(len(v) == 1 for v in seen.values()), {k: len(v) for k, v in seen.items()}
    assert len(seen) <= sh.shape_bound(), (sorted(seen), sh.shape_bound())
    assert total_sets == 3 * int(os.environ.get("CDC_V2_APPLY_SHAPE_SEEDS", "4"))
