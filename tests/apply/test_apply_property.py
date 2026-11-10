"""Package A, tier T0: property-based tests on local DuckDB (bounded; CDC_V2_APPLY_EXAMPLES raises the count).

The reference is the simulated source table (apply_sim): it applies source statements one by one, emits the
records Debezium would send, and its final rows are what the destination must hold, compared by the oracle's
canonical forms. The same properties run on MotherDuck in test_apply_md.py, and for hours in apply_campaign.py."""
from __future__ import annotations

import os
import random

import pytest
from hypothesis import HealthCheck, example, given, settings, strategies as st

from apply_harness import catch_up_scenario, local_conn, stream_scenario
from apply_sim import KeyedSource, change_set_table, create_table, random_meta
from apply_standin import StandInTx
from cdc_v2.apply import LocalChangeSet, Target, apply_group
from cdc_v2.apply.statements import Shapes

N = int(os.environ.get("CDC_V2_APPLY_EXAMPLES", "10"))
SETTINGS = dict(max_examples=N, deadline=None, suppress_health_check=[HealthCheck.too_slow], derandomize=True)
SEEDS = st.integers(min_value=0, max_value=2 ** 31)


@settings(**SETTINGS)
@given(seed=SEEDS, form=st.sampled_from(["local", "local", "relation"]))
@example(seed=2273038, form="local")     # regression: -infinity DATE key missed by a join in the same transaction
def test_keyed_stream_equals_source(seed, form):
    stream_scenario(local_conn(), seed, keyed=True, form=form)


@settings(**SETTINGS)
@given(seed=SEEDS, form=st.sampled_from(["local", "local", "relation"]))
def test_keyless_stream_equals_source(seed, form):
    stream_scenario(local_conn(), seed, keyed=False, form=form)


@settings(**SETTINGS)
@given(seed=SEEDS, form=st.sampled_from(["local", "local", "relation"]))
@example(seed=0, form="relation")        # regression: unresolved TIMETZ keys came back as TIME (offset lost)
@example(seed=260, form="relation")      # regression: same, through the lane connection's Arrow export
@example(seed=135239, form="local")      # regression: a later call took a value from an unresolved key's stale row
def test_catch_up_converges_and_is_idempotent(seed, form):
    catch_up_scenario(local_conn(), seed, form=form)


@pytest.mark.xfail(strict=True, reason="documented precondition of catch_up (docs/design/apply.md 2.6): a call that "
                   "ends before the newest snapshot of the table can take an unchanged value from a later row that "
                   "reused the key")
def test_catch_up_split_before_the_newest_snapshot():
    # found by the property test (seed 99008, row ids from 10): the first call sees d(K) + c(K2, k=K) with an
    # unchanged column; the table holds K from a snapshot after K was reused by a new row in the second call.
    catch_up_scenario(local_conn(), 99008, rid_offset=9, early_split=True)


def test_statement_shapes_depend_on_columns_only():
    """SPEC 2.6: over many change sets with every operation and every pattern of unchanged columns, each shape
    name always has the same SQL text, and the number of shapes stays within the table's bound."""
    rng = random.Random(7)
    meta, kinds = random_meta(rng, True, 4, value_kinds=["varchar", "interval", "double", "struct", "intlist", "blob"],
                              key_kinds=["bigint"])
    seen = {}
    total_sets = 0
    for seed in range(10):
        r2 = random.Random(seed)
        src = KeyedSource(meta, r2, kinds, toast_rate=0.7)
        src.seed_rows(8)
        conn = local_conn()
        create_table(conn, "s", "t", meta, src.snapshot())
        for _ in range(3):
            recs = src.ops_txn(r2.randint(1, 6))
            with StandInTx(conn) as tx:
                counts = apply_group(tx, Target("s", "t"), LocalChangeSet(change_set_table(meta, recs), None, meta))
                total_sets += 1
                texts = [s for s in tx.sent if not s.startswith("SELECT")]
                for shape, sql in zip([x for x in counts.shapes if x not in ("plan", "lookup")], texts):
                    seen.setdefault(shape, set()).add(sql)
    sh = Shapes(meta, Target("s", "t"), list(meta.unchanged_columns))
    assert all(len(v) == 1 for v in seen.values()), {k: len(v) for k, v in seen.items()}
    assert len(seen) <= sh.shape_bound(), (sorted(seen), sh.shape_bound())
    assert total_sets == 30
