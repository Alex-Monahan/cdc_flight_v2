"""The seeded workload generator without a database (T0): determinism per seed and the feature switches."""
import json
import re

import pytest

from cdc_v2.testing.workload import Features, Workload, WorkloadConfig, per_writer_statements


def plan(seed=1, rounds=5, **kw):
    return Workload(WorkloadConfig(seed=seed, **kw)).plan(rounds)


def statements(entries):
    return [s for e in entries for s in e["statements"]]


ALL = Features(large_txn=True, truncate=True, ddl=True, transfers=True)


def test_same_seed_same_statements():
    a = plan(42, rounds=8, features=ALL, large_txn_rows=50)
    b = plan(42, rounds=8, features=ALL, large_txn_rows=50)
    assert json.dumps(a) == json.dumps(b)


def test_different_seed_different_statements():
    assert json.dumps(plan(1, features=ALL)) != json.dumps(plan(2, features=ALL))


def test_writers_are_independent_of_each_other():
    """Writer 0's statements do not depend on how many other writers there are... per writer RNG streams."""
    one = per_writer_statements(plan(5, writers=2))
    again = per_writer_statements(plan(5, writers=2))
    assert one == again and set(one) >= {0, 1}


def test_config_round_trip():
    cfg = WorkloadConfig(seed=3, writers=4, features=Features.only(transfers=True))
    assert WorkloadConfig.from_dict(json.loads(json.dumps(cfg.to_dict()))) == cfg


def test_transfers_only():
    w = Workload(WorkloadConfig(seed=9, features=Features.only(transfers=True)))
    entries = w.plan(3)
    body = [s for s in statements(entries[1:]) if s not in ("BEGIN", "COMMIT")]
    assert body and all(re.match(r'UPDATE "public"\."w_xfer_\d" SET balance = balance \+ \(-?\d+\) WHERE id = \d+$', s)
                        for s in body)
    # every transfer sums to zero
    for e in entries[1:]:
        amounts = [int(m) for s in e["statements"] for m in re.findall(r"\+ \((-?\d+)\)", s)]
        assert sum(amounts) == 0
    assert w.transfer_sum == 3 * 8 * 1000


def test_switches_off_keep_groups_out():
    s = "\n".join(statements(plan(4, rounds=6, features=Features(keyless=False, toast_unchanged=False,
                                                                  savepoints=False, rollbacks=False))))
    assert "w_nk" not in s and "w_toast" not in s and "SAVEPOINT" not in s and "ROLLBACK" not in s
    assert "TRUNCATE" not in s and "ALTER TABLE" not in s.replace("REPLICA IDENTITY", "")


def test_feature_groups_appear_when_on():
    w = Workload(WorkloadConfig(seed=11, writers=3, txns_per_round=30, features=ALL, large_txn_rows=20,
                                large_txn_prob=0.05, truncate_prob=0.3, ddl_prob=0.9))
    s = "\n".join(statements(w.plan(12)))
    for needle in ("SAVEPOINT s1", "ROLLBACK TO SAVEPOINT s1", "\nROLLBACK", "TRUNCATE", "ADD COLUMN",
                   "generate_series", "w_nk", "SET n = n + 1", "w_xfer_"):
        assert needle in s, needle
    for k in ("pk_chain", "pk_swap", "txn_savepoint", "txn_rollback", "txn_multi_table", "truncate", "large_txn"):
        assert w.stats[k] > 0, k
    assert any(k.startswith("ddl_") for k in w.stats)


def test_keys_belong_to_their_writer():
    w = Workload(WorkloadConfig(seed=13, writers=3, features=Features(pk_updates=False)))
    for e in w.plan(4):
        if e["type"] != "txn":
            continue
        for s in e["statements"]:
            m = re.match(r'INSERT INTO "public"\."w_k\d" \(.*?\) VALUES \((\d+),', s)
            if m:
                assert (int(m.group(1)) - 1) % 3 == e["writer"]


def test_rolled_back_work_does_not_enter_the_model():
    w = Workload(WorkloadConfig(seed=17, writers=1, txns_per_round=200,
                                features=Features.only(rollbacks=True, inserts=True)))
    w.setup_statements()
    w.plan_round()
    assert all(not keys for keys in w.model.keys.values())


def test_savepoint_section_is_discarded_from_the_model():
    w = Workload(WorkloadConfig(seed=19, writers=1, txns_per_round=50,
                                features=Features.only(savepoints=True, inserts=True)))
    w.setup_statements()
    txns = w.plan_round()[0]
    inserted = set()
    for t in txns:
        rolled = False
        for s in t.statements:
            if s == "SAVEPOINT s1":
                rolled = True
            elif s == "ROLLBACK TO SAVEPOINT s1":
                rolled = False
            m = re.match(r'INSERT INTO "public"\."(w_k\d)" \(.*?\) VALUES \((\d+),', s)
            if m and not rolled:
                inserted.add((m.group(1), int(m.group(2))))
    model = {(t, k) for (_, t), keys in w.model.keys.items() for k in keys}
    assert model == inserted
