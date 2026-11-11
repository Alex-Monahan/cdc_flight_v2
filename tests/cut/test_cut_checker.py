"""Hermetic tests of the stress test's own checker (cdc_v2.testing.cutstress.Checker).

A checker that cannot fail proves nothing.  These tests hand it a small stream and cuts whose snapshot is
right, and cuts whose snapshot breaks one rule each, and demand that it says so."""
from array import array

import pytest

from cdc_v2.testing import cutstress as S

# (E, length of the commit record, transaction id, key), in stream order.  Key 100 gets the counter values
# 1, 2, 3 and key 200 the values 1, 2.
TXNS = [(1000, 48, 10, 100), (2000, 48, 11, 200), (3000, 48, 12, 100), (4000, 48, 13, 200), (5000, 48, 14, 100)]

# A cut with R0 = 2000 and R1 = 2952 (the start of the third commit record), so E1 = 3000:
# the first two transactions are inside, the third is unknown, the last two are outside.
R0, R1 = 2000, 2952
XMIN, XMAX = 12, 13


def make_stream() -> S.Stream:
    st = S.Stream()
    for e, slen, xid, key in TXNS:
        ix = st.idx.setdefault(key, array("I"))
        st.e.append(e)
        st.slen.append(slen)
        st.xid.append(xid)
        st.key.append(key)
        st.n.append(len(ix) + 1)
        ix.append(len(st.e) - 1)
    st.count = len(st.e)
    st.head = st.e[-1]
    return st


def check(vis: dict, *, head: int = 0, r1: int = R1) -> S.Checker:
    st = make_stream()
    checker = S.Checker(stream=st)
    rec = S.CutRecord("own", R0, r1, r1, XMIN, XMAX, head, vis)
    assert checker.ready(rec, st.count)
    checker.check(rec, st.count)
    assert checker.cuts == 1
    return checker


@pytest.mark.parametrize("vis", [{100: 1, 200: 1}, {100: 2, 200: 1}])
def test_a_right_snapshot_has_no_violation(vis):
    checker = check(vis)
    assert not checker.violations
    assert checker.hist["base"] == {1: 1}        # the transaction at E1 is the one unknown transaction


def test_the_wrong_rules_are_counted_when_the_snapshot_contradicts_them():
    seen = check({100: 2, 200: 1})                # sees the transaction at E1: its commit was in replay
    assert not seen.violations
    assert seen.wrong["r1_outside"] == 1          # "E > R1 means outside" is wrong for this cut
    assert seen.wrong["xmin_outside"] == 1        # "id at or above xmin means outside" as well
    unseen = check({100: 1, 200: 1})
    assert not unseen.violations
    assert unseen.wrong["xmax_inside"] == 1       # "id below xmax means inside" is wrong for this cut
    assert unseen.wrong["r1_outside"] == 0


def test_a_snapshot_that_misses_an_inside_transaction_is_a_violation():
    checker = check({100: 0, 200: 1})
    assert checker.violations["inside"] == 1
    assert checker.violations["library"] >= 1
    assert checker.violation_examples[0]["E"] == "0/3E8"


def test_a_snapshot_that_sees_an_outside_transaction_is_a_violation():
    checker = check({100: 2, 200: 2})
    assert checker.violations["outside"] == 1
    assert checker.violations["library"] >= 1


def test_the_test_only_narrowing_is_checked_under_its_own_name():
    """With R1 = 2900 the commit record of the transaction at E1 starts after R1 (at 2952).  The library, which
    has only the transaction id, calls it unknown; the narrowing by the record start (cdc_v2.testing.cutnarrow,
    not available to the pipeline) calls it outside.  A snapshot that sees it breaks that rule and no other."""
    seen = check({100: 2, 200: 1}, r1=2900)
    assert seen.violations["sharp"] >= 1 and set(seen.violations) == {"sharp"}
    unseen = check({100: 1, 200: 1}, r1=2900)
    assert not unseen.violations
    # the measurement: one unknown transaction by position and by id, none with the record start
    assert unseen.hist["base"] == {1: 1} and unseen.hist["xid"] == {1: 1}
    assert unseen.hist["sharp"] == {0: 1} and unseen.hist["xid+sharp"] == {0: 1}
    assert {"library", "sharp"} <= set(S.SOUND_RULES)


def test_the_checker_names_its_steps_in_every_result():
    assert S.CHECKER_STEPS == ("unknown_window", "per_key_boundary", "missing_rows")
    assert "once per connection" in S.TAKER_RULES


def test_a_snapshot_that_sees_more_than_the_stream_delivered_is_a_violation():
    assert check({100: 9, 200: 1}).violations["outside"] == 1


def test_a_snapshot_that_returns_no_row_for_a_key_is_a_violation():
    checker = check({200: 1})
    assert checker.violations["evidence"] == 1
    assert checker.stats["rows_missing_from_a_cut"] == 1


def test_a_cut_below_what_the_stream_had_received_is_counted():
    checker = check({100: 1, 200: 1}, head=3000)
    assert checker.stats["cuts_with_r0_below_received"] == 1
    assert checker.stats["cuts_that_miss_a_received_transaction"] == 1   # the third transaction, key 100
    assert not checker.violations                                       # the rule R0 >= P0 rejects this cut


# -- the Postgres binaries ---------------------------------------------------------------------------------------

def test_localpg_gets_the_binaries_variable_and_no_libpq_variable():
    """Everything in the stress tool and the live tests reaches Postgres through tools/localpg, which takes the
    binaries from CDC_V2_PG_BIN.  The variable must reach it; the libpq variables (a default port) must not."""
    env = S.Cluster.localpg_env({"CDC_V2_PG_BIN": "/somewhere/postgresql@16/bin", "PATH": "/bin",
                                 "PGPORT": "5432", "PGHOST": "example", "PGDATABASE": "x", "PGPASSWORD": "x"})
    assert env == {"CDC_V2_PG_BIN": "/somewhere/postgresql@16/bin", "PATH": "/bin"}
    assert S.PG_BIN_ENV == "CDC_V2_PG_BIN"
    assert not any(k.startswith("PG") for k in S.Cluster.localpg_env())      # the real environment, too


def test_the_cluster_refuses_the_port_of_the_owner():
    with pytest.raises(S.StressError):
        S.Cluster(home="/nonexistent", port_base=5430)
