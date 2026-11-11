"""Model check of the cut (docs/design/cut.md, "The model check").

The model (src/cdc_v2/testing/cutmodel.py) replays WAL the way the Postgres source does: a commit becomes visible
during the redo of its commit record, and the replay position moves afterwards.  A reader reads R0, takes a
snapshot and reads R1 at arbitrary moments between these steps.

Here: every reader interleaving of every small world, and random larger worlds.  The sound rules are never
violated; every plausible wrong rule is.

Sizes.  The default run uses the smallest sizes that still refute every wrong rule (about half a second for
this file).  The sizes of the first version of this file, and one step more, run with the marker `localpg`
(`uv run pytest -m localpg tests/cut/test_cut_model.py`; they need no Postgres, the marker only keeps them out
of the default run), and `tools/cutstress model` explores them too and writes the evidence
(evidence/p3/cut/model.json).
"""
import pytest
from hypothesis import given, settings, strategies as st

from cdc_v2.testing import cutmodel as M

# (transactions, WAL records at most)
SMALL = ((1, 2), (2, 2))                     # default run: 1,416 worlds, 91,860 reader interleavings
FULL = ((1, 4), (2, 3))                      # opt-in: 13,704 worlds, 1,390,340 reader interleavings
# (transactions, markers)
PRIMARY_SMALL = ((1, 1), (2, 1), (2, 2))     # default run: 687 histories
PRIMARY_FULL = ((3, 1),)                     # opt-in: 6,265 histories
RANDOM_WORLDS, RANDOM_WORLDS_FULL = 60, 600


def explored(sizes=SMALL):
    cache = explored.__dict__.setdefault("cache", {})
    if sizes not in cache:
        cache[sizes] = [M.explore(n, max_len) for n, max_len in sizes]
    return cache[sizes]


def assert_sound(results):
    for r in results:
        broken = set(r["violations"]) & set(M.SOUND)
        assert not broken, {rule: r["witness"][rule] for rule in broken}


def test_sound_rules_hold_in_every_small_world():
    results = explored()
    assert sum(r["worlds"] for r in results) > 1_000 and sum(r["interleavings"] for r in results) > 80_000
    assert_sound(results)


def test_every_wrong_rule_is_refuted():
    refuted = set()
    for r in explored():
        refuted |= set(r["violations"])
    assert refuted == set(M.WRONG)


@pytest.mark.localpg
def test_sound_rules_hold_in_every_world_of_the_full_sizes():
    results = explored(FULL)
    assert sum(r["worlds"] for r in results) > 13_000 and sum(r["interleavings"] for r in results) > 1_300_000
    assert_sound(results)
    assert set().union(*(set(r["violations"]) for r in results)) == set(M.WRONG)


def test_smallest_witnesses_are_the_ones_the_document_quotes():
    w = M.Standby((M.Tx(10),), (M.Rec("C", 0),)).run()
    # states: 0 start; 1 id known; 2 commit visible, position not yet moved; 3 position moved
    assert [(s.position, sorted(s.visible)) for s in w.states] == [
        (M.SEGMENT + 40, []), (M.SEGMENT + 40, []), (M.SEGMENT + 40, [0]), (M.SEGMENT + 104, [0])]
    assert "r1_inside" in M.check_reader(w, 0, 1, 3)       # snapshot before the flip, position read after
    assert "r1_outside" in M.check_reader(w, 0, 2, 2)      # snapshot and R1 between the flip and the move
    assert not M.check_reader(w, 0, 2, 2) & set(M.SOUND)
    # a commit record that starts right after a page header: its start is past R1, yet the snapshot sees it
    w = M.Standby((M.Tx(10),), (M.Rec("X", None, True), M.Rec("C", 0))).run()
    k = next(i for i, s in enumerate(w.states) if s.visible)
    assert w.states[k].position % M.PAGE == 0 and w.s[0] == w.states[k].position + 24
    assert M.check_reader(w, 0, k, k) == {"r1_outside", "naive_sharp"}
    w = M.Standby((M.Tx(10),), (M.Rec("X", None, True), M.Rec("C", 0)), start=M.STARTS[1]).run()
    k = next(i for i, s in enumerate(w.states) if s.visible)
    assert w.states[k].position % M.SEGMENT == 0 and w.s[0] == w.states[k].position + 40
    assert M.check_reader(w, 0, k, k) == {"r1_outside", "naive_sharp"}


def test_the_check_notices_a_server_with_the_other_order():
    """If the position moved before the commit became visible, the inside rule would be wrong.  The model check
    is only as good as the order it assumes; this shows it is sensitive to exactly that order."""
    r = M.explore(1, 2, advance_first=True)
    assert {"inside", "usable", "library"} <= set(r["violations"])
    assert "outside" not in r["violations"] and "r1_outside" not in r["violations"]


@st.composite
def random_world(draw):
    n = draw(st.integers(1, 4))
    sizes = [1 + draw(st.integers(0, 3)) for _ in range(n)]            # top-level id plus subtransactions
    pool = draw(st.permutations(range(M.FIRST_XID, M.FIRST_XID + sum(sizes) + draw(st.integers(0, 2)))))
    txs, at = [], 0
    for size in sizes:
        ids = sorted(pool[at:at + size])
        at += size
        txs.append(M.Tx(ids[0], tuple(ids[1:]), in_stream=draw(st.sampled_from([True, True, True, False]))))
    queues = [[("W", t)] * draw(st.integers(0, 2)) + [(draw(st.sampled_from("CCA")), t)] for t in range(n)]
    queues.append([("X", None)] * draw(st.integers(0, 2)))
    wal = []
    while any(queues):
        q = draw(st.sampled_from([q for q in queues if q]))
        kind, t = q.pop(0)
        wal.append(M.Rec(kind, t, draw(st.sampled_from([False, False, False, True]))))
    start = draw(st.sampled_from(M.STARTS + (M.SEGMENT + 40 + 64 * 3, 2 * M.SEGMENT - M.PAGE + 24 + 64)))
    assert M.well_formed(tuple(txs), tuple(wal))
    return M.Standby(tuple(txs), tuple(wal), start=start).run()


@settings(max_examples=RANDOM_WORLDS, deadline=None, derandomize=True)
@given(random_world())
def test_sound_rules_hold_in_random_larger_worlds(w):
    broken = set(M.check_world(w)) & set(M.SOUND)
    assert not broken, broken


@pytest.mark.localpg
@settings(max_examples=RANDOM_WORLDS_FULL, deadline=None, derandomize=True)
@given(random_world())
def test_sound_rules_hold_in_many_random_larger_worlds(w):
    broken = set(M.check_world(w)) & set(M.SOUND)
    assert not broken, broken


def test_well_formed_rejects_broken_worlds():
    assert not M.well_formed((M.Tx(10),), (M.Rec("C", 0), M.Rec("W", 0)))           # write after the end
    assert not M.well_formed((M.Tx(10),), (M.Rec("W", 0),))                         # never ends
    assert not M.well_formed((M.Tx(10, (9,)),), (M.Rec("C", 0),))                   # subtransaction id below top
    assert not M.well_formed((M.Tx(10), M.Tx(10)), (M.Rec("C", 0), M.Rec("C", 1)))  # same id twice
    assert M.well_formed((M.Tx(11), M.Tx(10, (12,))), (M.Rec("X"), M.Rec("C", 1), M.Rec("A", 0)))


# --- the primary ---------------------------------------------------------------------------------------------

def check_primary_sizes(sizes):
    refuted, histories = set(), 0
    for n, markers in sizes:
        r = M.explore_primary(n, markers)
        assert r["histories"] > 5
        histories += r["histories"]
        broken = set(r["violations"]) & set(M.PRIMARY_SOUND)
        assert not broken, {rule: r["witness"][rule] for rule in broken}
        refuted |= set(r["violations"])
    assert refuted == set(M.PRIMARY_WRONG)
    return histories


def test_primary_rules_in_every_small_history():
    assert check_primary_sizes(PRIMARY_SMALL) > 600


@pytest.mark.localpg
def test_primary_rules_in_every_history_of_the_full_size():
    assert check_primary_sizes(PRIMARY_FULL) > 6_000


def test_primary_gap_smallest_witness():
    """The commit record is written (the stream can deliver it) before the transaction leaves the running set
    (a snapshot can see it).  A snapshot taken in between does not see a transaction the stream already has."""
    events = (("begin", 0), ("record", 0), ("marker",), ("end", 0))
    assert M.primary_well_formed(events, 1)
    st_ = M.Primary(events).run()
    assert st_[1]["delivered"] == (0,) and st_[1]["visible"] == frozenset()
    assert st_[2]["marker"] == 11 and str(st_[2]["bounds"]) == "10:12:10"      # xmin 10 < X: not usable yet
    assert str(st_[3]["bounds"]) == "12:12:" and st_[3]["visible"] == {0}      # xmin 12 >= X: usable
    assert set(M.check_primary(events)) == {"later_snapshot", "xmax_ge_x", "probe_after_snapshot"}
