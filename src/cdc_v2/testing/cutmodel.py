"""A small model of how a hot standby replays WAL, and of how a primary commits.  For the model check of the cut.

The standby model follows the order of operations in the Postgres source (docs/design/cut.md, "Why it holds"):

* replay is serial: one record at a time;
* a record that carries a transaction id first makes that id (and every unseen id below it) *known assigned*;
* the redo of a commit or abort record then removes the transaction's ids from the known-assigned set and raises
  ``latestCompletedXid``, in one step.  For a commit this is the moment the transaction becomes visible to new
  snapshots;
* only after the record's redo has returned does the replay position (what ``pg_last_wal_replay_lsn()`` returns)
  advance to the end of the record;
* a snapshot is ``xmax = latestCompletedXid + 1``, ``xmin = min(xmax, oldest known-assigned id)``, and it treats
  every known-assigned id below ``xmax``, and every id at or above ``xmax``, as in progress.

A reader interleaves three actions with these steps: read ``R0``, take the snapshot, read ``R1``.

``Standby(..., advance_first=True)`` is a deliberately wrong server (the position moves before the visibility
flips); the tests use it to show that the check notices when the order of the source is not what the rule assumes.

The primary model: a transaction gets its id, later writes its commit record (from then on a slot can deliver
it), and only later leaves the set of running transactions (from then on snapshots see it).  The last two steps
of different transactions interleave freely.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from cdc_v2.pg.cut import SnapshotBounds

PAGE = 8192
SEGMENT = PAGE * 4  # small segments, so that a segment start is reached often
RECORD = 64
FIRST_XID = 10


@dataclass(frozen=True)
class Rec:
    """One WAL record.  kind: 'W' a write of transaction ``tx``; 'C' its commit; 'A' its abort; 'X' anything
    without a transaction id.  ``to_boundary``: the record ends exactly on a page boundary."""

    kind: str
    tx: Optional[int] = None
    to_boundary: bool = False


@dataclass(frozen=True)
class Tx:
    xid: int  # top-level id
    subxids: tuple[int, ...] = ()
    in_stream: bool = True  # False: the publication filters it out; the stream never shows it


@dataclass
class State:
    """What the standby looks like after some number of atomic steps."""

    position: int
    visible: frozenset  # transactions (indexes) whose commit is visible to a snapshot taken now
    xmin: int
    xmax: int


@dataclass
class Standby:
    txs: tuple[Tx, ...]
    wal: tuple[Rec, ...]
    advance_first: bool = False
    start: int = SEGMENT + 40
    # filled by run():
    s: dict = field(default_factory=dict)  # tx -> start of its commit record
    e: dict = field(default_factory=dict)  # tx -> end of its commit record
    states: list = field(default_factory=list)  # states[k] = after k atomic steps

    def run(self) -> "Standby":
        pos = self.start
        known: set[int] = set()
        latest_observed = FIRST_XID - 1
        latest_completed = FIRST_XID - 1
        visible: set[int] = set()
        self.states = []

        def snap() -> None:
            xmax = latest_completed + 1
            below = [x for x in known if x < xmax]
            self.states.append(State(pos, frozenset(visible), min(below) if below else xmax, xmax))

        snap()
        for rec in self.wal:
            start = pos
            if start % PAGE == 0:  # a page header comes before the first record of a page
                start += 40 if start % SEGMENT == 0 else 24
            end = (start // PAGE + 1) * PAGE if rec.to_boundary else start + RECORD
            tx = self.txs[rec.tx] if rec.tx is not None else None
            if tx is not None:  # RecordKnownAssignedTransactionIds
                top = max((tx.xid,) + tx.subxids) if rec.kind in "CA" else tx.xid
                if top > latest_observed:
                    known.update(range(latest_observed + 1, top + 1))
                    latest_observed = top
                    snap()
            if rec.kind == "C":
                self.s[rec.tx], self.e[rec.tx] = start, end
            if self.advance_first:
                pos = end
                snap()
            if rec.kind in "CA":  # ExpireTreeKnownAssignedTransactionIds
                tree = (tx.xid,) + tx.subxids
                known.difference_update(tree)
                latest_completed = max(latest_completed, max(tree))
                if rec.kind == "C":
                    visible.add(rec.tx)
                snap()
            if not self.advance_first:
                pos = end
                snap()
        return self

    def bounds(self, k: int) -> SnapshotBounds:
        st = self.states[k]
        return SnapshotBounds(st.xmin, st.xmax)

    def committed(self) -> list[int]:
        return sorted(self.e, key=self.e.get)

    def stream(self) -> list[int]:
        """Committed transactions the stream shows, in stream (= commit record) order."""
        return [t for t in self.committed() if self.txs[t].in_stream]


def well_formed(txs: tuple[Tx, ...], wal: tuple[Rec, ...]) -> bool:
    """Every transaction ends exactly once, after its writes; ids are distinct; a subtransaction id is greater
    than its top-level id.  Ids are handed out at any time before a transaction's first record, so the order of
    ids and the order of first records are independent."""
    ended: set[int] = set()
    for rec in wal:
        if rec.tx is None:
            if rec.kind != "X":
                return False
            continue
        if rec.kind not in "WCA" or rec.tx in ended or not 0 <= rec.tx < len(txs):
            return False
        if rec.kind in "CA":
            ended.add(rec.tx)
    ids = [x for t in txs for x in (t.xid,) + t.subxids]
    if len(ids) != len(set(ids)) or any(x < FIRST_XID for x in ids):
        return False
    return all(all(sx > t.xid for sx in t.subxids) for t in txs) and ended == set(range(len(txs)))


# --------------------------------------------------------------------------------------------------------------
# Primary
# --------------------------------------------------------------------------------------------------------------


@dataclass
class Primary:
    """Events: ('begin', t) assigns the next id to t; ('record', t) writes t's commit record; ('end', t) removes t
    from the running set; ('marker',) assigns an id to nobody (``pg_current_xact_id()``) and notes it.

    ``run`` returns, after every event, what a snapshot taken then would be, and what the stream has been able to
    deliver so far (transactions whose commit record is written, in record order).
    """

    events: tuple

    def run(self) -> list[dict]:
        next_xid = FIRST_XID
        xid: dict[int, int] = {}
        running: set[int] = set()
        recorded: list[int] = []
        ended: set[int] = set()
        latest_completed = FIRST_XID - 1
        out = []
        marker = None
        for ev in self.events:
            marker = None
            if ev[0] == "begin":
                xid[ev[1]] = next_xid
                running.add(next_xid)
                next_xid += 1
            elif ev[0] == "record":
                recorded.append(ev[1])
            elif ev[0] == "end":
                running.discard(xid[ev[1]])
                ended.add(ev[1])
                latest_completed = max(latest_completed, xid[ev[1]])
            elif ev[0] == "marker":
                marker = next_xid
                next_xid += 1
                # the marker's own tiny transaction ends at once; it never committed anything
                latest_completed = max(latest_completed, marker)
            xmax = latest_completed + 1
            xip = tuple(sorted(x for x in running if x < xmax))
            out.append({
                "bounds": SnapshotBounds(min(xip) if xip else xmax, xmax, xip),
                "visible": frozenset(t for t in ended if t in recorded),
                "delivered": tuple(recorded),
                "xid": dict(xid),
                "marker": marker,
            })
        return out


def primary_well_formed(events: tuple, n: int) -> bool:
    """begin, then optionally record, then end (an end without a record is an abort)."""
    stage = [0] * n
    for ev in events:
        if ev[0] == "marker":
            continue
        t = ev[1]
        if ev[0] == "begin" and stage[t] == 0:
            stage[t] = 1
        elif ev[0] == "record" and stage[t] == 1:
            stage[t] = 2
        elif ev[0] == "end" and stage[t] in (1, 2):
            stage[t] = 3
        else:
            return False
    return all(st == 3 for st in stage)


# --------------------------------------------------------------------------------------------------------------
# Rules, sound and wrong, checked against one reader interleaving
# --------------------------------------------------------------------------------------------------------------

SOUND = ("inside", "outside", "xmin", "xmax", "sharp", "library", "passed", "usable")
WRONG = ("r1_inside", "r1_outside", "naive_sharp", "xmax_inside", "xmin_outside")


def check_reader(w: Standby, k0: int, ks: int, k1: int) -> set[str]:
    """The reader read R0 after ``k0`` atomic steps, took its snapshot after ``ks`` and read R1 after ``k1``
    (``k0 <= ks <= k1``).  Returns the names of the rules, sound and wrong, that this interleaving violates.

    The sound rules (docs/design/cut.md):
      inside   E <= R0 => the snapshot sees the transaction
      outside  E > E1 => it does not (E1: first stream transaction with E > R1)
      xmin     xid < xmin => it sees it;   xmax  xid >= xmax => it does not
      sharp    the commit record starts after R1 (page headers allowed for) => it does not
      library  ``StandbyCut.classify`` is never wrong (it combines all of the above)
      passed   once the stream has applied a transaction with E > R1, it has applied all the snapshot sees
      usable   R0 >= P0 => the snapshot sees every transaction with E <= P0 (P0: any stream position)
    The wrong rules:
      r1_inside    E <= R1 => inside.  R1 stands for any position read after the snapshot was taken: by the
                   statement that takes the snapshot (it runs for some steps) or by a later statement.  So
                   this is also the rule "use the position read in the snapshot statement as R0".
      r1_outside   E > R1 => outside
      naive_sharp  the commit record starts after R1 => outside, without the page header allowance
      xmax_inside  xid < xmax => inside;   xmin_outside  xid >= xmin => outside
    """
    from cdc_v2.pg.cut import INSIDE, OUTSIDE, StandbyCut

    r0, r1 = w.states[k0].position, w.states[k1].position
    snap = w.states[ks]
    stream = w.stream()
    es = [w.e[t] for t in stream]
    e1 = next((e for e in es if e > r1), None)
    cut = StandbyCut(r0=r0, r1=r1, bounds=SnapshotBounds(snap.xmin, snap.xmax), e1=e1, wal_block_size=PAGE)
    bad: set[str] = set()
    seen_max = max((w.e[t] for t in stream if t in snap.visible), default=0)
    for a in es:  # a: an applied position of the stream
        if cut.has_passed(a) and seen_max > a:
            bad.add("passed")
    unseen_min = min((w.e[t] for t in stream if t not in snap.visible), default=None)
    for p0 in es:
        if cut.usable_after(p0) and unseen_min is not None and unseen_min <= p0:
            bad.add("usable")
    for t in stream:
        e, s, xid, vis = w.e[t], w.s[t], w.txs[t].xid, t in snap.visible
        if e <= r0 and not vis:
            bad.add("inside")
        if e1 is not None and e > e1 and vis:
            bad.add("outside")
        if xid < snap.xmin and not vis:
            bad.add("xmin")
        if xid >= snap.xmax and vis:
            bad.add("xmax")
        if cut.starts_after_r1(s) and vis:
            bad.add("sharp")
        v = cut.classify(e, xid, s=s)
        if (v is INSIDE and not vis) or (v is OUTSIDE and vis):
            bad.add("library")
        if e <= r1 and not vis:
            bad.add("r1_inside")
        if e > r1 and vis:
            bad.add("r1_outside")
        if s > r1 and vis:
            bad.add("naive_sharp")
        if xid < snap.xmax and not vis:
            bad.add("xmax_inside")
        if xid >= snap.xmin and vis:
            bad.add("xmin_outside")
    return bad


def check_world(w: Standby) -> dict:
    """Every reader interleaving of one world.  Returns rule -> first (k0, ks, k1) that violates it."""
    found: dict = {}
    n = len(w.states)
    for k0 in range(n):
        for ks in range(k0, n):
            for k1 in range(ks, n):
                for rule in check_reader(w, k0, ks, k1):
                    found.setdefault(rule, (k0, ks, k1))
    return found


def skeletons(n: int, max_len: int) -> list:
    """Every well-formed WAL of at most ``max_len`` records over ``n`` transactions, without boundary flags."""
    out = []

    def extend(prefix: list, ended: frozenset) -> None:
        if len(ended) == n:
            out.append(tuple(prefix))
        if len(prefix) + max(n - len(ended), 1) > max_len:
            return
        extend(prefix + [Rec("X")], ended)
        for t in range(n):
            if t not in ended:
                extend(prefix + [Rec("W", t)], ended)
                extend(prefix + [Rec("C", t)], ended | {t})
                extend(prefix + [Rec("A", t)], ended | {t})

    extend([], frozenset())
    return out


def id_variants(n: int):
    """Assignments of ids to ``n`` transactions: every order of the top-level ids; at most one transaction has
    a subtransaction (its id directly after its parent's, or after all others); at most one transaction is
    filtered out of the stream."""
    from itertools import permutations

    for order in permutations(range(n)):  # order[r] = the transaction with the r-th smallest top-level id
        for sub_owner in [None] + list(range(n)):
            for far in ((False, True) if sub_owner is not None else (False,)):
                slots = []
                for t in order:
                    slots.append(("top", t))
                    if t == sub_owner and not far:
                        slots.append(("sub", t))
                if far:
                    slots.append(("sub", sub_owner))
                top = {t: FIRST_XID + i for i, (kind, t) in enumerate(slots) if kind == "top"}
                sub = {t: (FIRST_XID + i,) for i, (kind, t) in enumerate(slots) if kind == "sub"}
                for hidden in [None] + list(range(n)):
                    yield tuple(Tx(top[t], sub.get(t, ()), in_stream=(t != hidden)) for t in range(n))


STARTS = (SEGMENT + 40, 2 * SEGMENT - PAGE + 24)  # the second: one record to a boundary reaches a segment start


def worlds(n: int, max_len: int):
    """Every small world: skeleton x at most one record that ends on a page boundary x id assignment."""
    for wal in skeletons(n, max_len):
        layouts = [(wal, STARTS[0])]
        for i in range(len(wal)):
            flagged = wal[:i] + (Rec(wal[i].kind, wal[i].tx, True),) + wal[i + 1:]
            layouts += [(flagged, start) for start in STARTS]
        for txs in id_variants(n):
            for layout, start in layouts:
                yield txs, layout, start


def explore(n: int, max_len: int, *, advance_first: bool = False) -> dict:
    """Checks every reader interleaving of every small world.  Returns counts and, per violated rule, the
    number of worlds that violate it and the first witness."""
    out = {"worlds": 0, "interleavings": 0, "violations": {}, "witness": {}}
    for txs, wal, start in worlds(n, max_len):
        w = Standby(txs, wal, advance_first=advance_first, start=start).run()
        k = len(w.states)
        out["worlds"] += 1
        out["interleavings"] += k * (k + 1) * (k + 2) // 6
        for rule, where in check_world(w).items():
            out["violations"][rule] = out["violations"].get(rule, 0) + 1
            out["witness"].setdefault(rule, describe(w, *where))
    return out


def describe(w: Standby, k0: int, ks: int, k1: int) -> str:
    wal = " ".join(r.kind + ("" if r.tx is None else str(r.tx)) + ("|" if r.to_boundary else "") for r in w.wal)
    ids = ", ".join("t%d: xid %d%s%s" % (i, t.xid, " sub %s" % (t.subxids,) if t.subxids else "",
                                         "" if t.in_stream else " (not in stream)") for i, t in enumerate(w.txs))
    snap = w.states[ks]
    return ("WAL [%s]; %s; R0=%d read after step %d; snapshot after step %d sees %s, xmin=%d xmax=%d; "
            "R1=%d read after step %d; commit records (start, end): %s"
            % (wal, ids, w.states[k0].position, k0, ks, sorted(snap.visible), snap.xmin, snap.xmax,
               w.states[k1].position, k1, {t: (w.s[t], w.e[t]) for t in w.committed()}))


# --------------------------------------------------------------------------------------------------------------
# Primary: rules, sound and wrong
# --------------------------------------------------------------------------------------------------------------

PRIMARY_SOUND = ("exact", "xmin_ge_x", "xmin_gt_max_delivered", "delivered_inside")
PRIMARY_WRONG = ("later_snapshot", "xmax_ge_x")


def check_primary(events: tuple) -> dict:
    """One primary history, every choice of: P0 (after event ``p`` the stream has delivered every commit record
    written so far), the marker X (taken after P0), the snapshot (taken after the marker, or for the rules that
    use no marker, after P0).  "Lost": the snapshot does not see a transaction delivered at or before P0.

      exact                  ``PrimaryCut.classify`` says inside exactly for what the snapshot sees
      xmin_ge_x              snapshot xmin >= X => nothing lost
      xmin_gt_max_delivered  snapshot xmin > the largest id delivered at or before P0 => nothing lost
      delivered_inside       nothing lost exactly when the cut calls every delivered transaction inside
      later_snapshot (wrong) a snapshot taken after P0 loses nothing
      xmax_ge_x (wrong)      snapshot xmax >= X => nothing lost
    """
    from cdc_v2.pg.cut import INSIDE, PrimaryCut

    st = Primary(events).run()
    bad: dict = {}
    final = st[-1]
    cuts = [PrimaryCut(x["bounds"]) for x in st]
    for i, snap in enumerate(st):
        for t in final["delivered"]:
            if (cuts[i].classify(final["xid"][t]) is INSIDE) != (t in snap["visible"]):
                bad.setdefault("exact", (None, None, i))
    markers = [m for m, x in enumerate(st) if x["marker"] is not None]
    for p, at_p in enumerate(st):
        delivered = at_p["delivered"]
        if not delivered:
            continue
        ids = [at_p["xid"][t] for t in delivered]
        for i in range(p + 1, len(st)):
            snap = st[i]
            lost = any(t not in snap["visible"] for t in delivered)
            if lost:
                bad.setdefault("later_snapshot", (p, None, i))
            if lost and snap["bounds"].xmin > max(ids):
                bad.setdefault("xmin_gt_max_delivered", (p, None, i))
            if lost != any(cuts[i].classify(x) is not INSIDE for x in ids):
                bad.setdefault("delivered_inside", (p, None, i))
            for m in markers:
                if p < m <= i:
                    if lost and snap["bounds"].xmin >= st[m]["marker"]:
                        bad.setdefault("xmin_ge_x", (p, m, i))
                    if lost and snap["bounds"].xmax >= st[m]["marker"]:
                        bad.setdefault("xmax_ge_x", (p, m, i))
    return bad


def primary_histories(n: int, markers: int = 1) -> list:
    """Every well-formed primary history of ``n`` transactions with exactly ``markers`` marker events.
    Transaction 0 begins first, then 1, and so on (ids follow the order of begin in any case)."""
    out = []

    def extend(prefix: list, stage: tuple, left: int) -> None:
        if all(s == 3 for s in stage) and left == 0:
            out.append(tuple(prefix))
            return
        if left:
            extend(prefix + [("marker",)], stage, left - 1)
        for t in range(n):
            if stage[t] == 0 and all(s > 0 for s in stage[:t]):
                extend(prefix + [("begin", t)], stage[:t] + (1,) + stage[t + 1:], left)
            elif stage[t] == 1:
                extend(prefix + [("record", t)], stage[:t] + (2,) + stage[t + 1:], left)
                extend(prefix + [("end", t)], stage[:t] + (3,) + stage[t + 1:], left)  # an abort
            elif stage[t] == 2:
                extend(prefix + [("end", t)], stage[:t] + (3,) + stage[t + 1:], left)

    extend([], (0,) * n, markers)
    return out


def explore_primary(n: int, markers: int = 1) -> dict:
    out = {"histories": 0, "violations": {}, "witness": {}}
    for events in primary_histories(n, markers):
        out["histories"] += 1
        for rule, where in check_primary(events).items():
            out["violations"][rule] = out["violations"].get(rule, 0) + 1
            text = " ".join("%s%s" % (e[0], e[1] if len(e) > 1 else "") for e in events)
            out["witness"].setdefault(rule, "events [%s]; P0 after event %s, marker at event %s, snapshot after "
                                      "event %s (events count from 0)" % ((text,) + tuple(where)))
    return out
