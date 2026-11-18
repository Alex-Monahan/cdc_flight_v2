"""The heartbeat, the release and a second take-over against the take-over, in every order (destination section
6: "Package L must test its own statements against its own take-over, in every order, as
`tests/destination/probes/probe_fence_columns.py` does for the fence row").

Every exclusion rests on a column that both sides change with a changed value (core 4.2):

| A (an old run, or a rival) | meets the take-over B on |
|---|---|
| heartbeat (`md.schema.HEARTBEAT_SQL`, autocommit) | `_cdc.heartbeat.beat_at` |
| release (`md.schema.RELEASE_SQL` in a fenced control-lane transaction) | the `control` fence row's `n` |
| another take-over from the same epoch | `_cdc.leader.epoch` |

The orders, on plain client connections with the exact texts (A and B each in a transaction of its own; the
heartbeat's autocommit statement is a transaction of one statement):

- ``a_commits_first``: A whole, then B whole (the baseline: both may commit, but a second take-over must not);
- ``stale``: A begins and reads (its snapshot), B commits, then A writes and commits;
- ``a_writes_first``: A writes, B writes, B commits, A commits;
- ``b_writes_first``: B writes, A writes, B commits, A commits.

A violation is a write of A that commits after B's take-over has committed, or two take-overs that both commit.
A heartbeat or a release that changes no row commits nothing (the layer raises `FenceLost`), and so does a
transaction whose statement met a conflict. Each round starts after a `CHECKPOINT`, where a write of an unchanged
value would not be seen (destination section 14.4).
"""
import uuid

import duckdb

from cdc_v2.md import schema as md_schema
from cdc_v2.md.lane import FENCE_SQL

ORDERS = ("a_commits_first", "stale", "a_writes_first", "b_writes_first")
CONTESTANTS = ("heartbeat", "release", "takeover")
E = 5


def _takeover_steps(boot, run_id):
    return [(boot.TAKEOVER_LEADER_SQL, ["f", 1, run_id, E], 1), (boot.TAKEOVER_FENCE_SQL, [E], 18),
            (boot.TAKEOVER_HEARTBEAT_SQL, [E], 1)]


def steps_of(boot, contestant):
    if contestant == "heartbeat":
        return [(md_schema.HEARTBEAT_SQL, [0, "streaming", E], 1)]
    if contestant == "release":
        return [(md_schema.RELEASE_SQL, [E], 1), (FENCE_SQL, [E, "control", E], 1)]
    return _takeover_steps(boot, "rival-" + uuid.uuid4().hex[:6])


def reset(con):
    con.execute("UPDATE _cdc.leader SET epoch = ?, state = 'leading', run_id = 'old', flight_id = 'f'", [E])
    con.execute("UPDATE _cdc.fence SET epoch = ?, n = n + 1", [E])
    con.execute("UPDATE _cdc.heartbeat SET epoch = ?, beat_at = uuid_extract_timestamp(uuidv7())", [E])


class Side:
    def __init__(self, con, steps):
        self.con, self.steps = con, steps
        self.state = "new"            # new, open, failed, committed
        self.what = ""

    def begin(self):
        self.con.execute("BEGIN")
        self.state = "open"

    def read(self):
        self.con.execute("SELECT epoch FROM _cdc.leader").fetchall()

    def write(self):
        if self.state != "open":
            return
        for sql, params, want in self.steps:
            try:
                n = self.con.execute(sql, params).fetchone()[0]
            except duckdb.Error as e:
                self.fail("conflict" if "onflict" in str(e) else type(e).__name__)
                return
            if n != want:
                self.fail("%d rows" % n)
                return

    def commit(self):
        if self.state != "open":
            return False
        try:
            self.con.execute("COMMIT")
            self.state = "committed"
            return True
        except duckdb.Error as e:
            self.fail("commit: " + ("conflict" if "onflict" in str(e) else type(e).__name__))
            return False

    def fail(self, what):
        self.what = what
        self.state = "failed"
        try:
            self.con.execute("ROLLBACK")
        except duckdb.Error:
            pass


def one(boot, cons, contestant, order, checkpoint):
    a_con, b_con, look = cons
    reset(look)
    if checkpoint:
        try:
            look.execute("CHECKPOINT")
        except duckdb.Error:
            pass
    a = Side(a_con, steps_of(boot, contestant))
    b = Side(b_con, _takeover_steps(boot, "new-" + uuid.uuid4().hex[:6]))
    commits = []

    def commit(side, name):
        if side.commit():
            commits.append(name)
    if order == "a_commits_first":
        a.begin(); a.write(); commit(a, "a")
        b.begin(); b.write(); commit(b, "b")
    elif order == "stale":
        a.begin(); a.read()
        b.begin(); b.write(); commit(b, "b")
        a.write(); commit(a, "a")
    elif order == "a_writes_first":
        a.begin(); a.write()
        b.begin(); b.write()
        commit(b, "b"); commit(a, "a")
    else:
        b.begin(); b.write()
        a.begin(); a.write()
        commit(b, "b"); commit(a, "a")
    both = commits == ["a", "b"] or commits == ["b", "a"]
    violation = (commits == ["b", "a"]) or (contestant == "takeover" and both)
    epoch = look.execute("SELECT epoch FROM _cdc.leader").fetchone()[0]
    return {"commits": commits, "violation": violation, "a": a.state + (" (%s)" % a.what if a.what else ""),
            "b": b.state + (" (%s)" % b.what if b.what else ""), "epoch_after": int(epoch)}


def run(boot, open_con, rounds=5, checkpoint=True):
    """Every contestant against the take-over, in every order, ``rounds`` times. Returns a list of cases with the
    counts and the distinct outcomes."""
    cons = [open_con() for _ in range(3)]
    out = []
    try:
        for contestant in CONTESTANTS:
            for order in ORDERS:
                results = [one(boot, cons, contestant, order, checkpoint) for _ in range(rounds)]
                distinct = []
                for r in results:
                    key = {k: r[k] for k in ("commits", "a", "b")}
                    if key not in distinct:
                        distinct.append(key)
                out.append({"contestant": contestant, "order": order, "rounds": rounds,
                            "violations": sum(r["violation"] for r in results),
                            "take_overs_committed": sum("b" in r["commits"] for r in results),
                            "a_committed": sum("a" in r["commits"] for r in results), "outcomes": distinct})
    finally:
        for c in cons:
            try:
                c.close()
            except duckdb.Error:
                pass
    return out
