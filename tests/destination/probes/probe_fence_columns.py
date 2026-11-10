"""Does a take-over that commits while a transaction of the old run is open stop that transaction at its fence
statement? It depends on which **columns** the two statements write.

Design 4.2 fences with ``UPDATE _cdc.fence SET n = n + 1 WHERE lane = ? AND epoch = ?`` and design 4.5 takes over
with ``UPDATE _cdc.fence SET epoch = ? + 1``. The two write different columns of the same row. This probe runs the
interleavings for four pairs of statement texts, on local DuckDB and on MotherDuck, with plain client connections:

  stale    old: BEGIN, a data statement | new: take-over, COMMIT | old: fence statement, COMMIT
  fenced   old: BEGIN, data, fence statement | new: take-over, COMMIT | old: COMMIT
  overlap  old: BEGIN, data | new: BEGIN, take-over statement | old: fence statement, COMMIT | new: COMMIT

and records for each whether the old transaction committed after the take-over had committed (a violation of "a
transaction of the old leader either committed before the take-over or fails", design 4.2).

Run:  tools/with-env uv run python tests/destination/probes/probe_fence_columns.py
Creates and drops one database named cdc_v2_t_d_<random>. Writes evidence/p3/destination/fence_columns.json.
"""
import sys

import duckdb

from common import MD_PREFIX, cls, save, want_motherduck
from cdc_v2.testing import destination as td

FENCE = {
    "n_only (design 4.2)": "UPDATE _cdc.fence SET n = n + 1 WHERE lane = $lane AND epoch = $epoch",
    "n_and_epoch_same_column": "UPDATE _cdc.fence SET n = n + 1, epoch = epoch WHERE lane = $lane AND epoch = $epoch",
    "n_and_epoch_parameter (as built)": "UPDATE _cdc.fence SET n = n + 1, epoch = $epoch WHERE lane = $lane "
                                        "AND epoch = $epoch",
}
TAKEOVER = {
    "epoch_only (design 4.5)": "UPDATE _cdc.fence SET epoch = ? + 1",
    "epoch_and_n": "UPDATE _cdc.fence SET epoch = ? + 1, n = n",
}
ROUNDS = 5


def one(dest, fence_sql, takeover_sql, order):
    """One interleaving on a fresh pair of connections. Returns what the old transaction saw."""
    old, new, look = dest.raw(), dest.raw(), dest.raw()
    try:
        epoch = look.execute("SELECT epoch FROM _cdc.fence WHERE lane = 'stream'").fetchone()[0]
        before = look.execute("SELECT v FROM probe_t").fetchone()[0]
        steps = []
        state = {"takeover": "open", "old": "open", "fence_rows": None}

        def rollback(con):
            try:
                con.execute("ROLLBACK")
            except duckdb.Error:
                pass

        def old_fence():
            if state["old"] != "open":
                return
            try:
                state["fence_rows"] = old.execute(fence_sql, {"lane": "stream", "epoch": epoch}).fetchone()[0]
                steps.append("old fence statement: %d row" % state["fence_rows"])
            except duckdb.Error as e:
                state["old"] = cls(e)
                steps.append("old fence statement: " + state["old"])
                rollback(old)

        def old_commit():
            if state["old"] != "open":
                return
            if state["fence_rows"] != 1:
                rollback(old)
                state["old"] = "rolled back by the exactly-one-row rule"
            else:
                try:
                    old.execute("COMMIT")
                    state["old"] = "committed"
                except duckdb.Error as e:
                    state["old"] = cls(e)
                    rollback(old)
            steps.append("old COMMIT: " + state["old"])

        def takeover_statement():
            try:
                new.execute("BEGIN")
                new.execute(takeover_sql, [epoch])
                steps.append("take-over statement: ok")
            except duckdb.Error as e:
                state["takeover"] = cls(e)
                steps.append("take-over statement: " + state["takeover"])
                rollback(new)

        def takeover_commit():
            if state["takeover"] != "open":
                return
            try:
                new.execute("COMMIT")
                state["takeover"] = "committed"
            except duckdb.Error as e:
                state["takeover"] = cls(e)
                rollback(new)
            steps.append("take-over COMMIT: " + state["takeover"])

        old.execute("BEGIN")
        old.execute("UPDATE probe_t SET v = v + 1")
        order_of = {
            "stale": (takeover_statement, takeover_commit, old_fence, old_commit),
            "fenced": (old_fence, takeover_statement, takeover_commit, old_commit),
            "overlap": (takeover_statement, old_fence, old_commit, takeover_commit),
        }[order]
        commits = []
        for step in order_of:
            step()
            if step is old_commit and state["old"] == "committed":
                commits.append("old")
            if step is takeover_commit and state["takeover"] == "committed":
                commits.append("take-over")
        after = look.execute("SELECT v FROM probe_t").fetchone()[0]
        epoch_after = look.execute("SELECT epoch FROM _cdc.fence WHERE lane = 'stream'").fetchone()[0]
        assert (after == before + 1) == ("old" in commits) and (epoch_after == epoch + 1) == ("take-over" in commits)
        return {
            "steps": steps,
            "take_over_committed": "take-over" in commits,
            "old_data_committed": "old" in commits,
            "violation": commits == ["take-over", "old"],
        }
    finally:
        for c in (old, new, look):
            try:
                c.close()
            except duckdb.Error:
                pass


def run(dest):
    setup = dest.raw()
    setup.execute("CREATE TABLE probe_t (v BIGINT)")
    setup.execute("INSERT INTO probe_t VALUES (0)")
    setup.close()
    out = []
    for fname, fsql in FENCE.items():
        for tname, tsql in TAKEOVER.items():
            for order in ("stale", "fenced", "overlap"):
                results = [one(dest, fsql, tsql, order) for _ in range(ROUNDS)]
                distinct = []
                for r in results:
                    if r["steps"] not in distinct:
                        distinct.append(r["steps"])
                out.append({
                    "fence_statement": fname, "take_over_statement": tname, "order": order, "rounds": ROUNDS,
                    "violations": sum(r["violation"] for r in results),
                    "take_overs_committed": sum(r["take_over_committed"] for r in results),
                    "old_commits": sum(r["old_data_committed"] for r in results),
                    "what_happened": distinct,
                })
                print(out[-1]["fence_statement"], "|", out[-1]["take_over_statement"], "|", order, "| violations",
                      out[-1]["violations"], "of", ROUNDS, "|", distinct[0], file=sys.stderr)
    return out


def main():
    body = {"fence_statements": FENCE, "take_over_statements": TAKEOVER,
            "violation_means": "the take-over committed, and the old transaction committed after it"}
    with td.local_destination() as dest:
        body["local_duckdb"] = run(dest)
    if want_motherduck():
        with td.motherduck_destination(MD_PREFIX) as dest:
            body["motherduck"] = run(dest)
    else:
        body["motherduck"] = "not run (no token in the environment)"
    save("fence_columns.json", "tools/with-env uv run python tests/destination/probes/probe_fence_columns.py",
         "Fence statement against a take-over that commits while the old transaction is open: which column pairs "
         "stop the old transaction", body)


if __name__ == "__main__":
    main()
