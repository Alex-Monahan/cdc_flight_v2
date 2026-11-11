"""A take-over at every point of an old run's lane transaction, many rounds, with the fence row, the leader row,
the transitions rows and the answers of both sides recorded after every round (fix round, item 8).

The second review found one case (1 of 30, `e1d_shapes.py`) in which the take-over returned and the old run's
empty block then reported a successful `COMMIT`. Either both transactions committed (a breach of SPEC 4.2), or
MotherDuck answered OK for a transaction that it had rolled back. This program repeats that shape and its
neighbours and tells the two apart by the fence row's counter `n`: only a committed transaction of the lane
`stream` raises it, and a take-over raises it by one too (it never goes back). So between the read before the old
block and the read after it, ``n`` grows by one more than the take-over's own raise exactly when the old block
committed.

One round:

1. the old run takes over (epoch ``e``), opens a lane `stream` and commits one empty transaction (registration);
2. the fence row and the leader row are read (``before``);
3. the old run opens a block of the given **shape** and a newcomer takes over at the given **point**;
4. ``after``: the fence row of `stream`, the epochs of all 18 fence rows, the leader row, the heartbeat's epoch,
   the old block's own trace (a row, a transition) and the rows of `_cdc.transitions` that the round wrote;
5. the old lane runs one more transaction; it must get `FenceLost`.

Checks of every round (a failed check is recorded and counted; the campaign goes on):

- every fence row and the heartbeat row hold the leader's epoch;
- the old lane's next transaction gets `FenceLost`;
- the old block committed (``n`` grew, or its row or transition is there) only at the point ``committed``,
  which lies after its `COMMIT` returned; at every other point it did not;
- the old block reported success exactly when it committed. A success without a commit is an **OK that
  committed nothing**; a commit after the take-over returned is a **breach**.

Shapes: ``nothing`` (the reviewer's case), ``row`` (one inserted row), ``transition`` (one state transition), and
the reviewer's ``ddl_only``, ``other_schema_rows``, ``returning``, ``registered_relation``. Points: ``in_block``
(before the block's end, so before the fence statement), ``fenced``, ``commit_sent``, ``committed`` (the crash
points of the lane; the take-over runs in a second thread and the old block waits for it, at most 25 s).

    tools/with-env uv run python tests/destination/fence_campaign.py --rounds 520 --log <file.jsonl>

The summary is written to ``evidence/p3/destination/fence_campaign.json`` (``--evidence``) through the one writer
of evidence files; every round is one line of the log file. The database is `cdc_v2_t_d_<random>` and is dropped
at the end, also after an error. `test_md_fence.py` runs a few rounds of every point as a permanent test.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import os
import platform
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from cdc_v2 import crashpoints, md, state                                  # noqa: E402
from cdc_v2.md import registry                                             # noqa: E402
from cdc_v2.testing import destination as td                               # noqa: E402
from cdc_v2.testing.harness_evidence import write_evidence                 # noqa: E402
from cdc_v2.testing.machines import example_machines                       # noqa: E402

MD_PREFIX = "cdc_v2_t_d_"
POINTS = ("in_block", "fenced", "commit_sent", "committed")
SHAPES = ("nothing", "row", "transition", "ddl_only", "other_schema_rows", "returning", "registered_relation")
#: The point at which the take-over runs, as the lane's crash point (``in_block`` runs inside the block's body).
CRASH_POINT = {"fenced": "stream.fenced", "commit_sent": "stream.commit_sent", "committed": "stream.committed"}
TAKEOVER_KEYS = ("attempts", "conflicts", "listed", "interrupted", "unknown", "seconds", "last_error")
SETUP = (
    "CREATE SCHEMA IF NOT EXISTS fc",
    "CREATE SCHEMA IF NOT EXISTS fc_other",
    "CREATE TABLE IF NOT EXISTS fc.rows (tag VARCHAR)",
    "CREATE TABLE IF NOT EXISTS fc_other.rows (tag VARCHAR)",
    "CREATE TABLE IF NOT EXISTS fc.ret (k INTEGER, tag VARCHAR)",
    "CREATE TABLE IF NOT EXISTS fc.reg (tag VARCHAR)",
)
DOOR = "fc_door"


class Hooks:
    """Runs a function at a named crash point of this process, once (the lane's crash points are module calls
    of `cdc_v2.crashpoints`; the original runs afterwards, so an armed crash point still works)."""

    def __init__(self):
        self._orig = None
        self._fns = {}

    def __enter__(self):
        self._orig = crashpoints.crash_point

        def hooked(name):
            fn = self._fns.pop(name, None)
            if fn is not None:
                fn()
            return self._orig(name)

        crashpoints.crash_point = hooked
        return self

    def __exit__(self, *exc):
        crashpoints.crash_point = self._orig
        self._fns.clear()

    def at(self, name, fn):
        self._fns[name] = fn

    def clear(self):
        self._fns.clear()


def _label(e):
    """What an answer was: ``OK`` or the class of the exception and the layer's own message (it holds no
    value of a row: the layer's errors repeat a class name and a phrase from a fixed list only)."""
    if e is None:
        return "OK"
    return "%s: %s" % (type(e).__name__, str(e)[:240])


def setup(dest):
    """Tables of the shapes, and the leader's migrations (for `_cdc.transitions`)."""
    epoch = dest.lead()
    conn = md.connect("setup", dest.settings)
    try:
        lane = md.Lane(conn, "control", epoch)
        with lane.transaction() as tx:
            for sql in SETUP:
                tx.execute(sql)
            tx.execute("INSERT INTO fc.ret VALUES (1, 'x')")
    finally:
        conn.close()
    return epoch


def shape_body(shape, tag, tx, door):
    """The statements of the old block. Returns what to look for afterwards: ``(sql, params)`` that counts the
    block's trace, or None (the shape leaves nothing but the fence row's counter)."""
    if shape == "nothing":
        return None
    if shape == "row":
        tx.execute("INSERT INTO fc.rows VALUES (?)", [tag])
        return "SELECT count(*) FROM fc.rows WHERE tag = ?", [tag]
    if shape == "transition":
        stored = state.read(tx, door, DOOR)
        to = "open" if stored == "closed" else "closed"
        state.transition(tx, door, DOOR, stored, to, "fence campaign %s" % tag)
        return "SELECT count(*) FROM _cdc.transitions WHERE cause = ?", ["fence campaign %s" % tag]
    if shape == "ddl_only":
        tx.execute("CREATE TABLE fc_other.ddl_%s (a INTEGER)" % tag)
        return ("SELECT count(*) FROM information_schema.tables WHERE table_catalog = current_database() "
                "AND table_name = ?", ["ddl_%s" % tag])
    if shape == "other_schema_rows":
        tx.execute("INSERT INTO fc_other.rows VALUES (?)", [tag])
        return "SELECT count(*) FROM fc_other.rows WHERE tag = ?", [tag]
    if shape == "returning":
        tx.fetchall("UPDATE fc.ret SET tag = ? WHERE k = 1 RETURNING k", [tag])
        return "SELECT count(*) FROM fc.ret WHERE tag = ?", [tag]
    if shape == "registered_relation":
        import pyarrow as pa
        tx.register("fc_rel", pa.table({"tag": [tag]}))
        tx.execute("INSERT INTO fc.reg SELECT tag FROM fc_rel")
        return "SELECT count(*) FROM fc.reg WHERE tag = ?", [tag]
    raise ValueError("unknown shape %r" % (shape,))


def read_rows(probe):
    """The fence row of `stream`, the set of fence epochs, the leader row and the heartbeat's epoch."""
    epoch, n = probe.fetchone("SELECT epoch, n FROM _cdc.fence WHERE lane = 'stream'")
    epochs = sorted({int(r[0]) for r in probe.fetchall("SELECT DISTINCT epoch FROM _cdc.fence")})
    lead = probe.fetchone("SELECT epoch, state, run_id FROM _cdc.leader")
    hb = probe.fetchone("SELECT epoch FROM _cdc.heartbeat")[0]
    return {"fence_stream": {"epoch": int(epoch), "n": int(n)}, "fence_epochs": epochs,
            "leader": {"epoch": int(lead[0]), "state": lead[1], "run_id": lead[2]}, "heartbeat_epoch": int(hb)}


def listed_on_server(probe, server_id):
    """Whether the server lists the connection with this id (an id this program stored; nothing else is
    asked): it does while the connection has a statement in flight or a transaction that ran a statement."""
    if server_id is None or not probe.is_motherduck:
        return None
    row = probe.fetchone("SELECT count(*) FROM md_active_server_connections() WHERE client_connection_id = ?::UUID",
                         [server_id])
    return int(row[0]) > 0


def one_round(dest, probe, hooks, shape, point, rnd, door, probe_before_commit=False):
    """One round (module text). Returns its record; ``record["checks"]`` lists the checks that failed."""
    settings = dest.settings
    tag = uuid.uuid4().hex[:12]
    run_old = "old-" + tag
    e_old = td.take_over(settings, run_id=run_old)
    conn = md.connect("old", settings)
    lane = md.Lane(conn, "stream", e_old)
    rec = {"round": rnd, "shape": shape, "point": point, "old_epoch": e_old}
    box, rep = {}, {}
    t0 = time.monotonic()
    try:
        with lane.transaction():
            pass                                             # registered under the old epoch, n + 1
        old_id = conn.server_id
        rec["before"] = read_rows(probe)

        def take():
            try:
                box["new_epoch"] = td.take_over(settings, report=rep, timeout_s=20.0, run_id="new-" + tag)
            except Exception as e:                           # noqa: BLE001 - recorded, the round goes on
                box["take_error"] = _label(e)
            box["t_take_returned"] = round(time.monotonic() - t0, 3)

        def at_point():
            box["t_point"] = round(time.monotonic() - t0, 3)
            t = threading.Thread(target=take, name="fc-takeover")
            t.start()
            t.join(25.0)
            box["take_alive_after_wait"] = t.is_alive()
            if probe_before_commit and point != "committed":
                with contextlib.suppress(Exception):
                    box["old_listed_before_commit"] = listed_on_server(probe, old_id)
            box["t_continue"] = round(time.monotonic() - t0, 3)

        trace = None
        err = None
        try:
            with lane.transaction() as tx:
                trace = shape_body(shape, tag, tx, door)
                if point == "in_block":
                    at_point()
                else:
                    hooks.at(CRASH_POINT[point], at_point)
        except Exception as e:                               # noqa: BLE001 - the old block's answer
            err = e
        finally:
            hooks.clear()
        rec["t_block_end"] = round(time.monotonic() - t0, 3)
        rec["old_block"] = _label(err)
        rec["takeover"] = {k: rep.get(k) for k in TAKEOVER_KEYS}
        rec.update({k: v for k, v in box.items()})
        rec["after"] = read_rows(probe)
        rec["old_trace"] = int(probe.fetchone(*trace)[0]) if trace is not None else None
        rec["transitions"] = [list(r) for r in probe.fetchall(
            "SELECT * EXCLUDE (changed_at) FROM _cdc.transitions WHERE cause LIKE ? ORDER BY ALL",
            ["%% %s" % tag])]
        nxt = None
        try:
            with lane.transaction():
                pass
        except Exception as e:                               # noqa: BLE001
            nxt = e
        rec["old_lane_next"] = _label(nxt)
    finally:
        with contextlib.suppress(Exception):
            conn.close()
    rec["checks"] = check_round(rec)
    rec["verdict"] = verdict(rec)
    # the new leader removes the rows of epochs before the previous one (design 4)
    with contextlib.suppress(Exception):
        registry.cleanup(probe, rec["after"]["leader"]["epoch"])
    return rec


def committed_by_counter(rec):
    """How many commits of the old lane the counter shows: its growth less the take-over's own raise."""
    took = 1 if rec.get("new_epoch") is not None else 0
    return rec["after"]["fence_stream"]["n"] - rec["before"]["fence_stream"]["n"] - took


def check_round(rec):
    """The checks of the module text; returns the names of the ones that failed."""
    bad = []
    after = rec["after"]
    lead = after["leader"]["epoch"]
    if rec.get("new_epoch") is None:
        bad.append("take_over_did_not_win")
    if after["fence_epochs"] != [lead] or after["heartbeat_epoch"] != lead:
        bad.append("fence_epoch_differs_from_leader_epoch")
    if not rec["old_lane_next"].startswith("FenceLost"):
        bad.append("old_lane_next_transaction_not_fence_lost")
    grew = committed_by_counter(rec)
    if grew not in (0, 1):
        bad.append("counter_moved_by_%d" % grew)
    traced = rec["old_trace"]
    committed = grew == 1 or bool(traced)
    if traced is not None and bool(traced) != (grew == 1):
        bad.append("trace_and_counter_disagree")
    reported_ok = rec["old_block"] == "OK"
    if rec["point"] == "committed":
        if not (committed and reported_ok):
            bad.append("commit_before_take_over_not_kept")
    else:
        if committed:
            bad.append("old_block_committed_after_take_over")
    if reported_ok and not committed:
        bad.append("ok_that_committed_nothing")
    if committed and not reported_ok and rec["old_block"].split(":")[0] not in ("CommitUnknown",):
        bad.append("committed_but_reported_%s" % rec["old_block"].split(":")[0])
    return bad


def verdict(rec):
    if "old_block_committed_after_take_over" in rec["checks"] or \
            "fence_epoch_differs_from_leader_epoch" in rec["checks"]:
        return "breach"
    if "ok_that_committed_nothing" in rec["checks"]:
        return "ok_that_committed_nothing"
    return "pass" if not rec["checks"] else "failed_check"


def plan(rounds, main_share=0.5):
    """The (shape, point) of every round: half of them the reviewer's case (``nothing`` at ``fenced``), the
    rest round-robin over every other point of the empty block and over the other shapes at ``fenced``."""
    others = [("nothing", p) for p in POINTS if p != "fenced"] + \
             [(s, "fenced") for s in SHAPES if s != "nothing"] + [("row", "commit_sent"), ("transition", "in_block")]
    out = []
    k = 0
    for i in range(rounds):
        if i % 2 == 0 and main_share >= 0.5:
            out.append(("nothing", "fenced"))
        else:
            out.append(others[k % len(others)])
            k += 1
    return out


def commit_of_tree():
    if os.environ.get("CDC_V2_CAMPAIGN_COMMIT"):             # a run from a copy of the tree at a commit
        return os.environ["CDC_V2_CAMPAIGN_COMMIT"]
    try:
        return subprocess.run(["git", "-C", HERE, "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                              timeout=10).stdout.strip() or None
    except Exception:                                        # noqa: BLE001
        return None


def summarise(records, started, ended, args, commit):
    by_case = {}
    for r in records:
        c = by_case.setdefault("%s@%s" % (r["shape"], r["point"]), Counter())
        c["rounds"] += 1
        c["old_block=" + r["old_block"].split(":")[0]] += 1
        c["verdict=" + r["verdict"]] += 1
        c["counter_grew=%d" % committed_by_counter(r)] += 1
    failed = [r for r in records if r["verdict"] != "pass"]
    conflicts = Counter(r["takeover"].get("conflicts") for r in records)
    return {
        "what": "fix round item 8: a take-over at every point of an old run's lane transaction; the fence row, "
                "leader row, transitions rows and both answers after every round",
        "command": "tools/with-env uv run python tests/destination/fence_campaign.py --rounds %d" % args.rounds,
        "commit": commit,
        "started_utc": started, "ended_utc": ended,
        "versions": {"duckdb_client": duckdb.__version__, "python": platform.python_version(),
                     "platform": "%s %s" % (platform.system(), platform.machine())},
        "machine_note": "10 cores, 16 GB, shared with other workers of the same build; one process, one round at "
                        "a time; times are indications only",
        "rounds": len(records),
        "verdicts": dict(Counter(r["verdict"] for r in records)),
        "failed_checks": dict(Counter(c for r in records for c in r["checks"])),
        "by_case": {k: dict(v) for k, v in sorted(by_case.items())},
        "takeover_conflicts": {str(k): v for k, v in sorted(conflicts.items(), key=lambda kv: str(kv[0]))},
        "probed_before_commit": dict(Counter(str(r.get("old_listed_before_commit")) for r in records
                                             if "old_listed_before_commit" in r)),
        "failed_rounds": failed[:40],
        "example_rounds": [r for r in records if r["verdict"] == "pass"][:6],
    }


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=520)
    ap.add_argument("--log", default=None, help="one JSON line per round (outside the repository)")
    ap.add_argument("--evidence", default=os.path.join(os.path.dirname(os.path.dirname(HERE)), "evidence", "p3",
                                                       "destination", "fence_campaign.json"))
    ap.add_argument("--local", action="store_true", help="a local in-memory destination (tier T0)")
    args = ap.parse_args(argv)
    import signal
    # SIGTERM ends the campaign with its clean-up (the database is dropped). SIGINT does not: the client library
    # turns it into an interrupt of the statement in flight, which a round records as its error.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    commit = commit_of_tree()
    started = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
    records = []
    log = open(args.log, "a", encoding="utf-8") if args.log else None
    ctx = td.local_destination() if args.local else td.motherduck_destination(MD_PREFIX)
    try:
        with ctx as dest, Hooks() as hooks:
            setup(dest)
            probe = dest.connect("probe")
            with example_machines() as (door, _job):
                with md.Lane(dest.connect("door"), "stream", td.read_leader(dest.settings)["epoch"]) \
                        .transaction() as tx:
                    for sql in ("CREATE SCHEMA IF NOT EXISTS _cdc_example",
                                "CREATE TABLE IF NOT EXISTS _cdc_example.door (door_id VARCHAR, state VARCHAR, "
                                "opened BIGINT)"):
                        tx.execute(sql)
                    state.create(tx, door, DOOR, "closed", "fence campaign setup")
                for i, (shape, point) in enumerate(plan(args.rounds)):
                    try:
                        rec = one_round(dest, probe, hooks, shape, point, i, door, probe_before_commit=(i % 4 == 2))
                    except Exception as e:                   # noqa: BLE001 - a round that could not be run
                        rec = {"round": i, "shape": shape, "point": point, "verdict": "round_error",
                               "checks": ["round_error"], "error": _label(e), "old_block": "n/a",
                               "takeover": {}, "before": {"fence_stream": {"n": 0}},
                               "after": {"fence_stream": {"n": 0}}}
                    records.append(rec)
                    if log is not None:
                        log.write(json.dumps(rec, default=str, sort_keys=True) + "\n")
                        log.flush()
                    if rec["verdict"] != "pass":
                        print(json.dumps({"round": i, "verdict": rec["verdict"], "checks": rec["checks"]}),
                              flush=True)
                    if (i + 1) % 25 == 0:
                        print(json.dumps({"progress": i + 1, "verdicts": dict(Counter(r["verdict"]
                                                                                       for r in records))}),
                              flush=True)
    finally:
        if log is not None:
            log.close()
    ended = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
    doc = summarise(records, started, ended, args, commit)
    if args.evidence and not args.local:
        write_evidence(args.evidence, doc)
    print(json.dumps({"summary": {k: doc[k] for k in ("rounds", "verdicts", "failed_checks")}}), flush=True)
    return 0 if all(r["verdict"] == "pass" for r in records) else 1


if __name__ == "__main__":
    sys.exit(main())
