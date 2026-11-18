"""Marker `md`: the fix round of package D against real MotherDuck, where the T0 form does not show it.

    tools/with-env uv run -q pytest -q -m md tests/destination/test_md_fix_round.py

- Items 6 and 15 (F7): a lane on a network path that answers nothing (`blackhole_child.py`, through the harness
  `NetProxy` and the CONNECT relay of this package, as in section 9.2 of the design document; copied from the
  second reviewer's `e3_blackhole.py`). Every call returns within its bound; the fence statement and the
  registration within the statement deadline; the calls that were given up are counted and capped, so threads
  and client connections stop growing; everything is freed after the network answers again.
- Item 8 (F0): the take-over at every point of a lane transaction, a few rounds of every case (the long campaign
  is `fence_campaign.py`).

The T0 tests of the fix round that make sense on MotherDuck run again through `test_md_again.py`. Evidence:
`evidence/p3/destination/fix_round.json`.
"""
import json
import os
import subprocess
import sys
import threading
import time

import pytest

import fence_campaign
from cdc_v2 import md, state
from cdc_v2.testing import destination as td
from cdc_v2.testing import machines as tm
from cdc_v2.testing.netproxy import NetProxy
from connect_relay import ConnectRelay

pytestmark = pytest.mark.md
HERE = os.path.dirname(os.path.abspath(__file__))
WHAT = "The fix round of package D on MotherDuck: a black-holed network path, and the take-over at every point"


class Child:
    """The child process of the black-hole test: one JSON line per command."""

    def __init__(self, db, epoch, env):
        self.p = subprocess.Popen([sys.executable, os.path.join(HERE, "blackhole_child.py"), db, str(epoch)],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True, env=env)
        self.lines, self.err = [], []
        self.cv = threading.Condition()
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=lambda: self.err.extend(self.p.stderr), daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            with self.cv:
                self.lines.append(json.loads(line))
                self.cv.notify_all()

    def send(self, s):
        self.p.stdin.write(s + "\n")
        self.p.stdin.flush()

    def next(self, timeout=120):
        end = time.monotonic() + timeout
        with self.cv:
            while not self.lines:
                left = end - time.monotonic()
                if left <= 0 or self.p.poll() is not None:
                    raise AssertionError("the child gave no line (rc=%s): %s" % (self.p.poll(), "".join(self.err)[-800:]))
                self.cv.wait(left)
            return self.lines.pop(0)


def test_a_black_holed_path_bounds_every_call_and_the_calls_left_waiting(md_dest, evidence):
    dest = md_dest
    epoch = dest.lead()
    for sql in ("CREATE SCHEMA IF NOT EXISTS bh", "CREATE TABLE bh.hold (k INTEGER, v BIGINT)",
                "INSERT INTO bh.hold VALUES (1, 0)"):
        dest.run(sql)
    rec = []
    t0 = time.monotonic()

    def note(**kw):
        kw["t_test"] = round(time.monotonic() - t0, 2)          # since the test began; "t" is the child's own
        rec.append(kw)
        return kw

    with ConnectRelay() as relay, NetProxy("127.0.0.1", relay.port, control=False) as px:
        proxy = "http://127.0.0.1:%d" % px.port
        env = dict(os.environ, https_proxy=proxy, grpc_proxy=proxy, HTTPS_PROXY=proxy)
        env.pop("no_proxy", None)
        env.pop("NO_PROXY", None)
        child = Child(dest.database, epoch, env)
        try:
            child.send("warm")
            r = note(phase="warm", **child.next(180))
            assert r["error"] is None, r

            def phase(name):
                child.send(name)
                r = child.next()
                assert r["ev"] == "READY", r
                px.set_fault("blackhole", direction="both")
                child.send("GO")
                r = note(phase=name, **child.next())
                px.set_fault("pass")
                child.send("warm")
                note(phase=name + ".recover", **child.next())
                return r

            # statement deadline 2 s, block 4 s, grace 0.5 s, connect 3 s
            bounds = {"statement": 2.0 + 0.5, "fence": 2.0 + 0.5, "commit": 4.0 + 0.5, "connect": 3.0 + 0.5,
                      "register": 2.0 + 0.5}
            for name, bound in bounds.items():
                r = phase(name)
                assert r["error"] in ("DeadlineExceeded", "CommitUnknown", "ConnectionLost"), r
                assert r["seconds"] <= bound + 0.6, (name, r)
            # many attempts while the path stays black-holed: two calls are given up, then every call fails at once
            px.set_fault("blackhole", direction="both")
            child.send("repeat 12")
            reps = [note(phase="repeat", **child.next()) for _ in range(12)]
            child.send("monitor 10 5")
            while not note(phase="monitor_still_blackholed", **child.next()).get("last"):
                pass
            px.set_fault("pass")
            note(event="healed")
            child.send("monitor 120 1 until_idle")
            while True:
                r = note(phase="monitor_after_heal", **child.next())
                if r.get("last"):
                    break
            freed_after_s = r["t"]                                  # seconds since the path passed again
            assert r["md_waiting_calls"] == 0 and r["md_background"] == 0, r
            child.send("final")
            final = note(phase="final", **child.next())
            assert final["error"] is None, final
            child.send("quit")
            quit_ = note(phase="quit", **child.next(60))
            assert quit_["idle"] is True
            child.p.wait(30)
            assert child.p.returncode == 0
        finally:
            if child.p.poll() is None:
                child.p.kill()
                child.p.wait(10)
    at_once = [x for x in reps if x["error"] == "ConnectionLost" and x["seconds"] < 0.2]
    given_up = [x for x in reps if x["error"] == "DeadlineExceeded"]
    assert len(given_up) <= 2 and len(at_once) >= 10, reps
    assert max(x["md_waiting_calls"] for x in reps) <= 2 + 1      # the cap, and one side interrupt in flight
    plateau = [x["threads"] for x in reps[len(given_up):]]
    assert max(plateau) == min(plateau), plateau                 # no thread is started once the cap is reached
    evidence("fix_round.json", "black_hole", {
        "what": "a lane whose network path answers nothing (NetProxy blackhole, both directions); child settings: "
                "statement 2 s, block 4 s, grace 0.5 s, connect 3 s (1 attempt), max_waiting_calls 2",
        "bounds_s": bounds, "phases": rec, "freed_after_heal_s": freed_after_s,
        "repeat_given_up": len(given_up), "repeat_failed_at_once": len(at_once),
        "threads_during_repeat": [x["threads"] for x in reps],
        "waiting_calls_during_repeat": [x["md_waiting_calls"] for x in reps]}, what=WHAT)


def test_a_take_over_at_every_point_of_a_lane_transaction_on_motherduck(md_dest, evidence):
    """Item 8 (F0): the permanent test on MotherDuck. Two rounds of the empty block at every point and one of
    every other shape at `stream.fenced`; every round passes every check of `fence_campaign.check_round`."""
    dest = md_dest
    fence_campaign.setup(dest)
    probe = dest.connect("probe")
    records = []
    with fence_campaign.Hooks() as hooks, tm.example_machines() as (door, _job):
        with md.Lane(dest.connect("door"), "stream", td.read_leader(dest.settings)["epoch"]).transaction() as tx:
            for sql in tm.EXAMPLE_TABLES:
                tx.execute(sql)
            state.create(tx, door, fence_campaign.DOOR, "closed", "setup")
        cases = [("nothing", p) for p in fence_campaign.POINTS for _ in range(2)] + \
                [(s, "fenced") for s in fence_campaign.SHAPES[1:]]
        for i, (shape, point) in enumerate(cases):
            records.append(fence_campaign.one_round(dest, probe, hooks, shape, point, i, door,
                                                    probe_before_commit=(i % 2 == 0)))
    evidence("fix_round.json", "takeover_at_every_point", {
        "rounds": len(records), "verdicts": {v: sum(1 for r in records if r["verdict"] == v)
                                             for v in {r["verdict"] for r in records}},
        "records": [{k: r.get(k) for k in ("shape", "point", "old_block", "before", "after", "old_trace",
                                           "old_lane_next", "takeover", "verdict", "old_listed_before_commit")}
                    for r in records]}, what=WHAT)
    assert [r for r in records if r["verdict"] != "pass"] == []
