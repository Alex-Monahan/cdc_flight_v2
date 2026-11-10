"""The destination layer in several processes, against real MotherDuck (marker `md`).

    tools/with-env uv run -q pytest -q -m md tests/destination/test_md_procs.py

Every test starts processes of the small system under test (`sut.py`: it moves a position and data rows together
through a lane) on a MotherDuck database `cdc_v2_t_d_<random>` of its own. A test kills its processes and drops its
database at its end, also when it fails. The take-over is the small one of `cdc_v2.testing.destination` (design
4.5: the epoch in the leader row and in every fence row, in one transaction); the leader machine is package L.

Words used here. The *old run* is a process that leads when the test begins. The *newcomer* is a process that takes
over. A process is *frozen* at a crash point when it sent itself SIGSTOP there (action `stop`); it goes on with
SIGCONT. A *data transaction* is a lane transaction that applied at least one event.

The tests write under `evidence/p3/destination/`: `two_process.json`, `takeover_open_tx.json`,
`commit_unknown.json`, `run_again_rule.json`, `connection_break.json`, `crash_ledger.jsonl`, `crash_summary.json`.
"""
import os
import time
import uuid
from collections import Counter

import duckdb
import pytest

import sut
from cdc_v2 import crashpoints, md
from cdc_v2.testing import crash, procs
from cdc_v2.testing import destination as td
from cdc_v2.testing.faults import FaultControl
from cdc_v2.testing.netproxy import NetProxy
from conftest import EVIDENCE, MD_PREFIX
from connect_relay import ConnectRelay

pytestmark = pytest.mark.md

STREAM = ("stream",)
# the points of one data transaction of the SUT, in the order it passes them
POINTS = ("sut.begun", "sut.written", "sut.moved", "stream.fenced", "stream.commit_sent", "stream.committed")
TAKEOVER_KEYS = ("attempts", "conflicts", "listed", "interrupted", "unknown", "seconds")


# ------------------------------------------------------------------------------------------------------ helpers
def tail(proc, n=1800):
    try:
        return proc.log_text()[-n:]
    except Exception:                                        # noqa: BLE001 - only for a failure message
        return ""


def wait_for(what, fn, timeout, proc=None, interval=0.05):
    """Wait until ``fn()`` is true and return its value. Fails when ``proc`` ended first or the time is over."""
    end = time.monotonic() + timeout
    while True:
        value = fn()
        if value:
            return value
        if proc is not None and not proc.alive():
            value = fn()
            if value:
                return value
            raise AssertionError("%s: %s ended first (%r)\n%s" % (what, proc.name, proc.poll(), tail(proc)))
        if time.monotonic() >= end:
            raise AssertionError("%s: not within %.0f s\n%s" % (what, timeout, tail(proc) if proc else ""))
        time.sleep(interval)


def ended(proc, timeout):
    """The exit status of a process that must end by itself within ``timeout`` seconds."""
    try:
        return proc.wait(timeout)
    except procs.DeadlineExceeded:
        raise AssertionError("%s did not end within %.0f s\n%s" % (proc.name, timeout, tail(proc))) from None


def events(proc, ev=None):
    return sut.log_events(proc, ev)


def error_kinds(evs):
    return [e["kind"] for e in evs if e.get("ev") == "error"]


def data_dones(evs):
    return [e for e in evs if e.get("ev") == "done" and e["to"] > e["frm"]]


def counts(kinds):
    return dict(sorted(Counter(kinds).items()))


def story(evs):
    """The events of a process in short: what it did, in order."""
    out = []
    for e in evs:
        ev = e.get("ev")
        if ev == "error":
            out.append("error:" + e["kind"])
        elif ev in ("done", "intend"):
            out.append("%s:%d-%d" % (ev, e["frm"], e["to"]))
        elif ev == "exit":
            out.append("exit:%d" % e["code"])
        else:
            out.append(str(ev))
    return out


def errors_before_first_commit(proc):
    kinds = []
    for e in events(proc):
        if e.get("ev") == "done" and e["to"] > e["frm"]:
            break
        if e.get("ev") == "error":
            kinds.append(e["kind"])
    return counts(kinds)


def takeover_of(proc):
    """What the newcomer's take-over reported, or None when it did not take over."""
    leader = events(proc, "leader")
    if not leader:
        return None
    out = {k: leader[0].get(k) for k in TAKEOVER_KEYS}
    out["epoch"] = leader[0]["epoch"]
    out["ms_after_process_start"] = leader[0]["ms"]
    again = events(proc, "interrupt_earlier")
    if again:
        out["interrupt_earlier_after_the_takeover"] = {k: again[0][k] for k in ("listed", "interrupted")}
    return out


class Scene:
    """One MotherDuck database, one source file and the SUT processes of one test."""

    def __init__(self, directory, lanes=STREAM):
        self.dir = str(directory)
        self.lanes = tuple(lanes)
        self.database = MD_PREFIX + uuid.uuid4().hex[:12]
        self.fleet = procs.Fleet()
        self.writer = sut.SourceWriter(os.path.join(self.dir, "source.jsonl"), self.lanes)
        self.settings = md.Settings(database=self.database, marker=sut.MARKER)
        self._raw = None
        td.create_database(self.database)       # the last statement: nothing to drop when it fails

    def hits(self, tag):
        return os.path.join(self.dir, "hits-%s.jsonl" % tag)

    def hit(self, proc, tag, point, fire=None):
        """The lines of the hit log of ``proc`` for ``point`` (only the firing one with ``fire=True``)."""
        return [h for h in crashpoints.read_log(self.hits(tag))
                if h.get("point") == point and h.get("pid") == proc.pid and (fire is None or h.get("fire") is fire)]

    def start(self, tag, crash=None, env=None, **options):
        """Start a SUT process. ``crash`` is ``(point, hit, action[, delay_ms])``."""
        e = dict(env or {})
        point, hit, action, delay = (tuple(crash) + (0,))[:4] if crash else (None, 1, "kill", 0)
        e.update(crashpoints.env_for(point, hit=hit, action=action, log=self.hits(tag), tag=tag, delay_ms=delay))
        options.setdefault("lanes", self.lanes)
        p = sut.start(self.database, self.writer.path, tag, env=e, log_path=os.path.join(self.dir, tag + ".log"),
                      **options)
        self.fleet.add(p)
        return p

    def frozen(self, tag, point, hit=1, timeout=120.0, **options):
        """Start a SUT process that freezes itself at ``point``; returns when it is frozen."""
        p = self.start(tag, crash=(point, hit, "stop"), **options)
        wait_for("%s frozen at %s" % (tag, point), lambda: self.hit(p, tag, point, fire=True), timeout, proc=p)
        p.wait_state("T", 10)
        return p

    def raw(self):
        if self._raw is None:
            self._raw = td.raw_connect(self.settings)
        return self._raw

    def check(self):
        return sut.check(self.raw(), self.writer.path, self.lanes)

    def caught_up(self):
        c = self.check()
        return c if c["ok"] and c["caught_up"] else None

    def position(self, lane="stream"):
        return int(self.raw().execute(sut.READ_POSITION_SQL, [lane]).fetchone()[0])

    def rows_by_tag(self):
        return {r[0]: int(r[1]) for r in
                self.raw().execute("SELECT tag, count(*) FROM sut.data GROUP BY tag").fetchall()}

    def close(self):
        try:
            self.fleet.kill_all(10)
        finally:
            try:
                if self._raw is not None:
                    self._raw.close()
            except duckdb.Error:
                pass
            finally:
                td.drop_database(self.database)


@pytest.fixture
def make_scene(tmp_path):
    scenes = []

    def make(lanes=STREAM):
        d = tmp_path / ("scene%d" % len(scenes))
        d.mkdir()
        s = Scene(d, lanes)
        scenes.append(s)
        return s

    yield make
    problems = []
    for s in scenes:
        try:
            s.close()
        except Exception as e:                               # noqa: BLE001 - every scene is closed, then reported
            problems.append("%s: %s" % (type(e).__name__, e))
    assert not problems, problems


@pytest.fixture
def scene(make_scene):
    return make_scene()


def init_run(scene, n_events=0):
    """A first run: it takes over, migrates, applies ``n_events`` events and ends. Afterwards the first lane
    transaction of every later process is a data transaction (no migration, no empty first transaction)."""
    if n_events:
        scene.writer.add("stream", n_events)
    p = scene.start("init", exit_idle=0.5)
    assert ended(p, 150).code == 0, tail(p)
    c = scene.check()
    assert c["ok"] and c["caught_up"], c
    return p


# ---------------------------------------------------------------------------- two runs, different epochs
TWO_PROCESS_WHAT = ("two SUT processes on one lane of one MotherDuck database. different_epochs: an old run is "
                    "frozen at a point of its second data transaction, a newcomer takes over and applies what is "
                    "pending, then the old run goes on. same_epoch: two processes work with one epoch (no take-over "
                    "between them); the run-again rule alone keeps them apart. The check after every step: every "
                    "event applied exactly once, the counter equals the position")


def test_two_runs_with_different_epochs_at_every_point(scene, evidence):
    init_run(scene)
    out = {}
    for point in POINTS:
        pos0 = scene.position()
        scene.writer.add("stream", 7)
        pending = scene.writer.counts["stream"] - pos0
        old_tag, new_tag = "old." + point, "new." + point
        old = scene.frozen(old_tag, point, 2)
        t_frozen = time.monotonic()
        n_old = len(events(old))
        new = scene.start(new_tag, exit_idle=3.0)
        assert ended(new, 150).code == 0, (point, tail(new))
        c1 = scene.check()
        assert c1["ok"] and c1["caught_up"], (point, c1)
        pos1 = scene.position()
        scene.writer.add("stream", 2)                        # something the old run would apply if it still could
        frozen_s = time.monotonic() - t_frozen
        t_cont = time.monotonic()
        old.cont()
        st = ended(old, 120)
        after_s = time.monotonic() - t_cont
        assert st.code == sut.EXIT_FENCE_LOST, (point, st, tail(old))
        c2 = scene.check()
        assert c2["ok"] and scene.position() == pos1, (point, c2)
        rows = scene.rows_by_tag()
        # the old run committed its first transaction (3 events), and its second only at `stream.committed`
        own = 6 if point == "stream.committed" else 3
        assert rows.get(old_tag, 0) == own and rows.get(new_tag, 0) == pending - own, (point, rows, pending)
        after = events(old)[n_old:]
        kinds = error_kinds(after)
        assert set(kinds) <= {"Conflict", "ConnectionLost", "CommitUnknown"}, (point, kinds)
        assert [e for e in after if e["ev"] == "fence_lost"], (point, story(after))
        assert len(data_dones(after)) == (1 if point == "stream.committed" else 0), (point, story(after))
        out[point] = {
            "old_run_frozen_for_s": round(frozen_s, 1),
            "newcomer": {"takeover": takeover_of(new), "errors_before_its_first_commit": errors_before_first_commit(new),
                         "ms_first_commit_after_process_start": (data_dones(events(new)) or [{"ms": None}])[0]["ms"],
                         "rows": rows.get(new_tag, 0), "exit_code": 0},
            "old_run_after_it_went_on": {"events": story(after), "seconds_to_exit": round(after_s, 2),
                                         "exit_code": st.code, "rows": rows.get(old_tag, 0)},
            "check": {"after_the_newcomer": c1["lanes"]["stream"], "after_the_old_run_ended": c2["lanes"]["stream"]},
        }
    evidence("two_process.json", "different_epochs",
             {"points": list(POINTS), "by_point": out, "exit_code_3_means": "FenceLost"}, what=TWO_PROCESS_WHAT)


# ---------------------------------------------------------------------------- two processes, the same epoch
def test_two_processes_with_the_same_epoch(scene, evidence):
    w = scene.writer
    w.add("stream", 3)
    lead = scene.start("lead", exit_idle=1.0)
    assert ended(lead, 150).code == 0, tail(lead)
    epoch = events(lead, "leader")[0]["epoch"]
    out = {"epoch": epoch}

    # both free: frozen before their first transaction, then let go at the same moment
    w.add("stream", 30)
    a = scene.frozen("free-a", "sut.read", 1, epoch=epoch, exit_idle=3.0)
    b = scene.frozen("free-b", "sut.read", 1, epoch=epoch, exit_idle=3.0)
    a.cont()
    b.cont()
    assert ended(a, 150).code == 0 and ended(b, 150).code == 0, (tail(a), tail(b))
    c = scene.check()
    assert c["ok"] and c["caught_up"], c
    rows = scene.rows_by_tag()
    assert rows.get("free-a", 0) + rows.get("free-b", 0) == 30, rows
    free = {}
    for tag, p in (("free-a", a), ("free-b", b)):
        kinds = error_kinds(events(p))
        assert set(kinds) <= {"Conflict"}, (tag, kinds)
        free[tag] = {"data_transactions": len(data_dones(events(p))), "rows": rows.get(tag, 0),
                     "errors": counts(kinds)}
    out["both_free"] = {"events": 30, "processes": free, "check": c["lanes"]["stream"]}

    by_point = {}
    for point in POINTS:
        w.add("stream", 9)
        t1, t2 = "c1." + point, "c2." + point
        c1 = scene.frozen(t1, point, 2, epoch=epoch, exit_idle=3.0)
        n1 = len(events(c1))
        c2 = scene.start(t2, epoch=epoch, exit_idle=3.0)
        holds = point in ("sut.moved", "stream.fenced", "stream.commit_sent")     # the first holds the position row
        if holds:
            wait_for("three conflicts of the second process", lambda: len(events(c2, "error")) >= 3, 120, proc=c2)
            blocked = events(c2)
            assert not data_dones(blocked) and set(error_kinds(blocked)) == {"Conflict"}, (point, story(blocked))
            c1.cont()
        else:
            assert ended(c2, 150).code == 0, (point, tail(c2))
            blocked = []
            c1.cont()
        assert ended(c1, 150).code == 0 and ended(c2, 150).code == 0, (point, tail(c1), tail(c2))
        c = scene.check()
        assert c["ok"] and c["caught_up"], (point, c)
        rows = scene.rows_by_tag()
        assert rows.get(t1, 0) + rows.get(t2, 0) == 9, (point, rows)
        after = events(c1)[n1:]
        kinds1, kinds2 = error_kinds(after), error_kinds(events(c2))
        assert set(kinds1) <= {"Conflict", "ConnectionLost"} and set(kinds2) <= {"Conflict"}, (point, kinds1, kinds2)
        if point in ("sut.begun", "sut.written"):
            # the second process applied the events of the frozen transaction: that transaction must not commit
            assert kinds1 and rows[t1] == 3 and rows[t2] == 6, (point, kinds1, rows)
        if point == "stream.committed":
            assert not kinds1 and rows[t1] == 6 and rows[t2] == 3, (point, kinds1, rows)
        if holds:
            # the frozen transaction commits after it went on, unless MotherDuck ended it while it was frozen (its
            # lease, about 29 s, section 8.3): then it got an error and the second process applied its events. The
            # run of 2026-11-10 19:21 UTC saw the second (sut.moved); exactly-once held in both (checked above)
            assert rows.get(t1, 0) >= 6 or (kinds1 and rows.get(t2, 0) >= 6), (point, kinds1, rows)
        by_point[point] = {
            "first_holds_the_position_row": holds,
            "second_while_the_first_is_frozen": {"errors": counts(error_kinds(blocked)),
                                                 "data_transactions": len(data_dones(blocked))} if holds else
                                                "applied everything and ended",
            "first_after_it_went_on": story(after),
            "rows": {"first": rows.get(t1, 0), "second": rows.get(t2, 0)}, "errors_of_the_second": counts(kinds2),
            "check": c["lanes"]["stream"],
        }
    out["first_frozen_at_a_point_of_its_second_transaction"] = by_point
    evidence("two_process.json", "same_epoch", out, what=TWO_PROCESS_WHAT)


# ---------------------------------------------------------------------------- take-over, open transaction
SLOW_S = 15.0                 # how long the slow old run keeps its transaction open
TAKEOVER_LIMIT_S = 100.0      # how long the newcomer tries to take over
HOLDS = {"position_row": "sut.moved", "fence_row": "stream.fenced"}
FATES = ("frozen", "killed", "slow")


def takeover_case(scene, n, hold, fate, interrupt):
    point = HOLDS[hold]
    old_tag, new_tag = "old%d" % n, "new%d" % n
    pos0 = scene.position()
    scene.writer.add("stream", 3)
    pending = scene.writer.counts["stream"] - pos0
    if fate == "frozen":
        old = scene.frozen(old_tag, point, 1)
    elif fate == "killed":
        old = scene.start(old_tag, crash=(point, 1, "kill"))
        assert ended(old, 150).killed, tail(old)
    elif hold == "position_row":
        old = scene.start(old_tag, hold_ms=int(SLOW_S * 1000))
        wait_for("the slow run wrote its rows", lambda: scene.hit(old, old_tag, "sut.written"), 150, proc=old)
        time.sleep(0.5)                                      # the statement that moves the position, then the hold
    else:
        fc = FaultControl(scene.dir, name="faults-" + old_tag)
        # the first COMMIT of the run is the registration of its lane's connection (a transaction of its own
        # since the fix round, item 2); the second one is the data transaction that holds the fence row
        fc.delay_at("md.commit", SLOW_S, skip=1, hits=1)
        old = scene.start(old_tag, env=fc.env())
        wait_for("the slow run reached its COMMIT", lambda: fc.fired("md.commit"), 150, proc=old)
    n_old = len(events(old))
    t0 = time.monotonic()
    new = scene.start(new_tag, exit_idle=3.0, takeover_timeout=TAKEOVER_LIMIT_S, no_interrupt=not interrupt)
    st_new = ended(new, TAKEOVER_LIMIT_S + 200)
    new_s = time.monotonic() - t0
    won = st_new.code == 0
    assert st_new.code in (0, sut.EXIT_NO_TAKEOVER), (hold, fate, st_new, tail(new))
    c1 = scene.check()
    assert c1["ok"] and (c1["caught_up"] or not won), (hold, fate, c1)
    rows = scene.rows_by_tag()
    scene.writer.add("stream", 1)                            # makes an old run that idles try one more transaction
    st_old = None
    if fate == "frozen":
        old.cont()
    if fate != "killed":
        if won:
            st_old = ended(old, 150)
        else:                                                # the old run still leads; it is not part of the result
            time.sleep(3.0)
            st_old = old.kill_and_wait() if old.alive() else old.poll()
    c2 = scene.check()
    assert c2["ok"], (hold, fate, c2)
    rows_after = scene.rows_by_tag()
    takeover = takeover_of(new)
    first = (data_dones(events(new)) or [{"ms": None}])[0]["ms"]
    if won:
        assert rows.get(old_tag, 0) + rows.get(new_tag, 0) == pending, (hold, fate, rows, pending)
        if fate != "killed":
            assert st_old.code == sut.EXIT_FENCE_LOST, (hold, fate, st_old, tail(old))
            assert rows_after.get(old_tag, 0) == rows.get(old_tag, 0), (hold, fate, rows, rows_after)
    if interrupt:
        assert won and takeover["seconds"] < 20, (hold, fate, takeover)
        if fate != "slow":
            assert rows.get(old_tag, 0) == 0 and rows.get(new_tag, 0) == pending, (hold, fate, rows)
    no = events(new, "no_takeover")
    return {
        "old_run": {"held": hold, "fate": fate, "rows_committed": rows.get(old_tag, 0),
                    "events_after_the_newcomer_started": story(events(old)[n_old:]),
                    "exit": st_old.as_dict() if st_old is not None else "killed at the point"},
        "newcomer": {"exit_code": st_new.code, "took_over": won, "takeover": takeover,
                     "no_takeover": {k: no[0].get(k) for k in ("attempts", "conflicts", "seconds")} if no else None,
                     "errors_before_its_first_commit": errors_before_first_commit(new),
                     "ms_first_commit_after_process_start": first,
                     "ms_from_takeover_to_first_commit": first - takeover["ms_after_process_start"]
                     if won and first is not None else None,
                     "rows": rows.get(new_tag, 0), "seconds_from_start_to_exit": round(new_s, 1)},
        "check_after_the_newcomer": c1["lanes"]["stream"],
    }


@pytest.mark.parametrize("interrupt", [True, False], ids=["with_interrupt", "without_interrupt"])
def test_takeover_in_the_middle_of_an_open_transaction(scene, evidence, interrupt):
    init_run(scene)
    out, n = {}, 0
    for hold in HOLDS:
        for fate in FATES:
            n += 1
            out["%s.%s" % (hold, fate)] = takeover_case(scene, n, hold, fate, interrupt)
    evidence("takeover_open_tx.json", "with_interrupt" if interrupt else "without_interrupt", {
        "newcomer_interrupts_the_connections_of_earlier_epochs": interrupt,
        "slow_old_run_keeps_its_transaction_open_for_s": SLOW_S,
        "newcomer_tries_to_take_over_for_s": TAKEOVER_LIMIT_S,
        "exit_codes": {"0": "ended idle", "3": "FenceLost", "4": "no take-over within the time"},
        "cases": out,
    }, what="a newcomer takes over while the old run has a transaction open that holds the position row (old run "
            "at sut.moved) or also the fence row (old run at stream.fenced). The old run is frozen (SIGSTOP), "
            "killed (SIGKILL at the point), or slow and alive. With and without the interrupt of the connections "
            "that _cdc.connections lists for earlier epochs. A SUT process needs 3 to 6 s from its start to its "
            "first statement, so 'at once' is that long")


# ---------------------------------------------------------------------------- commit outcome unknown
DELAYS_MS = (0, 10, 20, 40, 60, 80, 100, 150, 300)
REPEATS = 4


def test_commit_outcome_unknown_kill_at_commit_sent(scene, evidence):
    init_run(scene)
    victims = []
    for rep in range(REPEATS):
        for delay in DELAYS_MS:
            tag = "v%d.%d" % (delay, rep)
            scene.writer.add("stream", 3)
            p = scene.start(tag, crash=("stream.commit_sent", 1, "kill_later", delay))
            st = ended(p, 150)
            assert st.killed, (tag, st, tail(p))
            c = scene.check()                                # exactly once, after every killed commit
            assert c["ok"], (tag, c)
            victims.append((tag, delay, p))
    final = scene.start("final", exit_idle=3.0)
    assert ended(final, 150).code == 0, tail(final)
    c = scene.check()
    assert c["ok"] and c["caught_up"], c
    rows = scene.rows_by_tag()
    assert sum(rows.values()) == scene.writer.counts["stream"], rows
    by_delay = {}
    for tag, delay, p in victims:
        evs = events(p)
        saw = bool(data_dones(evs))
        landed = rows.get(tag, 0) > 0
        assert landed or not saw, (tag, rows.get(tag, 0), story(evs))     # an answered COMMIT is a commit
        assert error_kinds(evs) == [] or set(error_kinds(evs)) <= {"Conflict", "ConnectionLost", "CommitUnknown"}
        d = by_delay.setdefault(str(delay), {"victims": 0, "commit_landed": 0, "client_saw_the_answer": 0,
                                             "landed_without_an_answer_seen": 0, "rows": []})
        d["victims"] += 1
        d["commit_landed"] += landed
        d["client_saw_the_answer"] += saw
        d["landed_without_an_answer_seen"] += landed and not saw
        d["rows"].append(rows.get(tag, 0))
    total = len(victims)
    evidence("commit_unknown.json", "kill_at_commit_sent", {
        "victims": total, "repetitions_per_delay": REPEATS, "delays_ms": list(DELAYS_MS),
        "killed_commits_that_landed": sum(d["commit_landed"] for d in by_delay.values()),
        "killed_commits_that_did_not_land": total - sum(d["commit_landed"] for d in by_delay.values()),
        "landed_without_an_answer_seen": sum(d["landed_without_an_answer_seen"] for d in by_delay.values()),
        "by_delay_ms": by_delay,
        "exactly_once_after_every_victim": True, "final_check": c["lanes"]["stream"],
        "rows_of_the_final_run": rows.get("final", 0),
    }, what="every victim is a SUT process that takes over from the one before, applies three events in one lane "
            "transaction and is killed (SIGKILL from another thread) the given time after the crash point "
            "stream.commit_sent, that is while its COMMIT is on its way or just answered. The next process is the "
            "successor: it follows the run-again rule (reads the position inside its transaction, moves it with a "
            "condition). The check (every event exactly once, counter equals position) ran after every victim")


# ---------------------------------------------------------------------------- the run-again rule
RUN_AGAIN_FATES = ("sigstop", "sigkill", "kill_later")
KILL_LATER_MS = 60


def run_again_case(scene, n, naive, fate):
    """A first copy applies three events and its COMMIT lands after the second copy read the position outside any
    transaction. Returns what the check saw after the second copy ran."""
    pos0 = scene.position()
    scene.writer.add("stream", 6)
    first, second = "first%d" % n, "second%d" % n
    options = {"exit_idle": 3.0, "naive": naive}
    if fate == "sigstop":            # the first copy is frozen just before its COMMIT, the second reads, the first goes on
        f = scene.frozen(first, "stream.commit_sent", 1, max_tx=1)
        s = scene.frozen(second, "sut.read", 1, **options)
        f.cont()
        assert ended(f, 120).code == 0, tail(f)
    elif fate == "sigkill":          # the second reads, the first commits and is killed right after the answer
        s = scene.frozen(second, "sut.read", 1, **options)
        f = scene.start(first, crash=("stream.committed", 1, "kill"))
        assert ended(f, 150).killed, tail(f)
    else:                            # the second reads, the first is killed while its COMMIT is on its way
        s = scene.frozen(second, "sut.read", 1, **options)
        f = scene.start(first, crash=("stream.commit_sent", 1, "kill_later", KILL_LATER_MS))
        assert ended(f, 150).killed, tail(f)
    assert events(s, "read")[0]["known"].get("stream") == pos0, events(s, "read")   # the read did not see the commit
    s.cont()
    assert ended(s, 150).code == 0, tail(s)
    c = scene.check()
    rows = scene.rows_by_tag()
    landed = rows.get(first, 0) > 0
    return landed, c, {
        "second_copy_read_position": pos0, "first_copy_commit_landed": landed,
        "rows": {"first": rows.get(first, 0), "second": rows.get(second, 0)},
        "check": {"ok": c["ok"], "caught_up": c["caught_up"], "problems": c["problems"], **c["lanes"]["stream"]},
    }


@pytest.mark.parametrize("naive", [False, True], ids=["with_the_rule", "naive_control"])
def test_run_again_rule_with_the_naive_control(make_scene, evidence, naive):
    out = {}
    shared = None
    for n, fate in enumerate(RUN_AGAIN_FATES, 1):
        if naive or shared is None:      # a database that holds doubles cannot be used again
            scene = make_scene()
            init_run(scene, 3)
            shared = scene
        landed, c, rec = run_again_case(shared, n, naive, fate)
        if fate != "kill_later":
            assert landed, (fate, rec)
        if naive:
            # the control: without the rule both copies commit whenever the first one landed
            assert c["ok"] is (not landed), (fate, rec)
            if landed:
                assert c["lanes"]["stream"]["doubles"] == 3, (fate, rec)
        else:
            assert c["ok"] and c["caught_up"], (fate, rec)
        out[fate] = rec
    evidence("run_again_rule.json", "naive_control" if naive else "with_the_rule", {
        "second_copy_follows_the_rule": not naive, "kill_later_ms": KILL_LATER_MS, "cases": out,
    }, what="a first copy of a transaction (apply three events, move the position) whose COMMIT lands late, and a "
            "second copy started after a read that did not see it: the second process read the position outside "
            "any transaction and was frozen there (sut.read) until the first copy's COMMIT had landed or its "
            "process was dead. with_the_rule: the second copy reads the position again inside its transaction and "
            "moves it with a condition on the value read. naive_control: it uses the value read before and moves "
            "without a condition; the check then finds every event of the first copy twice")


# ---------------------------------------------------------------------------- lost connection in a child
BREAK_WHAT = ("a lost connection in a SUT process. fault_sites_in_a_child: a fault site raises a connection error "
              "once. real_break: the client library of the child reaches MotherDuck through a local proxy chain "
              "(environment https_proxy and grpc_proxy of the child point to the harness NetProxy, which forwards "
              "to a small CONNECT relay in the test process); the test resets or black-holes the proxy")
CHILD_FAULTS = (            # site, the error the SUT must report, whether the transaction at the fault committed
    ("md.execute", "ConnectionLost", False),
    ("md.fence", "ConnectionLost", False),
    ("md.commit", "CommitUnknown", False),
    ("md.commit_reply", "CommitUnknown", True),
)


def test_lost_connection_in_a_child_by_fault_sites(scene, evidence):
    fc = FaultControl(scene.dir)
    child = scene.start("child", env=fc.env())
    scene.writer.add("stream", 3)
    wait_for("the first commit of the child", lambda: data_dones(events(child)), 150, proc=child)
    wait_for("caught up", scene.caught_up, 60, proc=child, interval=0.3)
    out = {}
    for site, want, lands in CHILD_FAULTS:
        n_ev = len(events(child))
        rid = fc.raise_at(site, "ConnectionError", message="injected: connection reset", hits=1)
        scene.writer.add("stream", 3)
        t0 = time.monotonic()
        wait_for("the error at " + site, lambda: error_kinds(events(child)[n_ev:]), 60, proc=child)
        c = wait_for("caught up after " + site, scene.caught_up, 60, proc=child, interval=0.3)
        seconds = time.monotonic() - t0
        fc.remove(rid)

        def after_the_error():                               # the child's next transaction tells what landed
            evs = events(child)[n_ev:]
            i = next(k for k, e in enumerate(evs) if e["ev"] == "error")
            return [e for e in evs[i + 1:] if e["ev"] == "done"]
        wait_for("the transaction after the error at " + site, after_the_error, 60, proc=child)
        evs = events(child)[n_ev:]
        i = next(k for k, e in enumerate(evs) if e["ev"] == "error")
        intends = [e for e in evs[:i] if e["ev"] == "intend"]
        dones = [e for e in evs[i + 1:] if e["ev"] == "done"]
        landed = bool(intends) and bool(dones) and dones[0]["frm"] == intends[-1]["to"]
        kinds = error_kinds(evs)
        assert kinds == [want], (site, story(evs))
        assert landed is lands, (site, story(evs))
        assert len(fc.fired(site)) == 1, (site, fc.fired(site))
        out[site] = {"error_the_sut_got": kinds, "transaction_at_the_fault_committed": landed,
                     "events": story(evs), "seconds_until_caught_up": round(seconds, 2),
                     "check": c["lanes"]["stream"]}
    assert child.alive(), tail(child)
    evidence("connection_break.json", "fault_sites_in_a_child", {"sites": out}, what=BREAK_WHAT)


BREAK_OPTIONS = {"stmt_deadline": 4, "tx_deadline": 8, "grace": 1, "exit_idle": 4.0}


def test_a_real_connection_break_in_a_child(scene, evidence):
    init_run(scene)
    out = {}
    with ConnectRelay() as relay, NetProxy("127.0.0.1", relay.port, control=False) as px:
        proxy = "http://127.0.0.1:%d" % px.port
        env = {"https_proxy": proxy, "grpc_proxy": proxy}

        def case(name, point, fault, lift, what):
            """``fault()`` runs while the child is frozen at ``point`` of its second transaction; ``lift(first
            error seen or None)`` ends the fault."""
            pos0 = scene.position()
            scene.writer.add("stream", 6)
            before = px.stats()
            accepted = before["accepted"]
            child = scene.frozen(name, point, 2, env=env, **BREAK_OPTIONS)
            n_ev = len(events(child))
            fault()
            t_cont = time.monotonic()
            child.cont()
            lift(child, n_ev, t_cont)
            t_lift = time.monotonic()
            try:
                st = child.wait(150)
            except procs.DeadlineExceeded:
                st = None
            recovered = st is not None and st.code == 0
            seconds = time.monotonic() - t_lift
            px.set_fault("pass")
            successor = None
            if not recovered:                                # a finding; the exactly-once check stays as it is
                if st is None:
                    child.kill_and_wait()
                successor = scene.start(name + ".successor", exit_idle=3.0)
                assert ended(successor, 150).code == 0, tail(successor)
            c = scene.check()
            assert c["ok"] and c["caught_up"], (name, c, tail(child))
            evs = events(child)[n_ev:]
            kinds = error_kinds(evs)
            landed = None
            if kinds:
                i = next(k for k, e in enumerate(evs) if e["ev"] == "error")
                dones = [e for e in evs[i + 1:] if e["ev"] == "done"]
                landed = dones[0]["frm"] == pos0 + 6 if dones else None
            first_error = next((e["ms"] for e in evs if e["ev"] == "error"), None)
            out[name] = {
                "what": what, "child_frozen_at": point, "errors_the_sut_got": kinds, "events": story(evs),
                "transaction_at_the_break_committed": landed if kinds else "committed normally, no error",
                "recovered_by_itself": recovered, "exit": st.as_dict() if st is not None else "killed by the test",
                "seconds_from_lifting_to_exit": round(seconds, 1),
                "proxy_connections_accepted": px.stats()["accepted"] - accepted,
                "proxy_connections_reset": px.stats()["reset_by_fault"] - before["reset_by_fault"],
                "rows_of_the_child": scene.rows_by_tag().get(name, 0), "check": c["lanes"]["stream"],
            }
            if first_error is not None:
                out[name]["ms_of_first_error_after_process_start"] = first_error
            return out[name]

        def first_error(child, n_ev, timeout=60):
            return wait_for("an error of the child", lambda: error_kinds(events(child)[n_ev:]), timeout, proc=child)

        def reset_now():
            px.set_fault("reset")
            time.sleep(0.3)
            px.set_fault("pass")

        # A: every TCP connection is reset while a transaction is open and idle
        r = case("reset", "sut.written", reset_now, lambda child, n_ev, t: None,
                 "every TCP connection of the child is reset (RST) while its transaction is open; then the proxy "
                 "passes again and the child goes on")
        assert r["proxy_connections_reset"] >= 1, r           # the fault did break a live connection of the child

        # B: nothing passes in either direction during a statement; lifted 8 s later
        def lift_b(child, n_ev, t):
            first_error(child, n_ev)
            time.sleep(max(0.0, 8.0 - (time.monotonic() - t)))
            px.set_fault("pass")
        case("blackhole_statement", "sut.begun", lambda: px.set_fault("blackhole", direction="both"), lift_b,
             "nothing passes in either direction from the next statement on; the proxy passes again 8 s later "
             "(what it held is then delivered)")

        # C: the COMMIT arrives, its answer never does
        def lift_c(child, n_ev, t):
            first_error(child, n_ev)
            reset_now()
        r = case("commit_answer_lost", "stream.commit_sent", lambda: px.set_fault("blackhole", direction="down"),
                 lift_c, "the COMMIT reaches MotherDuck, nothing comes back; after the child's error the "
                         "connections are reset and the proxy passes again")
        assert r["errors_the_sut_got"][:1] == ["CommitUnknown"] and r["transaction_at_the_break_committed"] is True, r

        # D: the COMMIT is held on its way up and delivered late, when the successor transaction is already waiting
        def lift_d(child, n_ev, t):
            first_error(child, n_ev)
            time.sleep(4.0)
            px.set_fault("pass")
        r = case("commit_delivered_late", "stream.commit_sent", lambda: px.set_fault("blackhole", direction="up"),
                 lift_d, "the COMMIT is held by the proxy; 4 s after the child's error the proxy passes again and "
                         "delivers what it held: the late COMMIT and the statements of the child's next attempt")
        assert r["errors_the_sut_got"][:1] == ["CommitUnknown"], r

        # E: the COMMIT never arrives
        def lift_e(child, n_ev, t):
            first_error(child, n_ev)
            reset_now()
        r = case("commit_never_arrives", "stream.commit_sent", lambda: px.set_fault("blackhole", direction="up"),
                 lift_e, "the COMMIT is held by the proxy; after the child's error the connections are reset, so "
                         "the COMMIT is never delivered")
        # the outcome is unknown to the layer, and either outcome is right: the client delivered the held COMMIT on
        # a new connection after the reset in the run of 2026-11-10 19:21 UTC (committed), not in the runs before
        assert r["errors_the_sut_got"][:1] == ["CommitUnknown"], r

        stats = px.stats()
        tunnels = relay.tunnels
    assert tunnels >= 5 and stats["accepted"] >= 5, (tunnels, stats)      # the traffic did go through the proxy
    evidence("connection_break.json", "real_break", {
        "child_options": BREAK_OPTIONS, "cases": out,
        "proxy": {k: stats.get(k) for k in ("accepted", "closed_by_fault", "reset_by_fault")},
        "relay_tunnels": tunnels,
    }, what=BREAK_WHAT)


# ---------------------------------------------------------------------------- the harness crash runner
def test_crash_runner_over_the_sut(tmp_path, evidence):
    ledger = os.path.join(EVIDENCE, "crash_ledger.jsonl")
    if os.path.exists(ledger):
        os.remove(ledger)
    config = crash.Config(max_hit=3, crash_timeout=90.0, catch_up_timeout=90.0, workload_interval=0.25,
                          max_workload_steps=40, kills=3, kill_after=(1.0, 9.0), freeze_after=(4.0, 9.0),
                          freeze_for=(4.0, 8.0), settle_after_cont=1.0, workers=3, stop_timeout=10.0)
    del sut.SutAdapter.created[:]
    runner = crash.CrashRunner(sut.SutAdapter, str(tmp_path), ledger_path=ledger, config=config)
    t0 = time.monotonic()
    try:
        runner.run_crash_points(seeds=(1, 2, 3))
        runner.run_crash_points(points=[lane + ".commit_sent" for lane in sut.LANES], seeds=(1, 2, 3),
                                action="kill_later", delay_ms=40)
        runner.run_random_kills(seeds=(1, 2, 3))
        runner.run_freeze(seeds=(1, 2, 3), second_instance=True)
        summary = runner.summary()
        runner.write_summary(os.path.join(EVIDENCE, "crash_summary.json"))
    finally:
        for name in list(sut.SutAdapter.created):            # once more, whatever happened
            assert name.startswith(MD_PREFIX)
            td.drop_database(name)
    assert summary["all_passed"], summary["failures"]
    assert not summary["missing_points"], summary["missing_points"]
    assert sorted(summary["declared_points"]) == sorted(p for lane in sut.LANES for p in md.crash_points(lane))
    evidence("crash_runner.json", "sut_on_motherduck", {
        "runs": summary["runs"], "runs_by_mode": summary["runs_by_mode"], "outcomes": summary["outcomes"],
        "declared_points": len(summary["declared_points"]), "reached_points": len(summary["reached_points"]),
        "seconds": round(time.monotonic() - t0), "databases_created_and_dropped": len(sut.SutAdapter.created),
        "ledger": "evidence/p3/destination/crash_ledger.jsonl", "summary": "evidence/p3/destination/crash_summary.json",
    }, what="the harness crash runner (cdc_v2.testing.crash) over the SUT with three lanes on MotherDuck: a kill at "
            "every lane crash point (3 seeds, the hit chosen by the seed), a kill 40 ms after every commit_sent "
            "point, random kills, and a freeze (SIGSTOP) with a second instance that takes over. After every "
            "scenario a fresh process catches up and the check compares the destination with the source")
