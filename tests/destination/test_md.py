"""Marker `md`: what only real MotherDuck can show, in one process: the texts of its errors and what the layer
makes of them, deadlines against a statement that really runs on the server, the connection registry against real
server connections, starters at once, the clock, and timings. Every test works in a database of its own named
`cdc_v2_t_d_<random>` and drops it. The numbers go to `evidence/p3/destination/`.

    tools/with-env uv run -q pytest -q -m md tests/destination/test_md.py
"""
import re
import statistics
import threading
import time
import uuid

import pytest

import sut
from cdc_v2 import md
from cdc_v2.deadline import Deadline
from cdc_v2.md import connection as mdc
from cdc_v2.md import errors as md_errors
from cdc_v2.md import lane as lane_module
from cdc_v2.md import registry, schema
from cdc_v2.testing import destination as td
from cdc_v2.testing import procs
from conftest import MD_PREFIX
from test_lane import BUMP, counter, fence, value
# fence(c)[1] counts commits of the lane, the take-overs and the registrations of new connections: the counter
# only grows (fix round 2)

pytestmark = pytest.mark.md

#: A statement that runs on the server for about ``%d`` ms and then changes the counter row.
SLOW = "UPDATE t SET v = v + coalesce(sleep_ms(%d)::BIGINT, 0) + 1 WHERE k = 1"
LATE = td.LATE_COMMIT_WAIT_S


@pytest.fixture
def dest(md_dest):
    return md_dest


def ms(seconds):
    return round(seconds * 1000, 1)


def stats(values):
    v = sorted(values)
    return {"n": len(v), "min_ms": ms(v[0]), "median_ms": ms(statistics.median(v)),
            "p90_ms": ms(v[int(0.9 * (len(v) - 1))]), "max_ms": ms(v[-1])}


def raw_try(raw, sql, params=None):
    """Run a statement on a plain client connection; returns (ok, text, kind, seconds)."""
    t0 = time.monotonic()
    try:
        raw.execute(sql, params or []).fetchall()
        return True, "ok", None, time.monotonic() - t0
    except Exception as e:      # noqa: BLE001 - the test records whatever the client raised
        return False, md_errors.safe_text(e), md_errors.kind_of(e), time.monotonic() - t0


def in_thread(fn):
    """Run ``fn`` in a thread; returns (thread, box) where box gets ``result`` or ``error`` and ``seconds``."""
    box = {}

    def run():
        t0 = time.monotonic()
        try:
            box["result"] = fn()
        except BaseException as e:      # noqa: BLE001
            box["error"] = e
        box["seconds"] = time.monotonic() - t0
    t = threading.Thread(target=run)
    t.start()
    return t, box


def server_id(raw):
    return str(raw.execute("SELECT md_current_client_connection_id()").fetchone()[0])


# ------------------------------------------------------------------------------------------- connect and timings
def test_connect_keeps_the_server_id_and_the_session_is_utc(dest, evidence):
    times = []
    conns = []
    for i in range(5):
        t0 = time.monotonic()
        conns.append(dest.connect("stream"))
        times.append(time.monotonic() - t0)
    ids = [c.server_id for c in conns]
    assert len(set(ids)) == 5 and all(uuid.UUID(i) for i in ids)
    c = conns[0]
    assert c.is_motherduck and c.usable and c.generation == 1
    assert c.fetchone("SELECT current_setting('TimeZone')") == ("UTC",)
    a, b = c.clock_us(), c.clock_us()
    assert 1_700_000_000_000_000 < a < b
    evidence("timings.json", "connect", {
        "what": "md.connect() five times in one process, one after the other, to a database that this process "
                "already has open (the fixture connected once before); each call includes SET TimeZone, reading the "
                "connection id and one statement on the server",
        "seconds": [round(t, 3) for t in times]},
        what="Timings of the destination layer against MotherDuck")


def test_timings_of_a_lane_transaction(led, evidence):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:                              # registration is not part of the numbers
        tx.execute("SELECT 1")
    empty, one, clock = [], [], []
    for _ in range(25):
        t0 = time.monotonic()
        with lane.transaction():
            pass
        empty.append(time.monotonic() - t0)
    for _ in range(25):
        t0 = time.monotonic()
        with lane.transaction() as tx:
            tx.execute(BUMP)
        one.append(time.monotonic() - t0)
    for _ in range(25):
        t0 = time.monotonic()
        c.clock_us()
        clock.append(time.monotonic() - t0)
    raw = led.raw()
    parts = {"begin": [], "fence_statement": [], "commit": []}
    try:
        for _ in range(25):
            for name, sql, params in (("begin", "BEGIN", []),
                                      ("fence_statement", lane_module.FENCE_SQL, [led.epoch, "control", led.epoch]),
                                      ("commit", "COMMIT", [])):
                t0 = time.monotonic()
                raw.execute(sql, params).fetchall()
                parts[name].append(time.monotonic() - t0)
    finally:
        raw.close()
    assert value(c) == 25 and fence(c)[1] == 53
    evidence("timings.json", "lane_transaction", {
        "what": "one process, one connection, 25 repetitions each; an empty lane transaction is BEGIN, the fence "
                "statement and COMMIT; the parts were measured on a plain client connection with the same texts",
        "empty_lane_transaction": stats(empty), "lane_transaction_with_one_update": stats(one),
        "clock_reading": stats(clock), "begin": stats(parts["begin"]),
        "fence_statement": stats(parts["fence_statement"]), "commit_after_fence": stats(parts["commit"])})


def test_the_registration_row_carries_the_time_of_motherducks_clock(led):
    c = led.connect("look")
    before = c.clock_us()
    lane = led.lane("stream", led.epoch)
    with lane.transaction():
        pass
    after = c.clock_us()
    rows = c.fetchall("SELECT epoch, lane, connection_id::VARCHAR, epoch_us(registered_at) FROM _cdc.connections "
                      "WHERE lane = 'stream'")
    assert [r[:3] for r in rows] == [(led.epoch, "stream", lane.conn.server_id)]
    assert before < rows[0][3] < after                          # three readings of one clock: an exact order


# ------------------------------------------------------------------------------------------- the clock
def test_where_the_clock_expression_is_evaluated(led, evidence):
    """Design 4.1: every time comes from one MotherDuck clock expression. A statement that names no MotherDuck
    table is evaluated by the client, so `md.CLOCK_US_SQL` reads from `_cdc.leader` and every stored time is
    written by a statement that reads or writes a MotherDuck table."""
    raw = led.raw()
    clock = md.clock_sql
    try:
        raw.execute("CREATE TABLE clock_log (k VARCHAR, ts TIMESTAMPTZ)")
        forms = [
            ("bare select", "SELECT epoch_us(%s)" % clock, False),
            ("md.CLOCK_US_SQL", md.CLOCK_US_SQL, True),
            ("insert values", "INSERT INTO clock_log VALUES ('a', %s)" % clock, True),
            ("insert select without a table", "INSERT INTO clock_log SELECT 'a', %s" % clock, True),
            ("insert select from a table", "INSERT INTO clock_log SELECT 'a', %s FROM _cdc.leader" % clock, True),
            ("update set", "UPDATE clock_log SET ts = %s WHERE k = 'a'" % clock, True),
        ]
        plans = {}
        for name, sql, _remote in forms:
            text = "\n".join(str(r[1]) for r in raw.execute("EXPLAIN " + sql).fetchall())
            ops = re.findall(r"([A-Z_]+) \((L|R)\)", text)
            # every statement that MotherDuck runs ends in one client operator that receives the result
            client = [o for o, m in ops if m == "L" and o != "DOWNLOAD_SOURCE"]
            plans[name] = {"operators": ["%s (%s)" % o for o in ops],
                           "run_by_motherduck": not client and any(m == "R" for _o, m in ops)}
        # a bare select costs no round trip; the same expression read from a MotherDuck table costs one
        t_bare, t_remote, inside, whole_ms = [], [], 0, 0
        for _ in range(30):
            w0 = time.time_ns() // 1000
            t0 = time.monotonic()
            v = raw.execute("SELECT epoch_us(%s)" % clock).fetchone()[0]
            t_bare.append(time.monotonic() - t0)
            # the expression counts in whole milliseconds (a UUID version 7 carries no more)
            whole_ms += v % 1000 == 0
            inside += w0 - w0 % 1000 <= v <= time.time_ns() // 1000
            t0 = time.monotonic()
            raw.execute(md.CLOCK_US_SQL).fetchone()
            t_remote.append(time.monotonic() - t0)
    finally:
        raw.close()
    assert statistics.median(t_bare) < 0.01 < statistics.median(t_remote)
    assert inside == 30 and whole_ms == 30                      # the bare select is this machine's clock
    assert "FROM _cdc.leader" in md.CLOCK_US_SQL
    for name, _sql, remote in forms:
        assert plans[name]["run_by_motherduck"] is remote, (name, plans[name])
    evidence("clock.json", "where_evaluated", {
        "what": "EXPLAIN of each statement form: (L) marks an operator the client runs, (R) one that MotherDuck "
                "runs (a statement that MotherDuck runs ends in one client operator, DOWNLOAD_SOURCE, that receives "
                "the result); 30 repetitions of the bare select and of md.CLOCK_US_SQL on one connection",
        "clock_sql": clock, "plans": plans, "bare_select": stats(t_bare), "clock_us_sql": stats(t_remote),
        "bare_select_value_inside_the_clients_own_send_receive_window": "%d of 30" % inside,
        "values_that_are_whole_milliseconds": "%d of 30" % whole_ms,
        "reading": "a bare SELECT of the clock expression is answered by the client without a round trip and is "
                   "the client machine's clock; read through a MotherDuck table it is MotherDuck's"},
        what="Where the clock expression of design 4.1 is evaluated")


# ------------------------------------------------------------------------------------------- error texts
def test_the_texts_of_motherducks_errors_and_their_classes(dest, evidence):
    """The raw material of the error mapping (contract C4), on plain client connections: each case is produced
    for real and `md.errors.kind_of` must put it into the class the layer acts on."""
    out = {}
    a, b = dest.raw(), dest.raw()
    try:
        b.execute("CREATE TABLE t (k INTEGER, v BIGINT)")
        b.execute("INSERT INTO t VALUES (1, 0), (2, 0)")
        a_id = server_id(a)

        def case(name, steps, expect):
            """steps: (connection, sql[, params]) in order; expect: {step number: kind or 'ok'}."""
            seen = []
            for i, step in enumerate(steps):
                ok, text, kind, took = raw_try(*step)
                seen.append({"sql": step[1].split(" WHERE ")[0][:70], "outcome": text, "class": kind, "ms": ms(took)})
                want = expect.get(i)
                if want is not None:
                    assert (kind or "ok") == want, (name, i, text)
            out[name] = seen

        case("write_write_conflict_in_a_statement",
             [(a, "BEGIN"), (a, BUMP), (b, "BEGIN"), (b, BUMP), (b, "ROLLBACK"), (a, "COMMIT")],
             {3: "conflict", 5: "ok"})
        case("commit_after_a_failed_statement_answers_ok_and_commits_nothing",
             [(a, "BEGIN"), (a, BUMP), (b, "BEGIN"), (b, "UPDATE t SET v = v + 100 WHERE k = 2"), (b, BUMP),
              (b, "COMMIT"), (a, "ROLLBACK")],
             {4: "conflict", 5: "ok"})
        assert b.execute("SELECT v FROM t WHERE k = 2").fetchone()[0] == 0
        case("two_transactions_create_one_table",
             [(a, "BEGIN"), (a, "CREATE TABLE x (i INTEGER)"), (b, "BEGIN"), (b, "CREATE TABLE x (i INTEGER)"),
              (b, "ROLLBACK"), (a, "COMMIT")],
             {3: "conflict", 5: "ok"})
        case("idle_open_transaction_interrupted_on_the_server_then_a_statement",
             [(a, "BEGIN"), (a, BUMP), (b, "SELECT md_interrupt_server_connection(?::UUID)", [a_id]), (a, BUMP),
              (a, "SELECT v FROM t WHERE k = 1"), (a, "COMMIT")],
             {3: "transport", 4: "transport", 5: "ok"})
        case("idle_open_transaction_interrupted_on_the_server_then_commit",
             [(a, "BEGIN"), (a, BUMP), (b, "SELECT md_interrupt_server_connection(?::UUID)", [a_id]), (a, "COMMIT")],
             {3: "transport"})
        assert b.execute("SELECT v FROM t WHERE k = 1").fetchone()[0] == 1          # only the first case committed
        case("the_same_client_connection_works_again_after_the_interrupt",
             [(a, "BEGIN"), (a, BUMP), (a, "COMMIT")], {0: "ok", 1: "ok", 2: "ok"})
        assert b.execute("SELECT v FROM t WHERE k = 1").fetchone()[0] == 2

        # a statement in flight on the server, interrupted on the server
        t, box = in_thread(lambda: raw_try(a, SLOW % 20000))
        time.sleep(1.5)
        t0 = time.monotonic()
        assert b.execute("SELECT md_interrupt_server_connection(?::UUID)", [a_id]).fetchone()[0] is True
        t.join(15)
        ended_after = time.monotonic() - t0
        assert not t.is_alive() and box["result"][0] is False and box["result"][2] == "transport"
        assert ended_after < 3.0
        out["statement_in_flight_interrupted_on_the_server"] = [{
            "sql": "an UPDATE that sleeps 20 s on the server", "outcome": box["result"][1], "class": box["result"][2],
            "ended_ms_after_the_interrupt_was_sent": ms(ended_after)}]

        # the client's own interrupt() while the statement runs on the server
        t, box = in_thread(lambda: raw_try(a, SLOW % 4000))
        time.sleep(0.8)
        a.interrupt()
        t.join(20)
        assert not t.is_alive()
        ok, text, kind, took = box["result"]
        out["client_interrupt_while_the_statement_runs_on_the_server"] = [{
            "sql": "an UPDATE that sleeps 4 s on the server; interrupt() of the client after 0.8 s",
            "outcome": text, "class": kind, "returned_after_ms": ms(took),
            "stopped_early": took < 3.0}]
        raw_try(a, "ROLLBACK")
    finally:
        a.close()
        b.close()
    evidence("error_mapping.json", "raw_texts", {
        "what": "plain client connections a and b to one MotherDuck database; each list is one case, statement by "
                "statement; 'class' is what cdc_v2.md.errors.kind_of makes of the error",
        "cases": out},
        what="What MotherDuck and its client raise, and the error of contract C4 the layer turns it into")


# ------------------------------------------------------------------------------------------- the layer's mapping
def test_a_write_write_conflict_in_a_statement_is_conflict(led, evidence):
    c = counter(led)
    a, b = led.lane("stream", led.epoch), led.lane("control", led.epoch)
    with a.transaction() as ta:
        ta.execute(BUMP)
        with pytest.raises(md.Conflict) as info:
            with b.transaction() as tb:
                tb.execute(BUMP)
        with b.transaction() as tb:                             # the loser's lane is usable at once
            tb.execute("INSERT INTO t VALUES (2, 0)")
    assert value(c) == 1 and c.fetchone("SELECT count(*) FROM t") == (2,)
    assert b.conn.generation == 1                               # a conflict does not cost the connection
    evidence("error_mapping.json", "conflict_in_a_statement", {
        "what": "two lanes of one epoch; the second updates a row the first has updated and not committed",
        "raised": type(info.value).__name__, "text": md_errors.safe_text(info.value),
        "loser_rolled_back": True, "connection_kept": True})


def test_a_statement_interrupted_on_the_server_is_connection_lost(led, evidence):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    first = lane.conn.server_id

    def run():
        with lane.transaction() as tx:
            tx.execute(SLOW % 20000)
    t, box = in_thread(run)
    time.sleep(1.5)
    t0 = time.monotonic()
    assert c.interrupt_server_connection(first) is True
    t.join(15)
    ended = time.monotonic() - t0
    assert not t.is_alive() and isinstance(box.get("error"), md.ConnectionLost), box
    assert ended < 3.0
    assert value(c) == 0
    with lane.transaction() as tx:                              # the lane goes on with a new connection
        tx.execute(BUMP)
    assert value(c) == 1 and lane.conn.generation == 2 and lane.conn.server_id != first
    evidence("error_mapping.json", "statement_interrupted_on_the_server", {
        "what": "a lane statement that sleeps 20 s on the server; another connection calls "
                "md_interrupt_server_connection with the lane connection's id after 1.5 s",
        "raised": type(box["error"]).__name__, "text": md_errors.safe_text(box["error"]),
        "ended_ms_after_the_interrupt": ms(ended), "committed": False, "lane_went_on_with_a_new_connection": True})


@pytest.mark.parametrize("then", ["statement", "leave_the_block"])
def test_an_open_transaction_interrupted_on_the_server_is_connection_lost(led, evidence, then):
    """The server rolls an idle open transaction back when its connection is interrupted. The client learns it at
    its next statement, or at the fence statement: `ConnectionLost`, and COMMIT is never sent."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with pytest.raises(md.ConnectionLost) as info:
        with lane.transaction() as tx:
            tx.execute(BUMP)
            assert c.interrupt_server_connection(lane.conn.server_id) is True
            if then == "statement":
                tx.execute(BUMP)
    assert value(c) == 0 and fence(c)[1] == 2
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == 1 and fence(c) == (led.epoch, 4)
    evidence("error_mapping.json", "open_transaction_interrupted_then_" + then, {
        "what": "a lane transaction that has run one UPDATE; its server connection is interrupted; then the "
                "block " + ("runs another statement" if then == "statement" else "ends (the fence statement)"),
        "raised": type(info.value).__name__, "text": md_errors.safe_text(info.value), "committed": False})


def test_a_connection_interrupted_between_the_fence_statement_and_commit_is_commit_unknown(led, faults, evidence):
    """The take-over's interrupt can arrive after the fence statement and before COMMIT is sent. COMMIT then fails
    and nothing lands. The layer cannot tell this from a COMMIT whose answer was lost, so it says `CommitUnknown`."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    faults.block_at("md.commit", hits=1)

    def run():
        with lane.transaction() as tx:
            tx.execute(BUMP)
    t, box = in_thread(run)
    try:
        d = Deadline.after(20.0)
        while not faults.fired(site="md.commit"):
            d.sleep(0.02, "waiting for the transaction to reach COMMIT")
        assert c.interrupt_server_connection(lane.conn.server_id) is True
    finally:
        faults.clear()
    t.join(30)
    assert not t.is_alive() and isinstance(box.get("error"), md.CommitUnknown), box
    time.sleep(LATE)
    assert value(c) == 0 and fence(c) == (led.epoch, 3)         # nothing landed
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == 1
    evidence("error_mapping.json", "interrupted_between_fence_and_commit", {
        "what": "the lane transaction stands before COMMIT (fault site md.commit blocks), its server connection "
                "is interrupted, then COMMIT is sent",
        "raised": type(box["error"]).__name__, "text": md_errors.safe_text(box["error"]),
        "committed": False, "checked_after_s": LATE})


@pytest.mark.parametrize("site, error, lands", [
    ("md.execute", md.ConnectionLost, False), ("md.fence", md.ConnectionLost, False),
    ("md.commit", md.CommitUnknown, False), ("md.commit_reply", md.CommitUnknown, True)])
def test_a_lost_connection_by_fault_site(led, faults, evidence, site, error, lands):
    """The connection breaks (fault site) during a statement, before COMMIT, during COMMIT before it left, and
    during COMMIT after the server executed it. The abandoned transaction holds the counter row and the fence row
    on the server; the lane's next transaction can only work because the replaced connection is interrupted by id."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    faults.raise_at(site, "ConnectionError", message="injected: connection reset", skip=1 if site == "md.execute" else 0,
                    hits=1)
    with pytest.raises(error) as info:
        with lane.transaction() as tx:
            tx.execute(BUMP)
    t0 = time.monotonic()
    with lane.transaction() as tx:
        tx.execute(BUMP)
    again = time.monotonic() - t0
    assert value(c) == (2 if lands else 1) and lane.conn.generation == 2
    evidence("error_mapping.json", "lost_connection_at_" + site, {
        "what": "fault site %s raises ConnectionError once inside a lane transaction with one UPDATE" % site,
        "raised": type(info.value).__name__, "committed": lands,
        "next_transaction_on_a_new_connection_ms": ms(again)})


# ------------------------------------------------------------------------------------------- deadlines
def test_a_statement_that_runs_long_on_the_server_is_ended_at_its_deadline(led, evidence):
    """A real long statement on MotherDuck (DuckDB raises a write-write conflict at once, so a real lock wait does
    not exist; a statement that sleeps on the server stands for it)."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    first = lane.conn.server_id
    grace = led.settings.grace_s
    t0 = time.monotonic()
    with pytest.raises(md.DeadlineExceeded):
        with lane.transaction() as tx:
            tx.execute(SLOW % 30000, deadline=Deadline.after(1.0))
    in_block = time.monotonic() - t0
    assert 1.0 <= in_block < 1.0 + grace + 2.0, in_block
    assert not lane.conn.usable
    other = led.lane("control", led.epoch)
    t0 = time.monotonic()
    with other.transaction() as tx:                             # the row is free: the server transaction is gone
        tx.execute(BUMP)
    free_after = time.monotonic() - t0
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == 2 and lane.conn.generation == 2 and lane.conn.server_id != first
    assert md.wait_idle(Deadline.after(20.0))                   # the abandoned connection is closed by now

    conn = led.connect("auto")                                  # outside a transaction a write is refused (round 2)
    with pytest.raises(md.LaneMisuse):
        conn.execute(SLOW % 30000, None, Deadline.after(1.0))
    t0 = time.monotonic()
    with pytest.raises(md.DeadlineExceeded):
        conn.fetchall("SELECT sleep_ms(30000) FROM t", None, Deadline.after(1.0))
    read = time.monotonic() - t0
    assert read < 1.0 + grace + 2.0
    time.sleep(LATE)
    assert value(c) == 2
    evidence("deadlines.json", "long_statement_on_the_server", {
        "what": "an UPDATE that sleeps 30 s on the server, deadline 1.0 s, grace %.1f s" % grace,
        "caller_got_its_error_after_s": {"in_a_lane_transaction": round(in_block, 2),
                                         "write_outside_a_transaction": "refused before it is sent (fix round 2, N2)",
                                         "read_outside_a_transaction": round(read, 2)},
        "errors": {"in_a_lane_transaction": "DeadlineExceeded", "write_outside_a_transaction": "CommitUnknown",
                   "read_outside_a_transaction": "DeadlineExceeded"},
        "another_lane_updated_the_same_row_right_after_ms": ms(free_after),
        "committed": False},
        what="Deadlines of the destination layer against MotherDuck")


@pytest.mark.parametrize("site, skip, error, lands", [
    ("md.execute", 1, md.DeadlineExceeded, False), ("md.execute", 0, md.DeadlineExceeded, False),
    ("md.fence", 0, md.DeadlineExceeded, False), ("md.commit", 0, md.CommitUnknown, False),
    ("md.commit_reply", 0, md.CommitUnknown, True)])
def test_a_blocked_step_never_holds_the_caller_beyond_deadline_plus_grace(led, faults, evidence, site, skip, error,
                                                                          lands):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    grace, deadline = led.settings.grace_s, 2.0
    open_before = md.open_connections()
    faults.block_at(site, skip=skip, hits=1)
    t0 = time.monotonic()
    with pytest.raises(error):
        with lane.transaction(Deadline.after(deadline)) as tx:
            tx.execute(BUMP)
    took = time.monotonic() - t0
    assert deadline <= took < deadline + grace + 1.5, took
    assert faults.fired(site=site) and not lane.conn.usable
    faults.clear()
    assert md.wait_idle(Deadline.after(20.0))
    assert md.open_connections() == open_before - 1
    time.sleep(LATE if error is md.CommitUnknown else 0)
    assert value(c) == (1 if lands else 0)
    t0 = time.monotonic()
    with lane.transaction() as tx:
        tx.execute(BUMP)
    again = time.monotonic() - t0
    assert value(c) == (2 if lands else 1) and lane.conn.generation == 2
    evidence("deadlines.json", "blocked_at_%s_skip_%d" % (site, skip), {
        "what": "fault site %s blocks (after %d passes) inside a lane transaction with deadline %.1f s, grace %.1f s"
                % (site, skip, deadline, grace),
        "raised": error.__name__, "caller_got_its_error_after_s": round(took, 2), "committed": lands,
        "next_transaction_on_a_new_connection_ms": ms(again)})


def test_an_expired_deadline_stops_the_block_before_commit(led):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    with pytest.raises(md.DeadlineExceeded):
        with lane.transaction(Deadline.after(1.0)) as tx:
            tx.execute(BUMP)
            time.sleep(1.2)
    assert value(c) == 0 and fence(c) == (led.epoch, 3)
    with pytest.raises(md.DeadlineExceeded):
        with lane.transaction(Deadline.after(1.0)) as tx:
            time.sleep(1.2)
            tx.execute(BUMP)
    assert value(c) == 0 and lane.conn.generation == 1          # rolled back in time: the connection is kept
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == 1


def test_a_roll_back_that_hangs_gives_the_connection_up(no_leftovers, faults, evidence):
    with td.motherduck_destination(MD_PREFIX, rollback_deadline_s=1.0) as led:
        epoch = led.lead()
        c = counter(led)
        lane = led.lane("stream", epoch)
        with lane.transaction() as tx:
            tx.execute(BUMP)
        faults.block_at("md.execute", skip=2, hits=1)           # BEGIN, the statement, then ROLLBACK blocks
        t0 = time.monotonic()
        with pytest.raises(KeyError):
            with lane.transaction() as tx:
                tx.execute(BUMP)
                raise KeyError("boom")
        took = time.monotonic() - t0
        assert took < 1.0 + led.settings.grace_s + 2.0
        assert not lane.conn.usable
        faults.clear()
        with lane.transaction() as tx:                          # the row the abandoned transaction held is free
            tx.execute(BUMP)
        assert value(c) == 2 and lane.conn.generation == 2
        evidence("deadlines.json", "roll_back_blocked", {
            "what": "ROLLBACK blocks at a fault site after an exception in the block; roll-back deadline 1.0 s, "
                    "grace %.1f s" % led.settings.grace_s,
            "raised": "the caller's own exception", "caller_got_its_error_after_s": round(took, 2)})


def test_a_replaced_connection_is_interrupted_by_id_before_the_new_one_is_used(led, faults, monkeypatch, evidence):
    """The abandoned client connection leaves a transaction on the server that holds the counter row. The lane's
    next transaction changes the same row; it can only do so because the new connection interrupts the old
    server connection by id first. (The reaper of the old connection does the same in the background through a
    connection of its own; the statement on the new connection is the one that is certain to come first.)"""
    c = counter(led)
    seen = []
    real = mdc.Connection._interrupt_on

    def spy(self, sess, sid, deadline):
        t0 = time.monotonic()
        out = real(self, sess, sid, deadline)
        seen.append((self.role, str(sid), sess.server_id, out, time.monotonic() - t0))
        return out
    monkeypatch.setattr(mdc.Connection, "_interrupt_on", spy)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute(BUMP)
    first = lane.conn.server_id
    faults.raise_at("md.fence", "ConnectionError", message="injected: connection reset", hits=1)
    with pytest.raises(md.ConnectionLost):
        with lane.transaction() as tx:
            tx.execute(BUMP)                                    # the server transaction now holds the counter row
    assert not lane.conn.usable and seen == []
    faults.clear()
    t0 = time.monotonic()
    with lane.transaction() as tx:
        tx.execute(BUMP)
    again = time.monotonic() - t0
    assert len(seen) == 1 and seen[0][:3] == ("stream", first, lane.conn.server_id)     # its own old id, once
    assert lane.conn.server_id != first and lane.conn.generation == 2
    assert value(c) == 2
    rows = c.fetchall("SELECT connection_id::VARCHAR FROM _cdc.connections WHERE lane = 'stream' ORDER BY registered_at")
    assert [r[0] for r in rows] == [first, lane.conn.server_id]
    evidence("registry_takeover.json", "replaced_connection_interrupted_by_id", {
        "what": "a lane transaction loses its connection before COMMIT (fault site md.fence); the next transaction "
                "of the lane connects again, interrupts the old server connection by id and updates the same row",
        "interrupt_statement_ms": ms(seen[0][4]), "old_connection_still_held_a_transaction": seen[0][3],
        "next_transaction_with_connect_interrupt_and_registration_ms": ms(again)})


def test_connect_has_a_deadline_and_tries_again(dest, faults):
    faults.block_at("md.connect")
    t0 = time.monotonic()
    with pytest.raises(md.DeadlineExceeded):
        md.connect("slow", dest.settings, Deadline.after(1.0))
    assert time.monotonic() - t0 < 1.0 + dest.settings.grace_s + 2.0
    faults.clear()
    faults.raise_at("md.connect", "ConnectionError", hits=1)
    conn = md.connect("retry", dest.settings)
    assert conn.usable and conn.generation == 1
    conn.close()


# ------------------------------------------------------------------------------------------- registry, take-over
#: An interrupt answers before the server has let go of the rows of the transaction it ended (seeded by the run of
#: 2026-11-10: a writer that came right after the interrupt got `Conflict`; a take-over needed a third attempt).
#: So a test asserts that the row is free within this time, not at once. Without the interrupt it is never free:
#: the old transaction of these tests belongs to a client that is alive.
FREE_WITHIN_S = 10.0


def won_after_conflicts_only(report):
    """A take-over that interrupted won: at least one conflict, at least one connection interrupted, every attempt
    before the last one ended with a conflict, and it took less than `FREE_WITHIN_S`. How many conflicts it met is
    a measurement (1 or 2 so far); the evidence records it."""
    return (report["conflicts"] >= 1 and report["interrupted"] >= 1 and report["unknown"] == 0
            and report["attempts"] == report["conflicts"] + 1 and report["seconds"] < FREE_WITHIN_S)


def test_an_open_transaction_of_the_old_run_blocks_a_take_over_until_it_is_interrupted(led, evidence):
    """Task: "a transaction left open on an abandoned connection blocks a take-over without this and does not
    with it". The old run's transaction holds its lane's fence row, which the take-over writes (its fence
    statement ran early, through the layer's own entry: callers cannot write the leader tables since fix round 2;
    until then this case held the heartbeat row)."""
    c = counter(led)
    old = led.lane("control", led.epoch)
    without, with_ = {}, {}
    with pytest.raises(md.ConnectionLost) as info:
        with old.transaction() as tx:
            tx.execute(BUMP)
            tx._layer_execute_one(md.FENCE_SQL, [led.epoch, "control", led.epoch], "the fence statement")
            with pytest.raises(td.TakeoverFailed):
                led.take_over(interrupt=False, timeout_s=6.0, report=without)
            assert without["conflicts"] >= 3 and without["interrupted"] == 0
            assert td.read_leader(led.settings)["epoch"] == led.epoch
            assert led.take_over(timeout_s=20.0, report=with_) == led.epoch + 1
            assert won_after_conflicts_only(with_)
            tx.execute(BUMP)                                    # the old run learns it at its next statement
    assert value(c) == 0
    assert c.fetchone("SELECT phase FROM _cdc.heartbeat") == ("starting",)
    with pytest.raises(md.FenceLost):                           # its lane reconnects and is refused
        with old.transaction() as tx:
            tx.execute(BUMP)
    evidence("registry_takeover.json", "open_transaction_holds_a_leader_row", {
        "what": "the old run's control-lane transaction has updated the heartbeat row and stays open",
        "take_over_without_the_interrupt": dict(without, result="TakeoverFailed after 6 s"),
        "take_over_with_the_interrupt": dict(with_, result="won"),
        "old_run_at_its_next_statement": type(info.value).__name__,
        "old_run_at_its_next_transaction": "FenceLost"})


def test_an_open_transaction_that_holds_only_data_rows_does_not_block_the_take_over_but_the_new_run(led, evidence):
    """Why the new leader calls `md.interrupt_earlier` once more after it won: an old transaction that has not
    touched a leader row yet does not conflict with the take-over, so the take-over interrupts nobody, and the
    old transaction goes on holding its data rows."""
    c = counter(led)
    old = led.lane("stream", led.epoch)
    rep = {}
    with pytest.raises(md.ConnectionLost):
        with old.transaction() as tx:
            tx.execute(BUMP)
            new_epoch = led.take_over(report=rep)
            assert rep["conflicts"] == 0 and rep["interrupted"] == 0
            new = led.lane("stream", new_epoch)
            with pytest.raises(md.Conflict):
                with new.transaction() as tx2:
                    tx2.execute(BUMP)                           # the old run's open transaction holds the row
            listed, done = md.interrupt_earlier(c, new_epoch)
            assert listed >= 2 and done >= 1                    # control (migrations) and stream of the old epoch
            t0, conflicts = time.monotonic(), 0
            while True:                                         # the row is free a moment after the interrupt
                try:
                    with new.transaction() as tx2:
                        tx2.execute(BUMP)
                    break
                except md.Conflict:
                    conflicts += 1
                    assert time.monotonic() - t0 < FREE_WITHIN_S, "%d conflicts after the interrupt" % conflicts
                    time.sleep(0.1)
            free_s = time.monotonic() - t0
            assert value(c) == 1
            tx.execute(BUMP)
    assert value(c) == 1 and fence(c) == (new_epoch, 5)
    evidence("registry_takeover.json", "open_transaction_holds_only_data_rows", {
        "what": "the old run's stream-lane transaction has updated a data row and stays open",
        "take_over": dict(rep, result="won without a conflict"),
        "new_run_updating_the_same_row": "Conflict until md.interrupt_earlier(conn, new_epoch) ran",
        "interrupt_earlier": {"listed": listed, "interrupted": done},
        "new_run_after_the_interrupt": {"conflicts": conflicts, "row_free_after_s": round(free_s, 2),
                                        "pause_between_tries_s": 0.1},
        "old_run_at_its_next_statement": "ConnectionLost"})


def test_a_commit_that_hangs_holds_the_fence_row_until_the_take_over_interrupts_it(led, faults, evidence):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    faults.block_at("md.commit", hits=1)

    def old_run():
        with lane.transaction(Deadline.after(40.0)) as tx:
            tx.execute(BUMP)
    t, box = in_thread(old_run)
    without, with_ = {}, {}
    try:
        d = Deadline.after(20.0)
        while not faults.fired(site="md.commit"):
            d.sleep(0.02, "waiting for the old run to reach COMMIT")
        with pytest.raises(td.TakeoverFailed):
            led.take_over(interrupt=False, timeout_s=5.0, report=without)
        assert without["conflicts"] >= 3
        assert led.take_over(timeout_s=20.0, report=with_) == led.epoch + 1
        assert won_after_conflicts_only(with_)
    finally:
        faults.clear()
    t.join(60)
    assert not t.is_alive() and isinstance(box.get("error"), md.CommitUnknown), box
    time.sleep(LATE)
    assert value(c) == 0 and fence(c) == (led.epoch + 1, 4)
    evidence("registry_takeover.json", "old_run_stands_before_commit", {
        "what": "the old run's stream-lane transaction has run its fence statement and stands before COMMIT",
        "take_over_without_the_interrupt": dict(without, result="TakeoverFailed after 5 s"),
        "take_over_with_the_interrupt": dict(with_, result="won"),
        "old_run_gets": "CommitUnknown", "old_transaction_committed": False})


# ------------------------------------------------------------------------------------------- leader tables
def test_starters_at_once_end_with_exactly_one_row_set(no_leftovers, evidence, tmp_path):
    """At least three starters at the same time on a database without `_cdc`: threads of one process (4 at once,
    12 rounds) and separate processes (3 at once, 4 rounds). Every starter succeeds; the tables hold exactly one
    row set. What MotherDuck offers: `CREATE TABLE IF NOT EXISTS ... AS SELECT` creates table and rows in one
    statement; a starter that loses gets a catalog write-write conflict and finds the table at its next try."""
    rounds_threads, rounds_procs, starters = 12, 4, 4
    results = {"threads": [], "processes": []}
    with td.motherduck_destination(MD_PREFIX, leader_tables=False) as dest:
        raw = dest.raw()
        try:
            for _ in range(rounds_threads):
                raw.execute("DROP SCHEMA IF EXISTS _cdc CASCADE")
                conns = [dest.connect("starter") for _ in range(starters)]
                barrier = threading.Barrier(starters)
                out = [None] * starters

                def start(i):
                    barrier.wait(30)
                    try:
                        out[i] = md.ensure_leader_tables(conns[i])
                    except Exception as e:      # noqa: BLE001 - the test reports whatever it was
                        out[i] = md_errors.safe_text(e)
                threads = [threading.Thread(target=start, args=(i,)) for i in range(starters)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(120)
                counts = tuple(raw.execute(schema.CHECK_LEADER_TABLES_SQL).fetchone())
                results["threads"].append({"retries_or_error": out, "rows": list(counts)})
                assert all(isinstance(o, int) for o in out), out
                assert counts == (1, 18, 18, 1)
                schema.check_leader_tables(conns[0])
                for conn in conns:
                    conn.close()
            fleet = procs.Fleet()
            try:
                for r in range(rounds_procs):
                    raw.execute("DROP SCHEMA IF EXISTS _cdc CASCADE")
                    ps = [sut.start(dest.database, str(tmp_path / "none.jsonl"), "s%d-%d" % (r, i), init_only=True,
                                    log_path=str(tmp_path / ("init-%d-%d.log" % (r, i)))) for i in range(3)]
                    for p in ps:
                        fleet.add(p)
                    codes = [p.wait(120).code for p in ps]
                    retries = [e["retries"] for p in ps for e in sut.log_events(p, "leader_tables")]
                    counts = tuple(raw.execute(schema.CHECK_LEADER_TABLES_SQL).fetchone())
                    results["processes"].append({"exit_codes": codes, "retries": retries, "rows": list(counts)})
                    assert codes == [0, 0, 0], [p.log_text()[-300:] for p in ps]
                    assert counts == (1, 18, 18, 1)
            finally:
                fleet.kill_all(10)
        finally:
            raw.close()
    with_retry = sum(1 for r in results["threads"] if any(r["retries_or_error"]))
    evidence("leader_tables_concurrent.json", "starters_at_once", {
        "what": "ensure_leader_tables() by several starters at the same moment on a database without the schema "
                "_cdc; rows = (leader rows, fence rows, distinct lanes, heartbeat rows) after each round",
        "threads": {"starters": starters, "rounds": rounds_threads, "rounds_all_succeeded": rounds_threads,
                    "rounds_in_which_a_starter_had_to_retry": with_retry, "per_round": results["threads"]},
        "processes": {"starters": 3, "rounds": rounds_procs, "per_round": results["processes"]}},
        what="Simultaneous initialisation of the leader tables on MotherDuck")


ROUNDS_AFTER_INTERRUPT = 5
LISTED_SQL = ("SELECT count(*), coalesce(bool_and(contains(client_user_agent, ?)), false) "
              "FROM md_active_server_connections() WHERE client_connection_id = ?::UUID")


def test_what_the_server_lists_and_what_an_interrupt_answers(dest, evidence):
    """`md_active_server_connections()` (asked by one id of this test only) and `md_interrupt_server_connection`,
    state by state of the other connection. The server knows a connection only while it has a statement in flight
    or a transaction with at least one statement; an interrupt answers true in exactly those states."""
    counter(dest)
    marker = dest.settings.marker
    asker, other = dest.raw(), dest.raw()
    rows = {}
    try:
        oid = server_id(other)

        def look(name, interrupt=True):
            listed, agent = asker.execute(LISTED_SQL, [marker, oid]).fetchone()
            rows[name] = {"listed": int(listed)}
            if listed:
                rows[name]["user_agent_carries_the_marker"] = bool(agent)
            if interrupt:
                rows[name]["interrupt_answers"] = bool(asker.execute(mdc.INTERRUPT_SQL, [oid]).fetchone()[0])
            return rows[name]

        assert look("never ran a statement") == {"listed": 0, "interrupt_answers": False}
        other.execute("SELECT v FROM t WHERE k = 1").fetchall()
        other.execute(BUMP)
        assert look("idle after a read and a write in autocommit") == {"listed": 0, "interrupt_answers": False}
        t0 = time.monotonic()
        other.execute("BEGIN")
        begin_s = time.monotonic() - t0
        assert look("after BEGIN alone") == {"listed": 0, "interrupt_answers": False}
        other.execute(BUMP)
        after, held = [], []
        for round_no in range(ROUNDS_AFTER_INTERRUPT):
            if round_no:                                        # a new transaction on the same client connection
                t1 = time.monotonic()
                other.execute("BEGIN")
                while not raw_try(other, BUMP)[0]:              # the row of the round before is still held
                    assert time.monotonic() - t1 < FREE_WITHIN_S
                    raw_try(other, "ROLLBACK")
                    time.sleep(0.1)
                    other.execute("BEGIN")
                held.append(round(time.monotonic() - t1, 2))
            assert look("open transaction with one statement, idle") == {
                "listed": 1, "user_agent_carries_the_marker": True, "interrupt_answers": True}
            # What the server says right after the interrupt is recorded and not asserted: in the first runs it
            # kept the entry until the client came (a second interrupt answered true); on 2026-11-10 it did not.
            again = dict(look("the same transaction after the interrupt, before the client sent anything"))
            time.sleep(1.0)
            again["listed_1s_later"] = look("the same transaction 1 s later", interrupt=False)["listed"]
            after.append(again)
            ok, text, kind, _s = raw_try(other, BUMP)
            assert not ok and kind == "transport", text
            rows["the client's next statement"] = {"error": text, "class": kind}
            raw_try(other, "ROLLBACK")
            assert look("after the client heard of the interrupt") == {"listed": 0, "interrupt_answers": False}
        del rows["the same transaction 1 s later"]
        rows["the same transaction after the interrupt, before the client sent anything"] = {
            "rounds": len(after),
            "still_listed_at_once": sum(a["listed"] for a in after),
            "a_second_interrupt_answered_true": sum(a["interrupt_answers"] for a in after),
            "still_listed_1s_later": sum(a["listed_1s_later"] for a in after),
            "next_transaction_on_the_same_row_began_after_s": held}
        t, box = in_thread(lambda: other.execute(SLOW % 8000).fetchall())
        time.sleep(1.5)
        assert look("statement in flight") == {
            "listed": 1, "user_agent_carries_the_marker": True, "interrupt_answers": True}
        t.join(30)
        assert not t.is_alive() and md_errors.kind_of(box["error"]) == "transport", box
        rows["statement in flight"]["statement_ended_after_s"] = round(box["seconds"] - 1.5, 2)
        other.close()
        time.sleep(0.5)
        assert look("closed") == {"listed": 0, "interrupt_answers": False}
        rows["an id the server never saw"] = {
            "interrupt_answers": bool(asker.execute(mdc.INTERRUPT_SQL, [str(uuid.uuid4())]).fetchone()[0])}
        assert rows["an id the server never saw"]["interrupt_answers"] is False
        ok, text, kind, _s = raw_try(asker, mdc.INTERRUPT_SQL, [server_id(asker)])
        assert not ok and kind == "transport", text
        rows["a statement that interrupts its own connection"] = {"error": text, "class": kind}
        assert asker.execute("SELECT v FROM t WHERE k = 1").fetchone() == (1,)   # usable; only the first BUMP landed
    finally:
        for raw in (asker, other):
            try:
                raw.close()
            except Exception:           # noqa: BLE001 - closing twice
                pass
    evidence("registry_takeover.json", "what_the_server_answers", {
        "what": "one connection asks about one other connection of this test by its id: is it listed by "
                "md_active_server_connections(), and what does md_interrupt_server_connection(id) answer",
        "begin_alone_took_ms": ms(begin_s),
        "states": rows,
        "reading": "BEGIN is not sent to the server. The server lists a connection only while it has a statement "
                   "in flight or a transaction with at least one statement, and an interrupt answers true in "
                   "exactly those states. So the count `interrupted` of md.interrupt_earlier is the number of "
                   "connections that held something, not the number of connections that are still alive, and "
                   "a registered id of an idle connection cannot be told from one that is gone"},
        what="The connection registry and the take-over against MotherDuck")


def test_cleanup_and_interrupt_earlier_with_real_server_connections(led):
    c = counter(led)
    lanes = {led.epoch: led.lane("stream", led.epoch)}
    with lanes[led.epoch].transaction() as tx:
        tx.execute(BUMP)
    for epoch in (2, 3):
        assert led.take_over() == epoch
        lanes[epoch] = led.lane("stream", epoch)
        with lanes[epoch].transaction() as tx:
            tx.execute(BUMP)
    assert registry.interrupt_earlier(c, 1) == (0, 0)
    # control and stream of epoch 1, stream of epoch 2: all idle outside a transaction, so there is nothing to end
    assert registry.interrupt_earlier(c, 3) == (3, 0)
    lanes[1].conn.close()                                       # its server connection is gone
    with pytest.raises(md.ConnectionLost):
        with lanes[2].transaction() as tx:
            tx.execute(BUMP)                                    # the old run holds the counter row
            result = registry.interrupt_earlier(c, 3)
            tx.execute(BUMP)
    assert result == (3, 1)
    # a connection that is listed itself leaves itself out
    assert registry.interrupt_earlier(lanes[3].conn, 4) == (3, 0)
    with lanes[3].transaction() as tx:                          # the leader's own connection was not touched
        tx.execute(BUMP)
    assert value(c) == 4
    assert registry.cleanup(c, 3) == 2
    assert c.fetchall("SELECT DISTINCT epoch FROM _cdc.connections ORDER BY 1") == [(2,), (3,)]


# --------------------------------------------------------------------------------------------- test databases
def test_a_test_database_is_in_the_ledger_until_it_is_dropped():
    """Every database of these tests is created through `cdc_v2.testing.md`, which keeps a ledger. After a run the
    ledger says, by name, whether a database of this package is left (`td.leftover_databases`)."""
    with td.motherduck_destination(MD_PREFIX, leader_tables=False) as dest:
        name = dest.settings.database
        assert name.startswith(MD_PREFIX) and name in td.leftover_databases(MD_PREFIX)
        conn = dest.connect("look")
        assert conn.fetchone("SELECT current_database()")[0] == name
        conn.close()
    assert name not in td.leftover_databases(MD_PREFIX)
