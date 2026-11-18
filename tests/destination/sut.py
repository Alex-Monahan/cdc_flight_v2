"""A small system under test for the destination layer (package D): it moves a position and data together through
lanes, and nothing else.

    python tests/destination/sut.py --database NAME --source FILE --tag TAG [options]

One process is one run of a pipeline in miniature:

1. connect, create the leader tables if they are missing (`md.ensure_leader_tables`);
2. read the positions outside any transaction (crash point `sut.read`);
3. take over as design 4.5 says (`cdc_v2.testing.destination.take_over`), then interrupt the connections that
   `_cdc.connections` lists for earlier epochs once more (`md.interrupt_earlier`);
4. run the migrations in the `control` lane, among them this file's own package `sut` (two tables);
5. one thread per lane: read the events of the lane from the source file (JSON lines, appended by the test) and
   apply them, a few per transaction: insert the rows into `sut.data` and move `sut.position`.

Every transaction follows the **run-again rule** of contract C4: it reads its starting state (the position) inside
the transaction, and it moves the position with a condition on the value it read (`execute_one`). ``--naive``
switches the rule off (the position is the one this process read or wrote last, the update has no condition); it
exists to show that the tests can see the difference.

What the errors of the layer mean here: `FenceLost` ends the process (exit code 3); after `CommitUnknown` the lane
waits until a late commit must have landed and reads again; after `Conflict`, `ConnectionLost` and
`DeadlineExceeded` it tries again.

The process prints one JSON line per event to its standard output. It prints no secret, no host and no path.
The file also holds `check` (compare the destination with the source file) and `SutAdapter` (the crash runner's
adapter for MotherDuck).
"""
import argparse
import json
import os
import random
import sys
import threading
import time
import uuid

import duckdb

from cdc_v2 import crashpoints, md
from cdc_v2.deadline import Deadline
from cdc_v2.md import errors as md_errors
from cdc_v2.md import registry
from cdc_v2.testing import crash, procs
from cdc_v2.testing import destination as td

MARKER = "cdc_v2_t_d_sut"
LANES = ("stream", "control", "backfill.0")
EXIT_OK, EXIT_FENCE_LOST, EXIT_NO_TAKEOVER, EXIT_ERROR = 0, 3, 4, 5
HERE = os.path.dirname(os.path.abspath(__file__))

P_READ = crashpoints.declare("sut.read", "SUT: positions read outside any transaction, before the take-over")
P_BEGUN = crashpoints.declare("sut.begun", "SUT: transaction open, its starting position known")
P_WRITTEN = crashpoints.declare("sut.written", "SUT: data rows written, position not moved yet")
P_MOVED = crashpoints.declare("sut.moved", "SUT: position moved, transaction block not left yet")

STEPS = (
    md.Step("tables", (
        "CREATE SCHEMA IF NOT EXISTS sut",
        "CREATE TABLE sut.position (lane VARCHAR, pos UBIGINT, n BIGINT)",
        "INSERT INTO sut.position SELECT lane, 0, 0 FROM _cdc.fence",
        "CREATE TABLE sut.data (lane VARCHAR, seq UBIGINT, payload VARCHAR, epoch BIGINT, tag VARCHAR, "
        "applied_at TIMESTAMPTZ)",
    )),
)

READ_POSITIONS_SQL = "SELECT lane, pos FROM sut.position"
READ_POSITION_SQL = "SELECT pos FROM sut.position WHERE lane = ?"
MOVE_SQL = "UPDATE sut.position SET pos = ?, n = n + ? WHERE lane = ? AND pos = ?"
NAIVE_MOVE_SQL = "UPDATE sut.position SET pos = ?, n = n + ? WHERE lane = ?"
INSERT_ROW = "(?, ?, ?, ?, ?, " + md.clock_sql + ")"


# ---------------------------------------------------------------------------------------------------- the source
class Source:
    """The source file: JSON lines ``{"lane": ..., "seq": n, "payload": ...}``; per lane seq counts 1, 2, 3, ..."""

    def __init__(self, path):
        self.path = path
        self.offset = 0
        self.events = {}

    def poll(self):
        try:
            with open(self.path, "rb") as f:
                f.seek(self.offset)
                data = f.read()
        except FileNotFoundError:
            return self
        end = data.rfind(b"\n") + 1                  # a line that is still being written is read next time
        for line in data[:end].splitlines():
            e = json.loads(line)
            self.events.setdefault(e["lane"], []).append((int(e["seq"]), e["payload"]))
        self.offset += end
        return self

    def count(self, lane):
        return len(self.events.get(lane, ()))


class SourceWriter:
    """What a test uses to append events."""

    def __init__(self, path, lanes=LANES):
        self.path = path
        self.lanes = tuple(lanes)
        self.counts = {lane: 0 for lane in self.lanes}
        self._lock = threading.Lock()
        open(path, "ab").close()

    def add(self, lane, n=1, mark="e"):
        with self._lock:
            lines = []
            for _ in range(n):
                self.counts[lane] += 1
                seq = self.counts[lane]
                lines.append(json.dumps({"lane": lane, "seq": seq, "payload": "%s-%s-%d" % (mark, lane, seq)}))
            with open(self.path, "ab") as f:
                f.write(("\n".join(lines) + "\n").encode())
        return self.counts[lane]

    def add_all(self, n=1, mark="e"):
        for lane in self.lanes:
            self.add(lane, n, mark)


# ---------------------------------------------------------------------------------------------------- the check
def read_state(raw, lanes=LANES):
    """Positions and data of the destination: ``{lane: {"pos", "n", "rows": [(seq, payload, epoch, tag)]}}``.
    Read in one transaction, so that a commit that lands between two statements cannot tear the picture."""
    out = {}
    raw.execute("BEGIN")
    try:
        pos = {r[0]: (int(r[1]), int(r[2])) for r in raw.execute("SELECT lane, pos, n FROM sut.position").fetchall()}
        for lane in lanes:
            rows = raw.execute("SELECT seq, payload, epoch, tag FROM sut.data WHERE lane = ? ORDER BY seq, epoch, tag",
                               [lane]).fetchall()
            out[lane] = {"pos": pos[lane][0], "n": pos[lane][1],
                         "rows": [(int(r[0]), r[1], int(r[2]), r[3]) for r in rows]}
    finally:
        raw.execute("ROLLBACK")
    return out


def check(raw, source_path, lanes=LANES):
    """Compare the destination with the source file. For every lane: the data rows are exactly the events 1 to
    `pos`, each once, with the payload of the source; the counter `n` equals `pos`; `pos` is not beyond the source.
    Returns ``{"ok", "caught_up", "lanes": {lane: counts}, "problems": [...]}``."""
    src = Source(source_path).poll()
    state = read_state(raw, lanes)
    out = {"ok": True, "caught_up": True, "lanes": {}, "problems": []}
    for lane in lanes:
        s, events = state[lane], src.events.get(lane, [])
        seqs = [r[0] for r in s["rows"]]
        doubles = sorted({q for q in seqs if seqs.count(q) > 1})
        want = events[:s["pos"]]
        got = [(r[0], r[1]) for r in s["rows"]]
        problems = []
        if doubles:
            problems.append("%d events applied more than once (first %r)" % (len(doubles), doubles[:3]))
        if s["pos"] > len(events):
            problems.append("position %d is beyond the source (%d events)" % (s["pos"], len(events)))
        if s["n"] != s["pos"]:
            problems.append("counter %d differs from position %d" % (s["n"], s["pos"]))
        if not doubles and got != want:
            problems.append("data rows differ from the events 1 to %d (%d rows)" % (s["pos"], len(got)))
        out["lanes"][lane] = {"source": len(events), "pos": s["pos"], "n": s["n"], "rows": len(got),
                              "doubles": len(doubles)}
        out["problems"] += ["%s: %s" % (lane, p) for p in problems]
        if s["pos"] != len(events):
            out["caught_up"] = False
    out["ok"] = not out["problems"]
    if not out["ok"]:
        out["reason"] = "; ".join(out["problems"])[:400]
    return out


# ---------------------------------------------------------------------------------------------------- the run
class Run:
    def __init__(self, args):
        self.args = args
        self.t0 = time.monotonic()
        self._print = threading.Lock()
        self.stop = threading.Event()
        self.code = EXIT_OK
        self.source = Source(args.source)
        self._src_lock = threading.Lock()
        self.known = {}

    def say(self, **fields):
        fields["ms"] = int((time.monotonic() - self.t0) * 1000)
        fields["tag"] = self.args.tag
        with self._print:
            sys.stdout.write(json.dumps(fields, sort_keys=True) + "\n")
            sys.stdout.flush()

    def end(self, code, **fields):
        if code != EXIT_OK and self.code == EXIT_OK:
            self.code = code
        self.stop.set()
        if fields:
            self.say(**fields)

    def events(self, lane):
        with self._src_lock:
            self.source.poll()
            return list(self.source.events.get(lane, ()))

    # -------------------------------------------------------------------------------------------- one transaction
    def apply(self, lane, events):
        """One transaction of one lane; returns the position after it."""
        a, name = self.args, lane.name
        with lane.transaction(Deadline.after(a.tx_deadline)) as tx:
            if a.naive:
                pos = self.known[name]                       # NOT the rule: a value read before this transaction
            else:
                pos = int(tx.fetchone(READ_POSITION_SQL, [name])[0])
            crashpoints.crash_point(P_BEGUN)
            batch = [e for e in events if pos < e[0] <= pos + a.batch]
            new = pos
            if batch:
                params = []
                for seq, payload in batch:
                    params += [name, seq, payload, lane.epoch, a.tag]
                tx.execute("INSERT INTO sut.data (lane, seq, payload, epoch, tag, applied_at) VALUES "
                           + ", ".join([INSERT_ROW] * len(batch)), params)
                crashpoints.crash_point(P_WRITTEN)
                new = batch[-1][0]
                if a.naive:
                    tx.execute(NAIVE_MOVE_SQL, [new, len(batch), name])
                else:
                    tx.execute_one(MOVE_SQL, [new, len(batch), name, pos])
                if a.hold_ms:                                # a slow, live transaction that holds its rows
                    time.sleep(a.hold_ms / 1000.0)
                crashpoints.crash_point(P_MOVED)
            self.say(ev="intend", lane=name, frm=pos, to=new)
        self.say(ev="done", lane=name, frm=pos, to=new)
        return new

    def lane_loop(self, lane):
        a, name = self.args, lane.name
        done, idle_since = 0, time.monotonic()
        rng = random.Random(hash((a.tag, name)) & 0xFFFF)
        while not self.stop.is_set():
            events = self.events(name)
            known = self.known.get(name)
            if known is not None and (not events or events[-1][0] <= known):
                if a.exit_idle and time.monotonic() - idle_since > a.exit_idle:
                    return
                time.sleep(a.idle)
                continue
            try:
                new = self.apply(lane, events)
            except md.FenceLost:
                return self.end(EXIT_FENCE_LOST, ev="fence_lost", lane=name)
            except md.CommitUnknown:
                self.say(ev="error", kind="CommitUnknown", lane=name)
                if not a.naive:
                    self.known[name] = None                  # read again, inside the next transaction
                self.stop.wait(td.LATE_COMMIT_WAIT_S)
                continue
            except (md.Conflict, md.ConnectionLost, md.DeadlineExceeded) as e:
                self.say(ev="error", kind=type(e).__name__, lane=name)
                if not a.naive:
                    self.known[name] = None
                self.stop.wait(0.05 + 0.2 * rng.random())
                continue
            if new != known:
                done += 1
                idle_since = time.monotonic()
            self.known[name] = new
            if a.max_tx and done >= a.max_tx:
                return

    def lane_thread(self, lane):
        try:
            self.lane_loop(lane)
        except crashpoints.CrashPointHit as e:
            self.end(EXIT_ERROR, ev="crash_point_raised", point=e.point)
        except BaseException as e:                           # noqa: BLE001 - the process reports it and exits
            self.end(EXIT_ERROR, ev="fatal", lane=lane.name, error=md_errors.safe_text(e))

    # -------------------------------------------------------------------------------------------- the process
    def main(self):
        a = self.args
        settings = md.Settings(database=a.database, marker=MARKER, connect_deadline_s=20.0, connect_attempts=3,
                               statement_deadline_s=a.stmt_deadline, transaction_deadline_s=a.tx_deadline,
                               rollback_deadline_s=5.0, grace_s=a.grace, interrupt_deadline_s=5.0)
        self.say(ev="start")
        init = md.connect("init", settings)
        retries = md.ensure_leader_tables(init)
        self.say(ev="leader_tables", retries=retries)
        if a.init_only:
            init.close()
            return EXIT_OK
        try:
            self.known = {r[0]: int(r[1]) for r in init.fetchall(READ_POSITIONS_SQL)}
        except md.StatementError as e:
            if e.error_class != "CatalogException":       # the table does not exist yet; anything else is real
                raise
            self.known = {}
        self.say(ev="read", known={k: self.known[k] for k in a.lanes if k in self.known})
        crashpoints.crash_point(P_READ)
        if a.epoch:
            epoch = a.epoch
        else:
            rep = {}
            try:
                epoch = td.take_over(settings, run_id="%s-%s" % (a.tag, uuid.uuid4().hex[:8]),
                                     timeout_s=a.takeover_timeout, interrupt=not a.no_interrupt, report=rep)
            except td.TakeoverFailed:
                self.say(ev="no_takeover", **{k: rep.get(k) for k in ("attempts", "conflicts", "interrupted", "seconds")})
                return EXIT_NO_TAKEOVER
            self.say(ev="leader", epoch=epoch, **{k: rep.get(k) for k in ("attempts", "conflicts", "listed",
                                                                         "interrupted", "unknown", "seconds")})
            if not a.no_interrupt:
                listed, done = md.interrupt_earlier(init, epoch)
                self.say(ev="interrupt_earlier", listed=listed, interrupted=done)
        lanes = {name: md.Lane(md.connect(name, settings), name, epoch) for name in a.lanes}
        control = lanes.get("control") or md.Lane(md.connect("control", settings), "control", epoch)
        md.register_migrations("sut", STEPS)
        applied = md.migrate(control)
        self.say(ev="migrated", steps=len(applied))
        if not a.epoch:
            registry.cleanup(init, epoch)
        if a.naive:
            for name in a.lanes:
                self.known.setdefault(name, 0)
        else:
            self.known = {k: v for k, v in self.known.items() if k in a.lanes}
        threads = [threading.Thread(target=self.lane_thread, args=(lanes[name],), name="sut-" + name)
                   for name in a.lanes]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return self.code


def parse(argv=None):
    ap = argparse.ArgumentParser(prog="sut.py", description="System under test for the destination layer.")
    ap.add_argument("--database", required=True, help="a MotherDuck database name, or a local DuckDB file")
    ap.add_argument("--source", required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--lanes", default=",".join(LANES), type=lambda s: tuple(x for x in s.split(",") if x))
    ap.add_argument("--batch", type=int, default=3, help="most events per transaction")
    ap.add_argument("--epoch", type=int, default=0, help="do not take over; work with this epoch")
    ap.add_argument("--naive", action="store_true", help="do not follow the run-again rule")
    ap.add_argument("--no-interrupt", action="store_true", help="never interrupt connections of earlier epochs")
    ap.add_argument("--init-only", action="store_true", help="create the leader tables and exit")
    ap.add_argument("--max-tx", type=int, default=0, help="per lane: stop after this many transactions with data")
    ap.add_argument("--exit-idle", type=float, default=0.0, help="per lane: stop after this many idle seconds")
    ap.add_argument("--hold-ms", type=int, default=0, help="keep every transaction open this long after it moved the position")
    ap.add_argument("--idle", type=float, default=0.05)
    ap.add_argument("--stmt-deadline", type=float, default=15.0)
    ap.add_argument("--tx-deadline", type=float, default=30.0)
    ap.add_argument("--grace", type=float, default=2.0)
    ap.add_argument("--takeover-timeout", type=float, default=60.0)
    return ap.parse_args(argv)


def main(argv=None):
    run = Run(parse(argv))
    try:
        code = run.main()
    except md.FenceLost:
        run.say(ev="fence_lost", lane=None)
        code = EXIT_FENCE_LOST
    except crashpoints.CrashPointHit as e:
        run.say(ev="crash_point_raised", point=e.point)
        code = EXIT_ERROR
    except Exception as e:                                   # noqa: BLE001 - reported, then the process ends
        run.say(ev="fatal", error=md_errors.safe_text(e))
        code = EXIT_ERROR
    run.say(ev="exit", code=code)
    sys.stdout.flush()
    os._exit(code)                                           # no waiting for connections that hang


# ---------------------------------------------------------------------------------------------------- for tests
def start(database, source, tag, env=None, log_path=None, **options):
    """Start one SUT process. ``options`` are the command line options with underscores (``max_tx=2``,
    ``naive=True``)."""
    argv = [sys.executable, os.path.join(HERE, "sut.py"), "--database", database, "--source", source, "--tag", tag]
    for key, value in options.items():
        flag = "--" + key.replace("_", "-")
        if value is True:
            argv.append(flag)
        elif value not in (False, None):
            argv += [flag, ",".join(value) if isinstance(value, (tuple, list)) else str(value)]
    return procs.start(argv, env=env, log_path=log_path, name="sut-" + tag)


def log_events(proc_or_path, ev=None):
    """The JSON lines a SUT process printed (optionally of one kind)."""
    path = proc_or_path if isinstance(proc_or_path, str) else proc_or_path.log_path
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.startswith("{") and line.endswith("\n"):
                    try:
                        e = json.loads(line)
                    except ValueError:
                        continue
                    if ev is None or e.get("ev") == ev:
                        out.append(e)
    except FileNotFoundError:
        pass
    return out


class SutAdapter(crash.Adapter):
    """The crash runner's adapter: one MotherDuck database ``cdc_v2_t_d_<random>`` per scenario, dropped at the end.

    ``created`` collects every database name the adapters of one test made, so that the test can drop them once
    more at its end, whatever happened."""

    name = "destination-sut"
    prefix = "cdc_v2_t_d_"
    created = []

    def __init__(self):
        self.database = None
        self.writer = None
        self.raw = None
        self.dir = None
        self.n = 0

    def crash_points(self):
        return [p for lane in LANES for p in md.crash_points(lane)]

    def setup(self, workdir, seed):
        self.dir = workdir
        self.database = "%s%s" % (self.prefix, uuid.uuid4().hex[:12])
        type(self).created.append(self.database)
        td.create_database(self.database)
        self.writer = SourceWriter(os.path.join(workdir, "source.jsonl"))

    def start(self, tag, env):
        self.n += 1
        return start(self.database, self.writer.path, tag, env=env,
                     log_path=os.path.join(self.dir, "sut-%s-%d.log" % (tag, self.n)))

    def workload(self, step, seed):
        rng = random.Random(seed * 1_000_003 + step)
        for lane in LANES:
            if rng.random() < 0.7:
                self.writer.add(lane, rng.choice((1, 1, 2)), mark="s%d" % seed)

    def _raw(self):
        if self.raw is None:
            self.raw = td.raw_connect(md.Settings(database=self.database, marker=MARKER))
        return self.raw

    def wait_caught_up(self, timeout):
        end = time.monotonic() + timeout
        last = None
        while True:
            try:
                rows = dict(self._raw().execute(READ_POSITIONS_SQL).fetchall())
                last = {lane: int(rows[lane]) for lane in LANES}
                if all(last[lane] == self.writer.counts[lane] for lane in LANES):
                    return
            except duckdb.CatalogException:
                last = "no tables yet"
            if time.monotonic() >= end:
                raise procs.DeadlineExceeded("not caught up after %.0f s: positions %r, source %r"
                                             % (timeout, last, self.writer.counts))
            time.sleep(0.25)

    def check(self):
        return check(self._raw(), self.writer.path)

    def teardown(self):
        if self.raw is not None:
            try:
                self.raw.close()
            except duckdb.Error:
                pass
            self.raw = None
        if self.database is not None:
            td.drop_database(self.database)


if __name__ == "__main__":
    main()
