"""A stand-in pipeline for the leadership tests (task L, section 3): the real bootstrap starts it in place of
`python -m cdc_v2.pipeline` (`CDC_V2_PIPELINE=script:tests/leader/toy_pipeline.py`).

    python tests/leader/toy_pipeline.py <config.json>       # environment: contract C12, from the bootstrap

What it does, with the leader package as the real pipeline will use it:

1. `leader.RunContext.from_env()`; a stand-in supervisor (`leader_standin.py`); `leader.Heartbeat` (ticks to the
   bootstrap after each beat);
2. `md.migrate` on the `control` lane (its own list `toyleader`: `toyl.position`, `toyl.data`, `toyl.writer_log`),
   then `leader.on_leading` (the take-over's row, the alert `takeover`, the clean-up of `_cdc.connections`);
3. a loop: events from the source file (JSON lines, appended by the test) are applied a few per transaction on the
   `stream` lane: the rows go into `toyl.data`, the counter and position of `toyl.position` move with a
   condition on what was read (the run-again rule), and one row of `toyl.writer_log` records epoch, run id,
   positions and the order of commit (`seq`: the stream fence row's counter after the commit). That row is what
   a check reads for "never two writers" without relying on the fence;
4. every `contender_check_s` the contenders' notices become alerts (`leader.ContenderWatch`);
5. at `run.max_runtime` the planned hand-over (`leader.hand_over`): release, exit 0.

Commands (a JSON file the test rewrites; read at every turn of the loop): ``stall`` (stop making progress and
stop applying, while the process lives: a hung component), ``ignore_fence_lost`` (catch `FenceLost` and go on
trying: shows that the fence, not good behaviour, keeps an old run out), ``batch``. A commit that hangs is made
by the harness's fault site `md.commit` (action ``block``). The lane crash points are package D's.

Exit status (C12): 0 after a committed release, 10 when superseded, anything else a failure. It prints one
``TOY {json}`` line per event; no secret, no host, no path.
"""
import json
import os
import random
import sys
import threading
import time

from cdc_v2 import leader, md
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.md import schema as md_schema

from leader_standin import StandInSupervisor

STEPS = (
    md_schema.Step("toy_tables", (
        "CREATE SCHEMA IF NOT EXISTS toyl",
        "CREATE TABLE toyl.position (pos UBIGINT, n BIGINT)",
        "INSERT INTO toyl.position VALUES (0, 0)",
        "CREATE TABLE toyl.data (seq UBIGINT, payload VARCHAR, epoch BIGINT, run_id VARCHAR)",
        "CREATE TABLE toyl.writer_log (epoch BIGINT, run_id VARCHAR, from_pos UBIGINT, to_pos UBIGINT, seq UBIGINT, "
        "written_at TIMESTAMPTZ)",
    )),
)
md_schema.register_migrations("toyleader", STEPS)

READ_POS_SQL = "SELECT pos FROM toyl.position"
MOVE_SQL = "UPDATE toyl.position SET pos = ?, n = n + ? WHERE pos = ?"
INSERT_SQL = "INSERT INTO toyl.data VALUES (?, ?, ?, ?)"
LOG_SQL = ("INSERT INTO toyl.writer_log VALUES (?, ?, ?, ?, (SELECT CAST(n + 1 AS UBIGINT) FROM _cdc.fence WHERE "
           "lane = 'stream'), " + md.clock_sql + ")")

_out = threading.Lock()
T0 = time.monotonic()


def say(event, **fields):
    fields["event"] = event
    fields["t_s"] = round(time.monotonic() - T0, 3)
    with _out:
        sys.stdout.write("TOY " + json.dumps(fields, sort_keys=True, default=str) + "\n")
        sys.stdout.flush()


class Source:
    """The events: JSON lines ``{"seq": n, "payload": ...}``, n = 1, 2, 3, ..."""

    def __init__(self, path):
        self.path, self.offset, self.events = path, 0, []

    def poll(self):
        try:
            with open(self.path, "rb") as f:
                f.seek(self.offset)
                data = f.read()
        except FileNotFoundError:
            return self
        end = data.rfind(b"\n") + 1
        for line in data[:end].splitlines():
            e = json.loads(line)
            self.events.append(e["payload"])
        self.offset += end
        return self


class Commands:
    def __init__(self, path):
        self.path, self.stamp, self.cmds = path, None, {}

    def read(self):
        if not self.path:
            return self.cmds
        try:
            st = os.stat(self.path)
            stamp = (st.st_mtime_ns, st.st_size, st.st_ino)
        except FileNotFoundError:
            self.cmds = {}
            return self.cmds
        if stamp != self.stamp:
            try:
                with open(self.path, encoding="utf-8") as f:
                    self.cmds = json.load(f)
                self.stamp = stamp
            except (OSError, ValueError):
                pass
        return self.cmds


def spawn_daemon():
    """A grandchild in a session of its own whose parent exits at once (the test of LocalPlatform's kill)."""
    pid = os.fork()
    if pid == 0:
        try:
            if os.fork() == 0:
                os.setsid()
                os.execv(sys.executable, [sys.executable, "-c", "import time; time.sleep(600)"])
        finally:
            os._exit(0)
    os.waitpid(pid, 0)


class Toy:
    def __init__(self, cfg):
        self.cfg = cfg
        self.ctx = leader.RunContext.from_env()
        self.commands = Commands(cfg.get("control"))
        self.source = Source(cfg["source"])
        self.sup = StandInSupervisor(self.ctx.settings, deadline_s=float(cfg.get("progress_deadline_s", 3.0)))
        self.ignored = 0
        self.commits = 0

    def lost(self, reason):
        if self.commands.read().get("ignore_fence_lost"):
            self.ignored += 1
            say("fence_lost_ignored", where=reason, epoch=self.ctx.epoch)
            return
        say("fence_lost", where=reason, epoch=self.ctx.epoch)
        leader.on_fence_lost(reason)

    def run(self):
        ctx, cfg = self.ctx, self.cfg
        say("start", epoch=ctx.epoch, run_id=ctx.run_id, run_number=ctx.run_number,
            takeover_reason=(ctx.takeover or {}).get("reason"))
        if cfg.get("spawn_daemon"):
            spawn_daemon()
            say("daemon_spawned")
        md.configure(md.Settings(database=cfg["database"], **cfg.get("md", {})))
        self.sup.report("toy", "starting")
        self.hb = leader.Heartbeat(ctx, self.sup, on_lost=self.lost).start()
        control = md.Lane(md.connect("control"), "control", ctx.epoch)
        self.control = control
        d = Deadline.after(float(cfg.get("startup_s", 60.0)))
        with self.sup.waiting("toy", "migrations", d):
            while True:
                try:
                    md.migrate(control, d)
                    rec = leader.on_leading(control, ctx, d)
                    break
                except md.FenceLost:
                    self.lost("migrations")
                    time.sleep(0.2)
                except (md.Conflict, md.ConnectionLost, md.CommitUnknown, DeadlineExceeded):
                    time.sleep(0.1)
        say("leading", epoch=ctx.epoch, **rec)
        self.sup.report("toy", "streaming")
        stream = md.Lane(md.connect("stream"), "stream", ctx.epoch)
        clock = leader.HandoverClock(ctx)
        contenders = leader.ContenderWatch(ctx)
        while True:
            cmds = self.commands.read()
            if cmds.get("stall"):
                time.sleep(0.05)
                continue
            if clock.due():
                self.hand_over(stream)
            if cfg.get("contenders", True) and contenders.due():
                try:
                    n = contenders.run(control, Deadline.after(10.0))
                    if n:
                        say("contending_alerts", count=n)
                except md.FenceLost:
                    self.lost("contenders")
                except (md.ConnectionLost, DeadlineExceeded, md.StatementError):
                    pass
            self.apply_some(stream, int(cmds.get("batch") or cfg.get("batch", 3)))
            self.sup.report("toy")
            time.sleep(float(cfg.get("pace_s", 0.05)))

    def apply_some(self, stream, batch):
        ctx = self.ctx
        events = self.source.poll().events
        wait = Deadline.after(float(self.cfg.get("commit_wait_s", 3.0)))
        try:
            with self.sup.waiting("toy", "commit", wait):
                with stream.transaction(Deadline.after(float(self.cfg.get("transaction_s", 20.0)))) as tx:
                    pos = int(tx.fetchone(READ_POS_SQL)[0])
                    new = events[pos:pos + batch]
                    if not new:
                        return
                    for i, payload in enumerate(new, start=pos + 1):
                        tx.execute(INSERT_SQL, [i, payload, ctx.epoch, ctx.run_id])
                    tx.execute_one(MOVE_SQL, [pos + len(new), len(new), pos])
                    tx.execute(LOG_SQL, [ctx.epoch, ctx.run_id, pos, pos + len(new)])
            self.commits += 1
            say("committed", epoch=ctx.epoch, from_pos=pos, to_pos=pos + len(new))
        except md.FenceLost:
            self.lost("stream lane")
            time.sleep(0.1)
        except md.CommitUnknown:
            say("commit_unknown", epoch=ctx.epoch)
            time.sleep(ctx.settings.late_commit_wait_s)
        except (md.Conflict, md.ConnectionLost, DeadlineExceeded):
            time.sleep(0.05 + 0.1 * random.random())

    def hand_over(self, stream):
        ctx = self.ctx

        class _Stream:
            def finish_current_group(self, deadline):
                return None                     # the loop runs one group at a time: nothing is in flight here

        say("hand_over", epoch=ctx.epoch, run_age_s=round(ctx.run_age_s(), 3))
        try:
            code = leader.hand_over(ctx, self.control, stream=_Stream(), heartbeat=self.hb)
        except md.FenceLost:
            self.lost("release")
            code = leader.EXIT_SUPERSEDED
        except (DeadlineExceeded, md.DestinationError) as e:
            say("release_failed", error=type(e).__name__)
            code = leader.EXIT_FAILED
        say("exit", code=code, commits=self.commits)
        sys.stdout.flush()
        os._exit(code)


def main(argv):
    with open(argv[1], encoding="utf-8") as f:
        cfg = json.load(f)
    try:
        Toy(cfg).run()
    except md.LaneMisuse:
        raise
    except BaseException as e:                     # noqa: BLE001 - a failure status, never 0 or 10
        say("failed", error=type(e).__name__)
        sys.stdout.flush()
        os._exit(1)


if __name__ == "__main__":
    main(sys.argv)
