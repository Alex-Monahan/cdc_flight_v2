"""The planned hand-over (design 4.6; SPEC 4.2, D13).

At `run.max_runtime` (counted from the bootstrap's first line) the leader

1. reads MotherDuck's clock and plans the end about `release_before_boundary_s` (5 s) before a minute boundary
   (`schedule`), so that the next scheduled run, which starts after that boundary, finds the record released;
2. waits until `finish_budget_s` before that end, then asks the stream to finish the group in flight
   (`stream.finish_current_group(deadline)`, package S). A source transaction larger than a group that is still
   arriving is abandoned, not waited for: the slot sends it again to the next run;
3. releases: `md.schema.release(tx)` and the leader machine's row, in one fenced `control`-lane transaction
   (`release`);
4. stops the heartbeat and returns `EXIT_RELEASED` (0), the only success status of the pipeline (C12).

**A release whose commit outcome is unknown.** `release` waits `late_commit_wait_s`, reads the leader row in a new
read and decides: released at this epoch (the commit landed) is a release; leading at this epoch is tried again
(the conditional `WHERE epoch = ? AND state = 'leading'` makes a late first copy and the second one exclusive); a
higher epoch is `FenceLost`. If the destination cannot be read before the deadline, the pipeline ends with a
failure status (never 0) and the next run sees one of two records, both safe:

- `released`: the next scheduled run takes over without an alert and interrupts nothing (no open transaction of
  this run is left: the release was its last transaction);
- `leading` with a heartbeat that stops when this run ends: after the stale age the next run takes over with the
  alert `takeover` (reason `stale`); its fence makes every later write of this run fail anyway.
"""
from __future__ import annotations

import logging
import random
import time

from cdc_v2 import crashpoints, md
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.md import schema as md_schema

from . import machine, stop

log = logging.getLogger("cdc_v2.leader")


def schedule(now_us, minute_s, before_s, budget_s):
    """When to start the hand-over: ``(start_in_s, end_us, boundary_us)``. The run should end at ``end_us`` =
    the first minute boundary (MotherDuck's clock, microseconds) ``before_s`` ahead of which there is still
    ``budget_s`` from now, less ``before_s``; the work starts ``budget_s`` before that end."""
    minute = int(round(minute_s * 1e6))
    before, budget = int(round(before_s * 1e6)), int(round(budget_s * 1e6))
    boundary = (now_us // minute + 1) * minute
    while boundary - before - budget < now_us:
        boundary += minute
    end = boundary - before
    return (end - budget - now_us) / 1e6, end, boundary


class HandoverClock:
    """Says when `run.max_runtime` is reached, counted from the bootstrap's first line."""

    def __init__(self, ctx):
        self.ctx = ctx
        left = max(0.0, ctx.settings.max_runtime_s - ctx.run_elapsed_s)
        self.due_at = ctx.started_mono + left

    def remaining_s(self):
        return max(0.0, self.due_at - time.monotonic())

    def due(self):
        return time.monotonic() >= self.due_at


def release(control_lane, ctx, cause="planned hand-over", deadline=None):
    """Mark the record released in a fenced `control`-lane transaction, with the leader machine's row. Returns
    when the release is committed (also when a commit whose answer was lost is found to have landed). Raises
    `md.FenceLost` when another run leads, `DeadlineExceeded` when the destination did not let it decide in time."""
    s = ctx.settings
    d = deadline or Deadline.after(s.finish_budget_s + 30.0)
    detail = {"run_id": ctx.run_id, "run_number": ctx.run_number, "run_age_s": round(ctx.run_age_s(), 3)}
    while True:
        try:
            with control_lane.transaction(d.within(control_lane.conn.settings.transaction_deadline_s)) as tx:
                md_schema.release(tx)
                machine.record(tx, "leading", "released", cause, detail)
                crashpoints.crash_point("leader.release_ready")
            crashpoints.crash_point("leader.released")
            log.info("released", extra={"cdc": {"epoch": ctx.epoch, "run_id": ctx.run_id}})
            return "released"
        except (md.Conflict, md.ConnectionLost):
            d.sleep(0.05 + 0.2 * random.random(), "the release")
        except md.CommitUnknown:
            d.sleep(s.late_commit_wait_s, "the release (outcome unknown)")
            try:
                row = control_lane.conn.fetchone(machine.READ_LEADER_SQL, None, d.within(10.0))
            except (md.ConnectionLost, DeadlineExceeded, md.StatementError):
                continue
            epoch, state = int(row[0]), row[1]
            if epoch == ctx.epoch and state == "released":
                crashpoints.crash_point("leader.released")
                return "released"
            if epoch != ctx.epoch:
                raise md.FenceLost("the release: epoch %d no longer leads (the record holds %d)" % (ctx.epoch, epoch))


def hand_over(ctx, control_lane, stream=None, heartbeat=None, sleep=None):
    """Steps 1 to 4 of the module text. Returns `stop.EXIT_RELEASED` after a committed release. `FenceLost` is
    passed on (the caller reacts with `stop.on_fence_lost`). ``stream`` offers `finish_current_group(deadline)`
    (package S); ``heartbeat`` is the run's `Heartbeat`."""
    s = ctx.settings
    now_us = control_lane.conn.clock_us()
    start_in, end_us, boundary_us = schedule(now_us, s.minute_s, s.release_before_boundary_s, s.finish_budget_s)
    log.info("hand-over planned", extra={"cdc": {"epoch": ctx.epoch, "start_in_s": round(start_in, 3),
                                                 "ends_before_boundary_s": s.release_before_boundary_s}})
    (sleep or time.sleep)(start_in)
    budget = Deadline.after(s.finish_budget_s)
    if stream is not None:
        stream.finish_current_group(budget)
    release(control_lane, ctx, deadline=Deadline.after(max(1.0, budget.remaining()) + s.late_commit_wait_s + 10.0))
    if heartbeat is not None:
        heartbeat.stop()
    return stop.EXIT_RELEASED


__all__ = ["schedule", "HandoverClock", "release", "hand_over"]
