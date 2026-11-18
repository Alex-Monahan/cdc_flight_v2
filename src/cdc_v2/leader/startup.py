"""What the leader does once it may write (after its migrations), and the contenders' notices.

`on_leading(control_lane, ctx)` is one `control`-lane transaction, run once per run right after `md.migrate`:

1. **The take-over's row of the leader machine**, when the take-over could not write it (at a first run, and
   whenever a previous leader never migrated, `_cdc.transitions` did not exist yet).
2. **The alert `takeover`** (previous run, the age of its heartbeat, the reason, attempts, conflicts, interrupted
   connections, whether it was cancelled), unless the previous leader had released or nobody led before. The
   bootstrap cannot write it: `_cdc.alerts` belongs to the migrations, which only a leader runs, and an alert is
   written only through `alerts.emit` inside a lane transaction. So the take-over's facts travel to the pipeline
   (`RunContext.takeover`) and the new leader records them here. Its key is the epoch, so a repetition (an
   unknown commit outcome) records it once.
3. **The clean-up of `_cdc.connections`**: rows of epochs before the previous one (`registry.cleanup`). Once the
   take-over has committed and interrupted the connections of every earlier epoch, connections of epochs before
   the previous one can hold nothing that a later take-over must interrupt: a run of such an epoch can register
   no new connection (its fence row moved on). The rows of the previous epoch stay, for one more take-over.

`ContenderWatch` turns notices of contending Flights (`cdc_v2.leader.schema`) into the alert `contending_flight`,
one open alert per contending Flight, every `contender_check_s`.
"""
from __future__ import annotations

import logging
import random
import time

from cdc_v2 import alerts, crashpoints, md
from cdc_v2.deadline import Deadline
from cdc_v2.md import registry

from . import machine

log = logging.getLogger("cdc_v2.leader")

_ALERT_FIELDS = ("previous_epoch", "previous_flight_id", "previous_run_number", "previous_run_id",
                 "heartbeat_age_ms", "reason", "attempts", "conflicts", "interrupted", "interrupted_after",
                 "seconds", "previous_run_state")
_ALERT_COUNT_SQL = "SELECT count(*) FROM _cdc.alerts WHERE dedup_key = ?"
_OPEN_ALERT_COUNT_SQL = "SELECT count(*) FROM _cdc.alerts WHERE dedup_key = ? AND state = 'open'"


def takeover_alert_fields(ctx):
    """The fields of the alert `takeover`, or None when no alert is due (the previous leader released, or nobody
    led before). Identifiers, counts and durations only (SPEC 8.3)."""
    facts = ctx.takeover or {}
    if facts.get("previous_state") != "leading":
        return None
    out = {"epoch": ctx.epoch, "run_id": ctx.run_id}
    for name in _ALERT_FIELDS:
        v = facts.get(name)
        if isinstance(v, bool):
            v = int(v)
        if isinstance(v, (int, float)) or (isinstance(v, str) and len(v) <= 128 and v.isprintable()):
            out[name] = v
    out["cancelled"] = int(bool(facts.get("cancelled")))
    return out


def _takeover_row(ctx):
    facts = ctx.takeover or {}
    from_state = facts.get("previous_state")
    if from_state not in ("leading", "released"):
        from_state = "leading" if int(facts.get("previous_epoch") or 0) > 0 else "released"
    cause = "take-over (%s), row written by the new leader" % (facts.get("reason") or "unknown")
    detail = {"run_id": ctx.run_id, "flight_id": ctx.flight_id, "run_number": ctx.run_number}
    for name in ("previous_epoch", "previous_run_id", "previous_run_number", "heartbeat_age_ms"):
        if isinstance(facts.get(name), (int, str)) and not isinstance(facts.get(name), bool):
            detail[name] = facts[name]
    return from_state, cause, detail


def on_leading(control_lane, ctx, deadline=None):
    """Steps 1 to 3 of the module text, in one transaction. Returns what it did. `md.FenceLost` is passed on."""
    d = deadline or Deadline.after(60.0)
    fields = takeover_alert_fields(ctx)
    while True:
        done = {"row": False, "alert": False, "cleaned": 0}
        try:
            with control_lane.transaction(d.within(control_lane.conn.settings.transaction_deadline_s)) as tx:
                if int(tx.fetchone(machine.HAS_TAKEOVER_ROW_SQL, [ctx.epoch])[0]) == 0:
                    from_state, cause, detail = _takeover_row(ctx)
                    machine.record(tx, from_state, "leading", cause, detail)
                    done["row"] = True
                if fields is not None:
                    key = alerts.dedup_key("takeover", fields, ("epoch",))
                    if int(tx.fetchone(_ALERT_COUNT_SQL, [key])[0]) == 0:
                        alerts.emit(tx, "takeover", "warning", fields, key=("epoch",))
                        done["alert"] = True
                done["cleaned"] = registry.cleanup(tx, ctx.epoch) or 0
            crashpoints.crash_point("leader.recorded")
            return done
        except (md.Conflict, md.ConnectionLost):
            d.sleep(0.05 + 0.2 * random.random(), "the leader's first transaction")
        except md.CommitUnknown:
            # read again in a new transaction: the checks above make a repetition record nothing twice
            d.sleep(ctx.settings.late_commit_wait_s, "the leader's first transaction (outcome unknown)")


class ContenderWatch:
    """Every `contender_check_s`: notices of the last two periods, by contending Flight; an alert
    `contending_flight` for each Flight that has no open one. Notices older than a day are removed."""

    WINDOW_SQL = ("SELECT flight_id, count(*), max(run_number) FROM _cdc.contenders WHERE noticed_at > "
                  "uuid_extract_timestamp(uuidv7()) - to_microseconds(CAST(? AS BIGINT)) GROUP BY flight_id "
                  "ORDER BY flight_id")
    PRUNE_SQL = ("DELETE FROM _cdc.contenders WHERE noticed_at < uuid_extract_timestamp(uuidv7()) - "
                 "INTERVAL 1 DAY")

    def __init__(self, ctx):
        self.ctx = ctx
        self.next_at = 0.0
        self.alerts = 0

    def due(self):
        return time.monotonic() >= self.next_at

    def run(self, control_lane, deadline=None):
        """One look. Returns the number of alerts it wrote. `md.FenceLost` is passed on."""
        s = self.ctx.settings
        self.next_at = time.monotonic() + s.contender_check_s
        d = deadline or Deadline.after(30.0)
        window_us = int(2 * s.contender_check_s * 1e6)
        while True:
            written = 0
            try:
                with control_lane.transaction(d) as tx:
                    for flight_id, count, run_number in tx.fetchall(self.WINDOW_SQL, [window_us]):
                        fields = {"contending_flight_id": str(flight_id), "notices": int(count),
                                  "last_run_number": int(run_number or 0), "epoch": self.ctx.epoch}
                        key = alerts.dedup_key("contending_flight", fields, ("contending_flight_id",))
                        if int(tx.fetchone(_OPEN_ALERT_COUNT_SQL, [key])[0]) == 0:
                            alerts.emit(tx, "contending_flight", "critical", fields, key=("contending_flight_id",))
                            written += 1
                    tx.execute(self.PRUNE_SQL)
                self.alerts += written
                return written
            except (md.Conflict, md.ConnectionLost):
                d.sleep(0.05 + 0.2 * random.random(), "the contenders' notices")
            except md.CommitUnknown:
                d.sleep(s.late_commit_wait_s, "the contenders' notices (outcome unknown)")


__all__ = ["on_leading", "ContenderWatch", "takeover_alert_fields"]
