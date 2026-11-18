"""The attach step (design section 8; contract C10; plan 6.2 "V and S") — the stream's minimal version.

    attached = attach(ctx, deadline)        # -> Attached(engine, facts, finding)

The stream component starts its engine only through this function. Package V replaces the body (the decision table
of design 8, the enactment of a finding), not the callers. This minimal version:

1. reads the fence row of lane `stream` (a deposed run starts no engine: `FenceLost`);
2. reads the slot facts; a missing or invalid slot, or a slot whose `confirmed_flush_lsn` is above the applied
   position, is a **finding**: it is returned without an engine (the stream then stops with an alert; V enacts it);
3. terminates a holder of our slot name (`slot.terminate_holder`, our slot only) and reads the facts again;
4. starts the engine at the applied position and waits for `HELLO` (Debezium's heartbeat brings it within
   `heartbeat.interval.ms`, also while the server decodes a large transaction again);
5. reads the facts again: our engine's backend (`HELLO`'s pid and backend start) must hold the slot, and the
   position the decision uses must be the one of step 2; otherwise the engine is killed and the loop starts again.

The whole loop is one declared wait with the caller's deadline (at least 75 s after a take-over with the production
leader timings: the old run can take the slot back until its hard exit). No frame is applied and no `DURABLE` is
sent before the decision.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Optional

from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.md import errors as md_errors
from cdc_v2.stream import frames as F

log = logging.getLogger("cdc_v2.stream")

FENCE_ROW_SQL = "SELECT epoch FROM _cdc.fence WHERE lane = 'stream'"


@dataclass
class Attached:
    engine: object                  # the engine that holds the slot (None with a finding)
    facts: object                   # the slot facts of the decision (step 2), or None
    finding: Optional[str]          # None: continuous; else a short identifier (slot_missing, slot_invalid, slot_ahead)
    hello: object = None
    confirmed: Optional[int] = None


@dataclass
class AttachContext:
    lane: object                    # the stream lane (md.Lane)
    epoch: int
    slot: str
    applied: int                    # the applied position the engine starts at
    source: object                  # slot facts and holder termination (stream.source.PgSource or a stand-in)
    start_engine: Callable[[int], object]      # start_position -> engine
    wait: Callable[[float, str], None]         # a declared wait of the supervisor


def _fence_row(ctx: AttachContext, deadline: Deadline):
    row = ctx.lane.conn.fetchone(FENCE_ROW_SQL, None, deadline.within(30.0))
    if row is None or int(row[0]) != ctx.epoch:
        raise md_errors.FenceLost("lane stream: epoch %d no longer leads (fence row before the engine starts)"
                                  % ctx.epoch)


def _finding(facts, applied) -> Optional[str]:
    if facts is None:
        return "slot_missing"
    if facts.invalid:
        return "slot_invalid"
    if facts.confirmed_flush_lsn is None or facts.confirmed_flush_lsn > applied:
        return "slot_ahead"
    return None


def attach(ctx: AttachContext, deadline: Deadline) -> Attached:
    tries = 0
    while True:
        tries += 1
        deadline.check("attach: the engine did not take the slot")
        _fence_row(ctx, deadline)
        facts = ctx.source.slot_facts(deadline.within(30.0))
        why = _finding(facts, ctx.applied)
        if why:
            return Attached(None, facts, why)
        if facts.active:
            ctx.source.terminate_holder(deadline.within(30.0), expect=None)
            facts = ctx.source.slot_facts(deadline.within(30.0))
            why = _finding(facts, ctx.applied)
            if why:
                return Attached(None, facts, why)
            if facts.active:
                ctx.wait(0.5, "attach: the previous holder still holds the slot")
                continue
        engine = ctx.start_engine(ctx.applied)
        try:
            hello = _hello(engine, deadline)
            after = ctx.source.slot_facts(deadline.within(30.0))
        except Exception:
            engine.kill()
            raise
        if hello is None:
            engine.kill()
            ctx.wait(min(2.0, 0.25 * tries), "attach: the engine ended before it held the slot")
            continue
        holder = (hello.active_pid, hello.backend_start_us)
        if (after is None or after.holder != holder or after.confirmed_flush_lsn != facts.confirmed_flush_lsn
                or after.invalid):
            log.warning("attach: the slot changed between the read and the engine's start; again",
                        extra={"cdc": {"slot": ctx.slot, "try": tries}})
            engine.kill()
            continue
        return Attached(engine, facts, None, hello, facts.confirmed_flush_lsn)


def _hello(engine, deadline: Deadline):
    """The engine's first frame is HELLO. A FATAL before it (the slot is still held by an old engine, the source
    refused) or an exit ends this try: None."""
    from cdc_v2.stream.engine import EngineFatal
    try:
        f = engine.next_frame(deadline)
    except (EngineFatal, F.EngineBroken):
        return None
    except DeadlineExceeded:
        raise
    if not isinstance(f, F.Hello):
        return None
    return f


__all__ = ["attach", "Attached", "AttachContext"]
