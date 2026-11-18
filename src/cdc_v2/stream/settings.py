"""Settings of the stream (contract C0: a small frozen dataclass per package).

`StreamSettings` holds the `stream.*` keys of the configuration file, `GroupSettings` the `apply.*` keys that decide
how a group is closed, committed and run again (design section 3; the statements themselves take package A's
`ApplySettings`). Durations are seconds, sizes are bytes; `docs/configuration.md` lists every key.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_SLOT = re.compile(r"^cdc_v2[a-z0-9_]{0,57}$")


def _positive(owner, name, value, allow_zero=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value != value:
        raise ValueError("%s.%s must be a number, got %r" % (owner, name, value))
    if value < 0 or (value == 0 and not allow_zero) or value == float("inf"):
        raise ValueError("%s.%s must be %s and finite, got %r" % (owner, name, ">= 0" if allow_zero else "> 0", value))


def _count(owner, name, value, minimum=1):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError("%s.%s must be an integer >= %d, got %r" % (owner, name, minimum, value))


@dataclass(frozen=True)
class GroupSettings:
    """`apply.*`: when a group closes, how long its commit may take, how often it is run again (design section 3).

    A group closes on the first of `max_group_delay_s` since its first record, `max_group_bytes`, a transaction that
    is larger than a group on its own (it is staged, design 6.4), a planned hand-over, or an idle position step.
    `max_group_attempts` bounds the runs of one group after `Conflict`, `ConnectionLost` or a deadline; the pauses
    between them grow from `retry_pause_s` to `retry_pause_max_s`, so that the attempts cover the 29 s for which
    MotherDuck keeps the rows of an abandoned transaction (destination section 8.3). `max_table_attempts` bounds
    the failures of one table before it leaves the stream (SPEC 4.0). The commit deadline is
    `commit_timeout_base_s + commit_timeout_per_million_rows_s * rows / 1e6` (design 3)."""
    max_group_delay_s: float = 1.0
    max_group_bytes: int = 128 << 20
    max_group_attempts: int = 12
    retry_pause_s: float = 0.5
    retry_pause_max_s: float = 5.0
    max_table_attempts: int = 3
    commit_timeout_base_s: float = 60.0
    commit_timeout_per_million_rows_s: float = 60.0
    stage_chunk_bytes: int = 64 << 20

    def __post_init__(self):
        for n in ("max_group_delay_s", "retry_pause_s", "retry_pause_max_s", "commit_timeout_base_s",
                  "commit_timeout_per_million_rows_s"):
            _positive("apply", n, getattr(self, n))
        for n in ("max_group_bytes", "stage_chunk_bytes"):
            _count("apply", n, getattr(self, n), 1 << 10)
        for n in ("max_group_attempts", "max_table_attempts"):
            _count("apply", n, getattr(self, n))
        if self.retry_pause_max_s < self.retry_pause_s:
            raise ValueError("apply.retry_pause_max_s must be >= apply.retry_pause_s")
        if self.stage_chunk_bytes > self.max_group_bytes:
            raise ValueError("apply.stage_chunk_bytes must be <= apply.max_group_bytes")

    def commit_deadline_s(self, rows: int) -> float:
        return self.commit_timeout_base_s + self.commit_timeout_per_million_rows_s * max(0, rows) / 1e6

    def retry_pause(self, attempt: int) -> float:
        """The pause before attempt `attempt` (2, 3, ...) of one group."""
        return min(self.retry_pause_max_s, self.retry_pause_s * (2 ** max(0, attempt - 2)))


@dataclass(frozen=True)
class StreamSettings:
    """`stream.*`: the stream component (design sections 2, 3, 8; SPEC 4.3 to 4.6)."""
    slot: str = "cdc_v2_slot"
    #: the node the engine decodes from (replica mode: a standby; SPEC R17)
    node: str = "replica"
    #: no frame at all for this long, while the stream reads and no commit or declared wait runs: the engine is torn
    #: down (an early signal of a dead replication connection; docs/design/engine.md 6)
    frame_silence_s: float = 30.0
    #: a heartbeat written on the primary must come back as an END within this time (SPEC 4.6), counted only while
    #: no transaction is open and after the first BEGIN or marker of an engine life
    marker_timeout_s: float = 60.0
    #: the source-side heartbeat write on the primary (SPEC 4.4)
    heartbeat_interval_s: float = 10.0
    #: with nothing but heartbeat transactions, at most one destination commit per this interval (design 3, "Idle")
    idle_position_interval_s: float = 10.0
    #: SPEC 4.3: starts at one applied position after which a failure that belongs to no table takes the last line
    max_attempts_at_position: int = 3
    #: a SCHEMA frame whose key differs from the catalog's: engine restarts at one position before it is a failure
    #: of the table (rule K)
    max_key_restarts: int = 2
    #: the attach step's declared wait (design 8: at least 75 s after a take-over)
    attach_timeout_s: float = 90.0
    #: the source or the destination cannot be reached: how long the stream retries inside one declared wait before
    #: it fails and the supervisor takes over (SPEC 4.7)
    unreachable_wait_s: float = 1800.0
    #: pause between such retries (doubles up to 30 s)
    unreachable_pause_s: float = 1.0
    #: STOP of the engine at a planned stop
    engine_stop_s: float = 20.0
    #: the stand-in of `backfill.on_group` reads the request table at most this often (design 7.1)
    request_poll_s: float = 5.0
    #: alerts that waited in the outbox this long are written in a transaction of their own
    alert_flush_s: float = 5.0
    #: one source statement (catalog read, slot facts)
    source_statement_s: float = 30.0

    def __post_init__(self):
        if not isinstance(self.slot, str) or not _SLOT.match(self.slot):
            raise ValueError("stream.slot is a lower-case name that starts with cdc_v2, got %r" % (self.slot,))
        if not isinstance(self.node, str) or not self.node:
            raise ValueError("stream.node names a source node")
        for n in ("frame_silence_s", "marker_timeout_s", "heartbeat_interval_s", "idle_position_interval_s",
                  "attach_timeout_s", "unreachable_wait_s", "unreachable_pause_s", "engine_stop_s",
                  "request_poll_s", "alert_flush_s", "source_statement_s"):
            _positive("stream", n, getattr(self, n))
        _count("stream", "max_attempts_at_position", self.max_attempts_at_position)
        _count("stream", "max_key_restarts", self.max_key_restarts, 0)
        # SPEC 4.6 and the design order: the marker rule must outlast a beat and the silence rule
        if self.marker_timeout_s <= self.heartbeat_interval_s:
            raise ValueError("stream.marker_timeout must be longer than stream.heartbeat_interval")
        # design 8 wants the attach wait at least 75 s after a take-over with the production leader timings (the old
        # run can take the slot back until its hard exit, 60 s after it froze); tests shorten every leader timing,
        # so the order is checked against the hard-exit age by the configuration loader, not here


__all__ = ["GroupSettings", "StreamSettings"]
