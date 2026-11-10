"""The logical replication slot (C7, design section 8): create on a standby or a primary, drop, facts, terminate
the holder, timeline history.

Only an explicit operator action or the enactment of a slot finding calls `drop`. `terminate_holder` terminates
only the backend that holds *our* slot name, identified by the slot's `active_pid` together with that backend's
start time, read again in the terminating statement itself.
"""
from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from cdc_v2 import lsn as lsnmod
from cdc_v2.crashpoints import crash_point, declare
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.pg import errors
from cdc_v2.pg.conn import PgConnection, connect
from cdc_v2.pg.sql import check_slot_name, literal
from cdc_v2.pg.timeline import TimelineHistory, parse_history

log = logging.getLogger("cdc_v2.pg.slot")

declare("pg.slot_created", "a logical replication slot was created on a source node")
declare("pg.slot_dropped", "a logical replication slot was dropped on a source node")
declare("pg.holder_terminated", "the backend holding our slot was terminated")

PLUGIN = "pgoutput"


class SlotError(errors.SourceError):
    pass


class SlotExists(SlotError):
    pass


class SlotMissing(SlotError):
    pass


class SlotActive(SlotError):
    """The slot is held by a backend and the call was not told to terminate it."""


class SlotCreateTimeout(DeadlineExceeded):
    """Slot creation passed its deadline. `report` says what the standby was waiting for, as far as the roles may
    see it (the oldest running transaction id on the primary, prepared transactions), and whether the abandoned
    creation left anything behind (it never should: `leftover` is False)."""

    def __init__(self, what: str, report: dict):
        super().__init__(what)
        self.report = report


# --------------------------------------------------------------------------------------------------- facts

@dataclass(frozen=True)
class ServerFacts:
    system_identifier: str              # pg_control_system() (executable by every role on 16 and 18)
    timeline: int | None
    timeline_source: str                # 'wal' (primary: current WAL file), 'receiver', 'checkpoint', 'unknown'
    in_recovery: bool
    position: int                       # replay position on a standby, insert position on a primary
    receive_position: int | None        # standby: last received position (None if streaming has not started)
    server_version_num: int
    wal_level: str
    wal_sender_timeout_ms: int          # pg_settings unit is ms; 0 = no timeout (the pipeline refuses it)
    max_slot_wal_keep_size_mb: int      # -1 = unlimited
    idle_replication_slot_timeout_s: int | None    # 18+; None where the server has no such setting
    hot_standby_feedback: bool
    primary_slot_name: str | None
    read_at: dt.datetime                # the server's now() at the read (statement start)


@dataclass(frozen=True)
class SlotFacts:
    name: str
    plugin: str | None
    database: str | None
    slot_type: str
    temporary: bool
    active: bool
    active_pid: int | None
    restart_lsn: int | None
    confirmed_flush_lsn: int | None
    wal_status: str | None
    safe_wal_size: int | None
    conflicting: bool | None            # 16+
    invalidation_reason: str | None     # 17+ (None before)
    inactive_since: dt.datetime | None  # 17+
    failover: bool | None               # 17+
    synced: bool | None                 # 17+
    two_phase: bool | None
    two_phase_at: int | None            # 18+
    catalog_xmin: int | None
    xmin: int | None
    holder_backend_start: dt.datetime | None    # of the backend `active_pid` (microsecond precision)
    holder_backend_start_us: int | None         # the same as microseconds since the Unix epoch (exact)
    holder_application_name: str | None
    holder_backend_type: str | None
    stats_reset: dt.datetime | None             # pg_stat_replication_slots
    total_txns: int | None
    total_bytes: int | None
    spill_bytes: int | None
    server: ServerFacts = field(repr=False, default=None)

    @property
    def holder(self) -> tuple[int, int] | None:
        """(active_pid, backend start in µs): what identifies one engine's replication session."""
        if self.active_pid is None or self.holder_backend_start_us is None:
            return None
        return (self.active_pid, self.holder_backend_start_us)

    @property
    def invalid(self) -> bool:
        """The slot can never be used again: lost WAL, an invalidation reason, or a recovery conflict."""
        return bool(self.wal_status == "lost" or self.invalidation_reason or self.conflicting)


def _slot_columns(major: int) -> str:
    cols = ["s.plugin::text", "s.database::text", "s.slot_type", "s.temporary", "s.active", "s.active_pid",
            "s.restart_lsn::text", "s.confirmed_flush_lsn::text", "s.wal_status", "s.safe_wal_size",
            "s.conflicting" if major >= 16 else "NULL::bool",
            "s.invalidation_reason" if major >= 17 else "NULL::text",
            "s.inactive_since" if major >= 17 else "NULL::timestamptz",
            "s.failover" if major >= 17 else "NULL::bool",
            "s.synced" if major >= 17 else "NULL::bool",
            "s.two_phase",
            "s.two_phase_at::text" if major >= 18 else "NULL::text",
            "s.catalog_xmin::text::bigint", "s.xmin::text::bigint"]
    return ", ".join(cols)


_FACTS_SQL = """
WITH srv AS (
  SELECT (SELECT system_identifier::text FROM pg_control_system()) AS sysid,
         pg_is_in_recovery() AS rec,
         CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_insert_lsn() END::text AS pos,
         pg_last_wal_receive_lsn()::text AS recv,
         CASE WHEN NOT pg_is_in_recovery()
              THEN ('x' || substr(pg_walfile_name(pg_current_wal_insert_lsn()), 1, 8))::bit(32)::int END AS tli_wal,
         (SELECT received_tli FROM pg_stat_wal_receiver) AS tli_recv,
         (SELECT timeline_id FROM pg_control_checkpoint()) AS tli_ckpt,
         current_setting('server_version_num')::int AS ver,
         current_setting('wal_level') AS wal_level,
         (SELECT setting::bigint * CASE unit WHEN 's' THEN 1000 WHEN 'min' THEN 60000 ELSE 1 END
            FROM pg_settings WHERE name = 'wal_sender_timeout') AS wst_ms,
         (SELECT setting::bigint FROM pg_settings WHERE name = 'max_slot_wal_keep_size') AS mswks,
         (SELECT setting::bigint * CASE unit WHEN 'min' THEN 60 ELSE 1 END
            FROM pg_settings WHERE name = 'idle_replication_slot_timeout') AS irst,
         current_setting('hot_standby_feedback') = 'on' AS hsf,
         nullif(current_setting('primary_slot_name'), '') AS psn,
         now() AS read_at
)
SELECT srv.*, s.slot_name IS NOT NULL AS exists, {slot_cols},
       a.backend_start, (extract(epoch FROM a.backend_start) * 1000000)::bigint AS backend_start_us,
       a.application_name, a.backend_type,
       st.stats_reset, st.total_txns, st.total_bytes, st.spill_bytes
FROM srv
LEFT JOIN pg_replication_slots s ON s.slot_name = %(name)s
LEFT JOIN pg_stat_activity a ON a.pid = s.active_pid
LEFT JOIN pg_stat_replication_slots st ON st.slot_name = s.slot_name
"""

_SERVER_COLS = 15


def _read(node: PgConnection, name: str, deadline: Deadline) -> tuple[SlotFacts | None, ServerFacts]:
    check_slot_name(name)
    sql = _FACTS_SQL.format(slot_cols=_slot_columns(node.major))
    row = node.execute(sql, {"name": name}, deadline=deadline, what="slot.facts")[0]
    (sysid, rec, pos, recv, tli_wal, tli_recv, tli_ckpt, ver, wal_level, wst_ms, mswks, irst, hsf, psn,
     read_at) = row[:_SERVER_COLS]
    if tli_wal is not None:
        tli, src = tli_wal, "wal"
    elif tli_recv is not None:
        tli, src = tli_recv, "receiver"
    elif tli_ckpt is not None:
        tli, src = tli_ckpt, "checkpoint"
    else:
        tli, src = None, "unknown"
    server = ServerFacts(system_identifier=sysid, timeline=tli, timeline_source=src, in_recovery=rec,
                         position=lsnmod.parse(pos), receive_position=lsnmod.parse(recv) if recv else None,
                         server_version_num=ver, wal_level=wal_level, wal_sender_timeout_ms=int(wst_ms),
                         max_slot_wal_keep_size_mb=int(mswks),
                         idle_replication_slot_timeout_s=int(irst) if irst is not None else None,
                         hot_standby_feedback=hsf, primary_slot_name=psn, read_at=read_at)
    rest = row[_SERVER_COLS:]
    if not rest[0]:
        return None, server
    (plugin, database, slot_type, temporary, active, active_pid, restart, confirmed, wal_status, safe, conflicting,
     reason, inactive_since, failover, synced, two_phase, two_phase_at, catalog_xmin, xmin, bstart, bstart_us,
     appname, btype, stats_reset, total_txns, total_bytes, spill_bytes) = rest[1:]
    facts = SlotFacts(
        name=name, plugin=plugin, database=database, slot_type=slot_type, temporary=temporary, active=active,
        active_pid=active_pid, restart_lsn=lsnmod.parse(restart) if restart else None,
        confirmed_flush_lsn=lsnmod.parse(confirmed) if confirmed else None, wal_status=wal_status,
        safe_wal_size=safe, conflicting=conflicting, invalidation_reason=reason, inactive_since=inactive_since,
        failover=failover, synced=synced, two_phase=two_phase,
        two_phase_at=lsnmod.parse(two_phase_at) if two_phase_at else None, catalog_xmin=catalog_xmin, xmin=xmin,
        holder_backend_start=bstart if active_pid is not None else None,
        holder_backend_start_us=bstart_us if active_pid is not None else None,
        holder_application_name=appname if active_pid is not None else None,
        holder_backend_type=btype if active_pid is not None else None, stats_reset=stats_reset,
        total_txns=total_txns, total_bytes=total_bytes, spill_bytes=spill_bytes, server=server)
    return facts, server


def facts(node: PgConnection, name: str, *, deadline: Deadline) -> SlotFacts | None:
    """Everything section 8 of the design compares, read in one statement: the slot, its holder (the backend
    `active_pid`, its start time and application name) and the server. None when there is no such slot."""
    return _read(node, name, deadline)[0]


def server_facts(node: PgConnection, *, deadline: Deadline) -> ServerFacts:
    return _read(node, "cdc_v2_no_such_slot_x", deadline)[1]


# --------------------------------------------------------------------------------------------------- create

@dataclass(frozen=True)
class CreatedSlot:
    name: str
    consistent_point: int
    on_standby: bool
    failover: bool | None
    seconds: float
    nudges: int
    nudge_method: str | None


def nudge_method(primary: PgConnection, deadline: Deadline) -> str:
    """How this role can make the primary log a running-transactions record: 'standby_snapshot' when it may call
    pg_log_standby_snapshot(), else 'write' (a non-transactional logical message: WAL that the background writer
    follows with a running-transactions record within about 15 s), else 'none'."""
    row = primary.execute(
        "SELECT coalesce(bool_or(has_function_privilege(p.oid, 'EXECUTE')) FILTER "
        "                (WHERE p.proname = 'pg_log_standby_snapshot'), false), "
        "       coalesce(bool_or(has_function_privilege(p.oid, 'EXECUTE')) FILTER "
        "                (WHERE p.proname = 'pg_logical_emit_message'), false) "
        "FROM pg_proc p WHERE p.pronamespace = 'pg_catalog'::regnamespace "
        "AND p.proname IN ('pg_log_standby_snapshot', 'pg_logical_emit_message')",
        deadline=deadline, what="slot.nudge_method")[0]
    return "standby_snapshot" if row[0] else ("write" if row[1] else "none")


def _nudge_once(primary: PgConnection, method: str, deadline: Deadline) -> None:
    if method == "standby_snapshot":
        primary.execute("SELECT pg_log_standby_snapshot()", deadline=deadline, what="slot.nudge")
    elif method == "write":
        primary.execute("SELECT pg_logical_emit_message(false, 'cdc_v2.nudge', '')", deadline=deadline,
                        what="slot.nudge")


def create(node: PgConnection, name: str, deadline: Deadline, *, primary: PgConnection | None = None,
           nudge: str | Callable[[Deadline], None] | None = "auto", nudge_interval_s: float = 0.5,
           failover: bool = True) -> CreatedSlot:
    """Create the logical slot `name` (pgoutput, not temporary, two-phase off) and return its consistent point.

    On a standby the creation waits for a running-transactions record from the primary. With `primary` given, a
    helper thread asks for one while the creation waits, every `nudge_interval_s`, on a connection of its own opened
    from `primary`'s settings (the caller's connection is used only to choose the method and for the report):
    `pg_log_standby_snapshot()` if the role may call it, else a write of its own (`nudge_method`). `nudge` may also
    be a callable(deadline) (for example the heartbeat write) or None.

    The creating statement runs with `statement_timeout = 0` (the caller's deadline bounds it) and
    `client_connection_check_interval = 1000` (a backend whose client died abandons the creation). When the creation
    fails for any reason — the deadline (`SlotCreateTimeout`), a lost connection, any error — the creating backend,
    identified by process id and start time, is terminated from a second connection if it is still there, and the
    call checks whether a slot was left behind; the report is the exception's `report` attribute.

    On a primary: `failover = true` where the server supports it (17+), and the facts say so.
    """
    check_slot_name(name)
    t0 = time.monotonic()
    standby = node.in_recovery(deadline)
    if facts(node, name, deadline=deadline) is not None:
        raise SlotExists("slot %s already exists" % name)
    creator = tuple(node.execute("SELECT pid, (extract(epoch FROM backend_start) * 1000000)::bigint "
                                 "FROM pg_stat_activity WHERE pid = pg_backend_pid()", deadline=deadline,
                                 what="slot.create")[0])
    use_failover = (not standby) and failover and node.major >= 17
    if standby or not use_failover:
        sql = "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s, false, false)"
    else:
        sql = "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s, false, false, true)"
    method: str | None = None
    fn: Callable[[Deadline], None] | None = None
    if standby and callable(nudge):
        method, fn = "callable", nudge
    elif standby and nudge is not None and primary is not None:
        method = nudge_method(primary, deadline) if nudge == "auto" else nudge
        if method not in ("standby_snapshot", "write", "none"):
            raise ValueError("bad nudge method %r" % nudge)
    stop = threading.Event()
    count = [0]
    nudge_errors: list[str] = []

    def nudger():
        own = None
        try:
            while not stop.is_set() and deadline.remaining() > 1.0:
                step = Deadline(min(deadline.at - 0.5, Deadline.after(max(1.0, nudge_interval_s * 4)).at))
                try:
                    if fn is not None:
                        fn(step)
                    else:
                        if own is None or own.closed:
                            own = connect(primary.node, step, application_name="cdc_v2_slot_nudge")
                        _nudge_once(own, method, step)
                    count[0] += 1
                except Exception as e:      # noqa: BLE001 - reported; the creation goes on waiting
                    if len(nudge_errors) < 5:
                        nudge_errors.append(type(e).__name__)
                stop.wait(nudge_interval_s)
        finally:
            if own is not None:
                own.close()

    th = None
    if fn is not None or method in ("standby_snapshot", "write"):
        th = threading.Thread(target=nudger, daemon=True, name="cdc_v2-slot-nudge")
        th.start()
    failure: BaseException | None = None
    lsn_text = None
    try:
        lsn_text = _create_stmt(node, sql, name, deadline)
    except SlotExists:
        raise
    except BaseException as e:          # noqa: BLE001 - handled below, then raised again
        failure = e
    finally:
        stop.set()
        if th is not None:
            th.join(max(1.0, nudge_interval_s * 4) + 3 * (primary.node.cancel_grace_s if primary else 2.0) + 1)
    if failure is not None:
        report = _abandon(node, name, creator, primary if standby else None, method, count[0],
                          time.monotonic() - t0, nudge_errors, standby)
        if isinstance(failure, DeadlineExceeded):
            raise SlotCreateTimeout("slot.create %s: deadline passed after %.1f s" % (name, time.monotonic() - t0),
                                    report)
        try:
            failure.report = report
        except AttributeError:
            pass
        raise failure
    crash_point("pg.slot_created")
    return CreatedSlot(name, lsnmod.parse(lsn_text), standby, (use_failover if node.major >= 17 else None)
                       if not standby else None, time.monotonic() - t0, count[0], method)


def _create_stmt(node: PgConnection, sql: str, name: str, deadline: Deadline) -> str:
    # The caller's deadline bounds the creation, not the session's statement_timeout. A creation whose client is gone
    # (process killed) must not finish the slot later: with client_connection_check_interval the backend checks the
    # socket every second while it waits and aborts the creation when the client has closed.
    node.execute("SET statement_timeout = 0", deadline=deadline, what="slot.create")
    node.execute("SET client_connection_check_interval = 1000", deadline=deadline, what="slot.create")
    try:
        return node.scalar(sql, (name, PLUGIN), deadline=deadline, what="slot.create")
    except errors.SourceError as e:
        if e.sqlstate == "42710":
            raise SlotExists("slot %s already exists" % name, e.sqlstate) from None
        raise
    finally:
        if not node.closed:
            try:
                node.execute("RESET statement_timeout", deadline=Deadline.after(2), what="slot.create")
                node.execute("RESET client_connection_check_interval", deadline=Deadline.after(2), what="slot.create")
            except (errors.SourceError, DeadlineExceeded):
                pass


def _abandon(node: PgConnection, name: str, creator: tuple[int, int], primary, method, nudges, waited,
             nudge_errors, standby: bool) -> dict:
    """After a creation failed: make sure the creating backend (pid and start time) is gone, so that it cannot finish
    the slot later, and check whether a slot was left behind (`leftover`: True, False, or None when the check itself
    failed, with `cleanup_error`)."""
    report: dict = {"waited_s": round(waited, 2), "nudge_method": method, "nudges": nudges,
                    "nudge_errors": nudge_errors, "creator_terminated": False, "leftover": None}
    pid, start_us = creator
    match = "pid = %s AND (extract(epoch FROM backend_start) * 1000000)::bigint = %s"
    grace = Deadline.after(max(10.0, 4 * node.node.cancel_grace_s))
    try:
        with connect(node.node, grace, application_name="cdc_v2_slot_cleanup") as c:
            while c.scalar("SELECT count(*) FROM pg_stat_activity WHERE " + match, (pid, start_us), deadline=grace,
                           what="slot.cleanup"):
                if c.scalar("SELECT bool_or(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE " + match,
                            (pid, start_us), deadline=grace, what="slot.cleanup"):
                    report["creator_terminated"] = True
                grace.sleep(0.05, "slot.create cleanup")
            report["leftover"] = facts(c, name, deadline=grace) is not None
            if primary is None and not standby:
                _primary_report(c, report)
    except (errors.SourceError, DeadlineExceeded) as e:
        report["cleanup_error"] = "%s: %s" % (type(e).__name__, e)
    if primary is not None and not primary.closed:
        _primary_report(primary, report)
    log.warning("slot creation abandoned", extra={"cdc_v2": report})
    return report


def _primary_report(primary: PgConnection, report: dict) -> None:
    """What the role may see of why a creation waits: the oldest running transaction id, prepared transactions."""
    try:
        row = primary.execute(
            "SELECT pg_snapshot_xmin(pg_current_snapshot())::text, pg_snapshot_xmax(pg_current_snapshot())::text,"
            " (SELECT count(*) FROM pg_prepared_xacts), (SELECT min(prepared) FROM pg_prepared_xacts)",
            deadline=Deadline.after(3), what="slot.report")[0]
        report.update(oldest_running_xid=int(row[0]), next_xid=int(row[1]), prepared_transactions=row[2],
                      oldest_prepared_at=row[3].isoformat() if row[3] else None)
    except (errors.SourceError, DeadlineExceeded) as e:
        report["primary_report_error"] = type(e).__name__


# --------------------------------------------------------------------------------------------------- drop, terminate

def drop(node: PgConnection, name: str, deadline: Deadline, *, terminate: bool = False) -> bool:
    """Drop the slot. Only an operator action or the enactment of a slot finding calls this. Refuses an active slot
    (`SlotActive`) unless `terminate` is true, in which case the holder is terminated first. Returns False when the
    slot did not exist."""
    check_slot_name(name)
    while True:
        f = facts(node, name, deadline=deadline)
        if f is None:
            return False
        if f.active:
            if not terminate:
                raise SlotActive("slot %s is held by pid %s" % (name, f.active_pid))
            r = terminate_holder(node, name, deadline)
            if r.outcome == "holder unknown":
                raise SlotActive("slot %s is held by a backend this role cannot see" % name)
        try:
            node.execute("SELECT pg_drop_replication_slot(%s)", (name,), deadline=deadline, what="slot.drop")
        except errors.SourceError as e:
            if e.sqlstate == "42704":           # undefined_object: dropped meanwhile
                return False
            if e.sqlstate == "55006" and terminate:      # taken again between the terminate and the drop
                deadline.sleep(0.05, "slot.drop")
                continue
            if e.sqlstate == "55006":
                raise SlotActive("slot %s became active" % name, e.sqlstate) from None
            raise
        crash_point("pg.slot_dropped")
        return True


@dataclass(frozen=True)
class TerminateResult:
    outcome: str                                  # 'inactive' | 'missing' | 'other holder' | 'holder unknown'
    terminated: tuple[tuple[int, int], ...]       # (pid, backend start µs) of every backend terminated
    facts: SlotFacts | None                       # the last facts read


_TERMINATE_SQL = """
SELECT pg_terminate_backend(s.active_pid)
FROM pg_replication_slots s JOIN pg_stat_activity a ON a.pid = s.active_pid
WHERE s.slot_name = %(name)s AND s.active_pid = %(pid)s
  AND (extract(epoch FROM a.backend_start) * 1000000)::bigint = %(start)s
"""


def terminate_holder(node: PgConnection, name: str, deadline: Deadline, *, expect: tuple[int, int] | None = None,
                     _after_read: Callable[[SlotFacts], None] | None = None) -> TerminateResult:
    """Terminate the backend that holds our slot `name`, and only that: the terminating statement checks the slot's
    `active_pid` and that backend's start time again. Repeats until the slot is inactive, missing, or (with
    `expect` = (pid, backend start µs)) held by another backend than the expected one; raises DeadlineExceeded when
    the deadline passes first. Never terminates a pid taken from anywhere else.

    'holder unknown': the slot is active but this role cannot see its holder's start time (a backend of another
    role, without pg_read_all_stats); nothing is terminated."""
    check_slot_name(name)
    done: list[tuple[int, int]] = []
    while True:
        f = facts(node, name, deadline=deadline)
        if f is None:
            return TerminateResult("missing", tuple(done), None)
        if not f.active:
            return TerminateResult("inactive", tuple(done), f)
        if f.holder is None:
            return TerminateResult("holder unknown", tuple(done), f)
        if expect is not None and f.holder != tuple(expect):
            return TerminateResult("other holder", tuple(done), f)
        if _after_read is not None:
            _after_read(f)
        rows = node.execute(_TERMINATE_SQL, {"name": name, "pid": f.active_pid, "start": f.holder_backend_start_us},
                            deadline=deadline, what="slot.terminate_holder")
        if rows and rows[0][0] and f.holder not in done:
            done.append(f.holder)
            crash_point("pg.holder_terminated")
        deadline.sleep(0.05, "slot.terminate_holder %s" % name)


# --------------------------------------------------------------------------------------------------- timeline

def timeline_history(node: PgConnection, timeline: int | None = None, *, deadline: Deadline) -> TimelineHistory:
    """IDENTIFY_SYSTEM and TIMELINE_HISTORY over a `replication=database` connection (opened from the node's
    settings if `node` is not one). Needs the REPLICATION attribute. Without `timeline`: the server's current one."""
    own = None
    rconn = node
    if not node.replication:
        own = rconn = connect(node.node, deadline, replication=True,
                              application_name=node.node.application_name)
    try:
        sysid, tli, xlogpos, _db = rconn.simple("IDENTIFY_SYSTEM", deadline=deadline, what="IDENTIFY_SYSTEM")[0]
        current = int(tli)
        want = current if timeline is None else int(timeline)
        if want <= 1:
            hist = TimelineHistory(timeline=want, entries=())
        else:
            rows = rconn.simple("TIMELINE_HISTORY %d" % want, deadline=deadline, what="TIMELINE_HISTORY")
            content = rows[0][1]
            hist = parse_history(content.decode() if isinstance(content, (bytes, bytearray)) else content, want)
        return TimelineHistory(timeline=hist.timeline, entries=hist.entries,
                               system_identifier=sysid.decode() if isinstance(sysid, bytes) else str(sysid),
                               position=lsnmod.parse(xlogpos.decode() if isinstance(xlogpos, bytes) else xlogpos))
    finally:
        if own is not None:
            own.close()


__all__ = ["create", "drop", "facts", "server_facts", "terminate_holder", "timeline_history", "nudge_method",
           "SlotFacts", "ServerFacts", "CreatedSlot", "TerminateResult", "SlotError", "SlotExists", "SlotMissing",
           "SlotActive", "SlotCreateTimeout", "literal"]
