"""The stream component (task S sections 4 to 8; design sections 2, 3, 6.4, 8).

One instance runs in one supervisor thread. It goes through **engine lives**: each life starts with the start
transaction (staging dropped, progress row reset, attempt counter raised; `stream.state.start`), reads the catalog,
attaches its engine through the attach step, sends `DURABLE(applied)` after the decision, and then reads frames:

    frames -> Assembler (BEGIN..END, END counts, discard by position) -> Group (routing, segments)
           -> build (concatenate, transform) -> commit.run_group (design 3) -> DURABLE

A life ends by a planned stop (`finish_current_group`), or by `RestartEngine` (a key that differs from the catalog,
a table named in a FATAL, a protocol error, a silent engine, the marker rule, a reposition), after which a new life
starts with a new engine. Errors that no new life cures go to the supervisor. `FenceLost` and `md.LaneMisuse` are
final (`supervisor.Final`): the pipeline ends with the exit status of C12.

Bounds (docs/design/stream.md): `stream.frame_silence` while reading; the marker rule (`stream.marker_timeout`)
counted only while no transaction is open and after the first BEGIN or marker of a life; the attach wait
(`stream.attach_timeout`); retries against a source or destination that cannot be reached inside one declared wait
of `stream.unreachable_wait`; `stream.max_attempts_at_position` starts at one position before the last line of SPEC
4.3; `apply.max_group_attempts` runs of a group; `apply.max_table_attempts` failures of a table.
"""
from __future__ import annotations

import contextlib
import errno
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

from cdc_v2 import apply as A
from cdc_v2 import transform as T
from cdc_v2.crashpoints import crash_point, declare
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.faultsites import declare_site, fault_site
from cdc_v2.md import errors as md_errors
from cdc_v2.pg import errors as pg_errors
from cdc_v2.stream import frames as F
from cdc_v2.stream import state as st
from cdc_v2.stream.attach import AttachContext, attach
from cdc_v2.stream.beat import AlertOutbox, BeatBoard
from cdc_v2.stream.commit import (Acknowledger, GroupFailed, GroupJob, PositionDiverged, RunPolicy, TableFailures,
                                  run_group)
from cdc_v2.stream.group import (Assembler, EndedTxn, Group, PlanEntry, ProtocolError, TableFailure, TxnInfo, build)
from cdc_v2.stream.settings import GroupSettings, StreamSettings
from cdc_v2.stream.staging import Stage, StagedTxn, chunk_parts_from_works
from cdc_v2.supervisor import Final, Stopping

log = logging.getLogger("cdc_v2.stream")

CP_ATTACHED = declare("stream.attached", "the attach decision is committed (slot and stream: verified); DURABLE of "
                      "the applied position follows")
SITE_SPILL = declare_site("stream.spill_full", "the stream's spill area has no space left (transform or apply plan)")


class RestartEngine(Exception):
    """End this engine life and start a new one. `reason` is a short identifier; `backoff` asks for a pause."""

    def __init__(self, reason: str, backoff: bool = True):
        self.reason = reason
        self.backoff = backoff
        super().__init__(reason)


class SlotFinding(Exception):
    """The attach step found the slot not continuous (design 8). The minimal attach stops the stream with an
    alert; package V enacts the finding."""


@dataclass(frozen=True)
class EngineStart:
    start_position: int
    engine_id: str
    keyless_tables: tuple
    table_include: Optional[str]        # Debezium table.include.list, or None (every published table)


@dataclass
class StreamContext:
    """What the stream component needs from the pipeline. One per process; components are re-created."""
    settings: StreamSettings
    group: GroupSettings
    apply: A.ApplySettings
    transform: T.TransformSettings
    epoch: int
    run_id: str
    lane_factory: Callable[[], object]              # -> md.Lane("stream") on a new connection
    source: object                                  # stream.source.PgSource or a stand-in
    engine_factory: Callable[[EngineStart], object]
    board: BeatBoard
    outbox: AlertOutbox
    seams: object
    heartbeat_key: Optional[tuple] = ("cdc_v2", "heartbeat")
    clock: Callable[[], float] = time.monotonic
    failures: TableFailures = field(default_factory=TableFailures)
    key_restarts: dict = field(default_factory=dict)
    excluded: set = field(default_factory=set)      # tables left out of the engine's include list
    stats: dict = field(default_factory=lambda: {"groups": 0, "txns": 0, "rows": 0, "lives": 0, "restarts": {}})
    on_group: Optional[Callable] = None             # tests and evidence: called after each committed group


def _include_regex(names) -> str:
    """Debezium's table.include.list for exact names: each `schema.table` as a Java regular expression, commas
    escaped for Debezium's list splitter."""
    import re
    out = []
    for schema, table in sorted(names):
        pat = "%s\\.%s" % (re.escape(schema), re.escape(table))
        out.append(pat.replace(",", "\\,"))
    return ",".join(out)


class StreamComponent:
    """The stream component. `run(handle)` is the supervisor's entry; `finish_current_group(deadline)` the planned
    hand-over of C12."""

    def __init__(self, ctx: StreamContext):
        self.ctx = ctx
        self._finish = threading.Event()
        self._finished = threading.Event()
        self.finished_position: Optional[int] = None
        self.applied: Optional[int] = None
        self.life = None

    # -------------------------------------------------------------------------------------------- planned stop

    def finish_current_group(self, deadline: Deadline) -> Optional[int]:
        """C12: the group in flight is committed and acknowledged, no new group starts, a transaction larger than a
        group that is still arriving is abandoned (its staging is dropped by the next start). Returns the applied
        position, or None when the stream did not finish within `deadline`."""
        self._finish.set()
        if self._finished.wait(max(0.0, deadline.remaining())):
            return self.finished_position
        return None

    # -------------------------------------------------------------------------------------------- the component

    def run(self, handle):
        ctx = self.ctx
        lane = ctx.lane_factory()
        handle.on_close(lambda: _abort(lane))
        consecutive = 0
        try:
            while not handle.stopping:
                life = _Life(self, lane, handle)
                self.life = life
                ctx.stats["lives"] += 1
                try:
                    life.run()
                    if life.finished:
                        self.finished_position = life.applied
                        self._finished.set()
                        return
                    consecutive = 0
                except RestartEngine as r:
                    consecutive = consecutive + 1 if r.backoff else 0
                    ctx.stats["restarts"][r.reason] = ctx.stats["restarts"].get(r.reason, 0) + 1
                    log.warning("stream engine restart", extra={"cdc": {"reason": r.reason, "position": life.applied,
                                                                        "consecutive": consecutive}})
                    if consecutive:
                        handle.sleep(min(30.0, 0.5 * (2 ** (consecutive - 1))), "stream_restart")
                finally:
                    life.close()
        except (md_errors.FenceLost, md_errors.LaneMisuse) as e:
            raise Final(e) from None
        finally:
            with contextlib.suppress(Exception):
                lane.conn.close()


def _abort(lane):
    with contextlib.suppress(Exception):
        lane.conn.abort()


class _Life:
    """One engine life of the stream component."""

    def __init__(self, comp: StreamComponent, lane, handle):
        self.comp = comp
        self.ctx = comp.ctx
        self.s = comp.ctx.settings
        self.g = comp.ctx.group
        self.lane = lane
        self.handle = handle
        self.clock = comp.ctx.clock
        self.engine_id = "cdc_v2_e_" + uuid.uuid4().hex[:12]
        self.engine = None
        self.transformer = None
        self.applied: Optional[int] = None
        self.row: Optional[st.StreamRow] = None
        self.routes: dict = {}
        self.catalog: dict = {}
        self.version = None
        self.plans: dict = {}
        self.finished = False
        self.first_group = True
        self.alerted: set = set()
        self.ack: Optional[Acknowledger] = None
        self.staged: Optional[StagedTxn] = None

    # -------------------------------------------------------------------------------------------- helpers

    def close(self):
        if self.engine is not None:
            with contextlib.suppress(Exception):
                self.engine.kill()
            self.engine = None
        if self.transformer is not None:
            with contextlib.suppress(Exception):
                self.transformer.close()
            self.transformer = None

    def _wait(self, seconds: float, reason: str):
        self.handle.sleep(seconds, reason)

    @contextlib.contextmanager
    def _declared(self, reason: str, deadline: Deadline):
        with self.handle.waiting(reason, deadline):
            yield

    def _retrying(self, what: str, fn):
        """Run `fn(deadline)` again after errors of an unreachable source or destination, inside one declared wait
        of `stream.unreachable_wait` (SPEC 4.7: retry with backoff, resume by itself). `FenceLost` passes."""
        total = Deadline.after(self.s.unreachable_wait_s)
        pause = self.s.unreachable_pause_s
        with self._declared("waiting_" + what, total):
            while True:
                try:
                    return fn(total.within(max(self.s.source_statement_s, 60.0)))
                except (md_errors.FenceLost, md_errors.LaneMisuse):
                    raise
                except (md_errors.Conflict, md_errors.ConnectionLost, md_errors.CommitUnknown, DeadlineExceeded,
                        pg_errors.ConnectionLost, pg_errors.ReadAgain, pg_errors.LockTimeout,
                        pg_errors.PermissionDenied, pg_errors.SourceError) as e:
                    if total.expired:
                        raise
                    log.warning("stream: %s failed, retrying" % what, extra={"cdc": {"error": type(e).__name__}})
                    self.handle.progress("waiting_" + what)
                    if self.handle.stop_event.wait(min(pause, total.remaining())):
                        raise Stopping("stopped while waiting for %s" % what)
                    pause = min(30.0, pause * 2)

    def _tx(self, fn, deadline: Optional[Deadline] = None):
        """One stream-lane transaction running `fn(tx)`; after CommitUnknown the caller decides (raised)."""
        with self.lane.transaction(deadline) as tx:
            return fn(tx)

    def _alert_once(self, kind, severity, fields, key=()):
        k = (kind,) + tuple(fields.get(x) for x in key)
        if k in self.alerted:
            return
        self.alerted.add(k)
        self.ctx.outbox.put(kind, severity, fields, key)

    # -------------------------------------------------------------------------------------------- start

    def _start(self):
        """First start if the slot has no row (task S section 3), then the start transaction."""
        s = self.s

        def first(d):
            if st.read_row(self.lane.conn, s.slot) is not None:
                return
            facts = self.ctx.source.slot_facts(d)
            if facts is None:
                with self._declared("slot_creation", Deadline.after(max(120.0, d.remaining()))):
                    cp = self.ctx.source.create_slot(Deadline.after(120.0))
                crash_point(st.CP_SLOT_CREATED)
            else:
                cp = facts.confirmed_flush_lsn
            self._tx(lambda tx: st.first_row(tx, s.slot, cp, "first start"))
            crash_point(st.CP_FIRST_ROW)

        self._retrying("first_start", first)

        def start(d):
            def body(tx):
                row = st.start(tx, s.slot, self.engine_id)
                return row, st.load_routing(tx)
            return self._tx(body, d)

        self.row, self.routes = self._retrying("destination", start)
        crash_point(st.CP_STARTED)
        self.applied = self.row.applied_lsn
        self.comp.applied = self.applied

    def _read_catalog(self):
        def read(d):
            self.version = self.ctx.source.version(d)
            return self.ctx.source.tables(d)
        self.catalog = self._retrying("source_catalog", read)
        problems = self._retrying("source_heartbeat_check", lambda d: self.ctx.source.heartbeat_problems(d))
        if problems:
            self._alert_once("source_heartbeat_not_ready", "critical",
                             {"slot": self.s.slot, "problems": len(problems)}, key=("slot",))

    def _engine_start(self) -> EngineStart:
        keyless = tuple(sorted(k for k, t in self.catalog.items()
                               if t.published and not t.has_key and t.name_problem is None))
        include = None
        excluded = set(self.ctx.excluded) | set(getattr(self, "probe_excluded", ()))
        if excluded:
            include = _include_regex(k for k, t in self.catalog.items() if t.published and k not in excluded)
        return EngineStart(self.applied, self.engine_id, keyless, include)

    def _attach(self):
        start = self._engine_start()

        def start_engine(position):
            eng = self.ctx.engine_factory(EngineStart(position, start.engine_id, start.keyless_tables,
                                                      start.table_include))
            self.engine = eng
            return eng

        def do(d):
            ctx = AttachContext(self.lane, self.ctx.epoch, self.s.slot, self.applied, self.ctx.source, start_engine,
                                self._wait)
            return attach(ctx, Deadline.after(self.s.attach_timeout_s))

        attached = self._retrying("attach", do)
        if attached.finding:
            facts = attached.facts
            confirmed = None if facts is None else facts.confirmed_flush_lsn

            def broken(tx):
                st.slot_state(tx, self.s.slot, "unverified", "broken", "attach finding: %s" % attached.finding,
                              detail={"applied_lsn": self.applied,
                                      **({"confirmed_flush_lsn": confirmed} if confirmed is not None else {})})
                from cdc_v2 import alerts
                fields = {"slot": self.s.slot, "finding": attached.finding, "applied_position": self.applied}
                if confirmed is not None:
                    fields["confirmed_flush_lsn"] = confirmed
                alerts.emit(tx, "slot_finding", "critical", fields, key=("slot", "finding"))
            self._retrying("destination", lambda d: self._tx(broken, d))
            raise SlotFinding(attached.finding)
        self.engine = attached.engine

        def verified(tx):
            st.slot_state(tx, self.s.slot, "unverified", "verified", "attach decision: continuous",
                          detail={"applied_lsn": self.applied, "confirmed_flush_lsn": attached.confirmed,
                                  "holder_pid": attached.hello.active_pid or 0})
        self._retrying("destination", lambda d: self._tx(verified, d))
        crash_point(CP_ATTACHED)
        self.ack = Acknowledger(self.engine)
        self.ack.durable(self.applied, "read back")          # design 8: at once after the decision
        return attached

    # -------------------------------------------------------------------------------------------- the life

    def run(self):
        self._start()
        self._read_catalog()
        self.transformer = T.Transformer(self.ctx.transform)
        attached = self._attach()
        self.ctx.stats["confirmed_at_start"] = attached.confirmed
        self._loop()

    def _loop(self):
        s, g, clock = self.s, self.g, self.clock
        asm = Assembler(self.applied, clock)
        self.asm = asm
        group: Optional[Group] = None
        last_frame = last_tick = last_commit = clock()
        armed = False
        eligible = 0.0
        beats: list = []                                   # [position, eligible clock when counting started]
        while True:
            if self.comp._finish.is_set():
                self._planned_stop(group)
                return
            now = clock()
            wake = now + 1.0
            if group is not None and not group.empty:
                due = (group.first_at + g.max_group_delay_s) if group.has_work else (last_commit +
                                                                                     s.idle_position_interval_s)
                wake = min(wake, due)
            try:
                frame = self.engine.next_frame(Deadline.after(max(0.005, min(1.0, wake - now))))
            except DeadlineExceeded:
                frame = None
            except Exception as e:                                       # noqa: BLE001
                self._engine_failure(e)
            self.handle.progress("streaming")
            now = clock()
            if armed and not asm.in_transaction:
                eligible += now - last_tick
            last_tick = now
            for pos, _at in self.ctx.board.take():
                beats.append([pos, eligible if armed else None])
            if frame is None:
                if now - last_frame > s.frame_silence_s:
                    self._alert_once("engine_silent", "warning", {"slot": s.slot, "seconds": s.frame_silence_s},
                                     key=("slot",))
                    raise RestartEngine("frame_silence")
            else:
                ended = self._dispatch(frame)
                if isinstance(frame, F.Begin) and not armed:
                    armed = True
                    for b in beats:
                        b[1] = eligible if b[1] is None else b[1]
                if ended is not None:
                    pos = ended.position
                    if beats and any(b[0] <= pos for b in beats):
                        beats = [b for b in beats if b[0] > pos]
                        if not armed:
                            armed = True
                            for b in beats:
                                b[1] = eligible
                    group = self._take(ended, group)
                elif asm.open is not None and isinstance(frame, F.Rows):
                    group = self._bound_open(asm.open, group)
                last_frame = clock()                      # silence counts reading time only
            if armed:
                late = [b for b in beats if b[1] is not None and eligible - b[1] > s.marker_timeout_s]
                if late:
                    self._marker_timeout(late[0][0])
            if group is not None and not group.empty:
                now = clock()
                if group.large is not None or (group.has_work and (now - group.first_at >= g.max_group_delay_s
                                                                   or group.nbytes >= g.max_group_bytes)) \
                        or (not group.has_work and now - last_commit >= s.idle_position_interval_s):
                    self._commit(group)
                    group = None
                    last_commit = last_frame = last_tick = clock()
            elif self.ctx.outbox.age() > s.alert_flush_s and not asm.in_transaction:
                self._flush_alerts()
                last_frame = last_tick = clock()

    # -------------------------------------------------------------------------------------------- frames

    def _dispatch(self, frame) -> Optional[EndedTxn]:
        if isinstance(frame, F.Schema):
            self._schema(frame)
            return None
        if isinstance(frame, (F.Heartbeat, F.BatchEnd, F.Hello)):
            return None
        try:
            return self.asm.feed(frame)
        except ProtocolError as e:
            self._content_failure("protocol_error", str(e))

    def _take(self, ended: EndedTxn, group: Optional[Group]) -> Optional[Group]:
        """A transaction ended: discard it, close a staged one, or add it to the group."""
        t = ended.txn
        if not ended.keep:
            if t.staged and self.staged is not None:
                stage, self.staged = self.staged, None
                self._retrying("destination", lambda d: self._tx(lambda tx: stage.drop(tx), d))
            return group
        if t.staged:
            stage, self.staged = self.staged, None
            self._write_chunk(stage, t.take())
            stage.end = ended.end
            if group is not None and not group.empty:                    # pragma: no cover - committed when staging began
                self._commit(group)
            big = Group(self.applied, self.routes, self.ctx.heartbeat_key, t.arrived_at)
            big.txns.append(TxnInfo(t.xid, ended.end.ack_position, ended.end.commit_time_ms, t.rows))
            big.rows = t.rows
            big.large = stage
            for key, kind in stage.failures.items():
                big.failures[key] = kind
            return big
        if group is None:
            group = Group(self.applied, self.routes, self.ctx.heartbeat_key, t.arrived_at)
        group.add(ended, t.records, self.plans)
        return group

    def _bound_open(self, t, group: Optional[Group]) -> Optional[Group]:
        """Bounded memory (SPEC 5.4): a transaction that does not fit the group's byte limit closes the group before
        it; one larger than a group on its own goes to staging in chunks while it arrives (design 6.4)."""
        g = self.g
        if not t.staged:
            if group is not None and not group.empty and group.nbytes + t.nbytes > g.max_group_bytes:
                self._commit(group)
                group = None
            if t.nbytes > g.max_group_bytes:
                t.staged = True
                self.staged = StagedTxn(self.s.slot, self.engine_id, t.first_seq if t.first_seq is not None
                                        else t.begin.seq)
                self._write_chunk(self.staged, t.take())
        elif t.nbytes >= g.stage_chunk_bytes:
            self._write_chunk(self.staged, t.take())
        return group

    def _write_chunk(self, stage: StagedTxn, records: list):
        if not records:
            return
        chunk = Group(self.applied, self.routes, self.ctx.heartbeat_key, self.clock())
        chunk.txns.append(TxnInfo(None, 0, 0, 0))
        for key, kind in stage.left.items():
            acc = chunk._acc(key)
            acc.leave_at, acc.leave_kind = 0, kind
        for key, kind in stage.failures.items():
            chunk.failures[key] = kind
        for rec in records:
            chunk.add_record(0, rec, self.plans)
        works = self._build(chunk)
        for key, work in works.items():
            if work.leave_kind is not None:
                stage.left.setdefault(key, work.leave_kind)
            for k2, amb in ((key, work.ambiguous),):
                stage.ambiguous[k2] = stage.ambiguous.get(k2, 0) + amb
        for key, kind in chunk.failures.items():
            stage.failures.setdefault(key, kind)
        works = {k: w for k, w in works.items() if k not in stage.left and k not in stage.failures
                 and (self.routes.get(k) is not None and self.routes[k].visible)}
        parts = chunk_parts_from_works(works, stage.segs)
        for key in works:
            stage.segs.setdefault(key, 0)
        d = Deadline.after(self.g.commit_deadline_s(sum(p[3].num_rows for p in parts)))
        with self._declared("staging_chunk", d):
            stage.write_chunk(self.lane, parts, d, self._wait, self.g.max_group_attempts, self.g.retry_pause)

    # -------------------------------------------------------------------------------------------- schema frames

    def _plan(self, src, frame):
        if src is None:
            return None, "missing"
        try:
            return T.plan(src, frame, server_version_num=self.version), None
        except T.PlanMismatch as e:
            fields = {f for _p, f, _c, _s in e.differences}
            if fields & {"key", "has key", "key positions"}:
                return None, "key"
            return None, "mismatch"

    def _schema(self, frame: F.Schema):
        key = (frame.schema, frame.table)
        vkey = (frame.schema, frame.table, frame.version)
        r = self.routes.get(key)
        if r is None or r.routing == "skip":
            self.plans[vkey] = PlanEntry()
            return
        plan, err = self._plan(self.catalog.get(key), frame)
        if err == "key":
            k = (key, self.applied)
            n = self.ctx.key_restarts[k] = self.ctx.key_restarts.get(k, 0) + 1
            if n <= self.s.max_key_restarts:
                raise RestartEngine("key_mismatch", backoff=False)    # rule K: a fresh list of tables without a key
            self.plans[vkey] = PlanEntry(failure="key_mismatch")
            return
        if err is not None:
            fresh = self._retrying("source_catalog", lambda d: self.ctx.source.tables(d, names=[key]))
            src = fresh.get(key)
            if src is not None:
                self.catalog[key] = src
            plan, err = self._plan(src, frame)
            if err is not None:
                n = self.ctx.failures.add(key, self.applied)
                if n < self.g.max_table_attempts:
                    raise RestartEngine("plan_mismatch")
                self.plans[vkey] = PlanEntry(failure="plan_mismatch")
                return
        self.plans[vkey] = PlanEntry(plan, changed=plan.hash != r.plan_hash)

    # -------------------------------------------------------------------------------------------- failures

    def _engine_failure(self, e):
        from cdc_v2.stream.engine import EngineExited, EngineFatal
        if isinstance(e, EngineFatal):
            f = e.frame
            if f.schema or f.table:
                self._fatal_table((f.schema, f.table))
            failing = f.failing_position
            self._content_failure("engine_fatal", f.error_class, failing)
        if isinstance(e, EngineExited):
            if isinstance(e.status, int) and e.status < 0:
                raise RestartEngine("engine_killed")                     # a signal from outside: not the content
            self._content_failure("engine_exited", str(e.status))
        if isinstance(e, F.EngineBroken):
            self._content_failure("frame_stream_broken", "")
        raise e

    def _fatal_table(self, key):
        """A FATAL that names a table is a failure of that table (rule K): routing `skip`, an alert and quarantine
        at once, no loop of restarts; the engine starts again without the table in its include list."""
        r = self.routes.get(key)
        if r is not None and r.routing != "skip":
            def q(tx):
                return st.quarantine(tx, r, "engine_fatal_table", self.applied)
            self.routes[key] = self._retrying("destination", lambda d: self._tx(q, d))
            crash_point(st.CP_QUARANTINED)
        self.ctx.excluded.add(key)
        raise RestartEngine("fatal_table", backoff=False)

    def _content_failure(self, kind: str, detail: str, failing=None):
        """A failure that belongs to no table (task S section 7). After `stream.max_attempts_at_position` starts at
        one applied position: probing for a table whose values the driver cannot parse (a FATAL that names the
        failing event), else the last line: the slot is moved past the place, the position repositioned, every
        table backfilled."""
        here = self.row.attempts_here if self.applied == self.row.applied_lsn else 0
        log.warning("stream content failure", extra={"cdc": {"kind": kind, "position": self.applied,
                                                             "attempts": here}})
        if here >= self.s.max_attempts_at_position:
            if failing:
                self._probe(failing)
            self._last_line(kind)
        raise RestartEngine(kind)

    def _candidates(self) -> list:
        """Tables whose values the Postgres driver can fail on (docs/design/engine.md 10: now `interval`)."""
        out = []
        for key, r in sorted(self.routes.items()):
            if r.routing == "skip" or key in self.ctx.excluded:
                continue
            t = self.catalog.get(key)
            if t is None:
                continue
            fams = set()
            for c in t.columns:
                ti = c.type
                while ti is not None and ti.kind == "domain" and ti.base is not None:
                    ti = ti.base
                fams.add(ti.name if ti is not None else "")
            if "interval" in fams:
                out.append(key)
        return out

    def _probe(self, failing):
        """Restart the engine with candidate tables left out; if the failing transaction then passes, halve the set
        until one table remains, and quarantine it. Probing lives apply nothing and send no DURABLE."""
        cands = self._candidates()
        if not cands:
            return
        if self._probe_once(cands, failing) is not True:
            return
        s = list(cands)
        while len(s) > 1:
            half = s[:len(s) // 2]
            verdict = self._probe_once(half, failing)
            if verdict is None:
                return
            s = half if verdict else s[len(s) // 2:]
        culprit = s[0]
        r = self.routes.get(culprit)
        if r is not None:
            self.routes[culprit] = self._retrying(
                "destination", lambda d: self._tx(lambda tx: st.quarantine(tx, r, "engine_value", self.applied), d))
            crash_point(st.CP_QUARANTINED)
        self.ctx.excluded.add(culprit)
        raise RestartEngine("culprit_quarantined", backoff=False)

    def _probe_once(self, excluded, failing) -> Optional[bool]:
        """True: the failing transaction passed with `excluded` left out; False: it failed again; None: unknown."""
        from cdc_v2.stream.engine import EngineFatal
        if self.engine is not None:
            self.engine.kill()
            self.engine = None
        self.probe_excluded = set(excluded)
        try:
            start = self._engine_start()
            ctx = AttachContext(self.lane, self.ctx.epoch, self.s.slot, self.applied, self.ctx.source,
                                lambda p: self._set_engine(self.ctx.engine_factory(
                                    EngineStart(p, start.engine_id, start.keyless_tables, start.table_include))),
                                self._wait)
            att = self._retrying("attach", lambda d: attach(ctx, Deadline.after(self.s.attach_timeout_s)))
            if att.finding:
                return None
            lsn = failing.get("lsn") if isinstance(failing, dict) else None
            d = Deadline.after(max(120.0, self.s.attach_timeout_s))
            with self._declared("probing", d):
                while not d.expired:
                    try:
                        f = att.engine.next_frame(d.within(1.0))
                    except DeadlineExceeded:
                        self.handle.progress("probing")
                        continue
                    except EngineFatal as e:
                        return False if e.frame.failing_position == failing else None
                    except F.EngineBroken:
                        return None
                    self.handle.progress("probing")
                    if isinstance(f, F.End) and lsn is not None and f.ack_position is not None \
                            and f.ack_position >= lsn:
                        return True
            return None
        finally:
            self.probe_excluded = set()
            if self.engine is not None:
                self.engine.kill()
                self.engine = None

    def _set_engine(self, eng):
        self.engine = eng
        return eng

    def _last_line(self, kind: str):
        """SPEC 4.3, last line: stop the engine, move our slot past the place, and reposition: the applied position
        becomes the slot's new `confirmed_flush_lsn` and every table is backfilled (after a gap), in one stream-lane
        transaction with an alert that says what was skipped. The stream then goes on with a new engine."""
        if self.engine is not None:
            self.engine.kill()
            self.engine = None
        new = self._retrying("source_slot", lambda d: self.ctx.source.advance_slot(d))
        tables = [k for k, r in self.routes.items() if r.routing != "skip"]
        self._retrying("destination", lambda d: self._tx(
            lambda tx: st.reposition(tx, new, tables, "spec_4_3_last_line: %s" % kind, slot=self.s.slot,
                                     enact=self.ctx.seams.backfill_enact), d))
        crash_point(st.CP_REPOSITIONED)
        raise RestartEngine("repositioned", backoff=False)

    def _marker_timeout(self, position: int):
        """SPEC 4.6: a beat that was written did not come back within `stream.marker_timeout`: the slot and stream
        machine shows `stalled`, an alert says so (a standby that stopped replaying looks the same), and the engine
        is rebuilt through the attach step."""
        def stalled(tx):
            if not self.first_group:
                st.slot_state(tx, self.s.slot, "streaming", "stalled", "marker not seen within marker_timeout",
                              detail={"beat_position": position, "applied_lsn": self.applied})
            from cdc_v2 import alerts
            alerts.emit(tx, "marker_timeout", "warning", {"slot": self.s.slot, "beat_position": position,
                                                         "seconds": self.s.marker_timeout_s,
                                                         "note": "replication connection silent or standby replay "
                                                                 "stopped"}, key=("slot",))
        self._retrying("destination", lambda d: self._tx(stalled, d))
        raise RestartEngine("marker_timeout")

    # -------------------------------------------------------------------------------------------- groups

    def _build(self, group: Group) -> dict:
        """The change sets of a group (transform once per run of batches). A failure of one table counts toward
        `apply.max_table_attempts`, then the table leaves the group (quarantine in the group's transaction). A full
        spill area is a declared wait with one alert; nothing is dropped (SPEC 5.4)."""
        pause = 0.5
        spill_alerted = False
        while True:
            try:
                fault_site(SITE_SPILL)
                return build(group, self.transformer, T.ambiguous)
            except TableFailure as e:
                n = self.ctx.failures.add(e.key, group.prev)
                log.warning("table failure", extra={"cdc": {"schema": e.key[0], "table": e.key[1], "kind": e.kind,
                                                            "attempt": n}})
                if n >= self.g.max_table_attempts:
                    group.failures[e.key] = e.kind
                    group.tables.pop(e.key, None)
            except OSError as e:
                if e.errno not in (errno.ENOSPC, errno.EDQUOT):
                    raise
                if not spill_alerted:
                    spill_alerted = True
                    self._alert_once("spill_full", "warning", {"slot": self.s.slot, "where": "transform"},
                                     key=("slot",))
                self._wait(pause, "spill_area_full")
                pause = min(10.0, pause * 2)

    def _commit(self, group: Group):
        g, s = self.g, self.s
        works = self._build(group) if group.large is None else {}
        alerts_list = []
        for key in sorted(group.unknown):
            k = ("table_not_replicated", key)
            if k not in self.alerted:
                self.alerted.add(k)
                alerts_list.append(("table_not_replicated", "warning", {"schema": key[0][:128],
                                                                         "table": key[1][:128]},
                                    ("schema", "table")))
        ambiguous = {}
        for key, w in works.items():
            if w.ambiguous:
                ambiguous[key] = w.ambiguous
                k = ("interval_ambiguous", key)
                if k not in self.alerted:
                    self.alerted.add(k)
                    alerts_list.append(("interval_ambiguous", "warning", {"schema": key[0][:128],
                                                                          "table": key[1][:128]},
                                        ("schema", "table")))
        if group.large is not None:
            for key, n in group.large.ambiguous.items():
                if n:
                    ambiguous[key] = ambiguous.get(key, 0) + n
        outbox = self.ctx.outbox.peek()
        keys = sorted(set(works) | (set(group.large.tables) if group.large is not None else set()))
        confirmed = self.ctx.seams.tables.confirm(group.end, keys, Deadline.after(s.source_statement_s)) \
            if keys else {}
        last = group.txns[-1]
        kind = "large" if group.large is not None else ("group" if group.has_work else "idle")
        facts = st.GroupFacts(prev=group.prev, new=group.end, first_commit=group.txns[0].commit_lsn,
                              last_commit=last.commit_lsn, txns=len(group.txns), rows=group.rows,
                              commit_ms=last.commit_ms, xid=last.xid, run_id=self.ctx.run_id[:128], kind=kind)
        job = GroupJob(slot=s.slot, engine_id=self.engine_id, run_id=self.ctx.run_id, prev=group.prev, end=group.end,
                       kind=kind, facts=facts, works=works, transactions=group.transactions_table(),
                       routes=self.routes, large=group.large, quarantine=dict(group.failures), confirmed=confirmed,
                       alerts=alerts_list + outbox, ambiguous=ambiguous, first_of_life=self.first_group,
                       rows=group.rows)
        policy = RunPolicy(g.max_group_attempts, g.max_table_attempts, g.retry_pause)
        d_total = Deadline.after(g.commit_deadline_s(group.rows) * 2 + 60.0)
        try:
            with self._declared("commit", d_total):
                res = run_group(self.lane, job, policy,
                                deadline_for=lambda rows: Deadline.after(g.commit_deadline_s(rows)),
                                apply_settings=self.ctx.apply, seams=self.ctx.seams, ack=self.ack,
                                failures=self.ctx.failures, wait=self._wait,
                                on_spill=lambda j, k: self._stage_spilled(j, k),
                                read_position=lambda: st.applied_position(self.lane.conn, s.slot))
        except PositionDiverged as e:
            self.ctx.outbox.put("position_diverged", "critical", {"slot": s.slot, "prev": e.prev, "end": e.end,
                                                                   "found": -1 if e.found is None else e.found},
                                ("slot",))
            raise RestartEngine("position_diverged") from None
        except GroupFailed:
            raise RestartEngine("group_failed") from None
        self.ctx.outbox.drop(len(outbox))
        if res is None:                       # committed by an earlier run (read back): read the routing again
            self.routes = self._retrying("destination", lambda d: st.load_routing(self.lane.conn))
        else:
            self.routes = {**self.routes, **res.changes}
        if job.quarantine:
            crash_point(st.CP_QUARANTINED)
        self.applied = group.end
        self.comp.applied = group.end
        self.first_group = False
        stats = self.ctx.stats
        stats["groups"] += 1
        stats["txns"] += len(group.txns)
        stats["rows"] += group.rows
        if self.ctx.on_group is not None:
            self.ctx.on_group(group, job)

    def _stage_spilled(self, job: GroupJob, key):
        """ApplySpillFull (task S section 10): not a failure of the table. One alert; the table's change sets of
        this group are staged in `_cdc_stage` and applied in the relation form in the group's transaction."""
        self._alert_once("spill_full", "warning", {"slot": self.s.slot, "where": "apply"}, key=("slot",))
        if key is None or key not in job.works or key in job.staged_tables:
            self._wait(1.0, "spill_area_full")
            return
        if job.spill_stage is None:
            job.spill_stage = Stage(self.s.slot, self.engine_id, -(self.ctx.stats["groups"] + 1))
        stage = job.spill_stage
        work = job.works[key]
        parts = chunk_parts_from_works({key: work}, {}, job.transactions)
        d = Deadline.after(self.g.commit_deadline_s(job.rows))
        with self._declared("staging_spilled_table", d):
            stage.write_chunk(self.lane, parts, d, self._wait, self.g.max_group_attempts, self.g.retry_pause)
        job.staged_tables[key] = [stage.change_set(key, i, None, None, None) for i in range(len(work.segments))]

    def _flush_alerts(self):
        items = self.ctx.outbox.peek()
        if not items:
            return

        def body(tx):
            from cdc_v2 import alerts
            for kind, severity, fields, key in items:
                alerts.emit(tx, kind, severity, fields, key=key)
        try:
            self._tx(body, Deadline.after(60.0))
        except md_errors.CommitUnknown:
            return                                      # recorded twice at worst; tried again with the next commit
        except (md_errors.Conflict, md_errors.ConnectionLost, DeadlineExceeded):
            return
        self.ctx.outbox.drop(len(items))

    def _planned_stop(self, group: Optional[Group]):
        if group is not None and not group.empty:
            self._commit(group)
        if self.engine is not None:
            with contextlib.suppress(Exception):
                self.engine.stop(Deadline.after(self.s.engine_stop_s))
            self.engine = None
        self.finished = True


__all__ = ["StreamComponent", "StreamContext", "EngineStart", "RestartEngine", "SlotFinding", "CP_ATTACHED",
           "SITE_SPILL"]
