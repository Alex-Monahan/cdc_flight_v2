"""A stream component over a scripted slot and a local destination (tier T0)."""
from __future__ import annotations

import collections
import contextlib
import threading
import time
import uuid

from cdc_v2 import apply as A
from cdc_v2 import md
from cdc_v2 import transform as T
from cdc_v2.deadline import Deadline
from cdc_v2.stream import seams as seams_mod
from cdc_v2.stream import state as st
from cdc_v2.stream.beat import AlertOutbox, BeatBoard
from cdc_v2.stream.component import StreamComponent, StreamContext
from cdc_v2.stream.settings import GroupSettings, StreamSettings
from cdc_v2.supervisor import Supervisor, SupervisorSettings
from cdc_v2.testing import destination as td

from fakes import HB, K, N, FakeSource, Model, ScriptedEngine, ScriptedSlot

FAST_STREAM = dict(frame_silence_s=5.0, marker_timeout_s=20.0, heartbeat_interval_s=1.0,
                   idle_position_interval_s=0.2, attach_timeout_s=10.0, unreachable_wait_s=20.0,
                   unreachable_pause_s=0.05, engine_stop_s=2.0, request_poll_s=0.2, alert_flush_s=0.3,
                   source_statement_s=5.0)
FAST_GROUP = dict(max_group_delay_s=0.05, max_group_attempts=6, retry_pause_s=0.05, retry_pause_max_s=0.2,
                  commit_timeout_base_s=20.0)


class Rig:
    def __init__(self, dest, tmp_path, catalog=None, tables=(K, N), stream=None, group=None, slot=None):
        self.dest = dest
        self.tmp = tmp_path
        self.epoch = dest.lead()
        self.slot = slot or ScriptedSlot(catalog)
        self.source = FakeSource(self.slot)
        self.stream_settings = StreamSettings(**{**FAST_STREAM, **(stream or {})})
        self.group_settings = GroupSettings(**{**FAST_GROUP, **(group or {})})
        spill = tmp_path / "spill"
        spill.mkdir(exist_ok=True)
        self.apply_settings = A.ApplySettings(local_temp_directory=str(spill))
        self.transform_settings = T.TransformSettings(temp_directory=str(spill / "t"), max_temp_directory_size="1GB")
        self.engines = []
        self.board = BeatBoard()
        self.outbox = AlertOutbox()
        self.engine_kw = {}
        self._first_start(tables)

    def lane(self, name="stream"):
        return self.dest.lane(name, self.epoch)

    def _first_start(self, tables):
        lane = self.lane("stream")
        with lane.transaction() as tx:
            st.first_row(tx, self.stream_settings.slot, self.slot.consistent_point, "test")
        with lane.transaction() as tx:
            for key in tables:
                src = self.slot.catalog[key]
                st.register_table(tx, src, T.plan(src, server_version_num=180006))
        lane.conn.close()

    def engine_factory(self, start):
        e = ScriptedEngine(self.slot, start, **self.engine_kw)
        self.engines.append(e)
        return e

    def context(self, **kw) -> StreamContext:
        return StreamContext(settings=self.stream_settings, group=self.group_settings, apply=self.apply_settings,
                             transform=self.transform_settings, epoch=self.epoch, run_id="run-" + uuid.uuid4().hex[:8],
                             lane_factory=lambda: self.lane("stream"), source=self.source,
                             engine_factory=self.engine_factory, board=self.board, outbox=self.outbox,
                             seams=seams_mod.standins(self.stream_settings.request_poll_s), heartbeat_key=HB, **kw)

    def applied(self):
        conn = self.dest.connect("check")
        try:
            return st.applied_position(conn, self.stream_settings.slot)
        finally:
            conn.close()

    @contextlib.contextmanager
    def running(self, ctx=None, supervisor=None):
        ctx = ctx or self.context()
        comp = StreamComponent(ctx)
        sup = Supervisor(supervisor or SupervisorSettings(restart_after_s=30, check_interval_s=0.1, join_timeout_s=5))
        sup.add("stream", comp.run, primary=True)
        sup.start()
        try:
            yield comp, sup
        finally:
            comp.finish_current_group(Deadline.after(5))
            sup.stop(Deadline.after(10))

    def wait_applied(self, target, timeout=20.0, sup=None):
        d = Deadline.after(timeout)
        while True:
            a = self.applied()
            if a is not None and a >= target:
                return a
            if sup is not None and sup.past_limit:
                raise AssertionError("supervisor past its limit: %s" % sup.past_limit)
            d.sleep(0.05, "applied %s, waiting for %s" % (a, target))

    def dest_state(self):
        keyed = {r[0]: r[1] for r in self.dest.run('SELECT id, v FROM "public"."k"')}
        keyless = collections.Counter(tuple(r) for r in self.dest.run('SELECT a, b FROM "public"."n"'))
        return keyed, keyless

    def model(self):
        return Model().apply(self.slot.txns)

    def check_equal(self):
        m = self.model()
        keyed, keyless = self.dest_state()
        assert keyed == m.keyed, (keyed, m.keyed)
        assert keyless == m.keyless, (keyless, m.keyless)
