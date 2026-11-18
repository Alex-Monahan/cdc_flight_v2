"""Task S, default run: one walk over every declared crash point of the stream path. Crash there (action `raise`, in
the process: the supervisor restarts the component, the scripted slot re-sends what was not confirmed), start
again, and the destination equals the run without a crash, with a table without a key in the workload so that a
duplicate shows."""
import json
import os

import pytest

from cdc_v2 import crashpoints, md
from cdc_v2.deadline import Deadline
from cdc_v2.stream import state as st
from cdc_v2.testing import destination as td

import scenarios
from fakes import K, N, ScriptedSlot
from rig import Rig

# the stream path: the points of design 3, the lane's points of package D, and every durable step of package S
MAIN_POINTS = ["stream.started", "stream.attached", "stream.staged", "stream.begun", "stream.applied",
               "stream.position_written", "stream.fenced", "stream.commit_sent", "stream.committed",
               "stream.ack_sent", "stream.chunk_committed"]


def _run_scenario(tmp_path, point=None, hit=1, large=True, seed=7):
    """The workload with the point armed at `hit` (after the set-up, whose lane transactions pass the lane's
    points too). Returns the rig, the fired hits and the supervisor's restarts of the stream component."""
    log = tmp_path / ("hits_%s_%d.log" % (point or "none", hit))
    try:
        with td.local_destination() as dest:
            rig = Rig(dest, tmp_path, group=dict(max_group_bytes=64 << 10, stage_chunk_bytes=16 << 10)
                      if large else None)
            last = scenarios.main(rig.slot, seed=seed, txns=30, large_rows=400 if large else 0, big_text=200)
            crashpoints.reconfigure({crashpoints.ENV_POINT: point or "", crashpoints.ENV_HIT: str(hit),
                                     crashpoints.ENV_ACTION: "raise", crashpoints.ENV_LOG: str(log)})
            with rig.running() as (comp, sup):
                rig.wait_applied(last.commit_lsn, timeout=60, sup=sup)
                restarts = [e for e in sup.events if e[2] == "restart"]
            crashpoints.reconfigure({})
            rig.check_equal()
            hits = crashpoints.read_log(log)
            fired = [h for h in hits if h["fire"]]
            rig.hits = hits
            return rig, fired, restarts
    finally:
        crashpoints.reconfigure({})


def test_no_crash_baseline(tmp_path):
    rig, fired, restarts = _run_scenario(tmp_path)
    assert rig.engines and not fired and not restarts
    reached = {h["point"] for h in rig.hits}
    assert set(MAIN_POINTS) <= reached, sorted(set(MAIN_POINTS) - reached)
    assert sum(1 for h in rig.hits if h["point"] == "stream.chunk_committed") >= 2, "the large transaction is staged"


@pytest.mark.parametrize("point", MAIN_POINTS)
def test_crash_at_every_point_of_the_stream_path(tmp_path, point):
    hits = (1, 2) if point not in ("stream.started", "stream.attached") else (1,)
    for hit in hits:
        rig, fired, restarts = _run_scenario(tmp_path, point, hit)
        assert len(fired) == 1 and fired[0]["point"] == point, "crash point %s hit %d not reached" % (point, hit)
        # the component was restarted after the crash and reached the end (checked by _run_scenario)
        assert restarts, "the supervisor restarted the stream component"


def test_every_stream_point_is_walked():
    """A declared point that no walk reaches is a failure of the suite (C1). The walk of this file covers the main
    path; the first start, the quarantine and the last line have their own walks (test_stream_paths.py)."""
    import cdc_v2.stream.component  # noqa: F401 - declares its points
    import cdc_v2.stream.source  # noqa: F401
    import cdc_v2.stream.staging  # noqa: F401
    declared = {p.name for p in crashpoints.registry("stream.")}
    from test_stream_paths import PATH_POINTS
    walked = set(MAIN_POINTS) | set(PATH_POINTS)
    assert declared <= walked, sorted(declared - walked)
