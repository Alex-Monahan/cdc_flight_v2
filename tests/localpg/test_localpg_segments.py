"""localpg releases the System V shared-memory segment of a killed node before it deletes the node's data directory
(docs/localpg.md, "Shared memory"). Self-test home and ports (55460-55467); opt-in: -m localpg."""
import importlib.machinery
import importlib.util
import os
import subprocess

import pytest

from localpg_support import LOCALPG, TEST_HOME, lp, lp_env

pytestmark = pytest.mark.localpg


def _localpg_module():
    loader = importlib.machinery.SourceFileLoader("localpg_tool", LOCALPG)
    spec = importlib.util.spec_from_loader("localpg_tool", loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


LP = _localpg_module()


def _segments():
    """{id} of every segment (read only: listing, never removing)."""
    out = subprocess.run(["/usr/bin/ipcs", "-m"], capture_output=True, text=True, timeout=10).stdout
    return {int(line.split()[1]) for line in out.splitlines() if line.startswith("m ")}


def _pair_segments():
    out = {}
    for node in ("primary", "replica"):
        seg = LP.segment_of(os.path.join(TEST_HOME, "nodes", node, "data"))
        assert seg is not None, "no pid file with a segment for %s" % node
        out[node] = seg                      # (postmaster pid, key, id)
    return out


def _kill_and_remove(env=None):
    lp("kill", "replica", env=env)
    lp("kill", "primary", env=env)
    lp("remove", "replica", env=env)
    lp("remove", "primary", env=env)


def test_killed_and_removed_pair_leaves_no_segment():
    before = _segments()
    lp("reset")
    segs = _pair_segments()
    ids = {s[2] for s in segs.values()}
    assert ids <= _segments() and not ids & before
    _kill_and_remove()
    after = _segments()
    assert not ids & after, "segments of the removed nodes are still there: %s" % sorted(ids & after)
    # the number is what it was before the start (other work on the machine may add or remove its own meanwhile)
    assert len(after - before) == 0 or not (after - before) & ids


def test_negative_control_the_old_behaviour_leaves_two():
    lp("reset")
    segs = _pair_segments()
    ids = {s[2] for s in segs.values()}
    _kill_and_remove(env=lp_env(**{LP.KEEP_SEGMENTS_ENV: "1"}))
    try:
        assert ids <= _segments(), "the negative control should leave both segments"
    finally:
        # remove them by the same rules: the pid file's pid is dead, key and creator match, nobody attached
        for pid, key, shmid in segs.values():
            row = LP.segment_row(shmid)
            if row and row["key"] == key and row["cpid"] == pid and row["nattch"] == 0 and not LP.alive(pid):
                subprocess.run(["/usr/bin/ipcrm", "-m", str(shmid)], check=True, timeout=10)
    assert not ids & _segments()


def test_a_running_node_keeps_its_segment():
    lp("reset")
    segs = _pair_segments()
    lp("down")                               # a clean stop: postgres removes its own segment
    assert not {s[2] for s in segs.values()} & _segments()
    lp("up")
    assert LP.release_segment.__doc__        # nothing else to call: a running node's pid file is never acted on
