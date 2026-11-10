"""procs.signal_group: EPERM from killpg means "gone" only when the leader is reaped and the group has no living
member; otherwise it is raised as before (hermetic; killpg is replaced, no signal is sent)."""
import os
import sys

import pytest

from cdc_v2.testing import procs


def _eperm(pgid, sig):
    raise PermissionError(1, "Operation not permitted")


def test_eperm_for_a_group_that_is_gone_is_not_an_error(monkeypatch, tmp_path):
    p = procs.start([sys.executable, "-c", "pass"], name="short", log_path=str(tmp_path / "l.log"))
    p.wait(10)
    monkeypatch.setattr(os, "killpg", _eperm)
    assert p.signal_group(9) is False and p.group_gone


def test_eperm_for_a_living_group_is_raised(monkeypatch, tmp_path):
    p = procs.start([sys.executable, "-c", "import time; time.sleep(30)"], name="long",
                    log_path=str(tmp_path / "l.log"))
    try:
        monkeypatch.setattr(os, "killpg", _eperm)
        with pytest.raises(PermissionError):
            p.signal_group(0)
    finally:
        monkeypatch.undo()
        p.kill_and_wait(10)
