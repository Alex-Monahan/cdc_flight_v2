"""Tier T0: the bootstrap's watch (design 5, step 5) with real child processes and a real pipe; no database.

The watch must not be able to hang itself: it reads ticks with a timeout, shares no lock with a thread that can
block, and ends the run when no tick came for the hard-exit age, also when the pipeline is frozen (SIGSTOP), has
closed its end of the pipe without exiting, or fills the pipe. A pipeline that exits is passed on.
"""
import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from conftest import BOOTSTRAP, HERE

FAKE = os.path.join(HERE, "fake_pipeline.py")
AGE = 0.6

pytestmark = pytest.mark.timeout(60)


def start(mode, *args):
    r, w = os.pipe()
    os.set_blocking(r, False)
    p = subprocess.Popen([sys.executable, FAKE, mode, *map(str, args)], env=dict(os.environ, CDC_V2_TICK_FD=str(w)),
                         pass_fds=(w,), start_new_session=True, stdout=subprocess.DEVNULL)
    os.close(w)
    return p, r


def finish(p, r):
    try:
        os.killpg(p.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    p.wait(10)
    os.close(r)


@pytest.mark.parametrize("code", [0, 3, 10])
def test_a_pipeline_that_exits_is_passed_on(boot, code):
    p, r = start("exit", code)
    firsts = []
    try:
        rc = boot.watch_loop(p, r, AGE, None, lambda: firsts.append(time.monotonic()), 0.05)
    finally:
        finish(p, r)
    assert rc == code and len(firsts) == 1


def test_a_frozen_pipeline_ends_the_run_after_the_hard_exit_age(boot):
    p, r = start("tick")
    try:
        time.sleep(0.3)
        os.kill(p.pid, signal.SIGSTOP)
        t = time.monotonic()
        rc = boot.watch_loop(p, r, AGE, None, None, 0.05)
        took = time.monotonic() - t
    finally:
        finish(p, r)
    assert rc is None
    assert AGE - 0.1 <= took <= AGE + 0.5, took


def test_a_closed_pipe_without_an_exit_ends_the_run(boot):
    p, r = start("close")
    try:
        t = time.monotonic()
        rc = boot.watch_loop(p, r, AGE, None, None, 0.05)
        took = time.monotonic() - t
    finally:
        finish(p, r)
    assert rc is None and took < AGE + 1.0


def test_a_full_pipe_does_not_stop_the_watch(boot):
    p, r = start("full")
    try:
        rc = boot.watch_loop(p, r, AGE, None, None, 0.05)
    finally:
        finish(p, r)
    assert rc == 7


def test_a_pipeline_that_never_ticks_is_decided_by_the_start_up_rule(boot):
    p, r = start("silent")
    calls = []

    def before(now):
        calls.append(now)
        if len(calls) >= 5:
            raise RuntimeError("start-up deadline")      # stands for the bootstrap's hard exit
    try:
        with pytest.raises(RuntimeError):
            boot.watch_loop(p, r, AGE, before, None, 0.05)
    finally:
        finish(p, r)
    assert len(calls) == 5


DRIVER = textwrap.dedent("""
    import importlib.util, os, subprocess, sys
    spec = importlib.util.spec_from_file_location("b", %r)
    b = importlib.util.module_from_spec(spec); spec.loader.exec_module(b)
    r, w = os.pipe(); os.set_blocking(r, False)
    p = subprocess.Popen([sys.executable, %r] + sys.argv[1:], env=dict(os.environ, CDC_V2_TICK_FD=str(w)),
                         pass_fds=(w,), start_new_session=True, stdout=subprocess.DEVNULL)
    os.close(w)
    print("PIPELINE", p.pid, flush=True)
    rc = b.watch_loop(p, r, 0.6, None, None, 0.05)
    if rc is None:
        b.pass_on(p, None)
    b.pass_on(p, rc)
""") % (BOOTSTRAP, FAKE)


@pytest.mark.parametrize("mode,expect", [
    (("exit", "0"), 0), (("exit", "4"), 4), (("exit", "10"), 10),
    (("sigterm",), 128 + signal.SIGTERM), (("sigkill",), -signal.SIGKILL), (("tick",), "frozen"),
])
def test_the_bootstrap_exits_with_the_pipelines_status(mode, expect):
    """`pass_on` in a process of its own: an exit code is passed on; a death by SIGKILL is passed on as a death
    by SIGKILL (as the platform's own kill looks); another signal as 128 + its number; a frozen pipeline is killed
    with its whole group and the bootstrap exits 1."""
    d = subprocess.Popen([sys.executable, "-c", DRIVER, *mode], stdout=subprocess.PIPE, text=True)
    pid = int(d.stdout.readline().split()[1])
    if expect == "frozen":
        time.sleep(0.3)
        os.kill(pid, signal.SIGSTOP)
    rc = d.wait(20)
    d.stdout.close()
    if expect == "frozen":
        assert rc == 1
        time.sleep(0.2)
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)                               # the frozen pipeline was killed with its group
    else:
        assert rc == expect


def test_the_watch_shares_no_lock_and_reads_with_a_timeout(boot):
    """Structure: the watch uses `select` with a timeout and non-blocking reads, and takes no lock."""
    import inspect
    src = inspect.getsource(boot.watch_loop)
    assert "select.select" in src and "BlockingIOError" in src
    for word in ("Lock", "acquire", ".join(", "Event", "Queue"):
        assert word not in src, word
