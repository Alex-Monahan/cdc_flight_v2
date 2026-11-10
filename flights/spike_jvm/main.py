"""Bootstrap prototype (SPEC R4, PLAN §2 A 1): clone this public repository at a pinned commit and run it.

Config (environment, from the Flight config; overridable per run):
  CDC_V2_COMMIT    full 40-hex commit to check out (required)
  CDC_V2_REPO      repository URL (default: the public cdc_flight_v2 repository; cloned anonymously)
  CDC_V2_ENTRY     script inside the clone to run (default: spikes/jvm_on_flight/driver.py)
  CDC_V2_DEADLINE  hard limit in seconds for the child (default 1800); the whole process group is killed after it

Every step prints a `BOOT <step> <seconds>` line. The child gets the clone's src/ on PYTHONPATH and
CDC_V2_RUN_T0 (this script's first-line wall clock) so it can report time since run start. It runs in its own
process group; the bootstrap waits for it, kills the group at the deadline or when the bootstrap is told to stop,
and exits with the child's exit status. Kept minimal on purpose: it becomes the last-resort watchdog (SPEC 4.2).
"""
import time

T0 = time.time()
print("BOOT first_line %.3f" % T0, flush=True)

import os  # noqa: E402
import re  # noqa: E402
import signal  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402

REPO = os.environ.get("CDC_V2_REPO", "https://github.com/Alex-Monahan/cdc_flight_v2")
COMMIT = os.environ.get("CDC_V2_COMMIT", "")
ENTRY = os.environ.get("CDC_V2_ENTRY", "spikes/jvm_on_flight/driver.py")
DEADLINE = float(os.environ.get("CDC_V2_DEADLINE", "1800"))
CLONE = "/tmp/cdc_v2_src"


def step(name, t):
    print("BOOT %s %.3f" % (name, time.time() - t), flush=True)


def git(*args):
    subprocess.run(["git", "-c", "advice.detachedHead=false", *args], cwd=CLONE, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=120)


def main():
    if not re.fullmatch(r"[0-9a-f]{40}", COMMIT):
        print("BOOT error CDC_V2_COMMIT must be a full commit hash", flush=True)
        return 2
    t = time.time()
    os.makedirs(CLONE, exist_ok=True)
    git("init", "-q")
    git("remote", "add", "origin", REPO)
    git("fetch", "-q", "--depth", "1", "origin", COMMIT)
    git("checkout", "-q", "FETCH_HEAD")
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=CLONE, capture_output=True, text=True,
                          check=True).stdout.strip()
    if head != COMMIT:
        print("BOOT error checked out %s, wanted %s" % (head, COMMIT), flush=True)
        return 2
    step("clone", t)
    print("BOOT commit %s" % head, flush=True)

    env = dict(os.environ, CDC_V2_RUN_T0="%.3f" % T0, CDC_V2_SRC=CLONE,
               PYTHONPATH=os.path.join(CLONE, "src") + os.pathsep + os.environ.get("PYTHONPATH", ""))
    child = subprocess.Popen([sys.executable, os.path.join(CLONE, ENTRY)], cwd=CLONE, env=env,
                             start_new_session=True)

    def kill_group(*_):
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    signal.signal(signal.SIGTERM, lambda *a: (kill_group(), sys.exit(143)))
    try:
        rc = child.wait(timeout=DEADLINE)
    except subprocess.TimeoutExpired:
        print("BOOT deadline %.0f s reached: killing the process group" % DEADLINE, flush=True)
        kill_group()
        rc = 124
    step("child_exit", T0)
    print("BOOT exit %d" % rc, flush=True)
    return rc if rc >= 0 else 128 - rc


if __name__ == "__main__":
    sys.exit(main())
