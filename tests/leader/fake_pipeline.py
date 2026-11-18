"""A pipeline that only ticks, for the tests of the bootstrap's watch (no database).

    python fake_pipeline.py <mode> [<argument>]      # the tick descriptor is CDC_V2_TICK_FD (contract C12)

Modes: ``exit <code>`` (tick 10 times, then exit), ``tick`` (tick for ever; the test freezes or kills it),
``close`` (tick 3 times, close the pipe, live on), ``full`` (fill the pipe until it refuses a byte, then tick 10
times and exit 7), ``sigkill`` and ``sigterm`` (tick 3 times, then die by that signal), ``silent`` (never tick).
"""
import os
import signal
import sys
import time

fd = int(os.environ["CDC_V2_TICK_FD"])
os.set_blocking(fd, False)
mode = sys.argv[1]


def tick(n, pause=0.05):
    for _ in range(n):
        try:
            os.write(fd, b"t")
        except BlockingIOError:
            pass
        time.sleep(pause)


if mode == "exit":
    tick(10)
    sys.exit(int(sys.argv[2]))
elif mode == "tick":
    while True:
        tick(1)
elif mode == "close":
    tick(3)
    os.close(fd)
    while True:
        time.sleep(1)
elif mode == "full":
    written = 0
    while True:
        try:
            written += os.write(fd, b"t" * 4096)
        except BlockingIOError:
            break
    print("filled %d bytes" % written, flush=True)
    tick(10)
    sys.exit(7)
elif mode in ("sigkill", "sigterm"):
    tick(3)
    os.kill(os.getpid(), signal.SIGKILL if mode == "sigkill" else signal.SIGTERM)
    time.sleep(10)
elif mode == "silent":
    while True:
        time.sleep(1)
