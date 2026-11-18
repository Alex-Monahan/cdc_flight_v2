"""A lane on MotherDuck whose network path the parent test can black-hole (fix round, items 6 and 15; copied
from the second reviewer's `e3_child.py`). Run only by `test_md_fix_round.py`.

It reads commands from stdin and writes one JSON line per result to stdout. Short deadlines, so that the test
runs in minutes: statement 2 s, block 4 s, grace 0.5 s, connect 3 s (1 attempt), roll-back 1 s, interrupt 1 s,
and the default `max_waiting_calls` (2). Every line carries the counters of the layer (`md.waiting_calls()`,
`md.background_threads()`, `md.open_connections()`) and the number of threads of the process.

    python blackhole_child.py <database> <epoch>
"""
import json
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from cdc_v2 import crashpoints, md                       # noqa: E402
from cdc_v2.deadline import Deadline                      # noqa: E402

DB, EPOCH = sys.argv[1], int(sys.argv[2])
S = md.Settings(database=DB, connect_deadline_s=3.0, connect_attempts=1, connect_retry_pause_s=0.1,
                statement_deadline_s=2.0, transaction_deadline_s=4.0, rollback_deadline_s=1.0, grace_s=0.5,
                interrupt_deadline_s=1.0)
UPD = "UPDATE bh.hold SET v = v + 1 WHERE k = 1"
_hold_at = None
_orig_cp = crashpoints.crash_point


def counts():
    return {"threads": threading.active_count(), "md_background": len(md.background_threads()),
            "md_open_connections": md.open_connections(), "md_waiting_calls": md.waiting_calls()}


def out(**kw):
    kw.update(counts())
    print(json.dumps(kw, default=str, sort_keys=True), flush=True)


def wait_go():
    out(ev="READY")
    line = sys.stdin.readline().strip()
    if line != "GO":
        raise SystemExit("expected GO, got %r" % line)


def cp(name):
    global _hold_at
    if _hold_at is not None and name == _hold_at:
        _hold_at = None
        wait_go()
    return _orig_cp(name)


crashpoints.crash_point = cp          # pause at a named point of this process until the parent says GO


def timed(fn):
    t0 = time.monotonic()
    try:
        fn()
        e = None
    except Exception as exc:                              # noqa: BLE001 - reported to the parent
        e = exc
    return round(time.monotonic() - t0, 3), (type(e).__name__ if e is not None else None), \
        (str(e)[:160] if e is not None else None)


conn = md.connect("stream", S)
lane = md.Lane(conn, "stream", EPOCH)


def tx_update(pause_before_exit=False):
    with lane.transaction() as tx:
        tx.execute(UPD)
        if pause_before_exit:
            wait_go()


def main():
    global _hold_at
    for line in sys.stdin:
        cmd = line.split()
        if not cmd:
            continue
        what = cmd[0]
        if what == "warm":
            for _ in range(10):                       # a call given up in the phase before may still return
                s, e, m = timed(lambda: tx_update())
                if e != "ConnectionLost":
                    break
                time.sleep(1.0)
            out(ev="warm", seconds=s, error=e, gen=conn.generation)
        elif what == "statement":
            wait_go()
            s, e, m = timed(lambda: tx_update())
            out(ev="statement", seconds=s, error=e, message=m)
        elif what == "fence":
            s, e, m = timed(lambda: tx_update(pause_before_exit=True))
            out(ev="fence", seconds=s, error=e, message=m)
        elif what == "commit":
            _hold_at = "stream.commit_sent"
            s, e, m = timed(lambda: tx_update())
            out(ev="commit", seconds=s, error=e, message=m)
        elif what == "connect":
            wait_go()
            s, e, m = timed(lambda: md.connect("extra", S))
            out(ev="connect", seconds=s, error=e, message=m)
        elif what == "register":
            c2 = md.connect("reg", S)                     # connected while the path passes
            l2 = md.Lane(c2, "backfill.0", EPOCH)
            wait_go()

            def reg():
                with l2.transaction():
                    pass
            s, e, m = timed(reg)
            out(ev="register", seconds=s, error=e, message=m)
        elif what == "repeat":
            for i in range(int(cmd[1])):
                s, e, m = timed(lambda: tx_update())
                out(ev="repeat", i=i, seconds=s, error=e, message=m)
        elif what == "monitor":
            secs, step = float(cmd[1]), float(cmd[2])
            t0 = time.monotonic()
            while time.monotonic() - t0 < secs:
                out(ev="monitor", t=round(time.monotonic() - t0, 1))
                if cmd[3:] == ["until_idle"] and md.waiting_calls() == 0 and not md.background_threads():
                    break
                time.sleep(step)
            out(ev="monitor", t=round(time.monotonic() - t0, 1), last=True)
        elif what == "final":
            s, e, m = timed(lambda: tx_update())
            out(ev="final", seconds=s, error=e, gen=conn.generation)
        elif what == "quit":
            conn.close()
            out(ev="quit", idle=md.wait_idle(Deadline.after(10)))
            return


if __name__ == "__main__":
    main()
