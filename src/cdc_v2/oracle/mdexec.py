"""Deadlines for MotherDuck (DuckDB client) calls made by the oracle and the T1 harness (C0, SPEC 4.5).

The DuckDB client has no statement timeout. `guarded(conn, deadline)` arms a timer that calls `conn.interrupt()`
when the deadline passes and turns the resulting interruption into `DeadlineExceeded`; `execute` runs one
statement under it. `connect` opens a connection in a helper thread and stops waiting at the deadline (a connection
attempt cannot be interrupted; the helper thread is a daemon and closes its late connection).
"""
from __future__ import annotations

import contextlib
import threading
from typing import Any, Callable, Iterator, Optional

from cdc_v2.deadline import Deadline, DeadlineExceeded


@contextlib.contextmanager
def guarded(conn, deadline: Deadline, what: str) -> Iterator[None]:
    """Everything run on `conn` inside the block is interrupted when `deadline` passes (DeadlineExceeded)."""
    deadline.check(what)
    fired = threading.Event()

    def stop():
        fired.set()
        try:
            conn.interrupt()
        except Exception:                          # noqa: BLE001 - the statement may have just finished
            pass

    timer = threading.Timer(deadline.remaining(), stop)
    timer.daemon = True
    timer.start()
    try:
        yield
    except Exception as e:
        if fired.is_set():
            raise DeadlineExceeded("MotherDuck call past its deadline: %s" % what) from e
        raise
    finally:
        timer.cancel()


def execute(conn, sql: str, params: Optional[list] = None, *, deadline: Deadline, what: str = "",
            fetch: Optional[str] = "all", consume: Optional[Callable[[list], None]] = None,
            batch: int = 100_000) -> Any:
    """Run one statement within `deadline`. fetch: 'all' (rows), 'one' (a row or None), None (nothing), or
    'stream' (rows handed to consume() in batches of `batch`)."""
    with guarded(conn, deadline, what or sql[:80]):
        cur = conn.execute(sql, params) if params is not None else conn.execute(sql)
        if fetch == "all":
            return cur.fetchall()
        if fetch == "one":
            return cur.fetchone()
        if fetch == "stream":
            while True:
                rows = cur.fetchmany(batch)
                if not rows:
                    return None
                consume(rows)
        return None


def connect(target: str, *, deadline: Deadline, config: Optional[dict] = None):
    """duckdb.connect(target) within `deadline`."""
    import duckdb
    out: dict = {}
    done = threading.Event()
    abandoned = threading.Event()

    def run():
        try:
            c = duckdb.connect(target, config=config or {})
            if abandoned.is_set():
                c.close()
            else:
                out["conn"] = c
        except BaseException as e:                 # noqa: BLE001 - re-raised in the caller
            out["error"] = e
        finally:
            done.set()

    threading.Thread(target=run, daemon=True).start()
    if not done.wait(deadline.remaining()):
        abandoned.set()
        if done.is_set() and "conn" in out:        # it arrived just now: close it, the caller has given up
            out["conn"].close()
        raise DeadlineExceeded("connecting to MotherDuck")
    if "error" in out:
        raise out["error"]
    return out["conn"]
