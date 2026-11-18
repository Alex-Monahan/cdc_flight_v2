"""Shared by the leadership tests: settings of the bootstrap for a local database, a stand-in for its exit, and
running the take-over at a crash point of a lane (the hook of package D's fence campaign)."""
import uuid

from cdc_v2.md.connection import _interrupt_local

from conftest import ROOT

#: Timings in tenths of seconds (T0). The order rules hold: 0.3 <= 1.0 / 3, 1.0 < 2.0.
FAST = {"CDC_V2_HEARTBEAT_INTERVAL_S": "0.3", "CDC_V2_STALE_AGE_S": "1.0", "CDC_V2_HARD_EXIT_AGE_S": "2.0",
        "CDC_V2_STARTUP_TIMEOUT_S": "10", "CDC_V2_TAKEOVER_TIMEOUT_S": "5", "CDC_V2_TAKEOVER_PAUSE_S": "0.05",
        "CDC_V2_LATE_COMMIT_WAIT_S": "0.3", "CDC_V2_GUARD_TIMEOUT_S": "5", "CDC_V2_STATEMENT_TIMEOUT_S": "5",
        "CDC_V2_GRACE_S": "0.3"}
FLIGHT = "11111111-2222-3333-4444-555555555555"
OTHER_FLIGHT = "99999999-8888-7777-6666-555555555555"


def boot_env(database, flight_id=FLIGHT, run_number=1, run_id=None, **extra):
    env = dict(FAST)
    env.update({"CDC_V2_DATABASE": database, "MOTHERDUCK_FLIGHT_ID": flight_id,
                "MOTHERDUCK_FLIGHT_RUN_NUMBER": str(run_number),
                "MOTHERDUCK_FLIGHT_RUN_ID": run_id or str(uuid.uuid4()), "CDC_V2_SOURCE_DIR": ROOT})
    env.update({k: str(v) for k, v in extra.items()})
    return env


def local_interrupt(ids):
    """The local stand-in of `md_interrupt_server_connection` for connections of this process (package D)."""
    return sum(1 for i in ids if _interrupt_local(i))


class Exited(Exception):
    def __init__(self, code, fields):
        super().__init__("exit %s %r" % (code, fields))
        self.code, self.fields = code, fields


def fake_finish(code, **fields):
    raise Exited(code, fields)


def new_boot(boot, database, interrupt=True, **kw):
    s = boot.Settings(boot_env(database, **kw))
    return boot.Boot(s, interrupt_ids=local_interrupt if interrupt else None)


class Hooks:
    """Run a function at a named crash point of this process, once, in the thread that reaches it (the lane's
    crash points are calls of `cdc_v2.crashpoints`; the same device as package D's fence campaign)."""

    def __init__(self):
        self._orig = None
        self._fns = {}

    def __enter__(self):
        from cdc_v2 import crashpoints
        self._cp = crashpoints
        self._orig = crashpoints.crash_point

        def hooked(name):
            fn = self._fns.pop(name, None)
            if fn is not None:
                fn()
            return self._orig(name)

        crashpoints.crash_point = hooked
        return self

    def __exit__(self, *exc):
        self._cp.crash_point = self._orig
        self._fns.clear()

    def at(self, name, fn):
        self._fns[name] = fn


def make_stale(dest, epoch=None):
    """The heartbeat an hour old (the leader is not healthy), with a plain client connection (tests only)."""
    dest.run("UPDATE _cdc.heartbeat SET beat_at = uuid_extract_timestamp(uuidv7()) - INTERVAL 1 HOUR")
