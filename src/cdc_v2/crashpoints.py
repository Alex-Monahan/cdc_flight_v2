"""Named crash points (SPEC 1.7). Product code declares a point once and calls it at the place:

    from cdc_v2.crashpoints import crash_point, declare

    declare("stream.committed", "COMMIT of the stream group returned")
    ...
    crash_point("stream.committed")

The crash-point harness (`cdc_v2.testing.crash`) switches one point on through the environment of the process it
starts, kills the process there, restarts it without the switch and checks the result. When nothing is switched on,
`crash_point` returns after one test of a module global.

Environment contract
--------------------
The JVM engine implements the same contract in Java; keep both in step (`docs/testing/harness.md`). All variables are
read once, when this module is imported (or when `reconfigure` is called).

``CDC_V2_CRASH_POINT``
    Name of the one armed point. Unset or empty: no point is armed.
``CDC_V2_CRASH_HIT``
    Which hit of the armed point fires, counted per process from 1. ``N`` (a decimal integer >= 1; default ``1``),
    or ``seed:S:M``: hit = 1 + splitmix64(S) mod M, with S an unsigned 64-bit decimal integer and M >= 1.
    splitmix64 is the standard finaliser (constants below); splitmix64(0) = 0xE220A8397B1DCDAF.
``CDC_V2_CRASH_ACTION``
    What happens at the firing hit: ``kill`` (default; the process sends itself SIGKILL), ``stop`` (SIGSTOP to itself;
    after SIGCONT it continues normally), ``hang`` (the calling thread blocks for ever; signals still work),
    ``raise`` (raises :class:`CrashPointHit`, a ``BaseException`` so that ``except Exception`` does not swallow it).
    A point fires at most once per process.
``CDC_V2_CRASH_LOG``
    Optional hit log, an append-only file. Every call of every declared point appends one line, whether a point is
    armed or not. The line is written with one ``write`` on a file opened with ``O_APPEND`` before the action runs, so
    it survives SIGKILL and lines of concurrent processes do not interleave.
``CDC_V2_CRASH_TAG``
    Optional instance tag written into each log line (``[A-Za-z0-9_.-]``, at most 64 characters), so that several
    instances can share one log.

A hit log line is one JSON object followed by ``\\n``, keys in this order:
``{"point": "<name>", "hit": <n>, "pid": <pid>, "tag": "<tag or empty>", "t": <unix seconds, float>,
"fire": <true|false>, "action": "<action or empty>"}``. ``hit`` counts the calls of that point in that process.
``fire`` is true only on the line of the firing hit; ``action`` is set only there.

An invalid variable raises ``ValueError`` at import, so a typo in a harness never runs silently without a crash.
Calling an undeclared point raises :class:`UndeclaredCrashPoint` while the module is active (point armed or log set).

Point names are lower case, dotted, at least two parts: ``stream.committed``, ``backfill.chunk_committed``.
"""
import json
import os
import re
import signal
import sys
import threading
import time

ENV_POINT = "CDC_V2_CRASH_POINT"
ENV_HIT = "CDC_V2_CRASH_HIT"
ENV_ACTION = "CDC_V2_CRASH_ACTION"
ENV_LOG = "CDC_V2_CRASH_LOG"
ENV_TAG = "CDC_V2_CRASH_TAG"
ENV_VARS = (ENV_POINT, ENV_HIT, ENV_ACTION, ENV_LOG, ENV_TAG)

ACTIONS = ("kill", "stop", "hang", "raise")
NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$")
TAG_RE = re.compile(r"^[A-Za-z0-9_.-]{0,64}$")
_HIT_SEED_RE = re.compile(r"^seed:(\d+):(\d+)$")
_M64 = (1 << 64) - 1


class CrashPointHit(BaseException):
    """Raised at the firing hit when the action is ``raise``."""

    def __init__(self, point, hit):
        super().__init__("crash point %s fired at hit %d" % (point, hit))
        self.point = point
        self.hit = hit


class UndeclaredCrashPoint(RuntimeError):
    """crash_point() was called with a name that no module declared."""


class CrashPoint:
    __slots__ = ("name", "description", "module", "once")

    def __init__(self, name, description, module, once=False):
        self.name, self.description, self.module, self.once = name, description, module, once

    def as_dict(self):
        return {"name": self.name, "description": self.description, "module": self.module, "once": self.once}


_REGISTRY = {}
_REGISTRY_LOCK = threading.Lock()


def declare(name, description, once=False):
    """Register a crash point; returns the name. ``once`` says that a process reaches the point at most once (a
    start-up step), so the harness arms it only at hit 1. Declaring the same name again with the same description is
    a no-op (module reload); a different description raises ValueError."""
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ValueError("bad crash point name %r (lower case, dotted, two parts or more)" % (name,))
    if not isinstance(description, str) or not description.strip() or "\n" in description:
        raise ValueError("crash point %s needs a one-line description" % name)
    frame = sys._getframe(1)
    module = frame.f_globals.get("__name__", "?")
    with _REGISTRY_LOCK:
        old = _REGISTRY.get(name)
        if old is not None:
            if old.description != description:
                raise ValueError("crash point %s declared twice with different descriptions (%s, %s)"
                                 % (name, old.module, module))
            return name
        _REGISTRY[name] = CrashPoint(name, description, module, bool(once))
    return name


def registry(prefix=None):
    """Declared points, sorted by name; ``prefix`` keeps names that start with it (e.g. ``"stream."``)."""
    with _REGISTRY_LOCK:
        pts = sorted(_REGISTRY.values(), key=lambda p: p.name)
    return [p for p in pts if prefix is None or p.name.startswith(prefix)]


def declared(name):
    return name in _REGISTRY


def get(name):
    """The declared CrashPoint, or None."""
    return _REGISTRY.get(name)


def splitmix64(x):
    """The splitmix64 finaliser on an unsigned 64-bit integer. Portable: the JVM uses the same constants."""
    x = (x + 0x9E3779B97F4A7C15) & _M64
    z = x
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _M64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _M64
    return z ^ (z >> 31)


def hit_from_seed(seed, max_hit):
    """The hit that ``CDC_V2_CRASH_HIT=seed:<seed>:<max_hit>`` selects (1 .. max_hit)."""
    if max_hit < 1:
        raise ValueError("max hit must be >= 1")
    return 1 + splitmix64(seed & _M64) % max_hit


def parse_hit(text):
    text = (text or "").strip()
    if not text:
        return 1
    m = _HIT_SEED_RE.match(text)
    if m:
        seed, mx = int(m.group(1)), int(m.group(2))
        if seed > _M64 or mx < 1:
            raise ValueError("%s=%r: seed must fit 64 bits and M must be >= 1" % (ENV_HIT, text))
        return hit_from_seed(seed, mx)
    if not text.isdigit() or int(text) < 1:
        raise ValueError("%s=%r: expected N >= 1 or seed:S:M" % (ENV_HIT, text))
    return int(text)


class _State:
    def __init__(self, point, hit, action, log_path, tag):
        self.point, self.target, self.action, self.log_path, self.tag = point, hit, action, log_path, tag
        self.counts = {}
        self.fired = False
        self.lock = threading.Lock()
        self.log_fd = None
        if log_path:
            self.log_fd = os.open(log_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)

    def hit(self, name):
        if name not in _REGISTRY:
            raise UndeclaredCrashPoint("crash point %r is not declared" % (name,))
        with self.lock:
            n = self.counts.get(name, 0) + 1
            self.counts[name] = n
            fire = (not self.fired) and name == self.point and n == self.target
            if fire:
                self.fired = True
            if self.log_fd is not None:
                line = json.dumps({"point": name, "hit": n, "pid": os.getpid(), "tag": self.tag,
                                   "t": round(time.time(), 6), "fire": fire,
                                   "action": self.action if fire else ""}, separators=(",", ":")) + "\n"
                os.write(self.log_fd, line.encode())
        if fire:
            _fire(self.action, name, n)


def _fire(action, name, n):
    if action == "kill":
        os.kill(os.getpid(), signal.SIGKILL)
        while True:                      # SIGKILL is delivered asynchronously; never run past the point
            time.sleep(1)
    elif action == "stop":
        os.kill(os.getpid(), signal.SIGSTOP)
    elif action == "hang":
        while True:
            time.sleep(3600)
    elif action == "raise":
        raise CrashPointHit(name, n)


_state = None


def reconfigure(environ=None):
    """(Re)read the environment contract. Returns True when the module is active. Used at import and by tests."""
    global _state
    env = os.environ if environ is None else environ
    point = (env.get(ENV_POINT) or "").strip()
    log_path = (env.get(ENV_LOG) or "").strip()
    action = (env.get(ENV_ACTION) or "kill").strip() or "kill"
    tag = (env.get(ENV_TAG) or "").strip()
    if point and not NAME_RE.match(point):
        raise ValueError("%s=%r is not a crash point name" % (ENV_POINT, point))
    if action not in ACTIONS:
        raise ValueError("%s=%r: expected one of %s" % (ENV_ACTION, action, ", ".join(ACTIONS)))
    if not TAG_RE.match(tag):
        raise ValueError("%s=%r: [A-Za-z0-9_.-], at most 64 characters" % (ENV_TAG, tag))
    hit = parse_hit(env.get(ENV_HIT))
    old = _state
    _state = _State(point, hit, action, log_path, tag) if (point or log_path) else None
    if old is not None and old.log_fd is not None:
        os.close(old.log_fd)
    return _state is not None


def active():
    return _state is not None


def crash_point(name):
    """Mark a crash point. Returns at once unless the environment switched the harness on."""
    if _state is None:
        return
    _state.hit(name)


def env_for(point=None, hit=1, action="kill", log=None, tag=None):
    """Environment variables that switch on ``point`` in a child process (all five keys; empty means off)."""
    if action not in ACTIONS:
        raise ValueError("unknown action %r" % (action,))
    return {ENV_POINT: point or "", ENV_HIT: str(hit), ENV_ACTION: action, ENV_LOG: log or "", ENV_TAG: tag or ""}


def read_log(path):
    """Parse a hit log; a torn last line (a process killed while writing) is skipped."""
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if not line.endswith("\n"):
                    break
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
    except FileNotFoundError:
        pass
    return out


reconfigure()


def _main(argv=None):
    import argparse
    import importlib

    ap = argparse.ArgumentParser(prog="python -m cdc_v2.crashpoints",
                                 description="List declared crash points as JSON.")
    ap.add_argument("--import", dest="modules", action="append", default=[], metavar="MODULE",
                    help="import this module first (it declares points); repeatable")
    ap.add_argument("--prefix")
    a = ap.parse_args(argv)
    cp = importlib.import_module("cdc_v2.crashpoints")   # under -m this file is __main__, a second module object
    for m in a.modules:
        importlib.import_module(m)
    print(json.dumps([p.as_dict() for p in cp.registry(a.prefix)], indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(_main())
