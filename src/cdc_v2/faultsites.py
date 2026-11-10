"""Run-time fault sites: errors the TCP proxy cannot produce (the destination is a cloud service reached through its
own client). Product code declares a site once and calls it where the operation happens:

    from cdc_v2.faultsites import declare_site, fault_site

    declare_site("md.commit", "COMMIT of a MotherDuck data transaction")
    ...
    fault_site("md.commit")
    conn.execute("COMMIT")

A test tells a running process, through a control file, to raise an exception at a site, to delay there or to block
there, for a number of hits or a time window. `cdc_v2.testing.faults.FaultControl` writes that file. When the
environment names no control file, `fault_site` returns after one test of a module global.

Environment contract (read at import or by `reconfigure`)
--------------------------------------------------------
``CDC_V2_FAULT_FILE``  control file (JSON, below). Unset or empty: off. The file need not exist yet.
``CDC_V2_FAULT_LOG``   optional append-only log; one JSON line per fired fault (same writing rules as the crash-point
                       hit log: one ``write`` with ``O_APPEND``, written before the action runs).
``CDC_V2_FAULT_TAG``   optional instance tag; a rule with ``"tag"`` applies only to processes with that tag.

The file is re-read only when it changes: every call compares ``(inode, mtime_ns, size)`` from one ``stat`` with the
values of the last read. Writers replace the file atomically (write a temporary file, then rename).

Control file::

    {"rules": [{"id": "r1", "site": "md.commit", "action": "raise",
                "exception": "ConnectionError", "message": "injected",
                "skip": 0, "hits": 2, "start": 1731100000.0, "end": 1731100060.0, "seconds": 1.5, "tag": ""}]}

``id``        required, unique. Hit counters are kept per id across re-reads; a new id starts from zero.
``site``      a site name or an ``fnmatch`` pattern (``md.*``).
``action``    ``raise`` | ``delay`` (sleep ``seconds``, then continue) | ``block`` (wait while the rule is still in
              the file and its window is open, re-checking every 50 ms; at most ``seconds`` if given).
``exception`` for ``raise``: a built-in exception name (``ConnectionError``, ``TimeoutError``, ``OSError`` ...) or
              ``module.Class`` of a module the process has already imported (``duckdb.IOException``); never imports.
              Default :class:`FaultInjected`. The raised exception carries ``cdc_v2_fault = "<rule id>"``.
``skip``      matching hits inside the window that pass before the rule fires (default 0).
``hits``      how many hits fire after ``skip`` (default: no limit).
``start``/``end``  optional window in Unix seconds; hits outside it neither fire nor count.

The first rule (in file order) that fires decides; one call applies at most one rule.
"""
import builtins
import fnmatch
import json
import os
import re
import sys
import threading
import time

ENV_FILE = "CDC_V2_FAULT_FILE"
ENV_LOG = "CDC_V2_FAULT_LOG"
ENV_TAG = "CDC_V2_FAULT_TAG"
ENV_VARS = (ENV_FILE, ENV_LOG, ENV_TAG)
ACTIONS = ("raise", "delay", "block")
NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$")
BLOCK_POLL_S = 0.05


class FaultInjected(RuntimeError):
    """Default exception of a ``raise`` rule."""


class UndeclaredFaultSite(RuntimeError):
    pass


class FaultFileError(ValueError):
    pass


_SITES = {}
_SITES_LOCK = threading.Lock()


def declare_site(name, description):
    """Register a fault site; returns the name (same rules as crash points)."""
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ValueError("bad fault site name %r" % (name,))
    if not isinstance(description, str) or not description.strip() or "\n" in description:
        raise ValueError("fault site %s needs a one-line description" % name)
    module = sys._getframe(1).f_globals.get("__name__", "?")
    with _SITES_LOCK:
        old = _SITES.get(name)
        if old is not None and old[0] != description:
            raise ValueError("fault site %s declared twice with different descriptions" % name)
        _SITES.setdefault(name, (description, module))
    return name


def sites():
    with _SITES_LOCK:
        return [{"name": n, "description": d, "module": m} for n, (d, m) in sorted(_SITES.items())]


def resolve_exception(name):
    """Exception class for a rule; only built-ins and classes of modules that are already imported."""
    if not name:
        return FaultInjected
    if "." not in name:
        cls = getattr(builtins, name, None)
    else:
        mod_name, _, attr = name.rpartition(".")
        mod = sys.modules.get(mod_name)
        cls = getattr(mod, attr, None) if mod is not None else None
    if not (isinstance(cls, type) and issubclass(cls, Exception)):
        raise FaultFileError("exception %r is not a built-in or an already imported Exception class" % (name,))
    return cls


def validate_rules(doc):
    """Check a control document; returns the rule list. Used by the reader and by the test-side writer."""
    if not isinstance(doc, dict) or not isinstance(doc.get("rules", []), list):
        raise FaultFileError("control file must be an object with a 'rules' list")
    seen = set()
    rules = doc.get("rules", [])
    for r in rules:
        if not isinstance(r, dict):
            raise FaultFileError("rule is not an object: %r" % (r,))
        rid = r.get("id")
        if not isinstance(rid, str) or not rid or rid in seen:
            raise FaultFileError("rule id missing or duplicate: %r" % (rid,))
        seen.add(rid)
        if not isinstance(r.get("site"), str) or not r["site"]:
            raise FaultFileError("rule %s: site missing" % rid)
        if r.get("action") not in ACTIONS:
            raise FaultFileError("rule %s: action must be one of %s" % (rid, ", ".join(ACTIONS)))
        for k in ("skip", "hits"):
            v = r.get(k)
            if v is not None and (not isinstance(v, int) or isinstance(v, bool) or v < 0):
                raise FaultFileError("rule %s: %s must be an integer >= 0" % (rid, k))
        for k in ("start", "end", "seconds"):
            v = r.get(k)
            if v is not None and (not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0):
                raise FaultFileError("rule %s: %s must be a number >= 0" % (rid, k))
        if r["action"] == "delay" and r.get("seconds") is None:
            raise FaultFileError("rule %s: delay needs seconds" % rid)
        if r["action"] == "raise" and r.get("exception") and "." not in r["exception"]:
            resolve_exception(r["exception"])          # built-ins are checked at once
    return rules


class _State:
    def __init__(self, path, log_path, tag):
        self.path, self.tag = path, tag
        self.key = None
        self.rules = []
        self.counts = {}            # rule id -> [matching hits inside the window, fired]
        self.lock = threading.Lock()
        self.log_fd = os.open(log_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644) if log_path else None

    def _stat_key(self):
        try:
            st = os.stat(self.path)
        except FileNotFoundError:
            return None
        return (st.st_ino, st.st_mtime_ns, st.st_size)

    def refresh(self):
        """Re-read the file if it changed. Caller holds the lock."""
        key = self._stat_key()
        if key == self.key:
            return
        self.key = key
        if key is None:
            self.rules = []
        else:
            try:
                with open(self.path, encoding="utf-8") as f:
                    rules = validate_rules(json.load(f))
            except (OSError, ValueError) as exc:          # keep the previous rules; say so
                self.log({"error": "%s: %s" % (type(exc).__name__, exc)})
                return
            self.rules = [r for r in rules if not r.get("tag") or r.get("tag") == self.tag]
        ids = {r["id"] for r in self.rules}
        self.counts = {k: v for k, v in self.counts.items() if k in ids}

    def rule_present(self, rid):
        with self.lock:
            self.refresh()
            for r in self.rules:
                if r["id"] == rid:
                    return r
        return None

    def log(self, obj):
        if self.log_fd is None:
            return
        obj = dict(obj, pid=os.getpid(), tag=self.tag, t=round(time.time(), 6))
        os.write(self.log_fd, (json.dumps(obj, separators=(",", ":")) + "\n").encode())

    def pick(self, name):
        """The rule that fires for this call, or None. Counts hits."""
        now = time.time()
        with self.lock:
            self.refresh()
            for r in self.rules:
                if not fnmatch.fnmatchcase(name, r["site"]):
                    continue
                if (r.get("start") is not None and now < r["start"]) or (r.get("end") is not None and now >= r["end"]):
                    continue
                c = self.counts.setdefault(r["id"], [0, 0])
                c[0] += 1
                skip, hits = r.get("skip") or 0, r.get("hits")
                if c[0] <= skip or (hits is not None and c[1] >= hits):
                    continue
                c[1] += 1
                self.log({"site": name, "rule": r["id"], "action": r["action"], "n": c[1]})
                return r
        return None


def _window_open(r):
    now = time.time()
    return not ((r.get("start") is not None and now < r["start"]) or (r.get("end") is not None and now >= r["end"]))


_state = None


def reconfigure(environ=None):
    """(Re)read the environment contract. Returns True when fault sites are active."""
    global _state
    env = os.environ if environ is None else environ
    path = (env.get(ENV_FILE) or "").strip()
    old = _state
    _state = _State(path, (env.get(ENV_LOG) or "").strip(), (env.get(ENV_TAG) or "").strip()) if path else None
    if old is not None and old.log_fd is not None:
        os.close(old.log_fd)
    return _state is not None


def active():
    return _state is not None


def fault_site(name):
    """Mark a fault site. Returns at once unless a control file is configured and one of its rules fires here."""
    st = _state
    if st is None:
        return
    if name not in _SITES:
        raise UndeclaredFaultSite("fault site %r is not declared" % (name,))
    r = st.pick(name)
    if r is None:
        return
    action = r["action"]
    if action == "delay":
        time.sleep(r["seconds"])
    elif action == "block":
        t0 = time.monotonic()
        limit = r.get("seconds")
        while True:
            cur = st.rule_present(r["id"])
            if cur is None or not _window_open(cur):
                break
            if limit is not None and time.monotonic() - t0 >= limit:
                break
            time.sleep(BLOCK_POLL_S)
        with st.lock:
            st.log({"site": name, "rule": r["id"], "action": "unblock", "blocked_s": round(time.monotonic() - t0, 3)})
    else:
        cls = resolve_exception(r.get("exception"))
        exc = cls(r.get("message") or "fault injected at %s (rule %s)" % (name, r["id"]))
        try:
            exc.cdc_v2_fault = r["id"]
        except AttributeError:          # a few C exception types refuse attributes; the message still says it
            pass
        raise exc


reconfigure()
