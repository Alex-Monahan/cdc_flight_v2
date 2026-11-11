"""The pool of Postgres homes of the fuzz campaigns, and the record of what each scenario created.

Homes `~/.cache/cdc_flight_v2/pg-f<n>`, port base 55700 + 8·n, n = 0 … 11 (docs/design/p3_plan.md section 4).
One scenario holds one home under an exclusive `flock` on `fuzz/locks/pg-f<n>.lock`; the lock dies with the
process, so a killed campaign never blocks a home for good. Next to the lock, `pg-f<n>.json` records everything the
holder created: databases, MotherDuck databases, processes (pid and start time), and whether the cluster is dirty
(a fault kind changed its shape). `clean()` removes exactly what those records name, for every home whose lock it
can take, and nothing else: it never matches processes by name and never lists databases.

A cluster is reused across seeds (a fresh database per seed) until a scenario marks it dirty; the next holder then
rebuilds it with `localpg reset`. The ports base+5 … base+7 of a home are its TCP proxies (base … base+4 are the
nodes that localpg may create).
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional

from cdc_v2.testing import pg as tpg

CACHE = tpg.CACHE
LOCK_DIR = CACHE / "fuzz" / "locks"
FIRST_BASE, STEP, HOMES = 55700, 8, 12
PROXY_OFFSETS = (5, 7)


def home_path(n: int) -> Path:
    return CACHE / ("pg-f%d" % n)


def base_of(n: int) -> int:
    if not 0 <= n < HOMES:
        raise ValueError("fuzz homes are pg-f0 … pg-f%d" % (HOMES - 1))
    return FIRST_BASE + STEP * n


def _ps_start(pid: int) -> Optional[str]:
    try:
        out = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, timeout=10)
    except Exception:                                      # noqa: BLE001
        return None
    s = out.stdout.strip()
    return s or None


class HomeLease:
    """One home of the pool, held by this process. `record` is persisted on every change."""

    def __init__(self, n: int, fd: int):
        self.n, self.fd = n, fd
        self.home, self.base = home_path(n), base_of(n)
        self.record_path = LOCK_DIR / ("pg-f%d.json" % n)
        self.record = {"home": "pg-f%d" % n, "base": self.base, "holder": os.getpid(), "databases": [], "md": [],
                       "procs": [], "dirty": False}
        self._cluster = None
        self._save_lock = threading.Lock()

    @property
    def proxy_range(self) -> tuple:
        return (self.base + PROXY_OFFSETS[0], self.base + PROXY_OFFSETS[1])

    def save(self) -> None:
        with self._save_lock:                          # several threads of one scenario note processes
            tmp = self.record_path.with_suffix(".%d.%d.tmp" % (os.getpid(), threading.get_ident()))
            tmp.write_text(json.dumps(self.record, indent=1, sort_keys=True))
            os.replace(tmp, self.record_path)

    def note(self, kind: str, value) -> None:
        if value not in self.record[kind]:
            self.record[kind].append(value)
            self.save()

    def forget(self, kind: str, value) -> None:
        if value in self.record[kind]:
            self.record[kind].remove(value)
            self.save()

    def note_proc(self, pid: int) -> None:
        self.note("procs", {"pid": pid, "start": _ps_start(pid)})

    def forget_procs(self) -> None:
        """After a scenario has stopped its processes (checked: no survivor), its records go."""
        self.record["procs"] = []
        self.save()

    def mark_dirty(self, why: str) -> None:
        self.record["dirty"] = why
        self.save()

    def localpg(self, *args, check=True, timeout=600):
        return tpg.localpg(*args, home=self.home, port_base=self.base if not (self.home / "state.json").exists()
                           else None, check=check, timeout=timeout)

    def cluster(self, rebuild: bool = False) -> tpg.Cluster:
        """The home's cluster, started (built on first use, rebuilt when dirty or asked), prepared for the fuzzers."""
        if rebuild or self.record.get("dirty"):
            self.localpg("reset")
            self.record["dirty"] = False
            self.save()
            self._cluster = None
        elif self._cluster is None:
            self.localpg("up")
        else:
            st = self._cluster.status()
            if any(not n.get("running") for n in st.get("nodes", [])):
                self.localpg("up")
        if self._cluster is None:
            self._cluster = tpg.Cluster(self.home)
            # databases are created from template1: the runtime role may nudge a standby slot creation
            self._cluster.sql("GRANT EXECUTE ON FUNCTION pg_log_standby_snapshot() TO cdc_v2_runtime",
                              role="bootstrap", db="template1")
            from cdc_v2.evidence import default_redactor
            for s in self._cluster.secrets():
                default_redactor().add(s)
        return self._cluster

    def release(self) -> None:
        if self.fd is not None:
            self.record["holder"] = None
            self.save()
            with contextlib.suppress(OSError):
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None


def acquire(prefer: Optional[int] = None, wait_s: float = 0.0) -> HomeLease:
    """Lease a free home (the preferred one first). A home left behind by a killed holder is cleaned first."""
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    order = list(range(HOMES))
    if prefer is not None:
        order = [prefer] + [n for n in order if n != prefer]
    deadline = time.monotonic() + wait_s
    while True:
        for n in order:
            fd = os.open(LOCK_DIR / ("pg-f%d.lock" % n), os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                os.close(fd)
                continue
            if _is_fixture_home(n):
                os.close(fd)                               # the T1 fixtures' cluster is never a fuzz scenario's
                continue
            lease = HomeLease(n, fd)
            old = _read_record(n)
            if old:
                lease.record["dirty"] = old.get("dirty", False)
                _clean_record(old, lease)                  # may mark it dirty too; never clears a mark
            lease.save()
            return lease
        if time.monotonic() >= deadline:
            raise RuntimeError("no free fuzz home (pg-f0 … pg-f%d are all held)" % (HOMES - 1))
        time.sleep(0.5)


def _is_fixture_home(n: int) -> bool:
    named = os.environ.get("CDC_V2_PG_HOME")
    return bool(named) and Path(named).expanduser().resolve() == home_path(n).resolve()


def _read_record(n: int) -> Optional[dict]:
    p = LOCK_DIR / ("pg-f%d.json" % n)
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None


def _clean_record(rec: dict, lease: HomeLease) -> dict:
    """Remove what a record names. Returns what was done (names only, no secrets)."""
    done = {"home": rec.get("home"), "killed": 0, "databases": 0, "md": 0}
    for p in rec.get("procs", []):
        pid, start = p.get("pid"), p.get("start")
        if pid and start and _ps_start(pid) == start:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(pid, 9)
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(pid, 9)
            done["killed"] += 1
    if rec.get("databases") and (lease.home / "state.json").exists():
        try:
            cl = lease.cluster()
            for name in rec["databases"]:
                with contextlib.suppress(Exception):
                    db = tpg.PrivateDatabase.__new__(tpg.PrivateDatabase)     # an existing database, by name
                    db.cluster, db.name = cl, name
                    db.drop()
                    done["databases"] += 1
        except Exception:                                  # noqa: BLE001 - a broken cluster is rebuilt below
            lease.record["dirty"] = "clean found a broken cluster"
    for name in rec.get("md", []):
        with contextlib.suppress(Exception):
            from cdc_v2.testing import md
            md.drop_database(name)
            done["md"] += 1
    lease.record.update(databases=[], md=[], procs=[])
    return done


def clean(stop_clusters: bool = True) -> list:
    """`tools/fuzz clean`: for every home whose lock is free, remove what its record names and stop its cluster."""
    out = []
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    for n in range(HOMES):
        rec = _read_record(n)
        if rec is None and not (home_path(n) / "state.json").exists():
            continue
        if _is_fixture_home(n):
            out.append({"home": "pg-f%d" % n, "skipped": "the T1 fixtures' cluster (CDC_V2_PG_HOME)"})
            continue
        fd = os.open(LOCK_DIR / ("pg-f%d.lock" % n), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            out.append({"home": "pg-f%d" % n, "skipped": "held by a running campaign"})
            continue
        lease = HomeLease(n, fd)
        lease.record["dirty"] = (rec or {}).get("dirty", False)
        try:
            done = _clean_record(rec or {}, lease)
            if stop_clusters and (lease.home / "state.json").exists():
                lease.localpg("down", check=False)
                done["stopped"] = True
            lease.save()
            out.append(done)
        finally:
            lease.release()
    return out
