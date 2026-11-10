"""Shared helpers for the local streaming spike (PLAN §2 A 5 part 2): JVM preparation, engine launch, slots,
tables, CPU/memory sampling. Private cluster only (primary 55500, replica 55501).

JDK: $CDC_V2_JAVA_HOME or /opt/homebrew/opt/openjdk@21 (on this machine that path links to Homebrew's OpenJDK 23.0.2;
the JDK lock only has Linux x64 tarballs). Jars: jvm/debezium.lock.json, fetched and verified with
cdc_v2.artifacts into ~/.cache/cdc_flight_v2/jars. Classes: EngineMain (jvm/, unchanged) and the spike's FactsMain,
compiled with --release 21 into ~/.cache/cdc_flight_v2/spike_stream/classes.
"""
from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil

import common as C

sys.path.insert(0, str(C.REPO / "src"))
from cdc_v2 import artifacts  # noqa: E402

CACHE = Path.home() / ".cache" / "cdc_flight_v2"
WORK = CACHE / "spike_stream"
JARS = CACHE / "jars"
CLASSES = WORK / "classes"
LOGS = WORK / "logs"
JAVA_HOME = Path(os.environ.get("CDC_V2_JAVA_HOME", "/opt/homebrew/opt/openjdk@21"))
DBZ_LOCK = artifacts.load_lock(str(C.REPO / "jvm" / "debezium.lock.json"))
ENGINE_SRC = C.REPO / "jvm" / "src" / "cdc_v2" / "engine" / "EngineMain.java"
FACTS_SRC = C.HERE / "java" / "FactsMain.java"
PG_HOME = Path(os.environ.get("CDC_V2_SPIKE_TP_PG_HOME", CACHE / "pg-spike-tp"))
EVID = C.REPO / "evidence" / "p2" / "local_streaming"
PUB = "cdc_v2_stream_pub"
SLOT = "cdc_v2_stream_slot"
PORTS = {"replica": C.REPLICA_PORT, "primary": C.PRIMARY_PORT}
TABLES = ["stream_li", "stream_small", "upd_keyed", "upd_full"]


def prepare() -> dict:
    WORK.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    t = time.time()
    rep = artifacts.fetch(DBZ_LOCK, str(JARS), workers=16)
    artifacts.verify_dir(DBZ_LOCK, str(JARS))
    fetch_s = time.time() - t
    cp = artifacts.classpath(DBZ_LOCK, str(JARS))
    t = time.time()
    subprocess.run([str(JAVA_HOME / "bin" / "javac"), "--release", "21", "-cp", cp, "-d", str(CLASSES),
                    str(ENGINE_SRC), str(FACTS_SRC)], check=True, capture_output=True, timeout=300)
    javac_s = time.time() - t
    ver = subprocess.run([str(JAVA_HOME / "bin" / "java"), "-version"], capture_output=True, text=True).stderr
    return {"jars": rep["count"], "jar_bytes": rep["bytes"], "fetch_s": round(fetch_s, 2),
            "reused": sum(1 for a in rep["artifacts"] if a.get("reused")), "javac_s": round(javac_s, 2),
            "java_version": ver.splitlines()[0] if ver else None}


def classpath() -> str:
    return str(CLASSES) + os.pathsep + artifacts.classpath(DBZ_LOCK, str(JARS))


def runtime_password() -> str:
    return json.loads((PG_HOME / "secrets" / "passwords.json").read_text())["runtime"]["password"]


# ---------------------------------------------------------------- configurations

BASE_PROPS = {
    "name": "cdc_v2_stream_engine",
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "offset.storage": "org.apache.kafka.connect.storage.MemoryOffsetBackingStore",
    "offset.flush.interval.ms": "1000",
    "database.hostname": "localhost",
    "database.user": "cdc_v2_runtime",
    "database.dbname": "cdc_v2_test",
    "database.sslmode": "verify-full",
    "topic.prefix": "cdc_v2_stream",
    "plugin.name": "pgoutput",
    "slot.name": SLOT,
    "publication.name": PUB,
    "publication.autocreate.mode": "disabled",
    "snapshot.mode": "no_data",
    "table.include.list": ",".join("public." + t for t in TABLES),
    "tombstones.on.delete": "false",
    "max.batch.size": "2048",
    "max.queue.size": "8192",
    "poll.interval.ms": "500",
    "converter.schemas.enable": "false",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "cdc_v2.format": "struct",
    "cdc_v2.ack": "sync",
}

# "best" = the combined best configuration of docs/spike/jvm_on_flight.md §7 (run 8)
CONFIGS = {
    "default": ({}, ()),
    "best": ({"statistics.metrics.enabled": "false", "decimal.handling.mode": "double", "max.batch.size": "8192",
              "max.queue.size": "32768"}, ("-XX:+UseParallelGC",)),
}


class Engine:
    n = 0

    def __init__(self, node="replica", cfg="best", extra=None, latency=False, hold_acks=False, label="engine",
                 main="cdc_v2.engine.EngineMain", main_args=()):
        Engine.n += 1
        self.label = f"{label}_{os.getpid()}_{Engine.n}"
        self.node = node
        props, jvm_opts = CONFIGS[cfg]
        p = dict(BASE_PROPS)
        p.update(props)
        p.update(extra or {})
        p["database.port"] = str(PORTS[node])
        p["database.sslrootcert"] = str(PG_HOME / "tls" / "ca.crt")
        p["database.password"] = runtime_password()
        self.props_path = WORK / f"{self.label}.properties"
        fd = os.open(self.props_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            for k, v in p.items():
                f.write("%s=%s\n" % (k, str(v).replace("\\", "\\\\")))
        self.stats_path = LOGS / f"{self.label}.stats.json"
        self.log_path = LOGS / f"{self.label}.jvm.log"
        self.lat_path = LOGS / f"{self.label}.lat.bin" if latency else None
        jcmd = [str(JAVA_HOME / "bin" / "java"), "-Xmx1g", "-XX:ActiveProcessorCount=2", "-Xlog:disable",
                "-Xlog:all=warning:stderr", "-Dcdc_v2.frames=/dev/fd/@FRAMES_FD@", *jvm_opts,
                "-cp", classpath(), main, str(self.props_path), *main_args]
        if main == "cdc_v2.engine.EngineMain":
            cc = [sys.executable, str(C.HERE / "stream_consumer.py"), "--stats", str(self.stats_path),
                  "--jvm-log", str(self.log_path)]
            if latency:
                cc += ["--latency", str(self.lat_path)]
            if hold_acks:
                cc += ["--hold-acks"]
            self.consumer = subprocess.Popen(cc + ["--"] + jcmd)
            self.jvm_pid = None
            deadline = time.time() + 10
            while time.time() < deadline and not self.jvm_pid:
                self.jvm_pid = self.stats().get("jvm_pid")
                time.sleep(0.01)
        else:  # FactsMain writes its own file; the JVM is our direct child
            self.consumer = subprocess.Popen(jcmd, stdin=subprocess.PIPE, stdout=open(self.log_path, "ab"),
                                             stderr=subprocess.STDOUT)
            self.jvm_pid = self.consumer.pid
        self.jvm = psutil.Process(self.jvm_pid)

    def stats(self) -> dict:
        try:
            return json.loads(self.stats_path.read_text())
        except (OSError, ValueError):
            return {}

    def events(self) -> int:
        return self.stats().get("events", 0)

    def log_text(self) -> str:
        try:
            return self.log_path.read_text(errors="replace")
        except OSError:
            return ""

    def wait_polling(self, timeout=120) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if "POLLING_STARTED" in self.log_text():
                return True
            if self.consumer.poll() is not None:
                return False
            time.sleep(0.05)
        return False

    def wait_events(self, n, timeout) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.events() >= n:
                return True
            if self.consumer.poll() is not None:
                return False
            time.sleep(0.02)
        return False

    def errors(self, limit=10):
        return [C.redact(ln)[:300] for ln in self.log_text().splitlines()
                if (" ERROR " in ln or "Exception" in ln) and "password" not in ln.lower()][:limit]

    def thread_cpu(self) -> dict:
        """Per-thread CPU (ms) from a jcmd thread dump (JDK 11+ prints cpu=…ms per thread)."""
        out = subprocess.run([str(JAVA_HOME / "bin" / "jcmd"), str(self.jvm_pid), "Thread.print"],
                             capture_output=True, text=True, timeout=60).stdout
        res = {}
        for m in re.finditer(r'^"([^"]+)".*?cpu=([\d.]+)ms', out, re.M):
            name = re.sub(r"\d+", "N", m.group(1)) if m.group(1).startswith(("Thread-", "pool-")) else m.group(1)
            res[name] = res.get(name, 0.0) + float(m.group(2))
        for m in re.finditer(r'^"(GC Thread|C2 CompilerThread|C1 CompilerThread|G1 [^"]+|VM Thread)[^"]*".*?cpu=([\d.]+)ms',
                             out, re.M):
            pass
        return res

    def stop(self, sig=signal.SIGTERM, timeout=60) -> float:
        t = time.time()
        try:
            if self.jvm.is_running():
                os.kill(self.jvm_pid, sig)
        except (ProcessLookupError, psutil.NoSuchProcess):
            pass
        try:
            self.consumer.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.kill(self.jvm_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.consumer.wait(timeout=30)
        try:
            self.props_path.unlink()
        except OSError:
            pass
        return time.time() - t


def thread_groups(d: dict) -> dict:
    """Group jcmd thread CPU (ms) into the categories of docs/spike/jvm_on_flight.md §7.1, in seconds."""
    g = {"streaming": 0.0, "jit": 0.0, "gc": 0.0, "metrics_or_other_Thread-N": 0.0, "engine_poll_and_ours": 0.0,
         "other": 0.0}
    for k, v in d.items():
        if "change-event-source-coordinator" in k:
            g["streaming"] += v
        elif "Compiler" in k:
            g["jit"] += v
        elif "GC" in k or "G1" in k or "ParGC" in k:
            g["gc"] += v
        elif k.startswith("Thread-"):
            g["metrics_or_other_Thread-N"] += v
        elif k.startswith(("cdc_v2", "pool-")) or "engine" in k.lower():
            g["engine_poll_and_ours"] += v
        else:
            g["other"] += v
    return {k: round(v / 1000.0, 2) for k, v in g.items()}


def thread_delta(a: dict, b: dict) -> dict:
    return {k: b.get(k, 0.0) - a.get(k, 0.0) for k in b}


# ---------------------------------------------------------------- sampling

def proc_cpu(p: psutil.Process | None) -> float | None:
    if p is None:
        return None
    try:
        t = p.cpu_times()
        return t.user + t.system
    except psutil.NoSuchProcess:
        return None


class Sampler:
    """Samples JVM RSS (peak), the slot directory size on the decoding node (peak) and process CPU at the ends."""

    def __init__(self, eng: Engine, node: str, interval=0.2):
        self.eng = eng
        self.node = node
        self.interval = interval
        self.peak_rss = 0
        self.peak_slotdir = 0
        self.stop_ev = threading.Event()
        self.walsender = walsender(node)
        self.consumer = psutil.Process(eng.consumer.pid)
        self.c0 = self.snapshot()
        self.t = threading.Thread(target=self.loop, daemon=True)
        self.t.start()

    def snapshot(self):
        return {"jvm": proc_cpu(self.eng.jvm), "walsender": proc_cpu(self.walsender),
                "consumer": proc_cpu(self.consumer), "t": time.time()}

    def loop(self):
        d = PG_HOME / "nodes" / self.node / "data" / "pg_replslot" / SLOT
        while not self.stop_ev.wait(self.interval):
            try:
                self.peak_rss = max(self.peak_rss, self.eng.jvm.memory_info().rss)
            except psutil.NoSuchProcess:
                pass
            try:
                self.peak_slotdir = max(self.peak_slotdir, sum(f.stat().st_size for f in d.iterdir()))
            except OSError:
                pass

    def finish(self) -> dict:
        c1 = self.snapshot()
        self.stop_ev.set()
        self.t.join()
        cpu = {k: (round(c1[k] - self.c0[k], 2) if c1[k] is not None and self.c0[k] is not None else None)
               for k in ("jvm", "walsender", "consumer")}
        return {"cpu_s": cpu, "jvm_peak_rss_mb": round(self.peak_rss / 2 ** 20, 1),
                "slot_dir_peak_mb": round(self.peak_slotdir / 2 ** 20, 1), "window_s": round(c1["t"] - self.c0["t"], 2)}


def walsender(node: str) -> psutil.Process | None:
    with C.pg_connect(node, "admin") as c:
        r = c.execute("select active_pid from pg_replication_slots where slot_name = %s", (SLOT,)).fetchone()
    if r and r[0]:
        try:
            return psutil.Process(r[0])
        except psutil.NoSuchProcess:
            return None
    return None


def slot_stats(node: str) -> dict:
    with C.pg_connect(node, "admin") as c:
        r = c.execute("select spill_txns, spill_count, spill_bytes, stream_txns, total_txns, total_bytes "
                      "from pg_stat_replication_slots where slot_name = %s", (SLOT,)).fetchone()
    return dict(zip(["spill_txns", "spill_count", "spill_bytes", "stream_txns", "total_txns", "total_bytes"],
                    r)) if r else {}


def diff(a: dict, b: dict) -> dict:
    return {k: (b[k] - a.get(k, 0)) if isinstance(b[k], (int, float)) else b[k] for k in b}


# ---------------------------------------------------------------- setup, slots

def localpg_sql(node: str, sql: str) -> str:
    """Runs SQL as the bootstrap superuser through localpg's private socket (used only where a managed provider's
    admin role could not act: pg_log_standby_snapshot)."""
    return subprocess.run([str(C.LOCALPG), "sql", node, "-c", sql], check=True, capture_output=True, text=True,
                          timeout=60).stdout


def setup_tables() -> None:
    with C.pg_connect("primary", "admin") as p:
        for t in TABLES:
            p.execute(f"CREATE TABLE IF NOT EXISTS public.{t} (LIKE public.lineitem INCLUDING ALL)")
            p.execute(f"GRANT SELECT ON public.{t} TO cdc_v2_runtime")
        p.execute("ALTER TABLE public.upd_full REPLICA IDENTITY FULL")
        p.execute("CREATE SEQUENCE IF NOT EXISTS public.stream_small_seq")
        p.execute("CREATE TABLE IF NOT EXISTS public.stream_unpub (id bigint primary key, v text)")
        if not p.execute("select 1 from pg_publication where pubname = %s", (PUB,)).fetchone():
            p.execute(f"CREATE PUBLICATION {PUB} FOR TABLE {', '.join('public.' + t for t in TABLES)} "
                      f"WITH (publish = 'insert, update, delete')")
    C.wait_replica_caught_up()


def drop_slot(node: str, name: str = SLOT) -> None:
    with C.pg_connect(node, "runtime") as c:
        for _ in range(100):
            r = c.execute("select active from pg_replication_slots where slot_name = %s", (name,)).fetchone()
            if r is None:
                return
            if not r[0]:
                c.execute("select pg_drop_replication_slot(%s)", (name,))
                return
            time.sleep(0.1)
        raise RuntimeError("slot still active")


def create_slot(node: str, name: str = SLOT, help_standby=True, timeout=60.0) -> dict:
    """Create a pgoutput slot. On the replica the creation waits for a running-transactions record from the
    primary; with help_standby, pg_log_standby_snapshot() is called on the primary every 0.5 s meanwhile."""
    res = {"node": node, "help_standby": help_standby}
    done = threading.Event()
    calls = [0]

    def helper():
        while not done.wait(0.5):
            localpg_sql("primary", "select pg_log_standby_snapshot()")
            calls[0] += 1

    if node == "replica" and help_standby:
        threading.Thread(target=helper, daemon=True).start()
    t = time.time()
    try:
        with C.pg_connect(node, "runtime") as c:
            c.execute(f"SET statement_timeout = {int(timeout * 1000)}")
            lsn = c.execute("select lsn from pg_create_logical_replication_slot(%s, 'pgoutput')", (name,)).fetchone()[0]
        res.update(ok=True, seconds=round(time.time() - t, 2), consistent_point=str(lsn))
    except Exception as e:  # noqa: BLE001
        res.update(ok=False, seconds=round(time.time() - t, 2), error=C.redact(e).splitlines()[0])
    finally:
        done.set()
    res["pg_log_standby_snapshot_calls"] = calls[0]
    return res


def fresh_slot(node: str) -> dict:
    for n in ("replica", "primary"):
        drop_slot(n)
    return create_slot(node)


def wait_slot_idle(node: str, timeout=60) -> None:
    deadline = time.time() + timeout
    with C.pg_connect(node, "admin") as c:
        while time.time() < deadline:
            r = c.execute("select active from pg_replication_slots where slot_name = %s", (SLOT,)).fetchone()
            if not r or not r[0]:
                return
            time.sleep(0.1)


def truncate(*tables) -> None:
    with C.pg_connect("primary", "admin") as p:
        p.execute("TRUNCATE " + ", ".join("public." + t for t in tables))


def write_evidence(name: str, rec: dict) -> None:
    EVID.mkdir(parents=True, exist_ok=True)
    with open(EVID / name, "a") as f:
        f.write(C.redact(json.dumps(rec, default=str)) + "\n")
