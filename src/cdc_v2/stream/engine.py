"""The engine as seen from Python (plan C2): start the JVM, read its frames, send DURABLE and STOP, kill it.

    artifacts = cdc_v2.stream.jvm.prepare()
    engine = Engine(settings, start_position, artifacts)       # JVM child in the caller's process group
    for frame in engine.frames(Deadline.after(1.0)):           # until the deadline; EngineBroken / EngineFatal
        ...
    engine.durable(position)                                   # never blocks
    engine.stop(Deadline.after(20))                            # STOP, wait, then SIGKILL; engine.kill() any time

The JVM gets its frames pipe as file descriptor 3 and its commands on standard input; standard output and standard
error are drained into `logging` (logger `cdc_v2.engine.jvm`) on their own threads, with the password removed.
Secrets reach the JVM only in the CONFIG line: never in its arguments, its environment or a file.
"""
from __future__ import annotations

import collections
import json
import logging
import os
import re
import signal
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from cdc_v2 import crashpoints
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.stream import frames as F
from cdc_v2.stream.frames import EngineBroken

log = logging.getLogger("cdc_v2.engine")
jvm_log = logging.getLogger("cdc_v2.engine.jvm")

#: The crash points the JVM declares (Java class cdc_v2.engine.CdcEngine). `java_crash_points(artifacts)` lists
#: them from the JVM itself; a test keeps the two in step.
JAVA_CRASH_POINTS = {
    "engine.ack_marking": "DURABLE applied: the END or HEARTBEAT record to acknowledge is chosen, markProcessed "
                          "not called yet",
    "engine.acked": "markProcessed and markBatchFinished returned: the acknowledgement is handed to Debezium",
    "engine.flush_sent": "first batch after an acknowledgement: Debezium's poll has sent the flush position to the "
                         "walsender",
}

#: Debezium properties that carry a guarantee or add behaviour: `EngineSettings.extra` may not set them (plan C2,
#: hazard H-016). Entries ending in "." are prefixes. The JVM checks the effective values again at start.
PROTECTED_KEYS = (
    "skipped.operations", "offset.storage", "offset.storage.", "offset.flush.interval.ms", "offset.flush.timeout.ms",
    "offset.commit.policy", "lsn.flush.mode", "errors.max.retries", "slot.max.retries", "slot.retry.delay.ms",
    "provide.transaction.metadata", "snapshot.mode", "publication.autocreate.mode", "publication.name",
    "slot.name", "plugin.name", "converters", "cdcv2.", "tombstones.on.delete", "heartbeat.interval.ms",
    "heartbeat.action.query", "replica.identity.autoset.values", "read.only", "signal.", "offset.mismatch.strategy",
    "unavailable.value.placeholder", "decimal.handling.mode", "binary.handling.mode", "include.unknown.datatypes",
    "record.processing.order", "database.password", "database.hostname", "database.port", "cdc_v2.",
    # options that silently change or drop values or rows; the include list comes only from table_include
    "column.", "transforms", "transforms.", "predicates", "predicates.", "message.key.columns",
    "table.include.list", "table.exclude.list", "schema.include.list", "schema.exclude.list", "schema.refresh.mode",
)


def protected(key):
    return any(key.startswith(k) if k.endswith(".") else key == k for k in PROTECTED_KEYS)


class EngineFatal(Exception):
    """The engine sent FATAL: `frame` holds the class, message and cause chain (no row values)."""

    def __init__(self, frame: F.Fatal):
        self.frame = frame
        chain = "; ".join("%s: %s" % c for c in frame.causes)
        super().__init__("%s: %s%s" % (frame.error_class, frame.message, (" (caused by %s)" % chain) if chain else ""))


class EngineExited(EngineBroken):
    """The JVM ended without FATAL and without being asked to stop. `status` is its exit code (negative: signal)."""

    def __init__(self, status, detail=""):
        self.status = status
        super().__init__("engine exited with status %s%s" % (status, (": " + detail) if detail else ""))


@dataclass(frozen=True)
class EngineSettings:
    """Everything the engine needs. Connection values come from the environment of the caller, never from files
    in the repository. `extra` adds or overrides Debezium properties ((name, value) pairs)."""
    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)
    slot: str = "cdc_v2_slot"
    publication: str = "cdc_v2_pub"
    sslmode: str = "verify-full"
    sslrootcert: Optional[str] = None
    # JVM
    heap_mb: int = 512
    gc: str = "parallel"                         # parallel | g1
    active_processors: Optional[int] = None      # -XX:ActiveProcessorCount (the Flight: 2)
    # Debezium queue and batches (research 8.3, 8.4)
    max_batch_size: int = 8192
    max_queue_size: int = 32768
    max_queue_bytes: int = 256 << 20
    poll_interval_ms: int = 50
    record_processing_threads: int = 1
    heartbeat_interval_ms: int = 5000
    status_update_interval_ms: int = 10000
    message_prefixes: tuple = ("cdc_v2_.*",)
    table_include: Optional[str] = None          # Debezium table.include.list (None: every published table)
    # frames and acknowledgement (docs/design/engine.md)
    frame_max_rows: int = 65536
    frame_target_bytes: int = 8 << 20
    frame_max_bytes: int = F.DEFAULT_MAX_FRAME_BYTES
    max_value_bytes: int = 256 << 20
    ack_max_pending: int = 4096
    stop_timeout_ms: int = 15000
    stat_interval_ms: int = 10000
    extra: tuple = ()

    def __post_init__(self):
        if not (0 < self.port < 65536) or self.port == 5432 and self.host in ("localhost", "127.0.0.1", "::1"):
            raise ValueError("refusing port %r" % (self.port,))
        if (self.max_value_bytes > self.frame_max_bytes - (1 << 20) or self.frame_max_bytes > 0xFFFFFFFF
                or self.frame_target_bytes > self.frame_max_bytes - (1 << 20)):
            raise ValueError("max_value_bytes and frame_target_bytes must leave 1 MiB below frame_max_bytes "
                             "(at most 4 GiB)")
        if self.gc not in ("parallel", "g1"):
            raise ValueError("gc must be parallel or g1")
        if not 0 < self.heartbeat_interval_ms <= 10000:
            raise ValueError("heartbeat_interval_ms must be in 1..10000 (0 would never send HELLO on an idle source)")
        bad = [str(k) for k, _v in self.extra if protected(str(k))]
        if bad:
            raise ValueError("extra may not set protected properties: %s" % ", ".join(sorted(bad)))


def build_properties(s: EngineSettings, start_position=None, engine_id=""):
    """The Debezium properties plus our cdc_v2.* settings, as strings (docs/design/engine.md, "Configuration")."""
    p = {
        "name": "cdc_v2_engine",
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "offset.storage": "org.apache.kafka.connect.storage.MemoryOffsetBackingStore",
        "offset.flush.interval.ms": "0",
        "offset.flush.timeout.ms": "5000",
        "record.processing.order": "ORDERED",
        "record.processing.threads": str(s.record_processing_threads),
        "errors.max.retries": "0",
        "database.hostname": s.host,
        "database.port": str(s.port),
        "database.dbname": s.dbname,
        "database.user": s.user,
        "database.password": s.password,
        "database.sslmode": s.sslmode,
        "database.tcpKeepAlive": "true",
        "driver.options": "-c lc_monetary=C -c IntervalStyle=postgres -c bytea_output=hex -c DateStyle=ISO "
                          "-c extra_float_digits=3 -c TimeZone=UTC",
        "topic.prefix": "cdc_v2",
        "plugin.name": "pgoutput",
        "slot.name": s.slot,
        "publication.name": s.publication,
        "publication.autocreate.mode": "disabled",
        "slot.drop.on.stop": "false",
        "slot.failover": "false",
        "slot.max.retries": "0",
        "slot.retry.delay.ms": "1000",
        "lsn.flush.mode": "connector",
        "offset.mismatch.strategy": "no_validation",
        "status.update.interval.ms": str(s.status_update_interval_ms),
        "snapshot.mode": "no_data",
        "provide.transaction.metadata": "true",
        "skipped.operations": "none",
        "tombstones.on.delete": "false",
        "heartbeat.interval.ms": str(s.heartbeat_interval_ms),
        "message.prefix.include.list": ",".join(s.message_prefixes),
        "schema.refresh.mode": "columns_diff",
        "decimal.handling.mode": "string",
        "time.precision.mode": "structured",
        "interval.handling.mode": "string",
        "binary.handling.mode": "bytes",
        "hstore.handling.mode": "json",
        "include.unknown.datatypes": "true",
        "unavailable.value.placeholder": "hex:006364635f76325f746f61737400",
        "converters": "cdcv2",
        "cdcv2.type": "cdc_v2.engine.TransportConverter",
        "max.batch.size": str(s.max_batch_size),
        "max.queue.size": str(s.max_queue_size),
        "max.queue.size.in.bytes": str(s.max_queue_bytes),
        "poll.interval.ms": str(s.poll_interval_ms),
        "cdc_v2.engine.id": engine_id,
        "cdc_v2.frame.max.rows": str(s.frame_max_rows),
        "cdc_v2.frame.target.bytes": str(s.frame_target_bytes),
        "cdc_v2.frame.max.bytes": str(s.frame_max_bytes),
        "cdc_v2.max.value.bytes": str(s.max_value_bytes),
        "cdc_v2.ack.max.pending": str(s.ack_max_pending),
        "cdc_v2.stop.timeout.ms": str(s.stop_timeout_ms),
        "cdc_v2.stat.interval.ms": str(s.stat_interval_ms),
    }
    if s.sslrootcert:
        p["database.sslrootcert"] = s.sslrootcert
    if s.table_include:
        p["table.include.list"] = s.table_include
    if start_position is not None:
        p["cdc_v2.start.position"] = str(int(start_position))
    for k, v in s.extra:
        p[str(k)] = str(v)
    return p


def jvm_command(artifacts, s: EngineSettings):
    cmd = [artifacts.java, "-Xmx%dm" % s.heap_mb, "-Xms%dm" % min(s.heap_mb, 64),
           "-XX:+UseParallelGC" if s.gc == "parallel" else "-XX:+UseG1GC",
           "-XX:+ExitOnOutOfMemoryError", "-XX:MaxDirectMemorySize=64m", "-Xss1m",
           "-Dcdc_v2.frames=/dev/fd/3", "-Dfile.encoding=UTF-8", "-Duser.timezone=UTC",
           "-Dorg.slf4j.simpleLogger.defaultLogLevel=info", "-Dorg.slf4j.simpleLogger.logFile=System.err",
           "-Dorg.slf4j.simpleLogger.showDateTime=true",
           # this logger prints the text of a value it cannot parse (AbstractColumnValue: "Failed to parse ...")
           "-Dorg.slf4j.simpleLogger.log.io.debezium.connector.postgresql.connection.AbstractColumnValue=off",
           "-Dorg.slf4j.simpleLogger.log.io.debezium.connector.postgresql.connection.DateTimeFormat=off"]
    if s.active_processors:
        cmd.append("-XX:ActiveProcessorCount=%d" % s.active_processors)
    return cmd + ["-cp", artifacts.classpath, "cdc_v2.engine.CdcEngine"]


_SLF4J = re.compile(r"^\d+ \[[^\]]*\] (TRACE|DEBUG|INFO|WARN|ERROR) \S+ - ")
_QUOTED = re.compile(r"'[^']*'|\"[^\"]*\"")


def log_filter(line):
    """A JVM output line as it may be logged, or None when it must not be (see Engine._drain)."""
    if line.startswith("CDCV2 "):
        return line
    m = _SLF4J.match(line)
    if not m:
        return None                         # stack trace, exception header, "Caused by", "... n more", other text
    if m.group(1) in ("WARN", "ERROR"):
        return line[:m.end()] + _QUOTED.sub("'…'", line[m.end():])
    return line


def java_crash_points(artifacts, timeout=60):
    """The crash points declared by the JVM, in the format of `python -m cdc_v2.crashpoints`."""
    out = subprocess.run([artifacts.java, "-cp", artifacts.classpath, "cdc_v2.engine.CdcEngine",
                          "--list-crash-points"], capture_output=True, text=True, timeout=timeout, check=True)
    return json.loads(out.stdout.strip().splitlines()[-1])


def _child_env(extra=None):
    env = {"PATH": os.environ.get("PATH", os.defpath), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC"}
    for k in crashpoints.ENV_VARS:
        if k in os.environ:
            env[k] = os.environ[k]
    if extra:
        env.update(extra)
    return env


class Engine:
    """One engine instance (one JVM). Not thread safe except `durable`, `kill` and the read-only attributes."""

    def __init__(self, config: EngineSettings, start_position: Optional[int], artifacts, *, engine_id=None,
                 env=None, keep_log_lines=400):
        self.config = config
        self.engine_id = engine_id or "cdc_v2_e_" + uuid.uuid4().hex[:12]
        self.start_position = start_position
        self.hello: Optional[F.Hello] = None
        self.log_lines = collections.deque(maxlen=keep_log_lines)
        self._secret = config.password
        self._stopping = False
        self._done = False
        self._lock = threading.Condition()
        self._durable = None          # newest position asked for
        self._durable_sent = None
        self._stop_wanted = False
        self.durables_sent = 0
        r, w = os.pipe()
        try:
            script = 'exec "$@"' if w == 3 else 'exec 3>&%d %d>&- && exec "$@"' % (w, w)
            self.proc = subprocess.Popen(["/bin/sh", "-c", script, "cdc_v2-engine"] + jvm_command(artifacts, config),
                                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                         pass_fds=(w,), env=_child_env(env), close_fds=True)
        except BaseException:
            os.close(r)
            os.close(w)
            raise
        os.close(w)
        self._frames_fd = r
        self.reader = F.FrameReader(r, max_frame_bytes=config.frame_max_bytes)
        self.pid = self.proc.pid
        self._drains = [threading.Thread(target=self._drain, args=(self.proc.stdout, "out"), daemon=True,
                                         name="cdc_v2-engine-stdout"),
                        threading.Thread(target=self._drain, args=(self.proc.stderr, "err"), daemon=True,
                                         name="cdc_v2-engine-stderr")]
        for t in self._drains:
            t.start()
        line = "CONFIG " + json.dumps(build_properties(config, start_position, self.engine_id)) + "\n"
        try:
            self.proc.stdin.write(line.encode("utf-8"))     # a few KB: fits the pipe buffer, never blocks
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError):
            pass                                            # the JVM died at once: frames() reports it
        line = None
        self._writer = threading.Thread(target=self._write_commands, daemon=True, name="cdc_v2-engine-stdin")
        self._writer.start()

    # ------------------------------------------------------------------------------------------------ plumbing

    def _scrub(self, text):
        if self._secret:
            text = text.replace(self._secret, "<password>")
        return text

    def _drain(self, stream, which):
        """JVM output into the log, filtered so that no source value can pass (plan C0): our own lines and
        Debezium's log lines are kept (quoted text removed from WARN and ERROR lines); stack traces and exception
        lines, which can quote a value (for example `For input string: "…"`), are counted, not copied."""
        dropped = 0
        try:
            for raw in iter(stream.readline, b""):
                text = self._scrub(raw.decode("utf-8", "replace").rstrip("\n"))
                safe = log_filter(text)
                if safe is None:
                    dropped += 1
                    continue
                if dropped:
                    self._log("[%d line(s) of a stack trace not copied]" % dropped)
                    dropped = 0
                self._log(safe)
        except (OSError, ValueError):
            pass
        if dropped:
            self._log("[%d line(s) of a stack trace not copied]" % dropped)

    def _log(self, text):
        self.log_lines.append(text)
        jvm_log.info("%s", text)

    def _write_commands(self):
        while True:
            with self._lock:
                while not self._done and not self._stop_wanted and self._durable == self._durable_sent:
                    self._lock.wait(1.0)
                if self._done:
                    return
                todo = []
                if self._durable != self._durable_sent:
                    todo.append("DURABLE %d\n" % self._durable)
                    self._durable_sent = self._durable
                if self._stop_wanted:
                    todo.append("STOP\n")
                    self._stop_wanted = False
            try:
                self.proc.stdin.write("".join(todo).encode("ascii"))
                self.proc.stdin.flush()
                self.durables_sent += sum(1 for t in todo if t.startswith("DURABLE"))
            except (BrokenPipeError, OSError, ValueError):
                return

    # ------------------------------------------------------------------------------------------------ the API

    def durable(self, position: int):
        """The destination commit at `position` is durable. Never blocks: the newest position is sent by a thread."""
        if isinstance(position, bool) or not isinstance(position, int) or not 0 <= position < (1 << 64):
            raise ValueError("not a position: %r" % (position,))
        with self._lock:
            if self._durable is None or position > self._durable:
                self._durable = position
                self._lock.notify_all()

    def next_frame(self, deadline: Deadline):
        """The next frame; raises DeadlineExceeded, EngineFatal, EngineBroken (EngineExited when the JVM ended)."""
        try:
            f = self.reader.read(deadline)
        except EngineBroken:
            self._reap(Deadline.after(2))
            raise
        if f is None:
            status = self._reap(deadline.within(5))
            if self._stopping and status == 0:
                return None
            raise EngineExited(status, self._last_log())
        if isinstance(f, F.Fatal):
            self._reap(deadline.within(5))
            raise EngineFatal(F.Fatal(self._scrub(f.error_class), self._scrub(f.message),
                                      tuple((self._scrub(c), self._scrub(m)) for c, m in f.causes),
                                      f.positions, f.schema, f.table, f.failing_position))
        problem = None
        if isinstance(f, F.Hello):
            if self.hello is not None:
                problem = "second HELLO"
            self.hello = self.hello or f
        elif self.hello is None:
            problem = "first frame is %s, not HELLO" % type(f).__name__
        if problem:
            self.reader.parser.broken = EngineBroken(problem)
            self.kill()
            raise EngineBroken(problem)
        return f

    def frames(self, deadline: Deadline):
        """Yield frames until `deadline` (then the iteration ends) or the engine ends after STOP; raises
        EngineFatal or EngineBroken (EngineExited) on failure."""
        while True:
            try:
                f = self.next_frame(deadline)
            except DeadlineExceeded:
                return
            if f is None:
                return
            yield f

    def stop(self, deadline: Deadline):
        """STOP; discard frames until the JVM exits; SIGKILL when the deadline passes. Returns the exit status."""
        self._stopping = True
        with self._lock:
            self._stop_wanted = True
            self._lock.notify_all()
        while not deadline.expired:
            try:
                f = self.reader.read(deadline.within(0.5))
            except DeadlineExceeded:
                if self.proc.poll() is not None:
                    break
                continue
            except EngineBroken:
                break
            if f is None:
                break
        status = self.proc.poll()
        if status is None:
            try:
                status = self.proc.wait(timeout=max(0.0, deadline.remaining()))
            except subprocess.TimeoutExpired:
                status = None
        if status is None:
            log.warning("engine %s did not stop in time; killing it", self.engine_id)
            return self.kill()
        self._close()
        return status

    def kill(self):
        """SIGKILL the JVM, reap it, close every pipe. Returns the exit status."""
        if self.proc.poll() is None:
            try:
                os.kill(self.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            status = self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:   # pragma: no cover - SIGKILL is not ignorable
            status = None
        self._close()
        return status

    # ---------------------------------------------------------------------------------------------- internals

    def _last_log(self, n=5):
        return " | ".join(list(self.log_lines)[-n:])

    def _reap(self, deadline: Deadline):
        try:
            status = self.proc.wait(timeout=max(0.05, deadline.remaining()))
        except subprocess.TimeoutExpired:
            status = self.kill()
        self._close()
        return status

    def _close(self):
        with self._lock:
            if self._done:
                return
            self._done = True
            self._lock.notify_all()
        for f in (self.proc.stdin,):
            try:
                f.close()
            except OSError:
                pass
        self.reader.close()
        for t in self._drains:
            t.join(5)
        for f in (self.proc.stdout, self.proc.stderr):
            try:
                f.close()
            except OSError:
                pass
        self._writer.join(5)

    @property
    def alive(self):
        return self.proc.poll() is None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if self.proc.poll() is None:
            self.kill()
        else:
            self._close()

    def __del__(self):  # pragma: no cover - last resort; tests call kill/stop
        try:
            if self.proc.poll() is None:
                self.proc.kill()
        except Exception:  # noqa: BLE001
            pass


def wait_until(fn, deadline: Deadline, interval=0.1, what="condition"):
    """Poll `fn` until it returns a truthy value; raise DeadlineExceeded at the deadline. Returns the value."""
    while True:
        v = fn()
        if v:
            return v
        deadline.sleep(interval, what)
