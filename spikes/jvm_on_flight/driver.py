"""JVM + stock Debezium on an F16 Flight (PLAN §2 A 1). Run by flights/spike_jvm/main.py from a clone of this repo.

Phases (config CDC_V2_SPIKE_MODE, comma-separated, run in the given order):
  jdk        compare ways to get a JDK (Temurin 21, Corretto 21, Temurin 25 tarballs; Debian's apt OpenJDK 17),
             what the JVM detects, javac and source-launcher timings, parallel JDK + jar fetch
  e2e        start-up budget of a hand-over and end-to-end proof: JDK, jars, compile, then (fixture) Postgres, a
             backlog in an existing slot, engine start, first event, counts and values verified
  tput       throughput indication for one 1M-row transaction under several engine settings and formats
  memory     JVM resident memory at heap caps 256 MB .. 2 GB
  small      many small transactions: backlog catch-up and live
  big        one 10M-row transaction (best settings found by tput)
  supervise  clean shutdown, SIGKILL, parent death, frozen JVM: what happens to the JVM and the slot
  orphan     leaves a detached child running and exits: does the run end, does the child keep logging?

Prints `RESULT {json}` lines (one per measurement) and `T <seconds since run start> <step>` lines.
The Postgres installed here is a SPIKE FIXTURE ONLY (see pgfixture.py); the product never installs Postgres.
No secret is printed: the replication role's password exists only in a mode-600 properties file.
"""
import glob
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.environ.get("CDC_V2_SRC", os.path.dirname(os.path.dirname(HERE)))
sys.path.insert(0, os.path.join(SRC, "src"))
sys.path.insert(0, HERE)

from cdc_v2 import artifacts  # noqa: E402

RUN_T0 = float(os.environ.get("CDC_V2_RUN_T0", time.time()))
MODES = [m.strip() for m in os.environ.get("CDC_V2_SPIKE_MODE", "jdk,e2e").split(",") if m.strip()]
JDK_ID = os.environ.get("CDC_V2_JDK", "temurin-21")
WORK = "/tmp/cdc_v2_spike"
JARS = WORK + "/jars"
CLASSES = WORK + "/classes"
LOGS = WORK + "/logs"
JAVA_SRC = os.path.join(SRC, "jvm/src/cdc_v2/engine/EngineMain.java")
DBZ_LOCK = artifacts.load_lock(os.path.join(SRC, "jvm/debezium.lock.json"))
JDK_LOCK = artifacts.load_lock(os.path.join(SRC, "jvm/jdk.lock.json"))
CLK = os.sysconf("SC_CLK_TCK")

STATE = {"java": None, "cp": None, "pg": None, "password": None, "best": None}


def T(step):
    print("T %.3f %s" % (time.time() - RUN_T0, step), flush=True)


def result(phase, **kw):
    kw["phase"] = phase
    kw["t_run"] = round(time.time() - RUN_T0, 3)
    print("RESULT " + json.dumps(kw, default=str, sort_keys=True), flush=True)


def sh(cmd, timeout=600, **kw):
    t = time.time()
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, **kw)
    return p.returncode, (p.stdout + p.stderr), time.time() - t


# ---- /proc helpers ------------------------------------------------------------------------------------------------

def cpu_s(pid):
    try:
        with open("/proc/%d/stat" % pid) as f:
            parts = f.read().rsplit(")", 1)[1].split()
        return (int(parts[11]) + int(parts[12])) / CLK
    except (OSError, IndexError, ValueError):
        return None


def status(pid, keys=("VmRSS", "VmHWM", "State", "PPid", "Threads")):
    out = {}
    try:
        with open("/proc/%d/status" % pid) as f:
            for line in f:
                k, _, v = line.partition(":")
                if k in keys:
                    out[k] = v.strip()
    except OSError:
        return None
    return out


def rss_mib(pid, key="VmRSS"):
    s = status(pid, (key,))
    if not s or key not in s:
        return None
    return round(int(s[key].split()[0]) / 1024, 1)


def comm(pid):
    try:
        with open("/proc/%d/comm" % pid) as f:
            return f.read().strip()
    except OSError:
        return None


def alive(pid):
    s = status(pid, ("State",))
    return bool(s) and not s.get("State", "").startswith("Z")


def postgres_pids():
    return [int(p) for p in os.listdir("/proc") if p.isdigit() and comm(int(p)) == "postgres"]


def thread_cpu(pid):
    """CPU seconds per JVM thread group (native thread names with trailing numbers folded)."""
    out = {}
    try:
        tids = os.listdir("/proc/%d/task" % pid)
    except OSError:
        return out
    for tid in tids:
        try:
            with open("/proc/%d/task/%s/stat" % (pid, tid)) as f:
                raw = f.read()
        except OSError:
            continue
        name = raw[raw.index("(") + 1:raw.rindex(")")]
        parts = raw.rsplit(")", 1)[1].split()
        key = re.sub(r"[#\-]?\d+$", "", name).strip() or name
        out[key] = out.get(key, 0.0) + (int(parts[11]) + int(parts[12])) / CLK
    return out


def thread_delta(a, b, top=12):
    d = {k: round(v - a.get(k, 0.0), 2) for k, v in b.items()}
    return dict(sorted(((k, v) for k, v in d.items() if v > 0.05), key=lambda kv: -kv[1])[:top])


def jfr_summary(java_home, path, top=30):
    """Hot methods from a JFR recording: self samples and inclusive samples."""
    rc, out, _ = sh([java_home + "/bin/jfr", "print", "--events", "jdk.ExecutionSample", "--stack-depth", "64",
                     path], timeout=300)
    if rc != 0:
        return {"error": out[-500:]}
    self_c, incl_c, threads, n = {}, {}, {}, 0
    frames, thread, in_stack = [], None, False
    for ln in out.splitlines():
        t = ln.strip()
        if t.startswith("jdk.ExecutionSample"):
            frames, thread, in_stack = [], None, False
        elif t.startswith("sampledThread"):
            m = re.search(r'"([^"]*)"', t)
            thread = re.sub(r"[#\-]?\d+$", "", m.group(1)) if m else "?"
        elif t.startswith("stackTrace = ["):
            in_stack = True
        elif in_stack and t == "]":
            in_stack = False
        elif in_stack and t and t != "...":
            frames.append(re.sub(r"\s+line:.*$", "", t))
        elif t == "}" and frames:
            n += 1
            threads[thread] = threads.get(thread, 0) + 1
            self_c[frames[0]] = self_c.get(frames[0], 0) + 1
            for f in set(frames):
                incl_c[f] = incl_c.get(f, 0) + 1
            frames = []
    ours = ("io.debezium", "org.apache.kafka", "org.postgresql", "cdc_v2")
    pct = lambda c: round(100.0 * c / n, 1) if n else None  # noqa: E731
    return {"samples": n, "threads": {k: pct(v) for k, v in sorted(threads.items(), key=lambda kv: -kv[1])[:8]},
            "self_pct": [(k[:160], pct(v)) for k, v in sorted(self_c.items(), key=lambda kv: -kv[1])[:top]],
            "inclusive_pct": [(k[:160], pct(v)) for k, v in sorted(incl_c.items(), key=lambda kv: -kv[1])
                              if k.startswith(ours)][:top + 10]}


def cgroup_mem():
    out = {}
    for k in ("memory.current", "memory.peak", "memory.max"):
        try:
            with open("/sys/fs/cgroup/" + k) as f:
                out[k] = f.read().strip()
        except OSError:
            pass
    return out


class CpuWindow:
    """CPU seconds of named processes between two points."""

    def __init__(self, pids):
        self.pids = pids
        self.t0 = time.time()
        self.c0 = {k: cpu_s(p) for k, p in pids.items()}
        self.pg0 = {p: cpu_s(p) for p in postgres_pids()}

    def stop(self):
        wall = time.time() - self.t0
        out = {"wall_s": round(wall, 3)}
        for k, p in self.pids.items():
            c1 = cpu_s(p)
            if c1 is not None and self.c0.get(k) is not None:
                out[k + "_cpu_s"] = round(c1 - self.c0[k], 2)
                out[k + "_cpu_pct"] = round(100 * (c1 - self.c0[k]) / wall, 1) if wall else None
        pg = 0.0
        for p in postgres_pids():
            c1 = cpu_s(p)
            if c1 is not None:
                pg += c1 - (self.pg0.get(p) or 0.0)
        out["postgres_all_cpu_s"] = round(pg, 2)
        out["postgres_all_cpu_pct"] = round(100 * pg / wall, 1) if wall else None
        return out


# ---- JDK, jars, compile -------------------------------------------------------------------------------------------

def jvm_facts(java):
    rc, out, _ = sh([java, "-XshowSettings:system", "-version"], timeout=60)
    sysinfo = [ln.strip() for ln in out.splitlines() if ln.strip()]
    rc, out, _ = sh([java, "-XX:+PrintFlagsFinal", "-version"], timeout=60)
    flags = {}
    for ln in out.splitlines():
        m = re.match(r"\s*\S+\s+(\w+)\s+:?=\s+(\S+)", ln)
        if m and m.group(1) in ("ActiveProcessorCount", "MaxHeapSize", "InitialHeapSize", "MaxRAM", "UseG1GC",
                                "UseSerialGC", "UseParallelGC", "UseContainerSupport", "ParallelGCThreads",
                                "ConcGCThreads", "CICompilerCount", "MaxRAMPercentage"):
            flags[m.group(1)] = m.group(2)
    return {"system": sysinfo[:40], "flags": flags}


def java_version(java):
    rc, out, dt = sh([java, "-version"], timeout=60)
    return out.strip().splitlines()[:3], dt


def install_jdk(ident, root=WORK + "/jdk"):
    home, t = artifacts.install_jdk(JDK_LOCK, ident, root)
    return home, t


def fetch_jars(dest=JARS, workers=16):
    rep = artifacts.fetch(DBZ_LOCK, dest, workers=workers)
    artifacts.verify_dir(DBZ_LOCK, dest)
    return rep


def compile_java(home, dest=CLASSES):
    os.makedirs(dest, exist_ok=True)
    cp = artifacts.classpath(DBZ_LOCK, JARS)
    t = time.time()
    rc, out, dt = sh([home + "/bin/javac", "--release", "21", "-cp", cp, "-d", dest, JAVA_SRC], timeout=300)
    if rc != 0:
        raise RuntimeError("javac failed: " + out[-2000:])
    return dt, cp


def selftest(home, cp, source_launcher=False):
    if source_launcher:
        cmd = [home + "/bin/java", "-cp", cp, JAVA_SRC, "--selftest"]
    else:
        cmd = [home + "/bin/java", "-cp", CLASSES + os.pathsep + cp, "cdc_v2.engine.EngineMain", "--selftest"]
    rc, out, dt = sh(cmd, timeout=120)
    return {"rc": rc, "seconds": round(dt, 3), "version_line": next((ln for ln in out.splitlines()
                                                                     if "VERSION" in ln), out[-300:])}


def prepare(ident=JDK_ID):
    """JDK + jars + compile, sequentially, with timings (the hand-over start-up path)."""
    if STATE["java"]:
        return {}
    times = {}
    T("jdk_begin")
    home, t = install_jdk(ident)
    times["jdk_download_s"], times["jdk_unpack_s"] = round(t["download_s"], 3), round(t["unpack_s"], 3)
    T("jdk_done")
    rep = fetch_jars()
    times["jars_s"] = round(rep["seconds"], 3)
    times["jars_bytes"] = rep["bytes"]
    times["jars_count"] = rep["count"]
    T("jars_done")
    dt, cp = compile_java(home)
    times["javac_s"] = round(dt, 3)
    T("compile_done")
    STATE["java"], STATE["cp"], STATE["home"] = home + "/bin/java", cp, home
    return times


# ---- Postgres fixture ---------------------------------------------------------------------------------------------

def pg_up():
    import pgfixture as pgf
    if STATE["pg"]:
        return {}
    T("pg_install_begin")
    inst = pgf.install()
    T("pg_installed")
    st = pgf.start(inst["bindir"])
    pw, setup_s = pgf.setup(inst["bindir"])
    STATE["pg"], STATE["password"] = inst, pw
    T("pg_ready")
    out = dict(inst)
    out.update({k: v for k, v in st.items() if k != "settings"})
    out["setup_s"] = setup_s
    out["settings"] = st["settings"]
    return out


# ---- engine control -----------------------------------------------------------------------------------------------

BASE_PROPS = {
    "name": "cdc_v2_spike_engine",
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
    "offset.storage.file.filename": WORK + "/offsets.dat",
    "offset.flush.interval.ms": "1000",
    "database.hostname": "127.0.0.1",
    "database.port": "5432",
    "database.user": "cdc_v2_spike_repl",
    "database.dbname": "cdc_v2_spike",
    "topic.prefix": "cdc_v2_spike",
    "plugin.name": "pgoutput",
    "slot.name": "cdc_v2_spike_jvm_slot",
    "publication.name": "cdc_v2_spike_jvm_pub",
    "publication.autocreate.mode": "disabled",
    "snapshot.mode": "no_data",
    "table.include.list": "public.lineitem",
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


class Engine:
    n = 0

    def __init__(self, props=None, heap="1g", jvm_opts=(), decode="none", sink=None, label="engine"):
        Engine.n += 1
        self.label = "%s_%d" % (label, Engine.n)
        os.makedirs(LOGS, exist_ok=True)
        self.stats_path = "%s/%s.stats.json" % (LOGS, self.label)
        self.log_path = "%s/%s.jvm.log" % (LOGS, self.label)
        p = dict(BASE_PROPS)
        p.update(props or {})
        p["database.password"] = STATE["password"]
        self.props = p
        props_path = "%s/%s.properties" % (WORK, self.label)
        fd = os.open(props_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            for k, v in p.items():
                f.write("%s=%s\n" % (k, v.replace("\\", "\\\\")))
        # JVM unified logging writes warnings to stdout by default (and JFR prints "Started recording" there):
        # stdout carries our binary frames, so every JVM message is sent to stderr.
        cmd = [STATE["java"], "-Xmx" + heap, "-XX:ActiveProcessorCount=2", "-Xlog:disable",
               "-Xlog:all=warning:stderr", "-Dcdc_v2.frames=/dev/fd/@FRAMES_FD@", *jvm_opts,
               "-cp", CLASSES + os.pathsep + STATE["cp"], "cdc_v2.engine.EngineMain", props_path]
        ccmd = [sys.executable, os.path.join(HERE, "consumer.py"), "--stats", self.stats_path,
                "--jvm-log", self.log_path, "--decode", decode]
        if sink:
            ccmd += ["--sink", sink]
        self.spawn_at = time.time()
        self.consumer = subprocess.Popen(ccmd + ["--"] + cmd)
        self.jvm_pid = None
        deadline = time.time() + 10
        while time.time() < deadline and self.jvm_pid is None:
            s = self.stats()
            self.jvm_pid = s.get("jvm_pid") if s else None
            time.sleep(0.01)

    def stats(self):
        try:
            with open(self.stats_path) as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def events(self):
        return self.stats().get("events", 0)

    def log_lines(self):
        try:
            with open(self.log_path, errors="replace") as f:
                return f.read().splitlines()
        except OSError:
            return []

    def marks(self):
        out = {}
        for ln in self.log_lines():
            if ln.startswith("CDCV2 "):
                parts = ln.split()
                if len(parts) >= 3 and parts[1] not in out and parts[1] != "STAT":
                    out[parts[1]] = {"t": int(parts[2]) / 1000.0, "detail": " ".join(parts[3:])}
        return out

    def wait_mark(self, name, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            m = self.marks()
            if name in m:
                return m[name]
            if self.consumer.poll() is not None:
                return None
            time.sleep(0.05)
        return None

    def wait_events(self, n, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            e = self.events()
            if e >= n:
                return True
            if self.consumer.poll() is not None:
                return False
            time.sleep(0.02)
        return False

    def interesting_log(self, limit=25):
        keep = [ln for ln in self.log_lines() if (" ERROR " in ln or " WARN " in ln or "Exception" in ln)
                and "password" not in ln.lower()]
        return keep[:limit]

    def debezium_version_lines(self):
        pat = re.compile(r"(version|debezium=|Starting PostgresConnectorTask)", re.I)
        return [ln[:300] for ln in self.log_lines() if pat.search(ln) and "password" not in ln.lower()][:12]

    def stop(self, sig=signal.SIGTERM, timeout=60):
        """Signal the JVM and wait for it and the consumer to exit. Returns the time to JVM exit."""
        t = time.time()
        if self.jvm_pid and alive(self.jvm_pid):
            os.kill(self.jvm_pid, sig)
        try:
            self.consumer.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            if self.jvm_pid:
                os.kill(self.jvm_pid, signal.SIGKILL)
            self.consumer.wait(timeout=30)
        return time.time() - t


def fresh_slot():
    import pgfixture as pgf
    for _ in range(100):
        s = pgf.slot_state()
        if s is None or not s["active"]:
            break
        time.sleep(0.1)
    pgf.drop_slot()
    pgf.create_slot()
    try:
        os.remove(WORK + "/offsets.dat")
    except OSError:
        pass


def truncate():
    import pgfixture as pgf
    with pgf.connect() as c:
        c.execute("TRUNCATE lineitem")


def start_streaming(props=None, heap="1g", jvm_opts=(), label="engine", decode="none"):
    """Fresh slot, start the engine, prove it streams with a one-row canary transaction."""
    import pgfixture as pgf
    truncate()
    fresh_slot()
    eng = Engine(props, heap=heap, jvm_opts=jvm_opts, label=label, decode=decode)
    if not eng.wait_mark("POLLING_STARTED", 120):
        return eng, False
    pgf.insert_rows(10 ** 11, 1)
    ok = eng.wait_events(1, 60)
    return eng, ok


# ---- phases -------------------------------------------------------------------------------------------------------

def phase_jdk():
    T("phase_jdk")
    import pgfixture as pgf
    up = pgf.apt_update()
    result("apt_update", seconds=round(up, 3))
    jdks = {}
    for ident in ("temurin-21", "corretto-21", "temurin-25"):
        root = WORK + "/jdkcmp"
        home, t = install_jdk(ident, root)
        ver, dt = java_version(home + "/bin/java")
        rec = {"jdk": ident, "download_s": round(t["download_s"], 3), "unpack_s": round(t["unpack_s"], 3),
               "tarball_bytes": t["bytes"], "unpacked_bytes": t["unpacked_bytes"], "java_version": ver,
               "java_version_s": round(dt, 3)}
        jdks[ident] = home
        result("jdk_tarball", **rec)
    rc, out, dt = sh(["apt-get", "install", "-y", "-q", "--no-install-recommends", "openjdk-17-jdk-headless"],
                     timeout=600, env=dict(os.environ, DEBIAN_FRONTEND="noninteractive"))
    apt_home = next(iter(glob.glob("/usr/lib/jvm/java-17-openjdk-*")), None)
    rec = {"jdk": "debian-apt-openjdk-17", "rc": rc, "apt_install_s": round(dt, 3), "apt_update_s": round(up, 3),
           "unpacked_bytes": artifacts._du(apt_home) if apt_home else None}
    if apt_home:
        rec["java_version"], rec["java_version_s"] = java_version(apt_home + "/bin/java")
    else:
        rec["tail"] = out[-500:]
    result("jdk_apt", **rec)
    # what the JVM detects from the cgroup (2 CPUs, 16 GiB) on this host with 8 visible CPUs and 64 GB
    result("jvm_detects", jdk="temurin-21", **jvm_facts(jdks["temurin-21"] + "/bin/java"))
    result("jvm_detects", jdk="temurin-25", **jvm_facts(jdks["temurin-25"] + "/bin/java"))
    # jars, sequential (also used by e2e); then JDK and jars fetched concurrently into fresh directories
    rep = fetch_jars()
    result("jars_fetch", seconds=round(rep["seconds"], 3), bytes=rep["bytes"], count=rep["count"],
           slowest=sorted(rep["artifacts"], key=lambda r: -r["seconds"])[:3])
    par = {}

    def j():
        par["jdk"] = artifacts.install_jdk(JDK_LOCK, "temurin-21", WORK + "/jdkpar")[1]

    def k():
        par["jars"] = artifacts.fetch(DBZ_LOCK, WORK + "/jarspar", workers=16)["seconds"]

    t = time.time()
    th = [threading.Thread(target=j), threading.Thread(target=k)]
    [x.start() for x in th]
    [x.join() for x in th]
    result("jdk_and_jars_parallel", wall_s=round(time.time() - t, 3),
           jdk_download_s=round(par["jdk"]["download_s"], 3), jdk_unpack_s=round(par["jdk"]["unpack_s"], 3),
           jars_s=round(par["jars"], 3))
    # compile and start with each tarball JDK
    for ident in ("temurin-21", "temurin-25"):
        dt, cp = compile_java(jdks[ident], dest=CLASSES)
        st1 = selftest(jdks[ident], cp)
        st2 = selftest(jdks[ident], cp)
        src = selftest(jdks[ident], cp, source_launcher=True)
        result("compile_and_start", jdk=ident, javac_s=round(dt, 3), selftest_compiled=st1,
               selftest_compiled_again=st2, selftest_source_launcher=src)
    for d in (WORK + "/jdkpar", WORK + "/jarspar"):
        subprocess.run(["rm", "-rf", d])


def phase_e2e():
    T("phase_e2e")
    import pgfixture as pgf
    prep = prepare()
    result("prepare", jdk=JDK_ID, **prep)
    pg = pg_up()
    result("pg_fixture", note="SPIKE FIXTURE ONLY - the product never installs Postgres", **pg)
    # A hand-over: the slot exists and changes are waiting in it.
    pgf.create_slot()
    pgf.insert_rows(1, 10000)                                     # 10,000 inserts, one transaction
    with pgf.connect() as c:
        n_upd = c.execute("UPDATE lineitem SET l_comment = 'updated' WHERE l_orderkey <= 25").rowcount
        n_del = c.execute("DELETE FROM lineitem WHERE l_orderkey > 2487").rowcount
        rows = c.execute("SELECT count(*) FROM lineitem").fetchone()[0]
        ins_sum = c.execute("SELECT sum(l_orderkey) FROM generate_series(1, 10000) g, LATERAL li_row(g)"
                            ).fetchone()[0]
        upd_sum = c.execute("SELECT sum(l_orderkey) FROM lineitem WHERE l_orderkey <= 25").fetchone()[0]
        del_sum = c.execute("SELECT sum(l_orderkey) FROM generate_series(1, 10000) g, LATERAL li_row(g) r "
                            "WHERE r.l_orderkey > 2487").fetchone()[0]
    expected = {"c": 10000, "u": n_upd, "d": n_del}
    T("backlog_written")
    eng = Engine(label="e2e", decode="struct", sink=WORK + "/sink.bin")
    ok = eng.wait_events(sum(expected.values()), 180)
    s = eng.stats()
    m = eng.marks()
    T("e2e_events_received")
    time.sleep(1.5)
    slot = pgf.slot_state()
    stop_s = eng.stop()
    m2 = eng.marks()
    budget = {
        "jvm_start_s": round(m["MAIN"]["t"] - s["spawn_at"], 3) if "MAIN" in m else None,
        "engine_built_s": round(m["ENGINE_BUILT"]["t"] - m["MAIN"]["t"], 3) if "ENGINE_BUILT" in m else None,
        "connector_start_s": round(m["POLLING_STARTED"]["t"] - m["MAIN"]["t"], 3) if "POLLING_STARTED" in m
        else None,
        "first_event_s": round(s["first_event_at"] - m["POLLING_STARTED"]["t"], 3) if s.get("first_event_at")
        and "POLLING_STARTED" in m else None,
        "spawn_to_first_event_s": round(s["first_event_at"] - s["spawn_at"], 3) if s.get("first_event_at")
        else None,
        "all_backlog_s": round(s["last_event_at"] - s["spawn_at"], 3) if s.get("last_event_at") else None,
    }
    result("e2e", ok=ok, events=s.get("events"), expected=expected, ops=s.get("ops"), rows_in_table=rows,
           orderkey_sum_received=s.get("orderkey_sum"), orderkey_sum_expected=ins_sum + upd_sum + del_sum,
           sample=s.get("sample"), bytes=s.get("bytes"), batches=s.get("batches"), budget=budget,
           sink_bytes=os.path.getsize(WORK + "/sink.bin") if os.path.exists(WORK + "/sink.bin") else None,
           marks={k: round(v["t"] - RUN_T0, 3) for k, v in m2.items()},
           version_marks={k: v["detail"] for k, v in m2.items() if k in ("MAIN", "VERSION")},
           debezium_log=eng.debezium_version_lines(), slot_after=slot, stop_s=round(stop_s, 3),
           shutdown=m2.get("SHUTDOWN_DONE", {}).get("detail"), warnings=eng.interesting_log())


def tx_test(name, n_rows, props=None, heap="1g", jvm_opts=(), timeout=600, jfr=False):
    """One transaction of n_rows into a running engine. Returns the measurement record."""
    import pgfixture as pgf
    jfr_path = "%s/%s.jfr" % (WORK, name)
    if jfr:
        jvm_opts = tuple(jvm_opts) + ("-XX:StartFlightRecording=filename=%s,settings=profile,dumponexit=true"
                                      % jfr_path, "-XX:FlightRecorderOptions=stackdepth=96")
    eng, ok = start_streaming(props, heap=heap, jvm_opts=jvm_opts, label=name)
    rec = {"test": name, "rows": n_rows, "heap": heap, "jvm_opts": list(jvm_opts),
           "settings": {k: v for k, v in (props or {}).items()}, "started": ok}
    if not ok:
        rec["warnings"] = eng.interesting_log()
        rec["jvm_log_tail"] = [ln[:300] for ln in eng.log_lines()[-30:] if "password" not in ln.lower()]
        rec["consumer_rc"] = eng.consumer.poll()
        eng.stop(signal.SIGKILL)
        result("tx_test", **rec)
        return rec
    time.sleep(2)
    rec["rss_idle_mib"] = rss_mib(eng.jvm_pid)
    rec["consumer_rss_idle_mib"] = rss_mib(eng.consumer.pid)
    e0 = eng.events()
    pids = {"jvm": eng.jvm_pid, "consumer": eng.consumer.pid}
    ws = pgf.walsender_pids()
    if ws:
        pids["walsender"] = ws[0]
    cw_all = CpuWindow(pids)
    peak = {"rss": 0}
    stop_sampling = threading.Event()

    def sampler():
        while not stop_sampling.is_set():
            r = rss_mib(eng.jvm_pid)
            if r:
                peak["rss"] = max(peak["rss"], r)
            time.sleep(0.2)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()
    ins_s, commit_at = pgf.insert_rows(1, n_rows)
    cw_stream = CpuWindow(pids)
    th0 = thread_cpu(eng.jvm_pid)
    done = eng.wait_events(e0 + n_rows, timeout)
    stream_cpu = cw_stream.stop()
    rec["jvm_threads_cpu_s_commit_to_last"] = thread_delta(th0, thread_cpu(eng.jvm_pid))
    if not done and eng.jvm_pid and alive(eng.jvm_pid):
        rc, dump, _ = sh([STATE["home"] + "/bin/jcmd", str(eng.jvm_pid), "Thread.print"], timeout=60)
        blocks = [b for b in dump.split("\n\n") if ("debezium" in b or "cdc_v2" in b or "pool-" in b)]
        rec["stall_thread_dump"] = [b[:6000] if "change-event-source" in b else b[:1200] for b in blocks[:10]]
        rec["stall_jvm_stat"] = [ln for ln in eng.log_lines() if ln.startswith("CDCV2 STAT")][-1:]
        rec["stall_stats"] = eng.stats()
        rec["stall_slot"] = pgf.slot_state()
    all_cpu = cw_all.stop()
    stop_sampling.set()
    s = eng.stats()
    rec.update({
        "complete": done, "events": s.get("events", 0) - e0, "insert_s": round(ins_s, 3),
        "bytes": s.get("bytes"), "batches": s.get("batches"),
        "commit_to_last_event_s": round(s["last_event_at"] - commit_at, 3) if s.get("last_event_at") else None,
        "rows_per_s_after_commit": round(n_rows / (s["last_event_at"] - commit_at)) if done else None,
        "rows_per_s_insert_start_to_last": round(n_rows / (s["last_event_at"] - commit_at + ins_s)) if done
        else None,
        "rss_peak_mib": peak["rss"], "rss_hwm_mib": rss_mib(eng.jvm_pid, "VmHWM"),
        "consumer_rss_hwm_mib": rss_mib(eng.consumer.pid, "VmHWM"),
        "cpu_commit_to_last": stream_cpu, "cpu_insert_start_to_last": all_cpu, "cgroup": cgroup_mem(),
    })
    stop_s = eng.stop()
    rec["stop_s"] = round(stop_s, 3)
    rec["shutdown"] = eng.marks().get("SHUTDOWN_DONE", {}).get("detail")
    rec["warnings"] = eng.interesting_log(8)
    if jfr and os.path.exists(jfr_path):
        rec["jfr"] = jfr_summary(STATE["home"], jfr_path)
    result("tx_test", **rec)
    return rec


TPUT = [
    ("default_struct", {}, "1g", ()),
    ("json_noschema", {"cdc_v2.format": "json"}, "1g", ()),
    ("json_schema", {"cdc_v2.format": "json", "converter.schemas.enable": "true",
                     "key.converter.schemas.enable": "true", "value.converter.schemas.enable": "true"}, "1g", ()),
    ("batch8k_q32k", {"max.batch.size": "8192", "max.queue.size": "32768"}, "1g", ()),
    ("batch20k_q80k", {"max.batch.size": "20480", "max.queue.size": "81920"}, "1g", ()),
    ("poll10_batch8k", {"max.batch.size": "8192", "max.queue.size": "32768", "poll.interval.ms": "10"}, "1g", ()),
    ("ack_none_batch8k", {"max.batch.size": "8192", "max.queue.size": "32768", "cdc_v2.ack": "none"}, "1g", ()),
    ("parallelgc_batch8k", {"max.batch.size": "8192", "max.queue.size": "32768"}, "1g", ("-XX:+UseParallelGC",)),
]


# best non-JSON setting of run 3 (evidence/p2/jvm_on_flight/run3_e2e_tput_memory.json), used when tput did not run
MEASURED_BEST = (0, "parallelgc_batch8k", {"max.batch.size": "8192", "max.queue.size": "32768"},
                 ("-XX:+UseParallelGC",))


def phase_tput():
    T("phase_tput")
    pg_up()
    prepare()
    n = int(os.environ.get("CDC_V2_TPUT_ROWS", "1000000"))
    only = [x for x in os.environ.get("CDC_V2_TPUT_ONLY", "").split(",") if x and x != "all"]
    best = None
    for name, props, heap, opts in TPUT:
        if only and name not in only:
            continue
        rec = tx_test(name, n, props, heap, opts, timeout=300)
        r = rec.get("rows_per_s_after_commit") or 0
        if not name.startswith("json") and (best is None or r > best[0]):
            best = (r, name, props, opts)
    if best:
        STATE["best"] = best
        result("tput_best", name=best[1], rows_per_s_after_commit=best[0])


def phase_memory():
    T("phase_memory")
    pg_up()
    prepare()
    n = int(os.environ.get("CDC_V2_TPUT_ROWS", "1000000"))
    for heap in ("256m", "512m", "1g", "2g"):
        tx_test("heap_" + heap, n, {}, heap)
    tx_test("heap_256m_batch20k_q80k", n, {"max.batch.size": "20480", "max.queue.size": "81920"}, "256m")


def phase_profile():
    """Where the JVM spends its CPU: per-thread CPU and a JFR execution-sample profile of one 1M-row
    transaction, for the default settings and for cheaper value conversions."""
    T("phase_profile")
    pg_up()
    prepare()
    n = int(os.environ.get("CDC_V2_TPUT_ROWS", "1000000"))
    tx_test("profile_default", n, {}, "1g", (), timeout=300, jfr=True)
    only = [x for x in os.environ.get("CDC_V2_TPUT_ONLY", "").split(",") if x and x != "all"]
    for name, props, opts in PROFILE_VARIANTS:
        if not only or name in only:
            tx_test(name, n, props, "1g", opts, timeout=300)


PROFILE_VARIANTS = [
    ("poll10_retest", {"poll.interval.ms": "10"}, ()),
    ("poll10_batch8k_retest", {"max.batch.size": "8192", "max.queue.size": "32768", "poll.interval.ms": "10"}, ()),
    ("decimal_string", {"decimal.handling.mode": "string"}, ()),
    ("decimal_double", {"decimal.handling.mode": "double"}, ()),
    ("parallelgc_decimal_double", {"decimal.handling.mode": "double"}, ("-XX:+UseParallelGC",)),
]


def phase_big():
    T("phase_big")
    pg_up()
    prepare()
    best = STATE.get("best") or MEASURED_BEST
    tx_test("big_10m_" + best[1], int(os.environ.get("CDC_V2_BIG_ROWS", "10000000")), best[2], "1g", best[3],
            timeout=1200)


def pgbench(args, timeout=600):
    import pgfixture as pgf
    script = WORK + "/small_tx.sql"
    with open(script, "w") as f:
        f.write("INSERT INTO lineitem SELECT r.* FROM li_row(nextval('small_tx_seq')) r;\n")
    cmd = [STATE["pg"]["bindir"] + "/pgbench", "-n", "-h", pgf.SOCK, "-p", str(pgf.PORT), "-U", "postgres",
           "-f", script, *args, pgf.DB]
    rc, out, dt = sh(cmd, timeout=timeout)
    m = re.search(r"tps = ([\d.]+)", out)
    tx = re.search(r"number of transactions actually processed: (\d+)", out)
    return {"rc": rc, "seconds": round(dt, 3), "tps": float(m.group(1)) if m else None,
            "transactions": int(tx.group(1)) if tx else None, "tail": out[-300:] if rc else None}


def phase_small():
    T("phase_small")
    import pgfixture as pgf
    pg_up()
    prepare()
    best = STATE.get("best") or MEASURED_BEST
    n = int(os.environ.get("CDC_V2_SMALL_TX", "100000"))
    # (a1) backlog: n one-row transactions written while no engine runs, then caught up
    truncate()
    fresh_slot()
    pb = pgbench(["-c", "4", "-j", "2", "-t", str(n // 4)])
    eng = Engine(best[2], jvm_opts=best[3], label="small_backlog")
    eng.wait_mark("POLLING_STARTED", 120)
    pids = {"jvm": eng.jvm_pid, "consumer": eng.consumer.pid}
    time.sleep(0.2)
    ws = pgf.walsender_pids()
    if ws:
        pids["walsender"] = ws[0]
    cw = CpuWindow(pids)
    done = eng.wait_events(n, 600)
    cpu = cw.stop()
    s = eng.stats()
    rate = round(n / (s["last_event_at"] - s["first_event_at"])) if done and s.get("first_event_at") else None
    eng.stop()
    result("small_backlog", settings=best[1], transactions=n, pgbench=pb, complete=done, events=s.get("events"),
           tx_per_s_first_to_last=rate, cpu=cpu, warnings=eng.interesting_log(5))
    # (a2) live: pgbench at full speed for 30 s with the engine running alongside (all share 2 vCPUs)
    eng, ok = start_streaming(best[2], jvm_opts=best[3], label="small_live")
    e0 = eng.events()
    pids = {"jvm": eng.jvm_pid, "consumer": eng.consumer.pid}
    ws = pgf.walsender_pids()
    if ws:
        pids["walsender"] = ws[0]
    cw = CpuWindow(pids)
    t0 = time.time()
    pb = pgbench(["-c", "4", "-j", "2", "-T", "30"])
    t_end = time.time()
    e_at_end = eng.events() - e0
    cpu = cw.stop()
    total = pb.get("transactions") or 0
    done = eng.wait_events(e0 + total, 300)
    s = eng.stats()
    eng.stop()
    result("small_live", settings=best[1], pgbench=pb, events_during_load=e_at_end,
           events_per_s_during_load=round(e_at_end / (t_end - t0)), drained=done,
           drain_after_load_s=round(s["last_event_at"] - t_end, 3) if s.get("last_event_at") else None,
           cpu_during_load=cpu)


def wait_slot_inactive(timeout):
    import pgfixture as pgf
    t = time.time()
    while time.time() - t < timeout:
        s = pgf.slot_state()
        if s and not s["active"]:
            return round(time.time() - t, 3), s
        time.sleep(0.02)
    return None, pgf.slot_state()


def wait_dead(pid, timeout):
    t = time.time()
    while time.time() - t < timeout:
        if not alive(pid):
            return round(time.time() - t, 3)
        time.sleep(0.01)
    return None


def phase_supervise():
    T("phase_supervise")
    import pgfixture as pgf
    pg_up()
    prepare()
    out = {"pid1_comm": comm(1), "self_pid": os.getpid()}
    chain, p = [], os.getpid()
    while p > 1 and len(chain) < 8:
        st = status(p, ("PPid",))
        chain.append({"pid": p, "comm": comm(p)})
        p = int(st["PPid"]) if st else 1
    out["process_chain"] = chain
    result("process_tree", **out)

    # S1 clean shutdown (SIGTERM -> shutdown hook -> engine.close())
    eng, ok = start_streaming(label="s1_term")
    t = time.time()
    os.kill(eng.jvm_pid, signal.SIGTERM)
    exit_s = wait_dead(eng.jvm_pid, 60)
    inactive_s, slot = wait_slot_inactive(30)
    eng.consumer.wait(30)
    m = eng.marks()
    result("supervise_sigterm", started=ok, jvm_exit_s=exit_s, slot_inactive_after_signal_s=round(
        time.time() - t, 3) if inactive_s is not None else None, slot=slot,
        shutdown=m.get("SHUTDOWN_DONE", {}).get("detail"), consumer_rc=eng.consumer.returncode)

    # S2 SIGKILL of the JVM: what is left on the Postgres side, how long until the slot is free, restart works
    eng, ok = start_streaming(label="s2_kill")
    before = pgf.slot_state()
    t = time.time()
    os.kill(eng.jvm_pid, signal.SIGKILL)
    exit_s = wait_dead(eng.jvm_pid, 10)
    inactive_s, slot = wait_slot_inactive(120)
    inactive_total = round(time.time() - t, 3)
    eng.consumer.wait(30)
    cons = eng.stats()
    # restart on the same slot and offsets (no fresh slot): resumes
    t = time.time()
    eng2 = Engine(label="s2_restart")
    polling = eng2.wait_mark("POLLING_STARTED", 120)
    pgf.insert_rows(10 ** 11 + 10, 1)
    got = eng2.wait_events(1, 60)
    restart_s = round(time.time() - t, 3)
    eng2.stop()
    result("supervise_sigkill", started=ok, slot_before=before, jvm_exit_s=exit_s,
           slot_inactive_after_kill_s=inactive_total if inactive_s is not None else None, slot_after=slot,
           consumer_saw_eof=cons.get("eof_at") is not None, consumer_jvm_rc=cons.get("jvm_rc"),
           restart_to_event_s=restart_s, restart_polling=bool(polling), restart_event=got,
           restart_warnings=eng2.interesting_log(5))

    # S3 parent death: SIGKILL the consumer (the JVM's parent). The JVM sees EOF on stdin and halts.
    eng, ok = start_streaming(label="s3_parent")
    jpid = eng.jvm_pid
    t = time.time()
    os.kill(eng.consumer.pid, signal.SIGKILL)
    eng.consumer.wait(10)
    exit_s = wait_dead(jpid, 30)
    inactive_s, slot = wait_slot_inactive(60)
    m = eng.marks()
    result("supervise_parent_killed", started=ok, jvm_exit_after_parent_kill_s=exit_s,
           slot_inactive_after_parent_kill_s=round(time.time() - t, 3) if inactive_s is not None else None,
           jvm_marks=[k for k in m if k in ("STDIN_EOF", "SHUTDOWN_BEGIN", "SHUTDOWN_DONE", "HALT")],
           shutdown=m.get("SHUTDOWN_DONE", {}).get("detail"))

    # S4 parent death without the stdin watch: the JVM is orphaned and keeps the slot
    eng, ok = start_streaming({"cdc_v2.exit.on.stdin.eof": "false"}, label="s4_orphan")
    jpid = eng.jvm_pid
    os.kill(eng.consumer.pid, signal.SIGKILL)
    eng.consumer.wait(10)
    time.sleep(3)
    st = status(jpid)
    ppid = int(st["PPid"]) if st else None
    slot_orphan = pgf.slot_state()
    pgf.insert_rows(10 ** 11 + 20, 1)    # the orphan's next batch write hits a closed stdout
    time.sleep(5)
    still = alive(jpid)
    m = eng.marks()
    os.kill(jpid, signal.SIGKILL) if still else None
    dead = wait_dead(jpid, 10)
    zombie = status(jpid, ("State",))
    result("supervise_orphan_without_watch", started=ok, orphan_alive_3s=st is not None, orphan_ppid=ppid,
           orphan_ppid_comm=comm(ppid) if ppid else None, slot_while_orphaned=slot_orphan,
           alive_after_write_attempt=still, jvm_marks=[k for k in m if k in (
               "STDIN_EOF", "STDOUT_ERROR", "ENGINE_COMPLETED", "TASK_STOPPED", "EXIT")],
           completed=m.get("ENGINE_COMPLETED", {}).get("detail", "")[:300], killed_dead_s=dead,
           zombie_state=zombie)

    # S5 frozen JVM (SIGSTOP): the walsender keeps the slot until wal_sender_timeout (60 s)
    eng, ok = start_streaming(label="s5_frozen")
    jpid = eng.jvm_pid
    t = time.time()
    os.kill(jpid, signal.SIGSTOP)
    inactive_s, slot = wait_slot_inactive(150)
    frozen_total = round(time.time() - t, 3)
    os.kill(jpid, signal.SIGKILL)
    os.kill(jpid, signal.SIGCONT)
    eng.consumer.wait(30)
    result("supervise_sigstop", started=ok, slot_inactive_after_sigstop_s=frozen_total if inactive_s is not None
           else None, slot=slot, wal_sender_timeout="60s")


def phase_orphan():
    T("phase_orphan")
    code = ("import time,sys\n"
            "t0=time.time()\n"
            "for i in range(120):\n"
            "    print('ORPHAN tick %d %.1f' % (i, time.time()-t0), flush=True)\n"
            "    time.sleep(1)\n")
    p = subprocess.Popen([sys.executable, "-c", code], start_new_session=True)
    result("orphan_spawned", child_pid=p.pid, pid1_comm=comm(1))
    time.sleep(2.5)
    print("ORPHAN parent_exit %.3f" % time.time(), flush=True)
    os._exit(0)


def main():
    T("driver_start")
    result("driver", modes=MODES, jdk=JDK_ID, cpu_max=open("/sys/fs/cgroup/cpu.max").read().strip(),
           mem_max=open("/sys/fs/cgroup/memory.max").read().strip(), os_cpu_count=os.cpu_count())
    os.makedirs(WORK, exist_ok=True)
    phases = {"jdk": phase_jdk, "e2e": phase_e2e, "tput": phase_tput, "memory": phase_memory, "small": phase_small,
              "big": phase_big, "profile": phase_profile, "supervise": phase_supervise, "orphan": phase_orphan}
    rc = 0
    for m in MODES:
        try:
            phases[m]()
        except Exception as exc:  # noqa: BLE001
            import traceback
            result("phase_error", mode=m, error="%s: %s" % (type(exc).__name__, exc),
                   trace=traceback.format_exc()[-1500:])
            rc = 1
    if STATE["pg"]:
        import pgfixture as pgf
        pgf.stop(STATE["pg"]["bindir"], "immediate")
    T("driver_end")
    return rc


if __name__ == "__main__":
    sys.exit(main())
