"""P2 platform probe (cdc_v2_spike_hello). Prints platform facts as JSON lines prefixed with `FACT `.

This file is public. It prints environment variable NAMES only, never values; it never prints host names,
user names (other than a short list of generic system accounts) or the names of databases outside cdc_v2_*.
Secret checks print only presence and an HMAC keyed with a per-run nonce from the Flight config.

Mode (config CDC_V2_PROBE_MODE, overridable per run): facts | net | ticks | exception | exit3
"""
import time

T0 = time.time()
print("FACT " + '{"first_line_unix": %.3f}' % T0, flush=True)

import hashlib  # noqa: E402
import hmac  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import platform  # noqa: E402
import shutil  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import tempfile  # noqa: E402
import urllib.request  # noqa: E402

GENERIC_USERS = {"root", "nobody", "app", "flight", "flights", "runner", "user", "python", "worker", "daemon"}
REPO = "https://github.com/Alex-Monahan/cdc_flight_v2"


def fact(key, value):
    print("FACT " + json.dumps({key: value}, default=str), flush=True)


def sh(cmd, timeout=60):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return {"rc": p.returncode, "out": (p.stdout + p.stderr).strip()[:400]}
    except Exception as exc:  # noqa: BLE001
        return {"error": type(exc).__name__ + ": " + str(exc)[:200]}


def read(path, limit=2000):
    try:
        with open(path) as f:
            return f.read(limit).strip()
    except Exception as exc:  # noqa: BLE001
        return "unreadable: " + type(exc).__name__


def keyed(value):
    nonce = os.environ.get("CDC_V2_PROBE_NONCE", "")
    if not nonce or value is None:
        return None
    return hmac.new(nonce.encode(), value.encode(), hashlib.sha256).hexdigest()[:16]


def facts_system():
    fact("python", {"version": sys.version.split()[0], "executable": sys.executable, "prefix": sys.prefix})
    fact("platform", {"system": platform.system(), "release": platform.release(), "machine": platform.machine(),
                      "libc": platform.libc_ver()})
    fact("os_release", dict(line.split("=", 1) for line in read("/etc/os-release").splitlines() if "=" in line))
    cpus = {"os.cpu_count": os.cpu_count()}
    try:
        cpus["sched_getaffinity"] = len(os.sched_getaffinity(0))
    except Exception:  # noqa: BLE001
        pass
    cpus["cgroup_cpu.max"] = read("/sys/fs/cgroup/cpu.max")
    cpus["model"] = next((ln.split(":", 1)[1].strip() for ln in read("/proc/cpuinfo", 20000).splitlines()
                          if ln.startswith("model name")), None)
    fact("cpu", cpus)
    mem = {ln.split(":")[0]: ln.split(":")[1].strip() for ln in read("/proc/meminfo").splitlines()
           if ln.split(":")[0] in ("MemTotal", "MemAvailable", "SwapTotal")}
    mem["cgroup_memory.max"] = read("/sys/fs/cgroup/memory.max")
    mem["cgroup_memory.current"] = read("/sys/fs/cgroup/memory.current")
    fact("memory", mem)
    disks = {}
    for path in ("/", "/tmp", os.getcwd(), os.path.expanduser("~"), "/dev/shm"):
        try:
            u = shutil.disk_usage(path)
            ok = os.access(path, os.W_OK)
            disks[path] = {"total_gb": round(u.total / 1e9, 1), "free_gb": round(u.free / 1e9, 1), "writable": ok}
        except Exception as exc:  # noqa: BLE001
            disks[path] = {"error": type(exc).__name__}
    fact("disk", disks)
    fact("mounts", [ln.split()[1:4] for ln in read("/proc/mounts", 20000).splitlines()
                    if ln.split()[1] in ("/", "/tmp", "/dev/shm", "/home", "/root") or ln.split()[1].startswith("/app")])
    # write speed on /tmp (256 MB), then remove
    try:
        t = time.time()
        with tempfile.NamedTemporaryFile(dir="/tmp", delete=True) as f:
            block = os.urandom(1 << 20)
            for _ in range(256):
                f.write(block)
            f.flush()
            os.fsync(f.fileno())
        fact("tmp_write_256mb_s", round(time.time() - t, 2))
    except Exception as exc:  # noqa: BLE001
        fact("tmp_write_256mb_s", "error: " + type(exc).__name__)
    try:
        import pwd
        name = pwd.getpwuid(os.getuid()).pw_name
    except Exception:  # noqa: BLE001
        name = None
    fact("user", {"uid": os.getuid(), "gid": os.getgid(), "euid": os.geteuid(),
                  "name": name if name in GENERIC_USERS else "<not generic, redacted>",
                  "home": os.path.expanduser("~"), "cwd": os.getcwd(), "script": os.path.abspath(__file__),
                  "argv": sys.argv, "pid": os.getpid(), "ppid": os.getppid()})
    fact("process_tree", sh(["ps", "-eo", "pid,ppid,uid,etime,comm"]))
    bins = {}
    for b in ("git", "curl", "wget", "java", "javac", "tar", "gzip", "unzip", "xz", "zstd", "gcc", "cc", "make",
              "apt-get", "sudo", "pip", "uv", "psql", "pg_dump", "ps", "kill", "timeout", "bash", "ssh"):
        bins[b] = shutil.which(b)
    fact("binaries", bins)
    fact("git_version", sh(["git", "--version"]))
    fact("java_version", sh(["java", "-version"]) if bins["java"] else None)
    fact("env_names", sorted(os.environ))
    fact("path", os.environ.get("PATH"))
    fact("limits", sh(["sh", "-c", "ulimit -a"]))
    fact("time", {"tz_env": os.environ.get("TZ"), "localtime": time.strftime("%Z %z")})


def facts_python_packages():
    from importlib import metadata
    dists = sorted({(d.metadata["Name"] or "?").lower() + "==" + d.version for d in metadata.distributions()})
    fact("packages_count", len(dists))
    fact("packages", dists)
    try:
        fact("duckdb_installed", metadata.version("duckdb"))
    except metadata.PackageNotFoundError:
        fact("duckdb_installed", None)


def facts_motherduck():
    fact("token_env", {k: (k in os.environ) for k in ("MOTHERDUCK_TOKEN", "motherduck_token")})
    try:
        import duckdb
    except Exception as exc:  # noqa: BLE001
        fact("motherduck", {"error": "import duckdb: " + str(exc)[:200]})
        return
    t = time.time()
    try:
        con = duckdb.connect("md:")
        info = con.execute("SELECT username, org_type, region FROM md_user_info()").fetchone()
        dbs = con.execute("SELECT count(*), count(*) FILTER (WHERE starts_with(database_name, 'cdc_v2')) "
                          "FROM duckdb_databases() WHERE NOT internal").fetchone()
        fact("motherduck", {"connect_s": round(time.time() - t, 2), "client": duckdb.__version__,
                            "server": con.execute("SELECT version()").fetchone()[0],
                            "username_hmac": keyed(info[0]), "org_type": info[1], "region": info[2],
                            "databases_total": dbs[0], "databases_cdc_v2": dbs[1],
                            "databases_other": dbs[0] - dbs[1]})
        own = os.environ.get("CDC_V2_PROBE_FLIGHT_NAME")
        if own:
            rows = con.execute("SELECT flight_id FROM MD_LIST_FLIGHTS(\"LIMIT\" := 1000, owner_only := true) "
                               "WHERE flight_name = ?", [own]).fetchall()
            fact("flight_functions_inside", {"md_list_flights_ok": True, "own_flight_found": len(rows) == 1})
            if rows:
                runs = con.execute("SELECT run_number, status FROM MD_LIST_FLIGHT_RUNS(flight_id := '%s') "
                                   "ORDER BY run_number DESC LIMIT 3" % rows[0][0]).fetchall()
                fact("own_runs_seen_inside", [[r[0], r[1]] for r in runs])
        con.close()
    except Exception as exc:  # noqa: BLE001
        fact("motherduck", {"error": type(exc).__name__ + ": " + str(exc)[:300]})


def facts_secrets():
    sec, key = "cdc_v2_spike_secret", "CDC_V2_PROBE_SECRET"
    names = [k for k in os.environ if "PROBE_SECRET" in k or k.startswith("cdc_v2_spike_secret")]
    fact("secret", {"names_present": sorted(names), "namespaced_hmac": keyed(os.environ.get(sec + "_" + key)),
                    "bare_hmac": keyed(os.environ.get(key))})


def http_time(url, max_bytes, timeout=60):
    t = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "cdc_v2-probe"})
        if max_bytes:
            req.add_header("Range", "bytes=0-%d" % (max_bytes - 1))
        n = 0
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status = r.status
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    break
                n += len(chunk)
                if max_bytes and n >= max_bytes:
                    break
        dt = time.time() - t
        return {"status": status, "bytes": n, "s": round(dt, 2), "MB_per_s": round(n / 1e6 / dt, 1) if dt else None}
    except Exception as exc:  # noqa: BLE001
        return {"error": type(exc).__name__ + ": " + str(exc)[:200], "s": round(time.time() - t, 2)}


def facts_network():
    fact("egress_ip", read_url("https://checkip.amazonaws.com"))
    d = tempfile.mkdtemp(prefix="cdc_v2_clone_")
    t = time.time()
    r = sh(["git", "clone", "--depth", "1", REPO, d + "/repo"], timeout=120)
    fact("git_clone_public_repo", {"rc": r.get("rc"), "s": round(time.time() - t, 2),
                                   "head": sh(["git", "-C", d + "/repo", "rev-parse", "HEAD"]).get("out"),
                                   "error": r.get("out") if r.get("rc") else None})
    shutil.rmtree(d, ignore_errors=True)
    fact("maven_central", http_time(
        "https://repo1.maven.org/maven2/io/debezium/debezium-core/maven-metadata.xml", 0, 30))
    fact("maven_central_jar_download", http_time(
        "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.0.0.Final/"
        "debezium-connector-postgres-3.0.0.Final.jar", 0, 60))
    fact("jdk_download_adoptium_100mb", http_time(
        "https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse", 100_000_000, 120))
    fact("jdk_download_corretto_100mb", http_time(
        "https://corretto.aws/downloads/latest/amazon-corretto-21-x64-linux-jdk.tar.gz", 100_000_000, 120))
    fact("pypi", http_time("https://pypi.org/simple/duckdb/", 0, 30))


def read_url(url):
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return r.read(100).decode().strip()
    except Exception as exc:  # noqa: BLE001
        return "error: " + type(exc).__name__


def main():
    mode = os.environ.get("CDC_V2_PROBE_MODE", "facts")
    fact("mode", mode)
    fact("import_done_s", round(time.time() - T0, 3))
    if mode == "exception":
        raise RuntimeError("cdc_v2 probe: deliberate exception")
    if mode == "exit3":
        print("cdc_v2 probe: exiting with status 3", flush=True)
        sys.exit(3)
    if mode == "ticks":
        for i in range(int(os.environ.get("CDC_V2_PROBE_TICKS", "20"))):
            print("TICK %d %.3f" % (i, time.time()), flush=True)
            time.sleep(1)
        return
    if mode in ("facts", "all"):
        facts_system()
        facts_python_packages()
        facts_motherduck()
        facts_secrets()
    if mode in ("net", "all"):
        facts_network()
        facts_motherduck()
    fact("total_s", round(time.time() - T0, 2))


if __name__ == "__main__":
    main()
