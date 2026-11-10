"""JVM artifacts for the engine: a JDK, the locked Debezium jars, and our classes compiled with `javac`.

`prepare(cache_dir)` returns `JvmArtifacts(java, classpath, ...)`. It is idempotent and safe to call from several
processes at once (compiled classes are written to a temporary directory and renamed into place):

- jars: the 73 jars of `jvm/debezium.lock.json`, fetched and verified through `cdc_v2.artifacts` into
  `<cache>/jars` (already verified jars are reused); a jar that is not in the lock is refused;
- JDK: on Linux x86-64 (the Flight) the Temurin 21 tarball of `jvm/jdk.lock.json`, verified before unpacking, into
  `<cache>/jdk`. Elsewhere (a development machine: the lock holds Linux tarballs only) the JDK is taken from
  `java_home`, `$CDC_V2_JAVA_HOME`, `$JAVA_HOME` or the `javac` on the PATH; that choice is written into the
  result (`jdk_source`) so evidence can name it;
- classes: every `jvm/src/cdc_v2/engine/*.java`, compiled with `javac --release 17` into
  `<cache>/engine-classes/<key>`, where the key is a hash of the sources, the jar list and the `javac` version.

Standard library only, so the Flight bootstrap can use it before the pipeline's dependencies exist.
"""
from __future__ import annotations

import glob
import hashlib
import os
import platform
import shutil
import subprocess
import tempfile
import uuid
from dataclasses import dataclass

from cdc_v2 import artifacts

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
JVM_DIR = os.path.join(REPO, "jvm")
SRC_DIR = os.path.join(JVM_DIR, "src", "cdc_v2", "engine")
MAIN_CLASS = "cdc_v2.engine.CdcEngine"
DEFAULT_CACHE = os.path.expanduser("~/.cache/cdc_flight_v2")
JDK_ID = "temurin-21"
JAVA_RELEASE = "17"


class JvmError(RuntimeError):
    """The JVM artifacts could not be prepared."""


@dataclass(frozen=True)
class JvmArtifacts:
    java: str                 # path of the java executable
    classpath: str            # compiled classes first, then the locked jars in lock order
    classes_dir: str
    jars_dir: str
    java_version: str
    jdk_source: str           # "lock:temurin-21" or "local:<how it was found>"


def _run(argv, timeout):
    p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout + p.stderr


def _local_jdk(java_home):
    for how, home in (("argument", java_home), ("CDC_V2_JAVA_HOME", os.environ.get("CDC_V2_JAVA_HOME")),
                      ("JAVA_HOME", os.environ.get("JAVA_HOME"))):
        if home and os.path.exists(os.path.join(home, "bin", "javac")):
            return os.path.join(home, "bin"), "local:" + how
    javac = shutil.which("javac")
    if javac:
        return os.path.dirname(os.path.realpath(javac)), "local:PATH"
    raise JvmError("no JDK found: pass java_home or set CDC_V2_JAVA_HOME")


def _locked_jdk(cache_dir):
    lock = artifacts.load_lock(os.path.join(JVM_DIR, "jdk.lock.json"))
    art = artifacts.select(lock, JDK_ID)["artifacts"][0]
    root = os.path.join(cache_dir, "jdk")
    home = os.path.join(root, art.get("id", JDK_ID))
    marker = os.path.join(home, ".cdc_v2_verified")
    if not (os.path.exists(marker) and open(marker).read().strip() == art["sha256"]):
        home, _ = artifacts.install_jdk(lock, JDK_ID, root)
        with open(marker, "w") as f:
            f.write(art["sha256"])
    return os.path.join(home, "bin"), "lock:" + JDK_ID


def sources():
    """The Java sources of the engine, sorted."""
    return sorted(glob.glob(os.path.join(SRC_DIR, "*.java")))


def prepare(cache_dir=DEFAULT_CACHE, java_home=None, fetch_timeout=120, compile_timeout=300):
    """Prepare JDK, jars and compiled classes; return JvmArtifacts."""
    cache_dir = os.path.abspath(os.path.expanduser(cache_dir))
    os.makedirs(cache_dir, exist_ok=True)
    lock = artifacts.load_lock(os.path.join(JVM_DIR, "debezium.lock.json"))
    jars_dir = os.path.join(cache_dir, "jars")
    artifacts.fetch(lock, jars_dir, workers=16, timeout=fetch_timeout)
    artifacts.verify_dir(lock, jars_dir)
    jar_cp = artifacts.classpath(lock, jars_dir)

    if java_home is None and platform.system() == "Linux" and platform.machine() in ("x86_64", "AMD64"):
        bindir, source = _locked_jdk(cache_dir)
    else:
        bindir, source = _local_jdk(java_home)
    java, javac = os.path.join(bindir, "java"), os.path.join(bindir, "javac")
    rc, version = _run([javac, "-version"], 60)
    if rc != 0:
        raise JvmError("javac -version failed: %s" % version.strip()[:500])
    version = version.strip()

    h = hashlib.sha256()
    h.update(version.encode())
    h.update(JAVA_RELEASE.encode())
    for a in lock["artifacts"]:
        h.update(a["sha256"].encode())
    srcs = sources()
    for s in srcs:
        h.update(os.path.basename(s).encode())
        with open(s, "rb") as f:
            h.update(hashlib.sha256(f.read()).digest())
    key = h.hexdigest()[:24]
    classes_root = os.path.join(cache_dir, "engine-classes")
    classes = os.path.join(classes_root, key)
    if not os.path.exists(os.path.join(classes, ".complete")):
        os.makedirs(classes_root, exist_ok=True)
        tmp = tempfile.mkdtemp(prefix=".%s-" % key, dir=classes_root)
        try:
            rc, out = _run([javac, "--release", JAVA_RELEASE, "-nowarn", "-encoding", "UTF-8", "-cp", jar_cp,
                            "-d", tmp] + srcs, compile_timeout)
            if rc != 0:
                raise JvmError("javac failed:\n%s" % out[-4000:])
            open(os.path.join(tmp, ".complete"), "w").close()
            try:
                os.rename(tmp, classes)
            except OSError:
                if not os.path.exists(os.path.join(classes, ".complete")):
                    # a half-written directory from an older crash: replace it
                    aside = classes + ".old-" + uuid.uuid4().hex[:8]
                    os.rename(classes, aside)
                    os.rename(tmp, classes)
                    shutil.rmtree(aside, ignore_errors=True)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    return JvmArtifacts(java=java, classpath=classes + os.pathsep + jar_cp, classes_dir=classes, jars_dir=jars_dir,
                        java_version=version, jdk_source=source)
