"""Locked third-party artifacts: load a lock file, fetch in parallel, verify SHA-256, fail closed.

A lock file (format ``cdc_v2.artifact-lock/1``) lists every artifact with its file name, HTTPS URL, size and SHA-256.
``jvm/debezium.lock.json`` holds the stock Debezium runtime jars (Maven Central), ``jvm/jdk.lock.json`` the JDK
tarballs. Nothing third-party is committed to the repository; the Flight downloads exactly what is locked.

Fail closed: an artifact whose size or digest differs from the lock is deleted and :class:`FetchError` is raised;
nothing unverified is ever left under its final name. :func:`verify_dir` additionally refuses a directory that holds
a jar the lock does not list, so no class can be added or shadowed on the classpath (SPEC R2).

Standard library only (it runs in the Flight bootstrap's environment).
"""
import concurrent.futures
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import time
import urllib.request
import uuid

LOCK_FORMAT = "cdc_v2.artifact-lock/1"
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class LockError(ValueError):
    """The lock file is malformed."""


class FetchError(RuntimeError):
    """At least one artifact could not be fetched and verified. ``failures`` maps name -> reason."""

    def __init__(self, failures):
        self.failures = dict(failures)
        super().__init__("%d artifact(s) failed: %s" % (len(self.failures), "; ".join(
            "%s: %s" % kv for kv in sorted(self.failures.items()))))


def validate_lock(lock, allow_http=False):
    """Raise LockError unless ``lock`` (a parsed lock document) is well formed. Returns the artifact list."""
    if not isinstance(lock, dict) or lock.get("format") != LOCK_FORMAT:
        raise LockError("not a %s document" % LOCK_FORMAT)
    arts = lock.get("artifacts")
    if not isinstance(arts, list) or not arts:
        raise LockError("artifacts must be a non-empty list")
    seen = set()
    for i, a in enumerate(arts):
        if not isinstance(a, dict):
            raise LockError("artifact %d is not an object" % i)
        name, url, size, digest = a.get("name"), a.get("url"), a.get("size"), a.get("sha256")
        if not isinstance(name, str) or not _NAME.match(name) or ".." in name:
            raise LockError("artifact %d: unsafe or missing name %r" % (i, name))
        if name in seen:
            raise LockError("duplicate artifact name %r" % name)
        seen.add(name)
        if not isinstance(url, str) or not (url.startswith("https://") or (allow_http and url.startswith("http://"))):
            raise LockError("%s: url must be https" % name)
        if not url.rsplit("/", 1)[-1].endswith(name.rsplit(".", 1)[-1]):
            raise LockError("%s: url does not end in the same file type" % name)
        if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
            raise LockError("%s: size must be a positive integer" % name)
        if not isinstance(digest, str) or not _SHA256.match(digest):
            raise LockError("%s: sha256 must be 64 lower-case hex digits" % name)
    return arts


def load_lock(path, allow_http=False):
    with open(path, encoding="utf-8") as f:
        lock = json.load(f)
    validate_lock(lock, allow_http=allow_http)
    return lock


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _matches(path, art):
    try:
        return os.path.getsize(path) == art["size"] and sha256_file(path) == art["sha256"]
    except OSError:
        return False


def _download(art, dest_dir, timeout, retries, opener):
    """Download one artifact to dest_dir/name, verified. Returns a timing record; raises on failure."""
    final = os.path.join(dest_dir, art["name"])
    t0 = time.monotonic()
    if os.path.exists(final):
        if _matches(final, art):
            return {"name": art["name"], "bytes": art["size"], "seconds": time.monotonic() - t0, "reused": True}
        os.remove(final)  # present but wrong: never trust it
    last = None
    for attempt in range(retries + 1):
        part = os.path.join(dest_dir, ".%s.part-%s" % (art["name"], uuid.uuid4().hex[:8]))
        try:
            h = hashlib.sha256()
            n = 0
            with opener(art["url"], timeout=timeout) as r, open(part, "wb") as f:
                while True:
                    block = r.read(1 << 20)
                    if not block:
                        break
                    n += len(block)
                    if n > art["size"]:
                        raise FetchError({art["name"]: "larger than the locked size %d" % art["size"]})
                    h.update(block)
                    f.write(block)
            if n != art["size"]:
                raise FetchError({art["name"]: "size %d, locked %d" % (n, art["size"])})
            if h.hexdigest() != art["sha256"]:
                raise FetchError({art["name"]: "sha256 mismatch (got %s)" % h.hexdigest()})
            os.replace(part, final)
            return {"name": art["name"], "bytes": n, "seconds": time.monotonic() - t0, "reused": False,
                    "attempts": attempt + 1}
        except FetchError:
            _rm(part)
            raise  # a wrong digest or size is not retried: the source serves something else
        except Exception as exc:  # noqa: BLE001 - network errors are retried, then reported
            _rm(part)
            last = exc
            if attempt < retries:
                time.sleep(0.2 * (attempt + 1))
    raise FetchError({art["name"]: "%s: %s" % (type(last).__name__, last)})


def _rm(path):
    try:
        os.remove(path)
    except OSError:
        pass


def fetch(lock, dest_dir, workers=8, timeout=60, retries=2, opener=None, allow_http=False):
    """Fetch every artifact of ``lock`` into ``dest_dir`` in parallel and verify it.

    Returns ``{"seconds", "bytes", "count", "artifacts": [...]}``. Raises FetchError if any artifact fails; in that
    case every artifact that failed has no file under its final name (verified ones that succeeded are kept, they are
    correct by construction)."""
    arts = validate_lock(lock, allow_http=allow_http)
    opener = opener or urllib.request.urlopen
    os.makedirs(dest_dir, exist_ok=True)
    t0 = time.monotonic()
    results, failures = [], {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = {ex.submit(_download, a, dest_dir, timeout, retries, opener): a for a in arts}
        for fut in concurrent.futures.as_completed(futs):
            a = futs[fut]
            try:
                results.append(fut.result())
            except FetchError as exc:
                failures.update(exc.failures)
            except Exception as exc:  # noqa: BLE001
                failures[a["name"]] = "%s: %s" % (type(exc).__name__, exc)
    if failures:
        for name in failures:
            _rm(os.path.join(dest_dir, name))
        raise FetchError(failures)
    results.sort(key=lambda r: r["name"])
    return {"seconds": time.monotonic() - t0, "bytes": sum(r["bytes"] for r in results), "count": len(results),
            "artifacts": results}


def verify_dir(lock, dest_dir, suffix=".jar"):
    """Raise FetchError unless dest_dir holds exactly the locked ``*suffix`` files, each with the locked digest."""
    arts = validate_lock(lock, allow_http=True)
    failures = {}
    wanted = {a["name"]: a for a in arts if a["name"].endswith(suffix)}
    present = {n for n in os.listdir(dest_dir) if n.endswith(suffix)}
    for name in sorted(present - set(wanted)):
        failures[name] = "not in the lock"
    for name, a in wanted.items():
        if name not in present:
            failures[name] = "missing"
        elif not _matches(os.path.join(dest_dir, name), a):
            failures[name] = "size or sha256 differs from the lock"
    if failures:
        raise FetchError(failures)
    return sorted(wanted)


def classpath(lock, dest_dir, suffix=".jar"):
    """The classpath string of the locked jars, in lock order."""
    return os.pathsep.join(os.path.join(dest_dir, a["name"]) for a in validate_lock(lock, allow_http=True)
                           if a["name"].endswith(suffix))


def select(lock, name):
    """A copy of ``lock`` restricted to the one artifact called ``name``."""
    arts = [a for a in validate_lock(lock, allow_http=True) if a["name"] == name or a.get("id") == name]
    if not arts:
        raise LockError("no artifact %r in the lock" % name)
    out = dict(lock)
    out["artifacts"] = arts[:1]
    return out


def install_jdk(lock, ident, dest_root, workers=1, use_tar=True, opener=None, allow_http=False):
    """Fetch the JDK tarball ``ident`` (artifact id or name) verified, unpack it under dest_root and return
    ``(java_home, timings)``. The tarball is verified before anything is unpacked."""
    one = select(lock, ident)
    art = one["artifacts"][0]
    dl_dir = os.path.join(dest_root, "download")
    t0 = time.monotonic()
    fetched = fetch(one, dl_dir, workers=workers, opener=opener, allow_http=allow_http)
    t1 = time.monotonic()
    home = os.path.join(dest_root, art.get("id", art["name"].split(".tar")[0]))
    if os.path.isdir(home):
        shutil.rmtree(home)
    os.makedirs(home)
    tarball = os.path.join(dl_dir, art["name"])
    if use_tar and shutil.which("tar"):
        subprocess.run(["tar", "-xzf", tarball, "-C", home, "--strip-components=1", "--no-same-owner"], check=True)
    else:
        with tarfile.open(tarball) as tf:
            members = []
            for m in tf.getmembers():
                parts = m.name.split("/", 1)
                if len(parts) < 2 or not parts[1]:
                    continue
                m.name = parts[1]
                members.append(m)
            tf.extractall(home, members=members, filter="data")
    t2 = time.monotonic()
    if not os.path.exists(os.path.join(home, "bin", "java")):
        raise FetchError({art["name"]: "no bin/java after unpacking"})
    return home, {"download_s": t1 - t0, "unpack_s": t2 - t1, "bytes": fetched["bytes"],
                  "unpacked_bytes": _du(home)}


def _du(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_size
            except OSError:
                pass
    return total
