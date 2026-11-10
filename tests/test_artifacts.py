"""Unit tests for cdc_v2.artifacts: lock validation, parallel fetch with SHA-256 verification, fail-closed behaviour."""
import copy
import functools
import hashlib
import http.server
import io
import json
import os
import tarfile
import threading

import pytest

from cdc_v2 import artifacts as A

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture()
def server(tmp_path):
    root = tmp_path / "srv"
    root.mkdir()
    handler = functools.partial(_QuietHandler, directory=str(root))
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield root, "http://127.0.0.1:%d" % httpd.server_address[1]
    httpd.shutdown()
    httpd.server_close()


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):  # noqa: D401
        pass


def _art(root, base, name, data, **override):
    (root / name).write_bytes(data)
    a = {"name": name, "url": "%s/%s" % (base, name), "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    a.update(override)
    return a


def _lock(arts):
    return {"format": A.LOCK_FORMAT, "artifacts": arts}


# ---- validation ---------------------------------------------------------------------------------------------------

def _valid():
    return _lock([{"name": "a-1.jar", "url": "https://example.invalid/a-1.jar", "size": 3, "sha256": "0" * 64}])


def test_valid_lock_passes():
    assert len(A.validate_lock(_valid())) == 1


@pytest.mark.parametrize("mutate, message", [
    (lambda l: l.update(format="other"), "not a"),
    (lambda l: l.update(artifacts=[]), "non-empty"),
    (lambda l: l["artifacts"][0].update(name="../evil.jar"), "unsafe"),
    (lambda l: l["artifacts"][0].update(name="dir/evil.jar"), "unsafe"),
    (lambda l: l["artifacts"][0].update(name=".hidden.jar"), "unsafe"),
    (lambda l: l["artifacts"][0].update(url="http://example.invalid/a-1.jar"), "https"),
    (lambda l: l["artifacts"][0].update(url="https://example.invalid/a-1.zip"), "file type"),
    (lambda l: l["artifacts"][0].update(size=0), "size"),
    (lambda l: l["artifacts"][0].update(size="3"), "size"),
    (lambda l: l["artifacts"][0].update(size=True), "size"),
    (lambda l: l["artifacts"][0].update(sha256="A" * 64), "sha256"),
    (lambda l: l["artifacts"][0].update(sha256="0" * 63), "sha256"),
    (lambda l: l["artifacts"].append(dict(l["artifacts"][0])), "duplicate"),
])
def test_invalid_locks_are_refused(mutate, message):
    lock = _valid()
    mutate(lock)
    with pytest.raises(A.LockError, match=message):
        A.validate_lock(lock)


@pytest.mark.parametrize("path", ["jvm/debezium.lock.json", "jvm/jdk.lock.json"])
def test_committed_locks_are_valid(path):
    lock = A.load_lock(os.path.join(REPO, path))
    assert all(a["url"].startswith("https://") for a in lock["artifacts"])


def test_debezium_lock_is_stock_debezium_from_maven_central():
    lock = A.load_lock(os.path.join(REPO, "jvm/debezium.lock.json"))
    v = lock["debezium_version"]
    assert v.endswith(".Final")
    names = {a["name"] for a in lock["artifacts"]}
    for art in ("debezium-api", "debezium-embedded", "debezium-connector-postgres"):
        assert "%s-%s.jar" % (art, v) in names
    assert all(a["url"].startswith("https://repo1.maven.org/maven2/") for a in lock["artifacts"])
    assert lock["total_size"] == sum(a["size"] for a in lock["artifacts"])
    # every Debezium artifact carries the same pinned version
    for a in lock["artifacts"]:
        if a["coords"].startswith("io.debezium:"):
            assert a["coords"].endswith(":" + v)


# ---- fetch --------------------------------------------------------------------------------------------------------

def test_fetch_downloads_and_verifies_in_parallel(server, tmp_path):
    root, base = server
    arts = [_art(root, base, "j%02d.jar" % i, os.urandom(1000 + i)) for i in range(20)]
    out = tmp_path / "out"
    rep = A.fetch(_lock(arts), str(out), workers=8, allow_http=True)
    assert rep["count"] == 20 and rep["bytes"] == sum(a["size"] for a in arts)
    for a in arts:
        assert A.sha256_file(str(out / a["name"])) == a["sha256"]
    assert not [n for n in os.listdir(out) if n.startswith(".")]  # no partial files left
    assert A.verify_dir(_lock(arts), str(out)) == sorted(a["name"] for a in arts)


def test_digest_mismatch_fails_closed(server, tmp_path):
    root, base = server
    good = _art(root, base, "good.jar", b"good bytes")
    bad = _art(root, base, "bad.jar", b"served bytes", sha256=hashlib.sha256(b"locked bytes").hexdigest())
    out = tmp_path / "out"
    with pytest.raises(A.FetchError) as ei:
        A.fetch(_lock([good, bad]), str(out), allow_http=True)
    assert set(ei.value.failures) == {"bad.jar"}
    assert "sha256" in ei.value.failures["bad.jar"]
    assert not (out / "bad.jar").exists()
    assert not [n for n in os.listdir(out) if n.startswith(".")]


def test_same_length_tampering_is_detected(server, tmp_path):
    root, base = server
    a = _art(root, base, "x.jar", b"A" * 4096)
    (root / "x.jar").write_bytes(b"B" * 4096)  # same size, different content
    with pytest.raises(A.FetchError, match="sha256"):
        A.fetch(_lock([a]), str(tmp_path / "out"), allow_http=True)
    assert not (tmp_path / "out" / "x.jar").exists()


@pytest.mark.parametrize("served, locked_size, message", [
    (b"12345", 4, "larger than"),
    (b"123", 4, "size 3, locked 4"),
])
def test_size_mismatch_fails_closed(server, tmp_path, served, locked_size, message):
    root, base = server
    a = _art(root, base, "s.jar", served, size=locked_size)
    with pytest.raises(A.FetchError, match=message):
        A.fetch(_lock([a]), str(tmp_path / "out"), allow_http=True)
    assert not (tmp_path / "out" / "s.jar").exists()


def test_missing_artifact_fails_after_retries(server, tmp_path):
    _root, base = server
    a = {"name": "gone.jar", "url": base + "/gone.jar", "size": 1, "sha256": "0" * 64}
    calls = []

    def opener(url, timeout):
        calls.append(url)
        import urllib.request
        return urllib.request.urlopen(url, timeout=timeout)

    with pytest.raises(A.FetchError, match="HTTPError"):
        A.fetch(_lock([a]), str(tmp_path / "out"), retries=2, opener=opener, allow_http=True)
    assert len(calls) == 3


def test_transient_error_is_retried(server, tmp_path):
    root, base = server
    a = _art(root, base, "t.jar", b"payload")
    state = {"n": 0}

    def flaky(url, timeout):
        state["n"] += 1
        if state["n"] == 1:
            raise ConnectionResetError("reset")
        import urllib.request
        return urllib.request.urlopen(url, timeout=timeout)

    rep = A.fetch(_lock([a]), str(tmp_path / "out"), retries=1, opener=flaky, allow_http=True)
    assert rep["artifacts"][0]["attempts"] == 2


def test_verified_file_is_reused_and_corrupt_one_replaced(server, tmp_path):
    root, base = server
    a = _art(root, base, "r.jar", b"real content")
    out = tmp_path / "out"
    A.fetch(_lock([a]), str(out), allow_http=True)
    rep = A.fetch(_lock([a]), str(out), allow_http=True)
    assert rep["artifacts"][0]["reused"] is True
    (out / "r.jar").write_bytes(b"real contenX")  # corrupt in place
    rep = A.fetch(_lock([a]), str(out), allow_http=True)
    assert rep["artifacts"][0]["reused"] is False
    assert A.sha256_file(str(out / "r.jar")) == a["sha256"]


def test_verify_dir_refuses_extra_missing_and_modified_jars(server, tmp_path):
    root, base = server
    a = _art(root, base, "a.jar", b"aaa")
    b = _art(root, base, "b.jar", b"bbb")
    out = tmp_path / "out"
    A.fetch(_lock([a, b]), str(out), allow_http=True)
    (out / "shadow.jar").write_bytes(b"class io.debezium.Something")
    with pytest.raises(A.FetchError) as ei:
        A.verify_dir(_lock([a, b]), str(out))
    assert ei.value.failures == {"shadow.jar": "not in the lock"}
    os.remove(out / "shadow.jar")
    os.remove(out / "a.jar")
    (out / "b.jar").write_bytes(b"bbX")
    with pytest.raises(A.FetchError) as ei:
        A.verify_dir(_lock([a, b]), str(out))
    assert ei.value.failures == {"a.jar": "missing", "b.jar": "size or sha256 differs from the lock"}


def test_classpath_follows_lock_order(tmp_path):
    lock = _lock([{"name": n, "url": "https://x.invalid/" + n, "size": 1, "sha256": "0" * 64}
                  for n in ("b.jar", "a.jar", "c.tar.gz")])
    cp = A.classpath(lock, "/jars")
    assert cp == os.pathsep.join(["/jars/b.jar", "/jars/a.jar"])


# ---- JDK install --------------------------------------------------------------------------------------------------

def _fake_jdk_tarball():
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data, mode in (("jdk-x/bin/java", b"#!/bin/sh\necho fake\n", 0o755),
                                 ("jdk-x/release", b'JAVA_VERSION="21"\n', 0o644)):
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), mode
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


@pytest.mark.parametrize("use_tar", [True, False])
def test_install_jdk_verifies_then_unpacks(server, tmp_path, use_tar):
    root, base = server
    data = _fake_jdk_tarball()
    a = _art(root, base, "fake-jdk.tar.gz", data, id="fake-21")
    home, t = A.install_jdk(_lock([a]), "fake-21", str(tmp_path / "jdk"), use_tar=use_tar, allow_http=True)
    assert os.path.basename(home) == "fake-21"
    assert (tmp_path / "jdk" / "fake-21" / "bin" / "java").exists()
    assert t["bytes"] == len(data)


def test_install_jdk_refuses_tampered_tarball_before_unpacking(server, tmp_path):
    root, base = server
    data = _fake_jdk_tarball()
    a = _art(root, base, "fake-jdk.tar.gz", data, id="fake-21")
    lock = _lock([copy.deepcopy(a)])
    lock["artifacts"][0]["sha256"] = hashlib.sha256(b"other").hexdigest()
    with pytest.raises(A.FetchError):
        A.install_jdk(lock, "fake-21", str(tmp_path / "jdk"), allow_http=True)
    assert not (tmp_path / "jdk" / "fake-21").exists()


def test_select_unknown_artifact():
    with pytest.raises(A.LockError):
        A.select(_valid(), "nope")


def test_lock_json_roundtrip(tmp_path):
    p = tmp_path / "l.json"
    p.write_text(json.dumps(_valid()))
    assert A.load_lock(str(p))["artifacts"][0]["name"] == "a-1.jar"
