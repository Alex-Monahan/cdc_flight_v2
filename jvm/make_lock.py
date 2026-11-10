#!/usr/bin/env python3
"""Regenerate jvm/debezium.lock.json from jvm/pom.xml (development machine only; needs `mvn`).

    uv run python jvm/make_lock.py            # writes jvm/debezium.lock.json
    uv run python jvm/make_lock.py --check    # exit 1 if the committed lock differs from a fresh resolution

Maven resolves the runtime closure of the dependencies declared in jvm/pom.xml into the local repository. For every
jar this script records its coordinates, its Maven Central URL, its size and its SHA-256, and cross-checks the local
copy against the SHA-1 that Maven Central publishes next to it, so the lock describes exactly what Central serves.
The Flight never runs Maven: it fetches the locked jars with cdc_v2.artifacts (SHA-256 verified, fail closed).
"""
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
LOCK = os.path.join(HERE, "debezium.lock.json")
CENTRAL = "https://repo1.maven.org/maven2"
LINE = re.compile(r"^\s*([^:\s]+):([^:\s]+):([^:\s]+):(?:([^:\s]+):)?([^:\s]+):(compile|runtime):(\S+?\.jar)\b")


def resolve():
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "deps.txt")
        subprocess.run(["mvn", "-q", "-B", "-f", os.path.join(HERE, "pom.xml"), "dependency:list",
                        "-DincludeScope=runtime", "-DoutputAbsoluteArtifactFilename=true",
                        "-DoutputFile=" + out], check=True)
        with open(out, encoding="utf-8") as f:
            text = f.read()
    arts = []
    for line in text.splitlines():
        m = LINE.match(line)
        if not m:
            continue
        group, artifact, typ, classifier, version, scope, path = m.groups()
        if typ != "jar":
            continue
        arts.append(dict(group=group, artifact=artifact, version=version, classifier=classifier, scope=scope,
                         path=path))
    if not arts:
        raise SystemExit("no artifacts parsed from mvn dependency:list")
    return arts


def sha(path, algo):
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def entry(a):
    fname = "%s-%s%s.jar" % (a["artifact"], a["version"], "-" + a["classifier"] if a["classifier"] else "")
    url = "%s/%s/%s/%s/%s" % (CENTRAL, a["group"].replace(".", "/"), a["artifact"], a["version"], fname)
    with urllib.request.urlopen(url + ".sha1", timeout=30) as r:
        central_sha1 = r.read().decode().split()[0].strip().lower()
    local_sha1 = sha(a["path"], "sha1")
    if central_sha1 != local_sha1:
        raise SystemExit("local copy of %s differs from Maven Central (sha1 %s vs %s)" % (fname, local_sha1,
                                                                                         central_sha1))
    coords = "%s:%s:%s%s" % (a["group"], a["artifact"], a["version"], ":" + a["classifier"] if a["classifier"] else "")
    return {"name": fname, "coords": coords, "scope": a["scope"], "url": url, "size": os.path.getsize(a["path"]),
            "sha256": sha(a["path"], "sha256")}


def build():
    arts = resolve()
    entries = sorted((entry(a) for a in arts), key=lambda e: e["name"])
    dbz = next(e["coords"].split(":")[2] for e in entries if e["coords"].startswith("io.debezium:debezium-embedded:"))
    return {
        "format": "cdc_v2.artifact-lock/1",
        "description": "Runtime jars of the stock Debezium embedded engine and PostgreSQL connector, resolved by "
                       "jvm/make_lock.py from jvm/pom.xml. Fetched from Maven Central by cdc_v2.artifacts.",
        "debezium_version": dbz,
        "java_release": 21,
        "total_size": sum(e["size"] for e in entries),
        "artifacts": entries,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    lock = build()
    text = json.dumps(lock, indent=1, sort_keys=False) + "\n"
    if a.check:
        with open(LOCK, encoding="utf-8") as f:
            same = f.read() == text
        print("lock is up to date" if same else "lock differs from a fresh resolution")
        return 0 if same else 1
    with open(LOCK, "w", encoding="utf-8") as f:
        f.write(text)
    print("wrote %s: %d artifacts, %d bytes" % (os.path.relpath(LOCK), len(lock["artifacts"]), lock["total_size"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
