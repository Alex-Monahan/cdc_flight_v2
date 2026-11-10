#!/usr/bin/env python3
"""Blocks commits and pushes that contain secrets or keys. This repository is public.

Three layers, strongest first:
  1. exact values - every value in the local secrets file (default ~/.config/cdc_flight_v2/env, override with
                    CDC_SECRETS_ENV_FILE), also base64- and URL-encoded. Cannot be suppressed.
  2. file names   - env files, private keys, key stores. Cannot be suppressed.
  3. patterns     - well-known token formats, private-key blocks, URLs with embedded credentials, secret-looking
                    assignments and long high-entropy strings. A false positive is suppressed with
                    `secret-scan: allow` on the same line, or by a regex line in .secretscan-allow.
If gitleaks is installed it is run as a second opinion on staged content and on full-history scans.

Modes: --staged (default; pre-commit), --message-file F (commit-msg), --pre-push (reads git's pre-push stdin),
--all-history, --files PATH...   Exit status 1 when anything is found. Matched values are never printed.
"""
import argparse
import base64
import fnmatch
import math
import os
import re
import shutil
import subprocess
import sys
import urllib.parse

MAX_BYTES = 20 * 1024 * 1024
MIN_EXACT_LEN = 8
NON_SECRET_KEYS = {"MOTHERDUCK_SERVICE_ACCOUNT", "PG_PORT", "PG_REPLICA_PORT", "PG_SSLMODE", "PG_DATABASE",
                   "PLANETSCALE_BRANCH"}
# values too ordinary to block wherever they occur (a role called "postgres" must not block the word)
COMMON_VALUES = {"postgres", "postgresql", "localhost", "127.0.0.1", "motherduck", "planetscale", "replicator"}
PRAGMA = "secret-scan: allow"

FORBIDDEN_NAMES = [".env", ".env.*", "*.env", "env", "*.pem", "*.key", "*.p12", "*.pfx", "*.jks", "*.keystore",
                   "*.ppk", "*.kdbx", "id_rsa", "id_dsa", "id_ecdsa", "id_ed25519", ".netrc", ".pgpass", ".npmrc",
                   ".pypirc", "*.tfstate", "credentials.json"]
ALLOWED_NAMES = [".env.example", ".env.sample", ".env.template"]
NO_ENTROPY_NAMES = ["*.lock", "*.sum", "package-lock.json"]

PATTERNS = [(name, re.compile(rx)) for name, rx in [
    ("private-key-block", r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY( BLOCK)?-----"),
    ("github-token", r"\b(gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,})"),
    ("aws-access-key-id", r"\b(AKIA|ASIA|AGPA|AIDA|AROA|ANPA)[A-Z0-9]{16}\b"),
    ("slack-token", r"\bxox[abeoprs]-[A-Za-z0-9-]{10,}"),
    ("slack-webhook", r"hooks\.slack\.com/(services|workflows|triggers)/[A-Za-z0-9/_-]{20,}"),
    ("jwt", r"\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
    ("planetscale-credential", r"\bpscale_(tkn|pw|oauth)_[A-Za-z0-9_.-]{20,}"),
    ("anthropic-key", r"\bsk-ant-[A-Za-z0-9_-]{20,}"),
    ("openai-key", r"\bsk-(proj-)?[A-Za-z0-9_-]{32,}"),
    ("google-api-key", r"\bAIza[0-9A-Za-z_-]{35}"),
    ("stripe-key", r"\b[sr]k_(live|test)_[A-Za-z0-9]{20,}"),
]]
URL_CRED = re.compile(r"\b[a-z][a-z0-9+.-]{1,15}://([^\s/:@'\"<>]+):([^\s/@'\"<>]{3,})@")
ASSIGN = re.compile(r"(?i)\b[\w.-]*(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|salt)"
                    r"[\w.-]*[\"']?\s*[:=]\s*[\"']?([^\s\"',;]{12,})")
LONG_TOKEN = re.compile(r"[A-Za-z0-9+/_=-]{32,}")
# The session link in commit trailers is not a credential.
BUILTIN_ALLOW = [re.compile(r"^ai/code/session_[A-Za-z0-9]+$")]
PLACEHOLDER = re.compile(r"(?i)^(your|my|example|dummy|fake|test|change|redact|placeholder|password|passwd|secret|"
                         r"token|none|null|true|false|todo)|^[$<{%\[(*]|\.\.\.|…|^(.)\2+$")


def entropy(text):
    counts = {}
    for ch in text:
        counts[ch] = counts.get(ch, 0) + 1
    return -sum(n / len(text) * math.log2(n / len(text)) for n in counts.values())


def char_class(ch):
    return 0 if ch.islower() else 1 if ch.isupper() else 2 if ch.isdigit() else 3


def looks_random(text):
    """True for long strings that mix cases and digits the way generated keys do and identifiers do not:
    high entropy, frequent switches between character classes, digits in at least two places."""
    if text.count("/") > 2 or re.fullmatch(r"[0-9a-fA-F=_-]+", text):          # paths, hex digests, UUIDs
        return False
    if not (re.search(r"[A-Z]", text) and re.search(r"[a-z]", text) and len(re.findall(r"\d+", text)) >= 2):
        return False
    switches = sum(char_class(a) != char_class(b) for a, b in zip(text, text[1:])) / (len(text) - 1)
    return entropy(text) >= 4.0 and switches >= 0.40


def looks_secret(value, min_entropy):
    if PLACEHOLDER.search(value) or any(c in value for c in "()$<>{}"):
        return False
    return bool(re.search(r"[A-Za-z]", value) and re.search(r"\d", value)) and entropy(value) >= min_entropy


def redact(value):
    return "%s… (%d chars)" % (value[:4], len(value))


def load_exact_values():
    path = os.environ.get("CDC_SECRETS_ENV_FILE", os.path.expanduser("~/.config/cdc_flight_v2/env"))
    values = []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except OSError:
        return values
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.replace("export ", "").strip()
        value = value.strip().strip("'\"")
        if key in NON_SECRET_KEYS or len(value) < MIN_EXACT_LEN or value.lower() in COMMON_VALUES:
            continue
        variants = {value.encode(), base64.b64encode(value.encode()), urllib.parse.quote(value, safe="").encode()}
        values.append((key, variants))
    return values


def load_allow(root):
    try:
        with open(os.path.join(root, ".secretscan-allow"), encoding="utf-8") as f:
            lines = f.read().splitlines()
    except OSError:
        return []
    return [re.compile(l.strip()) for l in lines if l.strip() and not l.strip().startswith("#")]


def gitleaks(mode):
    """Second opinion from gitleaks when it is installed. Returns 1 if it reports a leak."""
    exe = shutil.which("gitleaks")
    if not exe or mode not in ("staged", "all-history"):
        return 0
    cmd = [exe, "git", "--no-banner", "--redact", "--verbose", "--log-level", "error"]
    if mode == "staged":
        cmd += ["--pre-commit", "--staged"]
    rc = subprocess.run(cmd + ["."], stdout=sys.stderr).returncode
    if rc:
        print("gitleaks: reported a leak (exit %d)" % rc, file=sys.stderr)
    return 1 if rc else 0


def scan(path, data, exact_values, allow):
    """Returns a list of (path, line number, rule, detail) for one file or message."""
    found = []
    name = os.path.basename(path)
    if any(fnmatch.fnmatch(name, p) for p in FORBIDDEN_NAMES) and name not in ALLOWED_NAMES:
        found.append((path, 0, "forbidden-file-name", name))
    for key, variants in exact_values:
        for variant in variants:
            at = data.find(variant)
            if at >= 0:
                found.append((path, data.count(b"\n", 0, at) + 1, "exact-secret-value", "value of %s" % key))
                break
    if len(data) > MAX_BYTES:
        found.append((path, 0, "file-too-large-to-scan", "%d bytes" % len(data)))
        return found
    binary = b"\0" in data[:8192]
    no_entropy = binary or any(fnmatch.fnmatch(name, p) for p in NO_ENTROPY_NAMES)
    for number, line in enumerate(data.decode("utf-8", errors="replace").splitlines(), 1):
        hits = []
        for rule, rx in PATTERNS:
            for m in rx.finditer(line):
                hits.append((rule, m.group(0)))
        if not binary:
            for m in URL_CRED.finditer(line):
                if not PLACEHOLDER.search(m.group(2)):
                    hits.append(("url-with-credentials", m.group(2)))
            for m in ASSIGN.finditer(line):
                if looks_secret(m.group(2), 3.2):
                    hits.append(("secret-assignment", m.group(2)))
        if not no_entropy:
            for m in LONG_TOKEN.finditer(line):
                if looks_random(m.group(0)):
                    hits.append(("high-entropy-string", m.group(0)))
        if PRAGMA in line:
            continue
        for rule, text in hits:
            if not any(rx.search(text) for rx in BUILTIN_ALLOW + allow):
                found.append((path, number, rule, redact(text)))
    return sorted(set(found))


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def git(*args, stdin=None):
    return subprocess.run(("git",) + args, input=stdin, stdout=subprocess.PIPE, check=True).stdout


def staged_items():
    for path in git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z").split(b"\0"):
        if path:
            name = path.decode("utf-8", errors="replace")
            try:
                yield name, git("show", ":" + name)
            except subprocess.CalledProcessError:
                yield name, b""


def commit_items(commits):
    seen = set()
    for commit in commits:
        yield "%s:message" % commit[:10], git("log", "-1", "--format=%B", commit)
        raw = git("diff-tree", "--root", "--no-commit-id", "-r", "-m", "-z", "--diff-filter=ACMR", commit).split(b"\0")
        for meta, path in zip(raw[0::2], raw[1::2]):
            blob = meta.split()[3].decode()
            name = path.decode("utf-8", errors="replace")
            if (blob, name) in seen or set(blob) == {"0"}:
                continue
            seen.add((blob, name))
            try:
                yield "%s:%s" % (commit[:10], name), git("cat-file", "blob", blob)
            except subprocess.CalledProcessError:
                continue


def pre_push_commits():
    commits = []
    for line in sys.stdin.read().splitlines():
        parts = line.split()
        if len(parts) != 4 or set(parts[1]) == {"0"}:
            continue
        spec = [parts[1], "--not", "--remotes"] if set(parts[3]) == {"0"} else ["%s..%s" % (parts[3], parts[1])]
        commits += git("rev-list", *spec).decode().split()
    return commits


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--staged", action="store_true")
    mode.add_argument("--message-file")
    mode.add_argument("--pre-push", action="store_true")
    mode.add_argument("--all-history", action="store_true")
    mode.add_argument("--files", nargs="+")
    args = ap.parse_args()

    mode = "staged"
    if args.files:
        mode = "files"
        root = os.getcwd()
        items = ((f, read_bytes(f)) for f in args.files)
    else:
        root = git("rev-parse", "--show-toplevel").decode().strip()
        os.chdir(root)
        if args.message_file:
            mode = "message"
            items = [("commit message", read_bytes(args.message_file))]
        elif args.pre_push:
            mode = "pre-push"
            items = commit_items(pre_push_commits())
        elif args.all_history:
            mode = "all-history"
            items = commit_items(git("rev-list", "--all").decode().split())
        else:
            items = staged_items()

    exact_values, allow = load_exact_values(), load_allow(root)
    findings = [f for path, data in items for f in scan(path, data, exact_values, allow)]
    for path, number, rule, detail in findings:
        print("%s:%s: [%s] %s" % (path, number or "-", rule, detail), file=sys.stderr)
    leaked = gitleaks(mode)
    if findings or leaked:
        print("\nsecret scan: %d finding(s); nothing was committed or pushed. Remove the value, or for a false\n"
              "positive of a pattern rule add `%s` on that line." % (len(findings) + leaked, PRAGMA), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
