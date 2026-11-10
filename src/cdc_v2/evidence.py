"""Evidence files under evidence/<item>/, redacted before they touch the disk (SPEC §4, R23).

The repository is public. Everything an evidence file or a test log contains goes through `Redactor.redact`, which
removes:

* exact values: every value of the env files (`~/.config/cdc_flight_v2/env` and `env.local`), the known secret
  variables of the environment (MotherDuck token, salt, webhook, PlanetScale and PG_* values), and values that a
  caller registers (for example the generated passwords of a local cluster);
* this machine's host name, the operating-system user name and the home directory path (`/Users/<name>` -> `~`);
* tokens by pattern (JWTs, bearer tokens, `motherduck_token=...`, `password=...`), credentials in URLs, libpq
  connection strings (`host=... user=... password=...`) and `postgres://` URIs, e-mail addresses, and host names
  under provider domains.

Usage:

    ev = EvidenceWriter("p3/oracle")
    ev.write_json("mutations", {...})
    ev.write_text("run.log", text)
"""
from __future__ import annotations

import getpass
import json
import os
import re
import socket
from pathlib import Path
from typing import Any, Iterable, Optional

REPO = Path(__file__).resolve().parents[2]
EVIDENCE_ROOT = REPO / "evidence"

SECRET_ENV_NAMES = ("motherduck_token", "MOTHERDUCK_TOKEN", "CDC_PII_SALT", "SLACK_WEBHOOK_URL",
                    "PLANETSCALE_ORG", "PLANETSCALE_DATABASE", "PLANETSCALE_SERVICE_TOKEN_ID",
                    "PLANETSCALE_SERVICE_TOKEN", "PLANETSCALE_BRANCH", "PG_HOST", "PG_REPLICA_HOST",
                    "PG_ADMIN_USER", "PG_ADMIN_PASSWORD", "PG_USER", "PG_PASSWORD", "PG_DATABASE",
                    "MOTHERDUCK_SERVICE_ACCOUNT", "PGPASSWORD", "PGUSER")
# Values too ordinary to redact wherever they appear (they would destroy unrelated text).
COMMON_VALUES = {"postgres", "postgresql", "localhost", "127.0.0.1", "motherduck", "planetscale", "replicator",
                 "true", "false", "verify-full", "require", "main", "disable", "prefer"}
MIN_LEN = 6

PLACEHOLDER = "<redacted>"

PATTERNS = [
    # JSON web tokens (MotherDuck tokens are JWTs)
    (re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"), PLACEHOLDER),
    # URL with credentials, and postgres URIs as a whole
    (re.compile(r"\bpostgres(?:ql)?://[^\s'\"<>]+"), "postgres://" + PLACEHOLDER),
    (re.compile(r"\b([a-z][a-z0-9+.-]{1,15}://)[^\s/:@'\"<>]+:[^\s/@'\"<>]+@"), r"\1" + PLACEHOLDER + "@"),
    # key=value pairs of libpq connection strings and MotherDuck connection parameters
    (re.compile(r"(?i)\b(host|hostaddr|user|password|passfile|sslpassword|motherduck_token|token|"
                r"sslkey|sslcert)\s*=\s*('[^']*'|\"[^\"]*\"|[^\s&;,'\"]+)"), r"\1=" + PLACEHOLDER),
    # "Authorization: Bearer ..." and similar
    (re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}"), r"\1 " + PLACEHOLDER),
    # secret-looking assignments in JSON or code: "password": "...", token: ...
    (re.compile(r"(?i)([\"']?[\w.-]*(?:password|passwd|secret|token|api[_-]?key|salt)[\w.-]*[\"']?\s*[:=]\s*)"
                r"([\"'])(?!<redacted>)[^\"']{4,}\2"), r"\1\2" + PLACEHOLDER + r"\2"),
    # e-mail addresses
    (re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"), PLACEHOLDER),
    # host names under provider and private domains
    (re.compile(r"(?i)\b[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:internal|local|lan|localdomain|amazonaws\.com|"
                r"psdb\.cloud|rds\.amazonaws\.com|compute|motherduck\.com)\b"), PLACEHOLDER),
]


def _env_file_values(paths: Iterable[Path]) -> list:
    out = []
    for p in paths:
        try:
            text = Path(p).read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            _, value = line.split("=", 1)
            out.append(value.strip().strip("'\""))
    return out


class Redactor:
    def __init__(self, extra_values: Iterable[str] = (), env: Optional[dict] = None,
                 env_files: Optional[Iterable[Path]] = None, home: Optional[str] = None,
                 user: Optional[str] = None, hostnames: Optional[Iterable[str]] = None):
        env = os.environ if env is None else env
        cfg = Path(env.get("CDC_V2_CONFIG_DIR", Path.home() / ".config" / "cdc_flight_v2"))
        files = list(env_files) if env_files is not None else [cfg / "env", cfg / "env.local"]
        values = set(_env_file_values(files))
        values.update(env.get(n, "") for n in SECRET_ENV_NAMES)
        values.update(extra_values)
        self.home = home if home is not None else str(Path.home())
        try:
            default_user = getpass.getuser()
        except Exception:                      # noqa: BLE001
            default_user = ""
        self.user = user if user is not None else (env.get("USER") or env.get("LOGNAME") or default_user)
        if hostnames is None:
            h = socket.gethostname()
            hostnames = {h, h.split(".")[0]}
        self.hostnames = {h for h in hostnames if h and len(h) >= 3 and h.lower() not in COMMON_VALUES}
        self.values: set = set()
        for v in values:
            self.add(v)

    def add(self, value: Optional[str]) -> None:
        """Register one more exact value to remove (ignored when short or ordinary)."""
        if value and len(value) >= MIN_LEN and value.lower() not in COMMON_VALUES:
            self.values.add(value)

    def redact(self, text: Any) -> str:
        s = str(text)
        for v in sorted(self.values, key=len, reverse=True):
            if v in s:
                s = s.replace(v, PLACEHOLDER)
        if self.home and len(self.home) > 1:
            s = s.replace(self.home, "~")
        for rx, repl in PATTERNS:
            s = rx.sub(repl, s)
        for h in sorted(self.hostnames, key=len, reverse=True):
            s = re.sub(r"(?i)(?<![A-Za-z0-9_-])%s(?![A-Za-z0-9_-])" % re.escape(h), "<host>", s)
        if self.user and len(self.user) >= 3 and self.user.lower() not in COMMON_VALUES:
            s = re.sub(r"(?<![A-Za-z0-9_])%s(?![A-Za-z0-9_])" % re.escape(self.user), "<user>", s)
        return s

    def redact_obj(self, obj: Any) -> Any:
        """Redact every string inside a JSON-like structure (keys included)."""
        if isinstance(obj, dict):
            return {self.redact(k) if isinstance(k, str) else k: self.redact_obj(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self.redact_obj(v) for v in obj]
        if isinstance(obj, str):
            return self.redact(obj)
        return obj


_DEFAULT: Optional[Redactor] = None


def default_redactor() -> Redactor:
    global _DEFAULT
    if _DEFAULT is None:
        _DEFAULT = Redactor()
    return _DEFAULT


def redact(text: Any) -> str:
    return default_redactor().redact(text)


_ITEM = re.compile(r"^[a-z0-9][a-z0-9_.-]*(/[a-z0-9][a-z0-9_.-]*)*$")
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class EvidenceWriter:
    """Writes redacted files to evidence/<item>/. `item` is a relative path such as 'p3/oracle' or '1.3'."""

    def __init__(self, item: str, root: Optional[Path] = None, redactor: Optional[Redactor] = None):
        if not _ITEM.match(item) or ".." in item.split("/"):
            raise ValueError("bad evidence item %r" % item)
        self.dir = Path(root or EVIDENCE_ROOT) / item
        self.redactor = redactor or default_redactor()

    def _path(self, name: str) -> Path:
        if not _NAME.match(name):
            raise ValueError("bad evidence file name %r" % name)
        self.dir.mkdir(parents=True, exist_ok=True)
        return self.dir / name

    def write_text(self, name: str, text: str) -> Path:
        p = self._path(name)
        body = self.redactor.redact(text)
        p.write_text(body if body.endswith("\n") else body + "\n", encoding="utf-8")
        return p

    def write_json(self, name: str, obj: Any) -> Path:
        if not name.endswith(".json"):
            name += ".json"
        p = self._path(name)
        body = json.dumps(self.redactor.redact_obj(obj), indent=2, sort_keys=True, default=str)
        p.write_text(self.redactor.redact(body) + "\n", encoding="utf-8")
        return p

    def append_text(self, name: str, text: str) -> Path:
        p = self._path(name)
        with p.open("a", encoding="utf-8") as f:
            line = self.redactor.redact(text)
            f.write(line if line.endswith("\n") else line + "\n")
        return p
