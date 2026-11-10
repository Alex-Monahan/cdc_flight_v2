"""Redaction rules for evidence and logs (SPEC §4, R23). The repository is public.

This module only redacts. The one writer of evidence files is `cdc_v2.testing.harness_evidence.write_evidence`
(contract C0): it redacts with `default_redactor()`, then scrubs paths and the user name, then refuses to write if
anything secret-looking is left (`Redactor.leftovers`, `harness_evidence.check_clean`).

`Redactor.redact` removes:

* exact values: every value of the env files (`~/.config/cdc_flight_v2/env` and `env.local`), the known secret
  variables of the environment (MotherDuck token, salt, webhook, PlanetScale and PG_* values), and values that a
  caller registers (for example the generated passwords of a local cluster). Values of 6 characters or more are
  removed wherever they occur; shorter ones (3 to 5 characters, not purely digits, not an ordinary word such as
  `main`) only as whole words;
* the home directory (as given and as its realpath, only at a path boundary: `/Users/ab` never cuts `/Users/abc`),
  this machine's host name and the operating-system user name;
* tokens by pattern (JWTs, PlanetScale tokens and passwords, bearer tokens, `motherduck_token=...`,
  `password=...`), credentials in URLs, libpq connection strings and `postgres://` URIs, e-mail addresses, IPv4 and
  IPv6 addresses (loopback excepted), and host names under provider and private domains.
"""
from __future__ import annotations

import getpass
import os
import re
import socket
from pathlib import Path
from typing import Any, Iterable, Optional

SECRET_ENV_NAMES = ("motherduck_token", "MOTHERDUCK_TOKEN", "CDC_PII_SALT", "SLACK_WEBHOOK_URL",
                    "PLANETSCALE_ORG", "PLANETSCALE_DATABASE", "PLANETSCALE_SERVICE_TOKEN_ID",
                    "PLANETSCALE_SERVICE_TOKEN", "PLANETSCALE_BRANCH", "PG_HOST", "PG_REPLICA_HOST",
                    "PG_ADMIN_USER", "PG_ADMIN_PASSWORD", "PG_USER", "PG_PASSWORD", "PG_DATABASE",
                    "MOTHERDUCK_SERVICE_ACCOUNT", "PGPASSWORD", "PGUSER")
# Values too ordinary to redact wherever they appear (they would destroy unrelated text).
COMMON_VALUES = {"postgres", "postgresql", "localhost", "127.0.0.1", "motherduck", "planetscale", "replicator",
                 "true", "false", "verify-full", "require", "main", "disable", "prefer", "public", "admin", "test",
                 "none", "null", "yes", "off"}
MIN_SUBSTRING = 6          # exact values at least this long are removed wherever they occur
MIN_WORD = 3               # shorter values (down to this length) are removed as whole words

PLACEHOLDER = "<redacted>"
_LOOPBACK = {"127.0.0.1", "0.0.0.0", "::1", "::"}

PATTERNS = [
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"), PLACEHOLDER),
    ("planetscale", re.compile(r"\bpscale_(?:tkn|pw|oauth|api)_[A-Za-z0-9_.-]{8,}"), PLACEHOLDER),
    ("postgres-uri", re.compile(r"\bpostgres(?:ql)?://[^\s'\"<>]+"), "postgres://" + PLACEHOLDER),
    ("url-credentials", re.compile(r"\b([a-z][a-z0-9+.-]{1,15}://)[^\s/:@'\"<>]+:[^\s/@'\"<>]+@"),
     r"\1" + PLACEHOLDER + "@"),
    ("conninfo", re.compile(r"(?i)\b(host|hostaddr|user|password|passfile|sslpassword|motherduck_token|token|"
                            r"sslkey|sslcert)\s*=\s*('[^']*'|\"[^\"]*\"|[^\s&;,'\"]+)"), r"\1=" + PLACEHOLDER),
    ("bearer", re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}"), r"\1 " + PLACEHOLDER),
    ("assignment", re.compile(r"(?i)([\"']?[\w.-]*(?:password|passwd|secret|token|api[_-]?key|salt)[\w.-]*[\"']?"
                              r"\s*[:=]\s*)([\"'])(?!<redacted>)[^\"']{4,}\2"), r"\1\2" + PLACEHOLDER + r"\2"),
    ("email", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"), PLACEHOLDER),
    ("provider-host", re.compile(r"(?i)\b[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:internal|local|lan|localdomain|"
                                 r"amazonaws\.com|psdb\.cloud|rds\.amazonaws\.com|compute|motherduck\.com)\b"),
     PLACEHOLDER),
]
_IPV4 = re.compile(r"(?<![\w.])(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)(?![\w.])")
_IPV6 = re.compile(r"(?<![\w:])(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}(?![\w:])"
                   r"|(?<![\w:])(?:[0-9A-Fa-f]{1,4}:){1,6}:(?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4}){0,5})?(?![\w:])"
                   r"|(?<![\w:])::(?:[0-9A-Fa-f]{1,4}:){1,6}[0-9A-Fa-f]{1,4}(?![\w:])")


def _ip(m: "re.Match") -> str:
    return m.group(0) if m.group(0) in _LOOPBACK else "<ip>"


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
        home = home if home is not None else str(Path.home())
        self.homes = sorted({h for h in (home, os.path.realpath(home)) if h and len(h) > 1}, key=len, reverse=True)
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
        self.words: set = set()
        for v in values:
            self.add(v)

    def add(self, value: Optional[str]) -> None:
        """Register one more exact value to remove (ordinary words and pure numbers are ignored)."""
        if not value or value.lower() in COMMON_VALUES or value.isdigit():
            return
        if len(value) >= MIN_SUBSTRING:
            self.values.add(value)
        elif len(value) >= MIN_WORD:
            self.words.add(value)

    def redact(self, text: Any) -> str:
        s = str(text)
        for v in sorted(self.values, key=len, reverse=True):
            if v in s:
                s = s.replace(v, PLACEHOLDER)
        for v in sorted(self.words, key=len, reverse=True):
            s = re.sub(r"(?<![A-Za-z0-9_])%s(?![A-Za-z0-9_])" % re.escape(v), PLACEHOLDER, s)
        for h in self.homes:
            s = re.sub(re.escape(h) + r"(?![A-Za-z0-9_.-])", "~", s)
        for _name, rx, repl in PATTERNS:
            s = rx.sub(repl, s)
        s = _IPV4.sub(_ip, s)
        s = _IPV6.sub(_ip, s)
        for h in sorted(self.hostnames, key=len, reverse=True):
            s = re.sub(r"(?i)(?<![A-Za-z0-9_-])%s(?![A-Za-z0-9_-])" % re.escape(h), "<host>", s)
        if self.user and len(self.user) >= 3 and self.user.lower() not in COMMON_VALUES:
            s = re.sub(r"(?<![A-Za-z0-9_])%s(?![A-Za-z0-9_])" % re.escape(self.user), "<user>", s)
        return s

    def leftovers(self, text: str) -> list:
        """Names of the rules that still match `text` (empty after `redact`); the writer refuses otherwise."""
        found = [name for name, rx, _ in PATTERNS
                 if any(PLACEHOLDER not in m.group(0) for m in rx.finditer(text))]
        found += ["exact value" for v in self.values if v in text][:1]
        found += ["ip address" for m in _IPV4.finditer(text) if m.group(0) not in _LOOPBACK][:1]
        return found

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
