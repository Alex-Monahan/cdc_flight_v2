"""The one place where the harness writes evidence files (ledgers, summaries, measurements).

Evidence is committed to a public repository (SPEC R23, §4). This writer scrubs what it can (the user's home becomes
``~``, the user name ``<user>``, registered run directories ``<run>``) and then fails closed: text that still holds an
absolute path under a user or temporary directory, or the user name, is refused with `EvidenceError`.

At the merge with the oracle worker's evidence writer, replace the body of `write_evidence` with a call to it; every
harness module writes through this function only.
"""
import getpass
import json
import os
import re
import tempfile

_ROOTS = []                          # (absolute path, placeholder), longest first
_ABS_PATH = re.compile(r"(?<![\w.~<>-])/(Users|home|private|var|tmp|Volumes|opt/homebrew/var)/")


class EvidenceError(ValueError):
    pass


def register_root(path, placeholder="<run>"):
    """Replace ``path`` (and its realpath) by ``placeholder`` in everything written from now on."""
    for p in {os.path.abspath(str(path)), os.path.realpath(str(path))}:
        if (p, placeholder) not in _ROOTS:
            _ROOTS.append((p, placeholder))
    _ROOTS.sort(key=lambda r: -len(r[0]))


def _user():
    try:
        return getpass.getuser()
    except Exception:
        return os.environ.get("USER", "")


def scrub(text):
    """Replace known roots, the temporary directory, the home directory and the user name."""
    text = str(text)
    for p, ph in _ROOTS:
        text = text.replace(p, ph)
    for t in {tempfile.gettempdir(), os.path.realpath(tempfile.gettempdir())}:
        text = text.replace(t, "<tmp>")
    home = os.path.expanduser("~")
    for h in {home, os.path.realpath(home)}:
        text = text.replace(h, "~")
    user = _user()
    if user and len(user) >= 3:
        text = re.sub(r"(?<![A-Za-z0-9])%s(?![A-Za-z0-9])" % re.escape(user), "<user>", text)
    return text


def check_clean(text):
    """Raise EvidenceError if ``text`` still holds an absolute user/temporary path or the user name."""
    m = _ABS_PATH.search(text)
    if m:
        raise EvidenceError("evidence holds an absolute path near %r" % text[max(0, m.start() - 20):m.end() + 20])
    user = _user()
    if user and len(user) >= 3 and re.search(r"(?<![A-Za-z0-9])%s(?![A-Za-z0-9])" % re.escape(user), text):
        raise EvidenceError("evidence holds the user name")


def scrub_obj(obj):
    """Scrub every string inside a JSON-like object."""
    if isinstance(obj, str):
        return scrub(obj)
    if isinstance(obj, dict):
        return {scrub(k): scrub_obj(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [scrub_obj(v) for v in obj]
    return obj


def write_evidence(path, content, append=False):
    """Write ``content`` (str, or a JSON-able object written as indented JSON) to ``path``, scrubbed and checked.
    With ``append`` a str is appended as is (a ledger line). Returns the text written."""
    text = content if isinstance(content, str) else json.dumps(scrub_obj(content), indent=2, sort_keys=True) + "\n"
    text = scrub(text)
    check_clean(text)
    d = os.path.dirname(os.path.abspath(str(path)))
    os.makedirs(d, exist_ok=True)
    with open(path, "a" if append else "w", encoding="utf-8") as f:
        f.write(text)
    return text
