"""The one place where the harness writes evidence files (ledgers, summaries, measurements).

Evidence is committed to a public repository (SPEC R23, §4). This writer scrubs what it can, longest path first and
only at a path boundary: registered run directories become ``<run>``, the temporary directory ``<tmp>``, the Python
installation ``<python>``, the user's home ``~`` and the user name ``<user>``. Then it fails closed: text that still
holds an absolute path under ``/Users``, ``/home``, ``/root``, ``/private``, ``/var``, ``/tmp``, ``/Volumes`` or
``/opt``, or the user name, is refused with `EvidenceError`.

After the scrub, the redaction rules of `cdc_v2.evidence` (secrets, tokens, connection strings, host names, IP
addresses, exact values of the env files) are applied, and the text is refused if any of them still matches. This
is the one writer of evidence files (contract C0); every harness module and test writes through it.
"""
import getpass
import json
import os
import re
import sys
import tempfile

_ROOTS = []                          # (absolute path, placeholder) registered by runners
_ABS_PATH = re.compile(r"(?<![\w.~<>-])/(Users|home|root|private|var|tmp|Volumes|opt)/")
_BOUNDARY = r"(?![A-Za-z0-9_-])"


class EvidenceError(ValueError):
    pass


def register_root(path, placeholder="<run>"):
    """Replace ``path`` (and its realpath) by ``placeholder`` in everything written from now on."""
    for p in {os.path.abspath(str(path)), os.path.realpath(str(path))}:
        if (p, placeholder) not in _ROOTS:
            _ROOTS.append((p, placeholder))


def _user():
    try:
        return getpass.getuser()
    except Exception:                # noqa: BLE001
        return os.environ.get("USER", "")


def _replacements():
    reps = list(_ROOTS)
    for t in (tempfile.gettempdir(),):
        reps += [(t, "<tmp>"), (os.path.realpath(t), "<tmp>")]
    for p in (sys.prefix, sys.base_prefix, sys.exec_prefix):
        reps += [(p, "<python>"), (os.path.realpath(p), "<python>")]
    home = os.path.expanduser("~")
    reps += [(home, "~"), (os.path.realpath(home), "~")]
    seen, out = set(), []
    for p, ph in sorted(reps, key=lambda r: (-len(r[0]), r[0], r[1])):    # longest first, deterministic
        if p and p != "/" and p not in seen:
            seen.add(p)
            out.append((p, ph))
    return out


def scrub(text):
    """Replace known roots, the temporary directory, the Python installation, the home and the user name."""
    text = str(text)
    for p, ph in _replacements():
        text = re.sub(re.escape(p) + _BOUNDARY, lambda _m, ph=ph: ph, text)
    user = _user()
    if user and len(user) >= 3:
        text = re.sub(r"(?<![A-Za-z0-9])%s(?![A-Za-z0-9])" % re.escape(user), "<user>", text)
    return text


def check_clean(text):
    """Raise EvidenceError if ``text`` still holds an absolute user, system or temporary path, or the user name."""
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
    from cdc_v2.evidence import default_redactor
    red = default_redactor()
    if not isinstance(content, str):
        content = json.dumps(red.redact_obj(scrub_obj(content)), indent=2, sort_keys=True, default=str) + "\n"
    text = red.redact(scrub(content))
    check_clean(text)
    left = red.leftovers(text)
    if left:
        raise EvidenceError("evidence still matches redaction rules: %s" % ", ".join(sorted(set(left))))
    d = os.path.dirname(os.path.abspath(str(path)))
    os.makedirs(d, exist_ok=True)
    with open(path, "a" if append else "w", encoding="utf-8") as f:
        f.write(text)
    return text
