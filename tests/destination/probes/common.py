"""Shared by the probes of package D: where evidence goes, what every evidence file says about its run.

A probe is a script that measures one behaviour of the client or of MotherDuck and writes one file under
`evidence/p3/destination/` through `write_evidence` (the one writer of evidence, which scrubs paths and names).
MotherDuck probes run through `tools/with-env` and use databases named `cdc_v2_t_d_*` only.
"""
import datetime
import os
import platform
import sys

import duckdb

from cdc_v2.md import errors as md_errors
from cdc_v2.testing.harness_evidence import write_evidence

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
EVIDENCE = os.path.join(ROOT, "evidence", "p3", "destination")
MD_PREFIX = "cdc_v2_t_d_"


def cls(exc):
    """Class and first line of an error, with tokens, paths, hosts and addresses removed."""
    return md_errors.safe_text(exc, 200)


def header(command, what):
    return {
        "what": what,
        "command": command,
        "date_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "versions": {"duckdb_client": duckdb.__version__, "python": platform.python_version(),
                     "platform": "%s %s" % (platform.system(), platform.machine())},
        "machine_note": "10 cores, 16 GB, shared with up to three other workers of the same build; "
                        "times are indications only",
    }


def save(name, command, what, body):
    """Write `evidence/p3/destination/<name>` and say so."""
    doc = header(command, what)
    doc.update(body)
    path = os.path.join(EVIDENCE, name)
    write_evidence(path, doc)
    print("wrote evidence/p3/destination/%s" % name, file=sys.stderr)
    return path


def want_motherduck():
    """True when the probe was started through tools/with-env (the token is in the environment)."""
    return bool(os.environ.get("motherduck_token"))
