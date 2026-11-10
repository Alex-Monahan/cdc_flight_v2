"""harness_evidence: scrubbing is deterministic and boundary-aware, and the writer fails closed."""
import os
import sys
import tempfile

import pytest

from cdc_v2.testing import harness_evidence as he


def test_scrub_home_tmp_python_and_run_roots(tmp_path):
    run = tmp_path / "work"
    he.register_root(run, "<run>")
    home = os.path.expanduser("~")
    text = "\n".join([str(run / "a" / "hits.jsonl"), os.path.join(home, "x", "y"),
                      os.path.join(tempfile.gettempdir(), "z"), os.path.join(sys.base_prefix, "lib", "os.py")])
    out = he.scrub(text)
    assert out.splitlines() == ["<run>/a/hits.jsonl", "~/x/y", "<tmp>/z", "<python>/lib/os.py"]
    he.check_clean(out)


def test_no_partial_replacement_and_fail_closed(tmp_path):
    home = os.path.expanduser("~")
    longer = home + "b/secret"                     # a different user's home that starts with ours
    assert he.scrub(longer) == longer               # not turned into "~b/secret"
    with pytest.raises(he.EvidenceError):
        he.write_evidence(tmp_path / "e.txt", longer)
    for bad in ("/private/var/folders/x", "/opt/homebrew/var/postgres", "/root/.ssh"):
        with pytest.raises(he.EvidenceError):
            he.check_clean(bad)
    assert not (tmp_path / "e.txt").exists()


def test_write_json_and_append(tmp_path):
    p = tmp_path / "out" / "s.json"
    he.write_evidence(p, {"path": os.path.expanduser("~/q"), "n": 1})
    assert '"path": "~/q"' in p.read_text()
    he.write_evidence(tmp_path / "l.jsonl", "a\n", append=True)
    he.write_evidence(tmp_path / "l.jsonl", "b\n", append=True)
    assert (tmp_path / "l.jsonl").read_text() == "a\nb\n"
