"""Evidence for the fix round after D's merge (docs/design/apply.md 10): the apply tests on D's lane, run with this
tree's code and with the apply code of another commit (default: origin/main, the code before the round), both as
the default run and as the full matrix (CDC_V2_APPLY_FULL=1). Writes evidence/p3/apply/after_d_lane.json.

    uv run python tests/apply/apply_old_code.py [--old origin/main]

The old code runs in a copy of this tree (`git archive HEAD`) whose `src/cdc_v2/apply` is the old commit's, under a
temporary directory that is removed afterwards. The old code has no `_Counting.count`, so in the copy the conftest's
check of counted statements is left out (it needs the method it checks); every other test is unchanged.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cdc_v2.testing.harness_evidence import write_evidence  # noqa: E402

SUMMARY = re.compile(r"(\d+) (passed|failed|deselected|xfailed|skipped|errors?)")


def git(*args, cwd=ROOT):
    return subprocess.run(["git", "-C", str(cwd)] + list(args), capture_output=True, text=True, check=True,
                          timeout=120).stdout.strip()


def run(tree: Path, full: bool) -> dict:
    env = dict(os.environ)
    env.pop("CDC_V2_APPLY_FULL", None)
    if full:
        env["CDC_V2_APPLY_FULL"] = "1"
    # one -q only (pyproject's addopts has the other): with two, pytest prints no summary line
    out = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "tests/apply"], cwd=str(tree),
                         env=env, capture_output=True, text=True, timeout=1800).stdout
    counts = {k: int(n) for n, k in SUMMARY.findall(out.strip().splitlines()[-1] if out.strip() else "")}
    failed = sorted(line.split(" ", 1)[1].split(" - ")[0] for line in out.splitlines() if line.startswith("FAILED "))
    return {"counts": counts, "failed": failed}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", default="origin/main")
    args = ap.parse_args()
    head = git("rev-parse", "--short", "HEAD")
    dirty = bool(git("status", "--porcelain", "--", "src", "tests"))
    old = git("rev-parse", "--short", args.old)
    tmp = Path(tempfile.mkdtemp(prefix="cdc_v2_apply_old_"))
    try:
        for name, ref, paths in (("head.tar", "HEAD", []), ("old.tar", args.old, ["src/cdc_v2/apply"])):
            git("archive", "-o", str(tmp / name), ref, *paths)
        copy = tmp / "tree"
        copy.mkdir()
        for name in ("head.tar", "old.tar"):
            with tarfile.open(tmp / name) as t:
                t.extractall(copy, filter="data")
        conf = copy / "tests" / "apply" / "conftest.py"
        text = conf.read_text()
        conf.write_text(text.replace("    with apply_lane.counted_statements():\n",
                                     "    with __import__('contextlib').nullcontext():\n"))
        results = {"this_code": {"default": run(ROOT, False), "full": run(ROOT, True)},
                   "old_code": {"default": run(copy, False), "full": run(copy, True)}}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    doc = {"commit": head + ("+changes" if dirty else ""), "old_code": "%s (%s, src/cdc_v2/apply only)" % (old, args.old),
           "command": "uv run python tests/apply/apply_old_code.py --old %s" % args.old,
           "what": "the apply tests on package D's lane over local DuckDB, default run and full matrix "
                   "(CDC_V2_APPLY_FULL=1), with this commit's apply code and with the old apply code; in the old "
                   "copy the conftest's check of counted statements is left out (the old code has no "
                   "_Counting.count)",
           "results": results}
    write_evidence(ROOT / "evidence" / "p3" / "apply" / "after_d_lane.json", doc)
    for side, r in results.items():
        for kind, v in r.items():
            print(side, kind, v["counts"], len(v["failed"]), "failed")


if __name__ == "__main__":
    main()
