"""cdc_v2_spike_md: runs MotherDuck-semantics spike scripts inside a Flight (PLAN P2, §2 A 3).

Bootstrap only. It fetches this public repository anonymously at the commit in config CDC_V2_COMMIT, runs the steps
of config CDC_V2_SPIKE_PLAN (JSON list of {"script": "<file in spikes/md_semantics>", "args": [...],
"env": {...}}) with this Flight's Python, prints each evidence file the step wrote between
"===== BEGIN EVIDENCE <name> =====" and "===== END EVIDENCE <name> =====" markers, and exits non-zero if any step
failed. It prints no environment values; the spike scripts redact the token, the host name and the home path.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = "https://github.com/Alex-Monahan/cdc_flight_v2"


def sh(args, cwd=None):
    subprocess.run(args, cwd=cwd, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def main() -> int:
    t0 = time.time()
    commit = os.environ["CDC_V2_COMMIT"]
    plan = json.loads(os.environ.get("CDC_V2_SPIKE_PLAN", "[]"))
    work = Path(tempfile.mkdtemp(prefix="cdc_v2_spike_md_"))
    repo = work / "repo"
    repo.mkdir()
    sh(["git", "init", "-q"], cwd=repo)
    sh(["git", "fetch", "-q", "--depth", "1", REPO, commit], cwd=repo)
    sh(["git", "checkout", "-q", "FETCH_HEAD"], cwd=repo)
    print(f"BOOT fetched commit {commit[:12]} in {time.time() - t0:.1f} s; {len(plan)} step(s)", flush=True)
    failed = 0
    for i, step in enumerate(plan):
        evdir = work / f"evidence_{i}"
        evdir.mkdir()
        env = dict(os.environ)
        env.update({k: str(v) for k, v in step.get("env", {}).items()})
        env["motherduck_token"] = os.environ.get("MOTHERDUCK_TOKEN", "")
        env["CDC_V2_SPIKE_EVIDENCE"] = str(evdir)
        env["CDC_V2_SPIKE_TMP"] = str(work / "tmp")
        env["CDC_V2_MEMORY_LIMIT"] = step.get("memory_limit", "8GB")
        script = repo / "spikes" / "md_semantics" / step["script"]
        print(f"STEP {i} {step['script']} {' '.join(step.get('args', []))} start", flush=True)
        s0 = time.time()
        rc = subprocess.run([sys.executable, str(script), *step.get("args", [])], cwd=script.parent, env=env).returncode
        print(f"STEP {i} {step['script']} exit {rc} after {time.time() - s0:.1f} s", flush=True)
        failed += rc != 0
        for f in sorted(evdir.iterdir()):
            print(f"===== BEGIN EVIDENCE {f.name} =====", flush=True)
            for line in f.read_text().splitlines():
                print(line, flush=False)
            print(f"===== END EVIDENCE {f.name} =====", flush=True)
    print(f"BOOT done in {time.time() - t0:.1f} s, failed steps: {failed}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
