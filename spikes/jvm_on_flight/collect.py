"""Turn a `flightctl --json run --wait --logs` document into an evidence file (local machine).

    python spikes/jvm_on_flight/collect.py RUN.json evidence/p2/jvm_on_flight/run3_e2e_tput_memory.json

Keeps the run record (flightctl already redacts user names), parses `RESULT` lines into objects, adds the container
start time (started_at -> bootstrap first line) and refuses to write if a local home path or a token-like string is
present.
"""
import datetime
import json
import os
import re
import sys


def ts(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def main(src, dst):
    with open(src) as f:
        run = json.load(f)
    logs = run.pop("logs", [])
    results, boot, marks = [], {}, []
    for ln in logs:
        if ln.startswith("RESULT "):
            results.append(json.loads(ln[7:]))
        elif ln.startswith("BOOT "):
            parts = ln.split()
            boot[parts[1]] = " ".join(parts[2:])
        elif ln.startswith("T "):
            marks.append(ln)
    derived = {}
    if run.get("started_at") and "first_line" in boot:
        derived["container_and_requirements_s"] = round(float(boot["first_line"]) - ts(run["started_at"]), 3)
    if run.get("started_at") and run.get("ended_at"):
        derived["run_wall_s"] = round(ts(run["ended_at"]) - ts(run["started_at"]), 3)
        derived["cost_usd_at_0.60_per_h"] = round(derived["run_wall_s"] / 3600 * 0.60, 4)
    doc = {"run": run, "derived": derived, "boot": boot, "marks": marks, "results": results, "raw_log": logs}
    text = json.dumps(doc, indent=1, sort_keys=True, default=str)
    home = os.path.expanduser("~")
    if home in text or re.search(r"eyJ[A-Za-z0-9_-]{20,}\.", text):
        raise SystemExit("refusing to write: local path or token-like string present")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "w") as f:
        f.write(text + "\n")
    print("wrote %s: %d results, %s" % (dst, len(results), derived))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
