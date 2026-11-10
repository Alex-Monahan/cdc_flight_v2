"""Extract the evidence files printed by flights/spike_md/main.py from a Flight run's log.

Run:  tools/with-env uv run python spikes/md_semantics/flight_extract.py <flight name> <run number> [--prefix flight_]
Writes evidence/p2/md_semantics/<prefix><file> for every BEGIN/END EVIDENCE block, plus
<prefix>run<N>_steps.txt with the bootstrap's BOOT/STEP lines and the run's timing.
"""
import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from cdc_v2.flightctl import FlightCtl  # noqa: E402
from common import evidence_dir, redact  # noqa: E402

BEGIN = re.compile(r"^===== BEGIN EVIDENCE (\S+) =====$")
END = re.compile(r"^===== END EVIDENCE (\S+) =====$")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("flight")
    ap.add_argument("run", type=int)
    ap.add_argument("--prefix", default="flight_")
    a = ap.parse_args()
    fc = FlightCtl()
    rows = fc.logs(a.flight, a.run)
    key = next(k for k in ("message", "line", "log", "text") if rows and k in rows[0])
    lines = [str(r[key]).rstrip("\n") for r in rows]
    out, cur, buf, steps = evidence_dir(), None, [], []
    for line in lines:
        m = BEGIN.match(line)
        if m:
            cur, buf = m.group(1), []
            continue
        if cur and END.match(line):
            (out / f"{a.prefix}{cur}").write_text(redact("\n".join(buf) + "\n"))
            print("wrote", f"{a.prefix}{cur}", len(buf), "lines")
            cur = None
            continue
        if cur:
            buf.append(line)
        elif line.startswith(("BOOT", "STEP")):
            steps.append(line)
    run = fc.get_run(a.flight, a.run)
    timing = {k: str(run.get(k)) for k in ("status", "exit_code", "started_at", "ended_at")}
    steps.append(f"run {a.run}: " + ", ".join(f"{k}={v}" for k, v in timing.items()))
    (out / f"{a.prefix}run{a.run}_steps.txt").write_text(redact("\n".join(steps) + "\n"))
    print("\n".join(steps))
    return 0


if __name__ == "__main__":
    sys.exit(main())
