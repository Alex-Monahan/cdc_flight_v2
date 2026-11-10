"""Measurement matrix for the read routes (sink discarded or parsed), 3 repeats each, interleaved.

    tools/with-env uv run --group spike python spikes/local_throughput/run_reads.py [--groups copy_count,copy_parse,duckdb,flight] [--repeats 3]
    tools/with-env uv run --group spike python spikes/local_throughput/run_reads.py --summarize

Each measurement runs in a fresh Python process (bench_copy.py / bench_duckdb.py), so peak RSS is per measurement.
Raw results are appended to evidence/p2/local_throughput/reads.jsonl; --summarize writes reads_summary.csv and prints
a Markdown table (median and spread over the repeats). Repeats are interleaved (repeat 1 of every variant, then
repeat 2, ...) so that a change in the machine's background load does not hit one variant only.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import subprocess
import sys
from collections import defaultdict

import common as C


def variants(groups: set[str]) -> list[tuple[str, list[str]]]:
    v: list[tuple[str, list[str]]] = []
    if "copy_count" in groups:
        for split in ("ctid", "pk"):
            for fmt in ("text", "csv", "binary"):
                for n in (1, 2, 4, 8):
                    v.append(("bench_copy.py", ["--workers", str(n), "--fmt", fmt, "--split", split,
                                                "--sink", "count"]))
    if "copy_parse" in groups:
        for n in (1, 2, 4, 8):
            for fmt in ("text", "csv"):
                v.append(("bench_copy.py", ["--workers", str(n), "--fmt", fmt, "--split", "ctid", "--sink", "arrow"]))
            v.append(("bench_copy.py", ["--workers", str(n), "--fmt", "binary", "--split", "ctid", "--sink", "rows"]))
    if "duckdb" in groups:
        for t in (1, 2, 4, 8):
            v.append(("bench_duckdb.py", ["--threads", str(t)]))
            v.append(("bench_duckdb.py", ["--threads", str(t), "--sink", "parquet"]))
        for ppt in (100, 10000):
            v.append(("bench_duckdb.py", ["--threads", "4", "--pages-per-task", str(ppt)]))
        v.append(("bench_duckdb.py", ["--threads", "4", "--binary", "0"]))
        v.append(("bench_duckdb.py", ["--threads", "4", "--ctid", "0"]))
        v.append(("bench_duckdb.py", ["--threads", "8", "--pool", "2"]))
    if "flight" in groups:  # F16 shape: 2 CPUs, memory limit; at most 2 client worker processes
        v.append(("bench_duckdb.py", ["--threads", "2", "--memory-limit", "1GB"]))
        v.append(("bench_duckdb.py", ["--threads", "2", "--memory-limit", "1GB", "--sink", "parquet"]))
        v.append(("bench_copy.py", ["--workers", "2", "--fmt", "binary", "--split", "ctid", "--sink", "count",
                                    "--snapshot"]))
        v.append(("bench_copy.py", ["--workers", "2", "--fmt", "csv", "--split", "ctid", "--sink", "arrow",
                                    "--snapshot"]))
    return v


def key(rec: dict) -> str:
    if rec["route"] == "copy":
        return (f"copy fmt={rec['fmt']} split={rec['split']} sink={rec['sink']} N={rec['workers']}"
                + (" snapshot" if rec.get("snapshot") else ""))
    return (f"duckdb threads={rec['threads']} sink={rec['sink']} ppt={rec['pages_per_task']} "
            f"binary={int(rec['binary'])} ctid={int(rec['ctid_scan'])} pool={rec['pool']} mem={rec['memory_limit']}")


def summarize() -> list[dict]:
    recs = [json.loads(line) for line in (C.EVIDENCE / "reads.jsonl").read_text().splitlines() if line.strip()]
    by = defaultdict(list)
    for r in recs:
        by[key(r)].append(r)
    rows = []
    for k, rs in by.items():
        s = lambda f: C.summarize([r.get(f) for r in rs])  # noqa: E731
        rps, cc, bc = s("rows_per_s"), s("client_cpu_util"), s("backend_cpu_util")
        mbs = s("mb_per_s") if rs[0]["route"] == "copy" else s("heap_mb_per_s")
        rss = s("client_peak_rss_mb_sum") if rs[0]["route"] == "copy" else s("client_peak_rss_mb")
        ccs, bcs = s("client_cpu_s"), s("backend_cpu_s")
        rows.append({"variant": k, "n": rps["n"], "rows_per_s_median": rps.get("median"),
                     "rows_per_s_min": rps.get("min"), "rows_per_s_max": rps.get("max"),
                     "spread_pct": rps.get("spread_pct"), "mb_per_s_median": mbs.get("median"),
                     "client_cpu_s_median": ccs.get("median"), "backend_cpu_s_median": bcs.get("median"),
                     "client_cores_median": cc.get("median"), "backend_cores_median": bc.get("median"),
                     "client_peak_rss_mb_median": rss.get("median"),
                     "load_avg_1m": "/".join(str(r["load_avg"][0]) for r in rs)})
    rows.sort(key=lambda r: r["variant"])
    with open(C.EVIDENCE / "reads_summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print("| variant | rows/s median | min-max (spread) | MB/s | client cores | backend cores | client CPU s | "
          "backend CPU s | peak RSS MB | load 1m |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        print(f"| {r['variant']} | {r['rows_per_s_median']:,.0f} | {r['rows_per_s_min']:,.0f}-{r['rows_per_s_max']:,.0f}"
              f" ({r['spread_pct']}%) | {r['mb_per_s_median']} | {r['client_cores_median']} | "
              f"{r['backend_cores_median']} | {r['client_cpu_s_median']} | {r['backend_cpu_s_median']} | "
              f"{r['client_peak_rss_mb_median']} | {r['load_avg_1m']} |")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--groups", default="copy_count,copy_parse,duckdb,flight")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--summarize", action="store_true")
    a = ap.parse_args()
    if a.summarize:
        summarize()
        return
    vs = variants(set(a.groups.split(",")))
    # warm-up: one full read so that the table is in the replica's OS page cache, not recorded
    subprocess.run([sys.executable, "bench_copy.py", "--workers", "4", "--fmt", "binary"], cwd=C.HERE, check=True,
                   capture_output=True, timeout=900)
    rnd = random.Random(42)
    for rep in range(a.repeats):
        order = list(vs)
        rnd.shuffle(order)
        for i, (script, args) in enumerate(order):
            p = subprocess.run([sys.executable, script, *args], cwd=C.HERE, capture_output=True, text=True,
                               timeout=1800)
            if p.returncode != 0:
                print(f"FAILED {script} {args}: {C.redact(p.stderr[-2000:])}", flush=True)
                continue
            rec = json.loads(p.stdout.strip().splitlines()[-1])
            rec["repeat"] = rep + 1
            C.append_jsonl("reads.jsonl", rec)
            print(f"rep {rep + 1} [{i + 1}/{len(order)}] {key(rec)}: {rec['rows_per_s']:,} rows/s "
                  f"wall {rec['wall_s']} s load {rec['load_avg'][0]}", flush=True)


if __name__ == "__main__":
    main()
