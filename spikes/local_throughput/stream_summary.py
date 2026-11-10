"""Summarise evidence/p2/local_streaming/stream.jsonl: medians and ranges over repetitions per (test, config, node,
rate). Writes stream_summary.csv next to it and prints Markdown tables.

    tools/with-env uv run --group spike python spikes/local_throughput/stream_summary.py
"""
from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict

import stream_common as S


def g(d, path):
    for p in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(p)
    return d


METRICS = [
    ("rows/s", "rows_per_s_first_to_last"), ("tx/s", "tx_per_s"), ("pgbench tps", "pgbench.tps"),
    ("commit->first s", "commit_to_first_event_s"), ("commit->replayed s", "commit_to_replayed_s"),
    ("spawn->first s", "spawn_to_first_event_s"), ("tx exec s", "tx_exec_s"),
    ("delay p50 ms", "delay_ms.p50"), ("delay p99 ms", "delay_ms.p99"), ("delay max ms", "delay_ms.max"),
    ("replay lag p99 s", "replay_lag_s.p99"), ("drain s", "drain_after_load_end_s"),
    ("JVM %", "cpu_pct.jvm"), ("walsender %", "cpu_pct.walsender"), ("consumer %", "cpu_pct.consumer"),
    ("JVM µs/ev", "us_per_row.jvm"), ("stream thr µs/ev", "us_per_row.streaming_thread"),
    ("walsender µs/ev", "us_per_row.walsender"), ("consumer µs/ev", "us_per_row.consumer"),
    ("JVM µs/tx", "us_per_tx.jvm"), ("stream thr µs/tx", "us_per_tx.streaming_thread"),
    ("walsender µs/tx", "us_per_tx.walsender"),
    ("JVM peak RSS MB", "jvm_peak_rss_mb"), ("slot dir peak MB", "slot_dir_peak_mb"),
    ("spill bytes", "slot_stats_delta.spill_bytes"), ("spill count", "slot_stats_delta.spill_count"),
    ("thr streaming s", "jvm_threads_cpu_s.streaming"), ("thr gc s", "jvm_threads_cpu_s.gc"),
    ("thr jit s", "jvm_threads_cpu_s.jit"), ("thr Thread-N s", "jvm_threads_cpu_s.metrics_or_other_Thread-N"),
    ("thr poll+ours s", "jvm_threads_cpu_s.engine_poll_and_ours"), ("redelivered", "redelivered_on_start"),
]


def main():
    recs = [json.loads(x) for x in (S.EVID / "stream.jsonl").read_text().splitlines() if x.strip()]
    by = defaultdict(list)
    for r in recs:
        if r.get("error"):
            continue
        k = (r["test"], r.get("cfg"), r.get("node"), r.get("rate_target"), r.get("signal"))
        by[k].append(r)
    rows = []
    for k, rs in sorted(by.items(), key=lambda x: tuple(str(i) for i in x[0])):
        row = {"test": k[0], "cfg": k[1], "node": k[2], "rate": k[3] or "", "signal": k[4] or "", "n": len(rs),
               "ok": all(r.get("ok") in (True, None) for r in rs),
               "load_1m": "/".join(str(r["load_avg"][0]) for r in rs if r.get("load_avg"))}
        for name, path in METRICS:
            vals = [g(r, path) for r in rs]
            vals = [v for v in vals if isinstance(v, (int, float))]
            if vals:
                m = statistics.median(vals)
                row[name] = (f"{m:,.0f}" if abs(m) >= 100 else f"{m:.3g}") + (
                    f" ({min(vals):,.4g}–{max(vals):,.4g})" if len(vals) > 1 else "")
        rows.append(row)
    cols = ["test", "cfg", "node", "rate", "signal", "n", "ok", "load_1m"] + [m[0] for m in METRICS]
    with open(S.EVID / "stream_summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    for r in rows:
        print("| " + " | ".join(f"{c}={r[c]}" for c in cols if r.get(c) not in (None, "")) + " |")


if __name__ == "__main__":
    main()
