# Evidence of package F (fuzzers), round 1

All files written through `cdc_v2.testing.harness_evidence.write_evidence` (scrubbed and redacted). The system under
test is the toy replicator (`tests/fuzz/toy/`); its runs are the self-test of the fuzzers and never count toward the
robustness bar. Machine: the shared 10-core, 16 GB laptop, with other workers' clusters and tests running at the same
time; numbers are indications only. docs/testing/fuzzers.md explains every field.

| File | What | Command |
|---|---|---|
| `variants.json` | every wrong variant of the toy, the fuzzer that found it, after how many seeds and seconds; the correct toy on the same seeds | `tools/fuzz variants` |
| `suite-1.7-toy-replica/summary.json` | the suite of SPEC 1.7 on the toy: crash points (3 seeds, plus `kill_later` before the commit), network faults, destination errors, source backend termination, slot loss, full spill area, a 15-minute soak | `tools/fuzz suite-1.7 --system toy --soak-minutes 15 --workers 1` |
| `suite-1.7-toy-replica/crash_ledger.jsonl`, `crash_summary.json` | the crash runner's ledger and summary of that suite | (same) |
| `suite-1.7-toy-replica/runs.jsonl` | the fault-fuzzer runs of that suite (groups and soak), one line each | (same) |
| `stream_offline.json` | the stream fuzzer against package E's decoder and the reference assembler: 100,000 cases, kinds, detections, the classes the format cannot detect, negative controls | `tools/fuzz stream-offline --cases 100000 --negative-controls --out …` |

The campaign ledger of the toy's self-test runs is `evidence/robustness/` (`ledger.json` from
`tools/fuzz status --write`).
