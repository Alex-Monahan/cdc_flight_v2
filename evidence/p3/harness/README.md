# P3 evidence: crash-point and fault-injection harness

Date: 2026-11-10 (UTC). Machine: macOS (Apple silicon), Python 3.13.1, DuckDB 1.5.6, psycopg 3.3.8, local
PostgreSQL 18 (shared development cluster, primary on port 55432). How the harness works: `docs/testing/harness.md`.
Every file here was written through `cdc_v2.testing.harness_evidence.write_evidence` (run directory shown as `<run>`,
temporary directory as `<tmp>`, home as `~`); it contains no password, host name from the env file or user name.

## Results

| Claim | Result | File |
|---|---|---|
| The runner reaches every declared point of the correct toy with 3 seeds; the correct toy always passes | 6/6 points, 18 runs, 18 pass; each instance died by SIGKILL exactly at the armed point and hit | `toy_correct_points_summary.json`, `toy_correct_points_ledger.jsonl` |
| The wrong toy (position in a separate transaction) is caught | 7/7 points reached; `diverged` (duplicates) at `toy.data_committed` and `toy.position_written` for all 3 seeds; every other point passes | `toy_wrong_points_summary.json`, `toy_wrong_points_ledger.jsonl` |
| Random kills | 3 seeds × 4 kills, all SIGKILL, all pass | `toy_correct_random_kills_*` |
| Freeze and continue, with a second instance started while the first is frozen | 3 seeds at random times plus 3 freezes at crash points (`toy.data_written`, `toy.position_written`, `toy.committed`), all pass | `toy_correct_freeze_*` |
| A hung component with a live process is reported, not waited for | hang at `toy.data_written` (it keeps the DuckDB file lock): outcome `no_catch_up` after the 2 s deadline | `toy_correct_hang_*` |
| A point that is never reached is a failure | `not_reached`, `missing_points = ["toy.never"]` | `toy_unreached_ledger.jsonl` |
| Opt-in long campaign on the correct toy | 160 runs (120 crash-point runs = 6 points × 20 seeds, 20 random-kill runs × 6 kills, 20 freezes), all pass, 6/6 points | `toy_correct_long_summary.json`, `toy_correct_long_ledger.jsonl` (hit lists removed, `hits_total` kept), `pytest_long_campaign.txt` |
| Byte-stream faults are detected and contained | 1,000 seeds over a framed stream, 1,795 faults fired (cut 296, dup 386, flip 405, stall 393, truncate 315): every one of the 915 seeds with a corrupting fault was detected, with only a prefix of true frames applied; the 85 stall-only seeds arrived whole | `pipefaults_detection.json` |
| psycopg through the TCP proxy (tier T1) | black hole noticed by the 2 s client deadline after 2.01 s (both directions, and reply lost); reset noticed in ≤ 6 ms (idle and in flight); close and refuse in ≤ 3 ms; a 0.15 s delay each way: statement succeeds in 0.305 s; TLS 1.3 passes through; keepalives (idle 1 s, interval 1 s, count 2) set on the socket but **do not** detect a proxy black hole within 5 s | `pg_proxy_detection.json`, `pytest_t1_pg_proxy.txt` |
| No process survives a test | every scenario ends with `Fleet.kill_all` + `assert_no_survivors`; each runner test also checks `ps` for any process naming its directory | ledgers (`exits_at_stop`), `pytest_hermetic.txt` |

## Test runs

| File | Command | Result |
|---|---|---|
| `pytest_hermetic.txt` | `uv run -q pytest -v tests/harness --durations=0` (with `CDC_V2_HARNESS_EVIDENCE`) | 59 passed, 1 skipped (the opt-in long campaign), 6 deselected (`t1`), 30.6 s |
| `pytest_t1_pg_proxy.txt` | `tools/with-env uv run -q pytest -v -m t1 tests/harness/test_harness_pg_proxy.py` | 6 passed, 10.3 s |
| `pytest_long_campaign.txt` | `CDC_V2_HARNESS_LONG=1 … -k long` | 1 passed |
| `pytest_default_run.txt` | `uv run pytest` (whole repository, default markers) | 156 passed, 1 skipped, 29 deselected, 40.3 s |

The default run of the repository took 15 s before this work; the harness self-tests add about 25 s.
