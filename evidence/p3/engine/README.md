# Evidence of package E (the engine)

Written by the suites in `tests/engine/` when the variable `CDC_V2_E_EVIDENCE` names this directory, through
`cdc_v2.testing.harness_evidence.write_evidence` (scrubbed). Machine: shared 10-core Apple-silicon laptop, other
workers running; Postgres 18.6 (Homebrew), private pair `pg-e` (ports 55620–55621, decoding on the standby);
Debezium 3.7.1.Final (the 73 locked jars); Homebrew OpenJDK 23.0.2 (the JDK lock holds Linux tarballs only), heap
512 MB, ParallelGC. Explanations: `docs/design/engine.md`.

| File | Test | What it shows |
|---|---|---|
| `types_baseline.json` | `test_engine_types.py` | every baseline family, value by value against Postgres |
| `operations_event_counts.json` | `test_engine_stream.py::test_operations_transactions_and_event_counts` | per-transaction records and END counts for every case |
| `unchanged_toast.json` | `test_engine_stream.py::test_unchanged_toast_h003` | H-003 cases |
| `schema_change.json` | `test_engine_stream.py::test_schema_change_between_transactions` | SCHEMA versions |
| `large_transaction.json` | `test_engine_stream.py::test_large_transaction_bounded_memory` | 1M rows, JVM peak RSS |
| `ack_invariant.json` | `test_engine_ack.py::test_slot_never_ahead_of_durable_and_follows_it` | slot never ahead of DURABLE; delays |
| `restarts.json` | `test_engine_ack.py::test_restart_after_random_kills_and_java_crash_points` | 10 engine lives, nothing lost or doubled |
| `frozen_reader.json` | `test_engine_ack.py::test_frozen_reader` | flat RSS, walsender alive past wal_sender_timeout |
| `attach_*.json`, `proxy_*.json` | `test_engine_attach.py` | HELLO holder, walsender terminated, slot already active, reset/close/blackhole |
| `parent_death.json` | `test_engine_attach.py::test_parent_death_ends_the_jvm_and_frees_the_slot` | JVM and slot free after the parent's SIGKILL |
| `position_mapping.json` | `test_engine_attach.py::test_positions_map_to_pgoutput_begin_and_commit` | END ack_position = pgoutput Commit end_lsn |
| `slot_flush_backwards_pg18.json` | `test_engine_attach.py::test_lower_flush_position_does_not_move_the_slot_back` | core design open point 14 on 18.6 |
| `end_only_heartbeat_storm.json` | `test_engine_ack.py::test_end_only_acknowledgement_under_a_heartbeat_storm` | 40,000 + 2,000 transactions, heartbeat 1 ms, kills: nothing lost or doubled, slot never above DURABLE |
| `held_list_bound.json` | `test_engine_ack.py::test_held_list_keeps_following_far_behind` | 60,000 ENDs behind: DURABLE confirms within the stated bound |
| `idle_ack_bound.json` | `test_engine_ack.py::test_idle_acknowledgement_bound` | idle delay against heartbeat interval + 1 s |
| `negative_control.json` | `test_engine_ack.py::test_negative_control_one_end_ahead_is_caught` | a pipeline one END ahead is caught |
| `unparsable_interval.json` | `test_engine_values.py::test_unparsable_interval_ends_in_fatal_without_the_value` | FATAL place, restarts, no value escapes, table excluded passes |
| `infinite_interval.json` | `test_engine_values.py::test_infinite_interval_is_delivered_as_zero` | finding: infinite intervals arrive as zero |
| `frames_detection.json` | `test_frames_detection.py -k long` (`CDC_V2_E_LONG=1`) | 1,000 pipefaults seeds |
| `throughput.json` | `test_engine_measure.py` (`CDC_V2_E_MEASURE=1`) | rows/s and CPU split, indications only |

Commands (from the repository root):

```sh
uv run -q pytest -q -m "jvm and localpg" tests/engine          # writes the files above when the variable is set
CDC_V2_E_LONG=1 uv run -q pytest -q tests/engine/test_frames_detection.py -k long
CDC_V2_E_MEASURE=1 uv run -q pytest -q -m "jvm and localpg" tests/engine/test_engine_measure.py -s
```
