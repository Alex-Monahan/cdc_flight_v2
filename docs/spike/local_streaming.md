# Local streaming rate of stock Debezium, decoding on the replica (PLAN §2 A 5, part 2)

> **These are first indications from a shared development laptop, not performance results.** Source, replica, JVM
> and consumer all run on one Apple-silicon machine (10 cores, 16 GB) that three other workers used at the same time
> (1-minute load average 3–52 during the runs; every record carries its own `load_avg`). SPEC: performance claims
> come only from Flight runs. The numbers here choose the design.

Measured 2026-11-10 (UTC). Postgres 18.6 (private cluster `pg-spike-tp`: primary 55500, replica 55501, TLS
`verify-full`, `logical_decoding_work_mem` 64 MB, `hot_standby_feedback` on). Stock Debezium **3.7.1.Final** embedded
engine in the merged, unchanged `jvm/src/cdc_v2/engine/EngineMain.java` (73 jars from `jvm/debezium.lock.json`,
fetched and verified by `cdc_v2.artifacts` into `~/.cache/cdc_flight_v2/jars`), `-Xmx1g -XX:ActiveProcessorCount=2`
as on the Flight, `pgoutput`, `snapshot.mode=no_data`, `MemoryOffsetBackingStore` (`docs/research/debezium.md` §3.4),
`struct` hand-off, synchronous acknowledgement per batch. Consumer: `spikes/local_throughput/stream_consumer.py`
(the JVM spike's consumer plus an optional per-event delay log; it counts and acknowledges). Evidence:
`evidence/p2/local_streaming/` (`stream.jsonl`: one record per measurement; `stream_summary.csv`; `setup.json`;
`confirm.jsonl`; `facts.json`; `attempt*_redelivery.jsonl`).

**JDK**: `/opt/homebrew/opt/openjdk@21` on this machine is a link to Homebrew's **OpenJDK 23.0.2** (aarch64), not 21.
The JDK lock (`jvm/jdk.lock.json`) has only Linux x64 tarballs, and the task forbids installing another JDK, so every
local run used 23.0.2 (classes compiled with `--release 21`). The Flight used Temurin 21 on x86-64.

## 0. The answer

| Question | Answer (medians of 3 runs) |
|---|---|
| Streaming rate, one 1M / 10M-row transaction, slot on the replica | **78K / 84K rows/s** (best config), 77K / 84K (defaults) |
| Per-row CPU of Debezium's streaming thread | **10.8–11.7 µs** here vs **30–40 µs** on the Flight: **≈ 3× lower, not the same** |
| Is that thread still the bottleneck? | **Yes**: busy ≈ 90 % of the wall time while the JVM as a whole uses ≈ 1 core; the walsender 0.4 core |
| What the source costs when it is not on the same CPUs | walsender **7–8 µs/row** (0.35–0.4 core at 80K rows/s), **33–36 µs per one-row transaction** at 8,000 tx/s; spill of a 10M-row transaction 2.6 GB on the replica's disk |
| Small transactions | backlog of 1M one-row transactions drained at **48–58K tx/s**; live 500–8,000 tx/s **kept up**, delay p50 0.27–0.49 s, p99 0.52–0.95 s, max ≤ 1.02 s; replica replay contributes ≤ 13 ms (p99) |
| Replica vs primary decoding | **no difference** beyond noise in rate, delay or behaviour |
| "best" vs "default" configuration (Flight run 8) | **no gain here** (ParallelGC costs more GC CPU; decimal-as-double saves little) |
| Surprises | (1) with the Flight spike's configuration Debezium **never confirms the last transaction before an idle period or a stop**, so every restart re-sends it; (2) a restart after a big transaction makes the walsender re-decode (and re-spill) it before anything new arrives |

## 1. Running the engine against the standby (item 1)

What was needed (`stream_setup.py`, `setup.json`):

- Publication **on the primary** (`CREATE PUBLICATION cdc_v2_stream_pub FOR TABLE … WITH (publish = 'insert, update,
  delete')`, by the admin role that owns the tables), `publication.autocreate.mode=disabled`; `SELECT` granted to the
  runtime role (`REPLICATION`, not a superuser); TLS through `database.sslmode=verify-full` and
  `database.sslrootcert`; the password only in a mode-600 properties file outside the repository.
- **The slot is created by our code on the standby** before the engine starts
  (`pg_create_logical_replication_slot(…, 'pgoutput')` as the runtime role). Creation waits for a running-transactions
  record from the primary:

| How | Time to create |
|---|---|
| unaided, right after writes on the primary | 4.8 s |
| unaided, primary idle for 20 s first | 8.1 s |
| unaided, primary idle 20 s, one write 2 s into the wait | 0.95 s (finished before the write: a record logged by the primary's background writer arrived first) |
| with `pg_log_standby_snapshot()` called on the primary every 0.5 s | 0.7–0.9 s (one call) |

  The admin role (the managed-provider-like role) **cannot** call it: `permission denied for function
  pg_log_standby_snapshot`; we used the bootstrap superuser through `tools/localpg`. Unaided, creation waited up to
  ≈ 8 s here because this primary always writes a little (the background writer logs a running-transactions record at
  most every 15 s, and only after other WAL); on a truly idle primary it would wait until the next write.
  The research rule (create the slot ourselves with a deadline, the primary heartbeat guarantees WAL) holds.
- Nothing else: the engine started on the standby slot in 0.7 s and the first 1,000 rows arrived 0.47 s after commit.
  No Debezium error or warning in any log.

## 2. Streaming rate (item 2)

`stream_bench.py --plan main`, 3 repetitions, rep-major (rep 1 of every test and configuration, then rep 2, …).
**Every test starts from a new slot** (§6 explains why). "best" = Flight run 8's combination
(`statistics.metrics.enabled=false`, `decimal.handling.mode=double`, batch 8192 / queue 32768, ParallelGC);
"default" = batch 2048 / queue 8192 / poll 500 ms, G1. CPU per event = CPU seconds of the process (or thread, from
`jcmd Thread.print`) over the measurement window ÷ events. Ranges are min–max of the 3 runs.

### 2a. One large transaction (`INSERT INTO stream_li SELECT * FROM lineitem LIMIT n`; `stream_li` has the primary key)

| | 1M best | 1M default | 10M best | 10M default |
|---|---|---|---|---|
| rows/s, first to last row | **78,438** (66,180–79,530) | **77,143** (59,370–79,340) | **83,648** (81,760–84,020) | **84,326** (71,730–85,230) |
| INSERT on the primary | 5.4 s | 5.8 s | 68.3 s | 62.4 s |
| replica replayed the commit, after COMMIT | 0.05 s | 0.08 s | 0.08 s | 0.11 s |
| COMMIT → first row in the consumer | 0.98 s | 1.12 s | 0.93 s | 0.99 s |
| JVM CPU / Debezium streaming thread (µs per row) | 20.9 / **11.2** | 21.6 / **11.7** | 18.1 / **10.8** | 18.3 / **11.4** |
| walsender / consumer (µs per row) | 7.7 / 0.2 | 6.9 / 0.2 | 7.4 / 0.2 | 7.2 / 0.2 |
| JVM / walsender share of a core | 109 % / 39 % | 108 % / 36 % | 96 % / 39 % | 99 % / 40 % |
| JVM peak RSS | 542 MB | 473 MB | 619 MB (600–784) | 484 MB |
| spill on the replica (`pg_stat_replication_slots`) | 4 spills, 260 MB | same | **39 spills, 2.60 GB** | same |
| `pg_replslot/<slot>` peak size | 256 MB | 256 MB | **2,559 MB** | 2,559 MB |

JVM thread CPU for 10M rows (best / default, seconds): streaming thread **108 / 114**, GC 20.8 / 8.0, metrics and
other `Thread-N` 22.9 / 24.2, engine poll plus our serialisation 24.7 / 32.9, JIT 3.2 / 3.7. At 83.6K rows/s the
10M rows take 119.5 s, so **the streaming thread is busy ≈ 90 % of the time**: it is the bottleneck, as on the Flight.
`statistics.metrics.enabled=false` again did not remove the `Thread-N` work (22.9 s vs 24.2 s).

The 10M-row transaction is decoded on the replica while it is replayed and spilled to `pg_replslot/` (39 files,
2.6 GB on the replica's disk); nothing is sent before COMMIT; the first row arrived 0.9–1.3 s after COMMIT (≈ 0.1 s
replay, the rest read-back and Debezium start of the batch).

### 2b. Small transactions (one `lineitem`-width row per transaction, `pgbench`, `stream_small`)

Backlog: 1M one-row transactions written by `pgbench` (16 clients; 23–35K tps) while no engine ran, then drained:

| | best | default |
|---|---|---|
| transactions/s, first to last event | **48,488** (41,570–59,420) | **57,592** (48,990–57,770) |
| JVM / streaming thread µs per transaction | 31.8 / 18.3 | 27.8 / 15.9 |
| spawn → first event | 1.5 s | 1.4 s |
| JVM peak RSS | 556 MB | 465 MB |

Per-transaction overhead adds ≈ 5–7 µs to the streaming thread (16–18 µs per one-row transaction vs 11 µs per row of a
big transaction): ≈ 40 % fewer events per second (the Flight measured "roughly halves").

Live load (`pgbench -R <rate> -T 60`, 4–32 clients, engine running). Delay = arrival in the consumer − Debezium's
`source.ts_ms` (the commit time on the primary), for every event committed in the window; replay lag =
`pg_stat_replication.replay_lag` of the replica on the primary, sampled every 0.25 s:

| rate (achieved) | config | kept up (drained after load end) | delay p50 / p99 / max | replay lag p99 | JVM / streaming thread / walsender µs per tx |
|---|---|---|---|---|---|
| 500 (500) | best | yes (0.7 s) | 494 / 945 / 1,017 ms | 9 ms | 178 / 55 / 83 |
| 500 (499) | default | yes (0.8 s) | 493 / 947 / 1,010 ms | 1 ms | 161 / 40 / 62 |
| 2,000 (2,003) | best | yes (0.4 s) | 293 / 823 / 1,002 ms | 1 ms | 74 / 54 / 57 |
| 2,000 (2,001) | default | yes (0.1 s) | 295 / 864 / 1,003 ms | 1 ms | 82 / 59 / 59 |
| 4,000 (3,992) | best | yes (0.4 s) | 276 / 716 / 920 ms | 2 ms | 54 / 40 / 42 |
| 4,000 (4,006) | default | yes (0.4 s) | 490 / 861 / 915 ms | 1 ms | 56 / 43 / 42 |
| 8,000 (8,004) | best | yes (0.4 s) | 280 / 752 / 923 ms | 1 ms | 43 / 32 / 33 |
| 8,000 (7,999) | default | yes (0.8 s) | 270 / 644 / 920 ms | 2 ms | 47 / 35 / 36 |

- **Every rate kept up**; the delay does not grow with the rate. It is set by Debezium's batching:
  `poll.interval.ms=500` gives p50 ≈ 0.27–0.5 s and max ≈ 1 s (two poll intervals). The replica adds ≤ 13 ms (p99
  replay lag ≤ 0.013 s in every run), so **almost all of the delay is on the Debezium side** and would shrink with a
  shorter poll interval (not measured here; the Flight saw no rate change from `poll.interval.ms` 10).
- Per-transaction CPU in live mode is higher than in the backlog (31–35 µs on the streaming thread at 8,000 tx/s,
  more at low rates where fixed per-poll and per-wake-up work is spread over fewer transactions). The walsender pays
  33–36 µs per transaction at 8,000 tx/s.

### 2c. Row shapes (1M rows in one transaction each; `upd_keyed` has the default replica identity, `upd_full` has `REPLICA IDENTITY FULL`)

| Statement | rows/s best | rows/s default | streaming thread µs/row (best / default) | walsender µs/row | spill |
|---|---|---|---|---|---|
| INSERT, keyed | 79,429 | 75,705 | 11.1 / 11.9 | 7.0 | 260 MB |
| UPDATE, keyed (`l_comment`) | **82,656** | 78,746 | 10.2 / 11.2 | 8.2 | 259 MB |
| DELETE, keyed | **105,996** | 97,713 | 8.4 / 9.0 | 5.4 | 148 MB |
| INSERT, FULL | 76,383 | 78,584 | 11.1 / 11.3 | 7.3 | 260 MB |
| UPDATE, FULL | **57,378** | 54,708 | 14.9 / 16.7 | 9.3 | 439 MB |
| DELETE, FULL | 77,010 | 76,735 | 11.6 / 12.0 | 7.5 | 259 MB |

A keyed DELETE carries only the key (cheapest); `REPLICA IDENTITY FULL` makes an UPDATE carry the whole old row too
(≈ 30 % slower, 70 % more spill) and a DELETE carry the whole row (as slow as an INSERT).

### 2d. Same thread, three times cheaper: comparison with the Flight

| Per row (1M / 10M insert) | Flight (`docs/spike/jvm_on_flight.md` §7.1) | here |
|---|---|---|
| Debezium streaming thread | 36–42 µs / 29.6 µs | 10.8–13.4 µs / 10.7–13.1 µs |
| metrics publisher and other `Thread-N` | 9–10 µs / 8.3 µs | 1.7–3.7 µs / 2.2–3.9 µs |
| GC | 1–2 µs / 4.2 µs | 0.9–2.3 µs / 0.8–2.1 µs |
| walsender | 10.3 µs / 9.5 µs | 6.9–8.3 µs / 7.1–8.8 µs |
| rate | 24–30K rows/s | 77–84K rows/s |

**The per-row cost is not the same.** The streaming thread needs ≈ 3× less CPU here, the walsender only ≈ 1.3× less.
A uniformly faster core would shrink both by about the same factor, so core speed alone does not explain the gap. Plausible
contributors that this machine cannot separate: the Flight's vCPUs are hyperthreads shared with the fixture Postgres
on the same 2 vCPUs (cache and SMT contention inflate the CPU time of a thread); JDK 23 on aarch64 here vs Temurin 21 on
x86-64 there (JIT code for the `Struct` building and date parsing that dominate the thread); `-XX:ActiveProcessorCount=2`
was the same. What is the same: **one Debezium streaming thread is the bottleneck on both machines**, and the rest of
the JVM roughly doubles its CPU. Consequence for SPEC R5/5.1: with the source off the Flight the Flight's streaming
thread may become cheaper than 30–40 µs if contention inflated it, but stage 2 must measure that (PLAN §2 B 8); this
laptop's ≈ 11 µs is not a prediction for an EPYC 7571 vCPU.

## 3. Decoding on the replica vs on the primary (item 3)

`stream_bench.py --plan primary` (best configuration, slot on 55500, 3 runs):

| | replica (best) | primary (best) |
|---|---|---|
| 1M-row transaction, rows/s | 78,438 (66,180–79,530) | 72,394 (71,050–82,280) |
| 1M: COMMIT → first row | 0.98 s | 1.23 s (0.95–2.20) |
| 1M: streaming thread / walsender µs per row | 11.2 / 7.7 | 11.9 / 7.8 |
| backlog of 1M one-row transactions, tx/s | 48,488 (41,570–59,420) | 55,183 (54,790–60,660) |

**No difference beyond the run-to-run spread** (which is itself 15–30 % on this shared machine), and no difference in
behaviour: same spill pattern, same events, no errors. On the replica, decoding waits for replay, but replay finished
0.05–0.11 s after COMMIT even for 10M rows, so the replica added nothing measurable. A replica that lags (replay
conflicts, part 1 §8) would add its lag one to one.

## 4. What Debezium delivers per event and per transaction, next to what the server sent (item 4)

`stream_facts.py` (`facts.json`, `facts_sample.jsonl`). Two pgoutput slots on the **replica**, created back to back:
Debezium's, and a peek slot read afterwards with `pg_logical_slot_peek_binary_changes(…, 'proto_version', '1',
'publication_names', 'cdc_v2_stream_pub')` and parsed (BEGIN: final LSN, commit time, xid; COMMIT: flags, commit
LSN, end LSN; the `lsn` column of each row). Debezium ran the stock engine with `provide.transaction.metadata=true`
through the spike program `spikes/local_throughput/java/FactsMain.java` (it dumps every record's offset, `source`,
`transaction` block and the BEGIN/END events; EngineMain drops non-row records, so it could not show them).
Workload: **1,222 committed transactions with published changes and 38,427 change events**: 636 inserts of 1–50
rows, 223 updates and 110 deletes of 1–5 rows on a keyed table, 231 multi-table transactions (122 of them also
wrote an unpublished table), a 20,000-row preload, 18 sentinels, 3 savepoint transactions, and the special cases
below. **All 1,222 server transactions were matched by exactly one Debezium BEGIN and END, and Debezium delivered
no transaction the server did not send.**

Every check below held for **all** transactions or events (the counts are in `facts.json` → `checks`):

| Debezium field | equals (server value) |
|---|---|
| change `source.lsn`, offset `lsn`, offset `lsn_proc` | the LSN of that change's row in the stream (the change record) |
| change `source.txId` | the BEGIN message's xid |
| change `source.sequence` | `[previous transaction's commit end LSN, source.lsn]` (`null` first element for the first transaction after slot creation) |
| change offset `lsn_commit` | the **previous** transaction's commit **end** LSN (never this transaction's) |
| END offset `lsn_commit` = `lsn` = `lsn_proc` | **this** transaction's commit **end** LSN (COMMIT message `end_lsn`; also the `lsn` column of the COMMIT row) |
| BEGIN offset `lsn_commit` | the previous transaction's commit end LSN |
| END `event_count` | the number of change events of the transaction = the server's I/U/D messages |
| END `data_collections[].event_count` | per-table change events |
| change `transaction.total_order` | 1-based position of the event in its transaction |
| BEGIN `id` | `"<xid>:<LSN of the first change>"` (= the `lsn` column of the server's BEGIN row) |
| change `transaction.id` | `"<xid>:<that change's own LSN>"` |
| END `id` | `"<xid>:<commit end LSN>"` |

Answers:

- **The commit end LSN is known from the END event** (offset `lsn_commit`, and the LSN part of END's `id`), for
  every transaction. A change event does **not** carry its own transaction's commit end LSN: its `lsn_commit` and
  `sequence[0]` are the *previous* transaction's. Without transaction metadata a consumer would learn T's commit end
  LSN only from the first event of the next transaction (its `sequence[0]`), i.e. only once another transaction
  arrives.
- **The commit record's start LSN cannot be known from Debezium's output at all**: the server sends it twice (BEGIN
  `final_lsn` = COMMIT `commit_lsn`, equal in 1,222 of 1,222), and no Debezium field ever equals it (0 of 1,222).
  It does not matter for the design: the server skips by commit end position on restart (`docs/research/debezium.md`
  §3.1), and the applied position of `docs/design/core.md` §2 is the END record's `lsn_commit` = commit end LSN.
- **Debezium's transaction `id` is not constant within a transaction**: BEGIN, each change and END carry
  `"<xid>:<own LSN>"`, so `END.id = BEGIN.id` held for 0 of 1,222 transactions. Only the xid prefix is common, and
  the xid is 32 bits and wraps. The consumer must therefore group events by their **order** in the stream (BEGIN,
  changes, END are contiguous, protocol 1) and check with `total_order` and END `event_count`, not by `id`.
- Special transactions (3 of each, between sentinel transactions):

| Transaction | Server (peek slot) | Debezium |
|---|---|---|
| empty (`BEGIN; COMMIT`) | nothing | nothing (between one sentinel's END and the next sentinel's END: only that sentinel's BEGIN and change) |
| xid assigned, no write (`pg_current_xact_id()`) | nothing | nothing |
| only an unpublished table written | nothing (PG 15+ skips empty transactions) | nothing |
| rolled back | nothing | nothing |
| insert 1; savepoint; insert 2; rollback to savepoint; insert 1; savepoint; insert 4; release | one transaction, 6 changes | one transaction, 6 change events, END `event_count` 6 |

## 5. Restart in the middle of a 10M-row transaction (item 5)

`stream_bench.py --plan restart` (best configuration, slot on the replica; `stream.jsonl` → `test: restart`). The
engine was stopped after ≈ 5M of the 10M events had been received and acknowledged, once with `SIGTERM` (clean
`engine.close()`, JVM gone after 0.49 s) and once with `SIGKILL` (gone after 0.04 s), then restarted on the same slot:

| | SIGTERM | SIGKILL |
|---|---|---|
| events received before the stop | 5,005,606 | 5,001,286 |
| slot `confirmed_flush_lsn` after the stop | moved only past the canary, **still before the 10M transaction** | same |
| slot `restart_lsn` | unchanged (before the transaction) | unchanged |
| restart: spawn → engine polling | 0.84 s | 0.92 s |
| restart: spawn → **first event** | **36.3 s** | **35.8 s** |
| events after restart | **10,000,000** (the whole transaction again, nothing else) | **10,000,000** |
| first → last event after restart | 133 s | 139 s |

The server re-sends **the whole transaction**: mid-transaction offsets carry the previous transaction's `lsn_commit`
(§4), so nothing inside the 10M transaction can ever be confirmed. The 36 s before the first event is the walsender
reading the WAL again from `restart_lsn` and **re-decoding and re-spilling the entire transaction** (2.6 GB on the
replica's disk) before it can send the first row. The same cost appeared, unplanned, when a new engine started on a
slot whose most recent transaction was a 10M-row insert (`attempt2_redelivery.jsonl`: 36.5 s to the first event,
2.6 GB spilled again).

## 6. Surprise: the last transaction before an idle period is never confirmed (configuration of the Flight spike)

The first two attempts of the main plan reused one slot for all tests. Every new engine first received again what
the previous engine had already received and acknowledged: the 1M-row transaction, later 11M events
(`attempt1_redelivery.jsonl`, `attempt2_redelivery.jsonl`; those runs are not used for any number above). The
dedicated test (`stream_bench.py --plan confirm`, `confirm.jsonl`; 1,000-row transaction, then idle):

| Configuration | confirmed after 2 s idle | after 10 s idle | after a clean stop | events re-sent on restart |
|---|---|---|---|---|
| `offset.flush.interval.ms=1000`, no heartbeat (the JVM spike's configuration) | no | no | no | **1,000** |
| `offset.flush.interval.ms=0`, `heartbeat.interval.ms=1000` | yes | yes | yes | 0 |

Debezium hands an LSN to the walsender only from `performCommit()` on a later `poll()` after an offset flush that
`markBatchFinished()` triggered (`docs/research/debezium.md` §3.1). With no further batches (idle source) the last
transaction's commit LSN is never sent, and `engine.close()` does not send it either. That is harmless for
correctness (the applier discards transactions at or below its applied position), but **every restart and every run
hand-over re-sends — and for a large transaction re-decodes — the last transaction**, and WAL retention on the
replica is held back by it. Research §3.3 already prescribes `offset.flush.interval.ms=0` plus
`heartbeat.interval.ms`; this measurement shows it is **required**, not optional. All benchmark tests above use a new
slot per test, so they keep the Flight spike's configuration and are unaffected.

## 7. Contradictions and consequences for SPEC, PLAN and `docs/design/core.md`

- `docs/design/core.md` §2 holds as written: applied position = END `lsn_commit` = commit end LSN (1,222 / 1,222);
  END `event_count` equals the events received (1,222 / 1,222); whole transactions are re-sent after a restart.
  To add: **group events by stream order, not by the transaction `id`** (it changes per record); the commit start
  LSN is not available.
- SPEC R5 / 5.1: even on these fast cores, with the source on other cores, one Debezium connector streams 77–84K
  rows/s of `lineitem` inserts (106K only for keyed deletes): **stock Debezium stays below 100K rows/s here too**,
  limited by its single streaming thread. On the Flight the cap measured 24–30K.
- SPEC 5.2: the stream delay is ≈ 0.3–0.5 s median and ≈ 1 s maximum at up to 8,000 tx/s, dominated by
  `poll.interval.ms`. A restart after a 10M-row transaction costs ≈ 36 s of re-decoding before anything new arrives;
  that counts against the 120 s recovery after a hand-over (SPEC 5.2) and is another reason to confirm promptly (§6).
- A 10M-row transaction needs 2.6 GB of spill on the server that holds the slot (the replica in replica mode), and
  the same again on every re-decode (hazard H-049 in research).

## 8. What could not be done, and open questions

- JDK 21 was not available locally (the "openjdk@21" path is OpenJDK 23.0.2); a Temurin 21 aarch64 tarball would
  have meant installing a JDK, which the task excluded.
- Per-thread CPU on macOS comes from `jcmd Thread.print` (`cpu=…ms` per thread) at the start and end of each window;
  the walsender's CPU from `psutil` (missing for the backlog runs, where the slot became active only after the sampler
  had started).
- Delay is measured against `source.ts_ms` (millisecond resolution, the primary's commit time, same host clock).
- Open for stage 2 (PLAN §2 B 8): the streaming thread's CPU per row on an F16 vCPU with the source elsewhere (30–40 µs
  with the fixture on the same 2 vCPUs; ≈ 11 µs here), and whether `poll.interval.ms` below 500 ms lowers the delay
  without costing rate.

## 9. Reproduce

```sh
spikes/local_throughput/localpg up                                                          # lineitem from part 1
tools/with-env uv run --group spike python spikes/local_throughput/stream_setup.py           # item 1, setup.json
tools/with-env uv run --group spike python spikes/local_throughput/stream_bench.py --plan confirm --reps 1 --configs best
tools/with-env uv run --group spike python spikes/local_throughput/stream_bench.py --plan main      # item 2, ≈ 75 min
tools/with-env uv run --group spike python spikes/local_throughput/stream_bench.py --plan primary --configs best   # item 3
tools/with-env uv run --group spike python spikes/local_throughput/stream_bench.py --plan restart --reps 1 --configs best  # item 5
tools/with-env uv run --group spike python spikes/local_throughput/stream_facts.py --tx 1200   # item 4
tools/with-env uv run --group spike python spikes/local_throughput/stream_summary.py           # stream_summary.csv
```

Load: three other workers ran tests and a stress test on this machine. The 1-minute load average was 3–8 for most
runs and reached 18–52 during a few (recorded per run); those runs are the low ends of the ranges (for example 1M
default 59K rows/s at load 23.7, UPDATE FULL 45K rows/s at load 51.6). Medians are used throughout.
