# Spike report — stage 1 (P2 Platform spike)

Date: 2026-11-10. Author: the orchestrator. For: the owner.

This report answers the five questions of PLAN §2 A. Each answer is a summary; the detail, the method and the
evidence files are in the report named in the right-hand column. The architecture that follows from the answers is
recorded in `docs/adr/0001-architecture.md`, the contracts in `docs/design/core.md`.

Two limits apply to every number below:

- A Flight cannot reach this machine. Numbers "on a Flight" were measured on real F16 Flights, but without a
  Postgres source outside the Flight. Where a source was needed on a Flight, it was a throw-away Postgres inside the
  same container, sharing the Flight's 2 CPUs.
- Numbers "local" were measured on the development machine (10 cores, 16 GB). PLAN: they choose the architecture and
  are never reported as performance results. The SPEC numbers (3.1, 5.1–5.3) are measured in stage 2.

## 1. Result in one page

**Nothing found contradicts SPEC. The build continues.** Architecture candidate B is confirmed.

What the owner should know, most important first:

1. **The stream is the bottleneck, and it is below the 100K rows/s target of SPEC 5.1.** Stock Debezium on a
   Flight streamed 23–30K rows/s at `lineitem` width (one streaming thread, 30–40 µs of CPU per row; the fixture
   Postgres shared the 2 CPUs). On the development machine, with the source on other cores, the same thread needs
   11 µs per row and the rate is 77–84K rows/s: still one thread, still the limit, still under 100K. No setting
   tried raised either number by more than 10 %. SPEC D3 expects this case; 5.1 is reported as measured, and the
   tuning is P9. What an F16 reaches with the source outside the Flight is the first measurement of stage 2.
   Everything after the stream has a margin: apply reaches 342–355K rows/s at 1M-row groups on one connection.
2. **The backfill target of SPEC 3.1 (100K rows/s) has a wide margin.** Local read rate from the replica: 1.3M
   rows/s with two connections; load into MotherDuck on a Flight: 1.58M rows/s. The real number is a stage 2
   measurement.
3. **All sessions of one MotherDuck user share one server instance.** Another session's heavy query made a Flight's
   server-side work 4–5 times slower. The pipeline needs an account that nothing else uses. The build currently
   runs under the owner's own account (PLAN §9).
4. **On a replica, anything that holds a lock on a table for long can stop the stream for 30 s.** When the primary
   takes an exclusive lock on that table (a schema change, a truncate, even a vacuum that shortens the table), replay
   on the replica waits up to `max_standby_streaming_delay` for the reader, and the slot on the replica delivers
   nothing meanwhile. The pipeline's own reads are built to avoid it; other users of the replica can still cause it.
   This is a risk for SPEC 5.2 in replica mode.
5. **Every platform stop is a kill without a signal**, a cancelled run can still commit for about 2.5 s, and an
   out-of-memory kill of any child ends the whole run. The fencing of SPEC 4.2 is therefore mandatory in every
   destination transaction, and nothing may rely on a shutdown handler.
6. **A dead leader is replaced after at most about 98 s** (30 s until the heartbeat is stale, up to 60 s to the next
   scheduled minute, 4–8 s for the newcomer's guard). SPEC 4.2 says "within 90 s plus start-up"; this holds if
   start-up includes the newcomer's guard decision.
7. **A hand-over needs 14.6 s** from the platform's start to the first change event (install Python packages, clone,
   JDK, jars, compile, engine start).
8. **The every-minute schedule costs $0.73–$1.46 a day**, if MotherDuck bills per second. Billing granularity is not
   documented; with a one-minute minimum per run it would be $14.40 a day.

Open decisions for the owner are listed in `STATUS.md` ("Questions for the owner"). None of them blocks the build.

## 2. The five questions

### 2.1 A JVM and stock Debezium on a Flight — `docs/spike/jvm_on_flight.md`

| Question | Answer |
|---|---|
| Can a JDK be installed at start-up? | Yes. Eclipse Temurin 21 as a tarball, locked by SHA-256: 4.1–4.6 s per run. No JVM is preinstalled; downloads run at 235–265 MB/s |
| Can the public repository be cloned without a credential? | Yes, at a pinned commit hash, in 0.3–0.4 s |
| Does stock Debezium run? | Yes. 3.7.1.Final, embedded engine, 73 jars (40.2 MB) from Maven Central, each verified by size and SHA-256; end to end against a Postgres 18 fixture inside the Flight |
| Time from run start to the first change event | **14.6 s** |
| Memory left for the pipeline | The JVM peaks at 0.3–0.8 GB resident with capped heaps; more than 14 GB of the 16 GB remain |
| Streaming rate (first indication, source on the same 2 CPUs) | 23–26K rows/s for a 1M-row transaction, 30K rows/s for a 10M-row transaction, 10K one-row transactions per second when draining a backlog |
| What supervision must handle | An orphaned JVM keeps running and keeps the slot: it must watch its parent. A frozen JVM holds the slot for 60 s (`wal_sender_timeout`). Change records need their own pipe |

### 2.2 Run semantics — `docs/spike/run_semantics.md`

| Question | Answer |
|---|---|
| A scheduled start while another run is active | **It starts.** No skip, no queue; up to 9 overlapping runs were seen. This is what makes replacement of a frozen leader possible |
| How long does a run that finds a healthy leader take, and what do 1,440 a day cost? | As a scheduled run: 5.5 s (median; mean 6.1 s) with the `duckdb` package, 2.5 s (mean 3.0 s) with a guard that reads the leader record through MotherDuck's Postgres endpoint and needs no package. $1.46 or $0.73 a day at per-second billing |
| Can a run cancel another run from inside a Flight? | Yes. The record shows `CANCELLED` after 0.5–0.6 s; the processes are killed 2.3–2.5 s after the call **and can commit until then** |
| Are failed runs retried? | No. Nothing is retried and nothing alerts |
| What happens at the maximum run time? | The run is killed without a signal, late and coarsely (3–118 s after the limit). Final state `CANCELLED` |
| Signals | None, for any platform stop (cancel, maximum run time, delete, teardown) |
| Out of memory | A kill in any child ends the whole run (exit code 137, nothing in the log) |
| A frozen run and the heartbeat hard-exit | The bootstrap's hard-exit ended a frozen pipeline's run 65 s after the freeze. A frozen bootstrap is bounded by the next run's guard |
| Deleting a Flight | Does not stop its active runs, and they can no longer be cancelled; cancel first, then delete |

### 2.3 MotherDuck semantics — `docs/spike/md_semantics.md`

| Question | Answer |
|---|---|
| Drop + rename in one transaction under a concurrent reader | Atomic. A reader sees the old or the new table, never neither; long scans across 60 swaps stayed consistent |
| `ALTER … TYPE UNION(…)` | Works, but not on a column with a key, a unique constraint or an index. Destination tables therefore carry none; the applier enforces keys |
| Size limits of one transaction | None hit: 50M rows inserted, updated and deleted in one transaction. Position and data commit together or not at all, also when the client is killed during the commit; such a commit can still land up to 2 s after the kill |
| Concurrent writers | Parallel transactions that update one shared row conflict: appliers need separate state rows. A delete against an update of the same row is **not** detected as a conflict: the leader row is only ever updated |
| Leader record as a compare-and-swap | Exactly one of several simultaneous starters wins. A dead client's open transaction holds its rows for about 30 s unless the newcomer interrupts that server connection |
| A single clock | `now()` is computed by the client. `uuid_extract_timestamp(uuidv7())` is computed by MotherDuck and is used for heartbeat age. A Flight's clock differed from MotherDuck's by 3 ms (median; largest 38 ms) |
| Bulk-load rate at `lineitem` width (on a Flight) | 1.58M rows/s by `COPY` from Parquet; 1.11M rows/s from one Arrow stream. More than two loader processes never helped (2 CPUs) |
| Apply cost (on a Flight) | One connection: 0.066 s for a transaction of 1–100 rows (round trip 8 ms); 212K rows/s at 100K-row groups; 342–355K rows/s at 1M-row groups. Small transactions must share commits: one connection does 30 commits a second, so 2,000 source transactions per second need about 70 per commit |

### 2.4 Service-account ownership — `docs/spike/flight_platform.md`

| Question | Answer |
|---|---|
| Create, schedule, run, cancel, read logs, delete through the Flight SQL functions, with secrets attached | All work from a SQL connection (`tools/flightctl`). Logs are readable within 1 s, also while the run is active |
| "The run-time token sees only this project's databases" | **Not shown in this session.** The build runs under the owner's own account (PLAN §9); the token a Flight receives is that user's and sees the user's other databases (counted, never listed). It must be shown again with the service account |
| Platform facts the design uses | 2 CPUs and 16 GB by cgroup, while the container reports 8 CPUs and 64 GB; the run is root; no Python package is preinstalled (`duckdb` installs in 2.5 s at every run); a run knows its own Flight id and run number; schedule granularity is one minute; clearing a schedule disables it but keeps the text |

### 2.5 Local throughput, first indication only — `docs/spike/local_throughput.md`, `docs/spike/local_streaming.md`

Read rate from the local replica (TPC-H SF2 `lineitem`, 12.0M rows, 2 GB):

| Method | 1 connection | 2 | 4 |
|---|---|---|---|
| Our `COPY` over `ctid` ranges, parsed into Arrow | 0.67M rows/s | 1.31M | 2.47M |
| The same over primary-key ranges (binary, not parsed) | 0.48M | 0.84M | 1.07M |
| DuckDB's Postgres reader, to local Parquet | 0.84M | 1.60M | 2.97M |

One connection is bound by its Postgres backend at about 1.0–1.1M rows/s, whatever the format. End to end from the
laptop into MotherDuck: 474–586K rows/s, limited by the upload. Estimate for an F16 Flight, not measured: 0.65–0.95M
rows/s for our reader.

Stock Debezium 3.7.1 streaming from a slot on the local replica (medians of 3 runs; the machine was shared with
three other workers, so the spread is 15–30 %):

| Measurement | Result |
|---|---|
| One 1M-row / 10M-row transaction | **78K / 84K rows/s**; with Debezium's defaults 77K / 84K |
| Other row shapes (1M rows) | keyed UPDATE 83K, keyed DELETE 106K; with `REPLICA IDENTITY FULL`: UPDATE 57K, DELETE 77K |
| A backlog of 1M one-row transactions | 48–58K transactions per second |
| Live load of 500 to 8,000 one-row transactions per second | every rate kept up; delay from commit to consumer p50 0.27–0.49 s, p99 0.52–0.95 s, max 1.02 s; the replica's replay adds at most 13 ms (p99) |
| CPU per row | Debezium's streaming thread 10.8–11.7 µs (30–40 µs on the Flight), busy 90 % of the time; the walsender 7–8 µs |
| Slot on the replica against slot on the primary | no difference beyond the spread (78K against 72K rows/s; 48K against 55K transactions per second) |
| A 10M-row transaction on the server that holds the slot | 2.6 GB spilled to disk (39 files); first row 0.9–1.0 s after the commit |
| Restart in the middle of a 10M-row transaction (SIGTERM and SIGKILL) | the slot had not moved; the server decoded and spilled the transaction again: first row after **36 s**, then exactly 10,000,000 rows |
| Creating the slot on the replica | 0.95–8.1 s unaided; 0.7–0.9 s when the primary is asked for a running-transactions record, which the admin role may not do |

What Debezium reports was compared with the server's own stream for 1,222 transactions and 38,427 changes, without
one mismatch:

- The END record carries the transaction's commit end LSN, and its `event_count` is exact. This is the applied
  position of the design.
- A change record carries the **previous** transaction's commit position, never its own. Debezium's transaction
  `id` differs from record to record. Records are therefore grouped by their order in the stream.
- Empty transactions, rolled-back transactions and transactions that wrote only unpublished tables produce
  nothing.

Two findings for the design:

- With the configuration of the Flight spike (offset flush every second, no heartbeat) **Debezium never confirms
  the last transaction before an idle period or a stop**; every restart receives it again. With
  `offset.flush.interval.ms=0` and a heartbeat interval it is confirmed within 2 s. The design already prescribed
  this configuration; it is now a requirement with a test (package E).
- **A restart after a large transaction costs the server a second decode** (36 s for 10M rows here) before anything
  new arrives. This time counts against the 120 s recovery of SPEC 5.2.

Not done: a run on JDK 21 (the machine's `openjdk@21` is OpenJDK 23; the Flight uses Temurin 21).

## 3. What the numbers mean for the SPEC targets

| SPEC item | Target | Indication | Limiting stage | Measured for real in |
|---|---|---|---|---|
| 3.1 Backfill | ≥ 100K rows/s on F16 | read 0.65–0.95M rows/s (estimate), load 1.58M rows/s (Flight) | the read, on 2 CPUs | P9 |
| 5.1 One 10M-row transaction | 100K rows/s, scored as measured | stream 23–30K rows/s (Flight, shared CPUs), 77–84K rows/s (local, source on other cores); apply 342–355K rows/s (Flight) | Debezium's streaming thread | P9, with the tuning log |
| 5.2 Latency | p99 ≤ 30 s, max ≤ 60 s; recovery ≤ 120 s | stream delay p99 under 1 s up to 8,000 transactions per second (local); a group commit costs 0.066 s; groups close after 1 s | stalls, not steady state: replay stalls on the replica (finding 4 above), very large transactions ahead in the stream, a second decode after a restart behind a large transaction (36 s for 10M rows, local) | P9 (24 h) |
| 5.3 Small transactions | ≥ 2,000 per second | stream 10K one-row transactions per second (Flight, backlog), 48–58K (local, backlog), 8,000 live kept up (local); apply needs about 70 transactions per commit | none found | P9 |
| 4.2 Replacement of a dead leader | within 90 s plus start-up | about 98 s worst case to the newcomer's decision, plus 14.6 s to the first event | the one-minute schedule | P6 locally, P10 |
| 5.4 Memory | bounded | JVM 0.3–0.8 GB; an unbounded Arrow reader grew to 2 GB; a whole 55 MB chunk buffered cost 1.4–2 GB per worker | hand-offs must be bounded | P4–P6 |

## 4. Decisions taken

All in `docs/adr/0001-architecture.md`:

1. Candidate B: Debezium 3.7.1.Final embedded in our own JVM child process, Temurin 21, records on their own pipe,
   acknowledge after the destination commit.
2. One leader: every-minute schedule with a guard; separate leader, fence and heartbeat rows; the fence as the last
   statement of every writing transaction; MotherDuck's clock for heartbeat age.
3. Apply: groups of whole source transactions, applied directly from local Arrow data with `DELETE … USING` +
   `INSERT`; staging tables only for a transaction larger than a group.
4. Backfill: our own parallel `COPY` over `ctid` ranges from the replica, every chunk in its own short transaction;
   pending log and atomic swap.
5. A MotherDuck account of its own for the pipeline.

## 5. Findings that changed the design after it was first written

- **Rows that no chunk reads.** With chunks read under different snapshots, a row that an update moves into a range
  that was already read is in no chunk. The stream carries the update, but for large values that did not change it
  carries only a placeholder. Such rows are now fetched by key during catch-up (`docs/design/core.md` 7.3). Reading
  by primary-key ranges does not avoid the case, because a key can change.
- **A key change with unchanged large values.** Debezium turns it into a delete and a create; the create carries the
  placeholder. The value is taken from the row with the old key (6.2).
- **The cut on a primary.** A transaction can be decoded by the slot before snapshots can see it. The rule for
  primary mode is being worked out and proven by the cut stress test (7.2, open point 12).
- **A table without a key and SPEC 3.7.** Such a table needs one snapshot for the whole read; if it is lost, the
  table is read again. The row bound of 3.7 is met for tables with a key only.
- **Grouping and confirmation** (section 2.5). Records are grouped by stream order, not by Debezium's transaction
  `id`; `offset.flush.interval.ms=0` with a heartbeat interval is required; the time of a second decode after a
  restart is part of the recovery time (`docs/design/core.md` sections 2 and 3).

## 6. What stage 2 must measure first

Collected in `STATUS.md` ("To verify in stage 2") and `docs/research/replica_decoding.md` §10. From the spikes:
the streaming rate with the source outside the Flight, and the CPU per row of Debezium's streaming thread there
(30–40 µs with the fixture on the same CPUs, 11 µs on the development machine); whether the provider's admin role
may ask for a running-transactions record; the read rate from a PlanetScale replica; the value of
`max_standby_streaming_delay` there and who else uses the replica; the service account's view of databases; the
billing of short runs.

## 7. Cost of the spikes

Recorded F16 run time: the JVM spike 51 min (about $0.51 at $0.60 an hour), the repeat of the MotherDuck measurements
on a Flight 21 min (about $0.21). The platform and run-semantics spikes recorded no total; their runs lasted seconds
to a few minutes each, the longest series being two every-minute schedules watched for 34 minutes. Every spike
Flight and spike database was deleted when its spike ended; this is checked again at the P2 gate.
