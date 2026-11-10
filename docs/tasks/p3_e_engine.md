# Task: package E — the engine

Read `docs/tasks/README.md` first; its rules bind you.

You build the part of the pipeline between the replication slot and the first Arrow batch in Python: our JVM
program around the stock Debezium embedded engine, the frame protocol on its pipe, and the Python side that starts
the JVM, reads frames and sends commands. Contract: `docs/design/p3_plan.md` C0, C1, C2. Everything downstream
(transform, stream assembly, apply, fuzzers) builds on what you deliver.

## Read as well

- `docs/research/debezium.md` completely. Sections 3 (delivery contract), 4–7 and 8.4 (configuration) are the
  basis of this package. Where the research says **[test]**, the claim is not proven yet; the ones on your path
  are yours to prove or refute.
- `docs/adr/0001-architecture.md` (decision 1), `docs/design/core.md` sections 1–3, `docs/spike/jvm_on_flight.md`,
  `docs/wal_hazards.md` H-002, H-003, H-013 and H-017.
- `docs/spike/local_streaming.md` if it exists when you start (how the position fields of Debezium's offsets map to
  the positions of the replication protocol). If it is not there yet, establish the mapping yourself with a test.
- `docs/testing/harness.md` (crash points, the Java side of the contract, process control, the byte-stream relay).
- The spike program `jvm/src/cdc_v2/engine/EngineMain.java` and `tests/test_spike_consumer.py`. Leave both as they
  are; your program is new, next to it. `src/cdc_v2/artifacts.py` fetches and verifies the JDK and the jars.

## Resources

- Postgres: `tools/localpg --home ~/.cache/cdc_flight_v2/pg-e --port-base 55620` (see `docs/localpg.md`). The
  engine decodes from the standby of that pair, as in production. No MotherDuck.
- JDK and jars: only what `jvm/jdk.lock.json` and `jvm/debezium.lock.json` lock, fetched through
  `cdc_v2.artifacts` into `~/.cache/cdc_flight_v2/`. No other Java library. Our Java code uses the JDK and the
  locked jars only. Debezium is used unmodified (SPEC R2): configuration and its documented extension points
  (`ChangeConsumer`, `CustomConverter`) only.

## What to build

### 1. The JVM program (`jvm/src/cdc_v2/engine/`, a new main class)

1. **Configuration.** The first line on standard input is `CONFIG <json>`: the Debezium properties, including the
   password. The program adds nothing secret to its arguments, its environment, a file or a log line, and it never
   logs a property value whose name contains `password`. Start from `docs/research/debezium.md` 8.4; every
   deviation is written down with its reason. The Python side builds the properties; Java only passes them on.
2. **Consumer.** `handleBatch` writes every record as frames to file descriptor 3 and never waits for the
   destination. Acknowledgement follows research 3.3 exactly: the program keeps references only to `END`
   records that are not acknowledged yet (never to heartbeat records; plan C2, changed after the second review),
   and on `DURABLE p` marks the newest one whose `ack_position` is at or below `p` as processed and finishes the
   batch. This bookkeeping is bounded: when it holds more than a
   configured number of records it thins them (dropping an entry is always safe: a later `DURABLE` then
   acknowledges an older record). `DURABLE` commands are applied at the start and at the end of every
   `handleBatch` call; show how long an acknowledgement can be delayed when the stream is idle and when it is busy.
3. **Frames.** As C2. You fix the byte layout and document it. Requirements:
   - `ROWS` frames are written by column, in Arrow buffer layout, little-endian, each buffer padded so that Python
     can wrap it without copying. A frame is flushed when it reaches a row count or a byte size (both
     configurable), when the table or schema version changes, and at the end of each engine batch. A transaction of
     10 million rows therefore never accumulates in the JVM: memory is bounded by the frame size and Debezium's
     queue, whatever the transaction size. A single value larger than the frame size (H-002: up to 1 GB as text)
     travels in a frame of its own; decide and document the limit and what happens above it (never silent
     truncation).
   - `SCHEMA` frame before the first `ROWS` of a table and whenever the record schema of a table changes: per
     column the position, source name, Connect type and Debezium semantic name, source type name, length and scale
     (`column.propagate.source.type`), nullable, key flag, and whether "unchanged" is possible for it.
   - The transport form of every Debezium value type: lossless first, then cheap. Baseline families must travel in
     a fixed-width or plain text/bytes form that DuckDB converts by column without a Python loop: booleans,
     integers, floats (NaN, ±infinity, −0 preserved), `numeric` as the text Debezium gives, text types, `bytea`,
     `uuid`, `json`/`jsonb` text, and the temporal types in a form that keeps ±infinity, years outside 1–9999 and
     `24:00:00` (research section 7, rule 2). Arrays, bit strings, composites, ranges and the rest travel as the
     Postgres text form (custom converter where the research says the default is lossy). Deliver the table
     "Postgres type → Debezium schema → transport form" in your document; package Y builds the conversion to
     destination types from it and nothing else. Mark each row of that table as tested or not.
   - "Unchanged" (H-003): the engine turns every unchanged-TOAST marker into the `u<i>` flag and a NULL in `a<i>`.
     No later code compares a value with a placeholder. The tests of H-003 are yours, including a real value equal
     to the placeholder bytes in a `bytea` column, a text column, and an array element.
   - Key change: on the create record that follows a key change, `k<i>` carries the old key. Establish from the
     records Debezium emits (delete with the old key, then create) how the engine recognises the pair, and prove it
     for a chain A→B, B→C and for a swap inside one transaction.
   - Tables without a key (`REPLICA IDENTITY FULL`): `b<i>` carries the old image for update and delete.
   - all raw position fields on `BEGIN`, `END`, `HEARTBEAT`; `ack_position` on `END`; `END` with the event counts.
   - `TRUNCATE` (set `skipped.operations=none`), `MESSAGE`, `BATCH_END`, `HELLO`, `FATAL`.
4. **Stopping.** `STOP`: close the engine and exit 0 within a deadline; if Debezium does not close in time
   (research 8.2, known stalls), halt. End of standard input: halt at once, acknowledging nothing. Any engine
   failure: a `FATAL` frame with class, message and cause chain (no row values, no connection string), then exit
   non-zero. A second thread watches the parent so that a blocked write on file descriptor 3 cannot keep the JVM
   alive after the parent is gone.
5. **Back-pressure and memory.** Explicit heap limit from the configuration; `max.queue.size.in.bytes` bounded;
   nothing unbounded between the slot and file descriptor 3. Show it: a reader that stops reading for minutes
   leaves the JVM's resident memory flat and the walsender connection alive (`wal_sender_timeout` 60 s).
6. **Crash points, Java side.** Implement the environment contract of `docs/testing/harness.md` section 1 in Java
   (the splitmix64 reference values, the hit log line, death by a real SIGKILL). Declare and call `engine.acked`
   (design section 3, step 8) and one point immediately before the acknowledgement is handed to Debezium. Provide
   a way to list the Java points so that the Python-side registry check can include them.

### 2. The Python side

- `src/cdc_v2/stream/frames.py` — decoder: validates magic, sequence, length limit and checksum; yields typed frame
  objects; `ROWS` as a `pyarrow.RecordBatch` built with `from_buffers` (no per-value work). A broken stream raises
  `EngineBroken` and yields nothing further. Reads are incremental: a frame is never required to fit a single
  `read`.
- `src/cdc_v2/testing/frames.py` — an encoder in Python that produces byte-identical frames (for hermetic tests
  here, and for the stream fuzzer and the tests of later packages, which must not need a JVM).
- `src/cdc_v2/stream/engine.py` — `Engine` as in C2: builds the properties from a frozen settings dataclass,
  starts the JVM as a child in the caller's process group with file descriptor 3 and standard input as pipes,
  passes `CDC_V2_CRASH_*` through, drains standard output and standard error into the log without blocking, sends
  `CONFIG`, `DURABLE`, `STOP`; `frames(deadline)`, `durable(position)`, `stop(deadline)`, `kill()`. Every wait has a
  `cdc_v2.deadline.Deadline`. After `kill()` or a failure, no process and no file descriptor is left.
- A function that prepares the JVM artifacts in a cache directory (JDK, jars, our classes compiled with `javac`,
  keyed by a hash of the sources), for the tests now and for the bootstrap later.

### 3. Tests

Hermetic (default run, a few seconds):

- Codec round trip with the Python encoder: every frame type, every physical type, NULLs, empty batches, zero-length
  values, values with a zero byte, the largest and smallest numbers; property-based with `hypothesis`.
- Detection: for encoded streams, every single-bit flip, every truncation point, a duplicated range and a dropped
  frame is either detected or leaves the decoded output identical; nothing wrong is ever yielded. Use
  `cdc_v2.testing.pipefaults` with at least 1,000 seeds in an opt-in run and a smaller number in the default run.

Marker `jvm` (a local JVM, no Postgres):

- Java encoder against Python decoder and Python encoder against Java output, byte for byte, on generated data
  (a small Java test main that writes frames from a seed is fine).
- Crash-point contract in Java: reference values, the log line, each action.

Markers `jvm` and `localpg` (the private pair; decode from the standby):

- Types: every baseline family with its edge values, through a real table, checked value by value against what
  Postgres holds (as text under fixed session settings).
- Transactions: counts of `END` equal the rows seen; several tables in one transaction keep `seq` order; a
  transaction of at least 1 million rows here (the 10 million one is the T1 gate test of package S), with the
  JVM's peak resident memory recorded; empty transactions; rolled-back transactions; subtransactions with a
  rolled-back savepoint.
- Operations: insert, update, delete, truncate (one table, several tables, `CASCADE`, `RESTART IDENTITY`), key
  change chains and swaps, keyless table with duplicate rows, unchanged TOAST (single and several columns, with
  and without a key change, `REPLICA IDENTITY FULL` too), logical messages (transactional, non-transactional,
  rolled back), schema change between two transactions (add, drop, rename, type change: a new `SCHEMA` frame with
  the right content at the right place).
- The acknowledgement invariant, probed continuously during a workload: the slot's `confirmed_flush_lsn` is never
  beyond the last `DURABLE` position sent, and reaches it within a bounded time; with no `DURABLE` at all it never
  moves. Heartbeats move the position when the tables are idle and other WAL is written.
- Restart: kill the JVM at random moments (also at each Java crash point); after a restart the first transaction
  delivered is the one after the last acknowledged one, complete, and nothing is missing.
- Parent death: SIGKILL of the Python parent ends the JVM within a bounded time, and the slot becomes inactive.
- Frozen reader: no memory growth, no walsender timeout, then everything arrives.
- A broken source connection (use the harness TCP proxy between the JVM and the standby): black hole, reset,
  close. Record what Debezium does and how long it takes to end with `FATAL` or to recover; nothing may hang
  without a bound. State what pgjdbc allows for TCP keepalive settings.

### 4. Measurements (indications; this machine is shared)

Rows per second for the `lineitem`-shaped table of the spike (see `docs/spike/local_throughput.md` for the table
and the spike's numbers), for inserts in transactions of 1, 1,000 and 1,000,000 rows: the engine alone with a
reader that discards, against the spike program. Report the CPU time of the JVM per row and where it goes
(Debezium's streaming thread, our handler, garbage collection), and the effect of the batch, queue and
`record.processing.threads` settings. If our handler costs more than a small part of the JVM's CPU, make it
cheaper. Tuning beyond that is a later phase.

## Deliver

- The code and tests above; `docs/design/engine.md` (byte layout, value transport table, configuration with
  reasons, acknowledgement timing, limits, contract notes); evidence under `evidence/p3/engine/`.
- In your report: every **[test]** claim of the research that you proved or refuted, every Debezium behaviour that
  surprised you, and anything that the contract C2 should say differently.
