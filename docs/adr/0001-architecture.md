# ADR 0001 — Architecture of the pipeline

Status: accepted by the orchestrator on 2026-11-10, at the P2 gate. The owner may overrule any point.
Basis: the spike reports under `docs/spike/` (summary in `docs/spike_report.md`). Detailed contracts:
`docs/design/core.md`.

## Context

PLAN §2 names three candidates for running stock Debezium next to a Python pipeline on a MotherDuck Flight:

- **A** — Debezium Server as a child process, delivering to a local sink that the applier consumes.
- **B** — the Debezium embedded engine inside a small JVM program of ours, as a child process, handing batches to
  the applier and acknowledging only after the destination commit.
- **C** — the embedded engine loaded into the Python process through a JVM bridge.

A Flight is one container with 2 CPUs and 16 GB. No JVM and no Python package is preinstalled; every run starts
from an empty disk; every stop arrives without a signal; an out-of-memory kill of any child ends the whole run.

## Decision 1 — Candidate B

Stock Debezium **3.7.1.Final**, embedded engine, in our own JVM program (`jvm/src/cdc_v2/engine/EngineMain.java`),
run as a child of the Python pipeline.

| Point | Decision | Why (evidence) |
|---|---|---|
| Java | Eclipse Temurin 21, tarball locked by URL and SHA-256 in `jvm/jdk.lock.json`, fetched and verified at every start | Debezium 3.7 is built with 21 and needs ≥ 17; 4.1–4.6 s per start; 4 times faster than `apt-get` (`jvm_on_flight.md` §1, §3) |
| Debezium | 73 jars (40.2 MB) from Maven Central, locked by size and SHA-256 in `jvm/debezium.lock.json`; a jar that is not in the lock is refused | R2: stock, unmodified, verifiable; 0.6 s per start |
| Our program | compiled on the Flight with `javac` at start (1.9–2.4 s); no binaries in the repository | the source launcher would compile at every JVM restart |
| Records | length-prefixed frames on a pipe of their own (fd 3); stdout and stderr stay logs | a log line can never be parsed as data |
| Offsets | memory offset store; the start position comes from the applied position in MotherDuck | the destination is the only durable position (SPEC 1.1) |
| Acknowledge | the engine marks a batch processed only when the pipeline reports a destination commit at or beyond it | the slot never moves past the applied position (SPEC 1.1, 1.8) |
| Supervision | the JVM watches its parent and exits when the parent is gone; the pipeline kills the JVM's process group | measured: an orphaned JVM keeps the slot; a frozen JVM holds it for 60 s |
| Memory | heap capped explicitly; all limits are sized from the cgroup, not from `os.cpu_count()` or total memory | the container reports 8 CPUs and 64 GB but has 2 CPUs and 16 GB |

Rejected: **A** adds a second configuration surface and a sink protocol between two processes, and gives no
acknowledge step tied to the destination commit. **C** puts the JVM heap and the Python process in one address
space: a JVM fault or a hang cannot be contained, killed or restarted on its own, which SPEC 4.5 and 4.7 need.

Known price of B: start-up of a hand-over is 14.6 s from the platform's start to the first change event, and
Debezium's single streaming thread limits the stream to about 23–30K rows/s at `lineitem` width on a Flight with
the source on the same CPUs (see `docs/spike_report.md` §3; tuning is P9, SPEC D3).

## Decision 2 — One leader, enforced inside every destination transaction

- The Flight is scheduled every minute. Every run starts with a **guard** that reads the leader record and exits at
  once when the leader is healthy. Overlapping starts are never skipped by the platform, so a dead or frozen leader
  is replaced by the next minute's run.
- Leader, fence and heartbeat are **separate rows**. The leader row is only ever updated. The fence statement is the
  last statement before `COMMIT` in every transaction that writes data and must change exactly one row.
- Heartbeat age is computed on one clock, MotherDuck's (`uuid_extract_timestamp(uuidv7())`); `now()` is folded by
  the client.
- A take-over is one transaction that raises the epoch in all three. If it conflicts with an open transaction of
  the old leader, the newcomer interrupts the server connections stored in the leader record. After winning it
  cancels exactly the run number stored in the record, never "all other runs". A cancelled run can still commit
  for about 2.5 s; the fence, not the cancel, is what makes that harmless.
- The bootstrap only watches the pipeline. No tick for 60 s: it kills the pipeline's process group and exits,
  which ends the run.

Why: `run_semantics.md` findings 1–8, `md_semantics.md` findings 1, 3, 4, 7.

## Decision 3 — Apply

- One destination commit applies a **group** of whole source transactions (about 1 s or a byte limit). The position
  moves in the same transaction as the data.
- Destination tables carry no keys and no indexes; apply is `DELETE … USING` + `INSERT` per table from a net change
  set. A table without a key is applied by counted deletes and inserts.
- A group is applied **directly** from local Arrow data. Staging tables in MotherDuck are used only for a source
  transaction that is larger than a group; its rows are uploaded while they arrive.
- Arrow data is handed to DuckDB in bounded batches.

Why: `md_semantics.md` findings 2, 5, 6, 10 and the Flight numbers (0.066 s for a small group; 212K rows/s at
100K-row groups, 342–355K rows/s at 1M-row groups on one connection).

## Decision 4 — Backfill

- Debezium takes no snapshot (R3). The pipeline reads tables itself: parallel `COPY` over `ctid` ranges from the
  replica, parsed into Arrow, two connections on an F16, **every chunk in its own short transaction**.
- A table with a key: every chunk has its own snapshot; changes that arrive meanwhile go to a pending log and are
  replayed onto the new table; the new table replaces the old one in one transaction, together with the position.
- A table without a key: one snapshot for the whole table, held by a connection that holds no lock on the table.
- Where a snapshot lies in the stream (the **cut**) is defined by replay positions on a standby and by transaction
  ids on a primary (`docs/design/core.md` 7.2); it is proven by a stress test before the backfill depends on it.
- DuckDB's Postgres reader is kept as a reference for cost, not as the mechanism: it holds every connection in one
  transaction for the whole scan, reports no progress per chunk, and silently uses separate snapshots when the
  export fails.

Why: `local_throughput.md` §2, §4, §6, §8 (a long read transaction on a standby is cancelled after 30 s by any
exclusive lock on the primary and stalls replay, and with it the stream, until then).

## Decision 5 — MotherDuck account

The pipeline runs under an account that nothing else uses (SPEC already expects a service account). All sessions of
one MotherDuck user share one server instance; a heavy query from another session made a Flight's server-side work
4–5 times slower (`md_semantics.md` finding 9). Until the owner provides that account, the build uses the owner's
own account (PLAN §9), and performance numbers are valid only when that account is otherwise idle.

## Consequences

- Correctness never depends on a shutdown handler: every stop is a crash, and a restart reads its position from
  MotherDuck.
- The stream, not the destination, limits throughput for large transactions (SPEC 5.1). The apply side has a margin
  of 3 or more over the 100K rows/s target.
- A hand-over costs about 15 s of start-up; the planned hand-over before the maximum run time is a leader decision,
  not the platform's.
- The every-minute guard costs $0.73–$1.46 a day at the measured run lengths, if billing is per second (not
  documented).
