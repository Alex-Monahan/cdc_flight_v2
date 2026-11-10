# A JVM and stock Debezium on an F16 Flight (P2 spike, PLAN §2 A 1)

Measured on 2026-11-10 (UTC) with real **F16** Flight runs of `cdc_v2_spike_jvm` (bootstrap `flights/spike_jvm/main.py`,
driver `spikes/jvm_on_flight/driver.py`, deployed and run with `tools/flightctl`). Each fact is **measured** (evidence
file under `evidence/p2/jvm_on_flight/`, produced by `spikes/jvm_on_flight/collect.py` from the run record and its
log) or **documented** (URL). The Flight and everything else created on MotherDuck was deleted afterwards
(`evidence/p2/jvm_on_flight/cleanup.json`).

**Throughput caveat — read before quoting any rate below.** Every rate in this document is a *first indication*, not a
performance result: the source Postgres, its walsender, the JVM and the consumer all share the Flight's **2 vCPUs**.
The source is a throw-away Postgres inside the Flight container (a spike fixture, see §5); the product never installs
Postgres, and in production the source runs elsewhere, which frees the CPU the fixture uses here.

| Run | Phases | Evidence |
|---|---|---|
| 1 | JDK comparison, jar fetch, compile/start (its `e2e` failed on a fixture bug, fixed) | `run1_jdk.json` |
| 2 | end-to-end proof and start-up budget | `run2_e2e.json` |
| 3 | end-to-end again, throughput sweep (1M-row transaction), memory at heap caps | `run3_e2e_tput_memory.json` |
| 4 | supervision facts; poll-interval retest | `run4_supervise_profile.json` |
| 5 | small transactions (backlog and live), one 10M-row transaction | `run5_profile_small_big.json` |
| 6 | an orphaned child after the main process exits | `run6_orphan.json` |
| 7 | JFR CPU profile of the default configuration; poll-interval retest | `run7_profile.json` |
| 8 | Debezium statistics metrics off; combined best settings | `run8_metrics.json` |

Cost of all eight runs: ≈ 51 min of F16 ≈ $0.51 (`derived.cost_usd_at_0.60_per_h` in each file).

## 1. Recommendation: how JDK, Debezium and our program reach the Flight

1. **Bootstrap** (`flights/spike_jvm/main.py`, 3,412 bytes of the 204,800 allowed; requirements 23 bytes of 20,480):
   anonymous `git init` + `git fetch --depth 1 origin <commit>` + `checkout FETCH_HEAD` of
   `https://github.com/Alex-Monahan/cdc_flight_v2` at the full commit hash from the Flight config `CDC_V2_COMMIT`,
   verified with `git rev-parse HEAD`; then it runs a script from the clone in its own process group, with a hard
   deadline that kills the group. **0.34–0.39 s** for the clone (measured in every run, `boot.clone`). Fetching a
   bare commit hash from GitHub works (measured: every run).
2. **JDK: Eclipse Temurin 21 LTS tarball**, URL and SHA-256 locked in `jvm/jdk.lock.json`, fetched and verified by
   `cdc_v2.artifacts.install_jdk` before anything is unpacked: **≈ 4.1–4.6 s** per run (0.7–1.2 s download, 3.3 s
   unpack). Why: Debezium 3.7 needs Java ≥ 17 (§2), 21 is the version Debezium itself builds with; the tarball is
   4× faster than `apt-get` and pinned by checksum; Corretto 21 is equivalent (choose either); Temurin 25 is smaller
   (2.8 s unpack) and ran Debezium 3.7.1 fine in the selftest, but it is not the Java version Debezium is built and
   tested with — revisit when Debezium moves to 25.
3. **Debezium: the 73 runtime jars of 3.7.1.Final** (40.2 MB) from Maven Central, coordinates/URL/size/SHA-256 in
   `jvm/debezium.lock.json`, downloaded in parallel (16 threads) by `cdc_v2.artifacts.fetch`, which fails closed on
   any size or digest mismatch, and `verify_dir` refuses any jar not in the lock (so nothing can be added or shadowed
   on the classpath, SPEC R2). **0.55–0.64 s** per run. Nothing from Debezium is committed.
4. **Our program: `jvm/src/cdc_v2/engine/EngineMain.java`, compiled on the Flight with the JDK's `javac`**: **1.9–2.4 s**.
   Rejected: Java's source launcher (compiles in memory on *every* JVM start: 1.9–2.1 s vs 0.3 s per start with
   compiled classes, `run1_jdk.json` → `compile_and_start`; a supervisor that restarts the JVM would pay it each time);
   Maven/Gradle (not on the Flight, would add a download and a dependency resolution); committed class files or jars
   (binaries in the repository).
5. JDK and jar downloads can overlap: in parallel they took 4.57 s wall instead of ≈ 5.2 s sequentially
   (`run1_jdk.json` → `jdk_and_jars_parallel`); the JDK download could also start while the clone runs.

`jvm/pom.xml` is used only on the development machine by `jvm/make_lock.py` to resolve the closure into the lock
(it cross-checks every local jar against the SHA-1 that Maven Central publishes). Unit tests: `tests/test_artifacts.py`
(lock validation, parallel fetch, digest/size mismatch, same-length tampering, retries, reuse, extra/missing jars,
JDK install refuses a tampered tarball before unpacking) and `tests/test_spike_consumer.py` (frame protocol).

## 2. Versions

| Fact | Value | Kind | Evidence |
|---|---|---|---|
| Newest stable Debezium on Maven Central (2026-11-09) | **3.7.1.Final** (3.8.0.Alpha1 is the newest pre-release) | measured | [maven-metadata.xml](https://repo1.maven.org/maven2/io/debezium/debezium-embedded/maven-metadata.xml); `jvm/debezium.lock.json` |
| `docs/research/debezium.md` on the researcher's branch | did not exist on `origin` when this spike started; version chosen here | measured (`git fetch origin`) | — |
| Java needed by Debezium 3.7.1 | connectors and core are compiled for **Java 17** (`debezium.java.connector.target` = 17), built with Java 21 (`debezium.java.source` = 21); Kafka 4.3.1 Connect needs 17+ | documented | [debezium-build-parent 3.7.1.Final pom](https://repo1.maven.org/maven2/io/debezium/debezium-build-parent/3.7.1.Final/debezium-build-parent-3.7.1.Final.pom) |
| Runtime closure | 73 jars, 40,204,549 bytes: 13 Debezium artifacts, Kafka 4.3.1 (`connect-api`, `connect-runtime`, `connect-json`, `kafka-clients`, …), PostgreSQL JDBC 42.7.13, Jackson, Jetty/Jersey (pulled in by `connect-runtime`), slf4j-simple 1.7.36 as the log backend | measured | `jvm/debezium.lock.json` |
| Debezium version line at run time | `CDCV2 VERSION … debezium=3.7.1.Final` — read from the connector jar's `build.version` through `io.debezium.connector.postgresql.Module.version()`; Debezium's own log prints the connector configuration (`Starting PostgresConnectorTask with configuration:`) but no version line | measured | `run2_e2e.json`, `run3_…json` → `e2e.debezium_log` |
| Engine implementation | `DebeziumEngine.create(...)` resolves to the **async embedded engine** (`ConvertingAsyncEngineBuilderFactory` is the only `BuilderFactory` service in `debezium-embedded` 3.7.1) | measured (jar inspection) | — |

## 3. JDK at start-up

From an empty container every time (each run is a fresh container; `run1_jdk.json`):

| Way | Download | Unpack / install | Size | `java -version` |
|---|---|---|---|---|
| Temurin 21.0.13 tarball (chosen) | 0.70 s (207.9 MB) | 3.28 s | 361.8 MB | 0.05 s |
| Corretto 21.0.13 tarball | 0.91 s (209.2 MB) | 3.25 s | 364.7 MB | 0.05 s |
| Temurin 25.0.5 tarball | 0.85 s (141.3 MB) | 2.84 s | 316.6 MB | 0.04 s |
| Debian `apt-get install openjdk-17-jdk-headless` (the only JDK in bookworm) | `apt-get update` 2.98 s | 15.43 s | 271.3 MB | 0.04 s |

A `jlink`-reduced runtime does not pay off: producing it needs the full JDK on the Flight first (it would only add
time), and shipping a prebuilt one means committing or hosting a binary. Unpacking (single-threaded gzip) is the
largest cost; Temurin 25's smaller tarball saves ≈ 0.5 s.

**What the JVM detects** (measured, `run1_jdk.json` → `jvm_detects`, Temurin 21 and 25 alike): `Provider: cgroupv2`,
**Effective CPU Count: 2** (CPU quota 200000/100000 µs), **Memory Limit: 16.00G**, default `MaxHeapSize` 4 GiB (25 %),
G1, `ParallelGCThreads` 2, `CICompilerCount` 2. Unlike Python's `os.cpu_count()` (8) the JVM reads the cgroup
correctly. We still set `-XX:ActiveProcessorCount=2` and an explicit `-Xmx` so that sizing never depends on detection.

## 4. Our JVM program (candidate B prototype)

`EngineMain` configures the Debezium embedded engine for the PostgreSQL connector with `plugin.name=pgoutput`
(publication created by us, `publication.autocreate.mode=disabled`, `snapshot.mode=no_data`), and receives change
batches through `DebeziumEngine.ChangeConsumer.handleBatch(records, committer)`:

- **Hand-off formats**: `struct` — the engine hands us Kafka Connect `SourceRecord`/`Struct` objects
  (`ChangeEventFormat.of(Connect.class)`) and our code writes each row compactly (op, LSN, txId, ts, table, typed column
  values); `json` — Debezium's own JSON (`DebeziumEngine.create(Json.class)`), with or without embedded schemas.
- **Explicit acknowledgement**: a batch is written as one frame; with `cdc_v2.ack=sync` the program calls
  `committer.markProcessed(...)` for every record and `committer.markBatchFinished()` only after the parent has
  acknowledged that batch (`A <seq>` on stdin). The parent is the prototype applier (`spikes/jvm_on_flight/consumer.py`):
  it counts or decodes events, optionally appends them to a file sink, and acknowledges.
- **Frames travel on a dedicated pipe (fd 3), not on stdout** — see §8 item 6 for the measured reason.
- Parent death: end of stdin ⇒ the program closes the engine and halts within `cdc_v2.eof.grace.ms` (3 s).
- `SIGTERM` ⇒ shutdown hook ⇒ `engine.close()`.

## 5. End-to-end proof on the Flight (spike fixture)

**Fixture — spike only, the product never installs Postgres**: PostgreSQL **18.6** from the PostgreSQL apt repository
(PGDG, `bookworm-pgdg`), installed in the Flight container as root with `apt-get` in **14.0–16.8 s** (`apt-get update`
2.7 s, install 10.9–13.6 s), `initdb` 2.7–3.0 s, started as the unprivileged `postgres` user with
`wal_level=logical` on 127.0.0.1 (`fsync=off`, `shared_buffers=1GB`). A table of TPC-H `lineitem` shape (16 columns,
realistic generated values), a publication `cdc_v2_spike_jvm_pub` and a role `cdc_v2_spike_repl` with `REPLICATION`
(not a superuser) whose random password exists only in a mode-600 properties file (`run2_e2e.json` → `pg_fixture`).

**Hand-over shape**: the slot exists and holds a backlog before the engine starts — 10,000 inserts (one transaction),
99 updates and 53 deletes. Result (`run2_e2e.json`, repeated in `run3_…json`):

- events received **10,152 = rows written 10,152** (`ops` c 10,000 / u 99 / d 53), decoded by the consumer;
- sum of `l_orderkey` over all received events **12,640,988 = expected 12,640,988** (values round-trip);
- first decoded row: `[1, 7920, 4730, 2, "200:2", "90031:2", "1:2", "1:2", "N", "F", 8036, 8066, 8050,
  "COLLECT COD              ", "AIR       ", "c4ca4238a0b"]` (decimals as unscaled:scale, dates as epoch days, `char(n)`
  padded as in Postgres);
- the file sink received all 2,140,052 payload bytes; clean shutdown took 189 ms.

### Start-up budget of a hand-over (measured, from the platform's `started_at` to the first change event received)

| Step | Run 2 | Run 3 | Notes |
|---|---|---|---|
| container + requirements (`started_at` → bootstrap first line) | 2.47 s | 2.45 s | uv installs `psycopg[binary]` (spike only); with `duckdb` ≈ 3 s (`docs/spike/flight_platform.md`) |
| clone at the pinned commit | 0.35 s | 0.36 s | |
| driver start (Python start, imports) | 1.31 s | 1.28 s | spike driver; includes importing psycopg |
| JDK (download + verify + unpack) | 4.22 s | 4.44 s | Temurin 21 |
| Debezium jars (73, parallel, verified) | 0.65 s | 0.61 s | |
| compile our program (`javac`) | 2.40 s | 1.98 s | |
| *(Postgres fixture — excluded)* | *17.3 s* | *19.9 s* | not part of the product |
| JVM start (spawn → `main`) | 0.12 s | 0.13 s | |
| connector start (`main` → engine polling) | 1.51 s | 1.72 s | engine built in 0.36–0.39 s |
| first event (polling → first batch received) | 1.61 s | 1.59 s | |
| **total to first event** | **14.6 s** | **14.6 s** | excluding the fixture |

All backlog events (10,152) had arrived 6.0 s after the JVM was spawned. Obvious savings for the product: overlap
JDK + jars with each other and with the clone (≈ −1 s), Temurin 25's smaller tarball (≈ −0.5 s); the hand-over then
costs ≈ 12–13 s plus the leader guard.

## 6. Memory

Resident memory of the JVM (`VmRSS`/`VmHWM`, `run3_…json` → `phase_memory`), default queue settings
(`max.batch.size` 2048, `max.queue.size` 8192, `max.queue.size.in.bytes` 0 = no byte bound, `poll.interval.ms` 500),
G1, one 1M-row transaction streamed:

| Heap cap (`-Xmx`) | RSS idle (streaming, no load) | RSS peak under load | Rate (indication) |
|---|---|---|---|
| 256 MB | 154 MB | 304 MB | 24,217 rows/s |
| 512 MB | 153 MB | 326 MB | 23,677 rows/s |
| 1 GB | 153 MB | 412 MB | 23,045 rows/s |
| 2 GB | 161 MB | 487 MB | 23,717 rows/s |
| 256 MB with `max.batch.size` 20480 / `max.queue.size` 81920 | 153 MB | 391 MB | 19,976 rows/s (GC pressure) |
| 1 GB, ParallelGC, 8192/32768, **10M-row transaction** (run 5) | 181 MB | **803 MB** | 29,829 rows/s |

The consumer process stayed at 14–20 MB. The heap cap does not move throughput (the 256 MB cap is as fast as 2 GB with
default queues); a larger queue needs a larger heap. With `-Xmx1g` the JVM stays below ≈ 0.85 GB resident even for a
10M-row transaction, so of the 16 GiB cgroup limit **≈ 15 GiB remain for the Python applier and DuckDB** (SPEC 5.4's
70 % ceiling is 11.2 GiB in total). Recommendation for the build: `-Xmx1g` (or 512 MB), `-XX:ActiveProcessorCount=2`,
and a byte bound on Debezium's queue (`max.queue.size.in.bytes`) so that wide rows and TOAST cannot grow it unbounded.

## 7. First throughput indication on F16 (2 vCPUs shared by source, JVM and consumer)

One transaction of 1M `lineitem` rows inserted (11.2–11.9 s inside the fixture Postgres), then streamed into the
consumer; *rows/s after commit* = 1M ÷ (last event received − commit). CPU = CPU-seconds ÷ wall time over the same
window (200 % = both vCPUs). Runs 3, 4, 7 and 8.

| Configuration | rows/s after commit | JVM CPU | walsender CPU | consumer CPU | evidence |
|---|---|---|---|---|---|
| default: `struct` hand-off, batch 2048 / queue 8192 / poll 500 ms, sync ack, G1, 1 GB | **23,767** | 149 % | 25 % | 1 % | run 3 |
| Debezium JSON, no schemas | 21,978 | 166 % | 23 % | 3 % | run 3 |
| Debezium JSON with schemas (Debezium's default) | 12,885 | 172 % | 14 % | 9 % | run 3 |
| batch 8192 / queue 32768 | 22,952 | 146 % | 24 % | 1 % | run 3 |
| batch 20480 / queue 81920 | 23,257 | 149 % | 24 % | 1 % | run 3 |
| batch 8192 + `poll.interval.ms` 10 | 23,988 | 148 % | 25 % | 1 % | run 4 |
| batch 8192, no acknowledgement wait | 23,683 | 146 % | 24 % | 1 % | run 3 |
| batch 8192 + ParallelGC | 25,363 | 147 % | 26 % | 1 % | run 3 |
| `decimal.handling.mode` string / double | 23,297 / 24,421 | 145 / 142 % | 24 % | 1 % | run 4 |
| ParallelGC + decimal double | 25,918 | 141 % | 27 % | 1 % | run 4 |
| `statistics.metrics.enabled=false` | 23,520 | 145 % | 24 % | 1 % | run 8 |
| combined: metrics off, decimal double, batch 8192, ParallelGC | 25,896 | 145 % | 25 % | 1 % | run 8 |
| **one 10M-row transaction**, ParallelGC, batch 8192 | **29,829** (335 s after a 115 s insert) | 144 % | 28 % | 1 % | run 5 |

Many small transactions (run 5, ParallelGC, batch 8192):

- **backlog**: 100,000 one-row transactions written by `pgbench` (8,334 tps) while no engine ran, then caught up at
  **10,150 transactions/s** (JVM 162 %, Postgres 20 %): per-transaction overhead roughly halves the rate;
- **live**: `pgbench` at full speed for 30 s alongside the engine reached 4,002 tps; the engine kept up
  (3,894 events/s during the load, drained 0.94 s after it ended; JVM 78 %, walsender 11 %).

**What moved the number**: the hand-off format (Debezium JSON with schemas costs 46 %; our compact `struct` encoding is
8 % faster than schemaless JSON and transfers 4.2× fewer bytes: 212 MB vs 883 MB vs 6.0 GB per 1M rows); ParallelGC
(+7 %); decimal as double (+3 %). **What did not**: batch and queue sizes, poll interval, the acknowledgement round
trip, the heap cap.

### 7.1 Where the CPU goes — the bottleneck

Per-thread CPU of the JVM (from `/proc/<pid>/task/*/stat`, `jvm_threads_cpu_s_commit_to_last`) for 1M rows, default
settings: Debezium's **streaming thread** (`debezium-postgresconnector-…-change-event-source-coordinator`) **36–42
CPU-seconds**, JIT compiler 7–9 s, Debezium's metrics publisher (`MeasurementCollector`, thread `Thread-N`) 9–10 s,
the engine's poll thread plus our serialisation 4–7 s, GC 1–2 s; the walsender used 10.3 s, the consumer 0.4 s. For
10M rows: streaming thread 296 s, metrics 83 s, poll+serialisation 43 s, GC 42 s, walsender 95 s.

So the limit is **one single-threaded Debezium streaming thread at ≈ 30–40 µs of CPU per row**, which caps stock
Debezium at **≈ 25–34K rows/s per connector regardless of the CPUs available**; the whole JVM spends ≈ 55–62 µs of
CPU per row. The JFR profile (run 7, `jfr.inclusive_pct`) shows inside the streaming thread: `PgOutputMessageDecoder`
→ `PostgresChangeRecordEmitter`: building the per-event source-info `Struct` (≈ 32 % of the thread), resolving and
converting column values from pgoutput's text format (≈ 33 %, of which ISO date parsing alone ≈ 17 %), Kafka
Connect's `Struct.put` validation (`ConnectSchema.validateValue`, ≈ 28 %; these shares overlap: validation happens
inside the first two). None of these is reachable by configuration
without patching Debezium (R2). (JFR's sampling over-represents our serialiser thread; thread CPU from `/proc` is the
authoritative split.)

`statistics.metrics.enabled=false` (run 8) did not remove the metrics publisher thread (still 8.9 s per 1M rows) and
did not change the rate (23,520 rows/s); which setting, if any, turns `MeasurementCollector` off is open.

**Consequence for SPEC R5/5.1**: 100K rows/s at `lineitem` width needs ≤ 20 µs of CPU per row for *everything* on
2 vCPUs (decode, hand-off, apply, DuckDB). Stock Debezium's streaming thread alone needs 30–40 µs, so a single
connector cannot reach the target even with the source elsewhere and the applier free; the measured best is ≈ 30K
rows/s. This is the starting point of the SPEC 5.1 tuning work (D3: stay on F16, keep improving, report the tuning
log). Levers worth measuring next: the source off the Flight (frees ≈ 25–35 % of the CPU), finding the switch for the
metrics publisher, ParallelGC,
JIT warm-up (C2 used 7–9 s per 1M rows), and — the only structural lever — several connectors on disjoint table sets
in parallel, which costs cross-slot transaction assembly for SPEC 1.3 and is still bounded by 2 vCPUs.

## 8. Supervision facts (run 4 unless noted)

| Situation | What happens | Evidence |
|---|---|---|
| `SIGTERM` to the JVM | shutdown hook closes the engine in 525 ms; JVM gone after **0.88 s**; slot `active=false` 0.89 s after the signal | `supervise_sigterm` |
| `SIGKILL` of the JVM | the walsender notices at once: slot **`active=false` 0.04 s** after the kill, no walsender left (`pg_stat_replication` empty), `confirmed_flush_lsn` = the last committed offset; the consumer sees EOF; a new engine on the same slot and offsets delivered a new change **2.66 s** after restart | `supervise_sigkill` |
| the JVM's parent (the applier) is `SIGKILL`ed | the JVM sees EOF on stdin, closes the engine (88 ms) and halts after **0.12 s**; slot free after 0.13 s | `supervise_parent_killed` |
| same, without our stdin watch | the JVM is **orphaned to PID 1 (`tower-runtime-e`, the platform runtime) and keeps the slot active**; it died only when it next wrote a batch to the closed pipe; after a later `SIGKILL` it stayed a **zombie** (PID 1 did not reap it within 10 s) | `supervise_orphan_without_watch` |
| JVM frozen (`SIGSTOP`) | the slot stays active until the walsender's `wal_sender_timeout` (60 s): **59.6 s** | `supervise_sigstop` |
| the Flight's main process exits while a detached child still runs (run 6) | the child keeps running and logging for ≈ 5 s, then the run ends (`ended_at` = main exit + **5.1 s**) and the child is killed with the container: **orphans do not keep a run alive** | `run6_orphan.json` |

Process tree inside a run: PID 1 `tower-runtime-e` → `uv` → `python main.py` (the bootstrap) → our children
(`process_tree`). Locally a killed process's socket is closed by the kernel at once; with a remote source the same
holds when only the process dies (the kernel sends FIN), while a dead host or network path is detected only by
`wal_sender_timeout` and TCP keepalives (SPEC 4.6).

Design consequences:

1. The JVM must watch its stdin (or a dedicated control pipe) and exit on EOF; the parent must not rely on Linux to
   kill children (there is no `PR_SET_PDEATHSIG` from Java).
2. Reap children: PID 1 of the container does not reap promptly, so the supervisor `wait()`s for its children.
3. After a hard kill of the JVM the slot is free within 0.1 s locally, so a restart within the same run is cheap
   (≈ 2.7 s to the next event without recompiling).
4. A frozen JVM holds the slot for `wal_sender_timeout`; the supervisor's progress deadline (SPEC 4.5) must kill it
   rather than wait.
5. The run ends ≈ 5 s after the bootstrap exits even if children live: the bootstrap's hard exit (SPEC 4.2) really
   ends the run.
6. **The JVM writes to its own stdout**: unified logging (`-Xlog`, warnings by default) and JFR start-up messages
   (`[0.8s][info][jfr,startup] …`) go to fd 1. With frames on stdout this corrupted the stream (measured: consumer
   `bad frame magic 5b302e38` = `"[0.8"`, run 5 log), even with `-Xlog:disable -Xlog:all=warning:stderr`. Frames
   therefore go to a dedicated pipe passed as fd 3 (`-Dcdc_v2.frames=/dev/fd/N`), and `System.out` is redirected to
   stderr; run 7 profiled with JFR on that design without corruption.

## 9. Verdict on candidate B, surprises, open points

**Candidate B is confirmed as workable**: a stock Debezium 3.7.1 embedded engine in our own small JVM program, fetched
and compiled at start-up in ≈ 7 s, streams real `pgoutput` changes into a Python parent with an explicit
acknowledge-then-commit step, can be health-checked, memory-capped (≈ 0.3–0.8 GB resident), killed and restarted
in seconds, and frees the slot within 0.1 s of a kill. Nothing measured speaks for A (Debezium Server would add a
second hand-off and HTTP/file sink) or C (a JVM bridge in the Python process removes the kill/restart isolation SPEC
4.5 needs).

Surprises and contradictions with SPEC/PLAN:

1. **Stock Debezium streaming is capped by its single streaming thread at ≈ 25–34K rows/s** on this hardware
   (§7.1) — about a third of SPEC R5's 100K rows/s, before any apply cost. SPEC D3 anticipated a shortfall; the size
   of it is the news.
2. Debezium's metrics publisher thread (`MeasurementCollector`) costs ≈ 9–10 µs of CPU per row, and
   `statistics.metrics.enabled=false` does not remove it.
3. The JVM writes to stdout by itself (§8 item 6): stdout cannot carry a binary protocol.
4. Orphans are reparented to the platform runtime, which does not reap them promptly (zombies).
5. A run ends ≈ 5 s (not 0.45 s) after its main process exits when another process still holds the log pipe.

Open points:

- Throughput with the source off the Flight (stage 2, PlanetScale) — the fixture takes 25–35 % of the CPU here.
- Whether trimming the 73-jar closure (Jetty/Jersey come from `connect-runtime` and are unused by the embedded
  engine) is safe; it would shorten the fetch slightly but risks class-loading failures on rare paths. Not done.
- Debezium's `max.queue.size.in.bytes` with TOAST-sized values (SPEC 5.4) — not measured here.
- An offset store in MotherDuck (our own `OffsetBackingStore`, configuration not patching) replaces the spike's file
  store; the spike used `FileOffsetBackingStore` with a 1 s flush interval.
