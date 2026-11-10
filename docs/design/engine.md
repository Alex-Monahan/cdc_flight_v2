# The engine (package E)

The part of the pipeline between the replication slot and the first Arrow batch in Python: our JVM program around
the stock Debezium 3.7.1 embedded engine, the frame protocol on its pipe, and the Python side that starts the JVM,
reads frames and sends commands. Contract: `docs/design/p3_plan.md` C0, C1, C2. Evidence: `evidence/p3/engine/`.

| File | What |
|---|---|
| `jvm/src/cdc_v2/engine/CdcEngine.java` | main class: configuration, consumer, acknowledgement, stop, FATAL, parent watch, test modes |
| `jvm/src/cdc_v2/engine/TransportConverter.java` | our Debezium `CustomConverter`: raw value → transport form; unchanged-TOAST markers by reference |
| `jvm/src/cdc_v2/engine/RowsBuffer.java`, `FrameOut.java`, `ByteBuf.java` | ROWS columns in Arrow layout; frame writer |
| `jvm/src/cdc_v2/engine/SlotProbe.java` | one JDBC query per engine life: which backend holds the slot (for HELLO) |
| `jvm/src/cdc_v2/engine/Crash.java` | crash points, Java side of `docs/testing/harness.md` §1 |
| `jvm/src/cdc_v2/engine/FrameGen.java`, `ReplProbe.java`, `Json.java` | frame generator shared with Python; replication test tool; CONFIG parser |
| `src/cdc_v2/stream/frames.py` | decoder: frame objects, `FrameParser` (incremental), `FrameReader` (fd + deadline), `EngineBroken` |
| `src/cdc_v2/testing/frames.py` | encoder producing the Java bytes, generator shared with Java, `rows()` helper |
| `src/cdc_v2/stream/engine.py` | `Engine`, `EngineSettings`, `build_properties`, `EngineFatal`, `EngineExited`, `JAVA_CRASH_POINTS` |
| `src/cdc_v2/stream/jvm.py` | `prepare(cache_dir)`: JDK, locked jars, our classes compiled with `javac`, keyed by a source hash |
| `tests/engine/` | hermetic, `jvm`, and `jvm`+`localpg` suites (end of this document) |

The spike program `EngineMain.java` is unchanged and still compiles next to the new classes.

## 1. Processes and pipes

The pipeline starts the JVM as a child **in its own process group** (no new session), through
`/bin/sh -c 'exec 3>&N N>&- && exec "$@"'`, so the frame pipe arrives as file descriptor 3 and no shell stays
behind. Standard input carries commands; standard output and standard error are logs (`System.out` is redirected to
standard error; Python drains both on their own threads into the logger `cdc_v2.engine.jvm`, with the password
replaced). The child's environment is minimal: `PATH`, `LANG`, `LC_ALL`, `TZ` and the `CDC_V2_CRASH_*` variables;
the password reaches the JVM only in the CONFIG line.

Commands, one per line on standard input:

| Command | Effect |
|---|---|
| `CONFIG <json>` | first line, once: Debezium properties and `cdc_v2.*` settings, all strings |
| `DURABLE <p>` | p (decimal) is durable in the destination; see section 5 |
| `STOP` | close the engine and exit 0 within `cdc_v2.stop.timeout.ms` (15 s); else halt with 3 |
| end of input | the parent is gone: halt at once with 4, acknowledge nothing |

A dedicated thread reads standard input at all times, also while the engine thread is blocked writing to file
descriptor 3; a second thread polls the parent process every 500 ms and halts with 4 when it is gone. In Python,
`Engine.durable(p)` only records the newest p under a lock; a writer thread sends it, so a full pipe never blocks the
caller (test: `tests/engine/test_engine_unit.py::test_durable_never_blocks_while_fd3_is_full`: 20,000 calls in
under 2 s while nobody reads fd 3; the fake engine read every command meanwhile).

Exit codes of the JVM: 0 STOP and clean close; 1 after a FATAL frame; 2 bad usage, configuration or crash
environment; 3 STOP but Debezium did not close in time (halted); 4 parent gone; 5 frame pipe closed.

## 2. Byte layout

Every integer is little-endian.

```
frame    = magic "CDCE" | type u8 | 3 zero bytes | seq u64 | length u32 | payload (length bytes) | crc32 u32
```

The CRC-32 is zlib's (`java.util.zip.CRC32`) over every byte before it. Frame `seq` starts at 0 and has no gaps.
Header 20 bytes. The decoder refuses a wrong magic, non-zero reserved bytes, an unknown type, a gap, a length
above its maximum (default 1 GiB + 16 MiB), a bad checksum, a payload that is too short or has unused bytes,
non-zero padding, invalid UTF-8 in a string field, and column buffers that do not validate in Arrow
(`Array.validate(full=True)`). After the first error the decoder stays broken.

Payload primitives: `u8 u16 u32 u64 i32 i64`; `str` = u32 byte length + UTF-8; `bin` = u32 length + bytes;
`opt_u64` = u8 0 (absent) or u8 1 + u64; `flag` = u8 0 or 1; `posmap` = u16 n, then n × (`str` key, u8 kind,
value: kind 0 none, 1 i64, 2 `str`, 3 `flag`), keys sorted (Java `TreeMap<String>` order).

| Type | Code | Payload |
|---|---|---|
| HELLO | 1 | u32 protocol version (1), str Debezium version, str slot, opt_u64 start position, u32 JVM pid, str engine id, opt_u64 `active_pid` of the slot, opt_u64 that backend's start (µs since 1970, UTC), str that backend's `application_name` |
| SCHEMA | 2 | str schema, str table, u32 version, flag has_key, u16 n + n × u16 key positions (key order), u16 ncols, per column: u16 position, str name, str Connect type, str Connect schema name, str transport form, u8 physical type, str source type, i32 length (−1 none), i32 scale (−1 none), flag nullable, flag key, flag unchanged-possible |
| ROWS | 3 | str schema, str table, u32 version, u32 rows, u16 ncols, per column: str name, u8 physical type, u32 null count, validity buffer, offsets buffer (text and bytes only), data buffer |
| BEGIN | 4 | u64 seq, str transaction id, i64 commit time (ms), posmap, opt_u64 ack_position |
| END | 5 | as BEGIN, then u64 event count, u32 n, n × (str data collection, u64 count) |
| TRUNCATE | 6 | u64 seq, str schema, str table, posmap |
| MESSAGE | 7 | u64 seq, str prefix, flag transactional, bin content, posmap |
| HEARTBEAT | 8 | u64 seq, posmap, opt_u64 ack_position |
| BATCH_END | 9 | u32 records in the engine batch, opt_u64 newest ack_position handed to Debezium so far |
| FATAL | 10 | str class, str message, u16 n, n × (str class, str message) of the cause chain |

A column **buffer** is u32 byte length, zero bytes up to a multiple of 8 counted from the payload start, the bytes,
zero bytes up to the next multiple of 8. So every buffer starts 8-aligned inside the payload; Python reads each
payload into its own `bytearray` (malloc-aligned) and wraps the buffers with `pyarrow.Array.from_buffers` without
copying (test: `test_rows_buffers_are_zero_copy_slices_of_the_payload`). Arrow layout per physical type:

| Code | Physical | Arrow | Data buffer |
|---|---|---|---|
| 1 | boolean | `bool_` | bitmap, ⌈n/8⌉ bytes, LSB first |
| 2, 3, 4 | 16/32/64-bit integer | `int16/32/64` | n × width |
| 5, 6 | 32/64-bit float | `float32/64` | n × width, raw IEEE bits (NaN payloads, −0 kept) |
| 7, 8 | text, bytes | `string`, `binary` | offsets: (n+1) × i32 starting at 0, then the data |

The validity buffer is empty when the column has no NULL, else ⌈n/8⌉ bytes. NULL slots hold zero bytes, a cleared
bit or an empty value, and bits beyond n are zero, so the Java and Python encoders agree byte for byte.

**Column sets** of a ROWS frame, named by the column position `i` in the schema version: `seq` (int64), `op`
(text `c`, `u`, `d`), `a<i>` for every column, `b<i>` for every column of a table without a key, `u<i>` (boolean)
for every column whose SCHEMA flag "unchanged possible" is set, `k<i>` for every key column of a table with a key.
`a<i>`: new image; for a delete of a table with a key only the key columns, the rest NULL; for a delete of a table
without a key all NULL (the old image is in `b<i>`). `k<i>`: the old key on the create record that follows a key
change, NULL otherwise.

**Order.** One record sequence number `seq` per engine life, from 0, over every record that produces a frame or a
row: BEGIN, END, HEARTBEAT, rows, TRUNCATE, MESSAGE (contract C2 names it for rows; giving it to every record costs
nothing and orders everything). Only one ROWS frame is open at a time, and it is flushed before any other frame,
when the table or schema version changes, at `cdc_v2.frame.max.rows` (65,536) or `cdc_v2.frame.target.bytes`
(8 MiB), and at the end of each engine batch. So frames are in `seq` order, and a ROWS frame never holds more rows
than one Debezium batch (`max.batch.size`, 8,192 by default): measured `max_rows_per_frame` = 8,192 for a 1M-row
transaction (`evidence/p3/engine/large_transaction.json`). A row that would push a frame beyond the target goes
into a frame of its own (the row is rolled back and written again after a flush).

## 3. Value transport

Our `TransportConverter` claims **every** column (a documented Debezium extension point, SPEC R2). It receives the
raw value that Debezium's pgoutput decoder produced (`ReplicationMessageColumnValueResolver`, read in the 3.7.1
source) and turns it into one of the forms below. Every schema it registers is optional, so a missing value is
NULL, never a fallback that Debezium would invent for a NOT NULL column (for example the non-key columns of a delete
under default replica identity). Domains arrive as their base type (Debezium resolves the root type's OID).
The SCHEMA frame names the form; package Y converts by form.

| Postgres type | Raw value from Debezium | Form | Physical | Transport value | Tested |
|---|---|---|---|---|---|
| boolean | Boolean | `bool` | bool | as is | yes |
| smallint | Integer | `int16` | int16 | as is | yes (min, max, 0) |
| integer | Integer | `int32` | int32 | as is | yes |
| bigint, oid | Long | `int64` | int64 | as is (oid 0 … 4294967295) | yes |
| real | Float (`Float.valueOf` of the text) | `float32` | float32 | raw bits | yes, bit for bit: NaN, ±Infinity, −0, subnormal, max |
| double precision | Double | `float64` | float64 | raw bits | yes, bit for bit (same edges) |
| numeric (any precision) | `SpecialValueDecimal` | `numeric_text` | text | `BigDecimal.toPlainString()` (scale kept), `NaN`, `Infinity`, `-Infinity` | yes: equals Postgres text for 1.50, NaN, ±Infinity, 1e-36, 45+ digits, −0.0, 1e300, numeric(15,2) |
| money | BigDecimal (`PostgresMoney.parse`, needs `lc_monetary=C`) | `money_text` | text | plain decimal | yes: equals `money::numeric::text` incl. the minimum value |
| text, varchar, char(n), name, "char", citext, json, jsonb, xml, uuid, inet, cidr, macaddr*, ranges, ltree, tsvector, enums, unknown types | String (Postgres text output) | `text` | text | as is (char(n) keeps its padding) | yes for text (incl. 100,000 chars, tab, quote, backslash, non-BMP), varchar, char(n), "char", name, uuid, json (huge numbers kept), jsonb, inet, int4range; not tested: citext, xml, macaddr, ltree, tsvector, enums, composite, multirange |
| bit(n), varbit | String | `text` | text | `0`/`1` string | yes: B'101' and B'0101' differ, B'' kept (research §7: the default conversion loses this) |
| bytea | byte[] | `bytes` | bytes | as is | yes: empty, `\x00`, 100,000 bytes |
| arrays (any `_type`) | `PgArray` built from the text | `array_text` | text | `PgArray.toString()` = Postgres text | yes: NULL elements, `[0:2]=…` lower bounds, 2-D, empty, quoted text elements |
| date | LocalDate | `date_pg_days` | int32 | days since 2000-01-01; +infinity = 2147483647, −infinity = −2147483648 (Postgres' own representation) | yes: ±infinity, 4713-11-24 BC, 0001-12-31 BC, 0001-01-01, 1999-12-31, 10000-01-01, 5874897-12-31 |
| timestamp | Instant (UTC) | `timestamp_pg_us` | int64 | µs since 2000-01-01; ±infinity = int64 max / min | yes: ±infinity, 4713-11-24 BC, 0044 BC, 294276-12-31 23:59:59.999999, 12345-06-07 |
| timestamptz | OffsetDateTime (UTC) | `timestamptz_pg_us` | int64 | µs since 2000-01-01 00:00 UTC; ±infinity as above | yes: same edges, offsets −08 and +05:45 |
| time | String (raw text) | `time_us` | int64 | µs since midnight, 0 … 86,400,000,000 (24:00:00) | yes: 00:00, 24:00:00, 23:59:59.999999 |
| timetz | String (raw text) | `text` | text | Postgres text | yes: 24:00:00+14, −15:59 |
| interval | `PGInterval` | `interval_text` | text | `M months D days U microseconds` (DuckDB casts this text exactly) | yes: mixed signs, −178956970 years −8 mons, 1 µs |
| point, box, … (geometric) | `PGpoint` etc. | `text` | text | `PGobject.getValue()` (Java float formatting) | no |

Why µs since 2000 and not since 1970: every finite Postgres timestamp fits in int64 relative to 2000 (that is how
Postgres stores it), but not relative to 1970 (the top ~10,000 years overflow); the same for dates in int32.

Limits found in the Debezium path (in the raw value, before our converter, so not curable by configuration):
an interval whose hour part exceeds 2^31 (`'2562047788 hours'`) and the infinite intervals of Postgres 17+ fail in
pgjdbc's `PGInterval` parser and stop the engine with FATAL (not tested here; P4 type wave); geometric values pass
through pgjdbc objects and lose Postgres' float formatting.

**Unchanged (H-003).** Debezium hands a TOAST column that pgoutput sent as `u` to the converter as one of the marker
objects of `UnchangedToastedReplicationMessageColumn` (compared by reference). The converter maps it to our own
marker objects (`UNCHANGED_TEXT`, a `String` instance, and `UNCHANGED_BYTES`), and the consumer compares by
reference only, sets `u<i>` and leaves `a<i>` NULL. No code compares a value with a placeholder; real values equal
to the placeholder bytes (`bytea`), its printable part or Debezium's default placeholder (`text`), or an array
element equal to it, arrive as values with `u<i>` false (`test_unchanged_toast_h003`). `unavailable.value.placeholder`
is still set to the NUL-framed value of research §8.4 as a second line. "Unchanged possible" is set for every
column whose form is text or bytes and whose type is not of fixed length (a conservative superset: enums and
unknown types are included; their `u<i>` is simply always false). A marker in a column without `u<i>`, or in an old
image, stops the engine with FATAL.

## 4. Configuration

The Python side builds the properties (`build_properties`); Java passes every key not starting with `cdc_v2.` to
Debezium. Differences from `docs/research/debezium.md` §8.4, with the reason:

| Property | §8.4 | Ours | Why |
|---|---|---|---|
| `errors.max.retries` | 1 | 0 | attach rule: the engine never attaches a second time by itself |
| `slot.max.retries` | 2 | 0 | same; with 0 a slot that is still active fails at once (1.5 s measured) |
| `slot.retry.delay.ms` | 5000 | 1000 | unused with 0 retries |
| `driver.options` | lc_monetary, IntervalStyle, bytea_output, DateStyle | + `extra_float_digits=3`, `TimeZone=UTC` | H-013: pin every text-output setting of the walsender session |
| `converters` / `cdcv2.type` | "our converter" | `cdc_v2.engine.TransportConverter` | section 3 |
| `time.precision.mode` | structured | structured (no effect on values) | our converter takes over every temporal column; the setting only shapes column defaults, which we drop |
| `table.include.list` | — | optional (`EngineSettings.table_include`) | the publication decides by default |
| `database.sslrootcert` | — | when given | `verify-full` against a private CA |

Everything else is as §8.4 (memory offset store, `offset.flush.interval.ms=0`, `provide.transaction.metadata=true`,
`skipped.operations=none`, `tombstones.on.delete=false`, `publication.autocreate.mode=disabled`,
`snapshot.mode=no_data`, `lsn.flush.mode=connector`, `heartbeat.interval.ms=5000`, `max.batch.size=8192`,
`max.queue.size=32768`, `max.queue.size.in.bytes=268435456`, `poll.interval.ms=50`).
`heartbeat.action.query` is not set.

JVM: `-Xmx` from `EngineSettings.heap_mb` (512 MB default), `-XX:+UseParallelGC` (the spike measured +7 %),
`-XX:+ExitOnOutOfMemoryError` (an OOM ends the JVM instead of leaving it half alive), `-XX:MaxDirectMemorySize=64m`,
optional `-XX:ActiveProcessorCount`.

Our settings (`cdc_v2.*`): `engine.id`, `start.position` (echoed in HELLO), `frame.max.rows` (65,536),
`frame.target.bytes` (8 MiB), `frame.max.bytes` (1 GiB + 16 MiB), `max.value.bytes` (256 MiB), `ack.max.pending`
(4,096), `stop.timeout.ms` (15,000), `stat.interval.ms` (10,000), `parent.poll.ms` (500).

**JDK.** `jvm.prepare()` uses the Temurin 21 tarball of `jvm/jdk.lock.json` on Linux x86-64 (the Flight). The lock
holds Linux tarballs only, so on the development machine it uses the local JDK (`java_home`, `$CDC_V2_JAVA_HOME`,
`$JAVA_HOME` or the PATH) and says so in `JvmArtifacts.jdk_source`; every number in this package was measured with
Homebrew OpenJDK 23.0.2 on macOS arm64, classes compiled with `--release 17`. Jars: always the 73 locked jars,
verified, nothing else on the classpath.

## 5. Acknowledgement

The consumer keeps references only to END and HEARTBEAT records not yet acknowledged, with their `ack_position`
(`lsn_commit` of the record's offset if present, else `lsn_proc`: exactly the value
`PostgresStreamingChangeEventSource.commitOffset` sends). `ack_position` of END and HEARTBEAT records must not
decrease over the engine's life; if it would, the engine stops with FATAL. On `DURABLE p` the consumer takes the
newest held record with `ack_position ≤ p`, calls `markProcessed(record)` and `markBatchFinished()`, and drops it
and every older entry. A `DURABLE` below every held record acknowledges nothing; the engine keeps the newest p it
received, so records with `ack_position ≤ p` that arrive later (after a restart the pipeline may send p before
the redelivered transactions arrive) are acknowledged when they come
(`test_durable_sent_before_the_redelivery_still_acknowledges`). A `DURABLE` above all held records acknowledges
the newest one. A malformed DURABLE line is logged (`BAD_COMMAND`) and ignored. The list
is thinned when it exceeds `cdc_v2.ack.max.pending` (every second entry is dropped, the newest always kept);
dropping is safe, a later DURABLE then acknowledges an older record.

DURABLE commands are applied at the start and at the end of every `handleBatch` call (Debezium's committer is not
thread safe; it is used only on the engine's thread). After `markBatchFinished` the memory offset store flushes at
once (`offset.flush.interval.ms=0`) and the task sets a commit flag; the next `poll()` (≤ `poll.interval.ms`, 50 ms)
calls `commitOffset`, which flushes the LSN to the walsender. Delay from `Engine.durable(p)` to the slot's
`confirmed_flush_lsn` showing p (`evidence/p3/engine/ack_invariant.json`):

- busy (50 transactions/s): p50 0.062 s, max 0.151 s;
- idle: 2.2 s; the bound is `heartbeat.interval.ms` (5 s) plus one poll, because when idle `handleBatch` is called
  only for a heartbeat record.

Invariant probe (same test, 704 samples every 50 ms, 842 transactions, 76 DURABLE): `confirmed_flush_lsn` was never
beyond the newest DURABLE sent before the sample; with no DURABLE for 6 s under load it did not move at all.

`ack_position` mapped to the replication protocol (`test_positions_map_to_pgoutput_begin_and_commit`, against a
second slot peeked with Debezium's options): END `ack_position` = `positions.lsn_commit` = `positions.lsn` = the
`end_lsn` of pgoutput's Commit message (end of the commit record). BEGIN `positions.lsn` is below the Begin
message's `final_lsn` (= Commit's `commit_lsn`, the start of the commit record); BEGIN's `ack_position` is the
previous transaction's commit end. `positions.txId` is the 32-bit xid of the Begin message.

Heartbeat records carry the current offset; with only unpublished tables written they do **not** advance
(`heartbeat_moved_with_unpublished_wal: false`), confirming research §2.6: the slot follows idle periods only
through a heartbeat table that is in the publication (package P).

**Restart.** Without stored offsets Debezium starts at the slot's `confirmed_flush_lsn`. Test
`test_restart_after_random_kills_and_java_crash_points`: a miniature pipeline applied 3,921 rows in 1,307
transactions across 10 engine lives (3 SIGKILLs at random moments, 6 deaths at the Java crash points
`engine.ack_marking` and `engine.acked` with 3 seeds each, a final clean run): after every restart the first
transaction delivered had an END `ack_position` above the slot's position at start; no row was missing and none was
applied twice.

**Slot regression (core design open point 14).** A standby status update with a flush position below the slot's
`confirmed_flush_lsn` does **not** move it back on Postgres 18.6 (`evidence/p3/engine/slot_flush_backwards_pg18.json`:
slot advanced with `pg_replication_slot_advance`, then a pgjdbc replication client started at the old position and
sent flush = old for 3 s; and our engine streamed for 3 s). Postgres 16 and 17 were not tested: Homebrew has no
17 on this machine, and `tools/localpg` cannot build a Postgres 16 pair (it writes `sync_replication_slots`, a
Postgres 17 setting, and the server refuses to start). The Postgres 18 source has an explicit guard
(`LogicalConfirmReceivedLocation`: "Prevent moving the confirmed_flush backwards"); stage 2 must repeat the test on
the PlanetScale version.

## 6. Attach rule

- **HELLO only after `START_REPLICATION`.** Debezium offers no documented point at which replication has started:
  its `Connected` streaming metric is set before `startStreaming` (`ChangeEventSourceCoordinator.initStreamEvents`),
  and the connector callbacks fire before it too. But records reach `handleBatch` only from the started stream, so
  the engine sends HELLO at the first `handleBatch` call, before any other frame. Heartbeat records guarantee that
  call within `heartbeat.interval.ms` of the start. At that moment the slot is held by this engine's walsender, and
  a one-time JDBC query (`SlotProbe`) reads the holder's `active_pid`, `backend_start` and `application_name` into
  HELLO.
- **Application name.** Debezium overrides `ApplicationName` with the fixed string `Debezium Streaming` for every
  replication connection (`PostgresConnection.addDefaultSettings`), after the `database.*` and `driver.*` properties;
  a startup option `-c application_name=…` loses against the startup parameter (measured with psycopg: parameter
  `A` and option `B` give `A`). So the engine **cannot** give its replication connection a unique name with stock
  Debezium. HELLO carries the engine id we generate, and the holder's `active_pid` and `backend_start` instead;
  `test_hello_names_the_backend_that_holds_the_slot` shows they equal `pg_replication_slots.active_pid` and
  `pg_stat_activity.backend_start`, and that the name on the server is `Debezium Streaming`. Contract note below.
- **No second attach.** `errors.max.retries=0`, `slot.max.retries=0`. Terminating the walsender ends the engine with
  FATAL in 0.46 s, exit 1, and 5 s later the slot is still inactive. Through the TCP proxy: `reset` → FATAL after
  0.52 s, `close` → FATAL after 5.5 s; the slot was not taken again.
- **Slot still active at start** (another engine holds it): Debezium fails `START_REPLICATION` with "replication slot
  … is active for PID …" and, with no retries, the engine ends with FATAL after 1.46 s without sending HELLO; the
  holder keeps the slot. The pipeline terminates the holder and starts a new engine.
- **Black hole** (proxy drops everything both ways): the engine did **not** end within 150 s. The proxy's kernel
  acknowledges TCP, so neither TCP keepalives nor any timeout of pgjdbc or Debezium fires; the walsender on the
  server ended after `wal_sender_timeout`. A silent connection is found by SPEC 4.6's end-to-end heartbeat marker
  (and the supervisor's 120 s silence rule) in the pipeline, which kills the engine. pgjdbc exposes only
  `tcpKeepAlive` (on/off, OS default timers: 2 h on Linux); idle/interval/count can only be set through a custom
  `socketFactory` (a documented pgjdbc property) using `jdk.net.ExtendedSocketOptions` — not built; proposed for P4.

## 7. Stopping, failures, parent death

- `STOP`: no more frames are written; `engine.close()` in a separate thread with `cdc_v2.stop.timeout.ms`; then halt
  with 0 (or 3 if Debezium did not close). Measured 4–66 ms. `Engine.stop()` discards frames while it waits and
  SIGKILLs the JVM at its deadline.
- Any engine failure: FATAL with class, message and cause chain, scrubbed (configured password, host, user and
  database name replaced, longest first; `password=…` and JDBC URLs removed; 2,000 characters at most); exit 1.
  Python scrubs the password from FATAL messages again before it raises `EngineFatal`. Our own messages never
  contain values (table, column position and `seq` only).
- Parent killed with SIGKILL: the JVM was gone after 0.05 s and the slot inactive after 0.06 s
  (`parent_death.json`).
- Frame pipe closed: halt with 5. `engine.kill()` from Python: SIGKILL, reap, close every pipe; after that every
  read raises `EngineBroken` (the closed descriptor is never read again). A HELLO protocol error (no HELLO first, a
  second HELLO) marks the stream broken and kills the JVM.

## 8. Back-pressure, memory, limits

Nothing between the slot and file descriptor 3 is unbounded: Debezium's queue has `max.queue.size` records and
`max.queue.size.in.bytes` (256 MiB); our handler writes synchronously to the pipe and holds at most one open ROWS
frame (target 8 MiB, at most 65,536 rows, in practice one engine batch). When the pipeline stops reading, the write
blocks, the queue fills, the slot read stops; Debezium's keepalive thread keeps the walsender alive.

- 1M-row transaction, heap 512 MB: JVM peak resident 350 MB (`large_transaction.json`).
- Frozen reader for 75 s (> `wal_sender_timeout` 60 s) after a 300,000-row insert: resident memory 322.5 → 322.6 MB,
  flat (an earlier run went down from 315 to 246 MB as the GC gave memory back); the walsender kept the same pid; then all rows arrived (`frozen_reader.json`). The 180 s variant runs with
  `CDC_V2_E_LONG=1`.

Limits: a single value larger than `cdc_v2.max.value.bytes` (default 256 MiB, settable up to the frame maximum minus
1 MiB), a row larger than `cdc_v2.frame.max.bytes` (1 GiB + 16 MiB), or a logical-message content larger than the
value limit stops the engine with FATAL naming the table, column position and `seq`; nothing is ever truncated.
The JVM heap must hold such a value about three times (Debezium's decoded bytes and string, our UTF-8 bytes, the
column buffer): a 256 MiB value needs `heap_mb` of about 1,024. Values up to 1 GB of text (H-002) cannot be held
with the 1 GB heap cap; they take the skip-and-backfill path of H-002.

## 9. Event counts (exact rule for the pipeline)

Proven by `test_operations_transactions_and_event_counts` (`operations_event_counts.json`), which also asserts for
every END that `event_count` = the records delivered in the transaction = the sum of the per-collection counts:

- every delivered row record counts once: insert, update, delete;
- a key change counts **2**: a delete with the old key and a create with the new row (no tombstone is emitted,
  `tombstones.on.delete=false`); a chain A→B, B→C in one transaction counts 4; a swap counts 4;
- a TRUNCATE counts one per table it names (`truncate t, k` → 2; `truncate parent cascade` → 2: parent and child;
  `restart identity` → 1);
- a transactional logical message counts 1; its data collection name is Debezium's object `toString`
  (`…LogicalDecodingMessageMonitor$LogicalDecodingMessageId@<hash>`), so match by prefix, not by name;
  non-transactional messages are outside every transaction and outside every count;
- rows of a table outside `table.include.list` are neither delivered nor counted; a transaction that touches only
  such tables arrives as BEGIN and END with `event_count` 0;
- an empty transaction and a transaction that touches only unpublished tables produce nothing (pgoutput skips them);
  rolled-back transactions and rolled-back savepoints produce nothing.

So the pipeline's rule is: `event_count` = number of ROWS rows + TRUNCATE frames + transactional MESSAGE frames
between BEGIN and END.

Other observations from the same tests: BEGIN and END carry different transaction id strings
(`xid:lsn` with different LSNs); identify a transaction by its END `ack_position` and `positions.txId`.

## 10. Measurements

Indications only (`evidence/p3/engine/throughput.json`; shared 10-core laptop, load average 5–6 from other work;
source, walsender, JVM and reader on one host; heap 512 MB, ParallelGC). A backlog of `lineitem`-shaped rows (the
16 columns of `docs/spike/local_throughput.md` §1) is written to the slot, then the engine drains it into a reader
that only reads frame headers and the row count of ROWS frames. Our engine never sends DURABLE in this test, so
every variant drains the same backlog; the spike program (`EngineMain`, struct format, no acknowledgement wait)
drains it last. Rates from the first frame to the last row; JVM CPU from `ps`; per-thread CPU from the engine's
STOP line (ThreadMXBean).

| Rows per transaction | Program, settings | Rows | Rows/s | JVM CPU µs/row | Debezium streaming thread | Engine poll thread (incl. our handler) | Our handler | GC (wall) |
|---|---|---|---|---|---|---|---|---|
| 1 | engine, default | 20,000 | 15,867 | 280 | 1.40 s | 0.65 s | 0.23 s | 0.04 s |
| 1 | spike | 20,000 | 19,177 | 307 | – | – | – | – |
| 1,000 | engine, default | 300,000 | 59,721 | 43.5 | 5.03 s | 1.09 s | 0.43 s | 0.14 s |
| 1,000 | spike | 300,000 | 56,989 | 49.4 | – | – | – | – |
| 1,000,000 | engine, default (batch 8,192, queue 32,768, 1 thread) | 1,000,000 | 64,566 | 29.8 | 15.25 s | 2.60 s | 1.29 s | 0.34 s |
| 1,000,000 | engine, batch 2,048, queue 8,192 | 1,000,000 | 69,356 | 27.5 | 14.27 s | 2.65 s | 1.29 s | 0.28 s |
| 1,000,000 | engine, batch 32,768, queue 131,072 | 1,000,000 | 69,734 | 27.6 | 14.11 s | 2.45 s | 1.19 s | 0.32 s |
| 1,000,000 | engine, `record.processing.threads=2` | 1,000,000 | 70,932 | 27.6 | 13.83 s | 3.05 s | 1.23 s | 0.36 s |
| 1,000,000 | spike | 1,000,000 | 60,486 | 32.1 | – | – | – | – |

Where the CPU goes (1M-row transaction): about half in Debezium's single streaming thread (14–15 µs/row: pgoutput
decoding, value resolution, `Struct` building, our converter), 2.5–3 µs/row in the engine's poll thread, of which
our handler (frame encoding and writing) is **1.2–1.3 µs/row, about 4.5 % of the JVM's CPU**; the rest is JIT,
keepalive and Debezium's metrics thread ("other", 3–4.5 s). The walsender used 7–8.5 s of CPU per million rows.
Batch size, queue size and a second processing thread do not move the rate beyond noise (64–71K rows/s). Our
engine is as fast as the spike program or faster (it builds no per-row byte stream twice and its converter skips
Debezium's temporal structs); for one-row transactions it costs about 12 µs of handler CPU per transaction (BEGIN,
ROWS, END and BATCH_END frames and a flush per batch). The handler being a small part, no further tuning was done
here (P9).

## 11. Contract notes

1. `Engine.frames(deadline)` ends the iteration when the deadline passes (no exception), so a caller can interleave
   other work; `Engine.next_frame(deadline)` raises `DeadlineExceeded`. After STOP the iteration ends when the JVM
   has exited 0. Errors: `EngineFatal` (FATAL frame), `EngineBroken` (stream), `EngineExited` (a subclass of
   `EngineBroken`: the JVM ended without FATAL and without STOP).
2. `start_position` is echoed in HELLO and not used to position Debezium: with the memory offset store Debezium starts
   at the slot's `confirmed_flush_lsn`; the pipeline discards redelivered transactions by position.
3. HELLO has three fields more than C2 names: JVM pid, engine id, and the slot holder's `active_pid`,
   `backend_start` and `application_name`. **C2 should change**: the application name cannot be unique with stock
   Debezium (section 6); the pipeline should identify its engine's backend by `active_pid` + `backend_start` from
   HELLO, checked against slot facts while the JVM is alive.
4. `seq` is given to every record (BEGIN, END, HEARTBEAT too), one counter from 0 per engine life.
5. BEGIN carries an `ack_position` too (informative; never acknowledged).
6. MESSAGE `transactional` = the message arrived between BEGIN and END.
7. `u<i>` columns exist for a conservative superset of the columns that can be unchanged (section 3).
8. Temporal transport uses the Postgres epoch (2000-01-01), section 3.
9. `posmap` integers are signed 64-bit in Python.
10. The marker `jvm` is registered by `tests/engine/conftest.py`, which also skips `jvm` tests unless `-m` selects
    them. `pyproject.toml` should list `jvm` in `markers` and in the default `addopts` exclusion (not edited here).

## 12. Review

A read-only reviewer with fresh context (Opus) found no problem in the acknowledgement path or the Java encoding and
eight smaller ones, all fixed with tests where testable: a closed frame descriptor could be read after `kill()` or
FATAL; HELLO protocol errors did not break the stream; a DURABLE that matched no held record was dropped (now kept,
section 5); secret scrubbing depended on map order; a corrupted length could raise `MemoryError`; a ROWS frame
without columns was accepted; `frame_target_bytes` was not checked against `frame_max_bytes`; a malformed DURABLE
line halted the JVM as "parent gone".

## 13. Tests and how to run them

```sh
uv run -q pytest -q tests/engine                                         # hermetic (default run): codec, detection, Engine with a fake JVM
uv run -q pytest -q -m jvm tests/engine                                  # local JVM, no Postgres
uv run -q pytest -q -m "jvm and localpg" tests/engine                    # the private pair pg-e (ports 55620-55627)
CDC_V2_E_LONG=1 uv run -q pytest -q -m "jvm and localpg" tests/engine    # long variants (1,000 detection seeds, 180 s freeze)
CDC_V2_E_MEASURE=1 uv run -q pytest -q -m "jvm and localpg" tests/engine/test_engine_measure.py -s
# with the variable CDC_V2_E_EVIDENCE set to the directory evidence/p3/engine, the suites also write the evidence files
```

The `localpg` fixture runs `tools/localpg --home ~/.cache/cdc_flight_v2/pg-e --port-base 55620 up` and `down` at the
end (`CDC_V2_E_KEEP_PG=1` keeps it running). Every scenario creates its own schema, publication and slot and drops
them, also on failure.
