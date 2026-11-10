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
| HEARTBEAT | 8 | u64 seq, posmap (information only: no ack_position; never acknowledged) |
| BATCH_END | 9 | u32 records in the engine batch, opt_u64 newest ack_position handed to Debezium so far |
| FATAL | 10 | str class, str safe message, u16 n, n × (str class, str safe message) of the cause chain, posmap of the last record handed over, str schema and str table of the record being handled (empty when unknown), `failing_position`: flag, then (if 1) opt_u64 lsn, opt_u64 txId of the event Debezium names as failing |

A column **buffer** is u32 byte length, zero bytes up to a multiple of 8 counted from the payload start, the bytes,
zero bytes up to the next multiple of 8. So every buffer starts 8-aligned inside the payload; Python reads each
payload into its own `bytearray` (malloc-aligned) and wraps the buffers with `pyarrow.Array.from_buffers` without
copying (test: `test_rows_buffers_are_zero_copy_slices_of_the_payload`). That `bytearray` grows as the payload's bytes arrive, never ahead by the length field, so a damaged length cannot reserve memory (`tests/engine/test_frames_length_alloc.py`). Arrow layout per physical type:

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
an interval whose hour part exceeds 2^31 (`'2562047788 hours'`) fails in pgjdbc's `PGInterval` parser and stops the
engine with FATAL; the infinite intervals of Postgres 17+ are parsed into a zero interval without an error (both
measured, section 10); geometric values pass through pgjdbc objects and lose Postgres' float formatting.

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
| `table.include.list` | — | optional (`EngineSettings.table_include`) | the publication decides by default; also the way to leave one table out (section 10) |
| `database.sslrootcert` | — | when given | `verify-full` against a private CA |

Everything else is as §8.4 (memory offset store, `offset.flush.interval.ms=0`, `provide.transaction.metadata=true`,
`skipped.operations=none`, `tombstones.on.delete=false`, `publication.autocreate.mode=disabled`,
`snapshot.mode=no_data`, `lsn.flush.mode=connector`, `heartbeat.interval.ms=5000`, `max.batch.size=8192`,
`max.queue.size=32768`, `max.queue.size.in.bytes=268435456`, `poll.interval.ms=50`).
`heartbeat.action.query` is not set.

**Protected properties** (plan C2, hazard H-016). `EngineSettings.extra` may not set any key of
`cdc_v2.stream.engine.PROTECTED_KEYS` (ValueError): `skipped.operations`, `offset.storage` and `offset.storage.*`,
`offset.flush.interval.ms`, `offset.flush.timeout.ms`, `offset.commit.policy`, `lsn.flush.mode`, `errors.max.retries`,
`slot.max.retries`, `slot.retry.delay.ms`, `provide.transaction.metadata`, `snapshot.mode`,
`publication.autocreate.mode`, `publication.name`, `slot.name`, `plugin.name`, `converters`, `cdcv2.*`,
`tombstones.on.delete`, `heartbeat.interval.ms`, `heartbeat.action.query`, `replica.identity.autoset.values`,
`read.only`, `signal.*`, `offset.mismatch.strategy`, `unavailable.value.placeholder`, `decimal.handling.mode`,
`binary.handling.mode`, `include.unknown.datatypes`, `record.processing.order`, `schema.refresh.mode`, the
connection keys and `cdc_v2.*`, and the options that silently change or drop values or rows: every key with the
prefix `column.` (`column.mask.*`, `column.truncate.to.*`, `column.mask.hash.*`, `column.exclude.list`,
`column.include.list`, …), `transforms` and `transforms.*`, `predicates` and `predicates.*`, `message.key.columns`,
`table.include.list` (it comes only from `table_include`), `table.exclude.list`, `schema.include.list`,
`schema.exclude.list`.
`EngineSettings` requires 0 < `heartbeat_interval_ms` ≤ 10,000 (with 0 HELLO never comes on an idle source and the
last transaction before an idle period is never confirmed). The JVM checks the effective configuration again before
anything else (`ProtectedConfig`): every guarantee-carrying key must have exactly our value (`skipped.operations=none`,
memory offset store, `offset.flush.interval.ms=0`, `lsn.flush.mode=connector`, both retry settings 0,
`provide.transaction.metadata=true`, `snapshot.mode=no_data`, `publication.autocreate.mode=disabled`, our converter,
`tombstones.on.delete=false`, `offset.mismatch.strategy=no_validation`, `plugin.name=pgoutput`,
`record.processing.order=ORDERED`, `include.unknown.datatypes=true`, `schema.refresh.mode=columns_diff`), the
behaviour-adding and value-changing keys above must be absent (`table.include.list` is allowed: Python sets it), the heartbeat interval must be in 1..10,000 ms and the host and port must be
present (no default host, never port 5432); otherwise FATAL naming the key, never a value (41 `jvm` cases in
`test_java_refuses_a_weakened_configuration`, 43 hermetic cases in `test_extra_may_not_set_a_protected_key`).

JVM: `-Xmx` from `EngineSettings.heap_mb` (512 MB default), `-XX:+UseParallelGC` (the spike measured +7 %),
`-XX:+ExitOnOutOfMemoryError` (an OOM ends the JVM instead of leaving it half alive), `-XX:MaxDirectMemorySize=64m`,
optional `-XX:ActiveProcessorCount`.

Our settings (`cdc_v2.*`): `engine.id`, `start.position` (echoed in HELLO), `test.probe.slot` (test hook: the slot the
HELLO probe reads; Python's `extra` cannot set it), `frame.max.rows` (65,536),
`frame.target.bytes` (8 MiB), `frame.max.bytes` (1 GiB + 16 MiB), `max.value.bytes` (256 MiB), `ack.max.pending`
(4,096), `stop.timeout.ms` (15,000), `stat.interval.ms` (10,000), `parent.poll.ms` (500).

**JDK.** `jvm.prepare()` uses the Temurin 21 tarball of `jvm/jdk.lock.json` on Linux x86-64 (the Flight). The lock
holds Linux x86-64 tarballs only (no entry for macOS arm64), so the suites could not run on the locked Temurin 21 here; so on the development machine it uses the local JDK (`java_home`, `$CDC_V2_JAVA_HOME`,
`$JAVA_HOME` or the PATH) and says so in `JvmArtifacts.jdk_source`; every number in this package was measured with
Homebrew OpenJDK 23.0.2 on macOS arm64, classes compiled with `--release 17`. Jars: always the 73 locked jars,
verified, nothing else on the classpath.

## 5. Acknowledgement

**Only END records are acknowledged** (plan C2). Debezium dispatches a heartbeat record that already carries a
transaction's commit position *before* it creates that transaction's END record
(`PostgresStreamingChangeEventSource.commitMessage`), and one with the position of a non-transactional message
before that message; a pipeline that adopted heartbeat positions lost transactions (second review). So the engine
never holds or marks a heartbeat record, a HEARTBEAT frame has no `ack_position` (an old-format frame with one is
refused by the decoder: unused payload bytes), and a non-transactional MESSAGE is not held either. HEARTBEAT frames
can arrive anywhere, also between BEGIN and END; they show that the engine is alive and they cause the batch calls in
which a DURABLE takes effect on an idle source. Measured in `end_only_heartbeat_storm.json`
(`heartbeat.interval.ms=1`): 162 of 1,056 HEARTBEAT frames carried the commit position of the END that followed them.

The `ack_position` of an END is `lsn_commit` of its offset, exactly the value
`PostgresStreamingChangeEventSource.commitOffset` sends; an END whose offset has no `lsn_commit` stops the engine with
FATAL, never a fallback to another field (`HeldEnds.endAck`, `test_java_held_list_bound_and_end_without_lsn_commit`).
END `ack_position` values must not decrease over the engine's life (FATAL otherwise). BEGIN still carries an
`ack_position` computed as before (the previous commit end, or `lsn_proc` in the first transaction): information
only, never acknowledged.

On `DURABLE p` the consumer takes the newest held END with `ack_position ≤ p`, calls `markProcessed(record)` and
`markBatchFinished()`, and drops it and every older one. A `DURABLE` below every held END acknowledges nothing; the
engine keeps the newest p it received, so ENDs with `ack_position ≤ p` that arrive later (after a restart the
pipeline may send p before the redelivered transactions arrive) are acknowledged when they come
(`test_durable_sent_before_the_redelivery_still_acknowledges`). A `DURABLE` above all held ENDs acknowledges the newest
one. Malformed, backward and repeated DURABLE lines (`test_durable_edge_cases`: nine malformed forms, a backward raw
line, three repeats, a value between two held ENDs) leave the slot as specified and the engine alive; a malformed
line is logged as `BAD_COMMAND` and ignored.

**The held list and its bound.** `HeldEnds` holds every k-th END plus always the newest one. When more than
`cdc_v2.ack.max.pending` (4,096) samples are held, the newest sample and every second one counted back from it are
kept, and the oldest one too, so no gap between held samples exceeds the new k; k doubles; when the list empties, k
returns to 1. **Bound: after a `DURABLE p`, the newest END confirmed so far lies at most k − 1 END records below the
newest END at or below p, with k ≤ max(1, 2 × lead / limit)**, where lead is the number of ENDs the engine has
received beyond the oldest one not yet confirmed; it is never above p. This holds for every limit ≥ 2, odd or even:
`test_java_held_list_property_random_limits` checks it for random limits 2–41 and random add/take sequences (seeds
1–3, 60 rounds each, 35,000–38,000 takes per seed, 0 violations); the previous thinning, which could leave the newest
sample only in the tail where the next END overwrote it, fails that test with odd limits (580 violations for seed 1). Example: 60,000 ENDs read with no DURABLE, then DURABLE
for END #20,000, #36,000 and #60,000: the slot reached #19,985, #35,985 and #60,000 (15 behind; stated bound 28)
within 0.7–0.8 s each and was never above (`held_list_bound.json`). The first version's thinning dropped the oldest
entry at every thinning and stopped following while the pipeline was more than about 24,000 ENDs behind.

**Timing.** DURABLE commands are applied at the start and at the end of every `handleBatch` call (Debezium's
committer is not thread safe; it is used only on the engine's thread). After `markBatchFinished` the memory offset
store flushes at once (`offset.flush.interval.ms=0`) and the task sets a commit flag; the next `poll()` calls
`commitOffset`, which sends the flush position to the walsender. Bound on an idle source: **`heartbeat.interval.ms`
+ 1 s** (one heartbeat interval until the next batch call, one poll, the flush); measured max 0.30 s at 300 ms and
0.26 s at 500 ms (`idle_ack_bound.json`, 5 runs each). Busy: within one batch and one poll.

**Crash points.** `engine.ack_marking` (before `markProcessed`), `engine.acked` (after `markBatchFinished`: the
flush is not sent yet) and `engine.flush_sent`: the first batch after an acknowledgement, which Debezium's `poll()`
delivers only after `performCommit()` has sent the flush of the previous acknowledgement (`BaseSourceTask.poll`). The
engine cannot observe the flush message itself (it is sent on Debezium's lsn-flush thread), so this is the earliest
place where it is known to have gone out.

**DURABLE after STOP** is not applied: STOP ends the engine without another batch call. That is safe: the
transactions after the last applied DURABLE are delivered again at the next start and discarded by position.

**Invariant and restarts.** `test_slot_never_ahead_of_durable_and_follows_it`: with no DURABLE for 6 s under load the
slot did not move; then, with DURABLE lagging three ENDs, `confirmed_flush_lsn` was never above the newest DURABLE
sent before the sample. With WAL for unpublished tables only, neither the slot nor the HEARTBEAT positions moved
(asserted; research §2.6: an idle source moves only through the published heartbeat table of package P).
`test_end_only_acknowledgement_under_a_heartbeat_storm`: 40,000 single-row and 2,000 message-only transactions,
heartbeat interval 1 ms, a pipeline stand-in to the new contract (applied position = the `ack_position` of the last
END applied, three transactions behind, DURABLE only for that, HEARTBEAT ignored, unapplied work and unread frames
dropped at each kill), three kills at random points (after 24,928, 27,732 and 29,548 rows applied): 0 samples
above DURABLE, nothing missing, nothing doubled.
`test_restart_after_random_kills_and_java_crash_points`: the same stand-in under continuous writes, 3 random kills
and kills at the three Java crash points with 3 seeds each (all 9 reached): 4,492 rows, none missing or doubled, the
first transaction of every life above the slot's position at start. Negative control
(`test_negative_control_one_end_ahead_is_caught`): a stand-in that sends DURABLE one END ahead of what it applied is
caught (52 samples with the slot above the applied position).

`ack_position` mapped to the replication protocol (`test_positions_map_to_pgoutput_begin_and_commit`, against a
second slot peeked with Debezium's options): END `ack_position` = `positions.lsn_commit` = `positions.lsn` = the
`end_lsn` of pgoutput's Commit message (end of the commit record). BEGIN `positions.lsn` is below the Begin
message's `final_lsn` (= Commit's `commit_lsn`, the start of the commit record). `positions.txId` is the 32-bit xid
of the Begin message.

**Slot regression (core design open point 14).** A standby status update with a flush position below the slot's
`confirmed_flush_lsn` moves the slot **back to the position sent on Postgres 16.6** and does **not** move it on
Postgres 18.6 (`evidence/p3/engine/slot_flush_backwards_pg16.json` and `slot_flush_backwards_pg18.json`: slot
advanced with `pg_replication_slot_advance`, then a pgjdbc replication client started at the old position and sent
flush = old for 3 s; then our engine streamed for 3 s). The difference is a guard in
`LogicalConfirmReceivedLocation` ("Prevent moving the confirmed_flush backwards"), a Postgres commit of 2025-05-19
that was back-patched through 13. The 16.10 release notes list it ("Never move a replication slot's
confirmed-flush position backwards"). The minor releases of the same date on the other branches are 13.22, 14.19,
15.14 and 17.6 (taken from the back-patch and the common release date; their release notes were not read); 18.0
has the guard from the start. Our engine sends no flush position before its first `DURABLE`: while it streamed,
the slot stayed where the probe had left it, on both versions. What this means for the attach rule: core design 8.

## 6. Attach rule

- **HELLO only after `START_REPLICATION`.** Debezium offers no documented point at which replication has started:
  its `Connected` streaming metric is set before `startStreaming` (`ChangeEventSourceCoordinator.initStreamEvents`),
  and the connector callbacks fire before it too. But records reach `handleBatch` only from the started stream, so
  the engine sends HELLO at the first `handleBatch` call, before any other frame. Heartbeat records guarantee that
  call within `heartbeat.interval.ms` of the start. At that moment the slot is held by this engine's walsender, and
  a one-time JDBC query (`SlotProbe`) reads the holder's `active_pid`, `backend_start` and `application_name` into
  HELLO. If the probe fails, or finds no slot row or no holder, the engine sends FATAL "slot not held at probe",
  never a HELLO with empty fields (`test_slot_not_held_at_probe_is_fatal`). The probe has no default host or port:
  without them the engine refuses at start (FATAL).
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
  acknowledges TCP, so neither TCP keepalives nor any timeout of pgjdbc or Debezium fires. HEARTBEAT frames (interval
  1 s) kept arriving for 8.2 s after the hole opened and then stopped (`proxy_blackhole.json`), so the stream can
  detect the hole by the absence of HEARTBEAT frames; the walsender on the server ended after 59.8 s
  (`wal_sender_timeout` 60 s). A silent connection is found by SPEC 4.6's end-to-end heartbeat marker
  (and the supervisor's 120 s silence rule) in the pipeline, which kills the engine. pgjdbc exposes only
  `tcpKeepAlive` (on/off, OS default timers: 2 h on Linux); idle/interval/count can only be set through a custom
  `socketFactory` (a documented pgjdbc property) using `jdk.net.ExtendedSocketOptions` — not built; proposed for P4.

## 7. Stopping, failures, parent death

- `STOP`: no more frames are written; `engine.close()` in a separate thread with `cdc_v2.stop.timeout.ms`; then halt
  with 0 (or 3 if Debezium did not close). Measured 4–66 ms. `Engine.stop()` discards frames while it waits and
  SIGKILLs the JVM at its deadline.
- Any engine failure: FATAL with class, cause chain, the position fields of the last record handed over and, when
  known, the schema and table of the record being handled (known for failures in our handler; not for failures in
  Debezium's decoding thread, see section 10); exit 1. **Messages never carry a source value** (plan C0): a message is
  kept only for our own exceptions (built without values) and for Debezium/Connect messages that start with a known
  safe prefix (cut at the first `;`); a SQL exception becomes `SQLState <code>`; every other message is dropped (class
  name only). Configured password, host, user and database name are scrubbed, longest first.
- **Logs never carry a source value.** Debezium's two loggers that print a value they cannot parse
  (`AbstractColumnValue`, `DateTimeFormat`) are switched off; the Python drain keeps only our `CDCV2` lines and
  Debezium's log lines (quoted text removed from WARN and ERROR lines) and counts, without copying, every other line:
  stack traces, exception headers (`For input string: "…"`), `Caused by`. Tested with a planted value
  (`test_unparsable_interval_ends_in_fatal_without_the_value`: neither the value nor a marker column of the same row
  appears in the FATAL frame, the JVM output or the Python log).
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
- rows of a table outside `table.include.list` are neither delivered nor counted, also in a transaction that touches
  included and excluded tables; a TRUNCATE of an excluded table in a mixed transaction is neither delivered nor
  counted; a transaction that touches only excluded tables arrives as BEGIN and END with `event_count` 0;
- a transactional message whose prefix is outside `message.prefix.include.list` is neither delivered nor counted; in
  a transaction of its own it arrives as BEGIN and END with `event_count` 0;
- an empty transaction and a transaction that touches only unpublished tables produce nothing (pgoutput skips them);
  rolled-back transactions and rolled-back savepoints produce nothing.

So the pipeline's rule is: `event_count` = number of ROWS rows + TRUNCATE frames + transactional MESSAGE frames
between BEGIN and END.

Other observations from the same tests: BEGIN and END carry different transaction id strings
(`xid:lsn` with different LSNs); identify a transaction by its END `ack_position` and `positions.txId`.

## 10. Values the driver cannot parse

Debezium resolves a column value through pgjdbc before our converter sees it (section 3), so a value pgjdbc cannot
parse never reaches our code. Measured on the Postgres 18.6 pair (`test_engine_values.py`):

- **`interval '2562047788 hours'`** (hour part above 2^31): pgjdbc's `PGInterval` fails (`NumberFormatException`
  inside `PSQLException`, SQLState 42820) in Debezium's decoding thread, and the engine ends with FATAL after
  1.4–1.5 s (`unparsable_interval.json`). Before FATAL: HELLO, and whatever of the queue the poll loop had handed
  over, never anything of the failing transaction beyond its BEGIN (in one run of three attempts: once only the BEGIN of the
  transaction before it, twice that transaction complete and the BEGIN of the failing one; in the final run all
  three complete). Never a partial ROWS of
  the failing transaction. FATAL carries the position fields of the last record handed over; its cause message
  `Error while processing event at offset {…}` names the failing event's offset (`txId`, `lsn`), the same in every
  attempt; the engine parses `lsn` and `txId` from it and sends them as `failing_position` in the FATAL frame
  (never the text). `failing_position` was present and equal over three starts
  (`test_unparsable_interval_ends_in_fatal_without_the_value`); `positions` (the last record handed over) can differ
  between starts (txId 580034 against 580035 in one run, equal in the last). The stream compares `failing_position`.
  The table is not known to the engine (the failure is in Debezium's thread, before a record exists).
  Neither the value nor a marker column of the row appears in the FATAL frame, the JVM output or the Python log. A
  restart ends again at the same transaction, 1.4–1.5 s after the start. **With the table left out of
  `table.include.list` the transaction passes**: it arrives as BEGIN and END with `event_count` 0 and the stream goes
  on (the next transaction's row arrives). So package S can quarantine one table: find the candidate tables (those
  with a column of a type in this class, here `interval`; or by elimination), restart the engine with them excluded,
  and backfill them.
- **An infinite interval arrives as a zero interval; the engine cannot tell them apart and changes nothing — the
  rule is the pipeline's** (the transform marks a zero interval from a source of Postgres 17 or later, the stream
  reads the true value from the source). Interval arrays are not affected: they travel as their text form, which
  keeps `infinity`. Details: `interval 'infinity'` and `interval '-infinity'` (Postgres 17+): pgjdbc's `PGInterval`
  parses them **into the zero interval, silently**; the engine delivers `0 months 0 days 0 microseconds`, the same as a real zero
  (`infinite_interval.json`, `test_infinite_interval_is_delivered_as_zero`). Our converter receives only the parsed
  object; the text is gone, so the engine cannot tell. **This is silent data loss with stock Debezium and is decided
  outside the engine**: the rule is "Ambiguous zero interval" in contract C3 of `docs/design/p3_plan.md`. Not a
  crash, so no restart loop.

## 11. Measurements

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

## 12. Contract notes

1. `Engine.frames(deadline)` ends the iteration when the deadline passes (no exception), so a caller can interleave
   other work; `Engine.next_frame(deadline)` raises `DeadlineExceeded`. After STOP the iteration ends when the JVM
   has exited 0. Errors: `EngineFatal` (FATAL frame), `EngineBroken` (stream), `EngineExited` (a subclass of
   `EngineBroken`: the JVM ended without FATAL and without STOP).
2. `start_position` is echoed in HELLO and not used to position Debezium: with the memory offset store Debezium starts
   at the slot's `confirmed_flush_lsn`; the pipeline discards redelivered transactions by position.
3. HELLO has fields beyond C2's list: JVM pid, engine id, and the slot holder's `active_pid`, `backend_start` and
   `application_name` (C2 now names the holder pair; the name is always `Debezium Streaming`).
4. `seq` is given to every record (BEGIN, END, HEARTBEAT too), one counter from 0 per engine life.
5. BEGIN carries an `ack_position` too (informative; never acknowledged). HEARTBEAT has none (C2 as changed).
6. MESSAGE `transactional` = the message arrived between BEGIN and END.
7. `u<i>` columns exist for a conservative superset of the columns that can be unchanged (section 3).
8. Temporal transport uses the Postgres epoch (2000-01-01), section 3.
9. `posmap` integers are signed 64-bit in Python.
10. The marker `jvm` is registered in `pyproject.toml` and excluded from the default run; `tests/engine/conftest.py`
    also skips `jvm` tests unless `-m` selects them (the skip reason).
11. FATAL has fields beyond C2's list: the position fields of the last record handed over, schema and table of
    the record being handled (empty when unknown), and `failing_position` ({`lsn`, `txId`} of the event that
    Debezium names in "Error while processing event at offset {…}"; absent when no exception names one). Compare
    `failing_position`, not `positions`, to recognise "the same place" across restarts. Its messages are filtered
    (section 7).
12. **Two transport forms of text, on purpose.** The walsender session of the engine uses `IntervalStyle=postgres`
    and `DateStyle=ISO` (`driver.options`, section 4); the read connections of the source package use `postgres`
    too (since the fix round of package Y, so that arrays and composites that hold intervals print alike) and
    `ISO, YMD`. The stream form of every type is the table of section 3 of this document; the backfill form is
    the source package's.
13. The source package's nudge writes a logical message with the prefix `cdc_v2.nudge`, which lies outside the
    default `message.prefix.include.list` (`cdc_v2_.*`) on purpose: it never reaches the pipeline.

## 13. Review

A read-only reviewer with fresh context (Opus) found no problem in the acknowledgement path or the Java encoding and
eight smaller ones, all fixed with tests where testable: a closed frame descriptor could be read after `kill()` or
FATAL; HELLO protocol errors did not break the stream; a DURABLE that matched no held record was dropped (now kept,
section 5); secret scrubbing depended on map order; a corrupted length could raise `MemoryError`; a ROWS frame
without columns was accepted; `frame_target_bytes` was not checked against `frame_max_bytes`; a malformed DURABLE
line halted the JVM as "parent gone".

The coordinator's review and a second review of the acknowledgement path asked for: END-only acknowledgement and a
HEARTBEAT frame without `ack_position` (section 5); a held list that keeps following with a stated bound (section 5);
protected configuration in Python and Java (section 4); FATAL instead of a HELLO without a holder, no default host or
port in the probe (section 6); FATAL with positions and without source values, and logs without them (section 7);
values the driver cannot parse (section 10); a restart test that catches over-acknowledgement, with a negative
control; DURABLE edge cases; a crash point after the flush; the black-hole HEARTBEAT record; more event-count cases.
All done; the tests are named where each is described.

## 14. Tests and how to run them

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
them, also on failure. TCP proxies of these tests lease ports 55624–55627 (`NetProxy(..., port_range=...)`).

**Other Postgres versions.** The tests take the Postgres binaries from `CDC_V2_PG_BIN` (as `tools/localpg` does) and
the home from `CDC_V2_E_PG_HOME`, for example
`CDC_V2_PG_BIN=/opt/homebrew/opt/postgresql@16/bin CDC_V2_E_PG_HOME=~/.cache/cdc_flight_v2/pg-e16 uv run -q pytest
-q -m "jvm and localpg" tests/engine`.

Run on 16.6 on 2026-11-10: the 14 tests whose result can depend on the server version. 13 give the same result as
on 18.6: restart after random kills and Java crash points, slot never ahead of `DURABLE`, frozen reader, `DURABLE`
sent before the redelivery, the holder that `HELLO` names, terminated walsender, slot held by another engine,
position mapping, operations and event counts, unchanged TOAST, schema change between transactions, large
transaction in bounded memory, baseline types value by value. The fourteenth is the slot regression test of
section 5, where 16.6 differs: `test_lower_flush_position_and_the_slot` knows the first minor release with the
guard on each branch and asserts the behaviour that belongs to the server it runs on. The other engine tests do
not touch a version-dependent part of the server and were not repeated on 16. Infinite intervals exist from
Postgres 17 on; their test skips on 16. Postgres 17 is not installed on the build machine: not run.
