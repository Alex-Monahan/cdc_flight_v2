# WAL hazard catalog (SPEC 4.3)

Status: **first complete draft, 2026-11-09** (PLAN phase P5 research, started early). Every entry is *proposed*;
nothing here is implemented or tested yet. Background and sources: `docs/research/debezium.md` (stock Debezium
3.7.1 behaviour), `docs/research/replica_decoding.md` (standby decoding, PostgreSQL 16–18, PlanetScale).

SPEC R12 asks that problematic WAL content be **discovered up front and prevented**, not merely recovered from. Each
entry therefore names a *prevention* — a configuration, a guard that runs before the hazard can do damage, or a design
rule — and says why it prevents. Where a hazard cannot be prevented from our side (the source database or the provider
does something we cannot stop), the entry says so and gives the earliest *detection* plus the automatic answer; SPEC
4.3's last line of defence (skip a message that fails repeatedly at the same position, backfill its tables, then 4.0)
stays in place behind every entry.

## Conventions

- Marks: **[doc]** documented (URL in the research notes or here), **[src]** read in source (file, version),
  **[test]** to be verified by test.
- Reproductions run against the T1 clusters of PLAN §5: **P** = primary (port 55432), **R** = streaming replica
  (port 55433), both PostgreSQL 18, managed only with `tools/localpg`. Common setup **S0**, used by most entries:

  ```sql
  -- P
  CREATE TABLE cdc_v2_t (id bigint PRIMARY KEY, v text);
  CREATE TABLE cdc_v2_hb (id int PRIMARY KEY, ts timestamptz);            -- heartbeat (SPEC 4.4)
  CREATE PUBLICATION cdc_v2_pub FOR TABLE cdc_v2_t, cdc_v2_hb;
  INSERT INTO cdc_v2_hb VALUES (1, now());                                -- any activity: lets R build a snapshot
  -- R (after the publication has replayed)
  SELECT pg_create_logical_replication_slot('cdc_v2_slot', 'pgoutput');
  -- R: raw view of what pgoutput would send (no Debezium needed)
  SELECT lsn, xid, encode(data, 'escape') FROM pg_logical_slot_peek_binary_changes(
    'cdc_v2_slot', NULL, NULL,
    'proto_version', '1', 'publication_names', 'cdc_v2_pub', 'messages', 'true');
  ```

  `peek` uses exactly Debezium's options (protocol 1, `messages`), so a reproduction shows what Debezium receives;
  Debezium-specific behaviour is then reproduced with the pipeline itself.
- "Test" names the automated test that proves the prevention; it gets an id `test_hNNN_*` in the suite. Every test
  ends with the oracle (SPEC §3 conventions) unless stated.
- Hazard numbers are stable; new classes get new numbers, retired ones keep their number with status "merged".

---

## A. Size and volume

### H-001 Oversized transactions

- **What.** One source transaction with millions of changes (SPEC 1.3/5.1: ≥ 10M rows, no size exemption).
- **Mechanism.** pgoutput protocol 1 (the only one Debezium speaks, debezium.md §1.5) sends nothing before COMMIT; the
  walsender keeps the transaction in its reorder buffer and spills to `pg_replslot/<slot>/` on the decoding server
  beyond `logical_decoding_work_mem` (64 MB default) **[doc]**, then streams it in one piece after COMMIT. In the JVM the
  `ChangeEventQueue` bounds memory (`max.queue.size`, `max.queue.size.in.bytes`) **[src]**. Risks: (a) our applier
  buffers the transaction in memory; (b) a destination commit of 10M rows exceeds a MotherDuck limit (PLAN §2 A 3);
  (c) everything committed after the big transaction waits behind it (head-of-line latency); (d) spill disk on the
  replica (H-049); (e) a restart in the middle replays the whole transaction (H-025).
- **Reproduction.** S0, then on P: `INSERT INTO cdc_v2_t SELECT g, repeat('x', 100) FROM generate_series(1, 10000000) g;`
  — on R: `SELECT * FROM pg_stat_replication_slots;` shows `spill_txns`/`spill_bytes` growing; Debezium receives the
  first event only after the COMMIT.
- **Prevention.** Design rules: the applier stages each source transaction to a bounded-memory spill area (SPEC 5.4)
  and commits it as one destination transaction; destination staging is sized from the P2 measurement of MotherDuck's
  transaction limits (if a single destination transaction cannot hold 10M rows, stage into a staging table across
  several statements and publish with one atomic swap or `INSERT … SELECT` inside one transaction); the JVM queue has a
  byte bound; the applier never acknowledges until the END record's `event_count` matches what it staged.
- **Why prevention.** Memory and atomicity are bounded by construction for any transaction size, rather than by
  recovering from an OOM.
- **Test.** `test_h001_10m_insert`, `…_10m_update`, `…_10m_delete` (each one transaction) with a concurrent invariant
  checker (SPEC 1.3) and peak-RSS assertion (SPEC 5.4); SIGKILL at 25/50/75 % of the stream (H-025).
- **Residual.** Head-of-line latency is inherent to protocol 1: rows committed right after a 10M-row transaction become
  visible only after it is applied (minutes at 100K rows/s). See finding F-3 in the summary.

### H-002 Oversized rows and values (the 1 GB limits)

- **What.** A single value or row whose *text* representation exceeds PostgreSQL's 1 GB allocation limit
  (`MaxAllocSize`), or that is very large for the JVM.
- **Mechanism.** pgoutput sends values in text form (`OutputFunctionCall` in `logicalrep_write_tuple`,
  `pg:src/backend/replication/logical/proto.c`) **[src]**. `byteaout` allocates `2 × size + 3` bytes for hex output
  (`pg:src/backend/utils/adt/bytea.c`) **[src]**, so any `bytea` value over ~512 MB fails to encode with "invalid memory
  alloc request size"; a message whose total size exceeds 1 GB fails in `enlargeStringInfo` ("string buffer exceeds
  maximum allowed length", `pg:src/common/stringinfo.c`) **[src]**. Both are walsender ERRORs at the same WAL position
  on every retry: the slot can never pass that transaction (a poison pill). On the JVM side `readColumnValueAsString`
  materialises each value as `byte[]` plus `String` (`PgOutputMessageDecoder`) **[src]**: a 300 MB text value costs
  ≥ 600 MB of heap.
- **Reproduction.** S0 + `ALTER TABLE cdc_v2_t ADD COLUMN b bytea;` on P:
  `INSERT INTO cdc_v2_t (id, b) SELECT 1, decode(repeat('ab', 600*1024*1024), 'hex');` (600 MB) — on R the peek above
  fails with "invalid memory alloc request size" **[test]**. Variant: two 600 MB `text` columns in one row.
- **Prevention.** (1) Guard on discovery and in the periodic catalog poll on the replica: per streamed table, an
  estimate of the maximum value size (`max(pg_column_size(col))` sampled during backfill, plus `pg_stats.avg_width`)
  and an alert when any value exceeds 256 MB; (2) JVM heap and `max.queue.size.in.bytes` sized so one 256 MB value
  fits; (3) the poison-pill rule of SPEC 4.3: three failures of the walsender at the same `confirmed_flush_lsn` ⇒ advance
  the slot to the replica's replay position and backfill every streamed table through the normal mechanism (the
  skipped range is unknown, so all tables are affected); backfill reads values with binary `COPY`, which has no hex
  doubling.
- **Why prevention.** We cannot stop the source from storing such a value, so the guard is early detection; the design
  rule guarantees that the pipeline never hangs on it and never acknowledges past it without a backfill.
- **Test.** `test_h002_bytea_600mb` (expect the alert, the skip, the backfill, oracle equality); `test_h002_text_300mb`
  (expect normal streaming within the heap cap).
- **Residual.** Values between 512 MB and 1 GB in `bytea` cannot be streamed by any pgoutput-text consumer; they are
  delivered only through backfill. Binary mode (debezium/dbz#2898, open) would remove the doubling.

### H-049 Decoder spill on the decoding server

*(Numbered with the replica group, §G, because the spill lands on the decoding server.)*

---

## B. TOAST and values

### H-003 Unchanged-TOAST placeholders

- **What.** An `UPDATE` that does not touch a TOASTed column carries no value for it.
- **Mechanism.** pgoutput writes `u` instead of a value when the new tuple holds an on-disk external datum
  ("Unchanged toasted datum … just a cheap check to avoid sending large values unnecessarily", `proto.c`) **[src]**.
  Debezium replaces it with `unavailable.value.placeholder` (default `__debezium_unavailable_value`) as a string,
  bytes, or a one-element list for arrays and hstore (`UnchangedToastedPlaceholder`,
  `PostgresValueConverter`) **[src]**. The placeholder is **in-band**: a real value equal to it is indistinguishable; with
  `REPLICA IDENTITY FULL` Debezium fills the value from the old tuple (`cachedOldToastedValues`) **[src]**. SPEC 2.6
  forbids re-reading from the source and rewriting the destination value. If the destination does not yet have the row
  (update overlapping a backfill), there is nothing to keep.
- **Reproduction.** S0; P: `INSERT INTO cdc_v2_t VALUES (1, repeat(md5(random()::text), 10000)); UPDATE cdc_v2_t SET id =
  id WHERE id = 1;` — peek on R shows `u` for `v` in the UPDATE. Collision variant: P: `INSERT INTO cdc_v2_t VALUES (2,
  '__debezium_unavailable_value');` (a legitimate value equal to the default placeholder).
- **Prevention.** (1) `unavailable.value.placeholder` set to a byte string that contains `0x00`
  (`hex:006364635f76325f746f61737400`): PostgreSQL `text`, `varchar`, `json`, `jsonb`, `xml` cannot contain NUL
  **[doc]** (<https://www.postgresql.org/docs/18/datatype-character.html>: "the character with code zero … cannot be
  stored"), so for every text-like type the placeholder is unambiguous; (2) for `bytea` and arrays of non-text types,
  where any byte string is legal, our `CustomConverter` maps the marker object
  (`UnchangedToastedReplicationMessageColumn.UNCHANGED_*`, compared by identity) to an out-of-band representation (a
  per-row "unchanged columns" bitmap in the hand-off); (3) the applier applies `u` columns as "keep destination value"
  with a bounded number of statement shapes (SPEC 2.6); (4) design rule: the snapshot↔stream hand-off guarantees that a
  row exists in the destination before any later streamed `UPDATE` for it is applied; an unchanged-TOAST update that
  finds no row is an invariant violation that triggers a re-backfill of that table, never a guessed value.
- **Why prevention.** The ambiguity is removed by choosing a representation that cannot collide, instead of detecting
  wrong data afterwards.
- **Test.** `test_h003_toast_unchanged` (text, jsonb, bytea, text[], int[], hstore; insert-then-update in one batch;
  consecutive updates; update during a backfill; 100 MB values, SPEC 2.6/5.4); `test_h003_placeholder_collision` (rows
  whose value equals the old default placeholder and the new placeholder's printable part); value fuzzer family "TOAST".
- **Residual.** If the source's own data could contain NUL in text (it cannot), or if a future Debezium changed the
  marker objects, the converter test fails first.

### H-009 Values Debezium cannot decode

- **What.** Columns of types that Debezium's type registry does not know: composite (row) types, custom base types,
  multiranges, `pg_lsn`, `tsquery`, `txid_snapshot`, extension types without a mapping.
- **Mechanism.** `ReplicationMessageColumnValueResolver.resolveValue` falls through to `asDefault`, which returns the
  text when `include.unknown.datatypes=true` and **null** otherwise (`AbstractColumnValue.asDefault`) **[src]** — the
  default silently drops the value (R16 violation).
- **Reproduction.** P: `CREATE TYPE cdc_v2_pair AS (a int, b text); ALTER TABLE cdc_v2_t ADD COLUMN p cdc_v2_pair;
  INSERT INTO cdc_v2_t VALUES (3, 'x', ROW(1,'y'));` — Debezium with defaults emits `p` = null.
- **Prevention.** `include.unknown.datatypes=true` (text delivered); type-mapping table (`docs/type_mapping.md`, SPEC
  2.4) lists every type family with its mapping; discovery guard: a newly discovered column whose type is neither mapped
  nor "text fallback" is still delivered as text and raises an informational alert.
- **Why prevention.** Configuration removes the drop path entirely.
- **Test.** `test_h009_unknown_types` (composite, nested composite, domain over composite, multirange, pg_lsn,
  tsquery, PostGIS if present); value fuzzer family "unknown".
- **Residual.** None known; the text form is lossless by definition.

### H-010 Numeric special values and precision

- **What.** `numeric` NaN, ±Infinity (14+), precision > 38, unconstrained scale (`1.50`), very long values.
- **Mechanism.** `decimal.handling.mode=precise` (default) throws `ConnectException` on NaN/±Infinity
  (`SpecialValueDecimal.fromLogical`: "current mode does not handle it") — the connector **fails** — and strips trailing
  zeros of unconstrained numerics (`stripTrailingZeros`) **[src]**.
- **Reproduction.** P: `CREATE TABLE cdc_v2_n (id int PRIMARY KEY, n numeric); ALTER PUBLICATION cdc_v2_pub ADD TABLE
  cdc_v2_n; INSERT INTO cdc_v2_n VALUES (1,'NaN'),(2,'Infinity'),(3,'1.50'),(4, 1e300::numeric * 1e300);`.
- **Prevention.** `decimal.handling.mode=string` (text of the exact value); the applier implements SPEC 2.4/D10
  (`DECIMAL(38,s)` while values fit, `UNION(DECIMAL, VARCHAR)` otherwise).
- **Why prevention.** No value reaches a lossy or failing converter.
- **Test.** `test_h010_numeric_specials`; value fuzzer family "numeric" (≥ 1M values: boundaries, NaN, ±Inf, scales
  0–1000, 131072 digits).
- **Residual.** None in the stream; destination mapping is SPEC 2.4's subject.

### H-011 Temporal extremes

- **What.** `date`/`timestamp`/`timestamptz` ±infinity, BC dates, years > 9999 and up to 294276 AD, `time '24:00:00'`,
  `timetz` offsets, `interval` with months/days/µs.
- **Mechanism.** Epoch-based modes map ±infinity to finite sentinels and overflow for extreme years (`nanoseconds`
  throws outside 1677–2262) **[doc]** (connector docs); BC parsing failed before 3.7.0 (debezium/dbz#1340, #2664)
  **[doc]**; `interval.handling.mode=numeric` uses an average month length (lossy) **[doc]**. Text output depends on the
  session's `DateStyle`/`IntervalStyle` (H-013).
- **Reproduction.** P: `CREATE TABLE cdc_v2_d (id int PRIMARY KEY, d date, ts timestamp, tz timestamptz, t time, i
  interval); ALTER PUBLICATION cdc_v2_pub ADD TABLE cdc_v2_d; INSERT INTO cdc_v2_d VALUES (1,'infinity','-infinity',
  'infinity','24:00:00','1 mon 1 day 00:00:00.000001'), (2,'4713-01-01 BC','294276-12-31 23:59:59.999999',
  '0001-01-01 00:00:00+14 BC','00:00:00','-178000000 years');`.
- **Prevention.** `time.precision.mode=structured` (components + `specialValue`, documented to preserve values beyond
  the epoch-microsecond range with pgoutput) and `interval.handling.mode=string`; walsender session GUCs pinned (H-013).
- **Why prevention.** The representation carries every component; no conversion can overflow.
- **Test.** `test_h011_temporal_extremes` (each value above, round-trip equality under `docs/type_mapping.md`); value
  fuzzer family "temporal".
- **Residual.** BC year encoding and `24:00:00` inside `structured` are **[test]**; if either fails, a custom converter
  emitting text is the fallback.

### H-012 Other lossy or failing value mappings

- **What.** Types whose default Debezium mapping loses information or throws.
- **Mechanism.** (a) `varbit`: bytes padded to the declared length lose the per-value bit length; `varbit(1)` value
  `B''` makes `Integer.valueOf("", 2)` throw (`PostgresValueConverter.convertBits`) **[src]**. (b) Arrays: the Connect
  ARRAY built from `PgArray` cannot carry non-default lower bounds or multiple dimensions **[test]**. (c) `money`: parsed
  by stripping one leading symbol and `,` (`PostgresMoney.parse`), which throws or mis-parses for locales with `,`
  decimals or suffix symbols **[src]**. (d) large objects: only the `oid` is in the table; content is never in the
  logical stream **[doc]**. (e) `hstore` in `map` mode loses nothing but is an unordered map; `json` mode is text.
- **Reproduction.** P: `CREATE TABLE cdc_v2_x (id int PRIMARY KEY, vb varbit, vb1 varbit(1), a int[], m money);
  ALTER PUBLICATION cdc_v2_pub ADD TABLE cdc_v2_x; INSERT INTO cdc_v2_x VALUES (1, B'0101', B'', '[0:2]={1,2,3}',
  '1234.56'), (2, B'101', B'1', '{{1,2},{3,4}}', '-0.01');` then on P `SET lc_monetary = 'de_DE.UTF-8';` is irrelevant
  for the walsender — set it server-wide or for the replication role to reproduce (c).
- **Prevention.** Our `CustomConverter` for `bit`, `varbit`, every array type, and `money`, emitting the PostgreSQL text
  form (raw values reach the converter: text for bit strings, `PgArray` whose text form is the original literal
  **[test]**, money text); the applier parses text into native types per `docs/type_mapping.md`; large objects documented
  as out of scope (their `oid` values are delivered).
- **Why prevention.** The lossy converters are never invoked for these columns.
- **Test.** `test_h012_varbit`, `…_array_bounds_and_dims`, `…_money_locales` (`lc_monetary` = `C`, `en_US`, `de_DE`,
  `ja_JP` for the replication role); value fuzzer families "bit", "array", "money".
- **Residual.** A converter bug is a correctness bug in our code; covered by the value fuzzer (≥ 1M values per family).

### H-013 Encoding and session-dependent text output

- **What.** pgoutput emits text produced by output functions **in the walsender's session**, under the server
  encoding; Debezium decodes it as UTF-8 and parses some types with fixed formats.
- **Mechanism.** `readColumnValueAsString` decodes with UTF-8 unconditionally (`PgOutputMessageDecoder`) **[src]**: a
  `SQL_ASCII` or `LATIN1` database yields replacement characters or mojibake. Output of `money` depends on
  `lc_monetary`, of timestamps on `DateStyle`/`TimeZone`, of intervals on `IntervalStyle`, of floats on
  `extra_float_digits`, of `bytea` on `bytea_output` **[doc]** (<https://www.postgresql.org/docs/18/runtime-config-client.html>).
  pgjdbc sets `DateStyle=ISO`, `extra_float_digits=3`, `client_encoding=UTF8` and `TimeZone` in its start-up packet
  **[doc]** (pgjdbc docs), **[test]** for the replication connection; `lc_monetary`, `IntervalStyle` and `bytea_output`
  are inherited from role/database/server settings.
- **Reproduction.** P: `ALTER ROLE <runtime role> SET intervalstyle = 'sql_standard';` then insert an interval; or
  `ALTER ROLE <runtime role> SET bytea_output = 'escape';` then insert bytea; observe Debezium's values.
- **Prevention.** Guard at every connection: `SHOW server_encoding` must be `UTF8` (otherwise the database is refused
  for streaming *with an explanation*, and its tables use full-refresh mode, whose reads convert server-side) —
  **[test]** whether PlanetScale databases are always UTF8; pin session GUCs for the replication connection through
  pgjdbc's `options` property (`-c lc_monetary=C -c IntervalStyle=postgres -c bytea_output=hex -c DateStyle=ISO
  -c extra_float_digits=3`), passed through Debezium's `driver.` pass-through prefix **[test]**; verify the effective
  values on the replication connection (`SHOW` over a normal connection with the same options).
- **Why prevention.** The text format is fixed before the first value is produced.
- **Test.** `test_h013_session_gucs` (role-level overrides of every listed GUC must not change delivered values);
  `test_h013_non_utf8_database_refused`.
- **Residual.** A non-UTF8 source database cannot be streamed losslessly by Debezium; finding F-5.

---

## C. Schema changes

### H-004 Add and drop column mid-stream; `ADD COLUMN … DEFAULT`

- **What.** Columns appear or disappear while changes stream; existing rows of an added column with a default.
- **Mechanism.** A Relation message precedes the first change after the DDL; no message for the DDL itself **[doc]**.
  `ADD COLUMN … DEFAULT <constant>` rewrites nothing and emits no row changes (the default lives in the catalog,
  `atthasmissing`) **[doc]** (<https://www.postgresql.org/docs/18/sql-altertable.html>); `ADD COLUMN … DEFAULT
  <volatile>` (e.g. `clock_timestamp()`, `gen_random_uuid()`) **rewrites the table**, and changes made by rewrites are
  not decoded (`relrewrite` filter in `ReorderBufferProcessTXN`, `pg:src/backend/replication/logical/reorderbuffer.c`)
  **[src]** — every existing row gets a different value that never reaches the stream.
- **Reproduction.** S0, rows present; P: `ALTER TABLE cdc_v2_t ADD COLUMN c1 int DEFAULT 7;` then `ALTER TABLE cdc_v2_t
  ADD COLUMN c2 uuid DEFAULT gen_random_uuid();` then `UPDATE cdc_v2_t SET v = v WHERE id = 1;` — peek shows Relation
  messages and one UPDATE; no rows for existing ids.
- **Prevention.** Catalog fence (design rule, defined under H-008) gives the applier the column's default expression and
  volatility (`pg_attrdef`, `pg_attribute.atthasmissing`/`attmissingval`) as of the DDL; constant defaults are applied to
  existing destination rows in the same destination transaction as the schema change (SPEC 2.1); a volatile default or
  any change flagged as a rewrite (`pg_class.relfilenode` changed between two fence observations) schedules an automatic
  backfill of that table before its next rows are made visible.
- **Why prevention.** The applier never shows a destination table whose existing rows lack values the source has.
- **Test.** `test_h004_add_drop_column` (constant default, NULL default, volatile default, drop with data, drop and
  re-add same name), oracle after each step.
- **Residual.** Between the DDL and the end of the backfill the table is served from its last complete version (SPEC 3.2).

### H-005 Column type change mid-stream (and `USING` rewrites)

- **What.** `ALTER TABLE … ALTER COLUMN … TYPE …`, possibly with `USING expr`.
- **Mechanism.** A Relation message with the new type OID/typmod precedes later changes **[doc]**. A type change that
  rewrites the table changes every stored value without decoded row changes (`relrewrite` filter) **[src]**; with
  `USING` the new values are arbitrary (`USING upper(v)`), and even a plain cast can change the text (`numeric(10,2)`
  → `numeric(10,0)` rounds).
- **Reproduction.** S0; P: `ALTER TABLE cdc_v2_t ALTER COLUMN v TYPE varchar(10) USING upper(left(v,10));` — no row
  events; destination keeps old values.
- **Prevention.** Rule: a type change whose fence observation shows a new `relfilenode` (rewrite) or a narrowing/lossy
  cast triggers a backfill of the table; a type change without rewrite (e.g. `varchar(n)` → `text`, `varchar(n)` →
  `varchar(m>n)`) is a metadata-only widening and follows SPEC 2.5 (in-place or UNION) with no backfill; after any type
  change a per-column checksum (replica vs destination, on the replica, in chunks) confirms equality.
- **Why prevention.** No stale values become visible as current after a rewrite.
- **Test.** `test_h005_type_changes` (SPEC 2.5 list plus `USING` rewrite and rounding narrowing).
- **Residual.** Backfill cost for rewriting type changes on very large tables.

### H-006 DDL and DML in one transaction; out-of-band catalog lookups

- **What.** A transaction alters a table and writes to it; or the catalog changes again before Debezium processes an
  earlier Relation message.
- **Mechanism.** The stream is correct (Relation message mid-transaction before the affected DML) **[doc]**, but
  Debezium enriches each Relation message with JDBC queries of the **current** catalog — defaults, nullability, type
  names, primary key (`handleRelationMessage`, comment about columns "from the future") **[src]**. When decoding lags
  (replay lag, backlog), these attributes describe a later schema; a table dropped meanwhile yields no columns from the
  queries and Debezium falls back to "optional" for every column. The primary key used for Debezium's record key may be
  a future one.
- **Reproduction.** S0; stop the pipeline; P: `BEGIN; ALTER TABLE cdc_v2_t ADD COLUMN k int; UPDATE cdc_v2_t SET k = 1;
  COMMIT; ALTER TABLE cdc_v2_t DROP CONSTRAINT cdc_v2_t_pkey; ALTER TABLE cdc_v2_t ADD PRIMARY KEY (k, id);` — restart
  the pipeline; Debezium decodes the first transaction with the later primary key.
- **Prevention.** Design rule: the applier never uses Debezium's record key, defaults or nullability; it uses the column
  list and values of each record, and takes keys, defaults and replica identity from the catalog fence (H-008), which is
  bracketed by WAL positions; DDL-only facts (key changes, new defaults) are applied at their fenced position.
- **Why prevention.** The only metadata the applier trusts is metadata tied to a WAL position.
- **Test.** `test_h006_ddl_dml_same_tx`, `test_h006_lagging_relation_lookup` (DDL sequence applied while the pipeline is
  stopped, then resumed).
- **Residual.** Debezium's schema for a record may still carry wrong optionality flags; harmless because unused.

### H-007 Drop and recreate with a new OID; table rename

- **What.** `DROP TABLE t; CREATE TABLE t (…)` (same name, new OID) or `ALTER TABLE t RENAME TO u`.
- **Mechanism.** DDL is not decoded; the recreated table's first change brings a Relation message with a new relation
  id and the same name; Debezium records carry schema and table **names** only (no OID) **[src]**. The destination,
  keyed by name, would keep the old table's rows and add the new ones. A rename produces Relation messages with the new
  name and no other signal.
- **Reproduction.** S0, rows present; P: `DROP TABLE cdc_v2_t; CREATE TABLE cdc_v2_t (id bigint PRIMARY KEY, v text);
  ALTER PUBLICATION cdc_v2_pub ADD TABLE cdc_v2_t; INSERT INTO cdc_v2_t VALUES (100, 'new');` — Debezium emits one
  insert for `public.cdc_v2_t`; the old rows are still in the destination.
- **Prevention.** Catalog fence (H-008): table identity is `(oid)`, not name; a name whose OID changed between two fence
  observations is a drop+recreate: the destination table is replaced (SPEC 1.5: fresh table, or `retain`) and the new
  table goes through the per-table snapshot mechanism; changes for that name inside the ambiguous window are held, not
  applied. A rename (same OID, new name) renames the destination table in place (SPEC 2.2).
- **Why prevention.** No change is applied to a destination table until its source identity at that WAL position is
  known.
- **Test.** `test_h007_drop_recreate` (with and without rows in between, inside one transaction, within one fence
  interval), `test_h007_rename_table`, `test_h007_rename_swap` (a↔b in one transaction).
- **Residual.** Changes to a recreated table become visible after its snapshot completes (SPEC 1.3 table-status rule).

### H-008 Rename versus drop+add ambiguity (the catalog fence)

- **What.** `ALTER TABLE t RENAME COLUMN a TO b` and `ALTER TABLE t DROP COLUMN a, ADD COLUMN b <same type>` produce the
  same Relation message (names and types only, no attnum) **[doc]** (message formats).
- **Mechanism.** Without attnums the applier must guess; a wrong guess either drops data (treating rename as drop+add)
  or keeps data in the wrong column (treating drop+add as rename).
- **Reproduction.** S0; P: `ALTER TABLE cdc_v2_t RENAME COLUMN v TO w; UPDATE cdc_v2_t SET w = w;` vs `ALTER TABLE
  cdc_v2_t DROP COLUMN v, ADD COLUMN w text; UPDATE cdc_v2_t SET w = 'z';` — identical Relation messages.
- **Prevention — the catalog fence (design rule).** A catalog poller on the replica records, for every streamed table,
  `(oid, relname, nspname, relfilenode, relreplident)` and `(attnum, attname, atttypid, atttypmod, attisdropped,
  attgenerated, default)` together with a WAL bracket: read `b = pg_last_wal_replay_lsn()`, then in one
  `REPEATABLE READ` transaction read the catalog and `a = pg_last_wal_replay_lsn()`; the snapshot then lies in `[b, a]`
  (a standby snapshot reflects exactly the commits replayed before it was taken) **[test]**. Dropped columns stay in
  `pg_attribute` with `attisdropped` and their attnum forever **[doc]** (<https://www.postgresql.org/docs/18/catalog-pg-attribute.html>),
  so a rename keeps the attnum while drop+add allocates a new one. The applier applies changes for a table at LSN `x`
  only when it has observations on both sides of `x`; if the catalog did not change between them, `x` is unambiguous; if
  it did, Relation messages inside the window are mapped by attnum (unique mapping ⇒ apply; non-unique ⇒ backfill the
  table). Poll interval 2–5 s on the replica (one query over `pg_class`/`pg_attribute` for streamed tables).
- **Why prevention.** Ambiguity is resolved from WAL-positioned facts before applying, never guessed.
- **Test.** `test_h008_rename_vs_dropadd` (both sequences, each inside one fence interval and across intervals; rename
  chains a→b, b→a; swap of two names in one transaction); workload fuzzer DDL mix.
- **Residual.** Adds up to one poll interval of latency to changes that follow a DDL (not to ordinary changes, whose
  tables' catalogs are unchanged). Needs the WAL-bracket property confirmed by test.

### H-032 Debezium refuses a table: column names that differ only in case

- **What.** A table with columns `"A"` and `"a"`.
- **Mechanism.** `handleRelationMessage` throws `DebeziumException("Table '…' has columns that differ only by case …
  Debezium does not support case-sensitive duplicate column names")` **[src]** — the connector task fails and keeps
  failing at that Relation message (a refusal, R16).
- **Reproduction.** P: `CREATE TABLE cdc_v2_case ("A" int, "a" int, id int PRIMARY KEY); ALTER PUBLICATION cdc_v2_pub
  ADD TABLE cdc_v2_case; INSERT INTO cdc_v2_case VALUES (1,2,3);`.
- **Prevention.** Discovery guard: never add such a table to the publication; serve it in `full_refresh` mode (SPEC 3.5,
  reads from the replica with quoted identifiers), alert once, and re-check on every catalog poll (an `ALTER TABLE
  RENAME COLUMN` can create the collision later: the fence sees it before the next change, the table is removed from
  the publication — `ALTER PUBLICATION … DROP TABLE` on the primary — and switched to `full_refresh`).
- **Why prevention.** The Relation message that kills Debezium is never produced for our slot.
- **Test.** `test_h032_case_collision_discovery`, `test_h032_case_collision_by_rename`.
- **Residual.** A race between the rename and the next change inside one fence interval still reaches Debezium; then
  the SPEC 4.3 last line applies (skip + backfill + 4.0). Finding F-6.

### H-033 Enum and type DDL

- **What.** Enum types recreated, values added, or new types created while streaming.
- **Mechanism.** debezium/dbz#2521 (open): an enum recreated (rename/create/alter/drop) causes an NPE in `TypeRegistry`
  that "kills the connector task permanently"; debezium/dbz#304 (open): schema not updated after `ALTER TYPE … ADD
  VALUE` **[doc]**. Type ('Y') messages are ignored; Debezium relies on its own JDBC type registry **[src]**.
- **Reproduction.** P: `CREATE TYPE cdc_v2_e AS ENUM ('a'); ALTER TABLE cdc_v2_t ADD COLUMN e cdc_v2_e; INSERT …;
  ALTER TYPE cdc_v2_e RENAME TO cdc_v2_e_old; CREATE TYPE cdc_v2_e AS ENUM ('a','b'); ALTER TABLE cdc_v2_t ALTER COLUMN
  e TYPE cdc_v2_e USING e::text::cdc_v2_e; INSERT …;`.
- **Prevention.** Enum values are delivered as text (`include.unknown.datatypes` irrelevant; enums resolve to string);
  the catalog fence detects type OID changes of streamed columns; on such a change the supervisor restarts the Debezium
  child (fresh `TypeRegistry`) at a transaction boundary before the next change of that table is decoded **[test]**
  whether a restart avoids the NPE.
- **Why prevention.** The stale registry entry that causes the NPE never meets a record.
- **Test.** `test_h033_enum_recreate`, `test_h033_enum_add_value`, `test_h033_new_domain_type`.
- **Residual.** If the restart does not avoid the NPE, the SPEC 4.3 last line applies; report upstream.

---

## D. Replica identity, keys, truncate, messages, transactions

### H-014 Replica identity missing: the publication breaks the source's writes

- **What.** Tables with `REPLICA IDENTITY NOTHING`, `DEFAULT` without a primary key, or `USING INDEX` whose index was
  dropped.
- **Mechanism.** "Tables with a replica identity defined as NOTHING, DEFAULT without a primary key, or USING INDEX with
  a dropped index, cannot support UPDATE or DELETE operations when included in a publication replicating these
  actions. Attempting such operations will result in an error on the publisher." **[doc]**
  (<https://www.postgresql.org/docs/18/logical-replication-publication.html>). Automatic discovery (R20) that adds such a
  table to our publication would make the **application's** updates and deletes fail on the primary.
- **Reproduction.** P: `CREATE TABLE cdc_v2_nokey (a int); INSERT INTO cdc_v2_nokey VALUES (1); ALTER PUBLICATION
  cdc_v2_pub ADD TABLE cdc_v2_nokey; UPDATE cdc_v2_nokey SET a = 2;` → ERROR "cannot update table … because it does not
  have a replica identity and publishes updates".
- **Prevention.** Discovery guard before `ADD TABLE`: if `relreplident` is `n`, or `d` without a primary key, or `i`
  with a missing index, first run `ALTER TABLE … REPLICA IDENTITY FULL` (SPEC 1.2 requires FULL for keyless tables;
  needs ownership, **[test]** on PlanetScale), and add the table only after that commits; the fence re-checks on every
  poll and, if identity is later weakened (index dropped, `REPLICA IDENTITY NOTHING`), immediately sets FULL again or
  removes the table from the publication and switches it to `full_refresh`, alerting.
- **Why prevention.** Our publication never contains a table for which the source's own writes would fail.
- **Test.** `test_h014_keyless_discovery` (application UPDATE/DELETE keep working), `test_h014_identity_index_dropped`.
- **Related source-side effects of publication membership** (same guard): a published table cannot be switched to
  `UNLOGGED` ("cannot change table … to unlogged because it is part of a publication", `ATPrepChangePersistence`,
  `pg:src/backend/commands/tablecmds.c`) and `ALTER TABLE … ALTER COLUMN … SET EXPRESSION` is refused for virtual
  generated columns of published tables (same file) **[src]**. Adding a table to our publication can therefore make an
  application's DDL fail; the operator documentation must say so, and `discovery.auto_replicate: false` (SPEC 2.3) is
  the switch for databases where that is unacceptable.
- **Residual.** A window between an index drop and our next poll in which the application's updates fail (seconds);
  only the operator's own DDL can open it. Finding F-7.

### H-015 Primary-key changes and duplicate rows under FULL

- **What.** `UPDATE` changing key columns (chains, swaps inside one transaction); identical rows in keyless tables.
- **Mechanism.** DEFAULT identity: Debezium turns a key change into `d` (old key) + tombstone + `c` (new row)
  (`PostgresChangeRecordEmitter`) **[src]**; swaps `A→B, B→A` in one transaction pass through an intermediate state that
  violates uniqueness if applied statement by statement. FULL identity on a keyless table: a delete of one of several
  identical rows carries the whole old row; applying it as `DELETE … WHERE all columns =` deletes all copies.
- **Reproduction.** P: `BEGIN; UPDATE cdc_v2_t SET id = -id WHERE id IN (1,2); UPDATE cdc_v2_t SET id = 2 WHERE id = -1;
  UPDATE cdc_v2_t SET id = 1 WHERE id = -2; COMMIT;`; keyless: `CREATE TABLE cdc_v2_dup (a int) ; ALTER TABLE cdc_v2_dup
  REPLICA IDENTITY FULL; INSERT INTO cdc_v2_dup VALUES (1),(1),(1); DELETE FROM cdc_v2_dup WHERE ctid = (SELECT ctid FROM
  cdc_v2_dup LIMIT 1);`.
- **Prevention.** Design rules for the applier: per transaction, collapse changes per key in order (net effect) before
  applying set-based statements, so intermediate uniqueness states never reach the destination; keyless tables carry a
  per-row multiplicity (or a synthetic row id) and a delete removes exactly one occurrence (SPEC 1.2).
- **Why prevention.** The destination never executes the intermediate states.
- **Test.** `test_h015_key_chains_swaps` (DEFAULT and FULL identity, history mode on), `test_h015_duplicate_rows`.
- **Residual.** None known.

### H-016 TRUNCATE variants (and Debezium's default that drops them)

- **What.** `TRUNCATE a, b`, `TRUNCATE … CASCADE`, `RESTART IDENTITY`, truncate of a partition, truncate inside a
  transaction with other changes.
- **Mechanism.** Debezium's `skipped.operations` default is `"t"` — **truncates are dropped unless configured**
  (`CommonConnectorConfig.SKIPPED_OPERATIONS`) **[src]**. When enabled, one `t` record per resolved table; flags dropped
  **[src]**. A truncate of a partition is not replicated when the publication uses `publish_via_partition_root = true`
  **[doc]**. Tables not in the publication but truncated by `CASCADE` are not sent.
- **Reproduction.** S0 + `CREATE TABLE cdc_v2_child (id int REFERENCES cdc_v2_t(id)); ALTER PUBLICATION cdc_v2_pub ADD
  TABLE cdc_v2_child;` then P: `BEGIN; INSERT INTO cdc_v2_t VALUES (9,'a'); TRUNCATE cdc_v2_t CASCADE; INSERT INTO
  cdc_v2_t VALUES (10,'b'); COMMIT;`.
- **Prevention.** `skipped.operations=none`; `publish_via_partition_root = false` in our publication (H-021); the applier
  applies truncates in commit order inside the transaction's destination commit (SPEC 1.5); a configuration self-check
  at start-up asserts the effective Debezium configuration (truncate not skipped).
- **Why prevention.** Configuration removes the drop; the start-up assertion prevents a regression.
- **Test.** `test_h016_truncate_variants` (multi-table, CASCADE, RESTART IDENTITY, inside a transaction with
  inserts before and after, soft-delete mode SPEC 8.1).
- **Residual.** None known.

### H-017 Logical messages

- **What.** `pg_logical_emit_message(transactional, prefix, content)` — transactional (inside a transaction, may roll
  back) and non-transactional ("written immediately and decoded as soon as the logical decoder reads the record")
  **[doc]** (<https://www.postgresql.org/docs/18/functions-admin.html>).
- **Mechanism.** Debezium requests `messages=true` (14+) and emits `m` records; a transactional message sits between the
  transaction's BEGIN and END; a non-transactional one is its own commit point (`commitMessage`) and is delivered even
  if the emitting transaction later aborts **[src]**. Restart bugs exist when the stored offset points at a
  transactional message (debezium/dbz#2058 fixed 3.6.0.Beta2, #2889 fixed 3.7.1, #2901 open) **[doc]**. Content can be
  up to 1 GB (H-002). Messages with prefixes not in `message.prefix.include.list` are filtered but still advance the
  stream (debezium/dbz#2733: no warning about WAL backlog for filtered messages) **[doc]**.
- **Reproduction.** S0; P: `BEGIN; INSERT INTO cdc_v2_t VALUES (20,'m'); SELECT pg_logical_emit_message(true,
  'app_evt', 'in-tx'); SELECT pg_logical_emit_message(false, 'app_evt', 'non-tx'); ROLLBACK;` — peek shows only the
  non-transactional message.
- **Prevention.** Memory offset store (no restart from a stored offset at a message, H-027); the applier writes
  transactional messages in the same destination transaction as their source transaction and non-transactional ones
  in stream order (SPEC 7.4); message content flows through the same size guard as values (H-002); document that a
  non-transactional message of an aborted transaction is delivered (PostgreSQL semantics, not an error).
- **Why prevention.** The restart path with the known bugs is never taken; atomicity follows from the transaction
  grouping that the applier already does.
- **Test.** `test_h017_messages` (transactional committed/rolled back, non-transactional inside aborted transaction,
  prefix filter, 100 MB content, SIGKILL right after a message).
- **Residual.** None known.

### H-018 Origin messages

- **What.** Transactions replayed by a logical-replication subscriber *on the source*, or sessions that set a
  replication origin, carry an origin.
- **Mechanism.** pgoutput sends an Origin message before such transactions when the client does not filter origins; the
  `origin` option (`none`/`any`) is a subscriber-side filter that Debezium does not send, so the default `any` applies
  and all transactions are delivered; Debezium records origin name/LSN in the source info **[src]**.
- **Reproduction.** On P: `SELECT pg_replication_origin_create('cdc_v2_o'); SELECT pg_replication_origin_session_setup(
  'cdc_v2_o'); INSERT INTO cdc_v2_t VALUES (30,'o');` (superuser on T1) — peek shows `O` before the INSERT.
- **Prevention.** Configuration rule: never pass `origin=none` (`slot.stream.params` stays empty); start-up assertion of
  the effective stream parameters.
- **Why prevention.** Origin-tagged changes are never filtered out.
- **Test.** `test_h018_origin_tagged_changes_delivered`.
- **Residual.** None.

### H-019 Two-phase (prepared) transactions

- **What.** `PREPARE TRANSACTION` / `COMMIT PREPARED` / `ROLLBACK PREPARED` on the primary.
- **Mechanism.** Without the `two_phase` option a prepared transaction is decoded at `COMMIT PREPARED` as a normal
  transaction; `ROLLBACK PREPARED` produces nothing **[doc]**
  (<https://www.postgresql.org/docs/18/logicaldecoding-two-phase-commits.html>). A prepared transaction that stays open
  holds back the slot's `restart_lsn` and `catalog_xmin` like any running transaction (H-031) and blocks logical slot
  creation (H-040) **[src]** (snapbuild.c).
- **Reproduction.** P (`max_prepared_transactions > 0`): `BEGIN; INSERT INTO cdc_v2_t VALUES (40,'p'); PREPARE
  TRANSACTION 'cdc_v2_gx';` — nothing in peek; `COMMIT PREPARED 'cdc_v2_gx';` — the transaction appears.
- **Prevention.** Configuration: no `two_phase`; guard: the periodic check lists `pg_prepared_xacts` on the primary and
  alerts on any prepared transaction older than 10 minutes (it pins WAL and blocks slot creation).
- **Why prevention.** Prepared transactions are delivered exactly like ordinary ones; their side effects are detected
  before WAL retention or slot creation suffers.
- **Test.** `test_h019_prepared_commit_and_rollback`, `test_h019_prepared_blocks_slot_creation` (with H-040).
- **Residual.** An abandoned prepared transaction is an operator problem we can only alert on.

### H-020 Streamed in-progress transactions

- **What.** Stream Start/Stop/Commit/Abort and Stream Prepare messages (protocol ≥ 2/4).
- **Mechanism.** Sent only when the client requests `streaming` with a sufficient `proto_version`
  (`pgoutput_startup`) **[src]**; Debezium 3.7.1 hard-codes protocol 1 **[src]**, so they never arrive; an unexpected
  message type would throw in `MessageType.forType` **[src]**.
- **Reproduction.** Not reachable with stock Debezium 3.7.1; with `pg_recvlogical -o proto_version=2 -o streaming=on` one
  sees the messages a future version would receive.
- **Prevention.** Version pin (debezium.md §1.2) and a pin-upgrade checklist item: any Debezium release that changes
  `proto_version` or adds `streaming` is a correctness change (SPEC §4 counters restart).
- **Why prevention.** The message classes cannot appear while the pin holds.
- **Test.** `test_h020_stream_options_pinned` (asserts the options in `pg_stat_replication`/server log on connect).
- **Residual.** None while pinned; the cost is H-001's head-of-line latency and H-049's spill.

### H-021 Partitions: attach, detach, drop, `publish_via_partition_root`

- **What.** Partitioned tables whose partitions are attached, detached or dropped while streaming.
- **Mechanism.** With `publish_via_partition_root = true`, changes are published as the root, `TRUNCATE` on a partition
  is not replicated, and attaching a table "does not result in the table's existing contents being replicated"; with
  `false`, leaves are published under their own names **[doc]** (CREATE PUBLICATION). `ATTACH` of a populated table
  emits no row changes; `DETACH` and `DROP` of a partition emit none either (DDL is not decoded).
- **Reproduction.** P: `CREATE TABLE cdc_v2_p (id int, k int) PARTITION BY RANGE (k); CREATE TABLE cdc_v2_p1 PARTITION
  OF cdc_v2_p FOR VALUES FROM (0) TO (10); ALTER PUBLICATION cdc_v2_pub ADD TABLE cdc_v2_p; CREATE TABLE cdc_v2_p2 (id
  int, k int); INSERT INTO cdc_v2_p2 VALUES (1, 15); ALTER TABLE cdc_v2_p ATTACH PARTITION cdc_v2_p2 FOR VALUES FROM (10)
  TO (20); ALTER TABLE cdc_v2_p DETACH PARTITION cdc_v2_p1;` — no row events for p2's existing row or p1's removal.
- **Prevention.** Our publication uses `publish_via_partition_root = false` (leaf names; partition truncates are
  delivered); the combined-table mode of SPEC 7.3 is implemented by the applier (leaf → root mapping), not by the
  publication; the catalog fence tracks `pg_inherits` and `relispartition`: an attach schedules a per-partition snapshot
  into the combined (or per-partition) destination table; a detach or drop removes that partition's rows from the
  combined table at the fenced position (and, for detach, the detached table becomes an ordinary discovered table).
- **Why prevention.** Every partition-membership change is applied at a known WAL position with an explicit snapshot,
  instead of leaving silent gaps.
- **Test.** `test_h021_attach_populated`, `…_detach`, `…_detach_concurrently`, `…_drop_partition`,
  `…_truncate_partition`, `…_default_partition_split`.
- **Residual.** Rows of a newly attached partition appear after its snapshot (SPEC 1.3 table-status rule).

### H-022 Generated and identity columns

- **What.** Stored and virtual generated columns; identity columns.
- **Mechanism.** PostgreSQL 18 makes generated columns **virtual by default**; virtual ones are never published; stored
  ones are published only with `publish_generated_columns = stored` or an explicit column list **[doc]** (18 release
  notes, CREATE PUBLICATION); PostgreSQL 17 never publishes generated columns **[doc]** (17 docs). Their values exist
  only in `SELECT` results, so a backfill sees them and the stream does not. Identity columns are ordinary values.
- **Reproduction.** P: `ALTER TABLE cdc_v2_t ADD COLUMN g1 int GENERATED ALWAYS AS (length(v)) STORED, ADD COLUMN g2 int
  GENERATED ALWAYS AS (id * 2) VIRTUAL; UPDATE cdc_v2_t SET v = 'abc' WHERE id = 1;` — peek shows neither column (default
  publication options).
- **Prevention.** Our publication sets `publish_generated_columns = stored` on 18 (stored values streamed); for virtual
  columns and for stored ones on 17, the applier computes the column in the destination when the generation expression
  is on a whitelist of translatable immutable expressions (catalog `pg_attrdef`, fenced), and otherwise the column is
  refreshed by keyed re-reads from the replica after each applied batch, flagged in the table status (documented
  exception to "no re-read" which SPEC 2.6 states only for TOAST). The choice is made at discovery, before the first row.
- **Why prevention.** Every generated value has a defined source before the table goes live.
- **Test.** `test_h022_stored_generated_pg18`, `…_virtual_generated_translatable`, `…_virtual_generated_reread`,
  `test_h022_identity_columns`.
- **Residual.** The re-read path is eventually consistent per row and can momentarily break cross-table invariants on
  generated columns; finding F-8.

### H-023 Publication column lists and row filters

- **What.** `CREATE PUBLICATION … FOR TABLE t (a, b)` or `… WHERE (expr)`.
- **Mechanism.** debezium/dbz#2375 (open): with a column list, after any schema refresh the decoder binds tuple values
  positionally to the wrong columns until the replication session restarts — wrong values without an error **[doc]**.
  Row filters publish an `UPDATE` that moves a row into the filter as INSERT and out of it as DELETE, and never publish
  filtered rows **[doc]** (<https://www.postgresql.org/docs/18/logical-replication-row-filter.html>).
- **Reproduction.** P: `CREATE PUBLICATION cdc_v2_pub_cl FOR TABLE cdc_v2_t (v, id);` and stream with it after a schema
  refresh (debezium/dbz#2375 recipe).
- **Prevention.** Design rule: our publication never uses column lists or row filters (PII exclusion and masking happen
  in the Flight, SPEC 8.3); the catalog fence checks `pg_publication_rel.prattrs`/`prqual` and `pg_publication` options
  of our publication on every poll and treats any change by a third party as a configuration fault (alert, stop
  streaming before the next change, restore the definition, backfill affected tables).
- **Why prevention.** The defective decoder path is never exercised.
- **Test.** `test_h023_publication_tampering` (column list, row filter, `publish` option changes added externally).
- **Residual.** None while the rule holds.

---

## E. Stream behaviour, restarts and Debezium itself

### H-024 Keepalives and sender timeouts during long applies

- **What.** The destination commit or apply takes minutes; or the walsender spends a long time decoding/skipping
  without sending.
- **Mechanism.** The walsender terminates clients that do not reply for `wal_sender_timeout` (60 s) **[doc]**. Debezium's
  keepalive thread sends status updates every `status.update.interval.ms` independently of the consumer **[src]**, so a
  slow applier does not trigger the timeout; JVM pauses longer than the timeout do. PostgreSQL itself sends keepalives
  while processing large or skipped transactions (`WalSndUpdateProgress`/`update_progress_txn`) **[src]**.
- **Reproduction.** Set `wal_sender_timeout = 10s` on R; make the applier sleep 5 min during a batch; observe no
  disconnect. Then `kill -STOP` the JVM for 20 s; observe "terminating walsender process due to replication timeout".
- **Prevention.** `status.update.interval.ms` ≤ `wal_sender_timeout` / 3 (checked against the server value at start);
  JVM heap and GC settings capped so pauses stay far below the timeout (P9 measurement); supervisor deadlines (SPEC 4.5)
  shorter than any server timeout so that our side restarts first, cleanly.
- **Why prevention.** The configuration makes a timeout impossible while the process is alive.
- **Test.** `test_h024_long_apply_no_timeout`, `test_h024_frozen_jvm_reconnects` (oracle after reconnect).
- **Residual.** Network partitions longer than the timeout always disconnect (correct; H-025 covers the replay).

### H-025 Redelivery after restart (and slot regression after a crash)

- **What.** After any restart the server re-sends transactions we may already have applied.
- **Mechanism.** The server starts at `confirmed_flush_lsn`, which trails our applied position by the acknowledgement
  lag (debezium.md §3.3) and, after a crash of the server holding the slot, may regress to the last
  checkpoint/restartpoint: "in the case of a crash the slot might return to an earlier LSN" **[doc]**. The 32-bit xid
  wraps and is not a key **[src]**.
- **Reproduction.** S0; stream and apply 1000 transactions; `tools/localpg kill` R (immediate shutdown) before its next
  restartpoint; restart; the slot's `confirmed_flush_lsn` is older than what was acknowledged; peek shows already-applied
  transactions.
- **Prevention.** Design rule (SPEC 1.1): the destination stores the applied **commit LSN** in the same transaction as
  the data; the applier discards every transaction whose commit LSN ≤ applied; whole-transaction delivery (protocol 1)
  makes this exact.
- **Why prevention.** Duplicates are recognised before they are applied.
- **Test.** `test_h025_replica_crash_redelivery`, crash-point suite (SPEC 1.7) with ≥ 3 seeds per point.
- **Residual.** None.

### H-026 Empty-transaction floods and unpublished-traffic floods

- **What.** Heavy write traffic on tables outside the publication (or many transactions that change nothing published)
  while published tables are idle.
- **Mechanism.** Since PostgreSQL 15 pgoutput sends nothing for transactions without published changes **[doc]** (15
  release notes); Debezium flushes only LSNs of messages it receives (`lsn.flush.mode=connector`) and warns after
  10 000 filtered events (`maybeWarnAboutGrowingWalBacklog`) **[src]**. The slot's position stalls while WAL grows, on
  the server holding the slot, until the WAL cap invalidates it (H-036).
- **Reproduction.** S0, heartbeat off; P: `CREATE TABLE cdc_v2_other (a int); INSERT INTO cdc_v2_other SELECT
  generate_series(1, 5000000);` repeatedly; R: `SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), restart_lsn) FROM
  pg_replication_slots;` grows.
- **Prevention.** Heartbeat row in the publication, written every 10 s on the primary (SPEC 4.4, R8): every 10 s a
  published transaction arrives, is applied and acknowledged, so the slot advances by construction.
- **Why prevention.** Slot progress never depends on application traffic to published tables.
- **Test.** `test_h026_unpublished_flood_wal_bounded` (SPEC 4.4: 30 minutes, both modes, retained WAL bounded).
- **Residual.** Long-running transactions still pin `restart_lsn` (H-031).

### H-027 Debezium restart-from-stored-offset defects

- **What.** Data loss or OOM when Debezium resumes from a stored offset inside or near a transaction.
- **Mechanism.** `WalPositionLocator`/`lsn_events_processed` defects: debezium/dbz#2889 (fixed 3.7.1), debezium/dbz#2901
  (open, DML lost after restart from a transactional message following COPY), debezium/dbz#2905 (open, unbounded
  `lsnSeen`, OOM on an 8M-row transaction); earlier silent-loss reports DBZ-8312, DBZ-9149 **[doc]**.
- **Reproduction.** As in the issues (FileOffsetBackingStore, stop mid-transaction, restart).
- **Prevention.** `offset.storage=…MemoryOffsetBackingStore`; the slot is the restart point and the destination holds
  the applied position (debezium.md §3.4) **[src]**.
- **Why prevention.** The defective code path is unreachable without a stored offset.
- **Test.** `test_h027_kill_mid_copy_with_message` (the #2901 shape), `test_h027_kill_mid_10m_tx_memory` (the #2905
  shape, heap capped at 512 MB).
- **Residual.** None known.

### H-028 Debezium acknowledging ahead of the durable position

- **What.** The slot is confirmed beyond what the consumer has made durable.
- **Mechanism.** debezium/dbz#2460 (open): `BaseSourceTask.performCommit()` acknowledges the newest in-memory offsets,
  which can be ahead of the persisted offsets; `lsn.flush.mode=connector_and_driver` can flush keepalive LSNs **[doc]**.
- **Reproduction.** As in debezium/dbz#2460 (SIGKILL between offset flush and acknowledgement).
- **Prevention.** Our consumer marks records processed only after the destination commit (debezium.md §3.3);
  `lsn.flush.mode=connector`; continuous invariant probe in tests (`confirmed_flush_lsn` ≤ destination applied commit
  LSN).
- **Why prevention.** `lastOffsets` can never contain a position the destination does not have.
- **Test.** `test_h028_slot_never_ahead_probe` (runs inside every fault-fuzzer seed).
- **Residual.** None.

---

## F. Failover, persistence, slots, publication and environment

### H-029 Timeline change after failover (primary mode)

- **What.** The primary fails over; a standby with a synchronised failover slot is promoted.
- **Mechanism.** Without `synchronized_standby_slots` the consumer can receive transactions the promoted standby never
  got; after promotion the old primary may still be reachable ("the logical subscribers may continue to receive data
  from the old primary server even after promotion") **[doc]** (logicaldecoding-explanation). Only slots with `synced =
  true`, persistent and valid before failover are usable **[doc]**.
- **Reproduction.** T1 with a second standby: create the slot with `failover = true` on P, `sync_replication_slots = on`
  on the standby, stream, `tools/localpg promote` the standby, keep P running.
- **Prevention.** At every acquisition in primary mode: check `pg_is_in_recovery() = false` and the timeline
  (`pg_control_checkpoint().timeline_id`) of the server; record the timeline with the applied position; after a timeline
  change, read the new timeline's history (`TIMELINE_HISTORY` replication command) and continue without backfill only if
  the applied commit LSN ≤ the switch point and the slot is `synced`, valid and not ahead; never write to or read from a
  server whose timeline is older than the recorded one (fencing of the old primary); recommend `synchronized_standby_slots`
  to the provider (PlanetScale: **[test]**).
- **Why prevention.** Continuity is proven from timeline history before any change is applied.
- **Test.** `test_h029_failover_slot_promotion` (with and without `synchronized_standby_slots`; old primary kept alive).
- **Residual.** Without `synchronized_standby_slots` on the provider, a failover can force a backfill.

### H-030 Unlogged and temporary tables, persistence changes

- **What.** Unlogged tables, `ALTER TABLE … SET LOGGED/UNLOGGED`.
- **Mechanism.** Unlogged and temporary tables write no WAL for data and cannot be added to a publication ("This
  operation is not supported for unlogged tables", `pg:src/backend/catalog/pg_publication.c`); a published table cannot be
  set unlogged (`ATPrepChangePersistence`) **[src]**; `SET LOGGED` rewrites the table without decodable row changes
  (H-004 mechanism).
- **Reproduction.** P: `CREATE UNLOGGED TABLE cdc_v2_u (id int PRIMARY KEY); ALTER PUBLICATION cdc_v2_pub ADD TABLE
  cdc_v2_u;` → ERROR.
- **Prevention.** Discovery guard: unlogged tables (`relpersistence = 'u'`) are never added to the publication and are
  served in `full_refresh` mode only if the operator allows reads from the primary — on a standby they cannot be read
  at all ("cannot access temporary or unlogged relations during recovery") **[test]** — otherwise reported as "not
  replicable";
  the fence detects `SET LOGGED` and moves the table to CDC with a snapshot.
- **Why prevention.** No silent gap: such tables are either served by a reading mode or reported.
- **Test.** `test_h030_unlogged_discovery`, `test_h030_set_logged_transition`.
- **Residual.** Unlogged data cannot be read on a replica at all; a documented limitation (finding F-9).

### H-031 Long-running transactions pin the slot

- **What.** A transaction on the primary that stays open for hours (idle in transaction, a long batch, a forgotten
  prepared transaction).
- **Mechanism.** A logical slot's `restart_lsn` cannot pass the start of the oldest transaction still running when
  decoding reaches it, and its `catalog_xmin` stays behind it; retained WAL grows on the server holding the slot even
  though we are fully caught up **[doc]** (logicaldecoding-explanation, "Caution"), **[src]** (snapbuild.c candidate
  restart LSN logic). Reaching the WAL cap invalidates the slot (H-036).
- **Reproduction.** P: session 1 `BEGIN; SELECT txid_current();` (keep open); session 2 writes for an hour; R: retained WAL
  (`pg_wal_lsn_diff(pg_last_wal_replay_lsn(), restart_lsn)`) grows despite `confirmed_flush_lsn` advancing.
- **Prevention.** Guard: the periodic check compares `restart_lsn` with `confirmed_flush_lsn` and reports the oldest
  `backend_xid`/`backend_xmin` on the primary (`pg_stat_activity`, `pg_prepared_xacts`) with an alert when retained WAL
  exceeds 25 % of the cap; the size-lag trigger (SPEC 3.6) measures `restart_lsn`-based retention, not only apply lag.
- **Why prevention.** The cause is reported long before the cap; we cannot end someone else's transaction.
- **Test.** `test_h031_long_tx_retention_alert`.
- **Residual.** If the operator ignores the alert, the cap invalidates the slot and the backfill path runs.

### H-034 Publication missing or recreated at a WAL position

- **What.** The slot decodes WAL from before the publication existed, or the publication is dropped and recreated.
- **Mechanism.** PostgreSQL 18's pgoutput skips a publication that does not exist at the decoded position with only a
  WARNING ("skipped loading publication … The publication does not exist at this point in the WAL") and then publishes
  nothing for it; PostgreSQL 17 raises an error instead (`LoadPublications`, 18 vs 17) **[src]**. Debezium's
  `all_tables` mode continues on a standby when the publication is missing (debezium.md §2.3) **[src]**. A drop and
  recreate makes every change between the two DDLs invisible.
- **Reproduction.** R: create the slot first; P: then `CREATE PUBLICATION cdc_v2_pub2 FOR TABLE cdc_v2_t; INSERT …`; R:
  peek with `publication_names` = `cdc_v2_pub2` from the slot start: WARNING, no rows before the publication's LSN.
  Recreate variant: `DROP PUBLICATION cdc_v2_pub; INSERT …; CREATE PUBLICATION cdc_v2_pub FOR TABLE cdc_v2_t;`.
- **Prevention.** Order rule: our code creates the publication on the primary, waits until it is visible on the replica
  (catalog query), and only then creates the slot, so every decodable position has the publication;
  `publication.autocreate.mode=disabled`; the fence records `pg_publication.oid` of our publication and treats any change
  (drop/recreate) as a stream discontinuity: stop, alert, backfill all tables, new slot.
- **Why prevention.** The slot never starts before its publication; a replaced publication is detected by identity, not
  by missing data.
- **Test.** `test_h034_slot_before_publication_refused`, `test_h034_publication_recreated`.
- **Residual.** Changes made between a third party's drop and recreate of our publication are absent from the stream;
  the OID change triggers the backfill, so this gap is repaired, not prevented. Finding F-4.

### H-035 Slot advanced, dropped, recreated or consumed externally (SPEC 1.8)

- **What.** Another session calls `pg_replication_slot_advance`, `pg_logical_slot_get_changes`, drops and re-creates the
  slot, or streams from it.
- **Mechanism.** Any of these moves `confirmed_flush_lsn` past changes we never applied, or replaces the slot (new
  `restart_lsn`, new catalog horizon); Debezium does not notice with `no_validation` **[src]**.
- **Reproduction.** R: `SELECT pg_replication_slot_advance('cdc_v2_slot', pg_last_wal_replay_lsn());` while the pipeline
  is stopped; or `SELECT pg_drop_replication_slot('cdc_v2_slot'); SELECT pg_create_logical_replication_slot('cdc_v2_slot',
  'pgoutput');`.
- **Prevention.** Slot check at every acquisition and every 30 s while streaming: slot exists, plugin `pgoutput`,
  database ours, `confirmed_flush_lsn` ≤ applied commit LSN, and a **slot identity** recorded at creation (creation
  consistent point plus `catalog_xmin` lower bound; on 17+ also `inactive_since` history) still plausible
  (`restart_lsn` never decreases between our observations except after a server crash we observed); otherwise alert +
  automatic backfill, no exit (SPEC 1.8). `active_pid` belonging to a session that is not ours ⇒ alert (contending
  consumer).
- **Why prevention.** No change is applied from a slot whose continuity is not proven.
- **Test.** The four SPEC 1.8 tests (`pg_replication_slot_advance`, drop-and-recreate, changes consumed by another
  session, invalidation by `max_slot_wal_keep_size`).
- **Residual.** A drop-and-recreate between two checks while we are disconnected is detected at the next acquisition.

### H-036 Slot invalidation by the WAL cap (`wal_removed`)

- **What.** `max_slot_wal_keep_size` on the server holding the slot (or WAL removal for other reasons) removes WAL the
  slot needs.
- **Mechanism.** Checked at checkpoints and, on a standby, at restartpoints (`InvalidateObsoleteReplicationSlots` from
  `CreateCheckPoint`/`CreateRestartPoint`, `pg:src/backend/access/transam/xlog.c`) **[src]**; the slot becomes `lost`
  permanently **[doc]** (view docs). Debezium retries forever (debezium/dbz#2430) **[doc]**.
- **Reproduction.** R: `ALTER SYSTEM SET max_slot_wal_keep_size = '64MB'; SELECT pg_reload_conf();` (T1 only), stop the
  pipeline, write 500 MB on P, `CHECKPOINT` on P, wait for a restartpoint on R (`CHECKPOINT` on R).
- **Prevention.** Size-lag trigger (SPEC 3.6) at 50 % of the cap with the cap read from the server holding the slot;
  alert at `wal_status = 'extended'`/`'unreserved'`; slot check treats `invalidation_reason IS NOT NULL` as slot loss.
- **Why prevention.** The backlog is abandoned (and re-backfilled) deliberately before the server invalidates the slot.
- **Test.** SPEC 1.8 invalidation test; `test_h036_size_trigger_before_cap`.
- **Residual.** A write burst faster than our check interval can still reach the cap; then the slot-loss path runs.

### H-037 Idle-slot timeout invalidation (PostgreSQL 18)

- **What.** `idle_replication_slot_timeout` invalidates slots inactive longer than the setting.
- **Mechanism.** Applies to ordinary slots on primaries and standbys (not to synced slots on a standby), at the next
  checkpoint/restartpoint (`CanInvalidateIdleSlot`, slot.c) **[src]**; default off **[doc]**. Our slot is inactive during
  run hand-overs (R18) and outages.
- **Reproduction.** R: `ALTER SYSTEM SET idle_replication_slot_timeout = '30s'`; stop the pipeline for 2 minutes;
  `CHECKPOINT` on P and R.
- **Prevention.** Read the setting at every acquisition; if non-zero and shorter than 10 × the worst measured hand-over
  gap (R18, SPEC 4.2), alert and refuse to rely on that server for replica mode; on PlanetScale ask for it to stay 0
  **[test]**.
- **Why prevention.** The configuration that would invalidate the slot is rejected before it can act.
- **Test.** `test_h037_idle_timeout_guard`.
- **Residual.** A provider-side change while we run; detected on the next check.

### H-038 Retained WAL grows billed storage or fills the disk

- **What.** Slot lag makes WAL accumulate; on PlanetScale storage autoscaling is on by default.
- **Mechanism.** "Storage autoscaling is enabled by default for network-attached storage clusters" **[doc]**
  (<https://planetscale.com/docs/postgres/postgres-compatibility.md>); retained WAL counts as storage. PLAN §5 forbids
  any action that changes what the instance costs.
- **Reproduction.** Stage 2 only, read-only observation of the storage metric while the slot lags in a controlled test.
- **Prevention.** Before stage 2 writes anything, record the cost baseline and the autoscaling threshold (PLAN §5); set
  the size-lag trigger (SPEC 3.6) and our alert to fire well below the free space that would trigger autoscaling, and,
  with the owner's approval, `max_slot_wal_keep_size` below that threshold; same limits for spill files (H-049).
- **Why prevention.** The pipeline abandons a backlog before the provider would grow storage.
- **Test.** `test_h038_lag_trigger_below_storage_headroom` (T1 with a simulated disk budget; stage 2 measurement).
- **Residual.** Other tenants of the instance can still grow storage; not ours to prevent.

### H-039 Row-level security and privileges silently filter backfill reads

- **What.** The runtime role lacks `BYPASSRLS` (or `SELECT` on a column/table) and a table has row-level security.
- **Mechanism.** Backfill and refresh queries are ordinary `SELECT`s and see only rows the policies allow; logical
  decoding itself does not apply RLS to decoded changes **[doc]** (logical-replication-security: "If the role lacks
  SUPERUSER and BYPASSRLS, publisher row security policies can execute"). A backfill would silently load a subset while
  the stream delivers every change — destination divergence.
- **Reproduction.** P: `ALTER TABLE cdc_v2_t ENABLE ROW LEVEL SECURITY; CREATE POLICY cdc_v2_pol ON cdc_v2_t USING (id <
  10);` and backfill as a non-owner role without BYPASSRLS.
- **Prevention.** Backfill and refresh sessions set `row_security = off`, which makes a query that would be filtered
  fail with an error instead **[doc]** (<https://www.postgresql.org/docs/18/runtime-config-client.html#GUC-ROW-SECURITY>);
  discovery checks `relrowsecurity` and the role's `rolbypassrls` and alerts; the runtime role is created with
  `BYPASSRLS` where the provider allows it (PlanetScale default role has it **[doc]**).
- **Why prevention.** A filtered read becomes impossible; it fails loudly.
- **Test.** `test_h039_rls_backfill_fails_loudly`.
- **Residual.** None.
