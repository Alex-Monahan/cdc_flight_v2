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

*(Numbered with the replica group but placed here because it follows from H-001.)* See §E.

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
