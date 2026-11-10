# The transform (package Y): `cdc_v2.transform`

The one place where a source value becomes a destination value. Contract: `docs/design/p3_plan.md` C3 (and C2,
C3a, C7, C8). Type table: `docs/type_mapping.md`. Evidence: `evidence/p3/transform/`.

| File | What |
|---|---|
| `src/cdc_v2/transform/planning.py` | `plan()`, `TablePlan`, `ColumnPlan`, `TableMeta`, destination names and collisions, the frame check |
| `src/cdc_v2/transform/families.py` | per type family: destination types, forms, and the conversion of each input form (SQL templates) |
| `src/cdc_v2/transform/convert.py` | `Transformer` (one local DuckDB connection), compile, run, the stream's pyarrow fast path, error diagnosis, `ambiguous` |
| `src/cdc_v2/transform/errors.py` | `PlanMismatch`, `WideningNeeded`, `ConversionError` |
| `tests/transform/` | hermetic tests (default run), `localpg`, `localpg and jvm`, the measurement (below) |

## 1. Interface as built

```python
from cdc_v2 import transform
plan = transform.plan(source_table, schema_frame=None, *, server_version_num)   # -> TablePlan
out  = transform.stream(plan, rows)        # ROWS batch (C2), transport form -> destination types
out  = transform.backfill(plan, rows)      # a<i> as Postgres text (real NULLs) -> destination types
mask = transform.ambiguous(plan, rows)     # BooleanArray, or None when the plan has no ambiguous column

t = transform.Transformer(transform.TransformSettings(threads=1, memory_limit="512MB",
                                                     temp_directory=..., max_temp_directory_size=...))
t.stream(plan, rows); t.backfill(plan, rows); t.ambiguous(plan, rows); t.close(); t.conn  # the connection
```

- `source_table` is `cdc_v2.pg.catalog.SourceTable`. Positions are its live columns in `attnum` order, which are the
  engine's positions. The plan is built from the catalog alone; a SCHEMA frame is only checked (column count,
  position, name, transport form, key flags and key positions; `PlanMismatch.differences` lists each as
  (position, field, catalog side, frame side)). So the backfill's plan and the stream's plan of one table are the
  same value with the same hash.
- `TablePlan` (frozen): `schema`, `table`, `oid`, `server_version_num`, `columns` (one `ColumnPlan` per position:
  `source_name`, `source_type` (format_type, with modifiers), `base_type`, `family`, `dest_name`, `native_type`,
  `wide_type`, `form` ('native' | 'wide'), `dest_type`, `key`, `unchanged_possible` (variable-length type, as the engine decides; not the column storage),
  `stream_forms`, `numeric`, `step`), `has_key`, `key_positions`, `ambiguous`, `ambiguous_key`, `collisions`, `hash`;
  `widened(position)`, `widen_sql(position, column_sql=None)`, `with_step(position, sql)`, `table_meta()`,
  `create_columns()`, `column_ddl()`, `arrow_schema()`.
- `hash` is a SHA-256 over everything the destination table and the conversion depend on, including the server's
  **major** version (a minor update changes nothing).
- `stream`: `seq`, `op`, `u<i>` and any column that is not `a<i>`/`b<i>`/`k<i>` pass through untouched; `a<i>`,
  `b<i>`, `k<i>` are converted by position. The result keeps the names and the row order. A value under a set
  `u<i>` is not used (SQL columns: `CASE WHEN u<i> THEN NULL`; the fast path masks it; passed-on columns are not
  looked at).
- `backfill`: `a<i>` must be `string` (`large_string` is accepted and narrowed).
- Module functions use the calling thread's own `Transformer` (`threading.local`). Nothing is shared between two
  transformers; each owns its DuckDB connection.

**The connection.** In memory; `enable_external_access = false` (no ATTACH, no file access, no extension loading:
`ATTACH 'md:'` fails), `autoload_known_extensions = false`, `autoinstall_known_extensions = false`,
`arrow_lossless_conversion = true`, `TimeZone = 'UTC'`, `threads` and `memory_limit` from the settings, and the spill
area (`temp_directory`, `max_temp_directory_size`) given at construction: with external access off DuckDB refuses
to change `temp_directory` later. Test: `test_connection_is_isolated_from_motherduck`,
`test_spill_area_setting_and_nothing_left_behind`.

**Keys** (core design 6: two keys that the source calls different must differ in the destination).
`TablePlan.ambiguous_key` lists the key positions whose stream form is not injective: a scalar `interval` key, also
through a domain over `interval`, on a source of Postgres 17 or later (0, `infinity` and `-infinity` all arrive as the
zero interval). Package S does not stream such a table (plan 6.2). Every other key type is injective: an interval
array or a composite with an interval field travels as text in which the engine keeps `infinity`, and since the fix
round both paths print it alike (cross-path test: `f_aiv`, `f_cp` identical); the wide forms of numeric and
timestamp keys use an exact fit test and distinct members; text-fallback keys are Postgres's output. Test:
`test_ambiguous_key_lists_exactly_the_key_columns_the_stream_cannot_deliver_injectively`.

## 2. How a caller handles `WideningNeeded`

```python
try:
    out = transform.stream(plan, rows)
except transform.WideningNeeded as e:            # nothing of the batch was converted
    for p in e.positions:                         # every position that needs it, at once
        sql = plan.widen_sql(p)                   # USING expression: native value -> member n, NULL stays NULL
        # ALTER TABLE t ALTER COLUMN <dest name> TYPE <plan.widened(p).columns[p].dest_type> USING <sql>  (P4)
        plan = plan.widened(p)
    out = transform.stream(plan, rows)            # convert the same batch again with the new plan
```

In P3 package S treats `WideningNeeded` like a schema change (plan C10). `widen_sql` is
`CASE WHEN c IS NULL THEN NULL ELSE CAST(union_value(n := c) AS <wide>) END`; a plain `CAST` of a typed NULL to a
UNION gives a non-NULL union whose member is NULL (DuckDB 1.5.6). Test `test_widening_expression_changes_no_value_
under_the_oracle`: before and after the `ALTER`, the oracle's canonical form of every value is the same, and NULL is
a real NULL.

A batch can hold a misfit and a broken value: the misfit is reported first (`WideningNeeded`); after widening the
broken value raises `ConversionError` (test `test_widening_needed_names_all_positions_and_carries_no_value`).

## 3. The statements

Per (plan hash, direction, input schema) one SELECT is compiled and cached as text (LRU, 256). Columns whose input
already has the destination layout are not in it: they are handed on (same buffers; tests check the buffer
addresses). The others are computed over the registered batch in layers: each layer is
`SELECT *, <intermediates> FROM (<previous layer>)`, so a value parsed once is used many times. Intermediates use
`TRY_CAST`, the final expression validates them; two markers, raised with `error()`, carry only a column name:
`cdc_v2:misfit:a3` and `cdc_v2:bad:a3`.

**No prepared statement.** A DuckDB prepared statement keeps reading the Arrow batch that was registered when it was
prepared, also after the name is registered again: the second batch returned the first batch's rows (found by the
first smoke test, DuckDB 1.5.6). So the SQL is executed as text each time; this costs the parse and plan per call.

**Fast path (stream).** Because one statement costs about a millisecond for a lineitem-width table, the common
stream values go through pyarrow without DuckDB: `date_pg_days` (add 10,957; the infinities fall back),
`timestamp(tz)_pg_us` in DuckDB's range (add the epoch shift; the infinities and values beyond 294247 fall back) and
`numeric(p ≤ 18, s)` (`pyarrow` cast; NaN raises and falls back). A batch with any value outside those takes the SQL
path whole. Test `test_stream_fast_path_equals_the_sql_path`, and the property and cross-path tests compare it with
the SQL path of the backfill.

**Errors.** On any DuckDB error the text is dropped inside the handler; only our own marker is kept. Outside the
handler (so nothing is chained): if a misfit query (`bool_or` of the misfit predicates) finds misfits,
`WideningNeeded(positions)`; otherwise each SQL column is run alone on halves of the batch (row numbers carried in an
extra Arrow column, never a window over values) until the first failing row is found: `ConversionError(table,
position, seq, row=…, column=…)`. Test `test_errors_carry_no_value` (13 cases): a planted marker is searched in
the message, `repr`, `args`, `vars`, the whole chain, the formatted traceback and the captured log; `__cause__`
and `__context__` are None.

**Per family**: `src/cdc_v2/transform/families.py`; dates and timestamps are counted with H. Hinnant's
days-from-civil algorithm (astronomical years, so `N BC` is year 1−N); intervals in the backfill are read in
IntervalStyle `postgres` (since the fix round; years and months, days and the time part each with its own sign,
hours unbounded) with one regular expression; the text member of the interval's wide form is ISO 8601 on both
paths (`PT2562047788H54.775807S`; `infinity` and `-infinity` as printed); the stream's wide text (timestamps above DuckDB's range, intervals above Arrow's) is printed
exactly as Postgres's `EncodeDateTime`/`EncodeInterval` print it.

## 4. Speed and memory

`evidence/p3/transform/speed.json` (`CDC_V2_Y_MEASURE=1`, one core, `threads=1`, shared laptop: indications only).

| Table | Direction | 1 row: µs per call | 1,000 rows: rows/s | 100,000 rows: rows/s (MB/s of input) |
|---|---|---|---|---|
| lineitem (16 columns) | stream | 92 | 5.6 M | 11.4 M (2,150) |
| lineitem | backfill | 8,229 | 108 K | 782 K (179) |
| narrow (bigint, int) | stream | 8 | 120 M | – (all passed on) |
| narrow | backfill | 735 | 1.3 M | 15.0 M (385) |
| 200 columns (10 families) | stream | 49,630 | 16 K | 52 K (137) |
| 200 columns | backfill | 201,830 | 4.3 K | 22 K (79) |

Final run, after the review fixes; an earlier run of the same code without the digit check of constrained
numerics gave 875 K rows/s for the lineitem backfill (other workers were running; numbers vary by about 10 %).

- At lineitem width the transform is about 8 times (backfill) and more than 100 times (stream) faster than the 100,000
  rows/s the pipeline needs (SPEC 3.1, 5.1).
- What limits it: (1) the parse and plan of one DuckDB statement per call (prepared statements are unusable, above):
  about 6–8 ms for lineitem's backfill statement, 50 to 200 ms for 200 columns. A one-row stream batch avoids it
  by the fast path (92 µs); a one-row backfill batch does not occur (chunks are large). (2) DuckDB's text parsing for
  some families, measured per column on 100,000 rows: interval 1.3 M values/s (both directions), timestamptz text
  1.0 M/s, date text 3.5 M/s, unconstrained numeric 16 M/s (it was 0.8 M/s: DuckDB parses text into a DECIMAL wider
  than 18 digits about 50 times slower than into `DECIMAL(18, s)`, so a text whose digits fit goes that way and is
  widened exactly). A 200-column table with 20 interval and 20 timestamptz columns is therefore limited to about
  22–52 K rows/s per core.
- For package S: at 2,000 small transactions per second (SPEC 5.3) a table whose stream columns all have a fast path
  costs about 0.1 ms per batch; a table with an `interval`, `uuid`, `timetz`, unconstrained numeric or money column
  costs a statement (about 1 ms) per batch. Concatenating the consecutive `ROWS` batches of one table and schema
  version before the transform (it is row-local) removes that cost.

**Memory** (`memory_100mb.json`, growth of the resident set during one conversion, measured in a fresh process with
a sampler): a 100 MB `bytea` value through the backfill (200 MB of hex text) adds 0.9–1.0 × the batch (about 2 × the
value; 0.89 in the final run); a 100 MB text value (both paths) and a 100 MB `bytea` value in the stream add nothing (passed on, no copy).

## 5. Contract notes

1. `ambiguous(plan, rows)` takes the batch in **transport form** (the zero interval is the text
   `0 months 0 days 0 microseconds`) and marks a row whose `a<i>` **or `k<i>`** holds it (an interval key column's
   old key is as ambiguous as the new image); `b<i>` is not marked (a keyless table with such a column is not
   streamed, C3).
2. `TablePlan.hash` includes the major server version only.
3. `TableMeta(columns=(MetaColumn(position, name, type, key, unchanged_possible), ...))`, `has_key` derived: the shape
   package A's task names; defined here because A is not merged. If A's class differs, `plan.table_meta()` is the
   one adapter.
4. Collisions: names that fold to one text under Unicode case folding (a superset of what DuckDB 1.5.6 refuses:
   ASCII letters; `É`/`é`, the Kelvin sign and `ß`/`ss` are held apart by DuckDB but reported). The lowest position
   keeps its name; every other becomes `<name>_<position>`, with `_` appended until unique.
5. A domain is planned as its base type without the domain's typmod (the catalog value has no `typtypmod`): a
   `numeric(10,2)` domain is planned as unconstrained numeric. Lossless; a finding for package P.
6. The transform rejects a batch that breaks the plan (an unknown position, a transport type outside the plan's
   forms, a non-text backfill column) with `ConversionError` (no value involved).
7. SPEC 8.3 seam: `ColumnPlan.step` / `TablePlan.with_step(position, sql)`: SQL over `{x}` (the converted value),
   cast back to the destination type, applied on both paths before the result leaves the transform (test
   `test_pii_step_seam_applies_on_both_paths`). No masking is built.
8. JSON and enums are `VARCHAR` (`docs/type_mapping.md`), so no column is handed on as `arrow.json`.

## 6. From the review of package A (apply)

1. **Lossless Arrow types**: `arrow_lossless_conversion = true` and `TimeZone = 'UTC'` are set before the first
   statement; UUID leaves as `arrow.uuid`, TIMETZ as DuckDB's `time_tz` opaque type, TIMESTAMPTZ as
   timestamp[us, tz=UTC]. JSON is VARCHAR on purpose (DuckDB's JSON refuses `"\ud800"`, which Postgres `json` holds).
   Test: `test_lossless_arrow_types_for_uuid_timetz_and_json_text`. BOOLEAN: DuckDB's lossless export gives
   `arrow.bool8`; the transform hands on the standard boolean (as the stream does).
2. **No aggregate, window, GROUP BY, DISTINCT, PARTITION BY or `=` over a value**: conversions are row-local
   expressions. The only comparisons are of transport integers with the infinity sentinels and of input text with
   literals (`'infinity'`, `'NaN'`, `'t'`), and in `ambiguous` of transport text with the zero-interval text; all
   exact. The diagnosis uses `bool_or` over misfit predicates (booleans, never values) and carried row numbers.
   Test: `test_negative_zero_and_nan_kept_by_bits_on_both_paths_also_inside_arrays` (−0, both NaN signs in the
   stream, arrays and composites as text, compared by bits and with `cdc_v2.oracle.canonical`).
3. **A source NULL is a real NULL**: every family with a wide form (numeric constrained and unconstrained,
   timestamp, timestamptz, interval) is tested after a DuckDB `INSERT` from the Arrow result:
   `test_source_null_is_a_real_null_in_a_union_column`. No family uses a STRUCT wrapper.

## 7. Findings

**Values for which the two paths differed** (cross-path test, Postgres 18.6 and 16.6): no baseline family. Fallback
columns: geometric types (Java float formatting). Arrays of `interval` and composites with an `interval` field
differed until the fix round (IntervalStyle of the two sessions); the read sessions now print `postgres` and they
are identical. On 18: `infinity`/`-infinity` in a scalar interval column (zero
through the stream, marked by `ambiguous`; true value through the backfill). An interval above 2^31 hours is a
stream-side gap (the driver fails), delivered by the backfill. Bugs of this package that the tests found during
development, all fixed and covered: the stream's wide timestamp text cut years of five or more digits (DuckDB's
`lpad`); the fast path silently fell back for dates and timestamps (a type promotion), caught by
`test_stream_fast_path_equals_the_sql_path`; a prepared statement returned a previous batch (no longer used).

**For package E**: geometric values lose Postgres's float formatting (known, P4). The IntervalStyle difference is
resolved: the read sessions of package P print `postgres` like the walsender session (decided by the coordinator).

**For package P**: `TypeInfo` has no domain typmod.

**For package O (oracle)**: the oracle cannot judge some values that P3 delivers: it has no VARCHAR member rule for
timestamp and interval columns (the wide forms); its Postgres timestamp form overflows `bigint` above 294247 AD;
arrays delivered as text never match (its array rule wants a LIST); composites as text are compared under the
oracle's own session (resolved by the IntervalStyle change: they compare equal now); `inet::text` differs from inet's output (`::1/128` vs `::1`); and
TIMETZ is compared through `CAST(TIMETZ AS VARCHAR)`, which DuckDB 1.5.6 prints wrongly for offsets with a zero
minute and seconds (`-01:00:30` printed `-01:30`; the stored value is right). The cross-path test lists these
tables and compares them by the identity of the two paths instead.

**DuckDB behaviours (1.5.6)** found here: prepared statements keep the first registered Arrow batch; lossless
Arrow export turns BOOLEAN into `bool8`; INTERVAL → Arrow multiplies µs by 1,000 and wraps silently above
9,223,372,036,854,775 µs; DATE −2147483648 from Arrow is a finite date (−infinity is −2147483647); TIMESTAMP
−2^63 from Arrow fails; `CAST('0044-03-15 BC' AS DATE)` is 44 AD; `CAST('1.5' AS INTEGER)` is 2;
`unhex('abc')` (odd length) silently reads `0abc`; text → `DECIMAL(38,s)` rounds extra digits silently and is 50
times slower than → `DECIMAL(18,s)`; `DECIMAL(s,s)` prints without the leading zero; `lpad` cuts a longer text;
`//` and `%` truncate like C; `TRY()` cannot wrap `error()`; a `CAST` of a typed NULL to a UNION is a non-NULL union;
DuckDB's JSON refuses lone surrogate escapes; TIMETZ → VARCHAR prints `-01:00:30` as `-01:30`; Arrow import of a
union whose selected child is NULL gives a real NULL; `TimeZone` cannot be given in the connect configuration
(only `SET` after connect); with external access off `temp_directory` cannot be changed after connect; column-name
folding is ASCII-only.

## 7a. Review

A read-only reviewer with fresh context (Opus) checked, by running hermetic code, 550,000 floats, 40,000 dates and
timestamps, 8,000 intervals, the numeric fast path, money and timetz, and found no silent wrong value for valid input.
It found four edge cases, all fixed with a regression test (`tests/transform/test_transform_vectors.py`, section
"review findings"): (1) a constrained numeric checked only NaN, so a row with more digits than the current typmod
(decoded before an `ALTER COLUMN TYPE`) would have been rounded by the cast: the digit test now applies to every
numeric and such a value widens; (2) the stream's date fast path could wrap on a transport value beyond Postgres's
range: it now covers only Postgres's range; (3) "unchanged possible" came from the column's storage, but `SET STORAGE
PLAIN` leaves out-of-line values in place: it now follows the type (variable length), as the engine does; (4) a step
template with a lone `}` failed at compile time: refused when set.

## 7b. Fix round after the coordinator's review

(1) A negative control for the cross-path identity check: one `-0` made `+0` in `f_f8` and one timestamp moved by
1 µs in `f_ts` on the stream path; `compare_paths` reports exactly those tables, column 1 and those ids (evidence key
`negative_control`). (2) `TablePlan.ambiguous_key` (section 1, "Keys"). (3) One interval text style on both paths:
`FIXED_SESSION_SETTINGS["IntervalStyle"]` is `postgres`; the backfill parses that style; Postgres-prints reads back
3,015 interval values (with the extremes, and `infinity`/`-infinity` on 18) exactly on 18 and 16; the cross-path
test finds arrays and composites with intervals identical. (4) Default run: 30 property examples per family
(`CDC_V2_Y_EXAMPLES` for more).

## 8. Tests and how to run them

```sh
uv run -q pytest -q tests/transform                                         # default run: hermetic, ~3 s
CDC_V2_Y_EXAMPLES=1500 uv run pytest -q tests/transform/test_transform_property.py     # longer property run
uv run pytest -m localpg tests/transform/test_transform_localpg.py           # Postgres prints; oracle negative controls
uv run pytest -m "localpg and jvm" tests/transform/test_transform_crosspath.py          # engine + backfill, Postgres 18
CDC_V2_Y_PG=16 uv run pytest -m "localpg and jvm" tests/transform/test_transform_crosspath.py   # resets pg-y to 16
CDC_V2_Y_MEASURE=1 uv run pytest tests/transform/test_transform_measure.py -s          # speed and memory
# CDC_V2_Y_EVIDENCE=1 writes the cross-path evidence; CDC_V2_Y_KEEP_PG=1 keeps the pair running
```

The `localpg` fixtures use `tools/localpg --home ~/.cache/cdc_flight_v2/pg-y --port-base 55656` (ports
55656–55663), run `up` and `down` at the end; every scenario creates its own schema, publication and slot and drops
them, also on failure. `CDC_V2_Y_PG=16` resets the home with the Postgres 16 binaries; reset it again before an 18
run.

Results (final runs): hermetic suite all green; property tests with 1,500 examples per family (up to 40 values each)
green; cross-path on 18.6: 59 tables, 3,284 streamed records, 6,795 values compared, identical except the listed
fallback columns and the two infinity rows; on 16.6: 58 tables, 3,279 records, identical except the listed fallback
columns, no ambiguous column; role-level settings changed nothing on either path (16 tables and the mixed table);
Postgres-prints: 10 families × 3,000 values read back exactly on 18 and 16; oracle negative controls (15-digit float
text, −0 normalised, BC date through DuckDB's cast) all reported different.
