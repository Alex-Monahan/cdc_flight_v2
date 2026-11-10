# Task: package Y — transform

Read `docs/tasks/README.md` first; its rules bind you.

You build the one place where a source value becomes a destination value: the destination type of every source
column, and the conversion of both input forms — the engine's transport form (stream) and the text that Postgres
prints (backfill) — into Arrow data of the destination types. Contract: `docs/design/p3_plan.md` C0, C3 (and C2 for
the transport form, C3a for the column names, C7 for the source table and the session settings, C8 for what
"equal" means). SPEC items that this package decides: the baseline of 2.4, the shape that 2.5 will need, the
"unchanged" pass-through and the memory side of 2.6, and the place where 8.3 will live.

The stream package (S), the backfill (B) and the apply (A) all rely on one promise: **the same source value gives
the same destination value on both paths**, bit for bit, and no source value is ever refused, dropped or changed
into NULL.

## Read as well

- `docs/design/core.md` section 6.1.
- `docs/design/engine.md` (package E): the value transport table — which Debezium form each source type arrives
  in, physical or text or bytes — and `docs/research/debezium.md` section 7 (why those forms). The `SCHEMA` frame
  and the `ROWS` batch of `cdc_v2.stream.engine` are your stream input.
- `docs/design/source.md` (package P): the session settings table. The backfill's text form is defined by it and
  by nothing else. `catalog.tables` gives your `source_table`.
- `docs/oracle.md` and `src/cdc_v2/oracle/canonical.py`: the rules by which the result is judged. Read them; share
  no code with them (C8) — an error that both sides inherit from one function would be invisible.
- `docs/wal_hazards.md` section B (H-003, H-009 to H-013), H-032, H-033.

Findings that the oracle package proved on DuckDB 1.5.6 and that decide your code:

1. `CAST('0044-03-15 BC' AS DATE)` returns the year 44 **AD**. No Postgres date or timestamp text goes through a
   DuckDB cast unless a test over the whole range (BC, year 1, years above 9999, both infinities) proves that the
   cast is right for it; where it is not, you take the text apart yourself.
2. Float texts are printed differently by different servers. Floats are parsed to binary once, by you, and never
   printed again; the oracle compares bits (`-0` ≠ `0`, all NaNs equal).
3. A `UNION` value whose member holds NULL is not NULL. A source NULL is a real NULL in the destination, for every
   type, also for a column whose destination type is a `UNION`.
4. An `UPDATE` to a value that compares equal is not written, and grouping merges such values (`-0`/`0`,
   `1 mon`/`30 days`). That is package A's problem, but your conversion must keep these values apart exactly as
   the source has them (an interval keeps months, days and microseconds as three numbers).

## From the engine as built — build on it

Package E is built and reviewed. `docs/design/engine.md` is the full account; where it differs from what this task
says elsewhere, these points and that document hold.

- Convert by the transport form that the `SCHEMA` frame names (`docs/design/engine.md` section 3), and by nothing
  else: `date_pg_days` (int32 days since 2000-01-01; +infinity and -infinity are the int32 maximum and minimum),
  `timestamp_pg_us` and `timestamptz_pg_us` (int64 µs since 2000-01-01 UTC; the infinities are the int64 maximum and
  minimum), `time_us` (0 to 86,400,000,000), `interval_text`, `numeric_text` (scale kept; NaN, Infinity, -Infinity),
  `money_text`, `array_text` (with lower bounds), `bytes`, `text` (`char(n)` keeps its padding). Take the exact
  names from the document.
- BC dates and dates outside DuckDB's range arrive as numbers. Never cast a date text through DuckDB (it reads a
  Postgres BC date text as an AD date).
- Geometric types arrive in Java's float formatting; they are not lossless yet (P4). Say so in the type table.
- `u<i>` exists for a superset of the columns that can be sent as unchanged; a column that never is has an all-false
  `u<i>`.
- Infinite intervals and intervals above 2^31 hours do not reach you through the stream today: the driver fails on
  them and the stream quarantines the table (package S). The backfill path does read them; your cross-path test
  lists them as stream-side gaps instead of skipping them silently.

## Resources

- Postgres: `tools/localpg --home ~/.cache/cdc_flight_v2/pg-y --port-base 55656` (8 ports from the base). Postgres
  18; run the cross-path test once on 16 as well (`--pg-bin /opt/homebrew/opt/postgresql@16/bin`, same home,
  after a reset).
- The engine (package E) with its pinned JRE and jars, for the stream side of the cross-path test. Local DuckDB.
- No MotherDuck: the transform never connects to it, and a test proves that (below).

## Interfaces

```python
plan = transform.plan(source_table, schema_frame=None) -> TablePlan
transform.stream(plan, rows: pyarrow.RecordBatch) -> pyarrow.RecordBatch     # engine transport form → destination types
transform.backfill(plan, rows: pyarrow.RecordBatch) -> pyarrow.RecordBatch   # Postgres text → destination types
```

- `TablePlan` is frozen and has a stable hash. Per column position: source name, source type (as the catalog
  names it, with modifiers), destination name, destination type, key flag, "unchanged possible", the family, and
  the **form** the column is currently stored in (below). It also gives `TableMeta` for package A (C3a) and the
  column list for `CREATE TABLE` (types only; who creates the table is not yours).
- `rows` for `stream` is a `ROWS` batch as C2 defines it (`seq`, `op`, `a<i>`, `b<i>`, `u<i>`, `k<i>`); `seq`,
  `op` and `u<i>` pass through untouched, and a value under a set `u<i>` is never looked at. `rows` for
  `backfill` has `a<i>` as text columns with real NULLs (how the reader gets them out of `COPY` is package B's).
  The result keeps the column names and the row order.
- With `schema_frame`, `plan` checks that the frame and the catalog agree (column positions, names, types, key)
  and raises `PlanMismatch` naming the difference; the stream package decides what follows (a catalog that is
  behind, or a schema change). Without it, the plan is for the backfill alone.
- **Forms and widening.** Some source types can hold values that their native destination type cannot (SPEC 2.4:
  no value is ever refused). Such a column has a *native form* and a *wide form*, both defined by you:
  for unconstrained `numeric` it is `DECIMAL(38, s)` and `UNION(DECIMAL(38, s), VARCHAR)` as SPEC 2.4 says; for
  the others you propose the pair (see "Baseline"). The plan says which form a column is in.
  `plan.widened(position) -> TablePlan` gives the plan with that column in its wide form. When a batch holds a
  value that does not fit the current form, the transform does **not** fail and does not convert the batch
  partly: it raises `WideningNeeded(table, positions)` — no value in it — and the caller widens the destination
  column, takes the new plan and converts the batch again. Altering the destination column is not yours (P4
  builds it on SPEC 2.5's mechanism); the expression that carries existing values of the native form into the
  wide form is yours, as text in the plan, with a test that it changes no value under the oracle's rules.
- `ConversionError(table, column position, seq)` for a value that the plan's form should hold and the conversion
  cannot produce: a bug or an input that breaks the contract. Never the value, never a fragment of it, not in the
  message, the `repr`, the arguments or a chained exception (DuckDB's own error texts quote the value: they must
  not survive). Never a silent NULL, never a truncated or rounded value.
- The transform runs in a local DuckDB connection that is not attached to MotherDuck. Decide how callers get
  it (an object that owns a connection, one per thread): package S runs it on the stream thread and sets the
  temporary directory and its size limit (its spill area, SPEC 5.4), package B runs several at once, one per
  reader. No state is shared between two of them. `threads` and `memory_limit` are settings.
- Destination names: the source column name, exactly, as a quoted identifier. DuckDB compares column names
  without regard to case, so a table with columns `"A"` and `"a"` cannot be created as it is; such a table never
  reaches the stream (H-032) and is served by full refresh in P4. The plan reports the collision as a property of
  the table (`collisions`) and gives deterministic destination names for it, so that P4 has them; say which
  rule. Any other name is legal: quotes, dots, spaces, keywords, non-ASCII, 63 bytes, and a column named like
  one of the transport columns (`seq`, `op`, `a1`).

## What it must do

**Every source type gets a plan.** A type is either in the baseline below and maps to a native type, or it is
delivered as `VARCHAR` holding exactly the text Postgres prints under the session settings of package P. The text
fallback is lossless by definition and is the P3 mapping of every type outside the baseline (arrays, composites,
ranges, bit strings, network and geometric types, `money`, `hstore`, `xml`, extension types, domains over them …);
the P4 type wave replaces fallbacks by native mappings one family at a time and must be able to do that without
touching the callers. No type makes `plan` fail. A domain is planned as its base type.

For a fallback type the stream form may not be the Postgres text (Debezium turns `hstore` into JSON, intervals
into ISO-8601, and so on). The cross-path test (below) finds every such type. For each one, either convert the
stream form into exactly the Postgres text, or — where the stream form has lost information — report it as a
finding for package E (a converter there can send the original text). Do not hide a difference by choosing a
looser destination type.

**Baseline (native in P3).** For each: the destination type, the native and the wide form if there are two, and
the proof.

| Family | Notes |
|---|---|
| `boolean`; `smallint`, `integer`, `bigint` | physical in the stream; text in the backfill |
| `real`, `double precision` | NaN, ±infinity, `-0`, subnormals, the largest and smallest values; exact bits on both paths (finding 2). Postgres has one NaN; say what you emit |
| `numeric(p, s)` with p ≤ 38 | `DECIMAL(p, s)`; NaN — and ±infinity where the source type allows it — do not fit: wide form. Negative scale and scale above precision (Postgres 15+): decide |
| `numeric` without precision, and p > 38 | SPEC 2.4: `DECIMAL(38, s)` while values fit, else `UNION(DECIMAL(38, s), VARCHAR)`. Choose `s`, justify it against the data the spec names (`1.50` keeps its trailing zero under the oracle's rule? read `canonical.py` and say what "equal" asks of you), and say exactly when a value "fits" (digits before the point, digits after it, NaN, ±infinity, 131,072 digits) |
| `text`, `varchar(n)`, `char(n)`, `name`, `"char"`, `citext` | `VARCHAR`; `char(n)` padding exactly as the source prints it; empty string ≠ NULL; any length (100 MB) |
| `bytea` | `BLOB`; empty value; 100 MB |
| `date` | BC, year 1, years above 9999, the Postgres limits, ±infinity (finding 1) |
| `timestamp`, `timestamptz` | microseconds; BC; the Postgres range is wider than DuckDB's at the top (294276 AD): values outside are a wide form, not an error; ±infinity; `timestamptz` is an instant — the session zone is UTC on both paths |
| `time`, `timetz` | `24:00:00`; `timetz` offsets to ±15:59:59; if DuckDB has no exact type for a value, wide form |
| `interval` | months, days, microseconds kept apart; negative parts; the Postgres limits against DuckDB's |
| `uuid` | `UUID` |
| `json`, `jsonb` | the text as printed, never parsed into numbers (huge and high-precision numbers, duplicate keys and key order in `json`); say which destination type and why |
| enums | H-033: labels are added, renamed and reordered at the source. A native `ENUM` needs an answer for each of these that loses nothing; if there is none, the mapping is `VARCHAR` — say so in the mapping table as a point for the owner |

For every family: NULL, and the column as part of a key where Postgres allows it.

**Speed and memory.** The conversion is vectorised: one prepared statement (or a few) per plan and direction, no
Python per value, no Python per row. Columns that are already in their destination layout are not copied.
Measure rows per second and bytes per second on one core for a table of TPC-H `lineitem` width, for a narrow
table and for a table of 200 columns, both directions, at batch sizes of 1, 1,000 and 100,000 rows; the whole
pipeline must carry 100,000 rows per second (SPEC 5.1, 3.1), so the transform has to be several times faster
than that on one core — say what limits it if it is not. A batch with one 100 MB value converts with client
memory below a small, stated multiple of the batch (SPEC 2.6); measure the peak.

**Where 8.3 will live.** Nothing downstream ever holds an unconverted value (C3), so the masking and hashing of
SPEC 8.3 will be a step of this conversion in P4. Leave the place: a per-column step in the plan that P4 can set,
applied on both paths before the result leaves the transform. Build no masking now.

## Tests

- Default run (hermetic, a few seconds): `plan` for every baseline family and for the fallback; names (the list
  above); the widening expressions; `WideningNeeded` and `ConversionError` carry no value (a test plants a marker
  value and searches the exception with its whole chain, the log and the traceback text); `PlanMismatch`;
  hand-written vectors for both directions, each with the exact expected destination value.
- **Cross-path test** (marker `localpg`, with the engine: marker `jvm`): one table per family in the local
  Postgres, filled with the boundary values above plus seeded random values; the rows are read once through the
  engine (stream form → `transform.stream`) and once as text under package P's session settings
  (`transform.backfill`); the two Arrow results must be identical, value for value and bit for bit, and each must
  equal the source under the oracle's rules — compare through `cdc_v2.oracle` with a local DuckDB destination
  (`Destination(conn=...)`). Insert, update and delete records; a keyless table (old image `b<i>`); a key change
  (`k<i>`). Every fallback type of `docs/research/debezium.md` section 7 gets a column too.
- Session settings: a role-level `ALTER ROLE … SET` of `DateStyle`, `IntervalStyle`, `extra_float_digits`,
  `bytea_output`, `TimeZone` and `lc_monetary` to other values changes nothing in either path (package P pins
  them for reads, package E for the stream; you prove the result).
- Property test (`hypothesis`, local DuckDB only): random values per family → Postgres-style text by a small
  printer of your own → `transform.backfill` → compare with the generating value. A second, slower variant under
  `localpg` lets Postgres print.
- Negative controls: a deliberately wrong conversion (a float printed and parsed with too few digits, a BC date
  through the plain cast, `-0` normalised) must be caught by the cross-path test or the oracle; keep them as
  tests.
- Isolation: the transform's connection has no MotherDuck extension loaded and no attached database besides its
  own; asserted in a test.
- Thread safety: several transformers convert at once (the backfill's use) and give the same results as one.
- Nothing is left behind: no cluster running, no temporary directory.

## Deliver

- `src/cdc_v2/transform/`, the tests, `docs/type_mapping.md` (the deliverable of SPEC 2.4, baseline part: per
  source type the destination type, both forms, the stream form, the backfill form, special values, and the test
  that proves the row; fallback types listed as "text, P4"), `docs/design/transform.md` (the interface as built,
  how a caller handles `WideningNeeded`, the statements, speed and memory, contract notes), evidence under
  `evidence/p3/transform/`.
- In your report: every value for which the two paths differed and what you did; every DuckDB behaviour that
  surprised you (casts, ranges, printing); every finding for package E (a stream form that loses information)
  and for package P (a session setting that is missing); and what packages S, B and A must know that C3 and C3a
  do not say yet.
