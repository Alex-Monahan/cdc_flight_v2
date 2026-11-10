# Type mapping (SPEC 2.4, baseline of P3)

The destination type of every source column, and how a value gets there on both paths. Built by package Y
(`src/cdc_v2/transform/`); the design is `docs/design/transform.md`. "Stream form" is the engine's transport form
(`docs/design/engine.md` section 3). "Backfill form" is the text that Postgres prints under the read sessions'
settings (`docs/design/source.md` section 1: DateStyle `ISO, YMD`, IntervalStyle `postgres`, TimeZone `UTC`,
`extra_float_digits = 3`, `bytea_output = hex`, `lc_monetary = C`). "Equal" is the oracle's rule
(`docs/oracle.md` 1.2).

A family with two forms has a **native form** and a **wide form**. A column starts native; a value that the native
type cannot hold raises `WideningNeeded` (no value in it) and the caller moves the column to the wide form
(`TablePlan.widened`, `TablePlan.widen_sql`). Every wide form is `UNION(n <native>, t VARCHAR)`: `n` holds every
value that fits, `t` the Postgres text of every value that does not. A source NULL is a real NULL, never a member
that holds NULL.

Tests named in the last column: `V` = `tests/transform/test_transform_vectors.py` (hand vectors, exact values),
`P` = `test_transform_property.py` (hypothesis, both paths), `X` = `test_transform_crosspath.py` (Postgres 18 and
16, engine and backfill, identical results, oracle), `L` = `test_transform_localpg.py` (Postgres prints, 3,000
values per family). Evidence: `evidence/p3/transform/`.

## Baseline (native in P3)

| Source type | Destination (native) | Wide form | Stream form | Backfill form | Special values | Tests |
|---|---|---|---|---|---|---|
| `boolean` | `BOOLEAN` | – | `bool` (passed on) | `t` / `f` | – | V, X |
| `smallint`, `integer`, `bigint` | `SMALLINT`, `INTEGER`, `BIGINT` | – | `int16`/`int32`/`int64` (passed on) | decimal text; must print back the same (no `1.5` → 2) | min, max | V, P, X |
| `real` | `FLOAT` | – | `float32` (passed on, bits kept) | shortest text, parsed once (correctly rounded) | NaN (one NaN in Postgres: `7fc00000` from either path), ±Infinity, −0, subnormals, max | V, P, X, L |
| `double precision` | `DOUBLE` | – | `float64` (passed on, bits kept) | as `real` | NaN `7ff8000000000000`, ±Infinity, −0, `5e-324`, max | V, P, X, L |
| `numeric(p, s)`, 0 ≤ s < p ≤ 38 | `DECIMAL(p, s)` | `UNION(n DECIMAL(p,s), t VARCHAR)` | `numeric_text` | the same plain text | NaN → wide (`t` = `NaN`); a constrained numeric cannot hold ±Infinity (Postgres refuses it) | V, P, X, L |
| `numeric(p, s)`, s ≥ p (Postgres 15+) | `DECIMAL(s+1, s)` (s+1 ≤ 38) | as above | as above | as above | DuckDB prints `DECIMAL(s, s)` without the leading zero, so one more digit of width | V, X |
| `numeric(p, s)`, s < 0 (Postgres 15+) | `DECIMAL(p−s, 0)` (p−s ≤ 38) | as above | as above | as above | | V, X |
| `numeric` (no typmod), `numeric(p, s)` with p > 38 or a scale one DECIMAL cannot hold | `DECIMAL(38, 10)` (with p > 38: `DECIMAL(38, min(max(s,0),37))`) | `UNION(n DECIMAL(38,10), t VARCHAR)` | as above | as above | fits ⇔ not NaN/±Infinity, ≤ 28 digits before the point (leading zeros not counted) and ≤ 10 after it (trailing zeros not counted); otherwise `t` = the text (131,072 digits, `1e300 * 1e300`, NaN, ±Infinity) | V, P, X, L |
| `text`, `varchar(n)`, `char(n)`, `name`, `"char"`, `citext` | `VARCHAR` | – | `text` (passed on) | the text (passed on) | `char(n)` padding as printed; `"char"` octal escapes as printed; empty ≠ NULL; any length (100 MB measured) | V, P, X |
| `bytea` | `BLOB` | – | `bytes` (passed on) | `\x` + hex (even length checked) | empty; 100 MB measured | V, P, X, L |
| `date` | `DATE` | – | `date_pg_days` | `YYYY-MM-DD[ BC]`, taken apart here (never DuckDB's date cast) | ±infinity (DuckDB's ±infinity, Arrow int32 max and −max); 4714-11-24 BC to 5874897-12-31: all fit | V, P, X, L |
| `timestamp` | `TIMESTAMP` | `UNION(n TIMESTAMP, t VARCHAR)` | `timestamp_pg_us` | `YYYY-MM-DD HH:MM:SS[.f][ BC]`, taken apart here | ±infinity native; 294247-01-10 04:00:54.775807 to 294276-12-31 (above DuckDB's range) → wide, `t` = Postgres text (the stream prints it the same way) | V, P, X, L |
| `timestamptz` | `TIMESTAMP WITH TIME ZONE` (an instant, UTC) | `UNION(n TIMESTAMPTZ, t VARCHAR)` | `timestamptz_pg_us` | as `timestamp` with `+00` | as `timestamp`; `t` ends in `+00` | V, P, X, L |
| `time` | `TIME` | – | `time_us` (reinterpreted, no copy) | `HH:MM:SS[.f]` | `24:00:00` fits (DuckDB and Arrow hold 86,400,000,000 µs) | V, P, X, L |
| `timetz` | `TIME WITH TIME ZONE` | – | `text` (same text) | `HH:MM:SS[.f]±HH[:MM[:SS]]` | offsets ±15:59:59 (DuckDB's range = Postgres's), `24:00:00`; built from the parts, not by DuckDB's cast | V, P, X, L |
| `interval` | `INTERVAL` | `UNION(n INTERVAL, t VARCHAR)` | `interval_text` | IntervalStyle `postgres` (`-1 years -2 mons +3 days -04:05:06.789`, `2562047788:00:54.775807`, `00:00:00`) | months, days, µs kept apart, each with its own sign; \|µs\| > 9,223,372,036,854,775 (≈ 2,562,047 h; Arrow's nanoseconds overflow) → wide; `infinity`/`-infinity` (17+) → wide; `t` = ISO 8601 text on both paths (`PT2562047788H54.775807S`; `infinity` as printed) | V, P, X, L |
| `uuid` | `UUID` | – | `text` | same text | – | V, P, X |
| `json`, `jsonb` | `VARCHAR` | – | `text` (passed on) | passed on | the text as printed: duplicate keys, key order, huge numbers. Not DuckDB `JSON`: it refuses `"\ud800"`, which Postgres `json` holds (test `test_lossless_arrow_types_for_uuid_timetz_and_json_text`) | V, X |
| enums | `VARCHAR` | – | `text` | label | **Point for the owner (H-033)**: a DuckDB `ENUM` cannot follow a label that is renamed or a reorder without rewriting the column, so labels are text | X |
| domains | the base type's mapping | | | | a domain's own typmod is not in the catalog value; `numeric(10,2)` domains are planned as unconstrained numeric (lossless) | V, X |

Keys: a scalar `interval` key (also through a domain) on a source of Postgres 17 or later is not injective in the
stream (`TablePlan.ambiguous_key`); see `docs/design/transform.md`, "Keys".

Every family: NULL is a real NULL on both paths (`test_source_null_is_a_real_null_in_a_union_column` for the wide
forms); every family can be a key column (the transform does not treat keys differently; `k<i>` and `b<i>` are
converted like `a<i>`, `test_old_image_old_key_and_unchanged_columns`, and the `mix` table of X).

## Arrow types handed on

`BOOLEAN` bool · `SMALLINT`/`INTEGER`/`BIGINT` int16/int32/int64 · `FLOAT`/`DOUBLE` float32/float64 · `DECIMAL(w,s)`
decimal128(w,s) · `VARCHAR` string · `BLOB` binary · `DATE` date32 · `TIMESTAMP` timestamp[us] · `TIMESTAMPTZ`
timestamp[us, tz=UTC] · `TIME` time64[us] · `TIMETZ` `arrow.opaque(time_tz, DuckDB)` over fixed_size_binary(8) ·
`INTERVAL` month_day_nano · `UUID` `arrow.uuid` · `UNION(n X, t VARCHAR)` sparse_union<n: X = 0, t: string = 1>
(a NULL: tag 0 with the child NULL; DuckDB reads it as a real NULL). These are DuckDB's own lossless Arrow
mappings (`arrow_lossless_conversion = true`), except BOOLEAN: DuckDB's lossless export gives `arrow.bool8`, which
the transform turns into the standard bit-packed boolean that the stream also delivers.

## Fallback (text, P4)

Every other type is `VARCHAR` holding exactly the text Postgres prints under the read sessions' settings. Columns of
all of them are in X; the stream form equals that text except where noted.

| Source type | Stream form vs Postgres text | Note |
|---|---|---|
| arrays (any element type), domains over arrays | equal (also arrays of `interval`) | |
| `money` | `money_text` (plain decimal) → formatted as Postgres prints it (`$1,234.50`, `-$0.01`) | equal |
| `bit(n)`, `varbit` | equal | `B'101'` ≠ `B'0101'` kept |
| `hstore` | equal | |
| composite types | equal (also with an `interval` field) | |
| ranges, multiranges | equal | |
| `inet`, `cidr`, `macaddr`, `macaddr8`, `xml`, `tsvector`, `tsquery`, `pg_lsn`, `ltree`, `oid` | equal (`oid`: int64 → decimal text) | |
| **geometric** (`point`, `box`, …) | **differs**: Debezium passes pgjdbc's objects, printed with Java's float formatting | finding for package E; not lossless in the stream yet |
| a scalar `interval` above 2^31 hours | the stream cannot deliver it (the driver fails; `docs/design/engine.md` 10) | the backfill delivers it (wide form) |
| a scalar `interval` `infinity` (Postgres 17+) | the stream delivers a zero interval, marked by `transform.ambiguous` | plan C3 "Ambiguous zero interval" (repair in P4) |
