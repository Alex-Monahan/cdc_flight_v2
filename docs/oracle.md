# Oracle and tier-T1 test harness

Status: first work item of P3. The pipeline does not exist yet; everything here is test tooling that later tests
use to decide whether source and destination are equal (SPEC §3 "Oracle"), and to give each test an isolated source
and destination.

| Part | Code | Tests | Evidence |
|---|---|---|---|
| Oracle | `src/cdc_v2/oracle/`, `tools/oracle` | `tests/test_oracle_*.py`, `tests/t1/test_oracle_t1.py`, `tests/t1/test_oracle_rate_t1.py` | `evidence/p3/oracle/{equal,mutations,canonical_postgres,cli,rate,duckdb_equal_update}.*` |
| Position tie (C8) | `oracle/comparison.py`, `oracle/position.py` | `tests/test_oracle_unit.py`, `tests/t1/test_oracle_t1.py`, `tests/t1/test_harness_t1.py` | `evidence/p3/oracle/lost_transaction.json` |
| T1 harness | `src/cdc_v2/testing/{pg,md,loader,datasets}.py`, `tests/conftest.py` | `tests/test_harness_unit.py`, `tests/t1/test_harness_t1.py` | `evidence/p3/oracle/harness.json` |
| Redaction | `src/cdc_v2/evidence.py`, used by `cdc_v2.testing.harness_evidence.write_evidence` | `tests/test_evidence.py`, `tests/harness/test_harness_evidence.py` | every file under `evidence/p3/oracle/` |
| Workload generator, observer | `src/cdc_v2/testing/{workload,observer}.py` | `tests/test_workload.py`, `tests/t1/test_observer_t1.py`, `tests/t1/test_harness_t1.py` | `evidence/p3/oracle/{observer,harness}.json` |

```sh
uv run -q pytest -q                                   # hermetic (T0), no server
export CDC_V2_PG_HOME=~/.cache/cdc_flight_v2/pg-<pkg> CDC_V2_PG_PORT_BASE=<base> CDC_V2_MD_PREFIX=cdc_v2_t_<pkg>_
tools/with-env uv run pytest -m t1 tests/t1           # T1: local Postgres + MotherDuck, isolated per test
CDC_V2_ORACLE_BENCH_ROWS=5000000 tools/with-env uv run pytest -m t1 tests/t1/test_oracle_rate_t1.py
```

This package's own runs use `CDC_V2_PG_HOME=~/.cache/cdc_flight_v2/pg-o`, `CDC_V2_PG_PORT_BASE=55520`,
`CDC_V2_MD_PREFIX=cdc_v2_t_o_`.

## 1. The oracle

```python
from cdc_v2.oracle import Source, Destination, OracleConfig, compare
q = workload.quiesce()                   # or workload.pause(): an int position (C0)
result = compare(source_db.source(), dest_db.destination(), OracleConfig(tables=["public.t"]),
                 position=q, slot_name="cdc_v2_slot")
result.equal          # bool
result.to_dict()      # positions, per table: rows on both sides, missing/extra columns, missing/extra/different rows
```

```sh
tools/with-env uv run python tools/oracle --source-port 55520 --source-db cdc_v2_t_x --dest-db cdc_v2_t_o_x \
    --position 0/3000EE8 --slot-name cdc_v2_slot [--wait 120] [--json result.json] [--tables public.a,public.b]
tools/with-env uv run python tools/oracle ... --unpositioned          # never a gate result; the output says so
```

Exit status 0 equal, 1 different, 2 usage or error, 3 refused: a side did not reach the position (or a deadline
passed). The password comes from the localpg home (`--pg-home`, `--source-role`); for stage 2
`--source-conninfo-env VAR` reads a conninfo string from a named environment variable. Output is redacted. The
source port must be named, and 5432 is refused. The destination must be named `cdc_v2_*`; it is opened in single
attach mode.

### 1.1 How a comparison runs

0. **Position (contract C8).** A comparison is tied to a source position `Q`, normally from `Workload.quiesce()`
   or `Workload.pause()` (`pg_current_wal_insert_lsn()` once every writer is idle). `compare(..., position=Q,
   slot_name=S)`:
   * Source, before the snapshot is taken: on a standby, `pg_last_wal_replay_lsn()` is awaited until it reaches
     `Q` (within `position_wait_s`, default 60 s); on a primary, `pg_current_wal_insert_lsn()` is read and must be
     at or past `Q`. Because the position is read before the snapshot exists, the snapshot contains at least it.
   * Destination, inside its read transaction: the applied position of slot `S` (the reader is a parameter,
     `applied_position(conn, slot_name)`; package S provides the real one, `stand_in_applied_position` reads the
     row `S` of `_cdc.stream_state` until then) must be at or past `Q`. It is not awaited inside the read
     transaction; `wait_for_applied` waits beforehand.
   * A side short of `Q` raises `PositionNotReached`: the comparison is refused, never reported equal.
   * `Q`, the source position and the destination's applied position are in the result and in every evidence file
     written from it. A comparison without a position must be asked for with `unpositioned=True`; its result has
     `positioned: false`, and the summary says `UNPOSITIONED, not a gate result`.
   * A comparison that compares no table (an empty `tables` list, or a source without user tables) raises.
   * Why: if the pipeline loses the last transaction T and the oracle reads a standby that has not replayed T,
     both sides lack T and an unpositioned comparison says EQUAL. `lost_transaction.json` shows exactly that on a
     private cluster with replay paused: unpositioned EQUAL; positioned refused after 3 s; after replay resumed,
     DIFFERENT with the lost row as missing.
1. **One source snapshot.** A coordinator transaction (`REPEATABLE READ, READ ONLY`) runs `pg_export_snapshot()`;
   every worker connection imports it with `SET TRANSACTION SNAPSHOT`. The T1 fixtures read the primary by default
   (`source_db.source()`; `source(node="replica")` is the option): on a standby the oracle's long snapshot and its
   table locks can stall replay (a replayed `ACCESS EXCLUSIVE` lock waits up to 30 s, also for a stream that decodes
   there) and then cancel the oracle with SQLSTATE 40001. The whole comparison is then started again, at most
   `conflict_retries` (3) times.

   **On a standby whose server has the export defect** (`cdc_v2.pg.cut.export_defect(server_version_num)`:
   every release up to 16.15, 17.11 and 18.6; `docs/design/cut.md` section 7) a snapshot exported there loses
   its list of running transactions while a primary transaction is open whose first write was inside a
   subtransaction, or that has more than 64 of them. An importer then sees transactions the exporter does not
   see, and marks the rows of a still-running transaction as aborted, for every later reader of that standby.
   The pipeline's own reads would then miss those rows, and the oracle, reading the same spoiled rows, could say
   EQUAL falsely. So there the coordinator keeps a snapshot only if it is quiet (`xmin = xmax` in
   `pg_current_snapshot()`: nothing to lose), taking another every 50 ms for at most `quiet_wait_s` (default
   5 s); if none comes, the coordinator reads alone on its own snapshot, the scan jobs take turns on that one
   connection, and nothing is exported or imported. The position is still read before each snapshot. On a
   primary and on a server with the fix nothing changes. `Result.source_snapshot` says which way was taken
   (`exported`, `quiet exported`, `one connection`); `snapshot_way` is the choice, tested in
   `tests/test_oracle_unit.py`. Live (`tests/cut/test_cut_live.py`, marker `localpg`, the cut's private
   cluster; `evidence/p3/cut/oracle_standby.json`): with such a transaction open the oracle took one connection
   and was right both ways, and after the commit an ordinary standby session saw the row. The oracle before
   this change, run the same way, left that row invisible to every reader of the standby and then reported an
   extra destination row that the source does have (`oracle_standby_before_the_fix.json`). The version table
   is shared with the pipeline: if it is wrong, both are.
2. **One destination transaction** on one connection (`BEGIN TRANSACTION` ... `ROLLBACK`). A caller may pass its own
   connection with an open transaction (`Destination(conn=..., in_transaction=True)`); the mutation tests use this.
   Every MotherDuck statement has a deadline (`md_statement_s`, default 1,800 s, never past the comparison's
   deadline, `timeout_s` 3,600 s or a `Deadline` passed in): the DuckDB client has no statement timeout, so
   `oracle/mdexec.py` interrupts the connection when the deadline passes and raises `DeadlineExceeded`. Postgres
   statements carry `statement_timeout`.
3. **Catalogs** of both sides are read inside that snapshot and that transaction. Source: every table of every
   non-system schema (`relkind` `r` that is not a partition, and `p`), with its primary key; a domain column (also
   a domain over a domain, and an array of a domain) is typed by its base type. Destination: every table of the
   database except `information_schema`, `pg_catalog` and every schema whose name starts with `_cdc` in any letter case (on the source too: C0). A source table without
   a destination table, a destination table that no source table maps to, and a missing or extra column are
   differences. Tables map by identical schema and name unless `table_map` says otherwise; with `tables=[...]` only
   those are compared and extra destination tables are not reported. Gate tests list their tables (C8).
4. **Checksum pass.** For each table, both servers compute, in one scan, `count(*)` and `sum(row hash)` per bucket:
   * *row text*: the canonical text of each compared column (section 1.2), each wrapped in a token: `N` for NULL,
     else `V<bytes>:<text>`. Tokens make NULL differ from the empty string, and keep `'a'||'bc'` apart from
     `'ab'||'c'`.
   * *row hash*: the first 60 bits of `md5(row text)` (both servers compute the same number; unit-tested).
   * *bucket*: the hash of the key text modulo the bucket count. The key text is the tokens of the primary key, or
     for a table without a primary key the whole row text, so identical rows share a bucket.
   * The sums are exact (`numeric` in Postgres, `HUGEINT` in DuckDB). Equal (count, sum) per bucket means equal
     multisets with overwhelming probability; duplicates count, order does not.
   * The source scan is split into `ctid` ranges (the last one open-ended) that run on `source_workers` snapshot
     connections; the destination scan runs at the same time.
   * Bucket count: `ceil(estimated rows / chunk_rows)`, at most 65,536 (`chunk_rows` default 100,000).
5. **Row pass**, only for buckets whose count or sum differ, at most `max_diff_buckets` (16) per table and
   `bucket_batch` (4) at a time: the destination's row hashes in those buckets are counted (integers only); the
   source rows of those buckets are streamed and matched against the counter, so only unmatched rows are kept; the
   hashes left over are extra rows, and only their values are fetched from the destination. For a table with a key,
   a missing and an extra row with the same key are one row whose values differ (listed column by column). The
   sample holds at most `sample_limit` (10) rows per kind. If more buckets differ than are examined, `truncated` is
   true and the row counts are lower bounds.

Memory: the client holds one (count, sum) per bucket, the destination row hashes of at most `bucket_batch` buckets,
and the values of differing rows (at most `row_fetch_limit` per side and batch; counts stay exact beyond it).
Measured peak client memory on the 5M-row benchmark is in section 1.6.

### 1.2 Canonical forms

Equality is defined per Postgres type family by a canonical text that the oracle owns
(`src/cdc_v2/oracle/canonical.py`). It imports nothing from the pipeline. Each family is one `TypeRule` with SQL for
Postgres, SQL for DuckDB (given the destination column's actual type), and a Python reference that both SQL
builders are tested against: the DuckDB side hermetically in a local DuckDB, including property-based tests (150
random doubles, 150 random float32 values, 100 decimals, 100 texts, 100 timestamps per run) and 8,000 fixed random
doubles and their float32 roundings; the Postgres side in T1 (`canonical_postgres.json`, 45 values, and 40,000
fixed random doubles).

| Family | Postgres types | Canonical form | Destination types accepted |
|---|---|---|---|
| integer | smallint, integer, bigint | decimal digits with an optional minus sign: `-42` | integer types, `DECIMAL(p,0)`, `VARCHAR` (text must already be canonical) |
| boolean | boolean | `t` or `f` | `BOOLEAN` |
| numeric | numeric | plain decimal, trailing fractional zeros removed (`1.50` -> `1.5`, `100.00` -> `100`); `NaN`, `Infinity`, `-Infinity`. Value equality: the display scale of a Postgres value is not compared | integer types, `DECIMAL(p,s)`, `VARCHAR` (for the text member of `UNION(DECIMAL, VARCHAR)`) |
| double precision | double precision | the IEEE-754 bits as 16 lowercase hex digits (`1.5` -> `3ff8000000000000`, `-0` -> `8000000000000000`); `NaN` (any payload), `Infinity`, `-Infinity` | `DOUBLE`; integer types, `DECIMAL`, `FLOAT` (converted to `DOUBLE`) |
| real | real | the value converted exactly to double precision, then as double precision (`0.1::real` -> `3fb99999a0000000`) | `FLOAT`; `DOUBLE` only when the value is exactly a float32 value; integer types, `DECIMAL` |
| text | text, character varying, name, citext | the text itself (an empty string is not NULL) | `VARCHAR`, `JSON` |
| character | character, bpchar | the text without trailing spaces (Postgres `char(n)` semantics) | `VARCHAR` (trailing spaces removed) |
| bytea | bytea | lowercase hex of the bytes, no prefix | `BLOB` |
| date | date | days since 1970-01-01 (`19724`); `infinity`, `-infinity` | `DATE` |
| timestamp | timestamp without time zone | microseconds since 1970-01-01 00:00:00, read as wall-clock time; `infinity`, `-infinity` | `TIMESTAMP` |
| timestamptz | timestamp with time zone | microseconds since 1970-01-01 00:00:00 UTC (the instant; the session time zone plays no part); `infinity`, `-infinity` | `TIMESTAMP WITH TIME ZONE` |
| time | time without time zone | microseconds since midnight (`24:00:00` -> `86400000000`) | `TIME` |
| interval | interval | `<months>:<days>:<microseconds>`, each field signed and kept apart (`1 mon` is not `30 days`); `infinity`, `-infinity` (Postgres 17+) | `INTERVAL` |
| uuid | uuid | lowercase hyphenated form | `UUID`, `VARCHAR` (lowercased) |
| json | json | the exact text Postgres stores (whitespace, key order and duplicate keys included) | `JSON`, `VARCHAR` |
| jsonb | jsonb | Postgres's jsonb output text (keys sorted and de-duplicated by Postgres) | `JSON`, `VARCHAR` |
| fallback (text) | any other | Postgres text output, compared with the destination value cast to `VARCHAR` | any type (cast to `VARCHAR`) |

Rules that apply to every family:

* **NULL** is never a canonical text; the token `N` encodes it, in every position: a column, an array element.
  If a canonical expression itself returns NULL for a value that is not NULL (a rule bug), the value becomes
  `!null-pg` on the source and `!null-md` on the destination, so such a bug can never compare equal.
* **Domains** are compared by the rule of their base type (resolved through domains over domains).
* **Arrays** of any registered scalar type: `[1:n]...` dimensions (empty for an empty array), then `{` tokens of the
  elements in order, separated by `,` `}`. `{1,NULL,3}` -> `[1:3]{V1:1,N,V1:3}`; `{{1,2},{3,4}}` ->
  `[1:2][1:2]{V1:1,V1:2,V1:3,V1:4}`. The destination side is a DuckDB `LIST` (nested for more dimensions). An array
  with a lower bound other than 1 cannot be held by a list and never matches.
* **`UNION` destination columns** (SPEC 2.4, R9): each member is canonicalised by the source family's rule for that
  member's type, chosen by `union_tag`. So `UNION(n DECIMAL(38,10), s VARCHAR)` holding `1.5000000000` in `n`
  and `NaN` in `s` gives `1.5` and `NaN`, equal to Postgres `1.50` and `NaN`. This settles open point 3 of
  `docs/design/core.md` (canonical comparison form for UNION and NaN): values of different members compare unequal
  in DuckDB, but their canonical texts can be equal; NaN has the canonical text `NaN` on both sides, so NaN equals
  NaN, and `-0` stays different from `0`. A union whose active member holds NULL is **not** NULL in DuckDB
  (`IS NULL` is false); the oracle reports it as a difference (canonical `!null-md`).
* A destination type that a family does not accept gives a text that starts with `!<TYPE>:` and never equals a
  canonical form: the value is reported as different, never silently compared as text.
* Source columns whose type has no rule use the fallback and are listed in the result (`fallback_columns`).

Adding a type (ranges, enums, composites, money, inet, ...): add one `TypeRule` to `RULES` with its Postgres type
names, the two SQL builders and the Python reference, add values to `CASES` in `tests/test_oracle_canonical.py` and
to `PG_VALUES` in `tests/t1/test_oracle_t1.py`, and add its row to the table above (a unit test checks that every
family is in this table). Arrays and `UNION` members of the new type then work without more code.

### 1.3 Waiting for an applied position

`wait_for_applied(md_conn, Q, deadline, slot_name, applied_position=...)` polls until the applied position of the
slot is at or past `Q`, and raises `cdc_v2.deadline.DeadlineExceeded` when the `Deadline` passes first. Positions
are ints; text `X/Y` goes through `cdc_v2.lsn`. The reader is a parameter with S's signature
(`applied_position(conn, slot_name) -> int | None`); until S is merged, `stand_in_applied_position` reads
`applied_lsn` of row `slot_name` of `_cdc.stream_state` (`UBIGINT`, C0; text `X/Y` is accepted too). Tests without a
pipeline record the position they stand in for with `cdc_v2.testing.md.write_stand_in_position`.
`tools/oracle --wait SECONDS` waits before comparing. `Q` is reached once the pipeline has applied a transaction that
committed at or after it; the 10 s heartbeat of SPEC 4.4 provides one when nothing else is written.

### 1.4 What the tests show (claim)

**Claim: the oracle reports equal data as equal and detects every difference class it was given.** A source with
every supported type family and its edge values (all-NULL row, boundaries, empty strings, `-0`, NaN, infinities,
`24:00:00`, intervals with months, numerics too wide for `DECIMAL(38,10)`, arrays with NULL elements, unicode), a
keyless table with identical rows and a 3,000-row lineitem-shaped table spread over 6 buckets was copied to
MotherDuck by the test loader and reported equal (`equal.json`), also when read from the replica. Then 60
mutations were applied to the destination one at a time (`mutations.json`, `all_detected: true`): a missing row, an
extra row, a duplicated key, a wrong number of identical rows in the keyless table (one less, one more, also for
identical rows with NULLs), the empty string turned into NULL, a missing and a changed row in the multi-bucket
table; one changed value in each family: every integer width, boolean, `numeric(12,4)` in the last digit, the
`DECIMAL` and the `VARCHAR` member of the union numeric (NaN to Infinity, NaN to NULL, a 39-digit value in the last
digit), real and double by one unit in the last place, `-0` versus `0`, NaN versus a number and back, the sign of
infinity, text NULL versus empty string, one character, char(n), bytea, date by one day and infinity, time, timestamp
and timestamptz by one microsecond, `1 mon` versus `30 days`, uuid, json whitespace, jsonb, each array family
(changed element, dropped NULL element, empty string versus NULL element, `-0` element, one microsecond); a missing
column, an extra column, a missing table, an extra table and a boolean column whose destination type became integer.
Each was reported as exactly the expected kind (missing / extra / different, with the right key and the one
changed column). Two non-differences were not reported: trailing spaces in `char(n)` and the display scale of a
numeric (`1.50` versus `1.5000`). Tables in `_cdc` and `_cdc_stage_*` are ignored. The command line returns 0 and 1
for the same two states (`cli.txt`). Every one of these comparisons is positioned (the loaded dataset's `Q` and a
stand-in applied position). A destination whose applied position is short of `Q` (or missing), and a source
primary short of `Q`, are refused; a source without tables is refused; columns of domains (numeric(10,2), a domain
over it, text, an array of a text domain) compare by their base types without fallback.

### 1.5 Findings for the apply design (core.md 6.2, 6.3) and the type mapping

1. **An UPDATE to an equal-comparing value is skipped.** DuckDB skips an `UPDATE` whose new value compares equal to
   the stored value, and then keeps the **old** representation: `-0` stays `-0` after `SET x = 0`, `1 mon` stays
   `1 mon` after `SET x = INTERVAL 30 DAY`, also through `UPDATE ... FROM` (`duckdb_equal_update.json`: locally and
   on MotherDuck, DuckDB 1.5.6). `DELETE` + `INSERT` always writes the new representation.
   Core.md 6.2 step 3 applies partial images with `UPDATE ... FROM`; for real, double and interval columns such an
   update can leave the destination different from the source, and the oracle will report it. Proposed: apply rows
   with such columns by delete and insert, or make the P4 type wave test it. The oracle's mutation tests replace
   rows for this reason.
2. **GROUP BY and PARTITION BY treat distinct values as one.** In DuckDB `-0` and `0` form one group, `NaN` and
   `-NaN` one group, and `1 mon`, `30 days` and `720 hours` one group (checked on DuckDB 1.5.6). Core.md 6.3
   computes the keyless delta with `GROUP BY` all columns and picks the rows to delete with `row_number() OVER
   (PARTITION BY <all columns>)`: a source delete of the row with `0` can then delete the destination row with
   `-0`. The note in 6.3 has the problem backwards: NaN does compare equal to NaN in DuckDB; the risk is distinct
   values that compare equal, and UNION values of different members that compare unequal. The keyless apply needs
   a grouping key that keeps such values apart (for example the canonical text of those columns).
3. **A NULL in a UNION column must be a real NULL.** `union_value(s := NULL)` is a non-NULL union whose member is
   NULL (`IS NULL` is false). The test loader first produced such values; the oracle reports them as different.
4. **DuckDB reads Postgres BC dates wrongly.** `CAST('0044-03-15 BC' AS DATE)` returns 0044-03-15 AD, silently. The
   pipeline must not pass Postgres date text with ` BC` through a DuckDB cast.
5. **Float texts differ between the servers, so floats are compared by their bits.** DuckDB's `FLOAT` text is not
   always the shortest text that reads back as the same float32 (12,084 of 256,329 random values; for example
   `1048576.25`, where `1048576.2` suffices and Postgres prints that). Postgres prints some doubles with 17 digits
   where DuckDB and Python print 16 digits that read back to the same double only by round-half-even (for example
   `-2.0604180037107712e+16` against `-2.060418003710771e+16`; found by the 40,000-value test). A first version of
   the oracle compared shortest texts and would have reported such values as different; it now compares the
   IEEE-754 bits (`float8send` on Postgres, `CAST(DOUBLE AS BIT)` on DuckDB). Loading is not affected: any of these
   texts reads back to the right value. Samples show the bits with the value, for example `1.5 (3ff8000000000000)`.

### 1.6 Rate (claim)

**Claim: the oracle compares a 5M-row lineitem-width table in 23.6 s (211,500 rows/s, checksum pass only), and
finds and reports one changed value in 77 s, with 172 MB peak client memory.** Measured on this laptop (10 cores,
16 GB), source the local primary (1.07 GB table), destination MotherDuck over the public internet, default settings
(100,000 rows per bucket, 50 buckets, 4 source workers), positioned (`rate.json` records `Q` and both sides'
positions); run on this package's own cluster (`pg-o`) while other workers may have been running. Scaled linearly, the checksum pass over a
60M-row table takes about 5 minutes. The row pass costs, per batch of up to four differing buckets, one more source
scan and two more destination scans (hashes first, then the values of the extra rows), which run one after another.
A first version that fetched all rows of the differing bucket from both sides was faster (48 s for the same case)
but peaked at 399 MB; memory now depends on the bucket size and the number of differing rows, not on the table size.

## 2. T1 harness

Markers: `t1` (local Postgres and MotherDuck together), `md`, `jvm`, `localpg`. The default `pytest` run deselects
all of them (`pyproject.toml`) and stays hermetic. T1 fixtures (`tests/conftest.py`) skip with a reason when the
Postgres binaries or the MotherDuck token are missing; `tools/with-env` provides the token.

**Which cluster, which ports, which databases** (docs/design/p3_plan.md section 4, contract C8):

| Variable | Meaning |
|---|---|
| `CDC_V2_PG_HOME`, `CDC_V2_PG_PORT_BASE` | the cluster on which the fixtures create one private database per test: a package's own home and port base (`localpg up` builds it on that base if it does not exist yet) |
| `CDC_V2_MD_PREFIX` | prefix of the MotherDuck databases the fixtures create; it must start with `cdc_v2_t_` and end with `_` (this package: `cdc_v2_t_o_`) |
| `CDC_V2_T1_SHARED=1` | allows the default home (~/.cache/cdc_flight_v2/pg, ports 55432-55439); for the orchestrator's gate runs only |

Without a named home and without `CDC_V2_T1_SHARED=1` the fixtures fail with a message that says what to export.
Naming the default home in `CDC_V2_PG_HOME` is not enough.

| Fixture | Gives | Clean-up |
|---|---|---|
| `source_db`, `make_source_db` | a database `cdc_v2_t_<10 hex>` on that cluster, owned by `cdc_v2_admin`, usable by `cdc_v2_runtime`; `.source()` reads the primary, `.source(node="replica")` the standby | the holder of each logical slot of that database is terminated, the slot dropped on every running node, then `DROP DATABASE ... WITH (FORCE)`; a failed GRANT at creation drops the database again |
| `dest_db`, `make_dest_db` | a MotherDuck database `<CDC_V2_MD_PREFIX><10 hex>`; `.conn` is a single-attach connection, `.connect()` opens another | `DROP DATABASE` |
| `private_cluster` | a primary and replica of their own (`~/.cache/cdc_flight_v2/pg-t1-<base>`) for destructive drills; `.localpg("kill", ...)` etc. | `localpg down`, home removed, port lease released |
| `shared_pg` | the cluster named above (`localpg up` if needed; a no-op when it runs) | none: it is never stopped or changed |
| `evidence` | `evidence("p3/oracle").write_json(name, obj)` / `.write_text(name, text)`: helpers over `write_evidence` | |

* **The fixture cluster is protected.** `cdc_v2.testing.pg.localpg()` refuses `down`, `reset`, `kill`, `promote`,
  `remove`, `set`, `unset`, `follow` and `add-standby` on the default home and on the home named by
  `CDC_V2_PG_HOME`; tests that break a cluster use `private_cluster`.
* **Port allocation for private clusters.** `PortLease.acquire()` walks the blocks 55840, 55848, ... 56088 (32 blocks
  of 8 ports) and takes an exclusive `flock` on `~/.cache/cdc_flight_v2/ports/<base>.lock`. The lock lives as long
  as the lease and dies with the process, so a crashed test never blocks a block for good. A block whose ports are
  in use by anything other than our own stale private cluster is skipped. A default-run test asserts that the
  leased range misses every fixed range (55432-55439 default home, 55440-55459 TCP proxies, 55460-55467 localpg
  self-tests, 55500-55509, 55600-55607, 55620-55823 the package homes). Tested with leases held by another process
  and with two private clusters at once.
* **MotherDuck databases.** CREATE and DROP run on a workspace connection that executes only that statement (as the
  P2 spikes do); everything else uses single attach mode. Nothing lists the account's databases. Every call has a
  deadline (`oracle/mdexec.py`). A ledger file per database (`~/.cache/cdc_flight_v2/md_ledger/<name>`) is written
  before CREATE and removed after DROP; `cdc_v2.testing.md.leftovers()` checks each remaining entry of this
  process's prefix by attaching it by name in a fresh process (a process that attached a database keeps a cached
  handle that still answers after the drop).
* **Temporary files** of a test run (`tmp_path`, DuckDB spill files) live under `~/.cache/cdc_flight_v2/tmp`.
* **Direct loader** (`cdc_v2.testing.loader`, tests only): copies source tables to the destination: one text export
  per column from Postgres, a local newline-delimited JSON file, one `read_json` + cast per column on MotherDuck. It
  shares no conversion code with the oracle. Its type mapping is test-only; unconstrained numeric becomes
  `UNION(n DECIMAL(38,10), s VARCHAR)` by default. The first run of the oracle found three bugs in this loader (NULL
  booleans, NULL arrays and NULL numerics were written as non-NULL values), which were fixed.

**Claim: tests get isolated sources and destinations, and destructive drills cannot touch the fixture cluster.**
`harness.json`: two private databases do not see each other's tables; dropping one removes it and its logical slot;
a private cluster gets a block in 55840-56095, streams to its replica, survives `localpg kill replica` + `up`, and
after `stop()` its home is gone, its ports are closed and its block can be leased again; two private clusters get
different blocks.

### Contract notes (C8)

* The source position on a primary is `pg_current_wal_insert_lsn()`, read before the snapshot; on a standby
  `pg_last_wal_replay_lsn()`, awaited before the snapshot.
* The stand-in reader expects `_cdc.stream_state(slot_name VARCHAR, applied_lsn UBIGINT)`; the column name
  `slot_name` is my reading of "one row per slot name" in S's task. S's reader replaces it through the parameter.
* `fallback_columns` is reported per table; deciding whether a gate test declared them is the gate test's job.

## 3. Redaction and the evidence writer

`cdc_v2.testing.harness_evidence.write_evidence(path, content)` is the one writer of evidence files (C0). It
scrubs registered run directories, the temporary directory, the Python installation, the home directory and the
user name (package H), then applies the redaction rules of `cdc_v2.evidence.Redactor` (this package), then refuses
to write (`EvidenceError`) if an absolute user path, the user name, or anything a redaction rule matches is left.
`cdc_v2.evidence` only redacts; it writes nothing. The rules:

* exact values: every value of `~/.config/cdc_flight_v2/env` and `env.local`, the secret variables of the
  environment (MotherDuck token, salt, Slack webhook, PlanetScale and `PG_*` values), and registered values (the
  fixtures register the generated passwords of every cluster they use). Values of 6 characters or more are removed
  wherever they occur; values of 3 to 5 characters as whole words; numbers and ordinary words (`main`, `public`)
  are kept;
* the home directory, as given and as its realpath, only at a path boundary (a longer path that starts with it is
  not cut), the machine's host name and the operating-system user name;
* patterns: JWTs, PlanetScale tokens and passwords (`pscale_tkn_…`, `pscale_pw_…`, `pscale_oauth_…`), `Bearer`
  tokens, credentials in URLs, `postgres://` URIs, libpq `host=`/`user=`/`password=`/`passfile=` pairs,
  `motherduck_token=` and `token=` parameters, `"password": "..."`-style assignments, e-mail addresses, IPv4 and IPv6
  addresses (loopback kept), host names under provider and private domains.

**Claim: evidence is redacted before it reaches the disk, and refused when it cannot be.** `tests/test_evidence.py`
(15 tests, hermetic, made-up secrets assembled at run time) covers each class above, nested JSON keys and values,
and a writer that refuses when redaction misses something; H's `tests/harness/test_harness_evidence.py` still
passes. Every evidence file of this package was written through `write_evidence`.

## 4. Seeded workload generator and transfers observer

`cdc_v2.testing.workload.Workload(WorkloadConfig(seed=...), connect=..., log_path=...)`.

* **Rounds.** Each round, every writer (own connection, own thread) runs `txns_per_round` transactions; then the
  driver may run one DDL group or one TRUNCATE while the writers wait. Between rounds the source is quiet.
* **Reproducible.** Statements come from the seed and an in-memory model, never from database reads. Writer `w`
  owns keys `k * writers + w + 1` and the keyless rows with `w = w`; transfers only add amounts, which commute. So
  the state after a round does not depend on the interleaving, and a seed always gives the same statements per
  writer. `Workload(cfg).plan(n)` produces them without a database.
* **Replay log.** Every executed transaction and driver step is appended to a JSON-lines file (header with seed and
  config, setup, `txn` entries with writer, round, sequence, shape, statements and outcome, `driver` entries,
  `quiesce` entries with the WAL position). `replay(log, connect)` runs it again in log order on one connection.
* **Quiesce points.** `quiesce()` between rounds returns `pg_current_wal_insert_lsn()` as an int (C0). In continuous mode, `start()` runs
  rounds back to back, `pause()` waits for the round to end, keeps the writers idle and returns the position, and
  `resume()` continues.
* **Feature switches** (`Features`, `Features.only(...)`): single-row, multi-row and multi-table transactions,
  savepoints with `ROLLBACK TO SAVEPOINT`, rolled-back transactions, a configurable large transaction (insert plus
  update of `large_txn_rows` rows), insert / update / delete, primary-key updates (chains `b -> new, a -> b` and swaps
  through a temporary key, in one transaction), updates that leave a large TOASTed text column unchanged
  (`w_toast`), a keyless table with identical rows (`REPLICA IDENTITY FULL`; one of several identical rows is
  deleted or updated by `ctid`), TRUNCATE of one or several tables, and a DDL group (add column, add column with a
  default, drop, rename, widening type change int -> bigint, numeric(12,2) -> numeric(16,4), text -> varchar(200),
  create table, drop table).
* **Transfers.** Tables `w_xfer_0..2` hold accounts with a constant global sum; a transfer moves amounts between
  2-4 accounts in different tables, updated in one global lock order (no deadlocks).
* **Observer.** `TransfersObserver(connect, tables, expected_sum)` polls the sum on the destination from its own
  connection, one statement (one transaction) per observation, and counts observations, violations and
  unavailable reads. `apply_transfers(..., split=True)` is a stand-in applier that commits each source transaction
  in two destination transactions.

**Claim: a seed reproduces a workload exactly.** Hermetic: the same seed gives byte-identical plans, different seeds
differ, rolled-back and savepoint-discarded work never enters the model, each feature group appears when on and is
absent when off. T1 (`harness.json`, `workload_replay`): 3 writers, 8 rounds with every feature on; the statements
per writer in the log equal the planning-only run of the same seed; replaying the log into a second database gives a
database the oracle finds equal to the original (loaded to MotherDuck).

**Claim: the observer catches a split source transaction.** `observer.json`: transfers applied to MotherDuck as
whole transactions gave 34 observations, all with the expected sum, and the oracle found the destination equal;
the same kind of transfers applied as two commits each gave 143 violations in 161 observations (numbers of the final run; they vary with timing), while the final
state was again equal: only the observer sees the split.

## 5. Not done, limits

* Partitioned tables are compared through the parent (one scan, no `ctid` split); per-partition mapping (SPEC 7.3)
  needs a `table_map`.
* Columns that later features add (`_cdc_deleted`, history tables, PII-excluded columns) are reported as extra or
  missing until those features give the oracle a mapping; the hooks are `table_map` and the destination schema
  ignore list.
* After a type change under R9 (a column becomes `UNION(old, new)`) existing values keep their old member while
  Postgres rewrote them in the new type. The cross-type acceptance above covers lossless cases; the exact rule is
  for the P4 schema wave.
* `json` is compared by exact text. If the pipeline cannot keep Postgres's text, the rule must change in one place.
* The workload's large transaction is sized by `large_txn_rows`; the 10M-row case of SPEC 1.3/5.1 is a
  configuration, not yet a test.
