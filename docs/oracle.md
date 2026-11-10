# Oracle and tier-T1 test harness

Status: first work item of P3. The pipeline does not exist yet; everything here is test tooling that later tests
use to decide whether source and destination are equal (SPEC §3 "Oracle"), and to give each test an isolated source
and destination.

| Part | Code | Tests | Evidence |
|---|---|---|---|
| Oracle | `src/cdc_v2/oracle/`, `tools/oracle` | `tests/test_oracle_*.py`, `tests/t1/test_oracle_t1.py`, `tests/t1/test_oracle_rate_t1.py` | `evidence/p3/oracle/{equal,mutations,canonical_postgres,cli,rate}.*` |
| T1 harness | `src/cdc_v2/testing/{pg,md,loader,datasets}.py`, `tests/conftest.py` | `tests/test_harness_unit.py`, `tests/t1/test_harness_t1.py` | `evidence/p3/oracle/harness.json` |
| Evidence writer | `src/cdc_v2/evidence.py` | `tests/test_evidence.py` | every file under `evidence/p3/oracle/` |
| Workload generator, observer | `src/cdc_v2/testing/{workload,observer}.py` | `tests/test_workload.py`, `tests/t1/test_observer_t1.py`, `tests/t1/test_harness_t1.py` | `evidence/p3/oracle/{observer,harness}.json` |

```sh
uv run -q pytest -q                                   # hermetic (T0), no server
tools/with-env uv run pytest -m t1                    # T1: local Postgres + MotherDuck, isolated per test
CDC_V2_ORACLE_BENCH_ROWS=5000000 tools/with-env uv run pytest -m t1 tests/t1/test_oracle_rate_t1.py
```

## 1. The oracle

```python
from cdc_v2.oracle import Source, Destination, OracleConfig, compare
result = compare(Source(host="localhost", port=55432, dbname=..., user=..., password=..., sslmode="verify-full",
                        sslrootcert=...),
                 Destination("cdc_v2_..."), OracleConfig())
result.equal          # bool
result.to_dict()      # per table: rows on both sides, missing/extra columns, missing/extra/different rows, sample
```

```sh
tools/with-env uv run python tools/oracle --source-port 55432 --source-db cdc_v2_t_x --dest-db cdc_v2_t_x \
    [--json result.json] [--wait-lsn 0/3000EE8] [--tables public.a,public.b] [--chunk-rows 100000] [--workers 4]
```

Exit status 0 equal, 1 different, 2 usage or error, 3 `--wait-lsn` not reached. The password comes from the localpg
home (`--pg-home`, `--source-role`); for stage 2 `--source-conninfo-env VAR` reads a conninfo string from a named
environment variable. Output is redacted. The source port must be named, and 5432 is refused. The destination must
be named `cdc_v2_*`; it is opened in single attach mode.

### 1.1 How a comparison runs

1. **One source snapshot.** A coordinator transaction (`REPEATABLE READ, READ ONLY`) runs `pg_export_snapshot()`;
   every worker connection imports it with `SET TRANSACTION SNAPSHOT`. The result records the snapshot's WAL
   position (`pg_current_wal_lsn()` on a primary, `pg_last_wal_replay_lsn()` on a standby). Reading from the
   replica works (`test_compare_reading_from_the_replica`).
2. **One destination transaction** on one connection (`BEGIN TRANSACTION` ... `ROLLBACK`). A caller may pass its own
   connection with an open transaction (`Destination(conn=..., in_transaction=True)`); the mutation tests use this.
3. **Catalogs** of both sides are read inside that snapshot and that transaction. Source: every table of every
   non-system schema (`relkind` `r` that is not a partition, and `p`), with its primary key. Destination: every
   table of the database except the schemas `_cdc`, `_cdc_stage*`, `information_schema` and `pg_catalog`.
   A source table without a destination table, a destination table that no source table maps to, and a missing or
   extra column are differences. Tables map by identical schema and name unless `table_map` says otherwise; with
   `tables=[...]` only those are compared and extra destination tables are not reported.
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
   `bucket_batch` (4) at a time: both sides return the canonical columns of the rows in those buckets; the multiset
   difference gives missing and extra rows, and for a table with a key, rows whose key matches but whose values
   differ (listed column by column). The sample holds at most `sample_limit` (10) rows per kind. If more buckets
   differ than are examined, `truncated` is true and the row counts are lower bounds.

Memory: the client holds one (count, sum) per bucket and the rows of at most `bucket_batch` buckets. Measured peak
client memory on the 5M-row benchmark is in section 1.6.

### 1.2 Canonical forms

Equality is defined per Postgres type family by a canonical text that the oracle owns
(`src/cdc_v2/oracle/canonical.py`). It imports nothing from the pipeline. Each family is one `TypeRule` with SQL for
Postgres, SQL for DuckDB (given the destination column's actual type), and a Python reference that both SQL
builders are tested against: the DuckDB side hermetically in a local DuckDB, including property-based tests (300
random doubles, 300 random float32 values, 200 decimals, 200 texts, 200 timestamps); the Postgres side in T1
(`canonical_postgres.json`, 43 values).

| Family | Postgres types | Canonical form | Destination types accepted |
|---|---|---|---|
| integer | smallint, integer, bigint | decimal digits with an optional minus sign: `-42` | integer types, `DECIMAL(p,0)`, `VARCHAR` (text must already be canonical) |
| boolean | boolean | `t` or `f` | `BOOLEAN` |
| numeric | numeric | plain decimal, trailing fractional zeros removed (`1.50` -> `1.5`, `100.00` -> `100`); `NaN`, `Infinity`, `-Infinity`. Value equality: the display scale of a Postgres value is not compared | integer types, `DECIMAL(p,s)`, `VARCHAR` (for the text member of `UNION(DECIMAL, VARCHAR)`) |
| double precision | double precision | shortest round-trip digits as `[-]0.<digits>e<exp>` (`0.1` -> `0.1e0`, `1e15` -> `0.1e16`); `0`, `-0` (sign of zero kept), `NaN`, `Infinity`, `-Infinity` | `DOUBLE`; integer types, `DECIMAL`, `FLOAT` (converted to `DOUBLE`) |
| real | real | as double precision, with the shortest digits that round-trip through float32 | `FLOAT`; `DOUBLE` only when the value is exactly a float32 value; integer types, `DECIMAL` |
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
  (`IS NULL` is false); the oracle reports it as a difference (canonical `!null`).
* A destination type that a family does not accept gives a text that starts with `!<TYPE>:` and never equals a
  canonical form: the value is reported as different, never silently compared as text.
* Source columns whose type has no rule use the fallback and are listed in the result (`fallback_columns`).

Adding a type (ranges, enums, composites, money, inet, ...): add one `TypeRule` to `RULES` with its Postgres type
names, the two SQL builders and the Python reference, add values to `CASES` in `tests/test_oracle_canonical.py` and
to `PG_VALUES` in `tests/t1/test_oracle_t1.py`, and add its row to the table above (a unit test checks that every
family is in this table). Arrays and `UNION` members of the new type then work without more code.

### 1.3 Waiting for an applied position

`wait_for_applied(md_conn, target_lsn, timeout, lookup=...)` polls until the destination's applied position is at
or past the target. The default lookup reads `max(applied_lsn)` from `_cdc.stream_state` (core.md section 2), as
text `X/Y` or as an integer, and returns nothing while the table does not exist. Because the pipeline does not exist
yet, the lookup is a parameter. `tools/oracle --wait-lsn` waits before comparing (exit 3 on timeout).
The target is normally the position a workload quiesce point returns (`pg_current_wal_lsn()` once every writer is
idle). It is reached once the pipeline has applied a transaction that committed at or after that point; the 10 s
heartbeat of SPEC 4.4 provides one when nothing else is written.

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
for the same two states (`cli.txt`).

### 1.5 Findings for the apply design (core.md 6.2, 6.3) and the type mapping

1. **An UPDATE to an equal-comparing value is skipped.** DuckDB skips an `UPDATE` whose new value compares equal to
   the stored value, and then keeps the **old** representation: `-0` stays `-0` after `SET x = 0`, `1 mon` stays
   `1 mon` after `SET x = INTERVAL 30 DAY`, also through `UPDATE ... FROM` (`duckdb_equal_update.json`: locally and
   on MotherDuck, DuckDB 1.5.6). In one table that had been loaded in bulk on MotherDuck the `-0` update was
   applied, so the behaviour depends on the storage state. `DELETE` + `INSERT` always writes the new representation.
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

### 1.6 Rate (claim)

**Claim: the oracle compares a 5M-row lineitem-width table in 23.5 s (213,000 rows/s, checksum pass only), and
finds and reports one changed value in 76 s, with 173 MB peak client memory.** Measured on this laptop (10 cores,
16 GB), source the local primary (1.07 GB table), destination MotherDuck over the public internet, default settings
(100,000 rows per bucket, 50 buckets, 4 source workers); `rate.json`. Scaled linearly, the checksum pass over a
60M-row table takes about 5 minutes. The row pass costs, per batch of up to four differing buckets, one more source
scan and two more destination scans (hashes first, then the values of the extra rows), which run one after another.
A first version that fetched all rows of the differing bucket from both sides was faster (48 s for the same case)
but peaked at 399 MB; memory now depends on the bucket size and the number of differing rows, not on the table size.

## 2. T1 harness

Marker `t1`: tests that need local Postgres and MotherDuck. The default `pytest` run deselects it (`pyproject.toml`)
and stays hermetic. T1 fixtures (`tests/conftest.py`) skip with a reason when the Postgres binaries or the MotherDuck
token are missing. `tools/with-env` provides the token.

| Fixture | Gives | Clean-up |
|---|---|---|
| `source_db`, `make_source_db` | a database `cdc_v2_t_<10 hex>` on the shared development cluster (primary 55432, replica 55433), owned by `cdc_v2_admin`, usable by `cdc_v2_runtime` | its logical slots on every node are dropped, then `DROP DATABASE ... WITH (FORCE)` |
| `dest_db`, `make_dest_db` | a MotherDuck database `cdc_v2_t_<10 hex>`; `.conn` is a single-attach connection, `.connect()` opens another | `DROP DATABASE` |
| `private_cluster` | a primary and replica of their own (`~/.cache/cdc_flight_v2/pg-t1-<base>`) for destructive drills; `.localpg("kill", ...)` etc. | `localpg down`, home removed, port lease released |
| `shared_pg` | the shared cluster (`localpg up` if needed; a no-op when it runs) | none: the shared cluster is never stopped or changed |
| `evidence` | `evidence("p3/oracle")` returns an evidence writer | |

* **Shared cluster.** `cdc_v2.testing.pg.localpg()` refuses `down`, `reset`, `kill`, `promote`, `remove`, `set`,
  `unset`, `follow` and `add-standby` on the shared home; only `up`, `status`, `conninfo`, `sql` and `logs` run there.
* **Port allocation for private clusters.** `PortLease.acquire()` walks the blocks 55520, 55528, ... (32 blocks of
  8 ports) and takes an exclusive `flock` on `~/.cache/cdc_flight_v2/ports/<base>.lock`. The lock lives as long as
  the lease and dies with the process, so a crashed test never blocks a block for good. A block whose ports are in
  use by anything other than our own stale private cluster is skipped. Tested with leases held by another process
  and with two private clusters at once.
* **MotherDuck databases.** CREATE and DROP run on a workspace connection that executes only that statement (as the
  P2 spikes do); everything else uses single attach mode. Nothing lists the account's databases. A ledger file per
  database (`~/.cache/cdc_flight_v2/md_ledger/<name>`) is written before CREATE and removed after DROP;
  `cdc_v2.testing.md.leftovers()` checks each remaining entry by attaching it by name in a fresh process (a process
  that attached a database keeps a cached handle that still answers after the drop).
* **Temporary files** of a test run (`tmp_path`, DuckDB spill files) live under `~/.cache/cdc_flight_v2/tmp`.
* **Direct loader** (`cdc_v2.testing.loader`, tests only): copies source tables to the destination: one text export
  per column from Postgres, a local newline-delimited JSON file, one `read_json` + cast per column on MotherDuck. It
  shares no conversion code with the oracle. Its type mapping is test-only; unconstrained numeric becomes
  `UNION(n DECIMAL(38,10), s VARCHAR)` by default. The first run of the oracle found three bugs in this loader (NULL
  booleans, NULL arrays and NULL numerics were written as non-NULL values), which were fixed.

**Claim: tests get isolated sources and destinations, and destructive drills cannot touch the shared cluster.**
`harness.json`: two private databases do not see each other's tables; dropping one removes it and its logical slot;
a private cluster gets a block at or above 55520, streams to its replica, survives `localpg kill replica` + `up`,
and after `stop()` its home is gone, its ports are closed and its block can be leased again; two private clusters
get different blocks.

## 3. Evidence writer

`cdc_v2.evidence.EvidenceWriter("<item>")` writes `evidence/<item>/<name>` (`write_text`, `write_json`,
`append_text`); item and file names are checked (no `..`, no absolute paths). Everything passes through one
`Redactor`, the single implementation for the repository (the spikes each had their own):

* exact values: every value of `~/.config/cdc_flight_v2/env` and `env.local`, the secret variables of the
  environment (MotherDuck token, salt, Slack webhook, PlanetScale and `PG_*` values), and registered values (the
  fixtures register the generated passwords of every cluster they use);
* the machine's host name, the operating-system user name and the home directory (`/Users/<name>` -> `~`);
* patterns: JWTs, `Bearer` tokens, credentials in URLs, `postgres://` URIs, libpq `host=`/`user=`/`password=`/
  `passfile=` pairs, `motherduck_token=` and `token=` parameters, `"password": "..."`-style assignments, e-mail
  addresses, host names under provider and private domains.

**Claim: evidence is redacted before it reaches the disk.** `tests/test_evidence.py` (14 tests, hermetic, made-up
secrets) covers each class above, nested JSON keys and values, and the refusal of bad paths. Every evidence file of
this work item was written through it.

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
* **Quiesce points.** `quiesce()` between rounds returns `pg_current_wal_lsn()`. In continuous mode, `start()` runs
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
whole transactions gave 38 observations, all with the expected sum, and the oracle found the destination equal;
the same kind of transfers applied as two commits each gave 143 violations in 161 observations, while the final
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
