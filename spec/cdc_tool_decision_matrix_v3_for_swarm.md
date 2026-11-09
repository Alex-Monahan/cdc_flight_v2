# Choosing a Postgres CDC Tool: A Decision Matrix (v2, re-categorized)

This is a re-organization of `cdc_tool_decision_matrix_with_failure_modes.md`: the same 42 rubric items, verbatim, regrouped into themed sub-categories (no category exceeds 7 items).

### 1. Delivery Guarantees & Correctness

How reliably and correctly change events (including edge cases like primary-key updates, truncates, and crashes) are delivered to MotherDuck.

1. Delivery guarantees for tables with a primary key
    1. at-most-once=1, at-least-once=3, exactly-once=5
2. Delivery guarantees for tables WITHOUT a primary key
    1. at-most-once=1, at-least-once=3, exactly-once=5
3. CDC Changes should be atomic in MotherDuck
    1. No Postgres transactional boundaries are respected in MotherDuck load=1, single table transactional batches are respected in MotherDuck=3, Multi-table transactional batches in MotherDuck=5
4. If a primary key is updated, correctly handle it
    1. Primary key update causes an error=1, primary key update causes duplication=2, primary key row is correctly deleted and inserted or updated=5
5. Truncate table and drop table propogate
    1. Truncate and drop are silently ignored=1, truncate and drop are logged in some way=2, truncate and drop are handled with tombstones / soft delete=3, truncate and drop are replicated just like Postgres handles them=5
6. Snapshot/backfill must be consistent with CDC
    1. inconsistent=1, consistent=5
7. Failures in the library do not cause correctness issues
    1. Duplication is possible due to a crash=1, duplication is impossible but not well tested=3, robust injection of failures in testing=5
8. If the replication slot is advanced by another method in a way that could cause data loss, it should be detected and cause a backfill / snapshot (see "offset.mismatch.strategy" from Debezium). Check whenever slot is acquired.
    1. slot can advance and silently cause data loss=1, any potential data loss from slot advancement triggers the process to exit=4, any potential data loss from slot advancement triggers a backfill / snapshot automatically=5
9. Any state that can affect consistency is managed with a state machine approach. *[Added 2026-07-31 by user directive]*
    1. No state machines=1, only 1 big state machine=3, An appropriate number of state machines (over 1)=5

### 2. Schema Evolution & Type Handling

How well the tool tracks schema changes and converts Postgres data types (including TOAST values) into MotherDuck.

1. Added or dropped columns must be handled
    1. no=1, yes=5
2. Renamed columns must be handled well
    1. no=1, old column renamed with tombstone=3, seamless rename=5
3. New tables and schemas must be automatically discovered
    1. no=1, infrequently or requiring restart=4, automatically on short interval=5
4. Postgres types accurately converted to native MotherDuck types
    1. Most types stored as text or json=1, Core scalar types handled well (nested types as text)=3, Core scalar types handled well (nested types as json)=4, Core scalar types and nested types handled well. Only obscure types (money, inet, etc.) as text. NaN and infinity supported by all types with DuckDB support for NaN and infinity=5
        - CLARIFICATION (user directive 2026-08-10, binding): storing `money` and `inet` **as text** is EXPLICITLY 5/5 behaviour — the 5-band names them as the acceptable "as text" cases. Do NOT deduct for them being text, and do NOT spend effort making them native. What is NOT acceptable is REFUSING them (failing to deliver them at all), which is what a wrong oracle caused in fix round 9. The same latitude extends to comparably obscure types delivered losslessly as text.
        - PRIORITY (same directive): fix all OTHER type-handling issues preferentially. If `money`/`inet`-as-text is the only outstanding item, award 5/5.
5. Data type changes should be supported
    1. Data type changes cause error=1, data type changes drop and add the column=3, data type changes that are widening are handled automatically=4, Uses MotherDuck UNION types to store both data types in MotherDuck=5
6. TOAST columns are handled well
    1. errors on TOAST=1, TOAST is handled but inefficiently=4, TOAST is handled efficiently=5

### 3. Backfill & Refresh Modes

How initial and re-triggered backfills behave — atomicity, resumability, coexistence with live CDC, and configurable refresh strategies.

1. Backfill must be scalable and performant (parallelized) for large datasets
    1. Backfill fails on large tables=1, backfill is slow on large tables=3, backfill is fast on large tables=5
2. Backfills must be atomic (so that MotherDuck consumers can continue to use tables while backfills happen)
    1. Backfills result in tables being cleared out and repopulated=1, Backfills populate alternatively named tables and then do an atomic rename swap with original table names=5, Backfills are fully atomic with CREATE OR REPLACE TABLE type of semantics (BEGIN / COMMIT transactionality fine too)=5
        - CLARIFICATION (user directive 2026-08-08, binding): the atomic rename swap now scores **5**, equal to CREATE OR REPLACE / transactional semantics. Both are fully acceptable. What is graded is that a consumer never observes a cleared, partial, or intermediate table — not which mechanism achieves it. The shadow-table + atomic rename route is explicitly preferred where it keeps **memory use lower**, since it can be loaded incrementally in chunks rather than requiring the whole result set to be materialized in one statement. Note this also composes with item 3.7 (resumable per-table backfills), which a single CREATE OR REPLACE ... AS SELECT cannot support. Do not add complexity purely to reach CREATE OR REPLACE semantics.
3. Existing tables continue to receive CDC events during normal / healthy snapshot
    1. Existing tables go stale=1, Existing tables continue to receive updates but it causes increased complexity or error cases=2, Existing tables continue to receive updates in a simple and elegant way=5
4. Snapshots / backfills should be able to be run for individual tables while other tables continue receiving CDC events
    1. Backfills are only for the entire database=1, snapshots can be done for one table=4, snapshots can be done for any arbitrary set of tables=5
5. [From Nick Ursa] Each table should be able to be configured to use either CDC, full refresh on a schedule, or incremental refresh on a schedule.
    1. CDC only=3, CDC and full refresh only=4, CDC, full refresh, or incremental refresh=5
6. If CDC falls behind by a certain amount, backfill instead
    1. Falls further and further behind=1, triggers a backfill if behind by certain size=4, triggers a backfill if behind by a certain size or certain time=5
7. If a backfill fails, it should resume midway through rather than starting over
    1. Restarts from the beginning=1, restarts only tables that were not completed=4, Retains partially backfilled tables and picks up midway through backfill per table=5 

### 4. Failure Detection & Recovery

How the tool detects, survives, and recovers from slot failures, bad WAL messages, dead connections, hangs, and concurrent runners.

0. **A permanently unprocessable row or table must not take down the pipeline (blast-radius containment).** *[Added 2026-08-09 by user directive]*
    1. One bad row or table stops the whole Flight — healthy tables stop being created and stop receiving data, the automatic recovery re-reads the same durable input and re-fails forever, and the slot cannot advance so WAL grows without bound=1, the bad table is retried indefinitely but healthy tables keep replicating=3, the bad table is automatically moved to a durable, observable quarantine with bounded retry, every healthy table keeps replicating, and the slot continues to advance so WAL retention stays bounded=5
    - Numbered 0 deliberately so it sorts first without renumbering 4.1–4.7, which are referenced throughout the reviews, prompts and status documents.
    - RELATIONSHIP TO NEIGHBOURING ITEMS, so this is not read as a duplicate: 4.3 grades recovery from a problematic *WAL message*; 4.5 grades *hanging or locking*; 4.7 grades *self-healing*. This item grades something none of them do — the BLAST RADIUS when a row genuinely cannot ever be processed. Such a row cannot be "healed", so the correct behaviour is to contain it and keep everything else running. A Flight can score 5 on 4.3, 4.5 and 4.7 and still score 1 here.
    - ORIGIN: proved end to end by review r8 (reviews/p2b_rereview_r8.md) on the 2.4-2.6 branch — one row whose value the type layer rejected raised `SchemaEvolutionRefused`, the automatic resnapshot re-read the same durable row and refused identically on runs 2 and 3, and a healthy co-published table was never created and never received a row on any attempt.

1. Recover from failed slot or slot in error state
    1. Throws error or requires restart to recover=1, Recovers from failed slot with backfill=3, Supports failover slot and recovers with backfill if no failover=5
2. Handle or prevent concurrent Flight instances
    1. Multiple Flights cause hangs or poorly handled errors or other bad consequences=1, multiple Flights fail unpredictably=3, multiple Flights fail in a predictable way=5
3. Recovers from unhandled or problematic WAL message
    1. hangs or gets stuck=0, requires restart to do backfill=2, handles backfill automatically=5
4. Idle-slot heartbeat to advance slot if only replicating subset of tables and other tables are very active
    1. no idle-slot heartbeat present=1, idle-slot heartbeat present=5
5. Errors should not cause hanging or locking that prevent recovery (includes network error handling also).
    1. hanging or locking can happen and is difficult to recover (kill Postgres process)=1, hanging or locking can happen but is recoverable=2, hanging and locking are systematically prevented with a heartbeat in the CDC library=5
6. Detect failure of Postgres node even with silently-dead-connection
    1. unable to detect silently-dead-connection=1, TCP keepalives set to provide under 30 minute detection=3, full heartbeat at under 1 hour interval=5
7. The Flight should always be able to self-heal without human intervention. *[Added 2026-07-31 by user directive]*
    1. The Flight has more than 2 cases that cause manual human intervention=1, The Flight requires human intervention in a catastrophic failure scenario=3, The Flight will self heal without causing data consistency issues in 100% of cases=5

### 5. Performance, Latency & Scale

Throughput, latency, and resource behavior under large changes and high transaction rates.

1. CDC must be scalable and performant for large changes
    1. CDC fails on large changes=1, CDC is slow on large changes=3, CDC is fast on large changes=5
2. CDC should be low latency on small changes
    1. minutes of latency=1, 30 seconds to 60 seconds on regular basis=4, consistently under 30 seconds=5
3. CDC should keep up with high Postgres transactions per second (through batching, etc.)
    1. Under 100 TPS=1, Between 100 and 300 TPS=2, Between 300 and 1000 TPS=3, Between 1000 and 2000 TPS=4, Over 2000 TPS=5 
4. Must have well managed low memory use and/or spill to disk under pressure
    1. all in memory with no guardrails=1, in memory but with guardrails=5, disk backed or able to spill=5

### 6. Observability & Alerting

Whether operators can see pipeline health in MotherDuck and get alerted on problems.

1. Logging must be detailed and easily consumable in MotherDuck
    1. Can't see health of slot and how far behind or only few logs=1, detailed logs but in Postgres=2, non-detailed logs in MotherDuck=3, detailed logs in MotherDuck including replication lag=5
2. Alerts and warnings for issues
    1. Not enough logs in MotherDuck to build good alerts=1, Enough logs but no alerts in the library=2, alerts built into the system=5

### 7. Postgres Features to Support

Fit with the Postgres source environment (extensions, replicas, permissions, partitioning)

1. Requires a postgres extension
    1. requires an obscure plugin=1, requires wal2json=2, uses pgoutput directly=5
2. Able to read from a Postgres Replica
    1. primary only=1, replica enabled but still heavily writes to or disrupts primary=3, replica enabled and light workload on primary=5
    2. *[Clarified 2026-07-30 by user directive: heartbeat writes should be routed to the primary via a separate connection while CDC reads from the replica — light heartbeat writes on the primary are consistent with a 5.]*
3. Handle partitioned Postgres tables gracefully
    1. unable to handle partitioned tables=1, handle partitioned tables except for detach partition and drop partition=3, cleanly handle partitioned tables with options for per-partition tables or one large table=4, cleanly handle partitioned tables with options for per-partition tables, one large table, or a partitioned DuckLake=5
4. Capture pg_logical_emit_message messages (useful for a heartbeat)
    1. not supported=1, supported=5

### 8. Specific CDC Features

Destination-side options

1. Hard and soft delete options
    1. soft delete only=1, hard delete only=4, hard and soft delete options=5
2. Support a change history mode for audit tables and slowly changing dimentions (SCD2).
    1. Not supported=1, supported with global flag=3, supported per table=4, supported per table with option to have both current state and changelog=5
3. PII controls to prevent PII data landing in the destination
    1. all columns always replicated=1, column exclusion only=3, exclusion + masking + salted-hash pseudonymization + truncation w/ per column-regex=5.

