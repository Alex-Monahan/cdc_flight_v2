# Configuration

Generated from `src/cdc_v2/config.py` (`uv run python -m cdc_v2.config > docs/configuration.md`); a test fails when the two differ, and when a settings field is neither a key here nor listed as derived. `config/example.toml` sets every key.

One TOML file configures a deployment; `cdc_v2.config.load(path, environ)` reads it and fills the settings dataclass of every package. Spellings: a **duration** is a number and a unit (`ms`, `s`, `m`, `h`), and the parts may follow each other (`"7h30m"`); a **size** is an integer and a unit (`B`, `KiB`, `MiB`, `GiB`); a **count** is an integer; a **switch** is true or false. No secret is in the file: a password is named by the environment variable that holds it (`password_env`), and so may a node's host, port, database and user (`*_env`). An unknown key, a wrong type, a value out of range or a broken order is refused at load, with a message that names the key and never a variable's value.

Orders checked: `leader.heartbeat_interval` < `leader.stale_age` / 3 and `leader.stale_age` < `leader.hard_exit_age`; `stream.attach_timeout` ≥ `leader.hard_exit_age` + 15 s; `stream.heartbeat_interval` < `stream.marker_timeout`; `engine.heartbeat_interval` < `stream.frame_silence` < `supervisor.restart_after`; `apply.retry_pause` ≤ `apply.retry_pause_max`; `apply.stage_chunk_bytes` ≤ `apply.max_group_bytes`.

## `[destination]`

Fills `cdc_v2.md.settings.Settings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `destination.database` | name | – | the MotherDuck database (C4; `cdc_v2_*` names in tests) |
| `destination.marker` | name | cdc_v2 | custom user agent of every connection (C4) |
| `destination.connect_deadline` | duration | 30 s | one connect attempt (SPEC 4.5; destination 4) |
| `destination.connect_attempts` | count | 3 | connect attempts inside the caller's deadline (destination 2.1) |
| `destination.connect_retry_pause` | duration | 0.5 s | pause between connect attempts (destination 2.1) |
| `destination.statement_deadline` | duration | 60 s | one statement, the fence and the registration (SPEC 4.5; destination 4) |
| `destination.transaction_deadline` | duration | 300 s | a lane block without its own deadline (destination 4; C11) |
| `destination.rollback_deadline` | duration | 5 s | the clean-up after a failed step (destination 3) |
| `destination.grace` | duration | 2 s | wait after an interrupt before a connection is given up (destination 4) |
| `destination.interrupt_deadline` | duration | 5 s | one call of the server's interrupt (destination 4) |
| `destination.max_waiting_calls` | count | 2 | calls a connection gave up that may still wait (destination 4) |

## `[source]`

Fills `cdc_v2.pg.settings.SourceSettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `source.publication` | name | cdc_v2_pub | our publication (C7; SPEC 2.3) |
| `source.slot` | name | cdc_v2_slot | our replication slot (C7; design 8) |
| `source.heartbeat_schema` | name | cdc_v2 | schema of the published heartbeat table (SPEC 4.4; source 5) |
| `source.heartbeat_table` | name | heartbeat | the published heartbeat table (SPEC 4.4; source 5) |

Derived, not keys: `nodes` (the tables [source.nodes.<name>]).

## `[source.nodes.<name>]`

Fills `cdc_v2.pg.settings.NodeSettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `source.nodes.<name>.host` | name | – | the node's host (prefer host_env: never committed, SPEC R23) |
| `source.nodes.<name>.port` | count | – | the node's port; always explicit (PLAN §9) |
| `source.nodes.<name>.dbname` | name | – | the source database |
| `source.nodes.<name>.user` | name | – | the runtime role (source 6) |
| `source.nodes.<name>.tls` | name | verify-full | verify-full, or disable for a host on this machine only (C7) |
| `source.nodes.<name>.sslrootcert` | name | – | CA file for verify-full, or "system" |
| `source.nodes.<name>.application_name` | name | cdc_v2 | application_name of the node's SQL sessions (C7) |
| `source.nodes.<name>.connect_timeout` | duration | 10 s | libpq connect timeout (source 1) |
| `source.nodes.<name>.statement_timeout` | duration | 30000 ms | server-side bound behind the client deadlines (SPEC 4.5) |
| `source.nodes.<name>.lock_timeout` | duration | 5000 ms | server-side lock bound (SPEC 4.5) |
| `source.nodes.<name>.idle_in_transaction_session_timeout` | duration | 60000 ms | no source lock held across a network wait (SPEC 4.5) |
| `source.nodes.<name>.keepalives_idle` | count | 30 | TCP keepalive idle seconds (SPEC 4.6) |
| `source.nodes.<name>.keepalives_interval` | count | 10 | TCP keepalive interval seconds (SPEC 4.6) |
| `source.nodes.<name>.keepalives_count` | count | 6 | TCP keepalive probes (SPEC 4.6) |
| `source.nodes.<name>.tcp_user_timeout` | duration | 120000 ms | bound of unacknowledged sent data, Linux (SPEC 4.6) |
| `source.nodes.<name>.cancel_grace` | duration | 2 s | grace after a cancel before the socket is shut (source 1) |
| `source.nodes.<name>.host_env` | name of a variable | – | the variable that holds the host |
| `source.nodes.<name>.port_env` | name of a variable | – | the variable that holds the port |
| `source.nodes.<name>.dbname_env` | name of a variable | – | the variable that holds the database name |
| `source.nodes.<name>.user_env` | name of a variable | – | the variable that holds the user name |
| `source.nodes.<name>.password_env` | name of a variable | – | the variable that holds the password (read when connecting; never in the file) |

Derived, not keys: `password` (password_env: the environment variable that holds it (read when connecting)).

## `[engine]`

Fills `cdc_v2.stream.engine.EngineSettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `engine.heap` | size | 512 MiB | JVM heap cap (SPEC 5.4; engine 4) |
| `engine.gc` | name | parallel | JVM garbage collector, parallel or g1 (engine 4) |
| `engine.active_processors` | count | – | -XX:ActiveProcessorCount (the F16 Flight: 2) |
| `engine.max_batch_size` | count | 8192 | Debezium max.batch.size (engine 4; SPEC 5.1) |
| `engine.max_queue_size` | count | 32768 | Debezium max.queue.size (SPEC 5.4) |
| `engine.max_queue_bytes` | size | 256 MiB | Debezium max.queue.size.in.bytes (SPEC 5.4) |
| `engine.poll_interval` | duration | 50 ms | Debezium poll.interval.ms |
| `engine.record_processing_threads` | count | 1 | Debezium record.processing.threads |
| `engine.heartbeat_interval` | duration | 5000 ms | Debezium heartbeat.interval.ms: HELLO on an idle source, DURABLE taking effect (C2) |
| `engine.status_update_interval` | duration | 10000 ms | Debezium status.update.interval.ms |
| `engine.message_prefixes` | list of names | `cdc_v2_.*` | logical message prefixes delivered (SPEC 7.4) |
| `engine.frame_max_rows` | count | 65536 | rows per ROWS frame (engine 2) |
| `engine.frame_target_bytes` | size | 8 MiB | target size of a ROWS frame (engine 2) |
| `engine.frame_max_bytes` | size | 1040 MiB | largest frame the reader accepts (C2; SPEC 5.4) |
| `engine.max_value_bytes` | size | 256 MiB | largest single value (engine 8; H-002) |
| `engine.ack_max_pending` | count | 4096 | END records the engine holds for acknowledgement (engine 5) |
| `engine.stop_timeout` | duration | 15000 ms | STOP of the JVM (engine 7) |
| `engine.stat_interval` | duration | 10000 ms | the engine's statistics lines |

Derived, not keys: `host` (the source node stream.node); `port` (the source node stream.node); `dbname` (the source node stream.node); `user` (the source node stream.node); `password` (the source node stream.node (read when the engine starts)); `slot` (source.slot); `publication` (source.publication); `sslmode` (the source node's tls); `sslrootcert` (the source node's sslrootcert); `table_include` (set by the stream at each engine start (tables left out)); `keyless_tables` (read from the catalog at each engine start (rule K)); `extra` (not configurable (protected properties, docs/design/engine.md 4)).

## `[transform]`

Fills `cdc_v2.transform.convert.TransformSettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `transform.threads` | count | 1 | threads of the transform's DuckDB (C3) |
| `transform.memory_limit` | size | 512MB | memory limit of the transform's DuckDB (SPEC 5.4) |
| `transform.max_cached_queries` | count | 256 | compiled conversion statements kept (transform 3) |

Derived, not keys: `temp_directory` (run.work_dir/spill/transform); `max_temp_directory_size` (a quarter of run.spill_limit).

## `[apply]`

Fills `cdc_v2.apply.core.ApplySettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `apply.statement_rows` | count | 1000000 | rows per statement from local data (apply 3.2; design 6.1) |
| `apply.local_threads` | count | 2 | threads of apply's planning DuckDB (apply 2.7) |
| `apply.local_memory_limit` | size | 1GB | memory limit of apply's planning DuckDB (SPEC 5.4) |
| `apply.check_new_keys` | switch | True | stream mode checks that a new key is free (apply 2.5) |

Derived, not keys: `local_temp_directory` (run.work_dir/spill/apply); `local_max_temp_directory_size` (three quarters of run.spill_limit).

## `[apply]` (the stream's group settings)

Fills `cdc_v2.stream.settings.GroupSettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `apply.max_group_delay` | duration | 1 s | a group closes this long after its first record (design 3; SPEC 5.2) |
| `apply.max_group_bytes` | size | 128 MiB | the memory one group may hold (design 6.1; SPEC 5.4) |
| `apply.max_group_attempts` | count | 12 | runs of one group after Conflict, ConnectionLost or a deadline (design 3; C10) |
| `apply.retry_pause` | duration | 0.5 s | first pause between runs of a group (destination 8.3) |
| `apply.retry_pause_max` | duration | 5 s | longest pause between runs of a group |
| `apply.max_table_attempts` | count | 3 | failures of one table before it leaves the stream (SPEC 4.0; C10) |
| `apply.commit_timeout_base` | duration | 60 s | commit deadline: base (design 3) |
| `apply.commit_timeout_per_million_rows` | duration | 60 s | commit deadline: per million rows (design 3; SPEC 5.1) |
| `apply.stage_chunk_bytes` | size | 64 MiB | one staging chunk of a transaction larger than a group (design 6.4) |

## `[stream]`

Fills `cdc_v2.stream.settings.StreamSettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `stream.node` | name | replica | the source node the engine decodes from (SPEC R17: a replica) |
| `stream.frame_silence` | duration | 30 s | no frame while reading: the engine is torn down (SPEC 4.6; engine 6) |
| `stream.marker_timeout` | duration | 60 s | a beat must come back within this (SPEC 4.6) |
| `stream.heartbeat_interval` | duration | 10 s | the source-side heartbeat write (SPEC 4.4) |
| `stream.idle_position_interval` | duration | 10 s | at most one commit per this when only heartbeats arrive (SPEC 4.4) |
| `stream.max_attempts_at_position` | count | 3 | starts at one position before the last line (SPEC 4.3) |
| `stream.max_key_restarts` | count | 2 | engine restarts for a key that differs from the catalog (rule K) |
| `stream.attach_timeout` | duration | 90 s | the attach step's declared wait (design 8) |
| `stream.unreachable_wait` | duration | 1800 s | retries against an unreachable source or destination in one declared wait (SPEC 4.7) |
| `stream.unreachable_pause` | duration | 1 s | first pause of those retries (doubles to 30 s) |
| `stream.engine_stop` | duration | 20 s | STOP of the engine at a planned stop (C12) |
| `stream.request_poll` | duration | 5 s | the stand-in of backfill.on_group reads requests this often (C10) |
| `stream.alert_flush` | duration | 5 s | alerts of other components wait at most this for a stream transaction (SPEC 6.2) |
| `stream.source_statement` | duration | 30 s | one source statement of the stream (SPEC 4.5) |

Derived, not keys: `slot` (source.slot).

## `[supervisor]`

Fills `cdc_v2.supervisor.SupervisorSettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `supervisor.restart_after` | duration | 120 s | a silent component is restarted (SPEC 4.5) |
| `supervisor.max_restarts` | count | 5 | restarts of one component in the window before the run ends (C11) |
| `supervisor.restart_window` | duration | 600 s | the window of max_restarts |
| `supervisor.join_timeout` | duration | 30 s | a restarted component's thread must return within this (C11) |
| `supervisor.check_interval` | duration | 1 s | how often the rules run |
| `supervisor.max_waiting_calls` | count | 8 | destination calls left waiting before the run ends (C11; destination 4) |

## `[run]`

Fills `__main__.RunSettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `run.max_runtime` | duration | 27000 s | planned hand-over before the platform's limit (SPEC 4.2; PLAN §7) |
| `run.work_dir` | name | – | local working directory of the run; the spill area is under it |
| `run.spill_limit` | size | 8192 MiB | the run's one spill area (SPEC 5.4): transform 1/4, apply 3/4 |
| `run.jvm_cache_dir` | name | – | JRE, jars and engine classes (artifacts) |
| `run.stop_timeout` | duration | 30 s | SIGTERM: the group in flight is finished within this (C12) |

## `[leader]`

Fills `__main__.LeaderSettings`.

| Key | Unit | Default | Serves |
|---|---|---|---|
| `leader.heartbeat_interval` | duration | 10 s | leader heartbeat (SPEC 4.2; design 4.3) |
| `leader.stale_age` | duration | 30 s | a heartbeat older than this is not healthy (SPEC 4.2) |
| `leader.hard_exit_age` | duration | 60 s | no tick for this long: the bootstrap kills the run (SPEC 4.2; design 5) |
| `leader.takeover_timeout` | duration | 45 s | the take-over gives up after this (design 4.5) |
| `leader.startup_timeout` | duration | 180 s | start-up of the run (design 5) |
