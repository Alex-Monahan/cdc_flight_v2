# Task: package V — slot continuity

Read `docs/tasks/README.md` first; its rules bind you.

The stream is only correct if the slot it reads still holds every change after the applied position. A slot can
be advanced by someone else, dropped, recreated under the same name, invalidated, or belong to another server
after a failover. You build the code that finds this out **at every slot acquisition and every 30 s while
streaming**, and that repairs it without an operator: an alert, a new position, a backfill of every CDC table,
and the stream goes on. The Flight never exits because of a slot finding.

Contracts: `docs/design/p3_plan.md` C0, C1, C2 (the attach rule), C4, C5, C7, C10, C11, and section 6.2 "V and S
(attach)". Design: `docs/design/core.md` section 8, section 9 (the slot and stream machine), open points 6 and
14. SPEC item you own the gate test of: **1.8**. SPEC 4.1 is built on your decision in P4; nothing you build may
stand in its way.

## Read as well

- `docs/design/stream.md` (package S): `_cdc.stream_state`, `stream.applied_position`, `stream.reposition`, the
  minimal attach step that you replace, the stream component's restart, the helper that starts the local path.
- `docs/design/source.md` (package P): `slot.facts` and which facts exist on which Postgres version, the answer
  on slot identity (open point 6), `slot.create` on a standby and its waits, `slot.drop`,
  `slot.terminate_holder`, `slot.timeline_history`, `timeline.contains`, the role split.
- `docs/design/engine.md` (package E): when `HELLO` is sent, the unique application name of an engine instance,
  what the engine does when it loses the slot, and whatever it found on open point 14.
- `docs/research/replica_decoding.md`, and in `docs/wal_hazards.md` every hazard about slots, invalidation,
  timelines and failover (H-029, H-034 to H-048). Where a hazard says **[test]** and the fact decides your
  finding, the test is yours unless package P's document already answers it; then cite it.

## Resources

- Postgres: `tools/localpg --home ~/.cache/cdc_flight_v2/pg-v --port-base 55680`, and a second pair
  `--home ~/.cache/cdc_flight_v2/pg-v2 --port-base 55688` (Postgres 16 through `--pg-bin`). Tests that
  invalidate a slot, fill WAL, promote or reconfigure a node take the fixture `private_cluster`.
  For the shared fixtures: `CDC_V2_PG_HOME`, `CDC_V2_PG_PORT_BASE`, `CDC_V2_MD_PREFIX=cdc_v2_t_v_`.
- MotherDuck through `tools/with-env` only; databases `cdc_v2_t_v_*` only. The engine as package E starts it.

## What to build

### 1. The stored identity

Next to the applied position the pipeline remembers what it needs to recognise its slot and its server at the
next acquisition: slot name, plugin and database, the server's system identifier, the timeline, and whatever
identifies one incarnation of the slot (package P's answer to open point 6; if nothing does, say so and use the
rule of plan 6.2: positions only). It is written in the same stream-lane transaction that first uses the slot
and in every transaction that changes it. Add it through migration steps of your own; do not rewrite package
S's.

### 2. The decision

`decide(stored, applied_position, facts, timeline_history) -> Finding` — a pure function, with no I/O, that
returns exactly one row of the table of design section 8 and the reason in words an operator can read:

| Finding | When | Action |
|---|---|---|
| `continuous` | valid slot of our plugin and database, same system identifier, applied position on the timeline history, `confirmed_flush_lsn` ≤ applied position, identity unchanged | stream from the applied position |
| `recreated_not_ahead` | as `continuous`, but the slot is another incarnation | alert; no backfill; store the new identity |
| `ahead` | `confirmed_flush_lsn` > applied position | alert; position := `confirmed_flush_lsn`; backfill every CDC table |
| `missing` | no slot of that name | alert; new slot; position := its consistent point; backfill |
| `invalid` | `wal_status = 'lost'`, `conflicting`, an invalidation reason, wrong plugin, wrong database, not logical, temporary | alert; drop it; new slot; position; backfill |
| `other_history` | different system identifier, or applied position not on the history of the server's timeline | alert; treated as `ahead` or `invalid` by what the slot shows; never streamed from as it is |

Write the full rule for every combination of facts, including facts that are absent on Postgres 16, a slot whose
`restart_lsn` is null, a slot that is `synced` or has `failover` set, a standby that was promoted (new timeline,
same system identifier, applied position before the switch point: `continuous`), and facts that cannot be read
(a finding is never invented from a failed read: the attach step waits and reads again, as a declared wait).
The default run tests the function over the whole table of combinations.

### 3. The attach step

`attach(ctx, deadline) -> Attached(engine, facts, finding)` — package S ships a minimal body; you replace the
body and keep the callers. Design section 8, steps 1 to 4, exactly:

1. Read the fence row; a deposed run touches nothing. Read `slot.facts`. If another session holds our slot,
   `slot.terminate_holder` (our slot name only) and read again.
2. Decide on those facts. For a finding that needs a new slot or a new position, enact it first (section 4) and
   start again at 1: after the enactment the same check finds `continuous`.
3. Start the engine at the applied position and wait for `HELLO`.
4. Read the facts again. The holder must be our engine (its backend, by the unique application name), and every
   fact the decision used must be unchanged: same incarnation, same `confirmed_flush_lsn`, same server. If not,
   kill the engine and start again at 1. No frame is handed to the stream before this.

- The loop has a deadline of at least 75 s after a take-over (`slot.attach_timeout`) and is a declared wait
  (C11) with a reason. When the deadline passes the stream component reports the failure to the supervisor; the
  supervisor's rules decide what follows. The attach step never gives up by exiting.
- Fault sites between the steps (`slot.attach.after_read`, `slot.attach.after_start`), so that a test can change
  the slot exactly there.
- Open point 14: prove on Postgres 16 and 18 whether a status update with a lower flush position can move
  `confirmed_flush_lsn` backwards. If it can, show that step 1 (read before the engine starts) is what protects
  the decision, with a negative control that reads only after the start and misses a slot that was ahead.

### 4. A finding enacted

Everything a finding changes in the destination happens in **one** stream-lane transaction through
`stream.reposition(tx, position, tables, cause)` (C10): the alert, the applied position, a backfill request for
every CDC table with `P0` = that position, the stored identity, and the transition of the slot and stream
machine. Around it:

- A new slot: `slot.drop` of the invalid one (never of a slot that is merely held), `slot.create` as a declared
  wait (on a standby it waits for a running-transactions record; package P bounds it), then the transaction
  above with the new slot's consistent point. A crash point after every durable step (`slot.dropped`,
  `slot.created`, `slot.finding_enacted`); show for each that the next attach converges: a slot created and not
  yet recorded is found `ahead`, and its position is taken.
- A finding cannot repeat for ever: after the commit the same facts give `continuous`. A test runs the attach
  twice after each finding and counts alerts and requests.
- While a backfill of every table from an earlier finding is still running, a second finding moves the position
  again and the backfills start again from the new `P0`; say how the requests are joined so that tables are not
  queued twice.
- Alerts name the finding, the positions and the bytes of WAL between them, never a source value.

### 5. The periodic check

Every `slot.verify_interval` (default 30 s) while streaming, as a component under the supervisor: read
`slot.facts` and compare. While our engine holds the slot nobody else can advance or drop it, so the check looks
for what can still happen: the holder is no longer our engine, the slot is invalidated or close to it
(`wal_status`, `safe_wal_size`: an alert **before** the loss, with the bytes left), the server changed
(system identifier, timeline, recovery state). A finding here restarts the stream component, and the attach step
decides. A read that fails is not a finding.

### 6. The slot and stream machine

Declare it with `state.declare` (C5) as design section 9 lists it — `unverified → verified → streaming ⇄
stalled`, `broken → rebuilding → verified` — together with package S, which already enacts some of its states:
one declaration, in one module, extended by you. Every transition goes through `state.transition` in the
transaction that does the work. Run package D's enumeration helper for it.

## Tests

**Default run (hermetic).** The decision over every combination; the attach loop against a scripted source and
a scripted engine, including a holder that comes back, a slot that changes between the two reads, a deadline
that passes; the enactment against a local DuckDB with crash points; the machine's enumeration.

**Marker `localpg`.** The facts behind each finding on a real node, on Postgres 18 and 16, with the slot on a
standby and on a primary: after `pg_replication_slot_advance`; after drop and recreate; after another session
consumed changes (`pg_logical_slot_get_binary_changes`); after invalidation by `max_slot_wal_keep_size`; after
a restart of the node; after a promotion of the standby that holds the slot (timeline history read through
package P, the applied position before the switch point is `continuous`). Open point 14.

**Marker `t1` — the gate test of SPEC 1.8.** The whole local path of package S's helper, a seeded workload that
keeps writing through everything, the slot on the standby (the default) and once on the primary:

1. `pg_replication_slot_advance` while the pipeline is stopped between two runs; and tried while it streams
   (the server must refuse; record it).
2. Drop and recreate while the pipeline is stopped; and **between the first read and the engine start** of the
   attach step, through the fault site.
3. Changes consumed by another session while the pipeline is stopped.
4. Invalidation by `max_slot_wal_keep_size`, while stopped and while streaming.
5. The pipeline killed at each crash point of section 4 during the enactment.

After each: the alert exists once; the pipeline's process did not exit and no operator did anything; the
applied position is the one the finding prescribes; a backfill of every CDC table was requested in the same
transaction; the stream runs again and the slot's `confirmed_flush_lsn` moves. **With package B merged**, the
test goes to the end: the backfills finish and `cdc_v2.oracle.compare(..., position=Q)` is equal. If package B
is not merged when you finish, deliver the test with that last step behind one switch and say so; the step is
run in a short follow-up round when B is there.

**Negative controls**, kept as tests: a check that only reads after the engine started; an attach that skips
the second read; a decision that ignores the system identifier. Each must be caught by one of the tests above.

## Deliver

- The attach module, the decision, the periodic check, your migration steps, the tests.
- `docs/design/slot_continuity.md`: the decision table as built, with the facts per Postgres version; the
  answers on open points 6 and 14 with the evidence; each crash point and what the next attach finds after it;
  the bounds (attach deadline, check interval, how long after an external change the finding is enacted);
  contract notes; what SPEC 4.1 will need from this code in P4 (another replica, a failover slot on a primary).
- Evidence under `evidence/p3/slot_continuity/`.
- In your report: any way you found to change the slot that the checks do not see, and what packages B, T and F
  must know.
