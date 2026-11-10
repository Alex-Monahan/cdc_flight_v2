# Evidence for the cut (P3)

This directory holds the output of every run that `docs/design/cut.md` cites. The document gives the argument
and the reading of the numbers. This file says what each file is, how it was made and under what conditions.

## Environment

- One machine, macOS, 10 cores, 16 GB.
- Postgres 18.6 (Homebrew), `server_version_num` 180006. No other version was run.
- A private cluster made by `tools/localpg --home <home> --port-base 55600`: a primary on port 55600 and one
  streaming standby (`replica`) on port 55601. `<home>` stands for the cluster's home directory. Settings are
  the defaults of `tools/localpg` (see `docs/localpg.md`) unless a run changes them; a run puts back what it
  changed.
- Runs with two-phase commit had `max_prepared_transactions = 16` on both nodes
  (`tools/localpg … set all max_prepared_transactions=16`). All other runs had the default 0.
- Seed 1 in every run. The seed fixes the keys and choices of the writers. It does not fix the interleaving;
  a repeated run gives other counts of the same order.
- All runs are from 2026-11-10 (UTC). The first campaigns ran between 06:41 and 08:15 UTC. The runs made after
  the coordinator's review ran between 10:24 and 10:47 UTC. Each `.json` file of a stress run carries its start
  time (`started_utc`). The single scenarios carry no time.
- Tool: `tools/cutstress` (modules `src/cdc_v2/testing/cutstress.py`, `cutprimary.py`, `cutmodel.py`); the
  rule under test is `src/cdc_v2/pg/cut.py`.

## What else was running

The machine was shared the whole time with a throughput measurement and with other workers of the same build
(their own Postgres clusters on other ports, test runs, Python processes). I did not control that load. Each
stress run prints the load average (1, 5 and 15 minutes) right before it starts; the table below repeats it.
The runs made after the review also print it at their end.

The load of the others was heaviest during the runs made after the review. `v1b_big_drop` started at 10.42 and
ended at 42.64; read by hand with `uptime` at 10:30 UTC, in the middle of that run, the load average was 85.22
24.31 13.04. That reading is in no file. `v3b_ddl_walswitch_pause` started at 42.64 and ended at 9.79. Before
the review the highest value is 41.55, before `v6_slot_on_primary`.

The main campaign does not print the load. I read it by hand: 5.25 5.67 5.16 before, 6.35 11.22 11.25 about
halfway, 6.24 8.89 10.24 after. The primary campaigns and the single scenarios do not record it. The primary
campaigns ran between 07:19 and 07:58 UTC, between the standby runs listed below.

My own load: at most 4 writer sessions, at most 4 cut-taking sessions and one session reading the stream.

Load changes how many cuts and transactions a run reaches and how large the unknown set is. It does not change
what counts as a violation.

## Standby: stress runs

Each run has two files: `NAME.txt` is what the tool printed (first line: the command), `NAME.json` is the full
result, with the configuration, counters, the distribution of the unknown set, examples for every wrong rule
and every violation.

### The checker, and which runs had all of it

The checker has three steps (`docs/design/cut.md` section 5.1): (1) every transaction in the window of
`unknown` against the library's answer, (2) for every key the boundary between what the snapshot sees and what
it does not, (3) a row for every key, so that a row a snapshot lost is counted. Step 3 was added after the first
campaigns had run. Before it, a key whose row was missing from a snapshot was skipped by step 2.

Three groups of runs follow from that. **The claims about the exported way and about lost rows rest on the
first group only.**

1. **Complete checker, no violation expected** (seven runs): `main_recheck`, `v5b_restart_crash`,
   `v7b_exported_no_subxact`, `v9_exported_open_no_subxact`, `qb_quiet_exported`, `v1b_big_drop`,
   `v3b_ddl_walswitch_pause`. Sum: 1,192,278 cuts, 5,025,620 transactions, no violation, no missing row.
2. **Steps 1 and 2 only** (seven runs, all with cuts taken the own way): `main`, `v2_twophase_subxact`,
   `v4_apply_delay`, `v5_restart_crash`, `v6_slot_on_primary`, `v8_idle`, `nc_mid_redo`. Sum: 2,982,523 cuts,
   9,353,889 transactions, no violation. A row that a snapshot lost would not have been counted in them. They
   are used for what steps 1 and 2 decide: the answers for single transactions, the wrong rules, the
   measurements of the unknown set.
3. **Not used** (four outputs): `v1_big_drop`, `v3_ddl_walswitch_pause`, `v7_exported_no_subxact`,
   `q_quiet_exported`. They ran with steps 1 and 2 only and were repeated after the review (`v1b`, `v3b`, `v7b`,
   `qb`). The files are kept; no claim and no number of the document comes from them.

The three runs that are expected to fail (`x_exported_subxact`, `xb_exported_subxact`, `xc_exported_subxact`)
had the complete checker.

How to tell from a file. The result files written after the review (`v7b`, `v9`, `qb`, `v1b`, `v3b`, `xb`,
`xc`) name the steps of their checker: key `checker_steps` in the `.json` file, line `checker steps:` in the
`.txt` file; their `.txt` file also prints the number of missing rows, 0 included. The files of
`main_recheck`, `v5b_restart_crash` and `x_exported_subxact` are older than that field, and a counter that
stays at 0 is not written into them. That these three ran with step 3 follows from two facts:
`x_exported_subxact`, the first of the three, has the counter `rows_missing_from_a_cut` (under `checker`,
value 1), which only step 3 fills; and step 3 and the three outputs were committed together (`58c2c64`), the
two others having run after it with the same code. The files of groups 2 and 3 are older than step 3.

Which runs were repeated after the review: variants 7, 1 and 3 and the quiet-only run (`v7b`, `v1b`, `v3b`,
`qb`), each at its full length. Added: `v9_exported_open_no_subxact`, and two repeats of the expected failure
(`xb`, `xc`). Not repeated: `main` at its length of 30 minutes (`main_recheck` is a repeat of 10 minutes),
`v2_twophase_subxact`, `v4_apply_delay`, `v6_slot_on_primary`, `v8_idle`, `nc_mid_redo`.

### The runs

Common options not repeated in the table: `tools/cutstress run --label NAME … --out evidence/p3/cut/NAME.json`.

| Run | Start (UTC) | Options | Checker | Cuts checked | Transactions | Violations of the sound rules | Missing rows | Load before |
|---|---|---|---|---:|---:|---|---|---|
| `main` | 06:41 | `--seconds 1800 --writers plain,async,subxact,abort --takers-own 3 --takers-exported 0` | steps 1, 2 | 1,854,463 | 7,305,063 | none | not counted | see above |
| `v2_twophase_subxact` | 07:24 | `--seconds 120 --writers twophase,plain,subxact:subs=3000 --takers-own 3 --takers-exported 0` | steps 1, 2 | 141,029 | 315,222 | none | not counted | 5.87 5.77 7.17 |
| `v1_big_drop` | 07:26 | `--seconds 120 --writers big,drop,plain,long --takers-own 2 --takers-exported 0` | steps 1, 2; **not used** | 104,047 | 157,457 | none | not counted | 7.97 6.83 7.41 |
| `v3_ddl_walswitch_pause` | 07:30 | `--seconds 120 --writers plain,async,subxact,abort --takers-own 2 --takers-exported 0 --chaos ddl,walswitch,pause` | steps 1, 2; **not used** | 84,094 | 483,333 | none | not counted | 4.71 6.12 7.00 |
| `v4_apply_delay` | 07:32 | `--seconds 120 --writers plain,async,subxact --takers-own 2 --takers-exported 0 --apply-delay-ms 300` | steps 1, 2 | 100,763 | 363,741 | none | not counted | 6.10 6.33 6.98 |
| `v5_restart_crash` | 07:34 | `--seconds 180 --writers plain,async,subxact,abort --takers-own 2 --takers-exported 0 --chaos restart,crash --chaos-period 10` | steps 1, 2 | 76,431 | 703,213 | none | not counted | 5.21 5.95 6.73 |
| `v6_slot_on_primary` | 07:37 | `--seconds 120 --writers plain,async,subxact,abort --takers-own 2 --takers-exported 0 --slot-node primary` | steps 1, 2 | 86,409 | 477,973 | none | not counted | 41.55 17.03 10.78 |
| `v7_exported_no_subxact` | 07:40 | `--seconds 120 --writers plain,async,abort --takers-own 1 --takers-exported 2` | steps 1, 2; **not used** | 125,445 | 485,337 | none | not counted | 5.73 11.53 9.70 |
| `v8_idle` | 07:42 | `--seconds 120 --writers plain:burst=1:pause=0.5 --takers-own 1 --takers-exported 0 --taker-sleep 0.01` | steps 1, 2 | 7,734 | 236 | none | not counted | 6.77 9.97 9.31 |
| `nc_mid_redo` | 07:44 | `--seconds 150 --writers drop,subxact:subs=3000,plain --takers-own 4 --takers-exported 0 --taker-sleep 0` | steps 1, 2 | 715,694 | 188,441 | none | not counted | 3.60 7.67 8.48 |
| `q_quiet_exported` | 07:47 | `--seconds 120 --writers plain,subxact,abort --takers-own 1 --takers-exported 0 --takers-quiet 2` | steps 1, 2; **not used** | 83,928 | 266,391 | none | not counted | 9.41 8.47 8.65 |
| `x_exported_subxact` | 07:54 | `--seconds 90 --writers plain,subxact,abort --takers-own 1 --takers-exported 2` | complete | 91,061 | 209,571 | **1,834, expected** | 1 | 1.60 4.92 7.05 |
| `main_recheck` | 08:00 | `--seconds 600 --writers plain,async,subxact,abort --takers-own 3 --takers-exported 0` | complete | 628,741 | 2,451,105 | none | 0 | 3.49 11.70 11.07 |
| `v5b_restart_crash` | 08:11 | as `v5_restart_crash` | complete | 89,718 | 722,901 | none | 0 | 5.59 6.61 8.47 |
| `v7b_exported_no_subxact` | 10:24 | as `v7_exported_no_subxact` | complete | 120,737 | 485,205 | none | 0 | 4.32 5.70 6.50 |
| `qb_quiet_exported` | 10:26 | as `q_quiet_exported` | complete | 82,928 | 268,363 | none | 0 | 6.00 5.94 6.49 |
| `v1b_big_drop` | 10:29 | as `v1_big_drop` | complete | 95,014 | 155,847 | none | 0 | 10.42 7.52 7.02 |
| `v3b_ddl_walswitch_pause` | 10:31 | as `v3_ddl_walswitch_pause` | complete | 81,540 | 477,939 | none | 0 | 42.64 22.15 12.89 |
| `xb_exported_subxact` | 10:33 | as `x_exported_subxact` | complete | 95,173 | 217,987 | **1,805, expected** | 1 | 9.79 16.39 11.85 |
| `xc_exported_subxact` | 10:39 | as `x_exported_subxact` | complete | 101,850 | 223,063 | **1,708, expected** | 2 | 3.22 6.92 8.86 |
| `v9_exported_open_no_subxact` | 10:44 | `--seconds 120 --writers long,plain,async,abort --takers-own 1 --takers-exported 2` | complete | 93,600 | 464,260 | none | 0 | 4.68 5.54 7.72 |

In every run: the stream never went back in `E`, no cut was left unchecked, and the slot was dropped at the
end.

What each run is for:

- `main`: the main campaign. Plain commits, commits with `synchronous_commit = off`, subtransactions, aborts.
  `main_recheck` is the same for 10 minutes with the complete checker.
- `v1b_big_drop`: big transactions and transactions that drop a table (long commit records, slow replay of a
  commit), a long-running transaction.
- `v2_twophase_subxact`: two-phase commit, committed or rolled back by another session and out of order;
  transactions with 3,000 subtransactions (more than 64).
- `v3b_ddl_walswitch_pause`: DDL on the tables, `VACUUM FULL`, WAL segment switches, replay paused and resumed.
- `v4_apply_delay`: `recovery_min_apply_delay = 300ms` on the standby.
- `v5_restart_crash`, `v5b_restart_crash`: the standby is restarted or killed every 10 s (7 restarts and 7
  kills in each run). `v5` has 6 cuts whose `R0` is below a position the stream had already delivered; `v5b`
  has none.
- `v6_slot_on_primary`: the slot is on the primary, the cuts on the standby. 4 cuts with `R0` below the stream.
- `v7b_exported_no_subxact`: cuts taken the exported way, writers of one statement, no subtransactions. 72,533
  exported cuts; all but 58 of them were quiet, so this run says little about snapshots that are not quiet.
- `v9_exported_open_no_subxact`: the same with a writer that holds its transaction open, so that most
  snapshots are not quiet: 55,598 exported cuts, 45,600 of them not quiet, none wrong. The design does not use
  this result (`docs/design/cut.md` section 5.5).
- `v8_idle`: an almost idle source (one commit every half second).
- `nc_mid_redo`: built to catch the two wrong rules that need rare timing (`r1_outside` 34,497 cuts,
  `naive_sharp` 34 cuts).
- `qb_quiet_exported`: exported snapshots that are used only when quiet (`xmin = xmax`), with subtransaction
  writers: 36,413 quiet exported cuts used (106,106 snapshots that were not quiet were dropped), and 46,515
  cuts taken the own way next to them. None wrong.
- `x_exported_subxact`, `xb_exported_subxact`, `xc_exported_subxact`: **expected to fail.** Exported snapshots
  on a standby next to subtransaction writers, every snapshot used. They show the server defect of
  `docs/design/cut.md` section 7 in bulk. The library is called with the switch that turns its refusal off
  (`accept_export_defect`), which exists for this test only.

  | Run | Exported cuts | Of them wrong | Own-way cuts | Of them wrong | Violations by rule | Missing rows |
  |---|---:|---:|---:|---:|---|---:|
  | `x` | 53,951 | 771 | 37,110 | 0 | `library` 1,361, `outside` 472, `evidence` 1 | 1 |
  | `xb` | 57,066 | 724 | 38,107 | 37 | `sharp` 858, `library` 472, `outside` 384, `inside` 90, `evidence` 1 | 1 |
  | `xc` | 61,599 | 684 | 40,251 | 0 | `sharp` 1,029, `outside` 339, `library` 338, `evidence` 2 | 2 |

  `sharp` is the narrowed answer that only the tests use; `x` ran before it was counted apart. `xb` is the run
  in which readers that imported nothing were wrong too (37 cuts, 74 violations): the damage an importer leaves
  for every other reader. From `xc` on the result records the violations per way and rule
  (`violations_by_way_and_rule`) and how many of the wrong cuts were quiet (none of 684 in `xc`). `xb` has the
  count per way only, so the rule its own-way cuts broke is an inference (`docs/design/cut.md` section 5.5).

### Negative control

Every run judges every cut a second time with six rules that are wrong on purpose and counts the cuts in which
the snapshot contradicts them. The line `wrong rules caught (cuts)` in each `.txt` file has the counts; the
`.json` file has one worked example per rule (`wrong_rule_examples`). In `main`:

| Wrong rule | Meaning | Cuts caught |
|---|---|---:|
| `stmt_as_r0` | the position read by the statement that takes the snapshot, used as `R0` | 15,291 |
| `r1_inside` | `E ≤ R1` taken as inside | 15,291 |
| `r1_outside` | `E > R1` taken as outside | 31 |
| `naive_sharp` | `S > R1` taken as outside, without the page-boundary exception | 0 (34 in `nc_mid_redo`, 2 in `v1b_big_drop`) |
| `xmax_inside` | id below `xmax` taken as inside | 716,780 |
| `xmin_outside` | id at or above `xmin` taken as outside | 847,284 |

`v8_idle` catches none of them: with one commit every half second no cut falls into the short windows. That is
what the idle run is expected to show.

### Distribution of the unknown set

Each `.txt` file ends with the table "unknown set (transactions per cut)": share of cuts with an empty set,
mean and maximum, for the base rule and with each addition. The `.json` file has the full histogram under
`unknown_set`.

- `xid` is what the pipeline has: the positions and the transaction id. `main`: 66.93% empty, mean 0.709,
  max 263.
- `sharp` and `xid+sharp` use the start of the commit record, which only the test decoder has (stock Debezium
  does not deliver it). They are a measurement of what is given up, not something the design counts on.
  `main`: `xid+sharp` 74.44% empty, mean 0.631, max 263.
- `base` is the positions alone. `main`: 0.00% empty, mean 1.878, max 265.

`v8_idle`: 7,716 cuts with one unknown transaction and 18 with two under the base rule, none with the id.

### The interrupted first attempt

`main_interrupted.txt` is the first attempt of the main campaign. It was stopped from outside after about
1,320 s (the whole session was paused), with no violation up to then. It has no summary and is not used.
`main` is the full repeat.

## Primary: campaigns

Command: `tools/cutstress primary-stress --label NAME … --out evidence/p3/cut/NAME.json`. Two cut-taking
sessions, a marker transaction every 0.02 s, the slot on the primary. With `--sync on` or `remote_apply` the
standby is a synchronous standby; with `--pause-period 2` its replay is paused every 2 s, so that commits are
in the stream before snapshots see them.

| Run | Options | Cuts | Answers checked | Wrong answers | Wrong rules caught (cuts) |
|---|---|---:|---:|---:|---|
| `primary_async` | `--seconds 60 --sync off` | 51,540 | 25,092,453 | 0 | `passed_flush` 10,130 |
| `primary_remote_apply` | `--seconds 60 --sync remote_apply --pause-period 2` | 54,489 | 26,441,568 | 0 | `later_snapshot` 2,317, `status_check` 2,313, `xmax_ge_x` 2,346, `passed_flush` 11,938 |
| `primary_sync_on` | `--seconds 40 --sync on` | 37,499 | 17,780,355 | 0 | `xmax_ge_x` 1, `passed_flush` 6,917 |
| `primary_twophase` | `--seconds 60 --writers plain,subxact,twophase --sync remote_apply --pause-period 2` | 53,684 | 40,979,142 | 0 | `later_snapshot` 3,103, `status_check` 3,092, `xmax_ge_x` 3,277 |

Sum: 197,212 cuts, 110,293,518 answers, none wrong. The first three runs use the default writers
`plain,async,subxact,long`. An "answer" is the library's `inside` or `outside` for one delivered transaction
in one cut, compared with what the snapshot shows.

The wrong rules of the primary: `later_snapshot` (any snapshot taken after the stream delivered a transaction
sees it), `status_check` (the same, after discarding snapshots whose in-progress list holds an id that
`pg_xact_status` calls committed: the coordinator's proposal), `xmax_ge_x` (a snapshot with `xmax` above the
id sees it), `passed_flush` (the stream has passed the snapshot when the applied position is above the flush
position read after the snapshot). `primary_twophase.txt` is the printed summary of that run; the three others
have the `.json` file only.

The primary campaigns ran before the review and were not repeated. The review did not change the rule they
test (the answer by 64-bit id). The live test `test_primary_rules_under_a_held_back_synchronous_standby` runs
a short form of them with the present code.

## Single scenarios

| File | Command | What it shows |
|---|---|---|
| `positions.json` | `tools/cutstress positions --out …` | Ten known transactions: `COMMIT.end_lsn` is `E`, `BEGIN.final_lsn` is the start of the commit record, the stream is in `E` order, a slot on the standby delivers the same ids and positions. Includes two-phase commits finished out of order by another session |
| `positions_no2pc.json` | the same, with `max_prepared_transactions = 0` | The earlier run; the two-phase cases are skipped |
| `export_defect.json` | `tools/cutstress export-defect --out …` | One case per kind of open transaction: a snapshot exported on a standby and imported by another session. Wrong when the open transaction has a subtransaction; right otherwise. `defect_shown` and the library's expectation (`export_defect(server_version_num)`) agree. Made again after the review |
| `keyless.json` | `tools/cutstress keyless --quiet-wait 1 --out …` | The rule for a table without a key on a server with the export defect. While a transaction with a subtransaction is open on the primary: the quiet-only wait ends with `NoQuietSnapshot` after 1.01 s and 18 attempts, and the keyless cut is one reader's own snapshot. After the commit: a quiet exported snapshot. New after the review |
| `standby_start.json` | `tools/cutstress standby-start --out …` | A commit waits for a synchronous standby across a checkpoint; the standby restarts from that checkpoint. Its snapshot hides the transaction although the commit record ends before `R0` (`rule_violated: true`). The library's settle floor refuses every such cut and accepts a cut after the next replayed commit. The file is the run made after the review; three earlier runs had the same outcome |
| `primary_gap.json` | `tools/cutstress primary-gap --out …` | The gap on a primary: a commit the stream delivers while snapshots do not see it yet, held for 3 s by a stopped or paused synchronous standby; plain, subtransaction and two-phase. What `pg_xact_status`, `pg_current_snapshot()` and the positions say during that time. Also `synchronized_standby_slots` with a failover slot |
| `primary_gap_no2pc.json` | the same, with `max_prepared_transactions = 0` | The earlier run; the two-phase cases are skipped |
| `promotion.json` | `tools/cutstress promotion --out …` | An open cut across a promotion: a second standby is added, a cut held open on it, the standby promoted, a row written on it, the standby removed again. The cut recorded timeline 1; the promoted node is on timeline 2, while its control file still says 1 right after the promotion; `valid_on` is false for the new timeline; the library refuses a new standby cut and a new settle floor on the promoted node; a primary cut taken there is valid on timeline 2 only. Made again after the review |

## Model check

`model.txt` is the output of `tools/cutstress model --sizes 1:3,2:3,2:4`, made again after the review: every
interleaving of a small model of replay and of a reader, for the standby rule, and of commit and delivery for
the primary rule. No sound rule is violated; every wrong rule is refuted. As a control, a model of a server
that moves the replay position before the commit becomes visible is caught (line 10 of the file).

The same check runs in the tests at two sizes (`tests/cut/test_cut_model.py`): the default test run has the
sizes 1:2 and 2:2 and 60 random larger worlds (under 1 s); with the marker `localpg`, the sizes 1:4 and 2:3
and 600 random larger worlds (about 10 s). `tools/cutstress model` without `--sizes` runs 1:4 and 2:3.

## Redaction

The files hold no host name, user name, password, connection string or home-directory path. The tool writes
`<home>` where the cluster home would appear. Ports 55600 and 55601 are the private cluster of this work.

## Reproduce

```
tools/localpg --home <home> --port-base 55600 up
tools/cutstress --home <home> --port-base 55600 model --sizes 1:3,2:3,2:4
tools/cutstress --home <home> --port-base 55600 run --label main --seconds 1800 \
    --writers plain,async,subxact,abort --takers-own 3 --takers-exported 0 --out main.json
tools/cutstress --home <home> --port-base 55600 primary-stress --label primary_remote_apply --seconds 60 \
    --sync remote_apply --pause-period 2 --out primary_remote_apply.json
tools/cutstress --home <home> --port-base 55600 standby-start --out standby_start.json
tools/cutstress --home <home> --port-base 55600 keyless --quiet-wait 1 --out keyless.json
tools/localpg --home <home> --port-base 55600 down
```

`--home` and `--port-base` default to the private cluster of this work, so the commands in the tables above
run as written. The other runs follow from the tables. For the runs with two-phase commit, first:
`tools/localpg --home <home> --port-base 55600 set all max_prepared_transactions=16`.

The live tests run a short version of the same things in about one minute:
`uv run pytest -m localpg tests/cut/test_cut_live.py`.

**Another major version.** `tools/cutstress` and the live tests reach Postgres only through `tools/localpg`,
which takes the binaries from `CDC_V2_PG_BIN`. A cluster home belongs to the major version that initialized
it, so a run on another version needs another home (and its own port range):

```
CDC_V2_PG_BIN=<bin directory of 16> tools/cutstress --home <another home> --port-base <base> standby-start
CDC_V2_PG_BIN=<bin directory of 16> tools/cutstress --home <another home> --port-base <base> export-defect
CDC_V2_PG_BIN=<bin directory of 16> CDC_V2_CUT_PG_HOME=<another home> CDC_V2_CUT_PG_PORT_BASE=<base> \
    uv run pytest -m localpg tests/cut/test_cut_live.py
```

None of this was run on 16 here.
