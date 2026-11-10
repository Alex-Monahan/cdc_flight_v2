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
- All runs are from 2026-11-10 (UTC). The campaigns ran between 06:41 and 08:15 UTC; each `.json` file of a
  campaign carries its start time (`started_utc`). The single scenarios carry no time; the earliest ran
  before the main campaign, the last (`promotion.json`) after 08:00 UTC.
- Tool: `tools/cutstress` (modules `src/cdc_v2/testing/cutstress.py`, `cutprimary.py`, `cutmodel.py`); the
  rule under test is `src/cdc_v2/pg/cut.py`.

## What else was running

The machine was shared the whole time with a throughput measurement and with other workers of the same build
(their own Postgres clusters on other ports, test runs, Python processes). I did not control that load. Each
stress run prints the load average (1, 5 and 15 minutes) right before it starts; the tables below repeat it.
The highest value is 41.55 before `v6_slot_on_primary`.

The main campaign does not print it. I read it by hand: 5.25 5.67 5.16 before, 6.35 11.22 11.25 about halfway,
6.24 8.89 10.24 after. The primary campaigns and the single scenarios do not record it. The primary campaigns
ran between 07:19 and 07:58 UTC, between the standby runs listed below.

My own load: at most 4 writer sessions, at most 4 cut-taking sessions and one session reading the stream.

Load changes how many cuts and transactions a run reaches and how large the unknown set is. It does not change
what counts as a violation.

## Standby: stress runs

Each run has two files: `NAME.txt` is what the tool printed (first line: the command), `NAME.json` is the full
result, with the configuration, counters, the distribution of the unknown set, examples for every wrong rule
and every violation.

Common options not repeated in the table: `tools/cutstress run --label NAME … --out evidence/p3/cut/NAME.json`.

| Run | Options | Cuts checked | Transactions | Violations of the sound rules | Load before |
|---|---|---:|---:|---|---|
| `main` | `--seconds 1800 --writers plain,async,subxact,abort --takers-own 3 --takers-exported 0` | 1,854,463 | 7,305,063 | none | see above |
| `v1_big_drop` | `--seconds 120 --writers big,drop,plain,long --takers-own 2 --takers-exported 0` | 104,047 | 157,457 | none | 7.97 6.83 7.41 |
| `v2_twophase_subxact` | `--seconds 120 --writers twophase,plain,subxact:subs=3000 --takers-own 3 --takers-exported 0` | 141,029 | 315,222 | none | 5.87 5.77 7.17 |
| `v3_ddl_walswitch_pause` | `--seconds 120 --writers plain,async,subxact,abort --takers-own 2 --takers-exported 0 --chaos ddl,walswitch,pause` | 84,094 | 483,333 | none | 4.71 6.12 7.00 |
| `v4_apply_delay` | `--seconds 120 --writers plain,async,subxact --takers-own 2 --takers-exported 0 --apply-delay-ms 300` | 100,763 | 363,741 | none | 6.10 6.33 6.98 |
| `v5_restart_crash` | `--seconds 180 --writers plain,async,subxact,abort --takers-own 2 --takers-exported 0 --chaos restart,crash --chaos-period 10` | 76,431 | 703,213 | none | 5.21 5.95 6.73 |
| `v6_slot_on_primary` | `--seconds 120 --writers plain,async,subxact,abort --takers-own 2 --takers-exported 0 --slot-node primary` | 86,409 | 477,973 | none | 41.55 17.03 10.78 |
| `v7_exported_no_subxact` | `--seconds 120 --writers plain,async,abort --takers-own 1 --takers-exported 2` | 125,445 | 485,337 | none | 5.73 11.53 9.70 |
| `v8_idle` | `--seconds 120 --writers plain:burst=1:pause=0.5 --takers-own 1 --takers-exported 0 --taker-sleep 0.01` | 7,734 | 236 | none | 6.77 9.97 9.31 |
| `nc_mid_redo` | `--seconds 150 --writers drop,subxact:subs=3000,plain --takers-own 4 --takers-exported 0 --taker-sleep 0` | 715,694 | 188,441 | none | 3.60 7.67 8.48 |
| `q_quiet_exported` | `--seconds 120 --writers plain,subxact,abort --takers-own 1 --takers-exported 0 --takers-quiet 2` | 83,928 | 266,391 | none | 9.41 8.47 8.65 |
| `main_recheck` | `--seconds 600 --writers plain,async,subxact,abort --takers-own 3 --takers-exported 0` | 628,741 | 2,451,105 | none | 3.49 11.70 11.07 |
| `v5b_restart_crash` | as `v5_restart_crash` | 89,718 | 722,901 | none | 5.59 6.61 8.47 |
| `x_exported_subxact` | `--seconds 90 --writers plain,subxact,abort --takers-own 1 --takers-exported 2` | 91,061 | 209,571 | **1,834, expected** (see below) | 1.60 4.92 7.05 |

Sums: the ten variants `v1` to `v8`, `nc_mid_redo` and `q_quiet_exported` have 1,525,574 cuts and 3,441,344
transactions. The two repeats `main_recheck` and `v5b_restart_crash` have 718,459 cuts and 3,174,006
transactions. With `main` that is 4,098,496 cuts and 13,920,413 transactions without a violation.
`x_exported_subxact` is not part of these sums.

What each run is for:

- `main`: the main campaign. Plain commits, commits with `synchronous_commit = off`, subtransactions, aborts.
- `v1_big_drop`: big transactions and transactions that drop a table (long commit records, slow replay of a
  commit), a long-running transaction.
- `v2_twophase_subxact`: two-phase commit, committed or rolled back by another session and out of order;
  transactions with 3,000 subtransactions (more than 64).
- `v3_ddl_walswitch_pause`: DDL on the tables, `VACUUM FULL`, WAL segment switches, replay paused and resumed.
- `v4_apply_delay`: `recovery_min_apply_delay = 300ms` on the standby.
- `v5_restart_crash`, `v5b_restart_crash`: the standby is restarted or killed every 10 s (7 restarts and 7
  kills in each run). `v5` has 6 cuts whose `R0` is below a position the stream had already delivered; `v5b`
  has none.
- `v6_slot_on_primary`: the slot is on the primary, the cuts on the standby. 4 cuts with `R0` below the stream.
- `v7_exported_no_subxact`: cuts taken the exported way, no subtransaction writers.
- `v8_idle`: an almost idle source (one commit every half second).
- `nc_mid_redo`: built to catch the two wrong rules that need rare timing (`r1_outside` 34,497 cuts,
  `naive_sharp` 34 cuts).
- `q_quiet_exported`: exported snapshots that are used only when quiet (`xmin = xmax`), with subtransaction
  writers.
- `main_recheck`, `v5b_restart_crash`: repeats with the complete checker (see "The late check").
- `x_exported_subxact`: **expected to fail.** Exported snapshots on a standby next to subtransaction writers.
  It shows the server defect of `docs/design/cut.md` section 7 in bulk: 771 of 53,951 exported cuts are wrong
  (472 violations of the outside rule, 1,361 answers of the library that the snapshot contradicts, 1 row
  missing from a snapshot), 0 of 37,110 cuts taken the own way. The library is called here with the switch that
  turns its refusal off (`accept_export_defect`), which exists for this test only.

### Negative control

Every run judges every cut a second time with six rules that are wrong on purpose and counts the cuts in which
the snapshot contradicts them. The line `wrong rules caught (cuts)` in each `.txt` file has the counts; the
`.json` file has one worked example per rule (`wrong_rule_examples`). In `main`:

| Wrong rule | Meaning | Cuts caught |
|---|---|---:|
| `stmt_as_r0` | the position read by the statement that takes the snapshot, used as `R0` | 15,291 |
| `r1_inside` | `E ≤ R1` taken as inside | 15,291 |
| `r1_outside` | `E > R1` taken as outside | 31 |
| `naive_sharp` | `S > R1` taken as outside, without the page-boundary exception | 0 (34 in `nc_mid_redo`, 11 in `v1_big_drop`) |
| `xmax_inside` | id below `xmax` taken as inside | 716,780 |
| `xmin_outside` | id at or above `xmin` taken as outside | 847,284 |

`v8_idle` catches none of them: with one commit every half second no cut falls into the short windows. That is
what the idle run is expected to show.

### Distribution of the unknown set

Each `.txt` file ends with the table "unknown set (transactions per cut)": share of cuts with an empty set,
mean and maximum, for the base rule and with each addition (`xid`, `sharp`, `xid+sharp`). The `.json` file has
the full histogram under `unknown_set`. `main`: base 0.00% empty, mean 1.878, max 265; `xid+sharp` 74.44%
empty, mean 0.631, max 263. `v8_idle`: 7,716 cuts with one unknown transaction and 18 with two under the base
rule, none with both additions.

### The late check

The checker compares, for every key, the last value the stream delivered inside the cut with what the snapshot
read. A check that the snapshot has a row for every key at all (step 3 of the checker, counter
`rows_missing_from_a_cut`) was added after `main` and the ten variants had run. `main_recheck`,
`v5b_restart_crash` and the second run of `x_exported_subxact` ran with it: 0 missing rows in the first two, 1
in the third (the expected failure). The earlier runs were not repeated. `tests/cut/test_cut_checker.py` tests
the checker itself, without a server.

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

## Single scenarios

| File | Command | What it shows |
|---|---|---|
| `positions.json` | `tools/cutstress positions --out …` | Ten known transactions: `COMMIT.end_lsn` is `E`, `BEGIN.final_lsn` is the start of the commit record, the stream is in `E` order, a slot on the standby delivers the same ids and positions. Includes two-phase commits finished out of order by another session |
| `positions_no2pc.json` | the same, with `max_prepared_transactions = 0` | The earlier run; the two-phase cases are skipped |
| `export_defect.json` | `tools/cutstress export-defect --out …` | One case per kind of open transaction: a snapshot exported on a standby and imported by another session. Wrong when the open transaction has a subtransaction; right otherwise. `defect_shown` and the library's expectation agree |
| `standby_start.json` | `tools/cutstress standby-start --out …` | A commit waits for a synchronous standby across a checkpoint; the standby restarts from that checkpoint. Its snapshot hides the transaction although the commit record ends before `R0` (`rule_violated: true`). The library's settle floor refuses every such cut and accepts a cut after the next replayed commit. Run three times with the same outcome; the file is the last run |
| `primary_gap.json` | `tools/cutstress primary-gap --out …` | The gap on a primary: a commit the stream delivers while snapshots do not see it yet, held for 3 s by a stopped or paused synchronous standby; plain, subtransaction and two-phase. What `pg_xact_status`, `pg_current_snapshot()` and the positions say during that time. Also `synchronized_standby_slots` with a failover slot |
| `primary_gap_no2pc.json` | the same, with `max_prepared_transactions = 0` | The earlier run; the two-phase cases are skipped |
| `promotion.json` | `tools/cutstress promotion --out …` | An open cut across a promotion: a second standby is added, a cut held open on it, the standby promoted, a row written on it, the standby removed again. The file has no version field; the server was 18.6 |

## Model check

`model.txt` is the output of `tools/cutstress model --sizes 1:3,2:3,2:4`: every interleaving of a small model
of replay and of a reader, for the standby rule, and of commit and delivery for the primary rule. No sound rule
is violated; every wrong rule is refuted. As a control, a model of a server that moves the replay position
before the commit becomes visible is caught (line 10 of the file). The hermetic test
`tests/cut/test_cut_model.py` runs smaller sizes on every default test run.

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
tools/localpg --home <home> --port-base 55600 down
```

`--home` and `--port-base` default to the private cluster of this work, so the commands in the tables above
run as written. The other runs follow from the tables. For the runs with two-phase commit, first:
`tools/localpg --home <home> --port-base 55600 set all max_prepared_transactions=16`.

The live tests run a short version of the same things in about one minute:
`uv run pytest -m localpg tests/cut/test_cut_live.py`.
