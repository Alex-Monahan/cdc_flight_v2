# Rules for every worker

You build one package of this repository, alone, in your own git worktree and on your own branch. The orchestrator
reviews and merges. These rules hold for every package; your task file adds the rest.

## Read first

1. `SPEC.md` and `PLAN.md` completely. PLAN §3 (working rules), §4 (boundaries) and §9 are binding for you.
2. `docs/design/core.md` (the design) and `docs/design/p3_plan.md` (the packages and the contracts between them).
3. Your task file, and what it names.

Before you start: `git fetch origin && git merge --ff-only origin/main`.

## Boundaries

- Work only inside your worktree, in `~/.cache/cdc_flight_v2`, and with public documentation and public source
  code. No other folders. No MCP tools. No browser tools.
- No Docker, no containers, no virtual machines. Postgres runs natively and only through `tools/localpg`, in the
  private home and on the ports that `docs/design/p3_plan.md` section 4 gives your package. Never the default
  home, never a port outside your range.
- The shared T1 fixtures (`tests/conftest.py`: `source_db`, `dest_db`, `private_cluster`, …) take their cluster
  and their MotherDuck prefix from the environment. Before you run a T1 test, export `CDC_V2_PG_HOME` and
  `CDC_V2_PG_PORT_BASE` (your own home and port base) and `CDC_V2_MD_PREFIX` (your own prefix); never set
  `CDC_V2_T1_SHARED`. The fixture `private_cluster` leases a block of ports from 55840–56095 for a cluster that
  a test may break; that range is open to every worker, through the fixture only.
- Port 5432 belongs to the owner. Never connect to it, never signal its processes, never touch its data.
- Signals only to processes that you started yourself or through `tools/localpg`. Never `pkill`, never `killall`.
- MotherDuck, when your task uses it: only through `tools/with-env` (it supplies the token; never print it, never
  write it anywhere). Create, read, write and drop only databases with your package's prefix (section 4 of the
  plan). Never list, name or read another database. Use `md_active_server_connections()` only filtered by
  connection ids of your own. Create no Flight unless your task says so.
- Subagents: none, with one exception. Before you finish you may launch one read-only reviewer with fresh context
  to look for bugs in your code; set its `model` to `opus` explicitly. Never use another model for it.
- The repository is public. Never commit a secret, password, host name, database user name that comes from the
  environment, connection string, home-directory path or operating-system user name. Evidence is redacted the same
  way (write `<home>` for a home directory). The secret scan runs as a git hook on every commit and push. Never
  bypass it (`--no-verify` is forbidden). Never change the hooks, the scanner, its tests or `.secretscan-allow`.

## Git

- Commit small and often on your branch; push it with `git push origin HEAD`. Never push to `main`; never merge
  into `main`. You may merge `origin/main` into your branch when you need something that was merged meanwhile.
- End every commit message with these two lines:

  ```
  Co-Authored-By: Claude <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_012frBvfckCxHP2mwrxFKN7z
  ```

- Do not edit `SPEC.md`, `PLAN.md`, `STATUS.md`, `docs/design/core.md`, `docs/design/p3_plan.md` or `docs/tasks/`.
  Do not edit `pyproject.toml` or `uv.lock` unless your task says so; a dependency or marker you need goes into
  your report. Run Python through `uv run`.

## Contracts

- Build exactly to the contracts of `docs/design/p3_plan.md` section 2. Other workers build against them at the
  same time.
- Where a contract leaves something open, take the simplest reading, write it down in your package's document
  under "Contract notes", and name it in your report.
- Where a contract or the design is wrong (it contradicts SPEC, or it cannot work), stop that part, write down the
  case with the evidence, and report it. Do not work around it silently.
- Shared small modules exist already; use them, do not make a second one: `cdc_v2.lsn` (positions),
  `cdc_v2.deadline` (deadlines), `cdc_v2.crashpoints` and `cdc_v2.faultsites` (named crash points and fault
  sites), `cdc_v2.testing` (process control, crash runner, TCP fault proxy, byte-stream relay, and
  `harness_evidence.write_evidence`, the one writer of evidence files); see `docs/testing/harness.md`.

## Tests and evidence

- The default run (`uv run -q pytest -q`) is hermetic and fast: no network, no Postgres, no JVM. What you add to
  it takes a few seconds in total. It must pass when you finish.
- Tests that need more carry markers and are opt-in: `localpg` (a private local Postgres pair), `md` (MotherDuck),
  `jvm` (a local JVM), `t1` (the tier-T1 harness: all of them together). Say in your report how to run each suite.
- A test removes everything it creates outside the repository, also when it fails.
- Never weaken a check to make a run pass. A failure you find is a result: it becomes a seeded regression test
  (SPEC §4), and it is named in your report.
- Evidence goes to `evidence/p3/<your package>/`, redacted, with the command, versions, seeds and counts that let
  a grader repeat it (SPEC §4).
- The machine has 10 cores and 16 GB and is shared with up to three other workers. Keep your load modest (a JVM
  heap of at most 1 GB, a few sessions), time-box long runs, and say in the evidence what else may have been
  running. Numbers measured here are indications only.

## Documents

Write plainly: short sentences, every term defined once, no claim without a pointer to evidence or a source.

## When you finish

- Leave the machine clean: your Postgres clusters stopped, no replication slot of yours left on a running node,
  nothing listening on your ports, no process of yours running, your MotherDuck test databases dropped.
- Run the default run and your opt-in suites one last time. Push your branch.
- Your final message is a report for the orchestrator, in plain prose: what is built and proven (with counts),
  what is not done or open, every contract note, files delivered, how to run the suites, the state you left the
  machine in, and anything that surprised you.
