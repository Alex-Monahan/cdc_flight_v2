# State machines

This file is generated from the declarations in the code. Do not edit it; run
`uv run python -m cdc_v2.state > docs/state_machines.md` after a machine was added or changed. A test
(`tests/destination/test_state.py`) fails when the file and the declarations differ.

## What a machine is

A state machine is a set of named states and a list of allowed changes from one state to another (transitions). Its
current state is stored in one column of one table in the destination, one row per key, in the same database and
the same transaction as the data the state governs (SPEC 1.9). A package declares its machine with
`cdc_v2.state.declare`.

A state is changed in one way only: `cdc_v2.state.transition(tx, machine, key, from_state, to_state, cause)`, and
`cdc_v2.state.create` for a new row. The function refuses a change that is not in the list below. It changes the
row only if the row still holds `from_state`. It writes one row to `_cdc.transitions` for every change: machine,
key, from, to, cause, epoch and MotherDuck's time. A static test fails when any other code under `src/` writes a
state column.

## How to read the blocks

Each block below is one machine.

- "Stored in" names the table and the column that hold the state.
- "Key" names the columns that identify one row.
- "States" lists every state the column can hold.
- "A new row starts in" lists the states `create` accepts.
- The table lists every allowed transition. A pair that is not in the table is refused. A state with no row in the
  "From" column is final.

## The machines

### alert

One alert record (SPEC 6.2). `resolved` means the condition it reported is over.

- Stored in: `_cdc.alerts.state`
- Key: `alert_id`
- States: `open`, `resolved`
- A new row starts in: `open`

| From | To |
|---|---|
| `open` | `resolved` |
