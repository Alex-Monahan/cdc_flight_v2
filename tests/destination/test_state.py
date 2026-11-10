"""Tier T0 (local DuckDB): the transition function (contract C5), the enumeration helper of SPEC 1.9 with two
example machines, and the generated document `docs/state_machines.md`."""
import os

import pytest

from cdc_v2 import alerts, md, state
from cdc_v2.testing import machines as tm

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def machine(**changes):
    args = dict(name="t_m", states=("a", "b"), transitions=(("a", "b"),), table="s.t", key_columns=("k",),
                state_column="st")
    args.update(changes)
    return state.Machine(**args)


@pytest.fixture
def ex(led):
    """A leading destination with the two example machines declared and their tables created."""
    lane = led.lane("stream", led.epoch)
    with tm.example_machines(lane) as (door, job):
        led.door, led.job, led.stream, led.look = door, job, lane, led.connect("look")
        yield led


def log_rows(conn, name):
    return [tuple(r) for r in conn.fetchall(
        "SELECT key, from_state, to_state, cause, epoch FROM _cdc.transitions WHERE machine = ? "
        "ORDER BY changed_at, to_state", [name])]


class Untouchable:
    """Stands for a transaction that must not be used."""
    def __getattr__(self, name):
        raise AssertionError("the database was touched (%s)" % name)


# ------------------------------------------------------------------------------------------------- declarations
@pytest.mark.parametrize("changes", [
    dict(name="Bad"), dict(name=""), dict(name="has space"), dict(name=None),
    dict(states=()), dict(states=("a", "a")), dict(states=("a", "B")), dict(states=("a", 1)),
    dict(transitions=(("a", "c"),)), dict(transitions=(("c", "a"),)), dict(transitions=(("a", "b"), ("a", "b"))),
    dict(table="a.b.c"), dict(table="bad name"), dict(table=""), dict(table="t; DROP TABLE x"),
    dict(key_columns=("k", "k")), dict(key_columns=("bad col",)), dict(key_columns=("st",)),
    dict(state_column="bad col"), dict(state_column=""),
    dict(initial=("c",)), dict(description="two\nlines"),
])
def test_a_wrong_declaration_is_refused(changes):
    with pytest.raises(ValueError):
        machine(**changes)


def test_declare_once():
    m = machine(key_columns="k", transitions=[["a", "b"], ("b", "b")])
    assert m.key_columns == ("k",) and m.transitions == (("a", "b"), ("b", "b"))
    assert m.allows("a", "b") and m.allows("b", "b") and not m.allows("b", "a") and not m.allows("a", "a")
    try:
        assert state.declare(m) is m
        assert state.declare(machine(key_columns="k", transitions=[["a", "b"], ("b", "b")])) is m    # the same again
        assert state.get("t_m") is m and m in state.machines()
        with pytest.raises(ValueError, match="twice"):
            state.declare(machine())                                    # same name, other content
        with pytest.raises(ValueError, match="share one state column"):
            state.declare(machine(name="t_other", table="S.T", state_column="ST"))
        with pytest.raises(TypeError):
            state.declare("t_m")
        names = [x.name for x in state.machines()]
        assert names == sorted(names)
    finally:
        state._forget("t_m")
    with pytest.raises(state.StateError):
        state.get("t_m")
    with pytest.raises(state.StateError, match="not declared"):
        state.transition(Untouchable(), m, 1, "a", "b", "cause")
    with pytest.raises(state.StateError):
        state.read(Untouchable(), "t_m", 1)
    with pytest.raises(Exception):
        m.name = "other"                                                # frozen


# ----------------------------------------------------------------------------------------- create, transition, read
def test_create_transition_and_read(ex):
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed", values={"opened": 0})
        assert state.read(tx, ex.door, "d1") == "closed"
        state.transition(tx, ex.door, "d1", "closed", "open", "someone came")
        assert state.read(tx, "example_door", ("d1",)) == "open"
        assert state.read(tx, ex.door, {"door_id": "d1"}) == "open"
        assert state.read(tx, ex.door, "d2") is None
    assert state.read(ex.look, ex.door, "d1") == "open"
    assert ex.look.fetchall("SELECT door_id, state, opened FROM _cdc_example.door") == [("d1", "open", 0)]
    assert log_rows(ex.look, "example_door") == [
        ('["d1"]', None, "closed", "installed", ex.epoch), ('["d1"]', "closed", "open", "someone came", ex.epoch)]
    assert ex.look.fetchone("SELECT count(*) FROM _cdc.transitions WHERE changed_at IS NULL")[0] == 0
    # the time is MotherDuck's clock in the statement (locally: the same expression), within a minute of now
    assert ex.look.fetchone("SELECT max(abs(epoch_us(changed_at) - " + md.clock_sql.join(["epoch_us(", ")"])
                            + ")) < 60000000 FROM _cdc.transitions")[0] is True


def test_a_transition_is_part_of_the_callers_transaction(ex):
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed")
    with pytest.raises(RuntimeError, match="boom"):
        with ex.stream.transaction() as tx:
            state.transition(tx, ex.door, "d1", "closed", "locked", "night")
            state.create(tx, ex.door, "d2", "closed", "installed")
            assert state.read(tx, ex.door, "d1") == "locked"
            raise RuntimeError("boom")
    assert state.read(ex.look, ex.door, "d1") == "closed" and state.read(ex.look, ex.door, "d2") is None
    assert len(log_rows(ex.look, "example_door")) == 1


def test_an_undeclared_pair_is_refused_before_the_database_is_touched(ex):
    for a, b in [("open", "locked"), ("locked", "open"), ("closed", "closed"), ("open", "no_such"), ("no_such", "open"),
                 (None, "open"), ("open", None)]:
        with pytest.raises(state.IllegalTransition) as info:
            state.transition(Untouchable(), ex.door, "d1", a, b, "cause")
        e = info.value
        assert (e.declared, e.stored, e.machine, e.key, e.from_state, e.to_state) == (
            False, None, "example_door", ("d1",), a, b)


def test_the_wrong_stored_state_is_refused_and_the_transaction_stays_usable(ex):
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed")
    with ex.stream.transaction() as tx:
        with pytest.raises(state.IllegalTransition) as info:
            state.transition(tx, ex.door, "d1", "open", "closed", "cause")      # declared, but the door is closed
        assert (info.value.declared, info.value.stored) == (True, "closed")
        assert "the stored state is 'closed'" in str(info.value)
        with pytest.raises(state.IllegalTransition) as info:
            state.transition(tx, ex.door, "nobody", "closed", "open", "cause")  # no row
        assert (info.value.declared, info.value.stored) == (True, None)
        state.transition(tx, ex.door, "d1", "closed", "open", "after the refusals")
    assert state.read(ex.look, ex.door, "d1") == "open"
    assert [r[3] for r in log_rows(ex.look, "example_door")] == ["installed", "after the refusals"]


def test_create_fails_when_the_key_exists_or_the_state_is_not_initial(ex):
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed")
        with pytest.raises(state.IllegalTransition) as info:
            state.create(tx, ex.door, "d1", "closed", "again")
        assert info.value.stored == "closed" and info.value.declared is True
        for bad in ("open", "locked", "no_such", None):                         # a door starts closed
            with pytest.raises(state.IllegalTransition) as info:
                state.create(Untouchable(), ex.door, "d9", bad, "cause")
            assert info.value.declared is False
        state.create(tx, ex.job, (1, 2), "done", "any state: the job machine names no initial states")
        for col in ("door_id", "state", "bad col", "x; y", 7):
            with pytest.raises(state.StateError):
                state.create(Untouchable(), ex.door, "d9", "closed", "cause", values={col: 1})
    assert ex.look.fetchone("SELECT count(*) FROM _cdc_example.door")[0] == 1
    assert state.read(ex.look, ex.job, (1, 2)) == "done"


def test_keys(ex):
    with ex.stream.transaction() as tx:
        state.create(tx, ex.job, (7, 1), "queued", "new")
        state.create(tx, ex.job, {"part": None, "batch": 7}, "queued", "a key value may be NULL")
        state.transition(tx, ex.job, [7, None], "queued", "running", "start")
        assert state.read(tx, ex.job, (7, None)) == "running" and state.read(tx, ex.job, (7, 1)) == "queued"
        for bad in ((7,), (7, 1, 2), 7, None, {"batch": 7}, {"batch": 7, "part": 1, "x": 2}):
            with pytest.raises(state.StateError):
                state.read(tx, ex.job, bad)
    assert log_rows(ex.look, "example_job")[-1][:3] == ("[7, null]", "queued", "running")


def test_two_rows_for_one_key_fail_the_transaction(ex):
    ex.look.execute("INSERT INTO _cdc_example.job VALUES (1, 1, 'queued'), (1, 1, 'queued')")
    with pytest.raises(state.StateError, match="not unique"):
        with ex.stream.transaction() as tx:
            try:
                state.transition(tx, ex.job, (1, 1), "queued", "running", "start")
            except state.StateError:
                pass                                    # a caller that swallows it still cannot commit
    assert ex.look.fetchall("SELECT DISTINCT status FROM _cdc_example.job") == [("queued",)]
    with pytest.raises(state.StateError, match="2 rows"):
        state.read(ex.look, ex.job, (1, 1))


@pytest.mark.parametrize("cause", ["", "   ", None, 7, "x" * 201, "two\nlines", "tab\there"])
def test_a_cause_is_a_short_line(ex, cause):
    with pytest.raises(state.StateError, match="cause"):
        state.transition(Untouchable(), ex.door, "d1", "closed", "open", cause)
    with pytest.raises(state.StateError, match="cause"):
        state.create(Untouchable(), ex.door, "d1", "closed", cause)


def test_two_transactions_cannot_both_make_one_transition(ex):
    """The function writes the column its condition reads, so two transactions that both saw `closed` conflict
    (conflicts are per column, `evidence/p3/destination/fence_columns.json`)."""
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed")
    control = ex.lane("control", ex.epoch)
    with pytest.raises(md.Conflict):
        with ex.stream.transaction() as tx1:
            state.transition(tx1, ex.door, "d1", "closed", "open", "first")
            with control.transaction() as tx2:
                assert state.read(tx2, ex.door, "d1") == "closed"       # it does not see the first one
                state.transition(tx2, ex.door, "d1", "closed", "locked", "second")
    assert state.read(ex.look, ex.door, "d1") == "closed"               # the inner error ended both blocks
    with ex.stream.transaction() as tx1:
        state.transition(tx1, ex.door, "d1", "closed", "open", "first")
    with control.transaction() as tx2:
        with pytest.raises(state.IllegalTransition) as info:
            state.transition(tx2, ex.door, "d1", "closed", "locked", "second")
        assert info.value.stored == "open"
    assert [r[3] for r in log_rows(ex.look, "example_door")] == ["installed", "first"]


def test_two_creators_of_one_key_meet_only_through_the_fence_row_of_their_lane(ex):
    """Found by the review. `create` looks for a row of the key with what its transaction sees, and it writes
    nothing that a second creator reads. Two transactions that overlap and create one key therefore meet only
    in the fence row: on one lane name the second one cannot commit, on two lanes both commit and the key has
    two rows. The layer does not prevent that (no table of this package has a unique constraint); it is a rule
    for the callers: the rows of one machine are created from one lane. What the layer does guarantee is that
    the damage is seen: every later `read`, `transition` and `create` of the key fails."""
    control = ex.lane("control", ex.epoch)
    with ex.stream.transaction() as tx1:
        state.create(tx1, ex.door, "d1", "closed", "first")
        with control.transaction() as tx2:
            assert state.read(tx2, ex.door, "d1") is None               # it does not see the first one
            state.create(tx2, ex.door, "d1", "closed", "second")
    assert ex.look.fetchone("SELECT count(*) FROM _cdc_example.door WHERE door_id = 'd1'")[0] == 2
    assert sorted(r[3] for r in log_rows(ex.look, "example_door")) == ["first", "second"]
    with pytest.raises(state.StateError, match="2 rows"):
        state.read(ex.look, ex.door, "d1")
    with pytest.raises(state.StateError, match="not unique"):
        with ex.stream.transaction() as tx:
            state.transition(tx, ex.door, "d1", "closed", "open", "someone came")
    with pytest.raises(state.StateError, match="2 rows"):
        with ex.stream.transaction() as tx:
            state.create(tx, ex.door, "d1", "closed", "third")          # nor is a third row added
    assert ex.look.fetchone("SELECT count(*) FROM _cdc_example.door WHERE door_id = 'd1'")[0] == 2
    # one lane name: the transactions share the fence row, so the one that commits second is refused
    twin = ex.lane("stream", ex.epoch)
    with pytest.raises(md.Conflict):
        with ex.stream.transaction() as tx1:
            state.create(tx1, ex.door, "d2", "closed", "first")
            with twin.transaction() as tx2:
                state.create(tx2, ex.door, "d2", "closed", "second")
    assert ex.look.fetchone("SELECT count(*) FROM _cdc_example.door WHERE door_id = 'd2'")[0] == 1
    assert state.read(ex.look, ex.door, "d2") == "closed"
    assert sorted(r[3] for r in log_rows(ex.look, "example_door") if r[0] == '["d2"]') == ["second"]


# ------------------------------------------------------------------------------------------------- enumeration
def test_every_pair_of_the_example_machines(ex):
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed")
        state.create(tx, ex.job, (1, 1), "queued", "new")
    door = tm.check_machine(ex.stream, ex.door, "d1")
    assert door == {"stored_states": 4, "attempts": 64, "legal": 4, "illegal": 60}
    job = tm.check_machine(ex.stream, "example_job", (1, 1), unknown=("no_such_state", "DONE"))
    assert job == {"stored_states": 6, "attempts": 216, "legal": 5, "illegal": 211}
    assert state.read(ex.look, ex.door, "d1") == "closed" and state.read(ex.look, ex.job, (1, 1)) == "queued"
    assert len(log_rows(ex.look, "example_door")) == 1 + 4 and len(log_rows(ex.look, "example_job")) == 1 + 5


def test_every_pair_of_the_alert_machine(ex):
    alert_id = alerts.emit_now(ex.stream, "test_alert", "info", {"n": 1})
    assert tm.check_machine(ex.stream, alerts.ALERT, alert_id) == {
        "stored_states": 3, "attempts": 27, "legal": 1, "illegal": 26}
    assert state.read(ex.look, alerts.ALERT, alert_id) == "open"


@pytest.mark.parametrize("broken", ["accepts_everything", "ignores_the_stored_state", "does_not_log",
                                    "refuses_but_changes", "refuses_a_legal_one"])
def test_the_enumeration_catches_a_wrong_transition_function(ex, monkeypatch, broken):
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed")
    real, m = state.transition, ex.door

    def write(tx, key, to_state):
        return tx.execute("UPDATE _cdc_example.door SET state = ? WHERE door_id = ?", [to_state, *key])

    def accepts_everything(tx, machine, key, a, b, cause):            # ``key`` is the tuple the helper passes
        write(tx, key, b)
        state._record(tx, m, key, a, b, cause)

    def ignores_the_stored_state(tx, machine, key, a, b, cause):
        if not m.allows(a, b):
            raise state.IllegalTransition(m.name, key, a, b, declared=False)
        write(tx, key, b)
        state._record(tx, m, key, a, b, cause)

    def does_not_log(tx, machine, key, a, b, cause):
        if m.allows(a, b) and state.read(tx, m, key) == a:
            write(tx, key, b)
            return
        real(tx, machine, key, a, b, cause)

    def refuses_but_changes(tx, machine, key, a, b, cause):
        if m.allows(a, b) and state.read(tx, m, key) != a:
            write(tx, key, b)
        real(tx, machine, key, a, b, cause)

    def refuses_a_legal_one(tx, machine, key, a, b, cause):
        if (a, b) == ("locked", "closed"):
            raise state.IllegalTransition(m.name, key, a, b, stored=state.read(tx, m, key))
        real(tx, machine, key, a, b, cause)

    monkeypatch.setattr(state, "transition", locals()[broken])
    with pytest.raises(AssertionError):
        tm.check_machine(ex.stream, ex.door, "d1")
    monkeypatch.setattr(state, "transition", real)
    assert tm.check_machine(ex.stream, ex.door, "d1")["legal"] == 4     # the row is as it was; the real one passes


def test_the_example_machines_are_declared_only_inside_their_block(led):
    with tm.example_machines() as (door, job):
        assert state.get("example_door") is door and state.get("example_job") is job
    assert "example_door" not in [m.name for m in state.machines()]
    with pytest.raises(state.StateError):
        state.get("example_job")


# ---------------------------------------------------------------------------------------------------- document
def test_the_document_is_generated_from_the_declarations():
    state.load_all()
    with open(os.path.join(ROOT, "docs", "state_machines.md"), encoding="utf-8") as f:
        on_disk = f.read()
    assert on_disk == state.render_document(), (
        "docs/state_machines.md differs from the declarations: run "
        "`uv run python -m cdc_v2.state > docs/state_machines.md`")
    assert "### alert\n" in on_disk and "| `open` | `resolved` |" in on_disk
    assert "example_door" not in on_disk


def test_a_block_of_the_document():
    assert state.render_machine(tm.EXAMPLE_JOB) == (
        "### example_job\n\n"
        "Example for tests: a job. Two key columns, a transition from a state to itself, a final state.\n\n"
        "- Stored in: `_cdc_example.job.status`\n"
        "- Key: `batch`, `part`\n"
        "- States: `queued`, `running`, `done`, `failed`\n"
        "- A new row starts in: any state\n\n"
        "| From | To |\n|---|---|\n"
        "| `queued` | `running` |\n| `running` | `running` |\n| `running` | `done` |\n| `running` | `failed` |\n"
        "| `failed` | `queued` |\n")
    single = state.Machine(name="t_single", states=("only",), transitions=(), table="t", key_columns=(),
                           state_column="s")
    text = state.render_machine(single)
    assert "- Key: none (the table has one row)" in text and "| (no transition) | |" in text
    doc = state.render_document([tm.EXAMPLE_DOOR, single])
    assert doc.startswith(state.DOC_HEADER) and doc.index("### example_door") < doc.index("### t_single")
