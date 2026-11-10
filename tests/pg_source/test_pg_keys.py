"""What Postgres lets a key be (H-015), on the private pair (marker localpg).

A deferrable key is not a replica identity; an immediate key is checked row by row; only a table with a deferrable
primary key and REPLICA IDENTITY FULL can hold two rows with one key inside a transaction, and its decoded stream
shows that state.
"""
from __future__ import annotations

import psycopg
import pytest

from .conftest import pgoutput_changes, record

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(300)]


def q(name):
    return '"' + name.replace('"', '""') + '"'


def refused(scratch, sql):
    """The server's answer to a statement that must fail: (SQLSTATE, first line of the message)."""
    with pytest.raises(psycopg.Error) as e:
        scratch.admin(sql)
    return e.value.sqlstate, e.value.diag.message_primary.replace(scratch.schema, "<schema>")


def max_holders(msgs, relid, start):
    """Largest number of rows that hold one key (column 0) after any record of the decoded stream."""
    held = dict(start)
    worst = 0
    for m in msgs:
        if m["kind"] not in "IUD" or m["relid"] != relid:
            continue
        for tag, vals in m["tuple"]:
            held[vals[0]] = held.get(vals[0], 0) + (1 if tag == "N" else -1)
        assert min(held.values()) >= 0, held
        worst = max(worst, max(held.values()))
    return worst


def test_what_postgres_lets_a_key_be(cluster, scratch):
    s = q(scratch.schema)
    scratch.admin(f"""
        create table {s}.imm (id int primary key, v text);
        create table {s}.imm_full (id int primary key, v text);
        alter table {s}.imm_full replica identity full;
        create table {s}.def_imm (id int primary key deferrable initially immediate, v text);
        create table {s}.def_def (id int primary key deferrable initially deferred, v text);
        create table {s}.def_uni (id int not null, v text, constraint def_uni_id unique (id) deferrable);
        create table {s}.def_full (id int primary key deferrable initially deferred, v text);
        alter table {s}.def_full replica identity full;
    """)
    facts = {r[0]: {"replident": r[1], "immediate": r[2], "identity_index": r[3]} for r in scratch.admin(
        "select c.relname, c.relreplident::text, i.indimmediate, pg_get_replica_identity_index(c.oid) is not null "
        "from pg_class c join pg_index i on i.indrelid = c.oid and (i.indisprimary or i.indisunique) "
        "where c.relnamespace = %s::regnamespace order by 1", (scratch.schema,))}
    # a deferrable primary key is no replica identity under DEFAULT; an immediate one is
    assert facts["imm"] == {"replident": "d", "immediate": True, "identity_index": True}
    assert facts["def_imm"] == {"replident": "d", "immediate": False, "identity_index": False}
    assert facts["def_def"] == {"replident": "d", "immediate": False, "identity_index": False}
    assert facts["def_full"] == {"replident": "f", "immediate": False, "identity_index": False}
    # and a deferrable unique index cannot be named as the identity
    using_index = refused(scratch, f"alter table {s}.def_uni replica identity using index def_uni_id")
    assert using_index == ("0A000", 'cannot use non-immediate index "def_uni_id" as replica identity')

    scratch.admin(f"create publication {q(scratch.pub)} for table {s}.imm, {s}.imm_full, {s}.def_imm, {s}.def_def, "
                  f"{s}.def_full")
    for t in ("imm", "imm_full", "def_imm", "def_def", "def_full"):
        scratch.admin(f"insert into {s}.{t} values (1, 'a'), (2, 'b')")
    relid = {r[0]: r[1] for r in scratch.admin(
        "select relname, oid::int from pg_class where relnamespace = %s::regnamespace", (scratch.schema,))}
    cluster.q("primary", "select pg_create_logical_replication_slot(%s, 'pgoutput')", (scratch.slot,), role="runtime")

    # published with a deferrable key and DEFAULT identity: the source refuses its own UPDATE and DELETE
    no_identity = [refused(scratch, f"update {s}.def_imm set v = 'x' where id = 1"),
                   refused(scratch, f"delete from {s}.def_def where id = 1")]
    assert [st for st, _m in no_identity] == ["55000", "55000"]
    assert "does not have a replica identity" in no_identity[0][1]

    # an immediate key is checked row by row, also under FULL: no statement passes through a state with two holders
    row_by_row = {}
    for t in ("imm", "imm_full"):
        row_by_row[t] = [refused(scratch, f"update {s}.{t} set id = 3 - id where id in (1, 2)"),
                         refused(scratch, f"update {s}.{t} set id = id + 1")]
        assert [st for st, _m in row_by_row[t]] == ["23505", "23505"]
        # the same swap through a free key and a chain inside one transaction
        scratch.admin(f"begin; update {s}.{t} set id = -1 where id = 1; update {s}.{t} set id = 1 where id = 2; "
                      f"update {s}.{t} set id = 2 where id = -1; update {s}.{t} set id = 5 where id = 2; "
                      f"update {s}.{t} set id = 6 where id = 5; delete from {s}.{t} where id = 1; "
                      f"insert into {s}.{t} values (1, 'again'); commit")
        assert scratch.admin(f"select id, v from {s}.{t} order by id") == [(1, "again"), (6, "a")]

    # a deferrable primary key with FULL: a direct swap, and a duplicate that lives until the old row is deleted
    scratch.admin(f"update {s}.def_full set id = 3 - id where id in (1, 2)")
    scratch.admin(f"begin; insert into {s}.def_full values (2, 'dup'); delete from {s}.def_full where v = 'a'; "
                  f"commit")
    assert scratch.admin(f"select id, v from {s}.def_full order by id") == [(1, "b"), (2, "dup")]

    msgs = pgoutput_changes(cluster, "primary", scratch.slot, scratch.pub)
    start = {"1": 1, "2": 1}
    holders = {t: max_holders(msgs, relid[t], start) for t in ("imm", "imm_full", "def_full")}
    assert holders == {"imm": 1, "imm_full": 1, "def_full": 2}
    swap = [[(tag, vals) for tag, vals in m["tuple"]] for m in msgs if m["kind"] in "IUD"
            and m["relid"] == relid["def_full"]]
    assert swap[:2] == [[("O", ["1", "a"]), ("N", ["2", "a"])], [("O", ["2", "b"]), ("N", ["1", "b"])]]
    record("keys", "what_postgres_lets_a_key_be", {
        "catalog": facts, "identity_using_a_deferrable_index": list(using_index),
        "published_deferrable_key_default_identity": [list(x) for x in no_identity],
        "immediate_key_swap_and_shift_in_one_statement": {t: [list(x) for x in v] for t, v in row_by_row.items()},
        "largest_number_of_holders_of_one_key_in_the_decoded_stream": holders,
        "deferrable_key_full_identity_direct_swap_decoded": swap[:2]})
