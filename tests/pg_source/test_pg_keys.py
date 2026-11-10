"""What Postgres lets a key be (H-015), on the private pair (marker localpg).

A deferrable key is not a replica identity; an immediate key is checked row by row; only a table with a deferrable
primary key and REPLICA IDENTITY FULL can hold two rows with one key inside a transaction, and its decoded stream
shows that state.
"""
from __future__ import annotations

import psycopg
import pytest

from cdc_v2.deadline import Deadline
from cdc_v2.pg import catalog, publication, replica_identity

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


# ----------------------------------------------------------------------------------------------- rule K (package K)

#: name -> (columns of "create table", replica identity or None, has a key by rule K, start of the refusal or None)
SHAPES = {
    "imm": ("(id int primary key, v text)", None, True, None),
    "imm_full": ("(id int primary key, v text)", "full", True, None),
    "imm_idx": ("(id int primary key, v text)", "using index imm_idx_pkey", True, None),
    "imm_other": ("(id int primary key, v text not null unique)", "using index imm_other_v_key", False,
                  "replica identity index is not the primary key"),
    "imm_nothing": ("(id int primary key, v text)", "nothing", False, "replica identity NOTHING"),
    "def_default": ("(id int primary key deferrable, v text)", None, False,
                    "primary key is deferrable: needs REPLICA IDENTITY FULL (replica identity is default)"),
    "def_deferred_full": ("(id int primary key deferrable initially deferred, v text)", "full", False, None),
    "def_other": ("(id int primary key deferrable, v text not null unique)", "using index def_other_v_key", False,
                  "primary key is deferrable: needs REPLICA IDENTITY FULL (replica identity is index)"),
    "def_nothing": ("(id int primary key deferrable, v text)", "nothing", False,
                    "primary key is deferrable: needs REPLICA IDENTITY FULL (replica identity is nothing)"),
    "nopk_full": ("(id int, v text)", "full", False, None),
    "nopk_default": ("(id int, v text)", None, False, "no primary key"),
    "nopk_uix_full": ("(id int not null unique, v text)", "full", False, None),
    "nopk_ri_idx": ("(id int not null, v text, constraint nopk_ri_idx_u unique (id))",
                    "using index nopk_ri_idx_u", False, "no primary key"),
    "nopk_duc_full": ("(id int not null, v text, constraint nopk_duc_full_u unique (id) deferrable)", "full", False,
                      None),
}


def D(s=10):
    return Deadline.after(s)


def test_catalog_follows_rule_k(cluster, scratch):
    """catalog.tables (SourceTable) and catalog.observe (TableFacts) on real tables of every shape agree with rule K
    and with each other; publication.check refuses exactly the shapes rule K refuses."""
    s = q(scratch.schema)
    for name, (cols, ri, _k, _p) in SHAPES.items():
        scratch.admin(f"create table {s}.{name} {cols}")
        if ri:
            scratch.admin(f"alter table {s}.{name} replica identity {ri}")
    cluster.replay_to_primary()
    conn = scratch.connect("replica")
    got = {t.name: t for t in catalog.tables(conn, lambda sch, n: sch == scratch.schema, publication=scratch.pub,
                                             deadline=D())}
    obs = catalog.observe(conn, [(scratch.schema, n) for n in SHAPES], publication=scratch.pub, deadline=D())
    seen = {}
    for name, (_cols, _ri, keyed, refusal) in SHAPES.items():
        t, f = got[name], obs.by_name[(scratch.schema, name)]
        assert t.has_key is keyed and f.has_key is keyed, name
        assert t.key_attnums == f.key_attnums == ((1,) if keyed else ()), name
        assert t.key == (("id",) if keyed else ()) and t.key_source == ("primary key" if keyed else None), name
        assert t.primary_key_deferrable is f.pk_deferrable is name.startswith("def_"), name
        assert t.replica_identity_problem == f.replica_identity_problem, name
        p = t.replica_identity_problem
        assert (p is None) if refusal is None else p.startswith(refusal), (name, p)
        assert f.identity[2] == t.key_attnums
        seen[name] = {"has_key": t.has_key, "key_attnums": list(t.key_attnums), "key_source": t.key_source,
                      "has_primary_key": t.has_primary_key, "primary_key_deferrable": t.primary_key_deferrable,
                      "replica_identity": t.replica_identity, "replica_identity_index": t.replica_identity_index,
                      "problem": p}
    # an index identity on a table without a primary key: named, but not a key
    assert got["nopk_ri_idx"].replica_identity_index == "nopk_ri_idx_u" and got["nopk_ri_idx"].key == ()
    assert publication.check(got.values()) == {(scratch.schema, n): got[n].replica_identity_problem
                                               for n, v in SHAPES.items() if v[3]}
    record("keys", "catalog_follows_rule_k", seen)


def test_deferrable_key_set_full_then_publish(cluster, scratch):
    """ensure refuses a table with a deferrable primary key before set_full, with the message of rule K; set_full,
    then ensure, publishes it, and the source's own UPDATE and DELETE keep working (H-014)."""
    s = q(scratch.schema)
    scratch.admin(f"create table {s}.dk (id int primary key deferrable initially deferred, v text); "
                  f"insert into {s}.dk values (1, 'a'), (2, 'b')")
    setup = scratch.connect("primary", role="admin")
    with pytest.raises(publication.NotPublishable) as e:
        publication.ensure(setup, [(scratch.schema, "dk")], publication=scratch.pub, deadline=D())
    assert e.value.problems == {(scratch.schema, "dk"):
                                "primary key is deferrable: needs REPLICA IDENTITY FULL (replica identity is default)"}
    assert not publication.state(setup, publication=scratch.pub, deadline=D()).has(scratch.schema, "dk")
    assert replica_identity.set_full(setup, (scratch.schema, "dk"), D()) is True
    r = publication.ensure(setup, [(scratch.schema, "dk")], publication=scratch.pub, deadline=D())
    assert r.added == ((scratch.schema, "dk"),)
    scratch.admin(f"update {s}.dk set id = 3 - id where id in (1, 2)")         # the swap a deferrable key allows
    scratch.admin(f"update {s}.dk set v = 'c' where id = 1")
    scratch.admin(f"delete from {s}.dk where id = 2")
    assert scratch.admin(f"select id, v from {s}.dk order by id") == [(1, "c")]
    t = catalog.tables(setup, [(scratch.schema, "dk")], publication=scratch.pub, deadline=D())[0]
    assert t.published and not t.has_key and t.replica_identity_problem is None and publication.check([t]) == {}
    record("keys", "deferrable_key_set_full_then_publish", {
        "refused_before_set_full": list(e.value.problems.values()), "added_after_set_full": True,
        "source_swap_update_and_delete_after": "worked"})


def _key_facts(scratch, conn, name):
    t = catalog.tables(conn, [(scratch.schema, name)], publication=scratch.pub, deadline=D())[0]
    f = catalog.observe(conn, [(scratch.schema, name)], publication=scratch.pub, deadline=D()).by_name[
        (scratch.schema, name)]
    assert (t.has_key, t.key_attnums, t.primary_key_deferrable) == (f.has_key, f.key_attnums, f.pk_deferrable)
    return t, f


def _pk_index(scratch, name):
    return scratch.admin("select indexrelid::int from pg_index where indrelid = %s::regclass and indisprimary",
                         ('%s.%s' % (q(scratch.schema), name),))[0][0]


def test_how_a_primary_key_becomes_deferrable(cluster, scratch):
    """How an existing table's primary key can become deferrable or immediate, and what the catalog reports before
    and after (package T needs the answer)."""
    s = q(scratch.schema)
    scratch.admin(f"create table {s}.k (id int primary key, v text); insert into {s}.k values (1, 'a'), (2, 'b')")
    scratch.admin(f"create publication {q(scratch.pub)} for table {s}.k")
    conn = scratch.connect("primary", role="admin")
    t0, f0 = _key_facts(scratch, conn, "k")
    assert t0.has_key and not t0.primary_key_deferrable
    out = {"start": {"has_key": t0.has_key, "key_attnums": list(t0.key_attnums)}}
    # 1. ALTER CONSTRAINT … DEFERRABLE on a primary key
    out["alter_constraint_deferrable"] = list(refused(scratch, f"alter table {s}.k alter constraint k_pkey "
                                                                f"deferrable"))
    assert out["alter_constraint_deferrable"][0] == "42809"      # "… is not a foreign key constraint"
    # 2. drop and add in one statement: the table keeps its OID and relfilenode, the index is new
    idx0 = _pk_index(scratch, "k")
    scratch.admin(f"alter table {s}.k drop constraint k_pkey, add primary key (id) deferrable initially deferred")
    t1, f1 = _key_facts(scratch, conn, "k")
    idx1 = _pk_index(scratch, "k")
    assert t1.primary_key_deferrable and not t1.has_key and t1.key_attnums == () and t1.has_primary_key
    assert (t1.oid, t1.relfilenode) == (t0.oid, t0.relfilenode) and idx1 != idx0
    assert t1.shape_hash() != t0.shape_hash() and f1.identity != f0.identity
    ch = catalog.diff([t0], [t1]).changed[0]
    assert ch.key_changed and not ch.rewritten and not ch.columns_changed and ch.replica_identity_changed is None
    assert catalog.diff([f0], [f1]).changed[0].key_changed
    assert t1.replica_identity_problem.startswith("primary key is deferrable")
    # the published table under DEFAULT identity now refuses its own UPDATE (H-014)
    upd = refused(scratch, f"update {s}.k set v = 'x'")
    assert upd[0] == "55000"
    out["drop_and_add_deferrable"] = {
        "has_key": t1.has_key, "key_attnums": list(t1.key_attnums), "primary_key_deferrable": True,
        "same_table_oid_and_relfilenode": True, "new_index_oid": idx1 != idx0, "shape_hash_changed": True,
        "diff_key_changed": ch.key_changed, "diff_rewritten": ch.rewritten, "problem": t1.replica_identity_problem,
        "source_update_while_published_with_default_identity": list(upd)}
    # 3. ALTER CONSTRAINT … NOT DEFERRABLE on the deferrable key
    out["alter_constraint_not_deferrable"] = list(refused(scratch, f"alter table {s}.k alter constraint k_pkey "
                                                                    f"not deferrable"))
    assert out["alter_constraint_not_deferrable"][0] == "42809"
    # 4. back to immediate through a unique index built beforehand: drop and add … USING INDEX
    scratch.admin(f"create unique index k_id on {s}.k (id)")
    scratch.admin(f"alter table {s}.k drop constraint k_pkey, add constraint k_pkey primary key using index k_id")
    t2, _f2 = _key_facts(scratch, conn, "k")
    assert t2.has_key and not t2.primary_key_deferrable and t2.key_attnums == (1,)
    assert t2.shape_hash() == t0.shape_hash()            # the same shape as at the start
    scratch.admin(f"update {s}.k set v = 'y' where id = 1")
    out["back_to_immediate_using_index"] = {"has_key": t2.has_key, "shape_hash_equals_the_start": True,
                                            "source_update": "worked"}
    # 5. deferrable again through … USING INDEX … DEFERRABLE (Postgres marks the index not immediate)
    scratch.admin(f"create unique index k_id2 on {s}.k (id)")
    scratch.admin(f"alter table {s}.k drop constraint k_pkey, "
                  f"add constraint k_pkey primary key using index k_id2 deferrable")
    t3, _f3 = _key_facts(scratch, conn, "k")
    assert t3.primary_key_deferrable and not t3.has_key
    out["using_index_deferrable"] = {"has_key": t3.has_key, "primary_key_deferrable": t3.primary_key_deferrable}
    record("keys", "how_a_primary_key_becomes_deferrable", out)


def _identity(scratch, conn, name):
    return catalog.observe(conn, [(scratch.schema, name)], publication=scratch.pub, deadline=D()).by_name[
        (scratch.schema, name)].identity


def test_identity_sees_every_change_of_the_key_facts(cluster, scratch):
    """TableFacts.identity (package T's gate) changes with each step: FULL to DEFAULT on a table without a key,
    DEFAULT to USING INDEX, the index of USING INDEX replaced, the key made deferrable."""
    s = q(scratch.schema)
    scratch.admin(f"create table {s}.nk (a int not null, b int not null, v text); "
                  f"alter table {s}.nk replica identity full; "
                  f"create unique index nk_a on {s}.nk (a); create unique index nk_b on {s}.nk (b); "
                  f"create table {s}.pk (id int primary key, w int not null unique, v text)")
    conn = scratch.connect("primary", role="admin")
    steps = {}
    prev = _identity(scratch, conn, "nk")
    for what, sql in (("FULL to DEFAULT", f"alter table {s}.nk replica identity default"),
                      ("DEFAULT to USING INDEX", f"alter table {s}.nk replica identity using index nk_a"),
                      ("the index of USING INDEX replaced", f"alter table {s}.nk replica identity using index nk_b")):
        scratch.admin(sql)
        cur = _identity(scratch, conn, "nk")
        assert cur != prev and cur[2] == prev[2] == (), (what, prev, cur)       # key_attnums stay ()
        steps[what] = {"before": list(prev[3:5]), "after": list(cur[3:5])}
        prev = cur
    prev = _identity(scratch, conn, "pk")
    scratch.admin(f"alter table {s}.pk drop constraint pk_pkey, add primary key (id) deferrable")
    cur = _identity(scratch, conn, "pk")
    assert cur != prev and cur[5] is True and prev[5] is False
    steps["the key made deferrable"] = {"key_attnums": [list(prev[2]), list(cur[2])], "pk_deferrable": [False, True]}
    record("keys", "identity_sees_every_change_of_the_key_facts", steps)


def test_name_rule_refuses_and_reports(cluster, scratch):
    """A double quote in a table name: publication.ensure refuses the table (NotPublishable, the message holds no
    name); publication.check reports such a table that was published by other means."""
    s = q(scratch.schema)
    bad = 'x "q"'
    scratch.admin(f"create table {s}.{q(bad)} (id int primary key); create table {s}.ok (id int primary key)")
    setup = scratch.connect("primary", role="admin")
    with pytest.raises(publication.NotPublishable) as e:
        publication.ensure(setup, [(scratch.schema, bad), (scratch.schema, "ok")], publication=scratch.pub,
                           deadline=D())
    assert set(e.value.problems) == {(scratch.schema, bad)}
    msg = e.value.problems[(scratch.schema, bad)]
    assert msg.startswith("table name contains a double quote") and bad not in msg
    assert not publication.state(setup, publication=scratch.pub, deadline=D()).exists or not publication.state(
        setup, publication=scratch.pub, deadline=D()).has(scratch.schema, bad)
    scratch.admin(f"alter publication {q(scratch.pub)} add table {s}.{q(bad)}")      # by other means
    got = catalog.tables(setup, [(scratch.schema, bad)], publication=scratch.pub, deadline=D())
    assert got[0].published and publication.check(got) == {(scratch.schema, bad): msg}
    record("keys", "name_rule_refuses_and_reports", {"refusal": msg, "published_by_other_means_reported": True})
