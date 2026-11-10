"""Package P, tier T0 (default run): no network, no Postgres. SQL builders, quoting, settings validation, error
mapping, catalog.diff, timeline parsing and timeline.contains, deadlines with fault sites."""
from __future__ import annotations

import json
import time

import psycopg
import pytest

from cdc_v2 import faultsites
from cdc_v2 import lsn as lsnmod
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.pg import catalog, errors, publication, slot, sql, timeline
from cdc_v2.pg.conn import connect
from cdc_v2.pg.settings import FIXED_SESSION_SETTINGS, NodeSettings, Password, SourceSettings


def node(**kw):
    d = dict(host="db.invalid", port=6543, dbname="d", user="u", password=Password(fn=lambda: "pw-secret-1234"))
    d.update(kw)
    return NodeSettings(**d)


# ----------------------------------------------------------------------------------------------- settings

def test_password_is_never_shown():
    p = Password(fn=lambda: "pw-secret-1234")
    n = node(password=p)
    for text in (repr(p), str(p), repr(n), str(n), json.dumps(n.conninfo())):
        assert "pw-secret-1234" not in text
    assert p.reveal() == "pw-secret-1234"
    assert "db.invalid" not in repr(n) and "db.invalid" not in repr(SourceSettings(nodes={"primary": n}))
    named = node(dbname="orders_db_x", user="role_x")           # neither the database nor the role is shown
    for text in (repr(named), str(named), repr(SourceSettings(nodes={"primary": named}))):
        assert "orders_db_x" not in text and "role_x" not in text
    with pytest.raises(TypeError):
        import pickle
        pickle.dumps(p)
    with pytest.raises(AttributeError):
        p._fn = None
    with pytest.raises(ValueError):
        Password()


def test_password_from_environment(monkeypatch):
    monkeypatch.setenv("CDC_V2_TEST_PW", "from-env-xyz")
    p = Password(env="CDC_V2_TEST_PW")
    assert p.reveal() == "from-env-xyz" and "from-env" not in repr(p)
    monkeypatch.delenv("CDC_V2_TEST_PW")
    with pytest.raises(Exception, match="not available"):
        p.reveal()


@pytest.mark.parametrize("kw", [dict(port=None), dict(port=0), dict(port=70000), dict(port=True), dict(host=""),
                                dict(tls="prefer"), dict(tls="disable"), dict(statement_timeout_ms=-1),
                                dict(application_name="bad name"), dict(cancel_grace_s=0)])
def test_settings_validation_refuses(kw):
    with pytest.raises(ValueError):
        node(**kw)


def test_tls_disable_only_local_and_explicit():
    assert node(host="localhost", tls="disable").conninfo()["sslmode"] == "disable"
    assert node(host="127.0.0.1", tls="disable").conninfo()["sslmode"] == "disable"
    assert node(host="/tmp/sock", tls="disable").conninfo()["sslmode"] == "disable"
    with pytest.raises(ValueError):
        node(host="10.0.0.1", tls="disable")
    ci = node().conninfo()
    assert ci["sslmode"] == "verify-full" and ci["sslrootcert"] == "system"
    assert node(sslrootcert="/x/ca.crt").conninfo()["sslrootcert"] == "/x/ca.crt"


def test_conninfo_carries_session_settings_and_keepalives():
    ci = node(statement_timeout_ms=1234, lock_timeout_ms=55, idle_in_transaction_session_timeout_ms=777).conninfo(
        replication=True)
    assert "password" not in ci and ci["replication"] == "database" and ci["port"] == 6543
    opts = ci["options"]
    for name in FIXED_SESSION_SETTINGS:
        assert "-c %s=" % name in opts
    assert "-c DateStyle=ISO,\\ YMD" in opts and "-c search_path=pg_catalog,\\ pg_temp" in opts
    assert "-c statement_timeout=1234" in opts and "-c lock_timeout=55" in opts
    assert "-c idle_in_transaction_session_timeout=777" in opts
    assert ci["keepalives"] == "1" and ci["tcp_user_timeout"] == "120000"
    exp = node(statement_timeout_ms=1234).expected_settings()
    assert exp["statement_timeout"] == "1234" and exp["TimeZone"] == "UTC" and exp["IntervalStyle"] == "postgres"


def test_source_settings():
    s = SourceSettings(nodes={"primary": node(), "replica": node(port=6544)})
    assert s.node("replica").port == 6544
    with pytest.raises(KeyError):
        s.node("nope")
    with pytest.raises(ValueError):
        SourceSettings(nodes={"replica": node()})
    with pytest.raises(ValueError):
        SourceSettings(nodes={"primary": node()}, slot="other_slot")


# ----------------------------------------------------------------------------------------------- quoting

@pytest.mark.parametrize("name,quoted", [("t", '"t"'), ('my "t"', '"my ""t"""'), ("a.b", '"a.b"'),
                                         ("Mixed Case", '"Mixed Case"'), ("ä ö", '"ä ö"'), ("x" * 63, '"' + "x" * 63 + '"')])
def test_ident(name, quoted):
    assert sql.ident(name) == quoted


@pytest.mark.parametrize("bad", ["", "a\x00b", "x" * 64, "ä" * 32, None])
def test_ident_refuses(bad):
    with pytest.raises(ValueError):
        sql.ident(bad)


def test_qualified_literal_slot_name():
    assert sql.qualified("s", 'n"') == '"s"."n"""'
    assert sql.literal("it's") == "'it''s'"
    assert sql.check_slot_name("cdc_v2_slot") == "cdc_v2_slot"
    for bad in ("Cdc", "a-b", "", "x" * 64, "a b"):
        with pytest.raises(ValueError):
            sql.check_slot_name(bad)


# ----------------------------------------------------------------------------------------------- errors

class FakeDiag:
    def __init__(self, primary):
        self.message_primary = primary


class E(Exception):
    def __init__(self, sqlstate, primary="boom"):
        super().__init__(primary + " at host db.invalid user u")
        self.sqlstate = sqlstate
        self.diag = FakeDiag(primary + " db.invalid")


@pytest.mark.parametrize("state,primary,rec,cls", [
    ("08006", "connection failure", None, errors.ConnectionLost),
    ("08001", "x", None, errors.ConnectionLost),
    ("57P01", "terminating connection due to administrator command", None, errors.ConnectionLost),
    ("57P03", "the database system is starting up", None, errors.ConnectionLost),
    ("25P03", "idle-in-transaction timeout", None, errors.ConnectionLost),
    ("40001", "canceling statement due to conflict with recovery", None, errors.RecoveryConflict),
    ("40001", "could not serialize access", True, errors.RecoveryConflict),
    ("40001", "could not serialize access", False, errors.SerializationFailure),
    ("40P01", "deadlock detected", False, errors.SerializationFailure),
    ("57P04", "database dropped", True, errors.RecoveryConflict),
    ("55P03", "canceling statement due to lock timeout", None, errors.LockTimeout),
    ("42501", "permission denied for table t", None, errors.PermissionDenied),
    ("28P01", "password authentication failed", None, errors.PermissionDenied),
    ("42P01", "relation does not exist", None, errors.SourceError),
])
def test_error_classification(state, primary, rec, cls):
    m = errors.classify(E(state, primary), in_recovery=rec, what="w", secrets=("db.invalid", "u"))
    assert type(m) is cls and m.sqlstate == state
    assert "db.invalid" not in str(m)
    assert isinstance(m, errors.ReadAgain) == (cls in (errors.RecoveryConflict, errors.SerializationFailure))


def test_error_classification_deadline_and_cancel():
    assert isinstance(errors.classify(E("57014", "canceling statement due to statement timeout")), DeadlineExceeded)
    other = errors.classify(E("57014", "canceling statement due to user request"))
    assert type(other) is errors.SourceError
    assert isinstance(errors.classify(E("42P01"), deadline_fired=True), DeadlineExceeded)
    assert isinstance(errors.classify(psycopg.OperationalError("server closed the connection")),
                      errors.ConnectionLost)
    assert isinstance(errors.classify(ConnectionResetError()), errors.ConnectionLost)
    assert errors.scrub("pw is hunter22 at hx", ("hunter22", "hx")) == "pw is <redacted> at <redacted>"
    missing = errors.classify(E("3D000", 'database "orders_db_x" does not exist'), what="connect",
                              secrets=("db.invalid", "role_x", "orders_db_x"))
    assert "orders_db_x" not in str(missing) and missing.sqlstate == "3D000"


# ----------------------------------------------------------------------------------------------- connect: deadlines

@pytest.fixture
def faults(tmp_path, monkeypatch):
    path = tmp_path / "faults.json"
    monkeypatch.setenv("CDC_V2_FAULT_FILE", str(path))
    faultsites.reconfigure()

    def write(rules):
        tmp = tmp_path / "f.tmp"
        tmp.write_text(json.dumps({"rules": rules}))
        tmp.replace(path)
    yield write
    monkeypatch.delenv("CDC_V2_FAULT_FILE")
    faultsites.reconfigure()


def test_connect_fault_site_raise_maps_to_connection_lost(faults):
    faults([{"id": "r1", "site": "pg.connect", "action": "raise", "exception": "ConnectionRefusedError"}])
    with pytest.raises(errors.ConnectionLost):
        connect(node(), Deadline.after(5))


def test_connect_is_bounded_by_its_deadline_even_when_the_call_hangs(faults):
    faults([{"id": "r1", "site": "pg.connect", "action": "block", "seconds": 5}])
    n = node(cancel_grace_s=0.2)
    t = time.monotonic()
    with pytest.raises(DeadlineExceeded):
        connect(n, Deadline.after(0.5))
    assert time.monotonic() - t < 0.5 + 0.2 + 0.3


def test_connect_requires_a_deadline():
    with pytest.raises(TypeError):
        connect(node(), 5)


# ----------------------------------------------------------------------------------------------- catalog.diff

def tf(oid, name, cols, *, schema="s", filenode=None, replident="default", pk=(1,), published=True,
       persistence="permanent"):
    return catalog.TableFacts(
        oid=oid, schema=schema, name=name, relfilenode=filenode or oid * 10, replica_identity=replident,
        relkind="ordinary", persistence=persistence, is_partition=False,
        all_columns=tuple(catalog.ColumnFacts(attnum=a, name=n, type_oid=t, typmod=-1, dropped=d, generated="",
                                              has_default=False) for a, n, t, d in cols),
        pk_attnums=tuple(pk), replica_identity_index_attnums=None, replica_identity_index_is_key=False,
        published=published, publication_has_column_list=False, publication_has_row_filter=False)


BASE = [(1, "id", 20, False), (2, "v", 25, False)]


def test_diff_nothing():
    a = [tf(1, "t", BASE)]
    assert catalog.diff(a, [tf(1, "t", BASE)]).empty


def test_diff_appeared_disappeared_recreated_renamed():
    old = [tf(1, "t", BASE), tf(2, "gone", BASE), tf(3, "r", BASE)]
    new = [tf(4, "t", BASE), tf(5, "new", BASE), tf(3, "r2", BASE)]
    d = catalog.diff(old, new)
    assert [(o.oid, n.oid) for o, n in d.recreated] == [(1, 4)]
    assert [t.oid for t in d.appeared] == [5]
    assert [t.oid for t in d.disappeared] == [2]
    assert [(o.name, n.name) for o, n in d.renamed] == [("r", "r2")]


def test_diff_rename_away_and_new_table_under_old_name_is_not_a_recreate():
    d = catalog.diff([tf(1, "t", BASE)], [tf(1, "u", BASE), tf(2, "t", BASE)])
    assert not d.recreated and [t.oid for t in d.appeared] == [2]
    assert [(o.name, n.name) for o, n in d.renamed] == [("t", "u")]


def test_diff_swap_of_two_names():
    d = catalog.diff([tf(1, "a", BASE), tf(2, "b", BASE)], [tf(1, "b", BASE), tf(2, "a", BASE)])
    assert len(d.renamed) == 2 and not d.recreated and not d.appeared and not d.disappeared


def test_diff_columns_rename_vs_drop_add_by_attnum():
    renamed = catalog.diff([tf(1, "t", BASE)], [tf(1, "t", [(1, "id", 20, False), (2, "w", 25, False)])])
    ch = renamed.changed[0]
    assert ch.columns_renamed == ((2, "v", "w"),) and not ch.columns_added and not ch.columns_dropped
    dropadd = catalog.diff([tf(1, "t", BASE)], [tf(1, "t", [(1, "id", 20, False), (2, "v", 25, True),
                                                            (3, "w", 25, False)])])
    ch = dropadd.changed[0]
    assert ch.columns_dropped == ((2, "v"),) and ch.columns_added == ((3, "w"),) and not ch.columns_renamed
    same_name = catalog.diff([tf(1, "t", BASE)], [tf(1, "t", [(1, "id", 20, False), (2, "v", 25, True),
                                                              (3, "v", 25, False)])])
    ch = same_name.changed[0]
    assert ch.columns_dropped == ((2, "v"),) and ch.columns_added == ((3, "v"),)


def test_diff_retype_rewrite_key_identity_persistence_publication():
    old = tf(1, "t", BASE)
    new = tf(1, "t", [(1, "id", 20, False), (2, "v", 1043, False)], filenode=99, replident="full", pk=(),
             published=False, persistence="unlogged")
    ch = catalog.diff([old], [new]).changed[0]
    assert ch.columns_retyped == ((2, "v", (25, -1), (1043, -1)),)
    assert ch.rewritten and ch.key_changed and ch.replica_identity_changed == ("default", "full")
    assert ch.persistence_changed == ("permanent", "unlogged") and ch.publication_changed == (True, False)
    assert ch.columns_changed
    only_rewrite = catalog.diff([old], [tf(1, "t", BASE, filenode=77)]).changed[0]
    assert only_rewrite.rewritten and not only_rewrite.columns_changed


def test_table_facts_identity_and_replica_identity_rule():
    t = tf(1, "t", [(1, "id", 20, False), (2, "x", 23, True), (3, "v", 25, False)])
    assert t.identity == (1, ((1, "id", 20, -1), (3, "v", 25, -1)), (1,), "default", None, False)
    assert t.replica_identity_problem is None
    assert "FULL" in tf(1, "t", BASE, pk=()).replica_identity_problem
    assert tf(1, "t", BASE, pk=(), replident="full").replica_identity_problem is None
    assert "FULL" in tf(1, "t", BASE, pk=(), replident="index").replica_identity_problem
    assert "NOTHING" in tf(1, "t", BASE, replident="nothing").replica_identity_problem
    assert catalog.identity_problem(True, "index", True) is None
    assert catalog.identity_problem(True, "index", False) is not None


def test_publication_state_problems_and_check():
    ok = publication.PublicationState(name="p", exists=True, oid=1, publish=publication.PUBLISH)
    assert ok.problems == []
    bad = publication.PublicationState(name="p", exists=True, oid=1, publish=("insert",), via_root=True, tables=(
        publication.PublicationTable(1, "s", "t", ("a",), "(a > 1)"),))
    assert len(bad.problems) == 4
    assert publication.PublicationState(name="p", exists=False).problems == ["missing"]


# ----------------------------------------------------------------------------------------------- timeline

HIST3 = "1\t0/3000158\tno recovery target specified\n\n2\t0/5000000\tbefore 2000-01-01\n"


def test_parse_history():
    h = timeline.parse_history(HIST3, 3)
    assert h.timeline == 3 and [(e.timeline, e.switch) for e in h.entries] == [
        (1, lsnmod.parse("0/3000158")), (2, lsnmod.parse("0/5000000"))]
    assert timeline.parse_history("", 1).entries == ()
    for bad in ("x\t0/1\n", "2\t0/5\n1\t0/9\n", "1\tnot-an-lsn\n", "3\t0/1\n"):
        with pytest.raises(ValueError):
            timeline.parse_history(bad, 3)


def test_timeline_contains():
    h = timeline.parse_history(HIST3, 3)
    sw1, sw2 = lsnmod.parse("0/3000158"), lsnmod.parse("0/5000000")
    assert timeline.contains(h, 10**12, 3)                 # anything on the current timeline
    assert timeline.contains(h, sw1, 1) and timeline.contains(h, sw1 - 1, 1)
    assert not timeline.contains(h, sw1 + 1, 1)            # written on timeline 1 after the fork: diverged
    assert timeline.contains(h, sw2, 2) and not timeline.contains(h, sw2 + 8, 2)
    assert not timeline.contains(h, 1, 4)                  # a timeline the history does not know
    assert not timeline.contains(timeline.parse_history("", 1), 5, 2)
    assert timeline.contains(timeline.parse_history("", 1), 5, 1)
    with pytest.raises(ValueError):
        timeline.contains(h, -1, 3)


def test_parse_full_xid():
    from cdc_v2.pg.preflight import parse_full_xid
    assert parse_full_xid("0:745") == 745 and parse_full_xid("2:5") == (2 << 32) | 5
    assert parse_full_xid("123") == 123 and parse_full_xid(None) is None


# ----------------------------------------------------------------------------------------------- slot values

def test_slot_column_lists_per_version():
    c16, c17, c18 = (slot._slot_columns(v) for v in (16, 17, 18))
    assert "NULL::text" in c16 and "s.invalidation_reason" not in c16 and "s.failover" not in c16
    assert "s.invalidation_reason" in c17 and "s.inactive_since" in c17 and "s.two_phase_at" not in c17
    assert "s.two_phase_at" in c18
    for c in (c16, c17, c18):
        assert c.count(",") == c18.count(",")             # the same number of columns on every version


def test_slot_facts_holder_and_invalid():
    f = slot.SlotFacts(name="s", plugin="pgoutput", database="d", slot_type="logical", temporary=False, active=True,
                       active_pid=7, restart_lsn=1, confirmed_flush_lsn=2, wal_status="reserved", safe_wal_size=None,
                       conflicting=False, invalidation_reason=None, inactive_since=None, failover=None, synced=None,
                       two_phase=False, two_phase_at=None, catalog_xmin=5, xmin=None, holder_backend_start=None,
                       holder_backend_start_us=1700000000123456, holder_application_name="x",
                       holder_backend_type="walsender", stats_reset=None, total_txns=0, total_bytes=0,
                       spill_bytes=0)
    assert f.holder == (7, 1700000000123456) and not f.invalid
    from dataclasses import replace
    assert replace(f, wal_status="lost").invalid and replace(f, invalidation_reason="rows_removed").invalid
    assert replace(f, conflicting=True).invalid and replace(f, active_pid=None).holder is None


# ----------------------------------------------------------------------------------------------- rule K (package K)

PKS = ("none", "immediate", "deferrable")
IDENTITIES = ("default", "index on the key", "other index", "full", "nothing")

#: (primary key, replica identity) -> (has a key by rule K, the start of the refusal or None)
RULE_K = {
    ("none", "default"): (False, "no primary key"), ("none", "index on the key"): (False, "no primary key"),
    ("none", "other index"): (False, "no primary key"), ("none", "full"): (False, None),
    ("none", "nothing"): (False, "no primary key"),
    ("immediate", "default"): (True, None), ("immediate", "index on the key"): (True, None),
    ("immediate", "other index"): (False, "replica identity index is not the primary key"),
    ("immediate", "full"): (True, None), ("immediate", "nothing"): (False, "replica identity NOTHING"),
    # a deferrable index cannot be the identity (Postgres refuses it, test_pg_keys); the rule is total anyway
    ("deferrable", "default"): (False, "primary key is deferrable"),
    ("deferrable", "index on the key"): (False, "primary key is deferrable"),
    ("deferrable", "other index"): (False, "primary key is deferrable"),
    ("deferrable", "full"): (False, None), ("deferrable", "nothing"): (False, "primary key is deferrable"),
}


def _replident(identity):
    return {"index on the key": "index", "other index": "index"}.get(identity, identity)


def source_table(pk, identity):
    """A SourceTable as catalog.tables builds it for one (primary key, replica identity) combination."""
    replident = _replident(identity)
    index_is_pk = identity == "index on the key" and pk != "none"
    keyed = catalog.has_key(pk != "none", pk == "deferrable", replident, index_is_pk)

    def col(n, a):
        return catalog.Column(attnum=a, name=n, type_oid=23, typmod=-1, type_name="integer",
                              type=catalog.TypeInfo(oid=23, schema="pg_catalog", name="int4", kind="base", typlen=4),
                              ndims=0, not_null=True, has_default=False, generated="", identity="", storage="plain",
                              toastable=False)

    ka = (1,) if keyed else ()
    return catalog.SourceTable(
        oid=1, schema="s", name="t", relfilenode=10, relkind="ordinary", is_partition=False, partition_root=None,
        persistence="permanent", rls_enabled=False, rls_forced=False, rls_applies_to_reader=False,
        columns=(col("id", 1), col("u", 2)), max_attnum=2, key=("id",) if ka else (), key_attnums=ka,
        key_source="primary key" if ka else None, has_primary_key=pk != "none", replica_identity=replident,
        replica_identity_index={"index on the key": "t_pkey", "other index": "t_u"}.get(identity),
        replica_identity_index_is_key=index_is_pk, published=False, publication_columns=None,
        publication_row_filter=None, primary_key_deferrable=pk == "deferrable")


def facts(pk, identity):
    from dataclasses import replace
    return replace(tf(1, "t", BASE, pk=() if pk == "none" else (1,), replident=_replident(identity)),
                   replica_identity_index_is_key=identity == "index on the key" and pk != "none",
                   replica_identity_index_attnums={"index on the key": (1,), "other index": (2,)}.get(identity),
                   pk_deferrable=pk == "deferrable")


@pytest.mark.parametrize("pk", PKS)
@pytest.mark.parametrize("identity", IDENTITIES)
def test_rule_k_and_identity_problem_over_every_combination(pk, identity):
    keyed, refusal = RULE_K[(pk, identity)]
    replident = _replident(identity)
    index_is_pk = identity == "index on the key" and pk != "none"
    assert catalog.has_key(pk != "none", pk == "deferrable", replident, index_is_pk) is keyed
    problem = catalog.identity_problem(pk != "none", replident, index_is_pk, pk_deferrable=pk == "deferrable")
    t, f = source_table(pk, identity), facts(pk, identity)
    for value in (problem, t.replica_identity_problem, f.replica_identity_problem):
        assert (value is None) if refusal is None else value.startswith(refusal), value
    if pk == "deferrable" and refusal:
        assert problem == "primary key is deferrable: needs REPLICA IDENTITY FULL (replica identity is %s)" % replident
    assert t.has_key is keyed and f.has_key is keyed
    assert f.key_attnums == ((1,) if keyed else ())             # TableFacts follows the rule
    assert f.identity[2] == f.key_attnums
    if not keyed:                    # a table without a key: no key columns and no key source
        assert t.key == () and t.key_attnums == () and t.key_source is None
    assert publication.check([t]) == ({} if refusal is None else {("s", "t"): problem})


def test_identity_sees_replica_identity_index_and_deferrability():
    """TableFacts.identity (package T's gate) changes with the replica identity, the identity index's columns and
    the deferrability of the primary key, also for a table without a key whose key_attnums stay ()."""
    from dataclasses import replace
    full = facts("none", "full")
    assert full.identity != replace(full, replica_identity="default").identity            # FULL -> DEFAULT
    imm = facts("immediate", "default")
    idx = facts("immediate", "index on the key")
    assert imm.identity != idx.identity                                                    # DEFAULT -> USING INDEX
    other = facts("immediate", "other index")
    assert other.identity != replace(other, replica_identity_index_attnums=(1, 2)).identity  # index replaced
    dfull = facts("deferrable", "full")
    assert dfull.key_attnums == full.key_attnums == ()
    assert dfull.identity != replace(dfull, pk_deferrable=False, pk_attnums=()).identity
    assert facts("immediate", "full").identity != facts("deferrable", "full").identity     # made deferrable
    assert full.identity == replace(full, relfilenode=999).identity                        # not relfilenode


# ----------------------------------------------------------------------------------------------- the name rule

QUOTED_NAMES = ('"b', 'x "q"', 'Odd.Name, "q"', '"q"', 'a"b', 'a."b', 'a,"b', 'a "b', 'a"')
PLAIN_NAMES = ("a.b", "a,b", "Mixed Case", "Odd.Name, q", "t", "schéma")


@pytest.mark.parametrize("where", ["schema", "table"])
@pytest.mark.parametrize("name", QUOTED_NAMES + PLAIN_NAMES)
def test_name_problem(where, name):
    """The name rule: a double quote anywhere in the schema or table name means the table is not streamed (stock
    Debezium 3.7.1 stops or drops such names, engine.md 4a). The message never holds the name."""
    from dataclasses import replace
    t = source_table("immediate", "default")
    t = replace(t, schema=name) if where == "schema" else replace(t, name=name)
    p = t.name_problem
    if '"' in name:
        assert p is not None and name not in p and '"' not in p, p
        assert publication.check([t]) == {t.qualified: p}
    else:
        assert p is None and publication.check([t]) == {}


def test_build_tables_reads_indimmediate():
    """The row handling of catalog.tables: pg_index.indimmediate of the primary key decides (rule K)."""
    def table(oid, replident):
        return (oid, "s", "t%d" % oid, oid * 10, "r", False, "p", False, False, False, replident, None, 2)
    cols = [(oid, a, n, 23, -1, "integer", 0, True, False, "", "", "p") for oid in (1, 2, 3, 4)
            for a, n in ((1, "id"), (2, "u"))]
    keys = [(1, 11, True, False, "t1_pkey", [1], True),        # immediate primary key, DEFAULT
            (2, 21, True, False, "t2_pkey", [1], False),       # deferrable primary key, FULL
            (3, 31, True, False, "t3_pkey", [1], False),       # deferrable primary key, DEFAULT
            (4, 41, False, True, "t4_u", [2], True)]           # no primary key, USING INDEX
    got = {t.oid: t for t in catalog._build_tables([table(1, "d"), table(2, "f"), table(3, "d"), table(4, "i")],
                                                   cols, keys, [], {}, {})}
    assert (got[1].has_key, got[1].key_attnums, got[1].key_source, got[1].primary_key_deferrable) == (
        True, (1,), "primary key", False)
    for oid in (2, 3, 4):
        assert (got[oid].has_key, got[oid].key, got[oid].key_attnums, got[oid].key_source) == (False, (), (), None)
    assert got[2].primary_key_deferrable and got[2].replica_identity_problem is None
    assert got[3].replica_identity_problem.startswith("primary key is deferrable")
    assert got[4].replica_identity_index == "t4_u" and got[4].replica_identity_problem.startswith("no primary key")


def test_shape_hash_and_diff_see_a_key_that_becomes_deferrable():
    from dataclasses import replace
    imm, dfr = source_table("immediate", "full"), source_table("deferrable", "full")
    assert imm.has_key and not dfr.has_key
    assert imm.shape_hash() != dfr.shape_hash()
    # also when a value claims the same key columns: deferrability alone changes the shape
    same_cols = replace(dfr, key=("id",), key_attnums=(1,), key_source="primary key")
    assert same_cols.shape_hash() != imm.shape_hash()
    assert replace(imm, relfilenode=99).shape_hash() == imm.shape_hash()
    assert source_table("none", "full").shape_hash() != dfr.shape_hash()
    for old, new in ((imm, dfr), (facts("immediate", "full"), facts("deferrable", "full"))):
        ch = catalog.diff([old], [new]).changed[0]
        assert ch.key_changed and not ch.columns_changed and ch.replica_identity_changed is None
        assert catalog.diff([new], [old]).changed[0].key_changed
    assert catalog.diff([dfr], [replace(dfr)]).empty
