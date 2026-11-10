"""Hermetic tests of the test-only pgoutput decoder.  Messages are built here byte by byte, following
src/backend/replication/logical/proto.c; the localpg tests check the decoder against a real server."""
import struct

import pytest
from hypothesis import given, strategies as st

from cdc_v2.testing import pgoutput as P


def cstr(s):
    return s.encode() + b"\x00"


def m_begin(final_lsn, ts, xid):
    return b"B" + struct.pack("!QqI", final_lsn, ts, xid)


def m_commit(commit_lsn, end_lsn, ts, flags=0):
    return b"C" + struct.pack("!BQQq", flags, commit_lsn, end_lsn, ts)


def m_relation(rel_id, ns, name, cols, ident=b"d"):
    out = b"R" + struct.pack("!I", rel_id) + cstr(ns) + cstr(name) + ident + struct.pack("!h", len(cols))
    for cname, key in cols:
        out += struct.pack("!B", 1 if key else 0) + cstr(cname) + struct.pack("!Ii", 23, -1)
    return out


def m_tuple(values):
    out = struct.pack("!h", len(values))
    for v in values:
        if v is None:
            out += b"n"
        elif v is P.UNCHANGED:
            out += b"u"
        elif isinstance(v, bytes):
            out += b"b" + struct.pack("!i", len(v)) + v
        else:
            b = str(v).encode()
            out += b"t" + struct.pack("!i", len(b)) + b
    return out


def m_insert(rel_id, values):
    return b"I" + struct.pack("!I", rel_id) + b"N" + m_tuple(values)


def m_update(rel_id, new, old=None, old_kind=b"K"):
    out = b"U" + struct.pack("!I", rel_id)
    if old is not None:
        out += old_kind + m_tuple(old)
    return out + b"N" + m_tuple(new)


def m_delete(rel_id, old, old_kind=b"K"):
    return b"D" + struct.pack("!I", rel_id) + old_kind + m_tuple(old)


def test_begin_commit_fields():
    b = P.decode(m_begin(0x16B374D800, 123456, 4_000_000_000))
    assert (b.final_lsn, b.commit_time_us, b.xid) == (0x16B374D800, 123456, 4_000_000_000)   # xid is unsigned
    c = P.decode(m_commit(0x16B374D800, 0x16B374D848, 123456))
    assert (c.flags, c.commit_lsn, c.end_lsn, c.commit_time_us) == (0, 0x16B374D800, 0x16B374D848, 123456)


def test_relation_and_changes():
    r = P.decode(m_relation(16400, "cdc_v2_cut", "counter", [("writer", True), ("n", False)]))
    assert (r.rel_id, r.namespace, r.name, r.replica_identity) == (16400, "cdc_v2_cut", "counter", "d")
    assert [(c.name, c.is_key, c.type_oid, c.type_mod) for c in r.columns] == [("writer", True, 23, -1),
                                                                             ("n", False, 23, -1)]
    assert P.decode(m_relation(1, "", "pg_class", [])).namespace == "pg_catalog"
    i = P.decode(m_insert(16400, ["1", None, P.UNCHANGED, b"\x00\x01"]))
    assert i.new == ("1", None, P.UNCHANGED, b"\x00\x01")
    u = P.decode(m_update(16400, ["1", "8"]))
    assert (u.old_kind, u.old, u.new) == (None, None, ("1", "8"))
    u = P.decode(m_update(16400, ["2", "8"], old=["1", None], old_kind=b"O"))
    assert (u.old_kind, u.old, u.new) == ("O", ("1", None), ("2", "8"))
    d = P.decode(m_delete(16400, ["1"]))
    assert (d.old_kind, d.old) == ("K", ("1",))
    t = P.decode(b"T" + struct.pack("!iBII", 2, 3, 16400, 16401))
    assert t.rel_ids == (16400, 16401) and t.cascade and t.restart_identity


def test_type_origin_message_are_skipped_but_length_checked():
    y = P.decode(b"Y" + struct.pack("!I", 16500) + cstr("public") + cstr("mood"))
    assert isinstance(y, P.Skipped) and y.kind == "Y"
    o = P.decode(b"O" + struct.pack("!Q", 77) + cstr("origin1"))
    assert o.kind == "O" and o.detail["origin_lsn"] == 77
    m = P.decode(b"M" + struct.pack("!BQ", 1, 99) + cstr("pfx") + struct.pack("!i", 3) + b"abc")
    assert m.kind == "M" and m.detail == {"transactional": True, "lsn": 99, "prefix": "pfx", "length": 3}
    with pytest.raises(P.ProtocolError):
        P.decode(b"M" + struct.pack("!BQ", 1, 99) + cstr("pfx") + struct.pack("!i", 4) + b"abc")


@pytest.mark.parametrize("bad", [
    b"", b"B", b"B" + b"\x00" * 19, m_begin(1, 2, 3) + b"x", b"S" + b"\x00" * 8, b"P", b"X",
    b"I" + struct.pack("!I", 1) + b"K" + m_tuple(["1"]),
    b"U" + struct.pack("!I", 1) + b"K" + m_tuple(["1"]),
    b"D" + struct.pack("!I", 1) + b"N" + m_tuple(["1"]),
    b"I" + struct.pack("!I", 1) + b"N" + struct.pack("!h", 1) + b"z",
    b"I" + struct.pack("!I", 1) + b"N" + struct.pack("!h", 1) + b"t" + struct.pack("!i", 10) + b"abc",
    b"R" + struct.pack("!I", 1) + b"ns",
])
def test_malformed_messages_raise(bad):
    with pytest.raises(P.ProtocolError):
        P.decode(bad)


@given(st.binary(max_size=64))
def test_decoder_never_fails_in_another_way(data):
    try:
        P.decode(data)
    except (P.ProtocolError, UnicodeDecodeError):
        pass


def rows_for(xid, s, e, first, changes):
    rows = [(P_lsn(first), str(xid), m_begin(s, 5, xid))]
    rows += [(P_lsn(l), str(xid), d) for l, d in changes]
    rows.append((P_lsn(e), str(xid), m_commit(s, e, 5)))
    return rows


def P_lsn(v):
    return "%X/%X" % (v >> 32, v & 0xFFFFFFFF)


def test_stream_decoder_builds_transactions_and_keeps_row_lsns():
    dec = P.StreamDecoder()
    rel = m_relation(16400, "cdc_v2_cut", "counter", [("writer", True), ("n", False)])
    rows = rows_for(700, 0x1F00, 0x1F40, 0x1E00, [(0x1E00, rel), (0x1E00, m_update(16400, ["1", "8"]))])
    rows += rows_for(701, 0x2100, 0x2140, 0x2000, [(0x2000, m_update(16400, ["1", "9"]))])   # relation cached
    txns = list(dec.feed(rows))
    assert [(t.xid, t.s, t.e, t.begin_row_lsn, t.commit_row_lsn) for t in txns] == [
        (700, 0x1F00, 0x1F40, 0x1E00, 0x1F40), (701, 0x2100, 0x2140, 0x2000, 0x2140)]
    assert txns[0].changes[0][:2] == (0x1E00, "cdc_v2_cut.counter") and txns[1].changes[0][2].new == ("1", "9")
    assert dec.column_names("cdc_v2_cut.counter") == ("writer", "n")
    assert (dec.transactions, dec.e_order_violations, dec.messages) == (2, 0, 7)


def test_stream_decoder_counts_e_order_violations():
    dec = P.StreamDecoder()
    rows = rows_for(700, 0x2100, 0x2140, 0x2000, []) + rows_for(701, 0x1F00, 0x1F40, 0x1E00, [])
    assert len(list(dec.feed(rows))) == 2 and dec.e_order_violations == 1


@pytest.mark.parametrize("rows", [
    [("0/10", "700", m_commit(1, 2, 3))],                                         # COMMIT without BEGIN
    [("0/10", "700", m_begin(1, 2, 700)), ("0/10", "700", m_begin(1, 2, 700))],   # nested BEGIN
    [("0/10", "701", m_begin(1, 2, 700))],                                        # row xid differs
    [("0/10", "700", m_begin(0x30, 2, 700)), ("0/40", "700", m_commit(0x31, 0x40, 2))],   # commit_lsn != final_lsn
    [("0/10", "700", m_begin(0x30, 2, 700)), ("0/41", "700", m_commit(0x30, 0x40, 2))],   # row lsn != end_lsn
    [("0/10", "700", m_begin(0x30, 2, 700)), ("0/30", "700", m_commit(0x30, 0x30, 2))],   # end not after start
    [("0/10", "700", m_begin(0x30, 2, 700)), ("0/20", "700", m_insert(5, ["1"]))],        # unknown relation
])
def test_stream_decoder_rejects_protocol_violations(rows):
    with pytest.raises(P.ProtocolError):
        list(P.StreamDecoder().feed(rows))
