"""Redaction of evidence and logs (SPEC §4, R23), and the one evidence writer that applies it. Hermetic: every
secret here is made up, and credential-shaped strings are assembled at run time so that none is committed."""
import json
import os

import pytest

from cdc_v2 import evidence as E
from cdc_v2.evidence import Redactor
from cdc_v2.testing import harness_evidence as he


def fake_jwt(*parts):
    return ".".join(["ey" + "J" + p for p in parts])


FAKE_JWT = fake_jwt("hbGciOiJIUzI1NiJ9", "zdWIiOiJ0ZXN0LXVzZXIifQ", "jMmxuYXR1cmUtbm90LXJlYWw")
PW = "Very$ecret" + "Value42"


@pytest.fixture
def env_file(tmp_path):
    p = tmp_path / "env"
    p.write_text("# comment\nPG_HOST=db-7.example-provider.net\nPG_USER=role_xyz_123\n"
                 "PG_PASSWORD='%s'\nPG_DATABASE=abq\nSHORT=ab\nPG_PORT=6543\nBRANCH=main\n" % PW)
    return p


@pytest.fixture
def red(env_file, tmp_path):
    home = tmp_path / "home" / "someone"
    home.mkdir(parents=True)
    link = tmp_path / "homelink"
    link.symlink_to(home)
    return Redactor(extra_values=["registered-value-77"], env={"motherduck_token": FAKE_JWT},
                    env_files=[env_file], home=str(link), user="someone",
                    hostnames=["laptop-17.lan", "laptop-17"])


def test_exact_values_from_env_files_and_environment(red):
    s = red.redact("host db-7.example-provider.net user role_xyz_123 pw %s tok %s" % (PW, FAKE_JWT))
    for secret in ("db-7.example-provider.net", "role_xyz_123", PW, FAKE_JWT):
        assert secret not in s
    assert "registered-value-77" not in red.redact("x registered-value-77 y")


def test_short_values_are_removed_as_whole_words(red):
    s = red.redact("database abq, not abqx or xabq; port 6543; branch main; ab")
    assert "database <redacted>," in s and "abqx" in s and "xabq" in s
    assert "6543" in s and "main" in s and " ab" in s          # numbers, ordinary words, 2 characters stay


def test_home_by_realpath_and_only_at_a_path_boundary(red, tmp_path):
    real = os.path.realpath(str(tmp_path / "homelink"))
    s = red.redact("%s/.cache/x and %s/y and %sother/z" % (str(tmp_path / "homelink"), real, real))
    assert "~/.cache/x" in s and "~/y" in s
    assert real + "other/z" in s                               # a longer path that starts with the home is not cut


def test_user_and_host(red):
    s = red.redact("run by someone on laptop-17.lan (laptop-17); someones stays")
    assert "someone " not in s and "laptop-17" not in s and "someones" in s


def test_connection_strings_and_uris(red):
    s = red.redact("host=10.1.2.3 port=5433 dbname=app user=alice password=hunter2xyz sslmode=verify-full")
    assert "10.1.2.3" not in s and "alice" not in s and "hunter2xyz" not in s
    assert "port=5433" in s and "dbname=app" in s
    cred = "bob" + ":" + "pw123"
    assert cred not in red.redact("postgresql://" + cred + "@db.example.org:5432/app")
    cred = "carol" + ":" + "pwxyz"
    assert cred not in red.redact("https://" + cred + "@api.example.org/x")
    tok = "abcdefgh" + "123"
    assert tok not in red.redact("md:cdc_v2_t_x?motherduck_" + "token=" + tok + "&attach_mode=single")
    assert "cdc_v2_t_x" in red.redact("md:cdc_v2_t_x?motherduck_" + "token=" + tok)


def test_planetscale_tokens(red):
    for kind in ("tkn", "pw", "oauth"):
        tok = "pscale_" + kind + "_" + "AbCdEf0123456789xyzXYZ"
        assert tok not in red.redact("value " + tok + " end")


def test_ip_addresses(red):
    s = red.redact("from 10.20.30.40 and 2001:db8::8a2e:370:7334 and fe80:0:0:0:0:0:0:1 via 127.0.0.1 and ::1")
    assert "10.20.30.40" not in s and "2001:db8" not in s and "fe80:" not in s
    assert "127.0.0.1" in s and "::1" in s
    assert red.redact("at 12:30:45 and version 1.5.6") == "at 12:30:45 and version 1.5.6"


def test_tokens_emails_and_provider_hosts(red):
    other = fake_jwt("hbGciOiJSUzI1NiJ9", "pc3MiOiJ4eXoifQ", "W5vdGhlci1zaWduYXR1cmU")
    s = red.redact("Bearer abcdefghijklmnop %s mail someone.else@example.com host aws.connect.psdb.cloud x.internal"
                   % other)
    assert other not in s and "abcdefghijklmnop" not in s and "example.com" not in s
    assert "psdb.cloud" not in s and "x.internal" not in s


def test_secret_assignments_in_json(red):
    s = red.redact('{"password": "p4ss' + 'w0rd!", "api_key": "k-123456", "count": 3}')
    assert "w0rd!" not in s and "k-123456" not in s and '"count": 3' in s


def test_redact_obj_walks_keys_and_values(red, tmp_path):
    home = str(tmp_path / "homelink")
    out = red.redact_obj({home + "/x": ["role_xyz_123", {"n": 1, "t": FAKE_JWT}]})
    assert json.dumps(out).count("<redacted>") == 2 and "~/x" in out


def test_leftovers_are_empty_after_redaction(red):
    raw = "user=alice token %s ip 10.0.0.9 %s" % (FAKE_JWT, PW)
    assert red.leftovers(raw)
    assert red.leftovers(red.redact(raw)) == []


# --------------------------------------------------------------------------------------------- the one writer

@pytest.fixture
def default_red(monkeypatch, red):
    monkeypatch.setattr(E, "_DEFAULT", red)
    return red


def test_write_evidence_redacts(tmp_path, default_red):
    p = tmp_path / "p3" / "oracle" / "result.json"
    he.write_evidence(p, {"dsn": "host=db-7.example-provider.net user=x", "pw": PW, "ip": "10.9.8.7", "n": 1})
    body = p.read_text()
    assert "db-7" not in body and PW not in body and "10.9.8.7" not in body and '"n": 1' in body
    t = tmp_path / "run.log"
    he.write_evidence(t, "token %s\n" % FAKE_JWT)
    he.write_evidence(t, "second %s\n" % PW, append=True)
    assert FAKE_JWT not in t.read_text() and PW not in t.read_text() and t.read_text().count("\n") == 2


def test_write_evidence_refuses_what_redaction_cannot_fix(tmp_path, default_red, monkeypatch):
    # a redactor that misses everything: the writer must refuse instead of writing
    monkeypatch.setattr(default_red, "redact", lambda s: str(s))
    with pytest.raises(he.EvidenceError):
        he.write_evidence(tmp_path / "x.txt", "token %s\n" % FAKE_JWT)
    assert not (tmp_path / "x.txt").exists()
