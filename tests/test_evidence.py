"""Redaction of evidence and logs (SPEC §4, R23). Hermetic: every secret here is made up for the test."""
import json
from pathlib import Path

import pytest

from cdc_v2.evidence import EvidenceWriter, Redactor



def fake_jwt(*parts):
    """A made-up token in JWT shape, assembled at run time so that no credential-shaped literal is committed."""
    return ".".join(["ey" + "J" + p for p in parts])


FAKE_JWT = fake_jwt("hbGciOiJIUzI1NiJ9", "zdWIiOiJ0ZXN0LXVzZXIifQ", "jMmxuYXR1cmUtbm90LXJlYWw")


@pytest.fixture
def env_file(tmp_path):
    p = tmp_path / "env"
    p.write_text("# comment\nPG_HOST=db-7.example-provider.net\nPG_USER=role_xyz_123\n"
                 "PG_PASSWORD='Very$ecretValue42'\nSHORT=ab\nPG_PORT=6543\n")
    return p


@pytest.fixture
def red(env_file):
    return Redactor(extra_values=["registered-value-77"], env={"motherduck_token": FAKE_JWT},
                    env_files=[env_file], home="/Users/someone", user="someone", hostnames=["laptop-17.lan", "laptop-17"])


def test_exact_values_from_env_files_and_environment(red):
    s = red.redact("host db-7.example-provider.net user role_xyz_123 pw Very$ecretValue42 tok %s" % FAKE_JWT)
    for secret in ("db-7.example-provider.net", "role_xyz_123", "Very$ecretValue42", FAKE_JWT):
        assert secret not in s
    assert "registered-value-77" not in red.redact("x registered-value-77 y")


def test_short_and_ordinary_values_are_kept(red):
    assert red.redact("ab cd port 6543 localhost") == "ab cd port 6543 localhost"


def test_home_user_and_host(red):
    s = red.redact("/Users/someone/.cache/cdc_flight_v2/pg run by someone on laptop-17.lan (laptop-17)")
    assert "/Users/someone" not in s and "~/.cache/cdc_flight_v2/pg" in s
    assert "someone" not in s and "laptop-17" not in s


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


def test_tokens_emails_and_provider_hosts(red):
    other = fake_jwt("hbGciOiJSUzI1NiJ9", "pc3MiOiJ4eXoifQ", "W5vdGhlci1zaWduYXR1cmU")
    s = red.redact("Bearer abcdefghijklmnop %s mail someone.else@example.com host aws.connect.psdb.cloud x.internal"
                   % other)
    assert other not in s and "abcdefghijklmnop" not in s and "example.com" not in s
    assert "psdb.cloud" not in s and "x.internal" not in s


def test_secret_assignments_in_json(red):
    s = red.redact('{"password": "p4ssw0rd!", "api_key": "k-123456", "count": 3}')
    assert "p4ssw0rd!" not in s and "k-123456" not in s and '"count": 3' in s


def test_redact_obj_walks_keys_and_values(red):
    obj = {"/Users/someone/x": ["role_xyz_123", {"n": 1, "t": FAKE_JWT}]}
    out = red.redact_obj(obj)
    assert json.dumps(out).count("<redacted>") == 2 and "~/x" in out


def test_evidence_writer(tmp_path, red):
    ev = EvidenceWriter("p3/oracle", root=tmp_path, redactor=red)
    p = ev.write_json("result", {"dsn": "host=db-7.example-provider.net user=x", "home": "/Users/someone/a"})
    body = p.read_text()
    assert p == tmp_path / "p3" / "oracle" / "result.json"
    assert "db-7" not in body and "/Users/someone" not in body
    t = ev.write_text("run.log", "token %s" % FAKE_JWT)
    assert FAKE_JWT not in t.read_text()
    ev.append_text("run.log", "second Very$ecretValue42")
    assert "Very$ecretValue42" not in t.read_text() and t.read_text().count("\n") == 2


@pytest.mark.parametrize("item", ["../x", "/abs", "p3/../../x", "", "A B"])
def test_evidence_writer_rejects_bad_items(tmp_path, item):
    with pytest.raises(ValueError):
        EvidenceWriter(item, root=tmp_path)


def test_evidence_writer_rejects_bad_names(tmp_path, red):
    ev = EvidenceWriter("p3", root=tmp_path, redactor=red)
    with pytest.raises(ValueError):
        ev.write_text("../escape", "x")
