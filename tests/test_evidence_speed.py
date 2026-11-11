"""write_evidence is linear in the size of the text: a 5 MB bundle with long values (20 kB hex runs, the shape of an
oracle sample of a TOASTed column) is written in under 10 s, and its planted secret-looking values are still
redacted (hermetic; the values are made up at run time)."""
import json
import random
import time

from cdc_v2.testing.harness_evidence import write_evidence


def test_a_5_mb_bundle_is_redacted_in_under_10_s(tmp_path):
    r = random.Random(5)
    pw = "pw" + "".join(r.choice("abcdefghjkmnpqrstuvwxyz23456789") for _ in range(14))
    mail = "someone%d@example.invalid" % r.randrange(1000)
    rows = []
    size = 0
    while size < 5_000_000:
        big = "%020000x" % r.getrandbits(80_000)
        row = {"key": r.randrange(1 << 40), "big": big, "note": "a.b-c_d" * 50, "host": "db1.internal"}
        rows.append(row)
        size += len(big) + 400
    rows.append({"password": pw, "contact": mail, "addr": "10.%d.0.7" % r.randrange(200)})
    text = json.dumps(rows)
    t = time.monotonic()
    out = write_evidence(tmp_path / "bundle.json", text)
    took = time.monotonic() - t
    assert took < 10.0, "%.1f s for %d bytes" % (took, len(text))
    assert pw not in out and mail not in out and "db1.internal" not in out and "10." not in out.split('"addr"')[1]
