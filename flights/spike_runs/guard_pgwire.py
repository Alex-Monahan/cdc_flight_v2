"""Shortest guard run, no requirements: read one leader row through MotherDuck's Postgres endpoint.

P2 run-semantics spike (docs/spike/run_semantics.md, question 3). Uses only the Python standard library: a minimal
PostgreSQL wire-protocol client (TLS, cleartext or SCRAM-SHA-256 password, simple query). The password is the
Flight's MOTHERDUCK_TOKEN; it is never printed. Endpoint: https://motherduck.com/docs/sql-reference/postgres-endpoint/

Config (environment): CDC_V2_RUNS_DB (database, cdc_v2_* only), CDC_V2_RUNS_REGION (default us-east-1),
CDC_V2_GUARD_TABLE (default guard_leader). CDC_V2_GUARD_FAST_EXIT=1 ends with os._exit(0). Prints `FIRST <unix>` first and `GUARD {json}` last; exits 0.
"""
import time

T0 = time.time()
print("FIRST %.3f" % T0, flush=True)

import base64  # noqa: E402
import hashlib  # noqa: E402
import hmac  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import socket  # noqa: E402
import ssl  # noqa: E402
import struct  # noqa: E402


class PgError(Exception):
    pass


class PgWire:
    """Just enough of the PostgreSQL v3 protocol for one simple query (text results)."""

    def __init__(self, host, port, user, password, database, options=None, timeout=15.0):
        raw = socket.create_connection((host, port), timeout=timeout)
        raw.sendall(struct.pack("!ii", 8, 80877103))  # SSLRequest
        if raw.recv(1) != b"S":
            raise PgError("server refused TLS")
        ctx = ssl.create_default_context()
        self.sock = ctx.wrap_socket(raw, server_hostname=host)
        params = {"user": user, "database": database, "application_name": "cdc_v2_spike_runs_guard"}
        if options:
            params["options"] = options
        body = struct.pack("!i", 196608) + b"".join(k.encode() + b"\0" + v.encode() + b"\0"
                                                    for k, v in params.items()) + b"\0"
        self.sock.sendall(struct.pack("!i", len(body) + 4) + body)
        self._buf = b""
        self._auth(password)

    def _recv_exact(self, n):
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise PgError("connection closed")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _msg(self):
        kind, length = struct.unpack("!ci", self._recv_exact(5))
        return kind, self._recv_exact(length - 4)

    def _send(self, kind, payload):
        self.sock.sendall(kind + struct.pack("!i", len(payload) + 4) + payload)

    @staticmethod
    def _error(payload):
        fields = {}
        for part in payload.split(b"\0"):
            if part:
                fields[chr(part[0])] = part[1:].decode(errors="replace")
        return PgError("%s %s" % (fields.get("C", "?"), fields.get("M", "?")))

    def _auth(self, password):
        scram = None
        while True:
            kind, payload = self._msg()
            if kind == b"E":
                raise self._error(payload)
            if kind == b"R":
                code = struct.unpack("!i", payload[:4])[0]
                if code == 0:
                    continue
                if code == 3:  # cleartext (inside TLS)
                    self._send(b"p", password.encode() + b"\0")
                elif code == 5:  # md5
                    md5_seed = payload[4:8]
                    inner = hashlib.md5(password.encode() + b"postgres").hexdigest().encode()
                    self._send(b"p", b"md5" + hashlib.md5(inner + salt).hexdigest().encode() + b"\0")
                elif code == 10:  # SASL: SCRAM-SHA-256
                    nonce = base64.b64encode(os.urandom(18)).decode()
                    first_bare = "n=,r=" + nonce
                    data = ("n,," + first_bare).encode()
                    self._send(b"p", b"SCRAM-SHA-256\0" + struct.pack("!i", len(data)) + data)
                    scram = {"nonce": nonce, "first_bare": first_bare}
                elif code == 11:
                    server_first = payload[4:].decode()
                    attrs = dict(kv.split("=", 1) for kv in server_first.split(","))
                    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), base64.b64decode(attrs["s"]),
                                                 int(attrs["i"]))
                    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
                    stored = hashlib.sha256(client_key).digest()
                    final_wo_proof = "c=biws,r=" + attrs["r"]
                    auth_msg = ",".join((scram["first_bare"], server_first, final_wo_proof)).encode()
                    sig = hmac.new(stored, auth_msg, hashlib.sha256).digest()
                    proof = base64.b64encode(bytes(a ^ b for a, b in zip(client_key, sig))).decode()
                    self._send(b"p", (final_wo_proof + ",p=" + proof).encode())
                elif code == 12:
                    continue
                else:
                    raise PgError("unsupported authentication code %d" % code)
            elif kind == b"Z":
                return

    def query(self, sql):
        self._send(b"Q", sql.encode() + b"\0")
        rows, err = [], None
        while True:
            kind, payload = self._msg()
            if kind == b"D":
                n = struct.unpack("!h", payload[:2])[0]
                pos, row = 2, []
                for _ in range(n):
                    ln = struct.unpack("!i", payload[pos:pos + 4])[0]
                    pos += 4
                    if ln < 0:
                        row.append(None)
                    else:
                        row.append(payload[pos:pos + ln].decode())
                        pos += ln
                rows.append(row)
            elif kind == b"E":
                err = self._error(payload)
            elif kind == b"Z":
                if err:
                    raise err
                return rows

    def close(self):
        try:
            self._send(b"X", b"")
            self.sock.close()
        except OSError:
            pass


def main():
    db = os.environ.get("CDC_V2_RUNS_DB", "cdc_v2_spike_runs_db")
    if not db.startswith("cdc_v2_"):
        raise SystemExit("database must be cdc_v2_*")
    table = os.environ.get("CDC_V2_GUARD_TABLE", "guard_leader")
    region = os.environ.get("CDC_V2_RUNS_REGION", "us-east-1")
    out = {"variant": "pgwire", "run": os.environ.get("MOTHERDUCK_FLIGHT_RUN_NUMBER")}
    t = time.time()
    con = PgWire("pg.%s-aws.motherduck.com" % region, 5432, "postgres", os.environ["MOTHERDUCK_TOKEN"], db,
                 options="--attach_mode=single")
    out["connect_s"] = round(time.time() - t, 3)
    t = time.time()
    rows = con.query("SELECT run_number, epoch, state, date_diff('millisecond', heartbeat_at, now()) "
                     "FROM %s WHERE id = 1" % table)
    out["query_s"] = round(time.time() - t, 3)
    con.close()
    if rows:
        run_number, epoch, state, age_ms = rows[0]
        out.update(leader_run=run_number, epoch=epoch, state=state, heartbeat_age_ms=int(age_ms))
        out["decision"] = "exit_healthy" if state == "leading" and int(age_ms) < 30000 else "would_take_over"
    else:
        out["decision"] = "no_leader_row"
    out["total_in_code_s"] = round(time.time() - T0, 3)
    print("GUARD " + json.dumps(out), flush=True)
    if os.environ.get("CDC_V2_GUARD_FAST_EXIT") == "1":
        os._exit(0)  # skip interpreter teardown


if __name__ == "__main__":
    main()
