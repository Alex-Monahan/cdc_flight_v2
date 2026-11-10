"""Transparent TCP fault proxy (asyncio, standard library) for the connections between the pipeline and Postgres.

    with NetProxy("localhost", 55432) as px:          # listens on a free port in 55440-55459
        dsn = "... host=localhost port=%d ..." % px.port
        px.set_fault("blackhole")                      # both directions; or direction="up" / "down"
        px.set_fault("delay", seconds=0.2)
        px.set_fault("throttle", rate=50_000)          # bytes per second
        px.set_fault("reset")                          # RST every open connection, and new ones while set
        px.set_fault("pass")
        px.stats()                                     # counters prove that a fault was in effect

Bytes are forwarded unchanged, so TLS passes through untouched. ``up`` is client to server, ``down`` server to client.

Faults (``set_fault(mode, direction="both", seconds=None, rate=None, conn=None)``); each call replaces the policy of
its scope (all connections, or one connection id from ``connections()``):

``pass``       forward normally.
``refuse``     stop listening: new connections get ECONNREFUSED; open connections are not touched (scope all only).
``close``      close open connections with FIN (both sides); new connections are accepted and closed at once.
``reset``      the same with RST (``SO_LINGER`` 0).
``blackhole``  connections stay open and nothing is forwarded in the given direction(s). The proxy keeps reading up to
               ``HOLD_LIMIT`` bytes per direction (counted as ``held``), then stops reading, so the sender's socket
               buffers fill. Held bytes are delivered when the hole is lifted, as TCP would retransmit them.
               Limit: the proxy's own kernel still acknowledges data and TCP keepalive probes, so a black hole here
               is detected by application deadlines and heartbeats, not by TCP keepalives (docs/testing/harness.md).
``delay``      every chunk is forwarded ``seconds`` after it was read, in order.
``throttle``   at most ``rate`` bytes per second.
``set_policy(...)`` combines delay, throttle and black hole per direction.

Other processes control a proxy through its control socket (``ProxyControl``, ``tools/netproxy``); the socket and
the port lock live under ``~/.cache/cdc_flight_v2/netproxy/``. A port is leased with an exclusive ``flock`` on
``port-<n>.lock`` for the proxy's lifetime, so concurrent test processes never share a port; the lock dies with the
process.
"""
import asyncio
import errno
import fcntl
import json
import os
import socket
import struct
import threading
import time

PORT_FIRST, PORT_LAST = 55440, 55459
STATE_DIR = os.path.expanduser(os.environ.get("CDC_V2_NETPROXY_DIR", "~/.cache/cdc_flight_v2/netproxy"))
HOLD_LIMIT = 1 << 20
CHUNK = 65536
CONNECT_TIMEOUT_S = 10.0
CALL_TIMEOUT_S = 10.0
MODES = ("pass", "refuse", "close", "reset", "blackhole", "delay", "throttle")
DIRECTIONS = ("up", "down", "both")
_LOOPBACK_NAMES = {"localhost", "localhost.", "::1", "0.0.0.0", "::"}


class ProxyError(RuntimeError):
    pass


def _is_loopback(host):
    return host in _LOOPBACK_NAMES or host.startswith("127.")


def check_target(host, port):
    """The owner's Postgres service listens on the local default port: never proxy to it."""
    if not isinstance(port, int) or not 0 < port < 65536:
        raise ProxyError("bad target port %r" % (port,))
    if port == 5432 and _is_loopback(host):
        raise ProxyError("refused: a local target on port 5432 is out of bounds")


class PortLease:
    """Exclusive lease of one port in 55440-55459 through ``flock`` on a lock file."""

    def __init__(self, port, fd):
        self.port, self.fd = port, fd

    @property
    def lock_path(self):
        return os.path.join(STATE_DIR, "port-%d.lock" % self.port)

    @property
    def control_path(self):
        return control_path(self.port)

    def release(self):
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


def control_path(port):
    return os.path.join(STATE_DIR, "port-%d.sock" % port)


def try_lease(port):
    os.makedirs(STATE_DIR, exist_ok=True)
    fd = os.open(os.path.join(STATE_DIR, "port-%d.lock" % port), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode())
    return PortLease(port, fd)


def lease_is_held(port):
    """True when some process holds the lease of ``port`` (a proxy runs there)."""
    lease = try_lease(port)
    if lease is None:
        return True
    lease.release()
    return False


def _set_linger0(sock):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    except OSError:
        pass


class Policy:
    FIELDS = ("blackhole_up", "blackhole_down", "delay_up", "delay_down", "rate_up", "rate_down", "on_accept")

    def __init__(self, **kw):
        self.blackhole_up = self.blackhole_down = False
        self.delay_up = self.delay_down = 0.0
        self.rate_up = self.rate_down = None
        self.on_accept = None             # None, "close" or "reset"
        for k, v in kw.items():
            if k not in self.FIELDS:
                raise ProxyError("unknown policy field %r" % k)
            setattr(self, k, v)

    def as_dict(self):
        return {k: getattr(self, k) for k in self.FIELDS}

    def blackholed(self, d):
        return self.blackhole_up if d == "up" else self.blackhole_down

    def delay(self, d):
        return self.delay_up if d == "up" else self.delay_down

    def rate(self, d):
        return self.rate_up if d == "up" else self.rate_down


def policy_for(mode, direction="both", seconds=None, rate=None):
    if mode not in MODES:
        raise ProxyError("unknown mode %r (one of %s)" % (mode, ", ".join(MODES)))
    if direction not in DIRECTIONS:
        raise ProxyError("unknown direction %r" % (direction,))
    up, down = direction in ("up", "both"), direction in ("down", "both")
    if mode == "blackhole":
        return Policy(blackhole_up=up, blackhole_down=down)
    if mode == "delay":
        if seconds is None or seconds < 0:
            raise ProxyError("delay needs seconds >= 0")
        return Policy(delay_up=seconds if up else 0.0, delay_down=seconds if down else 0.0)
    if mode == "throttle":
        if not rate or rate <= 0:
            raise ProxyError("throttle needs rate > 0 (bytes per second)")
        return Policy(rate_up=rate if up else None, rate_down=rate if down else None)
    if mode in ("close", "reset"):
        return Policy(on_accept=mode)
    return Policy()


class _Conn:
    def __init__(self, cid, cr, cw):
        self.id = cid
        self.cr, self.cw = cr, cw
        self.ur = self.uw = None
        self.policy = None                # per-connection override
        self.opened = time.time()
        self.bytes = {"up": 0, "down": 0}
        self.held = {"up": 0, "down": 0}
        self.state = "open"
        self.tasks = []
        self.peer = None

    def info(self):
        return {"id": self.id, "state": self.state, "bytes_up": self.bytes["up"], "bytes_down": self.bytes["down"],
                "held_up": self.held["up"], "held_down": self.held["down"],
                "age_s": round(time.time() - self.opened, 3),
                "policy": self.policy.as_dict() if self.policy else None}


class NetProxy:
    """One proxy: listener, connections, faults, counters, control socket. Runs its own event loop thread."""

    def __init__(self, target_host, target_port, port=None, control=True):
        check_target(target_host, target_port)
        self.target_host, self.target_port = target_host, target_port
        self.want_port = port
        self.want_control = control
        self.lease = None
        self.port = None
        self.loop = None
        self.thread = None
        self.servers = []
        self.control_server = None
        self.policy = Policy()
        self.global_mode = "pass"
        self.conns = {}
        self.next_id = 1
        self.counters = {"accepted": 0, "closed": 0, "closed_by_fault": 0, "reset_by_fault": 0,
                         "accept_closed": 0, "accept_reset": 0, "upstream_errors": 0, "bytes_up": 0, "bytes_down": 0,
                         "faults_set": 0, "listening": False}
        self.cond = None
        self._stopped = threading.Event()

    # -- life cycle ----------------------------------------------------------------------------------------------
    def start(self, timeout=CALL_TIMEOUT_S):
        ready = threading.Event()
        err = []

        def run():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            try:
                self.loop.run_until_complete(self._open())
            except BaseException as exc:          # noqa: BLE001 - reported to the caller of start()
                err.append(exc)
                ready.set()
                self.loop.close()
                return
            ready.set()
            try:
                self.loop.run_forever()
            finally:
                self.loop.run_until_complete(self._close_all())
                self.loop.close()
                self._stopped.set()

        self.thread = threading.Thread(target=run, name="netproxy", daemon=True)
        self.thread.start()
        if not ready.wait(timeout):
            raise ProxyError("proxy did not start within %.0f s" % timeout)
        if err:
            raise err[0]
        return self

    async def _open(self):
        self.cond = asyncio.Condition()
        ports = [self.want_port] if self.want_port else list(range(PORT_FIRST, PORT_LAST + 1))
        for p in ports:
            if not PORT_FIRST <= p <= PORT_LAST:
                raise ProxyError("port %d is outside %d-%d" % (p, PORT_FIRST, PORT_LAST))
            lease = try_lease(p)
            if lease is None:
                continue
            try:
                self.port = p
                await self._listen()
            except OSError as exc:
                lease.release()
                self.port = None
                if exc.errno != errno.EADDRINUSE:
                    raise
                continue
            self.lease = lease
            break
        if self.lease is None:
            raise ProxyError("no free proxy port in %d-%d" % (PORT_FIRST, PORT_LAST))
        if self.want_control:
            path = self.lease.control_path
            if os.path.exists(path):
                os.unlink(path)               # stale: we hold the lease, so nobody else serves it
            self.control_server = await asyncio.start_unix_server(self._control_client, path=path)
            os.chmod(path, 0o600)

    async def _listen(self):
        servers = [await asyncio.start_server(self._client, "127.0.0.1", self.port, reuse_address=True)]
        try:
            servers.append(await asyncio.start_server(self._client, "::1", self.port, reuse_address=True))
        except OSError as exc:
            if exc.errno == errno.EADDRINUSE:
                servers[0].close()
                raise
        self.servers = servers
        self.counters["listening"] = True

    def _unlisten(self):
        for s in self.servers:
            s.close()
        self.servers = []
        self.counters["listening"] = False

    async def _close_all(self):
        self._unlisten()
        for c in list(self.conns.values()):
            self._drop(c, "close")
        if self.control_server is not None:
            self.control_server.close()
            try:
                os.unlink(self.lease.control_path)
            except OSError:
                pass
        await asyncio.sleep(0)
        if self.lease is not None:
            self.lease.release()

    def stop(self, timeout=CALL_TIMEOUT_S):
        if self.loop is not None and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            if not self._stopped.wait(timeout):
                raise ProxyError("proxy did not stop within %.0f s" % timeout)
        if self.thread is not None:
            self.thread.join(timeout)

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()
        return False

    def _call(self, coro_fn, *args, **kw):
        fut = asyncio.run_coroutine_threadsafe(coro_fn(*args, **kw), self.loop)
        return fut.result(CALL_TIMEOUT_S)

    # -- public API (thread-safe) ----------------------------------------------------------------------------------
    def set_fault(self, mode, direction="both", seconds=None, rate=None, conn=None):
        return self._call(self._set_fault, mode, direction, seconds, rate, conn)

    def set_policy(self, conn=None, **fields):
        return self._call(self._set_policy, Policy(**fields), conn)

    def stats(self):
        return self._call(self._stats)

    def connections(self):
        return self._call(self._connections)

    def wait_for(self, predicate, timeout, interval=0.02):
        """Poll ``predicate(stats)`` until true; raises TimeoutError."""
        deadline = time.monotonic() + timeout
        while True:
            s = self.stats()
            if predicate(s):
                return s
            if time.monotonic() >= deadline:
                raise TimeoutError("proxy condition not met within %.1f s: %r" % (timeout, s))
            time.sleep(interval)

    # -- fault logic (event loop) ----------------------------------------------------------------------------------
    async def _set_fault(self, mode, direction, seconds, rate, cid):
        pol = policy_for(mode, direction, seconds, rate)
        self.counters["faults_set"] += 1
        targets = list(self.conns.values()) if cid is None else [self._conn(cid)]
        if cid is None:
            if mode == "refuse":
                if self.servers:
                    self._unlisten()
                pol = Policy()
            elif not self.servers:
                await self._listen()
            self.global_mode = mode
            self.policy = pol
            for c in targets:
                c.policy = None
        else:
            if mode == "refuse":
                raise ProxyError("refuse applies to the listener, not to one connection")
            self._conn(cid).policy = pol
        if mode in ("close", "reset"):
            for c in targets:
                self._drop(c, mode, by_fault=True)
        await self._wake()
        return await self._stats()

    async def _set_policy(self, pol, cid):
        self.counters["faults_set"] += 1
        if cid is None:
            if not self.servers:
                await self._listen()
            self.policy, self.global_mode = pol, "policy"
            for c in self.conns.values():
                c.policy = None
        else:
            self._conn(cid).policy = pol
        await self._wake()
        return await self._stats()

    def _conn(self, cid):
        c = self.conns.get(cid)
        if c is None:
            raise ProxyError("no open connection %r" % (cid,))
        return c

    def _pol(self, c):
        return c.policy or self.policy

    async def _wake(self):
        async with self.cond:
            self.cond.notify_all()

    async def _stats(self):
        s = dict(self.counters)
        s.update(port=self.port, target_port=self.target_port, mode=self.global_mode, policy=self.policy.as_dict(),
                 active=len(self.conns), held_up=sum(c.held["up"] for c in self.conns.values()),
                 held_down=sum(c.held["down"] for c in self.conns.values()))
        return s

    async def _connections(self):
        return [c.info() for c in self.conns.values()]

    def _drop(self, c, how, by_fault=False):
        if c.state != "open":
            return
        c.state = "reset" if how == "reset" else "closed"
        for t in c.tasks:
            t.cancel()
        for w in (c.cw, c.uw):
            if w is None:
                continue
            if how == "reset":
                sock = w.get_extra_info("socket")
                if sock is not None:
                    _set_linger0(sock)
                w.transport.abort()
            else:
                w.close()
        self.conns.pop(c.id, None)
        self.counters["closed"] += 1
        if by_fault:
            self.counters["reset_by_fault" if how == "reset" else "closed_by_fault"] += 1

    async def _client(self, cr, cw):
        cid = self.next_id
        self.next_id += 1
        self.counters["accepted"] += 1
        on_accept = self.policy.on_accept
        if on_accept:
            if on_accept == "reset":
                sock = cw.get_extra_info("socket")
                if sock is not None:
                    _set_linger0(sock)
                cw.transport.abort()
                self.counters["accept_reset"] += 1
            else:
                cw.close()
                self.counters["accept_closed"] += 1
            return
        c = _Conn(cid, cr, cw)
        self.conns[cid] = c
        try:
            c.ur, c.uw = await asyncio.wait_for(asyncio.open_connection(self.target_host, self.target_port),
                                                CONNECT_TIMEOUT_S)
        except (OSError, asyncio.TimeoutError):
            self.counters["upstream_errors"] += 1
            self._drop(c, "reset")
            return
        if c.state != "open":                 # a fault dropped it while connecting
            c.uw.close()
            return
        for sock_w in (c.cw, c.uw):
            sock = sock_w.get_extra_info("socket")
            if sock is not None:
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except OSError:
                    pass
        q_up, q_down = asyncio.Queue(), asyncio.Queue()
        c.tasks = [asyncio.ensure_future(self._reader(c, "up", c.cr, q_up)),
                   asyncio.ensure_future(self._writer(c, "up", c.uw, q_up)),
                   asyncio.ensure_future(self._reader(c, "down", c.ur, q_down)),
                   asyncio.ensure_future(self._writer(c, "down", c.cw, q_down))]
        try:
            await asyncio.gather(*c.tasks)
        except asyncio.CancelledError:
            pass
        except (OSError, ConnectionError):
            pass
        if c.state == "open":
            self._drop(c, "close")

    async def _reader(self, c, d, reader, q):
        try:
            while True:
                async with self.cond:
                    await self.cond.wait_for(lambda: c.held[d] < HOLD_LIMIT or c.state != "open")
                data = await reader.read(CHUNK)
                if not data:
                    await q.put(None)
                    return
                c.held[d] += len(data)
                await q.put((time.monotonic(), data))
        except (OSError, ConnectionError):
            await q.put(None)

    async def _writer(self, c, d, writer, q):
        try:
            while True:
                item = await q.get()
                if item is None:
                    if writer.can_write_eof():
                        writer.write_eof()
                    return
                t_read, data = item
                while True:
                    pol = self._pol(c)
                    if pol.blackholed(d):
                        async with self.cond:
                            await self.cond.wait()
                        continue
                    due = t_read + pol.delay(d)
                    wait = due - time.monotonic()
                    if wait > 0:
                        await asyncio.sleep(min(wait, 0.05))
                        continue
                    break
                rate = self._pol(c).rate(d)
                view = memoryview(data)
                step = max(1, int(rate / 20)) if rate else len(data)
                for i in range(0, len(data), step):
                    piece = view[i:i + step]
                    writer.write(piece)
                    await writer.drain()
                    n = len(piece)
                    c.bytes[d] += n
                    c.held[d] -= n
                    self.counters["bytes_" + d] += n
                    if rate:
                        await asyncio.sleep(n / rate)
                if c.held[d] < HOLD_LIMIT:
                    async with self.cond:
                        self.cond.notify_all()
        except (OSError, ConnectionError):
            return

    # -- control socket ------------------------------------------------------------------------------------------
    async def _control_client(self, r, w):
        try:
            while True:
                line = await r.readline()
                if not line:
                    return
                try:
                    req = json.loads(line)
                    reply = {"ok": True, "result": await self._dispatch(req)}
                except Exception as exc:        # noqa: BLE001 - errors go back to the caller
                    reply = {"ok": False, "error": "%s: %s" % (type(exc).__name__, exc)}
                w.write((json.dumps(reply) + "\n").encode())
                await w.drain()
                if reply.get("result") == "stopping":
                    return
        finally:
            w.close()

    async def _dispatch(self, req):
        op = req.get("op")
        if op == "set":
            return await self._set_fault(req.get("mode", "pass"), req.get("direction", "both"), req.get("seconds"),
                                         req.get("rate"), req.get("conn"))
        if op == "policy":
            return await self._set_policy(Policy(**req.get("fields", {})), req.get("conn"))
        if op == "stats":
            return await self._stats()
        if op == "connections":
            return await self._connections()
        if op == "stop":
            self.loop.call_soon(self.loop.stop)
            return "stopping"
        raise ProxyError("unknown op %r" % (op,))


class ProxyControl:
    """Client of a proxy's control socket (from another process)."""

    def __init__(self, port=None, path=None, timeout=CALL_TIMEOUT_S):
        self.path = path or control_path(port)
        self.timeout = timeout

    def call(self, **req):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.settimeout(self.timeout)
            s.connect(self.path)
            s.sendall((json.dumps(req) + "\n").encode())
            buf = b""
            while not buf.endswith(b"\n"):
                chunk = s.recv(65536)
                if not chunk:
                    break
                buf += chunk
        reply = json.loads(buf)
        if not reply.get("ok"):
            raise ProxyError(reply.get("error"))
        return reply["result"]

    def set_fault(self, mode, direction="both", seconds=None, rate=None, conn=None):
        return self.call(op="set", mode=mode, direction=direction, seconds=seconds, rate=rate, conn=conn)

    def set_policy(self, conn=None, **fields):
        return self.call(op="policy", fields=fields, conn=conn)

    def stats(self):
        return self.call(op="stats")

    def connections(self):
        return self.call(op="connections")

    def stop(self):
        return self.call(op="stop")


def running_ports():
    """Ports in 55440-55459 whose lease is held (a proxy of some process runs there)."""
    return [p for p in range(PORT_FIRST, PORT_LAST + 1) if lease_is_held(p)]
