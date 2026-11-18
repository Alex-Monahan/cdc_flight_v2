"""A minimal HTTP CONNECT relay on the loopback interface, for tests that break the real MotherDuck connection.

The client library is told to use a proxy through the environment of a child process (``https_proxy`` and
``grpc_proxy``). The chain in a test is

    client library  ->  cdc_v2.testing.netproxy.NetProxy (the faults)  ->  ConnectRelay (this file)  ->  MotherDuck

The relay has no fault logic of its own: it answers ``CONNECT host:port`` with ``200``, then copies bytes both ways.
TLS passes through unchanged, so the relay never sees the token or any query. It never logs the target name; it
only counts tunnels. It refuses a target on port 5432.
"""
import asyncio
import threading


class ConnectRelay:
    def __init__(self):
        self.port = None
        self.tunnels = 0                 # CONNECT requests answered with 200
        self.refused = 0
        self._loop = None
        self._server = None
        self._thread = None
        self._tasks = set()

    def start(self, timeout=10.0):
        ready = threading.Event()

        def run():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

            async def boot():
                self._server = await asyncio.start_server(self._client, "127.0.0.1", 0)
                self.port = self._server.sockets[0].getsockname()[1]
                ready.set()

            self._loop.run_until_complete(boot())
            self._loop.run_forever()
            self._loop.close()

        self._thread = threading.Thread(target=run, name="cdc_v2-test-connect-relay", daemon=True)
        self._thread.start()
        if not ready.wait(timeout):
            raise RuntimeError("the CONNECT relay did not start")
        return self

    def stop(self, timeout=10.0):
        if self._loop is None:
            return

        async def shut():
            self._server.close()
            for t in list(self._tasks):
                t.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._loop.stop()

        asyncio.run_coroutine_threadsafe(shut(), self._loop)
        self._thread.join(timeout)
        self._loop = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()

    async def _client(self, reader, writer):
        task = asyncio.current_task()
        self._tasks.add(task)
        up_writer = None
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10.0)
            parts = head.split(b"\r\n", 1)[0].split()
            if len(parts) < 2 or parts[0].upper() != b"CONNECT":
                self.refused += 1
                writer.write(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                await writer.drain()
                return
            host, _, port = parts[1].decode("ascii").rpartition(":")
            if not host or not port.isdigit() or int(port) == 5432:
                self.refused += 1
                writer.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
                await writer.drain()
                return
            up_reader, up_writer = await asyncio.wait_for(asyncio.open_connection(host, int(port)), 10.0)
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()
            self.tunnels += 1

            async def pump(src, dst):
                try:
                    while True:
                        data = await src.read(65536)
                        if not data:
                            break
                        dst.write(data)
                        await dst.drain()
                except (ConnectionError, OSError):
                    pass
                finally:
                    try:
                        dst.close()
                    except Exception:
                        pass

            await asyncio.gather(pump(reader, up_writer), pump(up_reader, writer))
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError, OSError, asyncio.CancelledError):
            pass
        finally:
            self._tasks.discard(task)
            for w in (writer, up_writer):
                if w is not None:
                    try:
                        w.close()
                    except Exception:
                        pass
