"""NetProxy.stop() with open connections leaves nothing on standard error (no "Task was destroyed but it is
pending", no unretrieved CancelledError). Hermetic: a local echo server, a proxy in the proxies' range."""
import gc
import socket
import threading
import time

from cdc_v2.testing.netproxy import NetProxy


def _echo_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    conns = []

    def serve():
        while True:
            try:
                c, _ = srv.accept()
            except OSError:
                return
            conns.append(c)

            def echo(c=c):
                try:
                    while True:
                        d = c.recv(4096)
                        if not d:
                            return
                        c.sendall(d)
                except OSError:
                    return
            threading.Thread(target=echo, daemon=True).start()
    threading.Thread(target=serve, daemon=True).start()
    return srv, conns


def test_stop_with_open_connections_is_quiet(capfd):
    srv, conns = _echo_server()
    port = srv.getsockname()[1]
    clients = []
    try:
        for _ in range(3):
            px = NetProxy("127.0.0.1", port).start()
            for _ in range(3):
                c = socket.create_connection(("127.0.0.1", px.port), timeout=5)
                c.sendall(b"hello")
                assert c.recv(5) == b"hello"
                clients.append(c)
            px.set_fault("blackhole")                      # also with bytes held in a hole
            clients[-1].sendall(b"held")
            time.sleep(0.05)
            px.stop()
        gc.collect()
        time.sleep(0.1)
    finally:
        for c in clients + conns:
            c.close()
        srv.close()
    err = capfd.readouterr().err
    assert "Task was destroyed" not in err and "never retrieved" not in err and "CancelledError" not in err, err
