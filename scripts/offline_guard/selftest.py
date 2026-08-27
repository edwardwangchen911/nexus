"""Executable positive and negative proof for the provider-test network guard."""

from __future__ import annotations

import socket

from nexus_offline_guard import OfflineNetworkError, guard_is_active


def main() -> int:
    if not guard_is_active():
        raise RuntimeError("offline socket guard is not active")
    _prove_loopback_connection()
    _prove_external_connection_is_blocked()
    _prove_external_dns_is_blocked()
    print("offline socket guard self-test: PASS")
    return 0


def _prove_loopback_connection() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(server.getsockname())
            connection, _ = server.accept()
            with connection:
                client.sendall(b"loopback")
                if connection.recv(16) != b"loopback":
                    raise RuntimeError("offline loopback self-test failed")


def _prove_external_connection_is_blocked() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        try:
            client.connect(("203.0.113.1", 443))
        except OfflineNetworkError:
            return
    raise RuntimeError("external socket guard self-test failed")


def _prove_external_dns_is_blocked() -> None:
    try:
        socket.getaddrinfo("provider.example", 443)
    except OfflineNetworkError:
        return
    raise RuntimeError("external DNS guard self-test failed")


if __name__ == "__main__":
    raise SystemExit(main())
