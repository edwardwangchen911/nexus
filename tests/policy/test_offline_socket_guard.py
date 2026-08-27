"""Offline socket guard contracts used by provider CI."""

from __future__ import annotations

import socket

import pytest

from scripts.offline_guard.nexus_offline_guard import (
    OfflineNetworkError,
    install_socket_guard,
    restore_socket_guard,
)


def test_guard_denies_non_loopback_connect_and_dns_before_the_syscall() -> None:
    install_socket_guard()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            with pytest.raises(OfflineNetworkError, match="non-loopback"):
                client.connect(("203.0.113.1", 443))
        with pytest.raises(OfflineNetworkError, match="non-loopback"):
            socket.getaddrinfo("provider.example", 443)
    finally:
        restore_socket_guard()


def test_guard_allows_real_ipv4_and_ipv6_loopback_connections() -> None:
    install_socket_guard()
    try:
        _round_trip("127.0.0.1", socket.AF_INET)
        if socket.has_ipv6:
            try:
                _round_trip("::1", socket.AF_INET6)
            except OSError as error:
                pytest.skip(f"IPv6 loopback unavailable: {error}")
    finally:
        restore_socket_guard()


def test_guard_installation_is_idempotent_and_restorable() -> None:
    original_connect = socket.socket.connect

    install_socket_guard()
    guarded_connect = socket.socket.connect
    install_socket_guard()

    assert guarded_connect is not original_connect
    assert socket.socket.connect is guarded_connect

    restore_socket_guard()
    assert socket.socket.connect is original_connect


def _round_trip(host: str, family: socket.AddressFamily) -> None:
    with socket.socket(family, socket.SOCK_STREAM) as server:
        server.bind((host, 0))
        server.listen(1)
        with socket.socket(family, socket.SOCK_STREAM) as client:
            client.connect(server.getsockname())
            connection, _ = server.accept()
            with connection:
                client.sendall(b"nexus-offline")
                assert connection.recv(32) == b"nexus-offline"
