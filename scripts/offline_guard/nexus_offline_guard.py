"""Deny non-loopback DNS and socket connections in offline provider tests."""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable
from typing import Any


class OfflineNetworkError(ConnectionRefusedError):
    """Raised before a provider test can reach a non-loopback network target."""


_OriginalConnect = Callable[[socket.socket, Any], None]
_OriginalConnectEx = Callable[[socket.socket, Any], int]
_OriginalGetAddrInfo = Callable[..., list[tuple[Any, ...]]]

_original_connect: _OriginalConnect = socket.socket.connect
_original_connect_ex: _OriginalConnectEx = socket.socket.connect_ex
_original_getaddrinfo: _OriginalGetAddrInfo = socket.getaddrinfo
_active = False


def install_socket_guard() -> None:
    """Install the process-wide guard once."""
    global _active
    if _active:
        return
    socket.socket.connect = _guarded_connect
    socket.socket.connect_ex = _guarded_connect_ex
    socket.getaddrinfo = _guarded_getaddrinfo
    _active = True


def restore_socket_guard() -> None:
    """Restore stdlib socket functions for isolated unit tests."""
    global _active
    if not _active:
        return
    socket.socket.connect = _original_connect
    socket.socket.connect_ex = _original_connect_ex
    socket.getaddrinfo = _original_getaddrinfo
    _active = False


def guard_is_active() -> bool:
    return _active


def _guarded_connect(client: socket.socket, address: Any) -> None:
    _assert_loopback_address(address)
    _original_connect(client, address)


def _guarded_connect_ex(client: socket.socket, address: Any) -> int:
    _assert_loopback_address(address)
    return _original_connect_ex(client, address)


def _guarded_getaddrinfo(
    host: str | bytes | None,
    port: str | int | None,
    family: int = 0,
    type: int = 0,
    proto: int = 0,
    flags: int = 0,
) -> list[tuple[Any, ...]]:
    if host is not None and not _is_loopback_host(host):
        raise OfflineNetworkError("offline socket guard denied non-loopback DNS")
    return _original_getaddrinfo(host, port, family, type, proto, flags)


def _assert_loopback_address(address: Any) -> None:
    if isinstance(address, str | bytes):
        return  # Unix-domain socket path.
    if (
        not isinstance(address, tuple)
        or not address
        or not _is_loopback_host(address[0])
    ):
        raise OfflineNetworkError("offline socket guard denied non-loopback connection")


def _is_loopback_host(host: object) -> bool:
    if isinstance(host, bytes):
        try:
            host = host.decode("ascii")
        except UnicodeDecodeError:
            return False
    if not isinstance(host, str):
        return False
    normalized = host.rstrip(".").lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized.split("%", 1)[0]).is_loopback
    except ValueError:
        return False
