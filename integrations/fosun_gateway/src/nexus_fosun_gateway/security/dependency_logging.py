"""Fail-closed logging boundary for the frozen Fosun SDK transport stack."""

from __future__ import annotations

import http.client
import logging
from threading import RLock

_SDK_DEPENDENCY_NAMESPACES = (
    "fsopenapi",
    "requests",
    "urllib3",
    "http.client",
)
_SDK_DEPENDENCY_LOG_LOCK = RLock()


class _DropSdkDependencyRecord(logging.Filter):
    """Reject records that may format provider URLs, headers, or payloads."""

    def filter(self, record: logging.LogRecord) -> bool:
        del record
        return False


_SDK_DEPENDENCY_FILTER = _DropSdkDependencyRecord()


def enforce_sdk_dependency_log_boundary() -> None:
    """Disable SDK/dependency diagnostics before construction and every wire call."""
    with _SDK_DEPENDENCY_LOG_LOCK:
        # http.client's wire debug bypasses logging and writes headers/body to
        # stdout. Reset both classes before touching any SDK/session surface.
        http.client.HTTPConnection.debuglevel = 0
        http.client.HTTPSConnection.debuglevel = 0

        names = set(_SDK_DEPENDENCY_NAMESPACES)
        names.update(
            name
            for name in logging.Logger.manager.loggerDict
            if isinstance(name, str)
            and any(
                name.startswith(f"{namespace}.")
                for namespace in _SDK_DEPENDENCY_NAMESPACES
            )
        )
        for name in names:
            dependency_logger = logging.getLogger(name)
            dependency_logger.disabled = True
            dependency_logger.setLevel(logging.CRITICAL + 1)
            dependency_logger.handlers.clear()
            dependency_logger.addHandler(logging.NullHandler())
            dependency_logger.propagate = False
            if _SDK_DEPENDENCY_FILTER not in dependency_logger.filters:
                dependency_logger.addFilter(_SDK_DEPENDENCY_FILTER)
