"""Fail closed on interpreter and TLS environment injection for Provider runtimes."""

from __future__ import annotations

import os
from collections.abc import Mapping

_UNSAFE_PROVIDER_ENVIRONMENT_NAMES = (
    "PYTHONHOME",
    "PYTHONPATH",
    "SSLKEYLOGFILE",
)


class UnsafeProviderEnvironmentError(Exception):
    """The Provider process was launched with an injection-capable environment."""


def reject_unsafe_provider_environment(
    environment: Mapping[str, str] | None = None,
) -> None:
    """Reject variables that can shadow the admitted SDK or export TLS secrets."""
    values = os.environ if environment is None else environment
    if any(name in values for name in _UNSAFE_PROVIDER_ENVIRONMENT_NAMES):
        raise UnsafeProviderEnvironmentError("unsafe Provider process environment")
