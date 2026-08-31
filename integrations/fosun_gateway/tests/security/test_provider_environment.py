from __future__ import annotations

import pytest
from nexus_fosun_gateway.security.provider_environment import (
    UnsafeProviderEnvironmentError,
    reject_unsafe_provider_environment,
)


def test_provider_environment_accepts_only_absence_of_injection_variables() -> None:
    reject_unsafe_provider_environment({"PATH": "/approved/runtime/bin"})


@pytest.mark.parametrize(
    "name",
    ["PYTHONHOME", "PYTHONPATH", "SSLKEYLOGFILE"],
)
def test_provider_environment_rejects_even_empty_injection_variables(name: str) -> None:
    with pytest.raises(
        UnsafeProviderEnvironmentError,
        match="unsafe Provider process environment",
    ):
        reject_unsafe_provider_environment({name: ""})
