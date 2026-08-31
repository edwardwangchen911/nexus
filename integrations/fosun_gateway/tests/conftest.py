"""Gateway test-process environment normalization."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _remove_provider_interpreter_injection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The offline guard is already loaded; Provider code must see a clean runtime."""
    for name in ("PYTHONHOME", "PYTHONPATH", "SSLKEYLOGFILE"):
        monkeypatch.delenv(name, raising=False)
    assert all(
        name not in os.environ
        for name in ("PYTHONHOME", "PYTHONPATH", "SSLKEYLOGFILE")
    )
