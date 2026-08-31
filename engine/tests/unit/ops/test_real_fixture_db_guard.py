"""Unit contracts for the local-only real fixture database guard."""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def _guard_module() -> ModuleType:
    path = Path(__file__).resolve().parents[4] / "scripts/e2e/real_fixture_db_guard.py"
    spec = importlib.util.spec_from_file_location("nexus_real_fixture_db_guard", path)
    assert spec is not None, "real fixture database guard module is required"
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_guard_converts_only_the_validated_asyncpg_scheme() -> None:
    guard = _guard_module()

    assert guard.asyncpg_dsn(
        "postgresql+asyncpg://fixture:fixture@127.0.0.1:55432/nexus_e2e_unit"
    ) == "postgresql://fixture:fixture@127.0.0.1:55432/nexus_e2e_unit"


def test_guard_errors_are_fixed_and_do_not_render_connection_data() -> None:
    guard = _guard_module()
    canary = "postgresql+asyncpg://credential-canary@127.0.0.1/nexus_e2e_unit"

    error = guard.DatabaseGuardError("DATABASE_NOT_EMPTY")

    assert str(error) == "real fixture E2E database must be empty"
    assert canary not in str(error)


def test_guard_rejects_unvalidated_connection_targets_without_rendering_them() -> None:
    guard = _guard_module()
    unsafe_urls = [
        "postgresql://credential-canary@127.0.0.1/nexus_e2e_unit",
        "postgresql+asyncpg://credential-canary@example.test/nexus_e2e_unit",
        "postgresql+asyncpg://credential-canary@127.0.0.1/nexus_production",
        "postgresql+asyncpg://credential-canary@127.0.0.1/nexus_e2e_unit?host=example.test",
    ]

    for canary in unsafe_urls:
        try:
            guard.asyncpg_dsn(canary)
        except guard.DatabaseGuardError as error:
            assert error.code == "INVALID_DATABASE_URL"
            assert canary not in str(error)
        else:
            raise AssertionError("unvalidated database target was accepted")
