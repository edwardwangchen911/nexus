"""Protected financial configuration must not expand shell interpolation."""

import subprocess
from uuid import UUID

import pytest
from nexus.ops.formal_budget_inventory import configuration
from sqlalchemy.exc import ArgumentError


@pytest.mark.parametrize(
    ("field", "ambient"),
    [
        ("DATABASE_URL", "postgresql+asyncpg://fixture:fixture@127.0.0.1:1/wrong_target"),
        ("AGENT_DAILY_BUDGET_USD", "999"),
        ("NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED", "false"),
    ],
)
def test_protected_file_rejects_shell_interpolated_authority(tmp_path, monkeypatch, field, ambient):
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True, capture_output=True)
    (tmp_path / ".gitignore").write_text("infra/.env.local\n")
    (tmp_path / "infra").mkdir()
    values = {
        "NEXUS_ENV": "test",
        "DATABASE_URL": "postgresql+asyncpg://fixture:fixture@127.0.0.1:1/file_target",
        "AGENT_DAILY_BUDGET_USD": "0.0008",
        "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED": "true",
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(UUID(int=1)),
        "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(UUID(int=2)),
    }
    values[field] = "${NEXUS_TEST_SHELL_VALUE}"
    path = tmp_path / "infra/.env.local"
    path.write_text("\n".join(f"{key}={value}" for key, value in values.items()) + "\n")
    path.chmod(0o600)
    monkeypatch.setenv("NEXUS_TEST_SHELL_VALUE", ambient)
    for key in values:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises((ValueError, ArgumentError)):
        configuration(tmp_path)


def _protected_model_fixture(tmp_path):
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True, capture_output=True)
    (tmp_path / ".gitignore").write_text("infra/.env.local\n")
    (tmp_path / "infra").mkdir()
    path = tmp_path / "infra/.env.local"
    base = (
        "\n".join(
            [
                "NEXUS_ENV=test",
                "DATABASE_URL=postgresql+asyncpg://fixture:fixture@127.0.0.1:1/file_target",
                "AGENT_DAILY_BUDGET_USD=0.0008",
                "NEXUS_AGENT_PAPER_CYCLE_ENABLED=true",
                f"NEXUS_AGENT_PAPER_ACCOUNT_ID={UUID(int=1)}",
                f"NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID={UUID(int=2)}",
            ]
        )
        + "\n"
    )
    path.write_text(base)
    path.chmod(0o600)
    return path, base


def test_formal_model_change_invalidates_configuration_fingerprint(tmp_path, monkeypatch):
    path, base = _protected_model_fixture(tmp_path)
    monkeypatch.setenv("NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID", str(UUID(int=999)))
    legacy = configuration(tmp_path)[-1]
    first = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    path.write_text(base + f"NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID={first}\n")
    bound = configuration(tmp_path)[-1]
    assert bound != legacy
    path.write_text(base + f"NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID={str(first).upper()}\n")
    assert configuration(tmp_path)[-1] == bound
    path.write_text(base + f"NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID={UUID(int=3)}\n")
    assert configuration(tmp_path)[-1] not in {legacy, bound}
    path.write_text(base)
    assert configuration(tmp_path)[-1] == legacy


@pytest.mark.parametrize("invalid", ["not-a-uuid", "${NEXUS_TEST_SHELL_VALUE}"])
def test_formal_model_identifier_is_validated_without_shell_expansion(
    tmp_path, monkeypatch, invalid
):
    path, base = _protected_model_fixture(tmp_path)
    monkeypatch.setenv("NEXUS_TEST_SHELL_VALUE", str(UUID(int=3)))
    path.write_text(base + f"NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID={invalid}\n")
    with pytest.raises(ValueError, match="FORMAL_BUDGET_INPUT_INVALID"):
        configuration(tmp_path)
