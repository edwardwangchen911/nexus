from uuid import UUID

import pytest
from nexus.ops.agent_paper_formal import require_formal_host_configuration_match
from nexus.settings import EngineSettings


@pytest.mark.parametrize(
    "change",
    [
        {"agent_paper_formal_model_config_id": None},
        {"agent_paper_formal_model_config_id": UUID(int=2)},
        {"database_url": "postgresql+asyncpg://u:p@127.0.0.1/other"},
        {"agent_runtime_mode": "fixture"},
    ],
)
def test_formal_host_rejects_reloaded_model_database_or_runtime_drift(change):
    frozen = EngineSettings(
        database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus_test_host",
        agent_runtime_mode="codex_sdk",
        NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID=str(UUID(int=1)),
    )
    assert frozen.agent_paper_formal_model_config_id == UUID(int=1)
    loaded = EngineSettings.model_validate(frozen.model_dump() | change)
    with pytest.raises(ValueError, match="CODEX_FORMAL_HOST_PROFILE_INVALID"):
        require_formal_host_configuration_match(frozen, loaded)


def test_formal_host_rejects_initial_runtime_mismatch_even_with_valid_reload():
    loaded = EngineSettings(
        database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus_test_host",
        agent_runtime_mode="codex_sdk",
        NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID=str(UUID(int=1)),
    )
    original = EngineSettings.model_validate(
        loaded.model_dump() | {"agent_runtime_mode": "fixture"}
    )
    with pytest.raises(ValueError, match="CODEX_FORMAL_HOST_PROFILE_INVALID"):
        require_formal_host_configuration_match(original, loaded)
