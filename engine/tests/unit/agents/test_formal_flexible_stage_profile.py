from datetime import datetime
from types import SimpleNamespace

from nexus.agents.codex_flexible_budget import flexible_stage_config, flexible_stage_config_hash
from nexus.agents.codex_formal_stage import formal_preparation_model_matches


def test_formal_v10_requires_exact_frozen_hash_and_keeps_diagnostic_closed():
    config = flexible_stage_config()
    model = SimpleNamespace(
        **{
            k: config[k]
            for k in (
                "model_id",
                "version",
                "max_input_tokens",
                "max_output_tokens",
                "max_tool_calls",
            )
        },
        config_hash=flexible_stage_config_hash(),
        input_usd_per_million=0,
        output_usd_per_million=0,
        pricing_available_at=datetime.fromisoformat(config["pricing_available_at"]),
        pricing_expires_at=datetime.fromisoformat(config["pricing_expires_at"]),
    )
    assert formal_preparation_model_matches(model)
    model.config_hash = "f" * 64
    assert not formal_preparation_model_matches(model)
    model.config_hash = flexible_stage_config_hash()
    model.version = "full-input-timing-diagnostic-v1"
    assert not formal_preparation_model_matches(model)


async def _formal_v10_replay():
    from datetime import UTC
    from unittest.mock import AsyncMock

    from nexus.agents.persistence import _verify_capacity_replay_binding

    run = SimpleNamespace(pricing_version="codex-sdk-chatgpt-v10-flexible-stage-budget")
    await _verify_capacity_replay_binding(
        AsyncMock(),
        run,
        False,
        None,
        datetime(2026, 10, 14, tzinfo=UTC),
        formal_stage_capacity=True,
    )


def test_formal_v10_replay_uses_formal_proof_not_isolated_activation():
    import asyncio

    asyncio.run(_formal_v10_replay())
