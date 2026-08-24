"""Fail-closed Agent runtime mode and official Responses transport tests."""

from collections.abc import Mapping
from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.agents.models import ResearchRequest
from nexus.agents.responses import ResponsesResearchModel
from nexus.agents.tools import ReadOnlyTools
from nexus.settings import EngineSettings, Environment
from pydantic import ValidationError


def test_non_fixture_runtime_requires_key_and_both_configured_model_ids() -> None:
    """A missing production model configuration must fail startup, not silently use a default."""
    with pytest.raises(ValidationError, match="Agent non-fixture mode requires"):
        EngineSettings(
            environment=Environment.PRODUCTION,
            database_url="postgresql+asyncpg://fixture",
            agent_runtime_mode="responses",
        )


@pytest.mark.asyncio
async def test_responses_adapter_uses_strict_schema_and_configured_model_with_fake_sdk() -> None:
    """A literal model or unconstrained output would violate the transport boundary."""
    fake = _FakeClient()
    model = ResponsesResearchModel(client=fake, model_id="configured-reasoning", timeout_seconds=12)
    request = ResearchRequest(
        instrument_id=UUID("0198d500-1000-7000-8000-000000000112"),
        data_snapshot_id=UUID("0198d500-1000-7000-8000-000000000113"),
        decision_time=datetime(2026, 8, 24, tzinfo=UTC),
    )
    result = await model.respond(request, ReadOnlyTools())
    assert result["cost_usd"] == "0.250"
    assert fake.kwargs["model"] == "configured-reasoning"
    assert fake.kwargs["timeout"] == 12
    assert fake.kwargs["text"]["format"]["strict"] is True
    assert fake.kwargs["tools"] and fake.kwargs["tools"][0]["type"] == "function"


class _FakeResponses:
    def __init__(self) -> None:
        self.kwargs: Mapping[str, object] = {}

    async def create(self, **kwargs: object) -> Mapping[str, object]:
        self.kwargs = kwargs
        return {"output_text": "{}", "usage": {"input_tokens": 100, "output_tokens": 50}}


class _FakeClient:
    def __init__(self) -> None:
        self.responses = _FakeResponses()

    @property
    def kwargs(self) -> Mapping[str, object]:
        return self.responses.kwargs
