"""A historical prompt publication cannot silently change its frozen model."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.paper.agent_challenger import PortfolioChallengerPublicationV1
from nexus.paper.economic_authorities import inspect_pair
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord

NOW = datetime(2026, 10, 8, 16, tzinfo=UTC)


def sources():
    model = AgentModelConfigRecord(
        id=UUID(int=1),
        model_id="gpt-5.6-terra",
        version="codex-sdk-chatgpt-v6-stage-time-budget",
        config_hash="a" * 64,
        max_input_tokens=96000,
        max_output_tokens=8192,
        max_tool_calls=0,
        input_usd_per_million=Decimal(0),
        output_usd_per_million=Decimal(0),
        pricing_available_at=NOW - timedelta(days=1),
        pricing_expires_at=NOW + timedelta(days=1),
    )
    incumbent = AgentPromptVersionRecord(
        id=UUID(int=2),
        role="portfolio_decision",
        version="portfolio-management-synthesis-v1",
        content="incumbent",
        release_channel="active",
        content_hash=sha256(b"incumbent").hexdigest(),
    )
    challenger = AgentPromptVersionRecord(
        id=UUID(int=3),
        role="portfolio_decision",
        version="portfolio-challenger-test",
        content="challenger",
        release_channel="challenger",
        content_hash=sha256(b"challenger").hexdigest(),
    )
    publication = PortfolioChallengerPublicationV1(
        schema_version="portfolio_challenger_pub_v1",
        proposal_hash="b" * 64,
        incumbent_prompt_version_id=incumbent.id,
        challenger_prompt_version_id=challenger.id,
        challenger_prompt_hash=challenger.content_hash,
        challenger_model_config_id=model.id,
        replayed=False,
    )
    return model, incumbent, challenger, publication


def test_matching_pair_is_only_observation_not_activation():
    model, incumbent, challenger, publication = sources()
    result = inspect_pair(model, incumbent, challenger, publication, published_model=model, now=NOW)
    assert result.reason_code == "VERSIONS_MATCH_AT_OBSERVATION"
    assert result.activation_permitted is False
    assert result.economic_status == "not_evaluated"
    assert result.configured_research_cost_usd == Decimal(0)


def test_old_publication_model_is_not_rebound_to_current_model():
    model, incumbent, challenger, publication = sources()
    old, *_ = sources()
    old.id = UUID(int=4)
    old.version = "codex-sdk-chatgpt-v4-actionable"
    old.config_hash = "c" * 64
    publication = publication.model_copy(update={"challenger_model_config_id": old.id})
    result = inspect_pair(model, incumbent, challenger, publication, published_model=old, now=NOW)
    assert result.reason_code == "MODEL_REVISION_MISMATCH"
    assert old.version == "codex-sdk-chatgpt-v4-actionable"
    assert result.activation_permitted is False


@pytest.mark.parametrize(
    "change,code",
    [
        ("expired", "MODEL_PRICING_UNAVAILABLE"),
        ("future", "MODEL_PRICING_UNAVAILABLE"),
        ("caps", "MODEL_SPEC_UNSUPPORTED"),
        ("channel", "PROMPT_PAIR_INVALID"),
        ("hash", "PROMPT_PAIR_INVALID"),
        ("identity", "PROMPT_PAIR_INVALID"),
    ],
)
def test_missing_or_changed_authority_stays_blocked(change, code):
    model, incumbent, challenger, publication = sources()
    if change == "expired":
        model.pricing_expires_at = NOW
    if change == "future":
        model.pricing_available_at = NOW + timedelta(seconds=1)
    if change == "caps":
        model.max_tool_calls = 1
    if change == "channel":
        challenger.release_channel = "active"
    if change == "hash":
        challenger.content = "changed"
    if change == "identity":
        incumbent.id = UUID(int=9)
    assert (
        inspect_pair(
            model, incumbent, challenger, publication, published_model=model, now=NOW
        ).reason_code
        == code
    )


def test_absent_publication_remains_explicit():
    model, incumbent, challenger, _ = sources()
    assert (
        inspect_pair(model, incumbent, challenger, None, published_model=None, now=NOW).reason_code
        == "PUBLICATION_UNAVAILABLE"
    )
