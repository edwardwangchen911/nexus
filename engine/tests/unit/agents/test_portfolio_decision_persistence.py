import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nexus.agents.portfolio_decision_contracts import PortfolioResearchIntentV1
from nexus.agents.portfolio_decision_persistence import (
    _load_model,
    _load_prompt,
    build_portfolio_evidence_manifest,
    canonical_portfolio_intent_schema_hash,
)
from nexus.domain.evidence import EvidenceRef

NOW = datetime(2026, 9, 10, 8, tzinfo=UTC)


def _ref(*, source: str, available_at: datetime, content_hash: str) -> EvidenceRef:
    return EvidenceRef(
        source=source,
        object_id=f"{source}-object",
        available_at=available_at,
        location=f"postgresql:{source}",
        content_hash=content_hash,
    )


def test_manifest_is_unique_sorted_and_hashes_canonical_full_identities() -> None:
    manifest, digest = build_portfolio_evidence_manifest(
        (
            _ref(source="z-source", available_at=NOW, content_hash="a" * 64),
            _ref(source="a-source", available_at=NOW - timedelta(seconds=1), content_hash="b" * 64),
            _ref(source="z-source", available_at=NOW, content_hash="a" * 64),
        ),
        decision_time=NOW,
    )

    assert len(manifest) == 2
    assert [item["source"] for item in manifest] == ["a-source", "z-source"]
    assert len(digest) == 64


def test_manifest_rejects_future_or_unbounded_authority() -> None:
    with pytest.raises(ValueError, match="future evidence"):
        build_portfolio_evidence_manifest(
            (
                _ref(
                    source="future",
                    available_at=NOW + timedelta(seconds=1),
                    content_hash="a" * 64,
                ),
            ),
            decision_time=NOW,
        )
    with pytest.raises(ValueError, match="between 1 and 100"):
        build_portfolio_evidence_manifest((), decision_time=NOW)


def test_intent_schema_hash_binds_the_canonical_output_contract() -> None:
    assert canonical_portfolio_intent_schema_hash() == canonical_portfolio_intent_schema_hash()
    assert len(canonical_portfolio_intent_schema_hash()) == 64
    schema = PortfolioResearchIntentV1.model_json_schema(mode="serialization")
    assert schema["properties"]["execution_locked"]["const"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("version", "created_at"),
    (("wrong-version", NOW), ("portfolio-decision-v1", NOW + timedelta(seconds=1))),
)
async def test_prompt_authority_requires_exact_version_visible_at_decision_time(
    version: str, created_at: datetime
) -> None:
    content = "prompt"
    prompt = SimpleNamespace(
        role="portfolio_decision",
        version=version,
        created_at=created_at,
        content=content,
        content_hash=hashlib.sha256(content.encode()).hexdigest(),
    )
    session = SimpleNamespace(get=AsyncMock(return_value=prompt))
    with pytest.raises(ValueError, match="prompt authority is invalid"):
        await _load_prompt(session, prompt_version_id=object(), now=NOW)


@pytest.mark.asyncio
async def test_portfolio_model_requires_one_tool_call() -> None:
    model = SimpleNamespace(
        max_tool_calls=0,
        version="portfolio-model-v1",
        pricing_available_at=NOW - timedelta(days=1),
        pricing_expires_at=NOW + timedelta(days=1),
    )
    session = SimpleNamespace(get=AsyncMock(return_value=model))
    with pytest.raises(ValueError, match="model authority is invalid"):
        await _load_model(session, model_config_id=object(), now=NOW)
