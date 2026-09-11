import hashlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.agents.portfolio_decision_contracts import PortfolioResearchIntentV1
from nexus.agents.portfolio_decision_persistence import (
    _load_model,
    _load_prompt,
    _select_scenario_claim,
    build_portfolio_evidence_manifest,
    canonical_portfolio_intent_schema_hash,
)
from nexus.agents.tools import ToolInputError
from nexus.domain.evidence import EvidenceRef
from nexus.optimizer.scenario_spread_cost import ScenarioSpreadEvidenceManifestV1

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
        await _load_model(
            session,
            model_config_id=object(),
            now=NOW,
            required_model_id=None,
            required_model_version=None,
        )


@pytest.mark.asyncio
async def test_scenario_claim_selection_skips_invalid_and_freezes_first_valid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = SimpleNamespace(id=UUID("018f0000-0000-7000-8000-000000000001"))
    second = SimpleNamespace(id=UUID("018f0000-0000-7000-8000-000000000002"))
    third = SimpleNamespace(id=UUID("018f0000-0000-7000-8000-000000000003"))
    manifest = _scenario_manifest()

    class _Session:
        async def scalars(self, statement):
            return (first, second, third)

    replay = AsyncMock(
        side_effect=(
            ToolInputError("invalid first claim"),
            (manifest, object()),
        )
    )
    monkeypatch.setattr(
        "nexus.agents.portfolio_decision_persistence.replay_valid_scenario_claim",
        replay,
    )

    selected, selected_manifest = await _select_scenario_claim(
        _Session(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        actor_id="fixture-actor",
        expected_result_hash="e" * 64,
        snapshot=object(),  # type: ignore[arg-type]
        base_result=object(),  # type: ignore[arg-type]
        now=NOW,
    )

    assert selected is second
    assert selected_manifest == manifest
    assert [call.kwargs["claim"] for call in replay.await_args_list] == [first, second]


def _scenario_manifest() -> ScenarioSpreadEvidenceManifestV1:
    return ScenarioSpreadEvidenceManifestV1(
        manifest_schema="scenario_spread_evidence_manifest_v1",
        evidence_selected_at=NOW,
        policy_status="unavailable",
        source_policy_id=None,
        source_policy_version=None,
        source_policy_payload_hash=None,
        calendar_authority=None,
        calculation_version="scenario_spread_cost_v1",
        no_cost_candidate_hash="1" * 64,
        scenario_cost_basis_hash="2" * 64,
        regulatory_reference_hash=None,
        selected_quotes=(),
        expected_result_hash="e" * 64,
    )
