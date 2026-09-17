"""Behavioral contracts for bounded Agent challenger proposals."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.agents.committee import ROLE_ORDER
from nexus.domain.evidence import EvidenceRef
from nexus.paper.agent_challenger import (
    PROMPT_CONTRACT_SUFFIX,
    AgentChallengerCycleFactV4,
    AgentChallengerReviewFactV4,
    AgentChallengerSuggestionV4,
    freeze_agent_challenger_proposal_v4,
    freeze_agent_challenger_retrospective_v4,
    verify_agent_challenger_retrospective_v4,
)
from nexus.paper.agent_challenger_runtime import compact_agent_challenger_retrospective
from pydantic import ValidationError

INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000009001")
PROMPT_ID = UUID("0198d500-1000-7000-8000-000000009002")
MODEL_ID = UUID("0198d500-1000-7000-8000-000000009003")
NOW = datetime(2026, 9, 17, 12, tzinfo=UTC)


def _reviews() -> tuple[AgentChallengerReviewFactV4, ...]:
    evidence = EvidenceRef(
        source="test",
        object_id="evidence-1",
        available_at=NOW,
        location="fixture",
        content_hash="e" * 64,
    )
    return (
        AgentChallengerReviewFactV4(
            claim_id="bear:0",
            status="unresolved",
            rationale="The claim lacks current catalyst evidence.",
            evidence_refs=(evidence,),
            resolved=False,
            missing_evidence=("Current catalyst evidence",),
            prevented_conclusion="Cannot distinguish watch from a buy candidate.",
        ),
        AgentChallengerReviewFactV4(
            claim_id="evidence_audit:0",
            status="unresolved",
            rationale="The independent audit found the same gap for a different claim.",
            evidence_refs=(evidence,),
            resolved=False,
            missing_evidence=("Current catalyst evidence",),
            prevented_conclusion="Cannot mark the evidence audit complete.",
        ),
    )


def _retrospective():  # type: ignore[no-untyped-def]
    portfolio_prompt = "Default to watch when authority is incomplete."
    facts = tuple(
        AgentChallengerCycleFactV4(
            cycle_id=UUID(f"0198d500-1000-7000-8000-{9100 + index:012d}"),
            decision_slot_at=NOW - timedelta(minutes=5 * (4 - index)),
            result_code="NO_ORDER_REQUIRED",
            strategy_decision_id=UUID(f"0198d500-1000-7000-8000-{9200 + index:012d}"),
            decision_hash=f"{index + 1:064x}",
            sizing_actions=("none",),
            research_run_ids=(UUID(f"0198d500-1000-7000-8000-{9300 + index:012d}"),),
            settled_cost_usd="0",
            stance="watch",
            exposure_action="none",
            quality_status="partial",
            evidence_audit="partial",
            uncertainties=("Authoritative catalyst evidence is missing.",),
            disagreement="Bull and bear evidence remain unresolved.",
            role_votes=("portfolio:abstain",),
            reviews=_reviews(),
            invalidation_signals=(),
            entry_signals=(),
            exit_signals=(),
        )
        for index in range(4)
    )
    manifest = {
        role: {
            "id": str(PROMPT_ID if index == 0 else UUID(int=PROMPT_ID.int + index)),
            "version": "incumbent-v1",
            "content_hash": f"{index + 1:064x}",
        }
        for index, role in enumerate(ROLE_ORDER)
    }
    manifest["portfolio"]["content_hash"] = hashlib.sha256(portfolio_prompt.encode()).hexdigest()
    prompt_hash = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return freeze_agent_challenger_retrospective_v4(
        instrument_id=INSTRUMENT_ID,
        as_of=NOW,
        incumbent_prompt_manifest=manifest,
        incumbent_prompt_hash=prompt_hash,
        incumbent_portfolio_prompt=portfolio_prompt,
        output_contract={"type": "object", "properties": {"uncertainties": {"type": "array"}}},
        incumbent_model_config_id=MODEL_ID,
        incumbent_model_hash="b" * 64,
        cycles=facts,
    )


def test_freeze_proposal_binds_exact_history_and_hard_boundaries() -> None:
    retrospective = _retrospective()
    verify_agent_challenger_retrospective_v4(retrospective)
    suggestion = AgentChallengerSuggestionV4(
        source_hash=retrospective.source_hash,
        observed_pattern="Four completed cycles all ended with watch/none.",
        hypothesis="Require an explicit evidence-gap diagnosis before repeating watch.",
        target_role="portfolio",
        prompt_addendum=(
            "When the prior completed cycles repeat watch/none, identify the exact "
            "missing evidence and state what observation would change the decision."
        ),
        expected_behavior_changes=("More specific abstention diagnostics",),
        invalidation_conditions=("No measurable decision-output difference",),
        execution_locked=True,
        fixed_aapl_scope=True,
        deterministic_risk_unchanged=True,
        broker_writes_forbidden=True,
        output_schema_unchanged=True,
    )

    first = freeze_agent_challenger_proposal_v4(retrospective, suggestion)
    second = freeze_agent_challenger_proposal_v4(retrospective, suggestion)

    assert first == second
    assert first.proposal_hash != retrospective.source_hash
    assert first.incumbent_prompt_manifest["orchestrator"].id == PROMPT_ID
    assert first.target_role == "portfolio"
    assert first.incumbent_model_config_id == MODEL_ID
    assert first.execution_locked is True
    assert first.broker_writes_forbidden is True
    assert first.prompt_addendum.endswith(PROMPT_CONTRACT_SUFFIX)
    assert len(retrospective.cycles[0].reviews) == 2
    assert retrospective.cycles[0].reviews[0].claim_id == "bear:0"
    assert retrospective.cycles[0].reviews[1].claim_id == "evidence_audit:0"


def test_freeze_proposal_rejects_a_suggestion_for_different_history() -> None:
    retrospective = _retrospective()
    suggestion = AgentChallengerSuggestionV4(
        source_hash="f" * 64,
        observed_pattern="Different source.",
        hypothesis="Different source.",
        target_role="portfolio",
        prompt_addendum="Explain the exact missing evidence before abstaining.",
        expected_behavior_changes=("Different diagnostics",),
        invalidation_conditions=("No difference",),
        execution_locked=True,
        fixed_aapl_scope=True,
        deterministic_risk_unchanged=True,
        broker_writes_forbidden=True,
        output_schema_unchanged=True,
    )

    with pytest.raises(ValueError, match="AGENT_CHALLENGER_SOURCE_MISMATCH"):
        freeze_agent_challenger_proposal_v4(retrospective, suggestion)


def test_provider_projection_catalogs_text_without_merging_reviews() -> None:
    compact = compact_agent_challenger_retrospective(_retrospective())

    assert len(compact["text_catalog"]) == 4  # type: ignore[arg-type]
    cycles = compact["cycles"]
    assert isinstance(cycles, list)
    reviews = cycles[0][8]
    assert len(reviews) == 2
    assert reviews[0][0] == "bear:0"
    assert reviews[1][0] == "evidence_audit:0"
    assert reviews[0][4] == reviews[1][4]
    assert len(reviews[0][3]) == 64


def test_suggestion_cannot_relax_execution_boundary() -> None:
    retrospective = _retrospective()
    with pytest.raises(ValidationError):
        AgentChallengerSuggestionV4(
            source_hash=retrospective.source_hash,
            observed_pattern="Repeated watch.",
            hypothesis="Change decision explanation.",
            target_role="portfolio",
            prompt_addendum="Explain missing evidence.",
            expected_behavior_changes=("Clearer explanation",),
            invalidation_conditions=("No difference",),
            execution_locked=False,
            fixed_aapl_scope=True,
            deterministic_risk_unchanged=True,
            broker_writes_forbidden=True,
            output_schema_unchanged=True,
        )
