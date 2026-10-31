"""Durable fixture execution must append the full Agent lifecycle."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.agents.actionable_research import safe_no_trade_intent_shape_valid
from nexus.agents.committee import (
    ROLE_ORDER,
    CommitteeRoleOutput,
    FixtureCommittee,
    load_prompt_assets,
)
from nexus.agents.execution import LIFECYCLE
from nexus.agents.models import ResearchRequest
from nexus.agents.synthesis import synthesize_intent
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.strategy.intents import ResearchClaim


def test_execution_lifecycle_is_exact_and_append_only() -> None:
    """Skipping an audit phase would make completed research unauditable."""
    assert LIFECYCLE == (
        "prepared",
        "running",
        "validated",
        "audited",
        "synthesized",
        "completed",
    )


def test_versioned_prompt_assets_drive_all_eight_fixture_role_outputs() -> None:
    """A missing role prompt or ignored prompt hash would make the committee unauditable."""
    prompts = load_prompt_assets()
    assert (
        tuple(prompts)
        == ROLE_ORDER
        == (
            "orchestrator",
            "fundamental",
            "market_state",
            "macro_sector",
            "bull",
            "bear",
            "evidence_audit",
            "portfolio",
        )
    )
    request = ResearchRequest(
        instrument_id=UUID("0198d500-1000-7000-8000-000000000112"),
        data_snapshot_id=UUID("0198d500-1000-7000-8000-000000000113"),
        decision_time=datetime(2026, 8, 24, tzinfo=UTC),
    )
    evidence = (
        EvidenceRef(
            source="fixture",
            object_id="evidence-1",
            available_at=datetime(2026, 8, 23, tzinfo=UTC),
            location="memory:fixture-evidence-1",
            content_hash="a" * 64,
        ),
    )

    async def execute_twice():
        committee = FixtureCommittee()
        results = []
        for _ in range(2):
            results.append(
                [
                    await committee.analyze(role, request, evidence, prompts[role])
                    for role in ROLE_ORDER
                ]
            )
        return tuple(results)

    first, second = asyncio.run(execute_twice())
    assert tuple(output.role for output in first) == ROLE_ORDER
    assert all(output.prompt_hash == prompts[output.role].content_hash for output in first)
    assert [output.model_dump(mode="json") for output in first] == [
        output.model_dump(mode="json") for output in second
    ]


def test_synthesis_uses_all_role_votes_for_nonnumeric_stance_and_paired_evidence() -> None:
    """Returning watch from every committee would erase both candidate and risk outcomes."""
    expected = (
        (
            ("support", "support", "support", "support", "support", "oppose", "abstain", "abstain"),
            ("buy_candidate", "eligible_for_increase"),
        ),
        (
            ("support", "support", "abstain", "abstain", "oppose", "oppose", "abstain", "abstain"),
            ("hold", "maintain"),
        ),
        (
            ("support", "abstain", "abstain", "oppose", "oppose", "oppose", "oppose", "oppose"),
            ("reduce_candidate", "reduce"),
        ),
        (
            ("abstain",) * 8,
            ("watch", "none"),
        ),
    )
    for votes, decision in expected:
        intent = _synthesize(votes)
        assert (intent.stance, intent.exposure_action) == decision
        assert tuple(vote.role for vote in intent.role_votes) == ROLE_ORDER
        assert intent.theses
        assert intent.counter_theses
        assert intent.invalidation_conditions
        assert intent.entry_conditions
        assert intent.exit_conditions


def _synthesize(
    votes: tuple[str, ...],
    *,
    include_price_authority: bool = True,
    price_authority_valid: bool | None = None,
    staged: bool = False,
    structured: bool = False,
    actionable: bool = False,
    assessment_stance: str = "buy_candidate",
    portfolio_context=None,
):
    now = datetime(2026, 8, 24, tzinfo=UTC)
    content_hash = "a" * 64
    object_id = (
        "price-bar-set:018f75b4-4d86-7c36-bc4c-cd71f2f64521"
        if include_price_authority
        else "filing:0000000001-26-000001"
    )
    location = (
        f"db://price-bar-set/018f75b4-4d86-7c36-bc4c-cd71f2f64521@sha256:{content_hash}"
        if include_price_authority
        else f"memory://sha256/{content_hash}"
    )
    instrument_id = UUID("0198d500-1000-7000-8000-000000000112")
    snapshot_id = UUID("0198d500-1000-7000-8000-000000000113")
    evidence = EvidenceRef(
        source="fixture" if include_price_authority else "sec",
        object_id=object_id,
        available_at=now,
        location=location,
        content_hash=content_hash,
    )
    from nexus.strategy.intents import SynthesisAssessment, SynthesisClaimReview

    structured = structured or actionable
    assessment = (
        SynthesisAssessment(
            stance=assessment_stance,
            claims=(
                ResearchClaim(
                    text="模型认为充分，但不得覆盖确定性准入。", evidence_refs=(evidence,)
                ),
            ),
            reviews=tuple(
                SynthesisClaimReview(
                    claim_id=f"{role}:0",
                    status="accepted",
                    rationale="已逐条回应。",
                    evidence_refs=(evidence,),
                    resolved=True,
                    missing_evidence=(),
                    prevented_conclusion=None,
                )
                for role in ("bear", "evidence_audit")
            ),
            review_conclusion="sufficient",
            uncertainties=(),
        )
        if structured
        else None
    )
    outputs = tuple(
        CommitteeRoleOutput(
            role=role,
            vote=vote,
            claims=(ResearchClaim(text=f"{role} evidence", evidence_refs=(evidence,)),),
            prompt_hash="b" * 64,
            input_tokens=1,
            output_tokens=1,
            synthesis_assessment=assessment if role == "portfolio" else None,
        )
        for role, vote in zip(ROLE_ORDER, votes, strict=True)
    )
    prompts = {
        role: AgentPromptVersionRecord(
            role=role,
            version="fixture-v1",
            content_hash="b" * 64,
            content="fixture",
        )
        for role in ROLE_ORDER
    }
    run = AgentResearchRunRecord(
        id=UUID("0198d500-1000-7000-8000-000000000111"),
        instrument_id=instrument_id,
        data_snapshot_id=snapshot_id,
        decision_time=now,
    )
    model = AgentModelConfigRecord(
        model_id="fixture-model",
        version="codex-sdk-chatgpt-v4-actionable"
        if actionable
        else "codex-sdk-chatgpt-v3"
        if structured
        else "codex-sdk-chatgpt-v2"
        if staged
        else "fixture-v1",
    )
    snapshot = DataSnapshot(id=snapshot_id, revision="fixture-v1")
    return synthesize_intent(
        run,
        "ABC",
        snapshot,
        prompts,
        model,
        (evidence,)
        + (
            (
                EvidenceRef(
                    source="nexus_portfolio_context",
                    object_id=f"{portfolio_context.portfolio_snapshot_id}:{instrument_id}",
                    available_at=portfolio_context.research_as_of,
                    location=f"memory://sha256/{portfolio_context.content_hash}",
                    content_hash=portfolio_context.content_hash,
                ),
            )
            if portfolio_context is not None
            else ()
        ),
        outputs,
        price_authority_valid=(
            include_price_authority if price_authority_valid is None else price_authority_valid
        ),
        **({"portfolio_context": portfolio_context} if portfolio_context is not None else {}),
    )


def test_missing_price_authority_forces_market_abstention_and_watch() -> None:
    intent = _synthesize(
        (
            "support",
            "support",
            "support",
            "support",
            "support",
            "oppose",
            "abstain",
            "abstain",
        ),
        include_price_authority=False,
    )
    market_vote = next(vote for vote in intent.role_votes if vote.role == "market_state")
    assert market_vote.vote == "abstain"
    assert (intent.stance, intent.exposure_action) == ("watch", "none")
    assert intent.quality_status == "partial"
    assert "Price authority is unavailable." in intent.uncertainties


def test_staged_synthesis_is_not_outvoted_by_earlier_support_and_keeps_evidence_veto():
    votes = ("support",) * 5 + ("oppose", "support", "oppose")
    intent = _synthesize(votes, staged=True)
    assert intent.stance == "watch"
    assert intent.theses[0].text == "portfolio evidence"
    assert [claim.text for claim in intent.counter_theses] == [
        "bear evidence",
        "evidence_audit evidence",
    ]
    missing = _synthesize(votes, staged=True, include_price_authority=False)
    assert missing.stance == "watch" and missing.exposure_action == "none"
    unresolved = _synthesize(votes[:6] + ("abstain", "support"), staged=True)
    assert unresolved.stance == "watch" and unresolved.quality_status == "partial"
    assert "Evidence review has unresolved objections." in unresolved.uncertainties


@pytest.mark.parametrize("final_vote", ["support", "oppose", "abstain"])
def test_staged_role_vote_cannot_establish_action_or_resolved_evidence(final_vote):
    intent = _synthesize(("support",) * 7 + (final_vote,), staged=True)
    assert (intent.stance, intent.exposure_action) == ("watch", "none")
    assert intent.quality_status == "partial"
    assert intent.evidence_audit == "partial"
    assert "三阶段角色表态未明确投资方向或审查缺口是否解决，仅供研究观察。" in intent.uncertainties


def test_actionable_v4_preserves_reviewed_direction_only_when_audit_is_complete() -> None:
    admitted = _synthesize(("support",) * 8, actionable=True)
    assert (admitted.stance, admitted.exposure_action) == (
        "buy_candidate",
        "eligible_for_increase",
    )
    assert admitted.quality_status == admitted.evidence_audit == "complete"

    rejected = _synthesize(("support",) * 6 + ("abstain", "support"), actionable=True)
    assert (rejected.stance, rejected.exposure_action) == ("watch", "none")
    assert rejected.quality_status == rejected.evidence_audit == "partial"


def test_actionable_v4_maps_resolved_audit_opposition_to_safe_no_trade() -> None:
    intent = _synthesize(
        ("support",) * 6 + ("oppose", "abstain"),
        actionable=True,
        assessment_stance="watch",
    )

    assert (intent.stance, intent.exposure_action) == ("watch", "none")
    assert intent.quality_status == intent.evidence_audit == "partial"
    assert intent.synthesis_assessment is not None
    assert intent.synthesis_assessment.review_conclusion == "limited"
    assert safe_no_trade_intent_shape_valid(intent)


def test_price_shaped_strings_do_not_establish_revalidated_authority() -> None:
    intent = _synthesize(
        (
            "support",
            "support",
            "support",
            "support",
            "support",
            "oppose",
            "abstain",
            "abstain",
        ),
        include_price_authority=True,
        price_authority_valid=False,
    )
    market_vote = next(vote for vote in intent.role_votes if vote.role == "market_state")
    assert market_vote.vote == "abstain"
    assert (intent.stance, intent.exposure_action) == ("watch", "none")
    assert intent.quality_status == "partial"


@pytest.mark.parametrize("price_valid", [True, False])
def test_structured_synthesis_cannot_self_authorize_complete_intent(price_valid):
    intent = _synthesize(("support",) * 8, structured=True, price_authority_valid=price_valid)
    assert (intent.stance, intent.exposure_action, intent.quality_status) == (
        "watch",
        "none",
        "partial",
    )
    assert intent.synthesis_assessment is not None
    assert intent.synthesis_assessment.stance == "buy_candidate"
    assert intent.theses == intent.synthesis_assessment.claims
    assert "synthesis_assessment" in intent.model_dump(mode="json")
    if not price_valid:
        assert "Price authority is unavailable." in intent.uncertainties


def test_legacy_intent_serialization_omits_only_new_assessment():
    intent = _synthesize(("support",) * 8, staged=True)
    assert "synthesis_assessment" not in intent.model_dump(mode="json")


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["settled_cost_usd", "input_tokens", "output_tokens"])
async def test_zero_call_close_cannot_erase_recorded_usage(monkeypatch, field):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import UUID

    from nexus.agents import execution
    from nexus.agents.budget import BudgetExhausted

    run = SimpleNamespace(
        id=UUID(int=1), settled_cost_usd=None, input_tokens=None, output_tokens=None
    )
    setattr(run, field, 1)
    release = AsyncMock()
    monkeypatch.setattr(execution, "release_daily_budget_if_reserved", release)
    session = AsyncMock()
    session.scalar.side_effect = ["research_stage_member", None]
    with pytest.raises(BudgetExhausted, match="recorded usage"):
        await execution._terminal_without_usage(
            session,
            run,
            output_sequence=1,
            event_sequence=2,
            status="abstained",
            reason="evidence_authority_missing",
        )
    release.assert_not_awaited()
    assert getattr(run, field) == 1


@pytest.mark.asyncio
async def test_zero_call_close_cannot_replace_settled_budget(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from nexus.agents import execution
    from nexus.agents.budget import BudgetExhausted

    run = SimpleNamespace(
        id=UUID(int=1), settled_cost_usd=None, input_tokens=None, output_tokens=None
    )
    monkeypatch.setattr(
        execution,
        "release_daily_budget_if_reserved",
        AsyncMock(return_value=SimpleNamespace(state="settled")),
    )
    persist = AsyncMock()
    monkeypatch.setattr(execution, "_persist_terminal", persist)
    session = AsyncMock()
    session.scalar.side_effect = ["research_stage_member", None]
    with pytest.raises(BudgetExhausted, match="settled reservation"):
        await execution._terminal_without_usage(
            session,
            run,
            output_sequence=1,
            event_sequence=2,
            status="abstained",
            reason="evidence_authority_missing",
        )
    persist.assert_not_awaited()
    assert run.settled_cost_usd is None and run.input_tokens is None and run.output_tokens is None
