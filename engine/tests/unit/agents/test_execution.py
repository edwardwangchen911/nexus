"""Durable fixture execution must append the full Agent lifecycle."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

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
    outputs = tuple(
        CommitteeRoleOutput(
            role=role,
            vote=vote,
            claims=(ResearchClaim(text=f"{role} evidence", evidence_refs=(evidence,)),),
            prompt_hash="b" * 64,
            input_tokens=1,
            output_tokens=1,
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
        model_id="fixture-model", version="codex-sdk-chatgpt-v2" if staged else "fixture-v1"
    )
    snapshot = DataSnapshot(id=snapshot_id, revision="fixture-v1")
    return synthesize_intent(
        run,
        "ABC",
        snapshot,
        prompts,
        model,
        (evidence,),
        outputs,
        price_authority_valid=(
            include_price_authority if price_authority_valid is None else price_authority_valid
        ),
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
    assert intent.stance == "reduce_candidate"
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
