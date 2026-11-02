"""Formal capacity uses reviewed actionable semantics, without granting execution."""

import pytest
from nexus.agents.codex_versions import (
    FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_CONFIG_VERSION,
)
from nexus.agents.synthesis import staged_intent_semantics_valid
from unit.agents.test_actionable_research import _case


@pytest.mark.parametrize("valid", [True, False])
@pytest.mark.parametrize(
    "version", [FORMAL_STAGE_CAPACITY_CONFIG_VERSION, FULL_INPUT_CAPACITY_CONFIG_VERSION]
)
def test_formal_stage_semantics_require_structured_review(valid, version):
    intent = _case()["intent"]
    if not valid:
        intent = intent.model_copy(update={"synthesis_assessment": None})
    assert staged_intent_semantics_valid(intent, version) is valid


@pytest.mark.parametrize(
    "flags",
    [
        {},
        {"formal_identity_verified": True, "candidate_identity_verified": True},
        {"formal_identity_verified": 1},
    ],
)
def test_v9_structured_semantics_do_not_grant_actionable_consumption(flags):
    from nexus.agents.actionable_research import (
        ActionableResearchError,
        admit_actionable_research_output,
    )

    case = _case()
    case["run"].pricing_version = FULL_INPUT_CAPACITY_CONFIG_VERSION
    with pytest.raises(ActionableResearchError):
        admit_actionable_research_output(**case, **flags)


@pytest.mark.parametrize("verified", ["candidate_identity_verified", "formal_identity_verified"])
def test_verified_v9_keeps_existing_content_admission_guards(verified):
    from nexus.agents.actionable_research import (
        ActionableResearchError,
        admit_actionable_research_output,
    )
    from nexus.agents.price_authority import FrozenPriceAuthorityStatus

    case = _case()
    case["run"].pricing_version = FULL_INPUT_CAPACITY_CONFIG_VERSION
    assert admit_actionable_research_output(**case, **{verified: True}).stance == "buy_candidate"
    case["price_authority_status"] = FrozenPriceAuthorityStatus.INVALID
    with pytest.raises(ActionableResearchError):
        admit_actionable_research_output(**case, **{verified: True})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "flags",
    [
        {"formal_stage_capacity": 1},
        {"formal_stage_capacity": "true"},
        {"formal_stage_capacity": True, "candidate_stage_capacity": True},
        {"formal_stage_capacity": True, "candidate_stage_capacity": 0},
    ],
)
async def test_formal_synthesis_invalid_modes_precede_reads(flags):
    from unittest.mock import AsyncMock

    from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis

    session = AsyncMock()
    with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
        await admit_portfolio_synthesis(
            session,
            cycle_id=None,
            research_output_ids=(),
            active_target_version_id=None,
            projection_hash="",
            daily_limit_usd=0,
            now=None,
            **flags,
        )
    assert session.mock_calls == []


@pytest.mark.parametrize(
    "change",
    [
        "valid",
        "no_proof",
        "candidate",
        "foreign_id",
        "foreign_hash",
        "wrong_price",
        "invalid_price_authority",
    ],
)
def test_native_formal_consumer_requires_exact_model_and_original_gates(change):
    from uuid import UUID

    from nexus.agents.actionable_research import (
        ActionableResearchError,
        admit_actionable_research_output,
    )
    from nexus.agents.platform_profile import sol_business_model_fields
    from nexus.agents.price_authority import FrozenPriceAuthorityStatus
    from nexus.persistence.models.agents import AgentModelConfigRecord
    from unit.agents.test_sol_pricing import sol_price_record

    case = _case()
    model = AgentModelConfigRecord(id=UUID(int=71), **sol_business_model_fields(sol_price_record()))
    run = case["run"]
    run.model_config_id = UUID(int=99) if change == "foreign_id" else model.id
    run.model_hash = "a" * 64 if change == "foreign_hash" else model.config_hash
    run.pricing_version = model.version
    if change == "wrong_price":
        model.platform_profile = {**model.platform_profile, "max_output_tokens": 1}
    if change == "invalid_price_authority":
        case["price_authority_status"] = FrozenPriceAuthorityStatus.INVALID
    flags = dict(
        native_formal_model=model,
        formal_identity_verified=change != "no_proof",
        candidate_identity_verified=change == "candidate",
    )
    if change == "valid":
        assert admit_actionable_research_output(**case, **flags).stance == "buy_candidate"
    else:
        with pytest.raises(ActionableResearchError):
            admit_actionable_research_output(**case, **flags)


@pytest.mark.parametrize(
    "changed", ["nothing", "intent_hash", "foreign_source", "foreign_output", "evidence_hash"]
)
def test_orphan_can_only_read_the_exact_already_admitted_source(changed):
    from uuid import UUID

    from nexus.agents.actionable_research import (
        admit_actionable_research_output,
        frozen_source_matches,
    )

    case = _case()
    action = admit_actionable_research_output(**case)
    refs = action.evidence_ref_ids if changed != "foreign_source" else (UUID(int=333),)
    if changed == "evidence_hash":
        case["bound_evidence"][0].content_hash = "a" * 64
    if changed == "foreign_output":
        case["bound_evidence"][0].output_id = UUID(int=334)
    assert frozen_source_matches(
        case["output"],
        case["intent"],
        "f" * 64 if changed == "intent_hash" else action.intent_hash,
        refs,
        case["bound_evidence"],
    ) is (changed == "nothing")
