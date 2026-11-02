"""Freeze normal Platform preparation in the existing native route transaction.

No new ledger, comparison identity, model request or implicit credential lookup.
Public terminal readers must not need a current Key or unexpired price record.
"""

from dataclasses import asdict
from datetime import datetime, timedelta
from time import monotonic
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.codex_versions import (
    BOUNDED_RESEARCH_HANDLER_TIMEOUT_SECONDS,
    BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
)
from nexus.agents.committee import PromptAsset
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution_authority import load_frozen_evidence, load_frozen_prompts
from nexus.agents.models import ResearchRequest
from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_profile import sol_business_model_matches
from nexus.agents.platform_wire import (
    PlatformAllocation,
    freeze_native_platform_allocation,
    platform_wire_profile,
)
from nexus.agents.synthesis import domain_evidence
from nexus.ops.platform_credentials import (
    PlatformCredential,
    _validate_credential,
    credential_fingerprint,
)
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.settings import AgentRuntimeMode, EngineSettings


def credential_for_preparation(settings: EngineSettings) -> PlatformCredential:
    """Use the normal caller's resolved project Key, with no ambient scope headers."""
    if (
        settings.agent_runtime_mode is not AgentRuntimeMode.RESPONSES
        or settings.openai_api_key is None
    ):
        raise ValueError("PLATFORM_CREDENTIAL_REQUIRED")
    credential = PlatformCredential(
        settings.openai_api_key,
        credential_fingerprint(settings.openai_api_key.get_secret_value()),
        None,
        None,
        key_scope_only=True,
    )
    _validate_credential(credential)
    return credential


async def native_increment_model_allowed(
    session: AsyncSession, *, parent: AgentResearchRunRecord, model_config_id: UUID | None
) -> bool:
    """New increments may use renewed Sol or an explicit complete SDK rollback.

    The parent remains exact evidence, with its original model/hash/price. This
    does not change any existing child or grant retry/fallback after dispatch.
    """
    if parent.model_config_id == model_config_id:
        return True
    from nexus.agents.codex_formal_stage import formal_preparation_model_matches

    old = await session.get(AgentModelConfigRecord, parent.model_config_id)
    new = (
        None
        if model_config_id is None
        else await session.get(AgentModelConfigRecord, model_config_id)
    )
    if (
        old is None
        or new is None
        or old.config_hash != parent.model_hash
        or old.version != parent.pricing_version
        or not formal_preparation_model_matches(old)
        or not formal_preparation_model_matches(new)
    ):
        return False
    return sol_business_model_matches(new) or (
        sol_business_model_matches(old) and new.version == BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    )


async def freeze_native_route_binding(
    session: AsyncSession,
    *,
    run: AgentResearchRunRecord,
    model: AgentModelConfigRecord,
    credential: PlatformCredential | None,
) -> dict[str, Any]:
    """Called after native owner/Job qualification, before original prepare commits."""
    if credential is None:
        raise ValueError("PLATFORM_CREDENTIAL_REQUIRED")
    _validate_credential(credential)
    if (
        not sol_business_model_matches(model)
        or model.id != run.model_config_id
        or run.model_hash != model.config_hash
        or run.pricing_version != model.version
        or run.reserved_cost_usd != model.worst_case_cost_usd
    ):
        raise ValueError("PLATFORM_NATIVE_MODEL_BINDING_INVALID")
    assert model.platform_profile is not None
    price = sol_price_record(model.platform_profile["price_record"])
    now = await session.scalar(text("SELECT clock_timestamp()"))
    if not isinstance(now, datetime):
        raise ValueError("PLATFORM_PRICE_WINDOW_INVALID")
    # Three bounded stock handlers may run serially. This reserves price validity,
    # not extra execution time; every original handler/lease deadline remains.
    price.require_current(
        now=now, deadline=now + timedelta(seconds=3 * BOUNDED_RESEARCH_HANDLER_TIMEOUT_SECONDS + 7)
    )
    allocation = await native_run_allocation(session, run=run, model=model)
    return dict(
        version=1,
        transport="platform_responses",
        model_config_id=str(model.id),
        model_hash=model.config_hash,
        price_version=price.version,
        price_record_hash=price.content_hash,
        credential_binding_hash=credential.binding_hash,
        allocation=asdict(allocation),
        allocation_hash=allocation.allocation_hash,
    )


async def native_run_allocation(
    session: AsyncSession, *, run: AgentResearchRunRecord, model: AgentModelConfigRecord
) -> PlatformAllocation:
    """The same full input envelope for optional daily preview and original registration."""
    if (
        not sol_business_model_matches(model)
        or model.config_hash != run.model_hash
        or model.version != run.pricing_version
    ):
        raise ValueError("PLATFORM_NATIVE_MODEL_BINDING_INVALID")
    assert model.platform_profile is not None
    price = sol_price_record(model.platform_profile["price_record"])
    prompts = await load_frozen_prompts(session, run)
    evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
    if prompts is None or not evidence:
        raise ValueError("PLATFORM_NATIVE_INPUT_REQUIRED")
    compiler = StructuredStagedCodexCommittee(
        prompts={
            role: PromptAsset(
                role=role, version=p.version, content=p.content, content_hash=p.content_hash
            )
            for role, p in prompts.items()
        },
        tools=await database_read_only_tools(
            session, run, allow_fixture_evidence=False, allow_fixture_price_evidence=False
        ),
        deadline=monotonic() + BOUNDED_RESEARCH_HANDLER_TIMEOUT_SECONDS,
        aggregate_input_tokens=96000,
        max_wire_bytes=100479,
        inline_citations=True,
        paper_stage_scope=True,
        stage_time_budget=True,
        stage_budget_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
    )
    request = ResearchRequest(
        instrument_id=run.instrument_id,
        data_snapshot_id=run.data_snapshot_id,
        decision_time=run.decision_time,
    )
    return freeze_native_platform_allocation(
        compiler,
        request=request,
        evidence_refs=tuple(domain_evidence(e) for e in evidence),
        inputs=await compiler._inputs(request),
        profile=platform_wire_profile(price.version, price_record=price),
        price_record=price,
    )
