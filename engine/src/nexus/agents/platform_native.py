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


def native_runtime_matches(settings: EngineSettings) -> bool:
    """Static supported request configuration; not a model/price or dispatch grant."""
    if (
        settings.agent_runtime_mode is not AgentRuntimeMode.RESPONSES
        or settings.openai_extraction_model != "gpt-6.1-sol"
        or settings.openai_reasoning_model != "gpt-6.1-sol"
    ):
        return False
    try:
        credential_for_preparation(settings)
    except ValueError:
        return False
    return True


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
    compiler, request, refs, inputs = await native_prepared_material(session, run=run, model=model)
    assert model.platform_profile is not None
    price = sol_price_record(model.platform_profile["price_record"])
    return freeze_native_platform_allocation(
        compiler,
        request=request,
        evidence_refs=refs,
        inputs=inputs,
        profile=platform_wire_profile(price.version, price_record=price),
        price_record=price,
    )


async def native_prepared_material(
    session: AsyncSession, *, run: AgentResearchRunRecord, model: AgentModelConfigRecord
) -> tuple[Any, Any, Any, Any]:
    """Build the normal committee's original frozen material, without a model call."""
    if (
        not sol_business_model_matches(model)
        or model.config_hash != run.model_hash
        or model.version != run.pricing_version
    ):
        raise ValueError("PLATFORM_NATIVE_MODEL_BINDING_INVALID")
    assert model.platform_profile is not None
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
    return (
        compiler,
        request,
        tuple(domain_evidence(e) for e in evidence),
        await compiler._inputs(request),
    )


async def read_native_allocation(
    session: AsyncSession, *, run: AgentResearchRunRecord
) -> tuple[dict[str, Any], PlatformAllocation]:
    """Historical read: validate the original route, not today's credentials or prices."""
    from pydantic import TypeAdapter

    row = (
        (
            await session.execute(
                text(
                    "SELECT platform_binding,public.nexus_sol_native_route_valid(r) AS valid "
                    "FROM research_stage_route r WHERE run_id=:run"
                ),
                {"run": run.id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or row["valid"] is not True or row["platform_binding"] is None:
        raise ValueError("PLATFORM_NATIVE_ROUTE_INVALID")
    binding = dict(row["platform_binding"])
    allocation = TypeAdapter(PlatformAllocation).validate_python(binding["allocation"])
    if allocation.allocation_hash != binding["allocation_hash"]:
        raise ValueError("PLATFORM_NATIVE_ROUTE_INVALID")
    return binding, allocation


async def prepare_native_pool(
    session: AsyncSession, settings: EngineSettings, run: AgentResearchRunRecord, *, deadline: float
) -> StructuredStagedCodexCommittee:
    """Revalidate the original owner pool before claim; never create replacement work."""
    import asyncio

    from nexus.agents.platform_wire import native_platform_input_hash
    from nexus.agents.research_stages.routes import selected_route

    credential = credential_for_preparation(settings)
    owner = (
        await session.execute(
            text("SELECT scope_id FROM research_stage_route WHERE run_id=:run"), {"run": run.id}
        )
    ).scalar_one()
    members = (
        await session.execute(
            text(
                "SELECT run_id,job_id FROM research_stage_route "
                "WHERE scope_id=:scope ORDER BY run_id"
            ),
            {"scope": owner},
        )
    ).all()
    root = await selected_route(
        session, next(job_id for run_id, job_id in members if run_id == run.id)
    )
    if root is None:
        raise ValueError("PLATFORM_NATIVE_ROUTE_INVALID")
    parallel = False
    request_record = None
    if root["kind"] == "independent":
        from nexus.agents.research_dispatch import require_dispatch_pool, uses_two_flight
        from nexus.paper.research_request import read_research_manifest
        from nexus.persistence.models.manual_paper import ManualPaperRequest

        request_record = await session.get(ManualPaperRequest, owner)
        if request_record is None:
            raise ValueError("PLATFORM_NATIVE_POOL_NOT_READY")
        manifest = await read_research_manifest(session, request_record)
        if manifest is None:
            raise ValueError("PLATFORM_NATIVE_POOL_NOT_READY")
        parallel = uses_two_flight(request_record.research_profile)
        if parallel:
            await require_dispatch_pool(session, request_record, manifest, run.id)
        expected_ids = {m.run_id for m in manifest.members}
    elif root["kind"] == "formal":
        from nexus.paper.agent_cycle import _events, _fact_uuids

        expected_ids = set(_fact_uuids(await _events(session, owner), "research_run_ids"))
    elif root["kind"] == "increment":
        expected_ids = {run.id}
    else:
        raise ValueError("PLATFORM_NATIVE_ROUTE_INVALID")
    if not expected_ids or expected_ids != {rid for rid, _ in members}:
        raise ValueError("PLATFORM_NATIVE_POOL_NOT_READY")
    selected = None
    bound_pool = []
    selected_material = None
    async with asyncio.timeout_at(deadline):
        for run_id, job_id in members:
            if monotonic() >= deadline:
                raise ValueError("CODEX_CAPACITY_PRECLAIM_TIMEOUT")
            route = await selected_route(session, job_id)
            member = await session.get(AgentResearchRunRecord, run_id)
            if route is None or member is None or route.get("platform_binding") is None:
                raise ValueError("PLATFORM_NATIVE_ROUTE_INVALID")
            model = await session.get(AgentModelConfigRecord, member.model_config_id)
            if (
                model is None
                or not sol_business_model_matches(model)
                or member.model_config_id != run.model_config_id
                or member.model_hash != run.model_hash
                or member.pricing_version != run.pricing_version
            ):
                raise ValueError("PLATFORM_NATIVE_MODEL_BINDING_INVALID")
            if member.status not in (
                {"prepared", "running", "completed"} if parallel else {"prepared", "completed"}
            ):
                raise ValueError("PLATFORM_NATIVE_POOL_NOT_READY")
            bound_pool.append(
                dict(
                    run_id=str(run_id), allocation_hash=route["platform_binding"]["allocation_hash"]
                )
            )
            binding, allocation = await read_native_allocation(session, run=member)
            if member.status == "completed" or (
                parallel and member.id != run.id and member.status == "running"
            ):
                continue
            if credential.binding_hash != binding["credential_binding_hash"]:
                raise ValueError("PLATFORM_HTTP_CREDENTIAL_CHANGED")
            assert model.platform_profile is not None
            price = sol_price_record(model.platform_profile["price_record"])
            at = await session.scalar(text("SELECT clock_timestamp()"))
            price.require_current(
                now=at, deadline=at + timedelta(seconds=max(0, deadline - monotonic()) + 7)
            )
            compiler, request, refs, inputs = await native_prepared_material(
                session, run=member, model=model
            )
            fresh = freeze_native_platform_allocation(
                compiler,
                request=request,
                evidence_refs=refs,
                inputs=inputs,
                profile=allocation.profile,
                price_record=price,
            )
            if (
                fresh != allocation
                or native_platform_input_hash(
                    compiler, request=request, evidence_refs=refs, inputs=inputs
                )
                != allocation.input_hash
            ):
                raise ValueError("PLATFORM_NATIVE_INPUT_CHANGED")
            if run_id == run.id:
                selected_material = (request, refs, inputs)
                selected = StructuredStagedCodexCommittee(
                    prompts=compiler._prompts,
                    tools=compiler._tools,
                    deadline=deadline,
                    aggregate_input_tokens=sum(allocation.stage_input_upper_bounds),
                    max_wire_bytes=max(allocation.stage_input_upper_bounds) - 8192,
                    inline_citations=True,
                    paper_stage_scope=True,
                    stage_time_budget=True,
                    stage_budget_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
                    platform_allocation=allocation,
                    platform_price_record=price,
                )
        if selected is None or selected_material is None or monotonic() >= deadline:
            raise ValueError("PLATFORM_NATIVE_PREPARATION_REQUIRED")
        from nexus.agents.codex_sdk import codex_request_hash
        from nexus.agents.platform_wire import _hash

        request, refs, inputs = selected_material
        selected._capacity_pool_manifest = _hash(dict(scope_id=str(owner), members=bound_pool))
        selected._capacity_pool_request = request
        selected._capacity_pool_evidence = refs
        selected._capacity_research_hash = codex_request_hash(
            *selected.build_stage_request(
                stage="research",
                request=request,
                evidence_refs=refs,
                frozen_inputs=inputs,
                previous=[],
            )
        )
        if request_record is not None and request_record.contract_version == 4:
            from nexus.monitoring.host import INCREMENT_ACTION_HEADROOM_SECONDS
            from nexus.paper.research_request import database_now
            from nexus.paper.single_sol_research import recheck_single_sol

            # Still inside the caller's bounded preclaim transaction; no HTTP or new right yet.
            await recheck_single_sol(
                session,
                settings,
                request_record,
                run_id=run.id,
                cleanup_deadline=await database_now(session)
                + timedelta(seconds=INCREMENT_ACTION_HEADROOM_SECONDS),
            )
        return selected


def native_http_client() -> Any:
    import httpx

    return httpx.AsyncClient(timeout=None, follow_redirects=False, auth=None, trust_env=True)


async def native_platform_semantics(
    session: AsyncSession, run: AgentResearchRunRecord, model: AgentModelConfigRecord
) -> bool:
    """Select original staged audit/synthesis semantics only for a native Sol owner."""
    from nexus.agents.research_stages.routes import selected_route

    if not sol_business_model_matches(model):
        return False
    job_id = await session.scalar(
        text("SELECT job_id FROM research_stage_route WHERE run_id=:run"), {"run": run.id}
    )
    route = None if job_id is None else await selected_route(session, job_id)
    if (
        run.model_hash != model.config_hash
        or run.pricing_version != model.version
        or route is None
        or route.get("platform_binding") is None
        or route["kind"] == "comparison"
    ):
        raise ValueError("PLATFORM_NATIVE_SYNTHESIS_AUTHORITY_INVALID")
    await read_native_allocation(session, run=run)
    return True
