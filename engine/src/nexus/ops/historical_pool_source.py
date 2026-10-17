"""Read historical authority once and export only the exact three-stock diagnostic case."""

import asyncio
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select, text

from nexus.agents.codex_bounded_budget import bounded_stage_config_hash
from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.execution_authority import load_frozen_evidence, load_frozen_symbol
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.agents.persistence import _shared_daily_budget_used
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.portfolio_synthesis_contracts import (
    SYNTHESIS_PROMPT_VERSION,
    SYNTHESIS_TOOL_HASH,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
)
from nexus.agents.synthesis import domain_evidence
from nexus.ops.full_input_timing_diagnostic import load_case, validate_source_pool
from nexus.ops.historical_pool_diagnostic import (
    PROFILE,
    CaseStore,
    preflight_frozen_pool,
    validate_case,
)
from nexus.ops.openai_local import local_settings
from nexus.ops.research_budget_diagnostic import _private_directory
from nexus.paper.agent_cycle import _events, _fact_uuids
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agents import AgentPromptVersionRecord, AgentResearchRunRecord
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
    StrategyDecisionTargetRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


async def load_historical_pool(settings: EngineSettings, cycle_id: UUID) -> dict[str, Any]:
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            )
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("HISTORICAL_CASE_CLOCK_INVALID")
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            if (
                cycle is None
                or not cycle.execution_locked
                or cycle.decision_time >= now
                or cycle.account_id != settings.agent_paper_account_id
            ):
                raise ValueError("HISTORICAL_CASE_SOURCE_INVALID")
            events = await _events(session, cycle.id)
            optional_runs = [
                await session.get(AgentResearchRunRecord, rid)
                for rid in _fact_uuids(events, "research_run_ids")
            ]
            if len(optional_runs) != 3 or any(run is None for run in optional_runs):
                raise ValueError("HISTORICAL_CASE_COMPLETE_POOL_REQUIRED")
            runs = [run for run in optional_runs if run is not None]
            paired = validate_source_pool(cycle, events, runs, runs[0])
            model = await require_formal_stage_model_profile(
                session, model_config_id=settings.agent_paper_formal_model_config_id, now=now
            )
            if (
                model.version != BOUNDED_STAGE_BUDGET_CONFIG_VERSION
                or model.config_hash != bounded_stage_config_hash()
            ):
                raise ValueError("HISTORICAL_CASE_V11_REQUIRED")
            used = await _shared_daily_budget_used(session, business_date=now.date())
            if used + 4 * model.worst_case_cost_usd > settings.agent_daily_budget_usd:
                raise ValueError("HISTORICAL_CASE_BUDGET_UNAVAILABLE")
            batch, _ = await validate_agent_paper_candidate_batch(
                session,
                batch_id=cycle.candidate_batch_id,
                account_id=cycle.account_id,
                as_of=cycle.decision_time,
                universe_snapshot_id=cycle.universe_snapshot_id,
            )
            projection = await load_paper_projection(
                session, account_id=cycle.account_id, as_of=cycle.decision_time
            )
            members = []
            for _, run in paired:
                bound = await read_agent_paper_research_context(
                    session,
                    snapshot_id=run.data_snapshot_id,
                    instrument_id=run.instrument_id,
                    as_of=run.decision_time,
                )
                if (
                    bound is None
                    or bound[0].paper_portfolio.projection_hash != projection.projection_hash
                ):
                    raise ValueError("HISTORICAL_CASE_PROJECTION_UNAVAILABLE")
                compiled = await load_case(
                    settings, run.id, deadline=asyncio.get_running_loop().time() + 300
                )
                evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
                if tuple(domain_evidence(ref) for ref in evidence) != compiled.evidence:
                    raise ValueError("HISTORICAL_CASE_EVIDENCE_CHANGED")
                snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
                symbol = await load_frozen_symbol(session, run)
                if snapshot is None or symbol is None:
                    raise ValueError("HISTORICAL_CASE_SOURCE_INVALID")
                context = await read_portfolio_context(
                    session, run.data_snapshot_id, run.instrument_id, run.decision_time
                )
                members.append(
                    dict(
                        source_run_id=str(run.id),
                        paper_context_validated=True,
                        request=compiled.request.model_dump(mode="json"),
                        evidence=[ref.model_dump(mode="json") for ref in compiled.evidence],
                        evidence_ids=[str(ref.id) for ref in evidence],
                        inputs=await compiled.committee._inputs(compiled.request),
                        prompts={
                            role: p.model_dump(mode="json") for role, p in compiled.prompts.items()
                        },
                        symbol=symbol,
                        snapshot_revision=snapshot.revision,
                        portfolio_context=None
                        if context is None
                        else context.model_dump(mode="json"),
                    )
                )
            # A subsequent target must never be projected backwards into the source case.
            target = await session.scalar(
                select(PaperTargetVersionRecord)
                .where(
                    PaperTargetVersionRecord.account_id == cycle.account_id,
                    PaperTargetVersionRecord.activated_at <= cycle.decision_time,
                )
                .order_by(PaperTargetVersionRecord.version_seq.desc())
                .limit(1)
            )
            target_rows = (
                []
                if target is None
                else list(
                    await session.scalars(
                        select(StrategyDecisionTargetRecord)
                        .where(
                            StrategyDecisionTargetRecord.strategy_decision_id
                            == target.strategy_decision_id
                        )
                        .order_by(StrategyDecisionTargetRecord.instrument_id)
                    )
                )
            )
            decision = (
                None
                if target is None
                else await session.get(StrategyDecisionRecord, target.strategy_decision_id)
            )
            if target is not None and (decision is None or decision.account_id != cycle.account_id):
                raise ValueError("HISTORICAL_CASE_TARGET_INVALID")
            prompt = await session.scalar(
                select(AgentPromptVersionRecord).where(
                    AgentPromptVersionRecord.role == "portfolio_decision",
                    AgentPromptVersionRecord.version == SYNTHESIS_PROMPT_VERSION,
                    AgentPromptVersionRecord.release_channel == "active",
                )
            )
            if (
                prompt is None
                or prompt.created_at > now
                or hashlib.sha256(prompt.content.encode()).hexdigest() != prompt.content_hash
            ):
                raise ValueError("HISTORICAL_CASE_PROMPT_INVALID")
            calendar = await session.scalar(
                select(ExchangeCalendarAuthorityRecord)
                .join(
                    ExchangeCalendarSessionRecord,
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == ExchangeCalendarAuthorityRecord.id,
                )
                .where(
                    ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                    ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                    ExchangeCalendarAuthorityRecord.published_at <= cycle.decision_time,
                    ExchangeCalendarAuthorityRecord.created_at <= cycle.decision_time,
                    ExchangeCalendarSessionRecord.session == cycle.decision_session,
                    ExchangeCalendarSessionRecord.created_at <= cycle.decision_time,
                )
                .order_by(
                    ExchangeCalendarAuthorityRecord.published_at.desc(),
                    ExchangeCalendarAuthorityRecord.id.desc(),
                )
                .limit(1)
            )
            if calendar is None:
                raise ValueError("HISTORICAL_CASE_CALENDAR_UNAVAILABLE")
            template = dict(
                schema_version="portfolio_synthesis_input_v1",
                cycle_id=str(uuid4()),
                account_id=str(cycle.account_id),
                portfolio_id=str(cycle.portfolio_id),
                candidate_batch_id=str(batch.batch_id),
                candidate_batch_hash=batch.batch_hash,
                candidate_instrument_ids=[str(i) for i in batch.instrument_ids],
                active_target_version_id=None if target is None else str(target.id),
                active_target_content_hash=None if target is None else target.target_content_hash,
                active_target_instrument_ids=[str(r.instrument_id) for r in target_rows],
                active_target_weights=[
                    dict(instrument_id=str(r.instrument_id), target_weight=str(r.target_weight))
                    for r in target_rows
                ],
                active_target_cash_weight=None if decision is None else str(decision.cash_weight),
                projection=projection.model_dump(mode="json"),
                projection_hash=projection.projection_hash,
                policy_version=cycle.sizing_policy_version,
                policy_hash=cycle.sizing_policy_hash,
                prompt_version_id=str(prompt.id),
                prompt_version=prompt.version,
                prompt_hash=prompt.content_hash,
                model_config_id=str(model.id),
                model_hash=model.config_hash,
                pricing_version=model.version,
                tool_hash=SYNTHESIS_TOOL_HASH,
                intent_schema_hash=canonical_portfolio_synthesis_hash(
                    PortfolioSynthesisIntentV1.model_json_schema()
                ),
                calendar_authority_id=str(calendar.id),
                decision_time=cycle.decision_time.isoformat(),
            )
            case = dict(
                profile=PROFILE,
                source_cycle_id=str(cycle.id),
                source_profile=runs[0].pricing_version,
                evidence_as_of=cycle.decision_time.isoformat(),
                compiled_at=now.isoformat(),
                model_id=model.model_id,
                model_hash=model.config_hash,
                members=members,
                synthesis_template=template,
                synthesis_prompt=dict(content=prompt.content, content_hash=prompt.content_hash),
                budget=dict(
                    research_calls=9,
                    synthesis_calls=1,
                    research_input_per_stock=96000,
                    research_output_per_stock=8192,
                    synthesis_input=64000,
                    synthesis_output=8192,
                    unknown_reservations="held",
                    formal_reservations_changed=False,
                ),
            )
            validate_case(case)
            return case
    finally:
        await factory.kw["bind"].dispose()


async def prepare_historical_case(root: Path, cycle_id: UUID) -> Path:
    root = root.resolve()
    _private_directory(root / ".secrets")
    parent = root / ".secrets/nexus-historical-three-stock"
    if not parent.exists():
        parent.mkdir(mode=0o700)
    _private_directory(parent)
    # Fixed source/profile identity: an existing attempt is never replaced or retried.
    directory = parent / f"{cycle_id}-v11"
    if directory.exists():
        raise ValueError("HISTORICAL_CASE_ALREADY_EXISTS")
    async with asyncio.timeout(195):
        case = await load_historical_pool(local_settings(root, backend="codex_sdk"), cycle_id)
        await preflight_frozen_pool(case)
    directory.mkdir(mode=0o700)
    path = directory / "case.sqlite3"
    with CaseStore.create(path, case):
        pass
    return path
