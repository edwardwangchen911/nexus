"""Atomic registration for one isolated paired Agent-version evaluation."""

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_sdk import MODEL_ID
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION, aggregate_input_limit
from nexus.agents.committee import ROLE_ORDER, AgentRole
from nexus.agents.execution_authority import load_frozen_prompts
from nexus.agents.persistence import load_complete_prompt_authority
from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.paper.agent_version_models import (
    POLICY_VERSION,
    AgentVersionEvaluationRegistration,
    AgentVersionEvaluationRequest,
    evaluation_policy_hash,
    evaluation_request_hash,
)
from nexus.persistence.models.agent_versions import (
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.paper import PaperAccountRecord
from nexus.persistence.models.portfolio import Account, Portfolio
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict

_ROUTE = "agent-version-evaluation:register"
_SCHEMA_VERSION = "agent_version_eval_reg_v1"
_NEW_YORK = ZoneInfo("America/New_York")


async def register_agent_version_evaluation(
    session: AsyncSession,
    *,
    request: AgentVersionEvaluationRequest,
    actor_id: str,
    idempotency_key: str,
    now: datetime,
) -> AgentVersionEvaluationRegistration:
    """Register frozen authority and two equal internal accounts without model work."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise ValueError("AGENT_VERSION_TIME_INVALID")
    request_hash = evaluation_request_hash(request)
    claim, owner = await claim_or_lock(
        session,
        actor_id,
        _ROUTE,
        idempotency_key,
        request_hash,
        _SCHEMA_VERSION,
    )
    if not owner:
        if claim.request_hash != request_hash:
            raise IdempotencyConflict(
                "idempotency key Agent-version request conflicts with prior request"
            )
        return _registration_from_payload(claim.response_payload, replayed=True)

    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1417))"),
        {"scope": "agent-version-evaluation-registration"},
    )
    incumbent_run = await session.get(
        AgentResearchRunRecord,
        request.incumbent_version_source_run_id,
        populate_existing=True,
    )
    if (
        incumbent_run is None
        or incumbent_run.status != "completed"
        or incumbent_run.instrument_id != request.instrument_id
    ):
        raise ValueError("AGENT_VERSION_INCUMBENT_INVALID")
    incumbent_prompts = await load_frozen_prompts(session, incumbent_run)
    incumbent_model = await session.get(
        AgentModelConfigRecord, incumbent_run.model_config_id, populate_existing=True
    )
    if (
        incumbent_prompts is None
        or incumbent_model is None
        or incumbent_model.config_hash != incumbent_run.model_hash
        or incumbent_model.version != incumbent_run.pricing_version
    ):
        raise ValueError("AGENT_VERSION_INCUMBENT_INVALID")

    challenger_prompts = await load_complete_prompt_authority(
        session, orchestrator_id=request.challenger_prompt_version_id
    )
    challenger_model = await session.get(
        AgentModelConfigRecord,
        request.challenger_model_config_id,
        populate_existing=True,
    )
    if challenger_prompts is None or challenger_model is None:
        raise ValueError("AGENT_VERSION_CHALLENGER_INVALID")
    if not _model_is_agent_paper_compatible(
        incumbent_model
    ) or not _model_is_agent_paper_compatible(challenger_model):
        raise ValueError("AGENT_VERSION_MODEL_INCOMPATIBLE")

    last_slot = request.oos_slots[-1]
    if not _pricing_covers(incumbent_model, now=now, last_slot=last_slot) or not _pricing_covers(
        challenger_model, now=now, last_slot=last_slot
    ):
        raise ValueError("AGENT_VERSION_PRICING_INVALID")
    challenger_prompt_hash = _prompt_manifest_hash(challenger_prompts)
    if (
        incumbent_run.prompt_hash == challenger_prompt_hash
        and incumbent_run.model_hash == challenger_model.config_hash
    ):
        raise ValueError("AGENT_VERSION_IDENTITY_UNCHANGED")

    if not await _is_aapl(session, request, now=now):
        raise ValueError("AGENT_VERSION_INSTRUMENT_INVALID")
    calendar = await _select_calendar(session, request=request, now=now)
    if calendar is None:
        raise ValueError("AGENT_VERSION_CALENDAR_INVALID")

    policy_hash = evaluation_policy_hash()
    authority_hash = _authority_hash(
        request=request,
        calendar=calendar,
        incumbent_prompt_hash=incumbent_run.prompt_hash,
        incumbent_model_hash=incumbent_run.model_hash,
        challenger_prompt_hash=challenger_prompt_hash,
        challenger_model_hash=challenger_model.config_hash,
        policy_hash=policy_hash,
    )
    existing = await session.scalar(
        select(AgentVersionEvaluationRecord).where(
            AgentVersionEvaluationRecord.authority_hash == authority_hash
        )
    )
    if existing is not None:
        registration = _registration(existing, replayed=True)
        claim.response_payload = registration.model_dump(mode="json")
        await session.flush()
        return registration

    open_evaluation = await session.scalar(
        select(AgentVersionEvaluationRecord)
        .outerjoin(
            AgentVersionEvaluationDecisionRecord,
            AgentVersionEvaluationDecisionRecord.evaluation_id
            == AgentVersionEvaluationRecord.id,
        )
        .where(AgentVersionEvaluationDecisionRecord.evaluation_id.is_(None))
        .limit(1)
    )
    if open_evaluation is not None:
        raise ValueError("AGENT_VERSION_EVALUATION_ALREADY_OPEN")

    evaluation_id = new_uuid7()
    incumbent_account = _evaluation_account(
        evaluation_id=evaluation_id,
        lane="incumbent",
        initial_capital=request.initial_capital,
    )
    challenger_account = _evaluation_account(
        evaluation_id=evaluation_id,
        lane="challenger",
        initial_capital=request.initial_capital,
    )
    session.add_all((incumbent_account[0], challenger_account[0]))
    await session.flush()
    session.add_all((incumbent_account[1], challenger_account[1]))
    await session.flush()
    session.add_all((incumbent_account[2], challenger_account[2]))
    await session.flush()
    evaluation = AgentVersionEvaluationRecord(
        id=evaluation_id,
        incumbent_account_id=incumbent_account[2].id,
        challenger_account_id=challenger_account[2].id,
        instrument_id=request.instrument_id,
        calendar_authority_id=calendar.id,
        incumbent_version_source_run_id=incumbent_run.id,
        challenger_prompt_version_id=request.challenger_prompt_version_id,
        challenger_model_config_id=challenger_model.id,
        incumbent_prompt_hash=incumbent_run.prompt_hash,
        incumbent_model_hash=incumbent_run.model_hash,
        challenger_prompt_hash=challenger_prompt_hash,
        challenger_model_hash=challenger_model.config_hash,
        train_slots=list(request.train_slots),
        oos_slots=list(request.oos_slots),
        policy_version=POLICY_VERSION,
        policy_hash=policy_hash,
        authority_hash=authority_hash,
    )
    session.add(evaluation)
    registration = _registration(evaluation, replayed=False)
    claim.response_payload = registration.model_dump(mode="json")
    await session.flush()
    return registration


def _evaluation_account(
    *, evaluation_id: UUID, lane: str, initial_capital: Decimal
) -> tuple[Account, Portfolio, PaperAccountRecord]:
    account_id = new_uuid7()
    portfolio_id = new_uuid7()
    paper_account_id = new_uuid7()
    account = Account(
        id=account_id,
        provider="internal_paper_evaluation",
        status="ACTIVE",
    )
    portfolio = Portfolio(
        id=portfolio_id,
        account_id=account_id,
        name=f"Agent version {lane} {evaluation_id}",
    )
    paper = PaperAccountRecord(
        id=paper_account_id,
        account_id=account_id,
        portfolio_id=portfolio_id,
        code=f"ave-{lane[:3]}-{evaluation_id.hex}",
        currency="USD",
        account_model="cash",
        position_model="long_only",
        initial_capital=initial_capital,
        capability="simulation_write_internal",
    )
    return account, portfolio, paper


async def _is_aapl(
    session: AsyncSession,
    request: AgentVersionEvaluationRequest,
    *,
    now: datetime,
) -> bool:
    first_date = _session(request.train_slots[0])
    last_date = _session(request.oos_slots[-1])
    alias = await session.scalar(
        select(InstrumentAlias.id)
        .where(
            InstrumentAlias.instrument_id == request.instrument_id,
            InstrumentAlias.symbol == "AAPL",
            InstrumentAlias.alias_type == "ticker",
            InstrumentAlias.created_at <= now,
            InstrumentAlias.valid_from <= first_date,
            (InstrumentAlias.valid_to.is_(None) | (InstrumentAlias.valid_to >= last_date)),
        )
        .limit(1)
    )
    return alias is not None


async def _select_calendar(
    session: AsyncSession,
    *,
    request: AgentVersionEvaluationRequest,
    now: datetime,
) -> ExchangeCalendarAuthorityRecord | None:
    slots = request.train_slots + request.oos_slots
    dates = {_session(slot) for slot in slots}
    candidates = tuple(
        await session.scalars(
            select(ExchangeCalendarAuthorityRecord)
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.range_start <= min(dates),
                ExchangeCalendarAuthorityRecord.range_end >= max(dates),
                ExchangeCalendarAuthorityRecord.published_at <= now,
            )
            .order_by(
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
        )
    )
    for candidate in candidates:
        rows = tuple(
            await session.scalars(
                select(ExchangeCalendarSessionRecord).where(
                    ExchangeCalendarSessionRecord.calendar_authority_id == candidate.id,
                    ExchangeCalendarSessionRecord.session.in_(dates),
                )
            )
        )
        by_date = {row.session: row for row in rows}
        if set(by_date) != dates:
            continue
        if all(
            by_date[_session(slot)].market_open_at <= slot
            and slot < by_date[_session(slot)].market_close_at
            for slot in slots
        ):
            return candidate
    return None


def _pricing_covers(
    model: AgentModelConfigRecord, *, now: datetime, last_slot: datetime
) -> bool:
    return model.pricing_available_at <= now and model.pricing_expires_at > last_slot


def _model_is_agent_paper_compatible(model: AgentModelConfigRecord) -> bool:
    return (
        model.model_id == MODEL_ID
        and model.version == ACTIONABLE_CONFIG_VERSION
        and model.max_input_tokens == aggregate_input_limit(model.version)
        and model.max_output_tokens == 8192
        and model.input_usd_per_million == 0
        and model.output_usd_per_million == 0
    )


def _prompt_manifest_hash(prompts: dict[AgentRole, AgentPromptVersionRecord]) -> str:
    manifest = {
        role: {
            "id": str(prompts[role].id),
            "version": prompts[role].version,
            "content_hash": prompts[role].content_hash,
        }
        for role in ROLE_ORDER
    }
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _authority_hash(
    *,
    request: AgentVersionEvaluationRequest,
    calendar: ExchangeCalendarAuthorityRecord,
    incumbent_prompt_hash: str,
    incumbent_model_hash: str,
    challenger_prompt_hash: str,
    challenger_model_hash: str,
    policy_hash: str,
) -> str:
    value = {
        "calendar_authority_id": str(calendar.id),
        "calendar_content_hash": calendar.content_hash,
        "challenger_model_hash": challenger_model_hash,
        "challenger_prompt_hash": challenger_prompt_hash,
        "incumbent_model_hash": incumbent_model_hash,
        "incumbent_prompt_hash": incumbent_prompt_hash,
        "initial_capital": format(request.initial_capital.normalize(), "f"),
        "instrument_id": str(request.instrument_id),
        "oos_slots": [_timestamp(slot) for slot in request.oos_slots],
        "policy_hash": policy_hash,
        "train_slots": [_timestamp(slot) for slot in request.train_slots],
    }
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _registration(
    evaluation: AgentVersionEvaluationRecord, *, replayed: bool
) -> AgentVersionEvaluationRegistration:
    return AgentVersionEvaluationRegistration(
        evaluation_id=evaluation.id,
        incumbent_account_id=evaluation.incumbent_account_id,
        challenger_account_id=evaluation.challenger_account_id,
        authority_hash=evaluation.authority_hash,
        replayed=replayed,
    )


def _registration_from_payload(
    payload: dict[str, object], *, replayed: bool
) -> AgentVersionEvaluationRegistration:
    try:
        if payload.get("model_jobs_created") != 0 or payload.get("execution_locked") is not True:
            raise ValueError
        return AgentVersionEvaluationRegistration.model_validate(
            {**payload, "replayed": replayed}
        )
    except (TypeError, ValueError) as error:
        raise RuntimeError("Agent-version idempotency response is incomplete") from error


def _session(value: datetime) -> date:
    return value.astimezone(_NEW_YORK).date()


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
