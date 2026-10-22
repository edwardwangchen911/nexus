"""Formal input preparation bound to an already committed native trigger."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import SecretStr
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.persistence import select_authoritative_snapshot
from nexus.jobs.models import job_input_hash
from nexus.ops.agent_paper_identity import prepare_agent_paper_identity_once
from nexus.paper.agent_cycle_authority import (
    ServiceCalendarAuthority,
    load_agent_paper_service_calendar_authority,
    select_agent_paper_service_calendar_authority,
)
from nexus.paper.agent_cycle_models import AgentPaperTriggerReservation
from nexus.paper.candidate_batch import (
    CandidateInstrumentIdentity,
    validate_agent_paper_candidate_batch,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.paper import PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.persistence.session import session_factory
from nexus.research.current_identity_storage import inspect_identity_binding_snapshot
from nexus.settings import EngineSettings

if TYPE_CHECKING:
    from nexus.ops.agent_paper_once import AgentPaperTriggerSettings


@dataclass(frozen=True)
class FormalPreparationContext:
    account_id: UUID
    reservation: AgentPaperTriggerReservation
    client_idempotency_key: str
    calendar_id: UUID | None = None


@dataclass(frozen=True)
class FormalPreparationScope:
    now: datetime
    calendar_id: UUID
    candidates: tuple[CandidateInstrumentIdentity, ...]


async def require_preparation_scope(
    session: AsyncSession, context: FormalPreparationContext, *, now: datetime
) -> ServiceCalendarAuthority:
    """A mode flag alone never grants a manual preparation window."""
    from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
    from nexus.paper.manual_request import require_preparing_manual_request

    r = context.reservation
    invalid = "AGENT_PAPER_TRIGGER_FAILED"
    if not r.formal_stage_capacity or r.trigger_mode not in {"manual", "scheduled"}:
        raise ValueError(invalid)
    model = await require_formal_stage_model_profile(
        session, model_config_id=r.formal_model_config_id, now=now
    )
    batch, _ = await validate_agent_paper_candidate_batch(
        session, account_id=context.account_id, batch_id=r.candidate_batch_id, as_of=now
    )
    if model.config_hash != r.formal_model_hash or batch.batch_hash != r.candidate_batch_hash:
        raise ValueError(invalid)
    manual = None
    if r.trigger_mode == "manual":
        if r.manual_request_id is None or r.manual_execution_id is None:
            raise ValueError(invalid)
        if context.client_idempotency_key != f"manual-entry:{r.manual_request_id}":
            raise ValueError(invalid)
        manual = await require_preparing_manual_request(
            session,
            request_id=r.manual_request_id,
            execution_id=r.manual_execution_id,
            account_id=context.account_id,
            batch_id=r.candidate_batch_id,
            model_config_id=r.formal_model_config_id,
            now=now,
        )
    elif r.manual_request_id is not None or r.manual_execution_id is not None:
        raise ValueError(invalid)
    route = "agent-paper-explicit-manual-v1" if manual else "agent-paper-schwab-trigger-v2"
    payload: dict[str, object] = {
        "schema": route,
        "account_id": str(context.account_id),
        "decision_session": r.decision_session.isoformat(),
        "decision_slot_at": r.decision_slot_at.isoformat(),
        "source_policy_id": str(PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID),
        "client_idempotency_hash": sha256(context.client_idempotency_key.encode()).hexdigest(),
        "candidate_batch_id": str(batch.batch_id),
        "candidate_batch_hash": batch.batch_hash,
        "formal_stage_capacity": True,
        "formal_model_config_id": str(model.id),
        "formal_model_hash": model.config_hash,
    }
    if manual:
        payload.update(
            manual_execution_id=str(r.manual_execution_id),
            trigger_mode="manual",
            manual_request_id=str(manual.id),
            manual_mode=manual.mode,
        )
    claim = await session.get(IdempotencyRecord, r.claim_id, populate_existing=True)
    if (
        claim is None
        or claim.actor_id != f"local-agent-paper-schwab:{context.account_id}"
        or claim.route != route
        or claim.schema_version
        != ("agent_paper_explicit_manual_v1" if manual else "agent_paper_schwab_trigger_v2")
        or claim.idempotency_key != (str(manual.id) if manual else r.decision_slot_at.isoformat())
        or claim.request_hash != job_input_hash(payload)
        or claim.response_payload != {}
    ):
        raise ValueError(invalid)
    calendar = (
        await load_agent_paper_service_calendar_authority(
            session, now=now, expected_calendar_authority_id=context.calendar_id
        )
        if context.calendar_id is not None
        else await select_agent_paper_service_calendar_authority(session, now=now)
    )
    if (
        calendar.session != r.decision_session
        or calendar.local_date != r.decision_session
        or not calendar.market_open_at <= now < calendar.market_close_at
        or (
            r.trigger_mode == "scheduled"
            and not r.decision_slot_at <= now < r.decision_slot_at + timedelta(minutes=5)
        )
    ):
        raise ValueError("AGENT_PAPER_OUTSIDE_WINDOW")
    return calendar


async def read_preparation_scope(
    settings: EngineSettings, context: FormalPreparationContext
) -> FormalPreparationScope:
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("AGENT_PAPER_TRIGGER_FAILED")
            calendar = await require_preparation_scope(session, context, now=now)
            _, candidates = await validate_agent_paper_candidate_batch(
                session,
                account_id=context.account_id,
                batch_id=context.reservation.candidate_batch_id,
                as_of=now,
            )
            return FormalPreparationScope(now, calendar.calendar_authority_id, tuple(candidates))
    finally:
        await factory.kw["bind"].dispose()


async def freeze_prepared_snapshots(
    settings: EngineSettings, context: FormalPreparationContext, scope: FormalPreparationScope
) -> None:
    from nexus.ops.agent_paper_loop import _prepare_current_trends_research_snapshot

    for candidate in scope.candidates:
        current = await read_preparation_scope(settings, context)
        snapshot_id = await _prepare_current_trends_research_snapshot(
            settings,
            instrument_id=candidate.instrument_id,
            now=current.now,
        )
        factory = session_factory(settings)
        try:
            async with factory() as session, session.begin():
                now = await session.scalar(select(func.clock_timestamp()))
                if not isinstance(now, datetime):
                    raise ValueError("AGENT_PAPER_TRIGGER_FAILED")
                await require_preparation_scope(session, context, now=now)
                exact = await session.get(DataSnapshot, snapshot_id)
                selected = await select_authoritative_snapshot(
                    session, instrument_id=candidate.instrument_id, as_of=now
                )
                diagnostic = await inspect_identity_binding_snapshot(
                    session,
                    instrument_id=candidate.instrument_id,
                    snapshot=exact,
                    as_of=now,
                    decision_session=context.reservation.decision_session,
                )
                if (
                    exact is None
                    or selected is None
                    or selected.id != exact.id
                    or exact.observed_at != current.now
                    or diagnostic["snapshot_status"] != "ready"
                ):
                    raise ValueError("AGENT_PAPER_TRIGGER_FAILED")
        finally:
            await factory.kw["bind"].dispose()


async def prepare_formal_trigger_inputs(
    settings: "AgentPaperTriggerSettings",
    *,
    context: FormalPreparationContext,
    repo_root: Path,
    process_environment: Mapping[str, str],
) -> None:
    from nexus.ops import agent_paper_loop as loop

    scope = await read_preparation_scope(settings, context)
    batch_id = context.reservation.candidate_batch_id
    if batch_id is None:
        raise ValueError("AGENT_PAPER_TRIGGER_FAILED")
    for candidate in scope.candidates:
        current = await read_preparation_scope(settings, context)
        status = await loop.prepare_prior_daily_once(
            settings,
            now=current.now,
            candidate=candidate,
            account_id=context.account_id,
            repo_root=repo_root,
            expected_calendar_authority_id=scope.calendar_id,
        )
        if status not in {"ready", "prepared"}:
            raise ValueError("AGENT_PAPER_TRIGGER_FAILED")
    identity = await prepare_agent_paper_identity_once(
        settings,
        batch_id=batch_id,
        contact=process_environment.get("SEC_CONTACT", ""),
        execution=context,
        account_id=context.account_id,
        repo_root=repo_root,
        expected_calendar_authority_id=scope.calendar_id,
    )
    if identity not in {"ready", "prepared"}:
        raise ValueError("AGENT_PAPER_TRIGGER_FAILED")
    macro_settings = loop._FileDailySettings.model_validate(dict(process_environment))
    for candidate in scope.candidates:
        current = await read_preparation_scope(settings, context)
        status = await loop.prepare_macro_once(
            macro_settings,
            now=current.now,
            candidate=candidate,
            fred_key=SecretStr(process_environment.get("FRED_API_KEY", "")),
            account_id=context.account_id,
            repo_root=repo_root,
            expected_calendar_authority_id=scope.calendar_id,
        )
        if status not in {"ready", "prepared", "unavailable"}:
            raise ValueError("AGENT_PAPER_TRIGGER_FAILED")
    for candidate in scope.candidates:
        current = await read_preparation_scope(settings, context)
        status = await loop.prepare_current_trends_once(
            settings,
            now=current.now,
            candidate=candidate,
            account_id=context.account_id,
            repo_root=repo_root,
            expected_calendar_authority_id=scope.calendar_id,
        )
        if status not in {"ready", "prepared"}:
            raise ValueError("AGENT_PAPER_TRIGGER_FAILED")
    await freeze_prepared_snapshots(settings, context, scope)
