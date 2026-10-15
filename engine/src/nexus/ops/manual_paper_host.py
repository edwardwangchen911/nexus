"""Single-host persisted queue inspection and one-time protected-scope claim."""

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.paper.agent_cycle_authority import select_agent_paper_service_calendar_authority
from nexus.paper.manual_request import (
    ManualRequestView,
    claim_manual_request,
    read_manual_request,
    record_manual_request_event,
)
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

if TYPE_CHECKING:
    from nexus.ops.agent_paper_loop import AgentPaperLoopSnapshot
    from nexus.ops.agent_paper_once import AgentPaperTriggerSettings


MANUAL_TRIGGER_PHASES = frozenset({"trigger_request", "trigger_reservation", "trigger_provider"})
MANUAL_TRIGGER_KINDS = frozenset(
    {"timeout", "database", "name_error", "attribute_error", "validation", "unexpected_exception"}
)
MANUAL_TRIGGER_CODES = frozenset(
    {
        "MANUAL_REQUEST_INVALID",
        "MANUAL_REQUEST_TERMINAL_OR_UNKNOWN",
        "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
        "MANUAL_PREPARATION_ACK_BLOCKED",
        "MANUAL_RESULT_UNKNOWN",
        "AGENT_PAPER_REQUEST_INVALID",
        "AGENT_PAPER_OUTSIDE_WINDOW",
        "AGENT_PAPER_RESULT_UNKNOWN",
        "AGENT_PAPER_IDEMPOTENCY_CONFLICT",
        "CANDIDATE_BATCH_INVALID",
        "CANDIDATE_BATCH_REQUIRED",
        "CODEX_FORMAL_CYCLE_PROFILE_INVALID",
        "CODEX_FORMAL_AUTHORITY_INVALID",
        "FORMAL_BUDGET_TRIGGER_BINDING_INVALID",
        "FORMAL_BUDGET_BINDING_INVALID",
        "PORTFOLIO_SYNTHESIS_PROMPT_INVALID",
        "PRIOR_DAILY_AUTHORITY_MISSING",
        "RESEARCH_SNAPSHOT_MISSING",
        "REVIEWED_SECTOR_MISSING",
        "AGENT_PAPER_CALENDAR_PRIOR_SESSION_MISSING",
    }
)


def annotate_manual_trigger_failure(error: Exception, *, phase: str) -> None:
    """Only fixed diagnostics cross the fail-closed manual wrapper."""
    if phase not in MANUAL_TRIGGER_PHASES:
        return
    try:
        setattr(error, "_nexus_agent_paper_failure_phase", phase)
        kind = (
            "timeout"
            if isinstance(error, TimeoutError)
            else "database"
            if isinstance(error, SQLAlchemyError)
            else "name_error"
            if isinstance(error, NameError)
            else "attribute_error"
            if isinstance(error, AttributeError)
            else "validation"
            if isinstance(error, ValueError)
            else "unexpected_exception"
        )
        setattr(error, "_nexus_agent_paper_failure_kind", kind)
        code = str(error)
        error._nexus_agent_paper_failure_code = (  # type: ignore[attr-defined]
            code if code in MANUAL_TRIGGER_CODES else "MANUAL_RESULT_UNKNOWN"
        )
    except Exception:
        return


async def dispatch_manual_request(
    settings: EngineSettings,
    trigger_settings: "AgentPaperTriggerSettings",
    *,
    request_id: UUID,
    repo_root: Path,
    calendar_authority_id: UUID,
    process_environment: Mapping[str, str],
) -> None:
    """Commit the claim before normal trigger; uncertain preparation is never replayed."""
    from nexus.ops.agent_paper_once import (
        AgentPaperTriggerRequest,
        _validate_trigger_dependencies,
        trigger_once,
    )
    from nexus.paper.manual_entry import manual_host_scope

    if manual_host_scope(settings) != manual_host_scope(trigger_settings):
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    _validate_trigger_dependencies(trigger_settings)
    factory = session_factory(settings)
    failure: ValueError | None = None
    try:
        async with factory.begin() as session:
            _, claimed = await claim_configured_manual_request(
                session, settings, request_id=request_id
            )
        if not claimed:
            return
        assert settings.agent_paper_account_id is not None
        assert settings.agent_paper_candidate_batch_id is not None
        try:
            await trigger_once(
                trigger_settings,
                AgentPaperTriggerRequest(
                    action="trigger",
                    trigger_mode="manual",
                    manual_request_id=request_id,
                    account_id=settings.agent_paper_account_id,
                    candidate_batch_id=settings.agent_paper_candidate_batch_id,
                    formal_stage_capacity=True,
                    formal_model_config_id=settings.agent_paper_formal_model_config_id,
                    idempotency_key=f"manual-entry:{request_id}",
                ),
                repo_root=repo_root,
                expected_calendar_authority_id=calendar_authority_id,
                process_environment=process_environment,
            )
        except Exception as error:
            failure = ValueError("MANUAL_RESULT_UNKNOWN")
            phase = getattr(error, "_nexus_agent_paper_failure_phase", "trigger_request")
            annotate_manual_trigger_failure(error, phase=phase)
            code = getattr(error, "_nexus_agent_paper_failure_code", None)
            annotate_manual_trigger_failure(failure, phase=phase)
            if code in MANUAL_TRIGGER_CODES:
                setattr(failure, "_nexus_agent_paper_failure_code", code)
            kind = getattr(error, "_nexus_agent_paper_failure_kind", None)
            if kind in MANUAL_TRIGGER_KINDS:
                setattr(failure, "_nexus_agent_paper_failure_kind", kind)
            try:
                async with factory.begin() as session:
                    row = await session.get(ManualPaperRequest, request_id)
                    if row is not None:
                        view = await read_manual_request(session, row)
                        if view.status == "preparing":
                            await record_manual_request_event(
                                session, row, status="unknown", code="MANUAL_RESULT_UNKNOWN"
                            )
            except Exception:
                # A secondary persistence failure cannot replace the primary safe evidence.
                # A preparing request also remains non-replayable if this transaction fails.
                pass
            raise failure from None
    finally:
        try:
            await factory.kw["bind"].dispose()
        except Exception:
            if failure is None:
                raise


def bind_manual_queue(
    snapshot: "AgentPaperLoopSnapshot", request: ManualRequestView
) -> "AgentPaperLoopSnapshot":
    if request.status == "active":
        if request.cycle_id != snapshot.cycle_id:
            raise ValueError("MANUAL_RESULT_UNKNOWN")
        return snapshot
    if request.status in {"queued", "preparing", "unknown"}:
        return replace(
            snapshot,
            manual_pending_request_id=request.request_id,
            manual_pending_status=request.status,
        )
    return snapshot


async def inspect_manual_queue(
    settings: EngineSettings, *, account_id: UUID, snapshot: "AgentPaperLoopSnapshot"
) -> "AgentPaperLoopSnapshot":
    if not settings.agent_paper_manual_enabled:
        return snapshot
    if settings.agent_paper_account_id != account_id:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            view = await peek_manual_request(session, account_id=account_id)
            return snapshot if view is None else bind_manual_queue(snapshot, view)
    finally:
        await factory.kw["bind"].dispose()


def bind_manual_close(
    snapshot: "AgentPaperLoopSnapshot", request: ManualRequestView
) -> "AgentPaperLoopSnapshot":
    """A projected completion schedules a write; it never creates completion facts."""
    if request.status != "active":
        return snapshot
    if request.cycle_id is None or request.cycle_id != snapshot.cycle_id:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    if snapshot.cycle_status not in {"completed", "failed", "unavailable"}:
        return snapshot
    if (
        snapshot.evaluation_id is not None
        or snapshot.cycle_status == "completed"
        and request.mode != "research_and_paper"
    ):
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    return replace(snapshot, manual_finish_request_id=request.request_id)


async def inspect_manual_close(
    settings: EngineSettings, *, account_id: UUID, snapshot: "AgentPaperLoopSnapshot"
) -> "AgentPaperLoopSnapshot":
    from nexus.paper.manual_request import manual_request_for_cycle

    if (
        settings.agent_paper_account_id is None
        or snapshot.cycle_id is None
        or snapshot.cycle_status not in {"completed", "failed", "unavailable"}
    ):
        return snapshot
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            row = await manual_request_for_cycle(session, cycle_id=snapshot.cycle_id)
            if row is None:
                return snapshot
            if row.account_id != account_id or settings.agent_paper_account_id != account_id:
                raise ValueError("MANUAL_RESULT_UNKNOWN")
            return bind_manual_close(snapshot, await read_manual_request(session, row))
    finally:
        await factory.kw["bind"].dispose()


async def peek_manual_request(
    session: AsyncSession, *, account_id: UUID
) -> ManualRequestView | None:
    latest_status = (
        select(ManualPaperRequestEvent.status)
        .where(ManualPaperRequestEvent.request_id == ManualPaperRequest.id)
        .order_by(ManualPaperRequestEvent.sequence.desc())
        .limit(1)
        .scalar_subquery()
    )
    rows = (
        await session.scalars(
            select(ManualPaperRequest).where(
                ManualPaperRequest.account_id == account_id,
                latest_status.is_(None) | latest_status.not_in(("completed", "unavailable")),
            )
        )
    ).all()
    from nexus.paper.manual_preparation_close import preparation_closed

    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    pending = []
    for row in rows:
        view = await read_manual_request(session, row)
        if view.status == "unknown" and await preparation_closed(session, row, now=now):
            continue
        pending.append(view)
    from nexus.paper.preparation_unknown_ack import require_acknowledged_native_claims

    await require_acknowledged_native_claims(
        session,
        account_id=account_id,
        now=now,
        excluding_request_id=pending[0].request_id
        if len(pending) == 1 and pending[0].status == "preparing"
        else None,
    )
    if len(pending) > 1:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    return None if not pending else pending[0]


async def manual_work_pending(settings: EngineSettings, *, account_id: UUID) -> bool:
    """Read-only wakeup signal, never permission to collect or resume unknown work."""
    if not settings.agent_paper_manual_enabled:
        return False
    if account_id != settings.agent_paper_account_id:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            return await peek_manual_request(session, account_id=account_id) is not None
    finally:
        await factory.kw["bind"].dispose()


async def claim_configured_manual_request(
    session: AsyncSession, settings: EngineSettings, *, request_id: UUID
) -> tuple[ManualRequestView, bool]:
    """Seal preparation once, or close a queued request that must not roll to tomorrow."""
    account_id = settings.agent_paper_account_id
    if account_id is None:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    await lock_portfolio_account(session, account_id)
    row = await session.get(ManualPaperRequest, request_id)
    if row is None or row.account_id != account_id:
        raise ValueError("MANUAL_NOT_FOUND")
    view = await read_manual_request(session, row)
    if view.status != "queued":
        return view, False
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    available = (
        settings.agent_paper_cycle_enabled
        and settings.agent_paper_manual_enabled
        and not settings.formal_budget_protection_enabled
        and row.candidate_batch_id == settings.agent_paper_candidate_batch_id
        and row.model_config_id == settings.agent_paper_formal_model_config_id
        and row.requested_at.date() == now.date()
    )
    if available:
        try:
            calendar = await select_agent_paper_service_calendar_authority(session, now=now)
        except ValueError:
            available = False
        else:
            available = (
                calendar.local_date == calendar.session
                and calendar.market_open_at <= now < calendar.market_close_at
            )
    if not available:
        return await record_manual_request_event(
            session, row, status="unavailable", code="MANUAL_INPUTS_UNAVAILABLE"
        ), False
    return await claim_manual_request(session, request_id=request_id)


async def finish_configured_manual_request(
    session: AsyncSession,
    settings: EngineSettings,
    *,
    request_id: UUID,
    cancel_unused: bool = False,
) -> ManualRequestView:
    """Explicit persisted close via native execution facts, never a read-side action."""
    from nexus.paper.agent_cycle import finish_manual_paper_cycle

    account_id = settings.agent_paper_account_id
    if account_id is None:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    await lock_portfolio_account(session, account_id)
    row = await session.get(ManualPaperRequest, request_id)
    if row is None or row.account_id != account_id:
        raise ValueError("MANUAL_NOT_FOUND")
    view = await read_manual_request(session, row)
    if view.status in {"completed", "unavailable"}:
        return view
    if view.status != "active" or view.cycle_id is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    from nexus.paper.manual_request import close_failed_manual_request
    from nexus.persistence.models.paper import AgentPaperCycleEventRecord

    native_status = await session.scalar(
        select(AgentPaperCycleEventRecord.to_status)
        .where(AgentPaperCycleEventRecord.cycle_id == view.cycle_id)
        .order_by(AgentPaperCycleEventRecord.sequence.desc())
        .limit(1)
    )
    if native_status in {"failed", "unavailable"}:
        return await close_failed_manual_request(
            session, request_id=request_id, cancel_unused=cancel_unused
        )
    if view.mode != "research_and_paper":
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    await finish_manual_paper_cycle(session, cycle_id=view.cycle_id, now=now)
    return await read_manual_request(session, row)
