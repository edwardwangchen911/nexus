"""Explicit one-session consent, kept outside the account's admitted work."""

from datetime import datetime, timedelta
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import ResearchAccountingBusy
from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
from nexus.agents.research_maintenance import require_research_open
from nexus.jobs.models import job_input_hash
from nexus.paper.agent_cycle_authority import select_agent_paper_service_calendar_authority
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.manual_entry import check_configured_formal_admission, manual_host_scope
from nexus.paper.manual_request import admit_manual_request
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.manual_paper import FormalIntradayIntent, ManualPaperRequest
from nexus.settings import EngineSettings

_WAIT_CODES = {
    "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
    "MANUAL_PREPARATION_ACK_BLOCKED",
    "MANUAL_PREPARATION_CLOSE_BLOCKED",
    "MANUAL_PREPARATION_RECOVERY_BLOCKED",
    "SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED",
    "INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED",
    "FORMAL_BUDGET_SLOT_ALREADY_CLAIMED",
}


class IntentReceipt(BaseModel):
    intent_id: UUID
    status: str
    code: str
    expires_at: datetime
    last_checked_at: datetime | None
    request_id: UUID | None
    execution_locked: Literal[True] = True


async def _now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    return now


def _view(row: FormalIntradayIntent, now: datetime) -> IntentReceipt:
    expired = row.state == "pending" and now >= row.expires_at
    return IntentReceipt(
        intent_id=row.id,
        status="expired" if expired else row.state,
        code="FORMAL_INTENT_EXPIRED" if expired else row.code,
        expires_at=row.expires_at,
        last_checked_at=row.checked_at,
        request_id=row.request_id,
    )


def _account(settings: EngineSettings) -> UUID:
    if settings.agent_paper_account_id is None:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    return settings.agent_paper_account_id


async def _scope(session: AsyncSession, settings: EngineSettings, now: datetime) -> str:
    from nexus.agents.platform_profile import sol_business_model_matches

    account = _account(settings)
    batch_id = settings.agent_paper_candidate_batch_id
    if not batch_id or not settings.agent_paper_formal_model_config_id:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    batch, _ = await validate_agent_paper_candidate_batch(
        session, account_id=account, batch_id=batch_id, as_of=now
    )
    model = await require_formal_stage_model_profile(
        session, model_config_id=settings.agent_paper_formal_model_config_id, now=now
    )
    if settings.research_admission_mode != (
        "responses" if sol_business_model_matches(model) else "codex_sdk"
    ):
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    return job_input_hash(
        {
            "mode": "research_and_paper",
            "scope": manual_host_scope(settings),
            "batch": batch.batch_hash,
            "model": model.config_hash,
            "daily_limit": str(settings.agent_daily_budget_usd.normalize()),
        }
    )


async def intent_command(
    session: AsyncSession, settings: EngineSettings, *, action: str, key: str
) -> IntentReceipt:
    account = _account(settings)
    if not key.strip() or len(key) > 255:
        raise ValueError("MANUAL_REQUEST_INVALID")
    key_hash = sha256(key.encode()).hexdigest()
    if action != "read-formal-intent":
        await lock_portfolio_account(session, account)
    row = await session.scalar(
        select(FormalIntradayIntent).where(
            FormalIntradayIntent.account_id == account, FormalIntradayIntent.key_hash == key_hash
        )
    )
    now = await _now(session)
    if row is not None:
        if action == "cancel-formal-intent" and row.state == "pending":
            row.state = "expired" if now >= row.expires_at else "cancelled"
            row.code = "FORMAL_INTENT_" + row.state.upper()
            row.checked_at = now
        return _view(row, now)
    if action != "register-formal-intent":
        raise ValueError("MANUAL_NOT_FOUND")
    require_research_open()
    if not settings.agent_paper_manual_enabled or not settings.agent_paper_cycle_enabled:
        raise ValueError("MANUAL_ENTRY_DISABLED")
    if settings.formal_budget_protection_enabled:
        raise ValueError("MANUAL_BUDGET_PROTECTED")
    if await session.scalar(
        select(ManualPaperRequest.id).where(
            ManualPaperRequest.account_id == account, ManualPaperRequest.key_hash == key_hash
        )
    ):
        raise ValueError("MANUAL_IDEMPOTENCY_CONFLICT")
    cal = await select_agent_paper_service_calendar_authority(session, now=now)
    if cal.local_date != cal.session or not cal.market_open_at <= now < cal.market_close_at:
        raise ValueError("MANUAL_SESSION_UNAVAILABLE")
    current = await session.scalar(
        select(FormalIntradayIntent).where(
            FormalIntradayIntent.account_id == account,
            FormalIntradayIntent.state.in_(("pending", "checking")),
        )
    )
    if current is not None:
        if current.state != "pending" or now < current.expires_at:
            raise ValueError("FORMAL_INTENT_ALREADY_REGISTERED")
        current.state, current.code, current.checked_at = "expired", "FORMAL_INTENT_EXPIRED", now
        await session.flush()
    digest = await _scope(session, settings, now)
    assert settings.agent_paper_candidate_batch_id and settings.agent_paper_formal_model_config_id
    now = await _now(session)
    if now >= cal.market_close_at:
        raise ValueError("MANUAL_SESSION_UNAVAILABLE")
    row = FormalIntradayIntent(
        account_id=account,
        batch_id=settings.agent_paper_candidate_batch_id,
        model_id=settings.agent_paper_formal_model_config_id,
        scope_hash=digest,
        key_hash=key_hash,
        calendar_id=cal.calendar_authority_id,
        session_date=cal.session,
        registered_at=now,
        expires_at=cal.market_close_at,
        state="pending",
        code="FORMAL_INTENT_REGISTERED",
    )
    session.add(row)
    await session.flush()
    return _view(row, now)


async def due_intent(session: AsyncSession, account_id: UUID) -> UUID | None:
    now = await _now(session)
    return (
        await session.scalars(
            select(FormalIntradayIntent.id).where(
                FormalIntradayIntent.account_id == account_id,
                FormalIntradayIntent.state == "pending",
                or_(
                    FormalIntradayIntent.checked_at.is_(None),
                    FormalIntradayIntent.expires_at <= now,
                    FormalIntradayIntent.checked_at <= now - timedelta(seconds=30),
                ),
            )
        )
    ).one_or_none()


async def intent_claim_available(
    session: AsyncSession, settings: EngineSettings, *, request_id: UUID, now: datetime
) -> bool:
    row = await session.get(FormalIntradayIntent, request_id)
    if row is None:
        return True
    if row.state != "converted" or row.request_id != request_id or now >= row.expires_at:
        return False
    cal = await select_agent_paper_service_calendar_authority(session, now=now)
    return (
        row.account_id == _account(settings)
        and cal.calendar_authority_id == row.calendar_id
        and cal.session == row.session_date == cal.local_date
        and cal.market_close_at == row.expires_at
        and await _scope(session, settings, now) == row.scope_hash
    )


async def advance_intent(
    session: AsyncSession, settings: EngineSettings, *, intent_id: UUID, convert: bool
) -> bool:
    """Persist checking first. Only its caller may convert, never reclaim checking."""
    from nexus.ops.agent_paper_loop import _portfolio_slot

    require_research_open()
    await lock_portfolio_account(session, _account(settings))
    row = await session.get(FormalIntradayIntent, intent_id, populate_existing=True)
    if (
        row is None
        or row.account_id != _account(settings)
        or row.state != ("checking" if convert else "pending")
    ):
        return False
    now = await _now(session)
    if now >= row.expires_at:
        row.checked_at = now
        row.state, row.code = "expired", "FORMAL_INTENT_EXPIRED"
        return False
    try:
        cal = await select_agent_paper_service_calendar_authority(session, now=now)
        if (
            cal.calendar_authority_id != row.calendar_id
            or cal.session != row.session_date
            or cal.local_date != cal.session
            or not cal.market_open_at <= now < cal.market_close_at
            or cal.market_close_at != row.expires_at
            or await _scope(session, settings, now) != row.scope_hash
        ):
            raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
        # The opt-in intent yields the original daily preparation and first-claim window.
        if now < _portfolio_slot(cal.decision_window_start_at) + timedelta(minutes=5):
            row.checked_at = now
            row.state, row.code = "pending", "FORMAL_INTENT_DAILY_PRIORITY"
            return False
        # Serialize both orders with the existing economic writer under G/day.
        from nexus.agents.persistence import _lock_budget_day
        from nexus.paper.formal_budget_protection import latest_binding

        if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
            raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
        await _lock_budget_day(session, now.date().isoformat())
        if await latest_binding(session, business_date=now.date()) is not None:
            row.checked_at = now
            row.state, row.code = "pending", "FORMAL_BUDGET_SLOT_ALREADY_CLAIMED"
            return False
        await check_configured_formal_admission(session, settings)
        if convert:
            async with session.begin_nested():
                await admit_manual_request(
                    session,
                    account_id=row.account_id,
                    batch_id=row.batch_id,
                    model_config_id=row.model_id,
                    mode="research_and_paper",
                    idempotency_key=str(row.id),
                    intent_id=row.id,
                    runtime_mode=str(settings.research_admission_mode),
                    daily_limit_usd=settings.agent_daily_budget_usd,
                )
    except ValueError as exc:
        code = str(exc)
        row.checked_at = now
        if code in _WAIT_CODES:
            row.state, row.code = "pending", code
            return False
        if code in {
            "MANUAL_CONFIGURATION_UNAVAILABLE",
            "MANUAL_BUDGET_PROTECTED",
            "MANUAL_ENTRY_DISABLED",
            "CANDIDATE_BATCH_INVALID",
            "CODEX_FORMAL_AUTHORITY_INVALID",
            "AGENT_PAPER_CALENDAR_UNAVAILABLE",
        }:
            row.state, row.code = "rejected", code
            return False
        # A validation failure with no trusted classification cannot be retried.
        row.state, row.code = "rejected", "FORMAL_INTENT_ADMISSION_UNCONFIRMED"
        return False
    # The caller rolls back a conversion that ran across the authorization deadline.
    if await _now(session) >= row.expires_at:
        raise ValueError("FORMAL_INTENT_EXPIRED")
    row.checked_at = now
    if convert:
        row.state, row.code, row.request_id = "converted", "FORMAL_INTENT_CONVERTED", row.id
    else:
        row.state, row.code = "checking", "FORMAL_INTENT_CONVERSION_UNCONFIRMED"
    return True


async def consume_intent(settings: EngineSettings, *, intent_id: UUID) -> None:
    """A fresh host must never resume an unconfirmed conversion."""
    from nexus.persistence.session import session_factory

    factory = session_factory(settings)
    converting = False
    try:
        async with factory.begin() as session:
            allowed = await advance_intent(session, settings, intent_id=intent_id, convert=False)
        if not allowed:
            return
        converting = True
        async with factory.begin() as session:
            await advance_intent(session, settings, intent_id=intent_id, convert=True)
    except (ResearchAccountingBusy, ValueError) as error:
        # Only known body refusals after a completed rollback may resume waiting.
        if not isinstance(error, ResearchAccountingBusy) and str(error) != "FORMAL_INTENT_EXPIRED":
            raise
        async with factory.begin() as session:
            await lock_portfolio_account(session, _account(settings))
            row = await session.get(FormalIntradayIntent, intent_id)
            if row is not None and row.state == ("checking" if converting else "pending"):
                now = await _now(session)
                row.checked_at = now
                if now >= row.expires_at:
                    row.state, row.code = "expired", "FORMAL_INTENT_EXPIRED"
                elif converting:
                    row.state, row.code = "pending", "RESEARCH_ACCOUNTING_BUSY"
                else:
                    row.code = "RESEARCH_ACCOUNTING_BUSY"
    finally:
        await factory.kw["bind"].dispose()
