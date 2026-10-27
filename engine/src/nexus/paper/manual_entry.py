"""Protected manual admission, shared by HTTP and CLI; never collects or executes."""

import re
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Protocol
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.jobs.models import job_input_hash
from nexus.paper.agent_cycle_authority import select_agent_paper_service_calendar_authority
from nexus.paper.manual_request import (
    ManualMode,
    ManualRequestView,
    admit_manual_request,
    read_manual_request,
)
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.settings import EngineSettings

# Advertised only by a host that implements timely queue inspection and dispatch.
# Disabled or incompletely bound hosts keep the scheduled-only protocol.
MANUAL_HOST_PROTOCOL = "nexus-manual-paper-v1"
SAFE_ENTRY_CODES = frozenset(
    {
        "MANUAL_ENTRY_DISABLED",
        "MANUAL_CONFIGURATION_UNAVAILABLE",
        "MANUAL_HOST_NOT_READY",
        "MANUAL_HOST_EXIT_UNCONFIRMED",
        "MANUAL_SESSION_UNAVAILABLE",
        "MANUAL_BUDGET_PROTECTED",
        "MANUAL_NOT_FOUND",
        "MANUAL_IDEMPOTENCY_CONFLICT",
        "MANUAL_REQUEST_INVALID",
        "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
        "MANUAL_RESULT_UNKNOWN",
        "MANUAL_PREPARATION_CLOSE_BLOCKED",
        "MANUAL_PREPARATION_RECOVERY_BLOCKED",
        "MANUAL_PREPARATION_ACK_BLOCKED",
        "SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED",
    }
)


class HostHeartbeat(Protocol):
    worker_id: str
    seen_at: datetime
    started_at: datetime
    process_version: str
    migration_head: str


def require_manual_host(
    rows: Sequence[HostHeartbeat], *, now: datetime, release_revision: str | None, scope_hash: str
) -> None:
    if (
        release_revision is None
        or re.fullmatch(r"[0-9a-f]{40}", release_revision) is None
        or re.fullmatch(r"[0-9a-f]{64}", scope_hash) is None
    ):
        raise ValueError("MANUAL_HOST_NOT_READY")
    live = [row for row in rows if row.seen_at >= now - timedelta(seconds=30)]
    if len(live) != 1:
        raise ValueError("MANUAL_HOST_NOT_READY")
    row = live[0]
    if (
        row.seen_at > now
        or row.started_at > row.seen_at
        or row.process_version != f"{MANUAL_HOST_PROTOCOL}+{release_revision}"
        or row.migration_head != CURRENT_MIGRATION_HEAD
        or re.fullmatch(
            rf"agent-paper-loop-[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}:manual:{scope_hash}",
            row.worker_id,
        )
        is None
    ):
        raise ValueError("MANUAL_HOST_NOT_READY")


def manual_host_scope(settings: EngineSettings) -> str:
    """Bind consumption capability to the same protected scope without exposing IDs."""
    return job_input_hash(
        {
            "contract": "manual_paper_host_scope_v1",
            "account_id": str(settings.agent_paper_account_id),
            "batch_id": str(settings.agent_paper_candidate_batch_id),
            "model_id": str(settings.agent_paper_formal_model_config_id),
            "budget_protection": settings.formal_budget_protection_enabled,
            "manual_enabled": settings.agent_paper_manual_enabled,
            "runtime_mode": settings.agent_runtime_mode,
        }
    )


async def _read_presented_manual_request(
    session: AsyncSession, row: ManualPaperRequest
) -> ManualRequestView:
    # Keep the canonical event reader independent: acknowledgement validation
    # uses it to verify the original unknown facts and must never recurse.
    view = await read_manual_request(session, row)
    if row.contract_version in {2, 3}:
        return view
    from nexus.paper.current_preparation_acceptance import current_unknown_accepted

    if await current_unknown_accepted(session, row):
        return view.model_copy(update={"preparation_disposition": "unknown_acknowledged"})
    from nexus.paper.current_preparation_recovery import read_preparation_recovery

    recovery = await read_preparation_recovery(session, row)
    if recovery is not None:
        return view.model_copy(update={"preparation_recovery": recovery})
    if view.code == "MANUAL_PREPARATION_CANCELLED":
        raise ValueError("MANUAL_PREPARATION_RECOVERY_BLOCKED")
    if view.status != "unknown" or view.cycle_id is not None:
        return view
    from nexus.paper.preparation_unknown_ack import acknowledged_request

    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    if await acknowledged_request(session, row, now=now):
        return view.model_copy(update={"preparation_disposition": "unknown_acknowledged"})
    return view


async def read_configured_manual_request(
    session: AsyncSession, settings: EngineSettings, *, request_id: UUID
) -> ManualRequestView:
    row = await session.get(ManualPaperRequest, request_id)
    if row is None or row.account_id != settings.agent_paper_account_id:
        raise ValueError("MANUAL_NOT_FOUND")
    return await _read_presented_manual_request(session, row)


async def lookup_configured_manual_request(
    session: AsyncSession, settings: EngineSettings, *, idempotency_key: str
) -> ManualRequestView | None:
    """Locate persisted intent only; absence is not permission to repeat a write."""
    if settings.agent_paper_account_id is None:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    if not idempotency_key.strip() or len(idempotency_key) > 255:
        raise ValueError("MANUAL_REQUEST_INVALID")
    row = await session.scalar(
        select(ManualPaperRequest).where(
            ManualPaperRequest.account_id == settings.agent_paper_account_id,
            ManualPaperRequest.key_hash == sha256(idempotency_key.encode()).hexdigest(),
        )
    )
    return None if row is None else await _read_presented_manual_request(session, row)


async def start_configured_manual_request(
    session: AsyncSession, settings: EngineSettings, *, mode: ManualMode, idempotency_key: str
) -> ManualRequestView:
    account_id = settings.agent_paper_account_id
    if account_id is None:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    if not idempotency_key.strip() or len(idempotency_key) > 255:
        raise ValueError("MANUAL_REQUEST_INVALID")
    await lock_portfolio_account(session, account_id)
    row = await session.scalar(
        select(ManualPaperRequest).where(
            ManualPaperRequest.account_id == account_id,
            ManualPaperRequest.key_hash == sha256(idempotency_key.encode()).hexdigest(),
        )
    )
    # A response lost in transport is a read of the original immutable intent,
    # including after disabling the entry or rotating its configured batch/model.
    if row is not None:
        if row.mode != mode:
            raise ValueError("MANUAL_IDEMPOTENCY_CONFLICT")
        return await _read_presented_manual_request(session, row)
    if not settings.agent_paper_manual_enabled or not settings.agent_paper_cycle_enabled:
        raise ValueError("MANUAL_ENTRY_DISABLED")
    batch_id = settings.agent_paper_candidate_batch_id
    model_id = settings.agent_paper_formal_model_config_id
    if batch_id is None or model_id is None:
        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
    if settings.formal_budget_protection_enabled:
        raise ValueError("MANUAL_BUDGET_PROTECTED")
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise ValueError("MANUAL_RESULT_UNKNOWN")
    now = now.astimezone(UTC)
    from nexus.paper.preparation_unknown_ack import require_acknowledged_operation_scope

    await require_acknowledged_operation_scope(
        session, account_id=account_id, model_config_id=model_id, now=now
    )
    from nexus.paper.manual_host_exit import valid_host_exit

    heartbeats = (
        await session.scalars(
            select(WorkerHeartbeat).where(
                WorkerHeartbeat.queue == "agent_paper",
                WorkerHeartbeat.seen_at >= now - timedelta(seconds=30),
                ~valid_host_exit(),
            )
        )
    ).all()
    require_manual_host(
        heartbeats,
        now=now,
        release_revision=settings.release_revision,
        scope_hash=manual_host_scope(settings),
    )
    if mode not in {"research_only", "research_and_monitor"}:
        try:
            calendar = await select_agent_paper_service_calendar_authority(session, now=now)
        except ValueError:
            raise ValueError("MANUAL_SESSION_UNAVAILABLE") from None
        # Current same-session quote reader does not support an overnight research
        # contract. Reject it explicitly; a queued click is never tomorrow's order.
        if calendar.local_date != calendar.session or not (
            calendar.market_open_at <= now < calendar.market_close_at
        ):
            raise ValueError("MANUAL_SESSION_UNAVAILABLE")
    from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    from nexus.persistence.models.agents import AgentModelConfigRecord

    model = await session.get(AgentModelConfigRecord, model_id)
    return await admit_manual_request(
        session,
        account_id=account_id,
        batch_id=batch_id,
        model_config_id=model_id,
        mode=mode,
        idempotency_key=idempotency_key,
        contract_version=3
        if mode == "research_and_monitor"
        else 2
        if mode == "research_only"
        else 1,
        runtime_mode=""
        if settings.agent_runtime_mode is None
        else settings.agent_runtime_mode.value,
        daily_limit_usd=settings.agent_daily_budget_usd,
        two_flight=(
            mode in {"research_only", "research_and_monitor"}
            and model is not None
            and model.version == BOUNDED_STAGE_BUDGET_CONFIG_VERSION
        ),
    )
