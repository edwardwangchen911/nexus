"""Explicit one-root priority; never a recurring budget exemption or a new credit."""

from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import AuditEvent
from nexus.settings import AgentRuntimeMode, EngineSettings

INVALID = "INDEPENDENT_PRIORITY_INVALID"
EXHAUSTED = "INDEPENDENT_SHARED_BUDGET_EXHAUSTED"
ACTION = "paper.independent_priority.authorized"
CONSUMED = "paper.independent_priority.consumed"
ACTOR = "local-independent-priority"
SCHEMA = "independent_priority_v1"
SUCCESSOR_SCHEMA = "independent_priority_v2"
Policy = Literal["one_root_priority_v1", "one_root_priority_successor_v1"]
LIMIT = Decimal("13")
MAXIMUM = Decimal("6.36576")


class Grant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    policy: Policy = "one_root_priority_v1"
    mode: Literal["research_and_monitor"] = "research_and_monitor"
    contract_version: Literal[3] = 3
    account_id: UUID
    key_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    batch_id: UUID
    batch_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    model_id: UUID
    model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    budget_day: date
    authorized_at: datetime
    expires_at: datetime
    daily_limit: Decimal
    root_maximum: Decimal


class Predecessor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: UUID
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    grant_id: UUID
    grant_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    acceptance_id: UUID
    acceptance_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    acceptance_schema: str


class SuccessorGrant(Grant):
    policy: Policy = "one_root_priority_successor_v1"
    predecessor: Predecessor


def parse_grant(event: AuditEvent) -> Grant:
    try:
        if event.schema_version == SCHEMA:
            grant = Grant.model_validate(event.payload)
            if grant.policy != "one_root_priority_v1":
                raise ValueError(INVALID)
        elif event.schema_version == SUCCESSOR_SCHEMA:
            grant = SuccessorGrant.model_validate(event.payload)
            if grant.policy != "one_root_priority_successor_v1":
                raise ValueError(INVALID)
        else:
            raise ValueError(INVALID)
        return grant
    except ValidationError:
        raise ValueError(INVALID) from None


async def capture_predecessor(
    session: AsyncSession, *, request_id: UUID, account: UUID, day: date, now: datetime
) -> Predecessor:
    from nexus.paper import independent_unknown_acceptance as acceptance

    row = await session.get(ManualPaperRequest, request_id, populate_existing=True)
    if (
        row is None
        or row.account_id != account
        or row.requested_at.date() != day
        or row.contract_version != 3
        or row.mode != "research_and_monitor"
    ):
        raise ValueError(INVALID)
    ref = binding(row)
    if ref is None:
        raise ValueError(INVALID)
    events = await _events(session, account, row.key_hash)
    if len(events) != 1:
        raise ValueError(INVALID)
    event = events[0]
    await validate_audit(session, event, ACTION)
    grant = parse_grant(event)
    historical = await _historical_receipt(session, event, grant, now=now)
    if (
        event.id != ref.audit_id
        or event.event_hash != ref.event_hash
        or grant.budget_day != day
        or historical.consumed_request_id != row.id
    ):
        raise ValueError(INVALID)
    audits = await acceptance._audits(session, row.id)
    if len(audits) != 1:
        raise ValueError(INVALID)
    # This explicit continuation supports accepted unknown predecessors only.
    # The original versioned validator proves terminal state and retained money.
    await acceptance._validate(session, row=row, audit=audits[0], now=now)
    return Predecessor(
        request_id=row.id,
        request_hash=row.request_hash,
        grant_id=event.id,
        grant_hash=event.event_hash,
        acceptance_id=audits[0].id,
        acceptance_hash=audits[0].event_hash,
        acceptance_schema=audits[0].schema_version,
    )


async def recheck_predecessor(session: AsyncSession, grant: Grant, now: datetime) -> None:
    if isinstance(grant, SuccessorGrant):
        current = await capture_predecessor(
            session,
            request_id=grant.predecessor.request_id,
            account=grant.account_id,
            day=grant.budget_day,
            now=now,
        )
        if current != grant.predecessor:
            raise ValueError(INVALID)
        children = list(
            await session.scalars(
                select(AuditEvent).where(
                    AuditEvent.action == ACTION,
                    AuditEvent.payload["predecessor"]["grant_id"].astext == str(current.grant_id),
                )
            )
        )
        if len(children) != 1:
            raise ValueError(INVALID)
        await validate_audit(session, children[0], ACTION)
        if parse_grant(children[0]) != grant:
            raise ValueError(INVALID)


class Reference(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    policy: Policy = "one_root_priority_v1"
    audit_id: UUID
    event_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class PriorityReceipt(BaseModel):
    status: Literal["independent_priority_registered"] = "independent_priority_registered"
    audit_id: UUID
    expires_at: datetime
    consumed_request_id: UUID | None = None
    grant_unconsumed: bool
    expired: bool
    budget_reserved: Literal[False] = False
    execution_locked: Literal[True] = True
    external_requests: Literal[0] = 0


def binding(row: ManualPaperRequest) -> Reference | None:
    value = (row.research_profile or {}).get("budget_priority")
    if value is None:
        return None
    try:
        result = Reference.model_validate(value)
    except ValidationError:
        raise ValueError(INVALID) from None
    if row.contract_version != 3 or row.mode != "research_and_monitor":
        raise ValueError(INVALID)
    return result


async def validate_audit(session: AsyncSession, event: AuditEvent, action: str) -> None:
    canonical = canonical_audit_event(
        AuditChainInput(
            event_seq=event.event_seq,
            occurred_at=event.occurred_at,
            actor_id=event.actor_id,
            action=event.action,
            schema_version=event.schema_version,
            payload=event.payload,
        )
    )
    previous = await session.scalar(
        select(AuditEvent.event_hash).where(AuditEvent.event_seq == event.event_seq - 1)
    )
    if (
        event.actor_id != ACTOR
        or event.action != action
        or event.schema_version
        not in ((SCHEMA, SUCCESSOR_SCHEMA) if action == ACTION else (SCHEMA,))
        or event.chain_version != "audit_jcs_sha256_v1"
        or event.canonical_event != canonical
        or event.previous_hash != ("0" * 64 if event.event_seq == 1 else previous)
        or event.event_hash != audit_event_hash(event.previous_hash, canonical)
    ):
        raise ValueError(INVALID)


async def _events(session: AsyncSession, account: UUID, key_hash: str) -> list[AuditEvent]:
    return list(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == ACTION,
                AuditEvent.payload["account_id"].astext == str(account),
                AuditEvent.payload["key_hash"].astext == key_hash,
            )
        )
    )


async def resolve(
    session: AsyncSession,
    *,
    account_id: UUID,
    batch_id: UUID,
    batch_hash: str,
    model_id: UUID,
    model_hash: str,
    daily_limit: Decimal,
    maximum: Decimal,
    now: datetime,
    key_hash: str | None = None,
    row: ManualPaperRequest | None = None,
) -> tuple[Reference, Grant] | None:
    expected = None if row is None else binding(row)
    key_hash = row.key_hash if row is not None else key_hash
    if key_hash is None:
        return None
    events = await _events(session, account_id, key_hash)
    if not events:
        if expected is not None:
            raise ValueError(INVALID)
        return None
    if len(events) != 1:
        raise ValueError(INVALID)
    event = events[0]
    await validate_audit(session, event, ACTION)
    try:
        grant = parse_grant(event)
    except ValidationError:
        raise ValueError(INVALID) from None
    await recheck_predecessor(session, grant, now)
    reference = Reference(policy=grant.policy, audit_id=event.id, event_hash=event.event_hash)
    if (
        grant.account_id != account_id
        or grant.batch_id != batch_id
        or grant.batch_hash != batch_hash
        or grant.model_id != model_id
        or grant.model_hash != model_hash
        or grant.daily_limit != daily_limit
        or daily_limit != LIMIT
        or grant.root_maximum != 3 * maximum
        or grant.root_maximum > MAXIMUM
        or grant.budget_day != now.date()
        or grant.authorized_at != event.occurred_at
        or grant.authorized_at.utcoffset() is None
        or grant.expires_at.utcoffset() is None
        or not grant.authorized_at <= now < grant.expires_at
        or row is not None
        and (
            expected != reference
            or row.requested_at < grant.authorized_at
            or row.requested_at.date() != grant.budget_day
        )
    ):
        raise ValueError(INVALID)
    consumed = list(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == CONSUMED,
                AuditEvent.payload["grant_id"].astext == str(event.id),
            )
        )
    )
    if row is None:
        if consumed or await session.scalar(
            select(ManualPaperRequest.id).where(
                ManualPaperRequest.account_id == account_id, ManualPaperRequest.key_hash == key_hash
            )
        ):
            raise ValueError(INVALID)
    else:
        if len(consumed) != 1:
            raise ValueError(INVALID)
        await validate_audit(session, consumed[0], CONSUMED)
        if (
            consumed[0].payload != consumption(row, reference)
            or not row.requested_at <= consumed[0].occurred_at <= now
        ):
            raise ValueError(INVALID)
    if await session.scalar(
        text("SELECT 1 FROM single_sol_budget_scopes WHERE budget_day=:day"), {"day": now.date()}
    ):
        raise ValueError(INVALID)
    return reference, grant


def consumption(row: ManualPaperRequest, ref: Reference) -> dict[str, object]:
    return {
        "grant_id": str(ref.audit_id),
        "grant_hash": ref.event_hash,
        "request_id": str(row.id),
        "request_hash": row.request_hash,
        "key_hash": row.key_hash,
    }


async def consume(session: AsyncSession, row: ManualPaperRequest, *, now: datetime) -> None:
    ref = binding(row)
    if ref is not None:
        await append_audit_event(
            session,
            actor_id=ACTOR,
            action=CONSUMED,
            schema_version=SCHEMA,
            payload=consumption(row, ref),
            occurred_at=now,
        )


async def require_no_active_priority(session: AsyncSession, *, account_id: UUID) -> None:
    # Same account/day locks as grant/root creation. Pending grants do not occupy money.
    if await session.scalar(
        text("""SELECT 1 FROM manual_paper_requests r
        WHERE r.account_id=:account AND r.research_profile ? 'budget_priority'
        AND (SELECT e.status FROM manual_paper_request_events e WHERE e.request_id=r.id
             ORDER BY e.sequence DESC LIMIT 1) IN ('queued','preparing','active') LIMIT 1"""),
        {"account": account_id},
    ):
        raise ValueError(INVALID)


async def authorize(
    session: AsyncSession,
    settings: EngineSettings,
    *,
    idempotency_key: str,
    accept_priority: bool,
    predecessor_request_id: UUID | None = None,
) -> PriorityReceipt:
    from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
    from nexus.agents.research_maintenance import require_research_open
    from nexus.paper.agent_cycle_authority import select_agent_paper_service_calendar_authority
    from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
    from nexus.paper.independent_history_scope import (
        _locks,
        capture,
        execution_boundary,
        require_capacity,
    )
    from nexus.paper.manual_request import require_manual_account_available
    from nexus.paper.research_request import database_now

    require_research_open()
    account = settings.agent_paper_account_id
    batch_id = settings.agent_paper_candidate_batch_id
    model_id = settings.agent_paper_formal_model_config_id
    if (
        accept_priority is not True
        or account is None
        or batch_id is None
        or model_id is None
        or settings.agent_daily_budget_usd != LIMIT
        or not settings.agent_paper_manual_enabled
        or not settings.agent_paper_cycle_enabled
        or settings.research_admission_mode != AgentRuntimeMode.RESPONSES
        or settings.formal_budget_protection_enabled
        or not idempotency_key.strip()
        or len(idempotency_key) > 255
    ):
        raise ValueError(INVALID)
    now = await database_now(session)
    await _locks(session, account, now)
    key_hash = sha256(idempotency_key.encode()).hexdigest()
    batch, _ = await validate_agent_paper_candidate_batch(
        session, account_id=account, batch_id=batch_id, as_of=now
    )
    model = await require_formal_stage_model_profile(session, model_config_id=model_id, now=now)
    events = await _events(session, account, key_hash)
    if events:
        if len(events) != 1:
            raise ValueError(INVALID)
        event = events[0]
        await validate_audit(session, event, ACTION)
        try:
            grant = parse_grant(event)
        except ValidationError:
            raise ValueError(INVALID) from None
        if (
            grant.account_id != account
            or grant.batch_id != batch_id
            or grant.batch_hash != batch.batch_hash
            or grant.model_id != model_id
            or grant.model_hash != model.config_hash
            or grant.daily_limit != LIMIT
            or grant.root_maximum != 3 * model.worst_case_cost_usd
            or grant.root_maximum > MAXIMUM
        ):
            raise ValueError(INVALID)
        expected_predecessor = (
            grant.predecessor.request_id if isinstance(grant, SuccessorGrant) else None
        )
        if predecessor_request_id != expected_predecessor:
            raise ValueError(INVALID)
        return await _historical_receipt(session, event, grant, now=now)
    # Unknown versions in today's authorization set cannot disappear from the
    # uniqueness decision merely because they use another payload layout.
    daily_events = list(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == ACTION,
                AuditEvent.payload["account_id"].astext == str(account),
                AuditEvent.payload["budget_day"].astext == now.date().isoformat(),
            )
        )
    )
    for daily_event in daily_events:
        await validate_audit(session, daily_event, ACTION)
        parse_grant(daily_event)
    predecessor = None
    if predecessor_request_id is not None:
        predecessor = await capture_predecessor(
            session, request_id=predecessor_request_id, account=account, day=now.date(), now=now
        )
        # Never recycle a predecessor, even after its successor expired or failed.
        successors = await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == ACTION,
                AuditEvent.payload["predecessor"]["grant_id"].astext == str(predecessor.grant_id),
            )
        )
        if list(successors):
            raise ValueError(INVALID)
    if predecessor is None and await session.scalar(
        select(AuditEvent.id).where(
            AuditEvent.action == ACTION,
            AuditEvent.payload["account_id"].astext == str(account),
            AuditEvent.payload["budget_day"].astext == now.date().isoformat(),
        )
    ):
        raise ValueError(INVALID)
    if await session.scalar(
        select(ManualPaperRequest.id).where(
            ManualPaperRequest.account_id == account, ManualPaperRequest.key_hash == key_hash
        )
    ):
        raise ValueError(INVALID)
    await require_manual_account_available(session, account_id=account, now=now)
    if await capture(session, account_id=account, model_config_id=model_id, now=now) is None:
        raise ValueError(INVALID)
    calendar = await select_agent_paper_service_calendar_authority(session, now=now)
    grant = Grant(
        account_id=account,
        key_hash=key_hash,
        batch_id=batch_id,
        batch_hash=batch.batch_hash,
        model_id=model_id,
        model_hash=model.config_hash,
        budget_day=now.date(),
        authorized_at=now,
        expires_at=execution_boundary(
            now=now,
            market_open_at=calendar.market_open_at,
            session_date=calendar.session,
            pricing_expires_at=model.pricing_expires_at,
        ),
        daily_limit=LIMIT,
        root_maximum=3 * model.worst_case_cost_usd,
    )
    if grant.root_maximum > MAXIMUM:
        raise ValueError(INVALID)
    if predecessor is not None:
        grant = SuccessorGrant.model_validate(
            grant.model_dump()
            | {
                "policy": "one_root_priority_successor_v1",
                "predecessor": predecessor,
            }
        )
    event = await append_audit_event(
        session,
        actor_id=ACTOR,
        action=ACTION,
        schema_version=SUCCESSOR_SCHEMA if isinstance(grant, SuccessorGrant) else SCHEMA,
        payload=grant.model_dump(mode="json"),
        occurred_at=now,
    )
    await require_capacity(
        session,
        account_id=account,
        batch_id=batch_id,
        model_id=model_id,
        daily_limit=LIMIT,
        now=now,
        key_hash=key_hash,
    )
    return PriorityReceipt(
        audit_id=event.id, expires_at=grant.expires_at, grant_unconsumed=True, expired=False
    )


async def read_priority(
    session: AsyncSession, settings: EngineSettings, *, idempotency_key: str
) -> PriorityReceipt:
    """Historical read never creates a grant or reevaluates expired model authority."""
    from nexus.paper.research_request import database_now

    account = settings.agent_paper_account_id
    if account is None:
        raise ValueError(INVALID)
    key_hash = sha256(idempotency_key.encode()).hexdigest()
    events = await _events(session, account, key_hash)
    if len(events) != 1:
        raise ValueError(INVALID)
    event = events[0]
    await validate_audit(session, event, ACTION)
    try:
        grant = parse_grant(event)
    except ValidationError:
        raise ValueError(INVALID) from None
    return await _historical_receipt(session, event, grant, now=await database_now(session))


async def _historical_receipt(
    session: AsyncSession, event: AuditEvent, grant: Grant, *, now: datetime
) -> PriorityReceipt:
    """Shared exact consumption proof; never evaluates new dispatch eligibility."""
    roots = list(
        await session.scalars(
            select(ManualPaperRequest).where(
                ManualPaperRequest.account_id == grant.account_id,
                ManualPaperRequest.key_hash == grant.key_hash,
            )
        )
    )
    consumed = list(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == CONSUMED,
                AuditEvent.payload["grant_id"].astext == str(event.id),
            )
        )
    )
    if len(roots) != len(consumed) or len(roots) > 1:
        raise ValueError(INVALID)
    if roots:
        reference = Reference(policy=grant.policy, audit_id=event.id, event_hash=event.event_hash)
        await validate_audit(session, consumed[0], CONSUMED)
        if binding(roots[0]) != reference or consumed[0].payload != consumption(
            roots[0], reference
        ):
            raise ValueError(INVALID)
    return PriorityReceipt(
        audit_id=event.id,
        expires_at=grant.expires_at,
        consumed_request_id=roots[0].id if roots else None,
        grant_unconsumed=not roots,
        expired=now >= grant.expires_at,
    )
