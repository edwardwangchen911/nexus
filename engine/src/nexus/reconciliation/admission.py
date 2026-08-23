"""Globally idempotent, account-serialized broker sync admission."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from sqlalchemy import delete, desc, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRequest,
    BrokerSyncRun,
    Portfolio,
)
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.reconciliation.admission_claim import claim_or_replay, validate_request_identity
from nexus.reconciliation.persistence_errors import (
    BrokerSyncPersistenceError,
    SyncCooldownActive,
)

_MANUAL_COOLDOWN = timedelta(seconds=60)
TriggerKind = Literal["MANUAL", "AUTOMATIC"]


@dataclass(frozen=True)
class AdmissionResult:
    run: BrokerSyncRun
    replayed: bool
    joined_active: bool = False


async def admit_broker_sync(
    session: AsyncSession,
    *,
    account_id: UUID,
    portfolio_id: UUID,
    trigger_kind: TriggerKind,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    accepted_at: datetime,
) -> AdmissionResult:
    accepted_at = require_utc(accepted_at)
    validate_request_identity(actor_id, route, idempotency_key, request_hash)
    claim, replay = await claim_or_replay(session, actor_id, route, idempotency_key, request_hash)
    if replay is not None:
        return AdmissionResult(run=replay, replayed=True)
    try:
        return await _admit_claimed(
            session,
            claim=claim,
            account_id=account_id,
            portfolio_id=portfolio_id,
            trigger_kind=trigger_kind,
            actor_id=actor_id,
            route=route,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            accepted_at=accepted_at,
        )
    except BrokerSyncPersistenceError:
        await session.execute(delete(IdempotencyRecord).where(IdempotencyRecord.id == claim.id))
        raise


async def _admit_claimed(
    session: AsyncSession,
    *,
    claim: IdempotencyRecord,
    account_id: UUID,
    portfolio_id: UUID,
    trigger_kind: TriggerKind,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    accepted_at: datetime,
) -> AdmissionResult:
    account = await session.scalar(
        select(Account).where(Account.id == account_id).with_for_update()
    )
    if account is None or account.status != "ACTIVE":
        raise BrokerSyncPersistenceError("broker account is not active")
    portfolio = await session.scalar(
        select(Portfolio).where(
            Portfolio.id == portfolio_id,
            Portfolio.account_id == account_id,
        )
    )
    if portfolio is None:
        raise BrokerSyncPersistenceError("portfolio does not belong to broker account")
    active = await session.scalar(
        select(BrokerSyncRun).where(
            BrokerSyncRun.account_id == account_id,
            BrokerSyncRun.status == "ACTIVE",
        )
    )
    if active is not None:
        await _complete_request_claim(
            session,
            claim=claim,
            run=active,
            account_id=account_id,
            trigger_kind=trigger_kind,
            accepted_at=accepted_at,
        )
        return AdmissionResult(run=active, replayed=False, joined_active=True)
    if trigger_kind == "MANUAL":
        await _enforce_manual_cooldown(session, account_id, accepted_at)
    run = BrokerSyncRun(
        id=new_uuid7(),
        account_id=account_id,
        portfolio_id=portfolio_id,
        trigger_kind=trigger_kind,
        actor_id=actor_id,
        route=route,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        status="ACTIVE",
        accepted_at=accepted_at,
        completed_at=None,
        attempt_count=0,
        failure_reason=None,
        cash_flow_quality="NOT_RUN",
    )
    session.add(run)
    await session.flush()
    await _complete_request_claim(
        session,
        claim=claim,
        run=run,
        account_id=account_id,
        trigger_kind=trigger_kind,
        accepted_at=accepted_at,
    )
    return AdmissionResult(run=run, replayed=False)


async def _complete_request_claim(
    session: AsyncSession,
    *,
    claim: IdempotencyRecord,
    run: BrokerSyncRun,
    account_id: UUID,
    trigger_kind: TriggerKind,
    accepted_at: datetime,
) -> None:
    await session.execute(
        update(IdempotencyRecord)
        .where(IdempotencyRecord.id == claim.id)
        .values(response_payload={"broker_sync_run_id": str(run.id)})
    )
    session.add(
        BrokerSyncRequest(
            id=new_uuid7(),
            sync_run_id=run.id,
            idempotency_record_id=claim.id,
            account_id=account_id,
            trigger_kind=trigger_kind,
            accepted_at=accepted_at,
        )
    )
    await session.flush()


async def _enforce_manual_cooldown(
    session: AsyncSession, account_id: UUID, accepted_at: datetime
) -> None:
    previous = await session.scalar(
        select(BrokerSyncRequest.accepted_at)
        .join(BrokerSyncRun, BrokerSyncRun.id == BrokerSyncRequest.sync_run_id)
        .where(
            BrokerSyncRequest.account_id == account_id,
            BrokerSyncRequest.trigger_kind == "MANUAL",
            BrokerSyncRun.status == "READY",
        )
        .order_by(desc(BrokerSyncRequest.accepted_at), desc(BrokerSyncRequest.id))
        .limit(1)
    )
    if previous is not None and accepted_at - previous < _MANUAL_COOLDOWN:
        raise SyncCooldownActive("manual broker sync cooldown is active")
