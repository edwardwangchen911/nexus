"""Small facade over transactional broker persistence boundaries."""

from datetime import datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
from nexus.providers.broker.base import BrokerSyncResult
from nexus.reconciliation.admission import AdmissionResult, TriggerKind, admit_broker_sync
from nexus.reconciliation.persistence_errors import (
    BrokerSyncPersistenceError,
    IdempotencyConflict,
    SnapshotNotProposalEligible,
    SyncCooldownActive,
)
from nexus.reconciliation.proposal_guard import require_proposal_baseline
from nexus.reconciliation.snapshot_persistence import finalize_broker_sync

__all__ = [
    "AdmissionResult",
    "BrokerSyncPersistenceError",
    "BrokerSyncRepository",
    "IdempotencyConflict",
    "SnapshotNotProposalEligible",
    "SyncCooldownActive",
]


class BrokerSyncRepository:
    """Own short database transactions around external broker reads."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def admit(
        self,
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
        return await admit_broker_sync(
            self._session,
            account_id=account_id,
            portfolio_id=portfolio_id,
            trigger_kind=trigger_kind,
            actor_id=actor_id,
            route=route,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            accepted_at=accepted_at,
        )

    async def finalize(
        self,
        result: BrokerSyncResult,
        *,
        completed_at: datetime,
    ) -> PortfolioSnapshotRecord:
        return await finalize_broker_sync(
            self._session,
            result,
            completed_at=completed_at,
        )

    async def require_proposal_baseline(
        self,
        *,
        portfolio_id: UUID,
        snapshot_id: UUID,
        now: datetime,
    ) -> UUID:
        return await require_proposal_baseline(
            self._session,
            portfolio_id=portfolio_id,
            snapshot_id=snapshot_id,
            now=now,
        )
