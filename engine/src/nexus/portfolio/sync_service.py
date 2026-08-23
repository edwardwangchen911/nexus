"""Read-only broker sync admission and redacted run projections."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.portfolio import BrokerSyncRunResponse
from nexus.persistence.models.portfolio import BrokerSyncRun, Portfolio
from nexus.reconciliation.persistence import (
    BrokerSyncPersistenceError,
    BrokerSyncRepository,
    IdempotencyConflict,
    SyncCooldownActive,
)

_IDEMPOTENCY_ROUTE = "/internal/v1/broker-sync-runs"
_MANUAL_COOLDOWN_SECONDS = 60


class BrokerSyncApiService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def admit(
        self,
        *,
        portfolio_id: UUID,
        actor_id: str,
        idempotency_key: str,
        request_hash: str,
        accepted_at: datetime,
    ) -> BrokerSyncRunResponse:
        account_id = await self._session.scalar(
            select(Portfolio.account_id).where(Portfolio.id == portfolio_id)
        )
        if account_id is None:
            raise _not_found("portfolio", portfolio_id)
        try:
            admission = await BrokerSyncRepository(self._session).admit(
                account_id=account_id,
                portfolio_id=portfolio_id,
                trigger_kind="MANUAL",
                actor_id=actor_id,
                route=_IDEMPOTENCY_ROUTE,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                accepted_at=accepted_at,
            )
        except IdempotencyConflict as error:
            raise NexusError(
                ErrorCode.IDEMPOTENCY_CONFLICT,
                "The idempotency key conflicts with an earlier request.",
                status_code=409,
            ) from error
        except SyncCooldownActive as error:
            raise NexusError(
                ErrorCode.VALIDATION_ERROR,
                "The manual broker sync cooldown is active.",
                status_code=429,
                details={"cooldown_seconds": _MANUAL_COOLDOWN_SECONDS},
            ) from error
        except BrokerSyncPersistenceError as error:
            raise NexusError(
                ErrorCode.VALIDATION_ERROR,
                "The broker sync request could not be admitted.",
                status_code=422,
            ) from error
        return _run_response(admission.run)

    async def get(self, run_id: UUID) -> BrokerSyncRunResponse:
        row = (
            await self._session.execute(
                select(
                    BrokerSyncRun.id.label("run_id"),
                    BrokerSyncRun.portfolio_id,
                    BrokerSyncRun.status,
                    BrokerSyncRun.trigger_kind,
                    BrokerSyncRun.accepted_at,
                    BrokerSyncRun.completed_at,
                    BrokerSyncRun.attempt_count,
                    BrokerSyncRun.cash_flow_quality,
                )
                .where(BrokerSyncRun.id == run_id)
                .order_by(desc(BrokerSyncRun.id))
                .limit(1)
            )
        ).first()
        if row is None:
            raise _not_found("broker sync run", run_id)
        return BrokerSyncRunResponse(
            broker_sync_run_id=row.run_id,
            portfolio_id=row.portfolio_id,
            status=row.status,
            trigger_kind=row.trigger_kind,
            accepted_at=row.accepted_at,
            completed_at=row.completed_at,
            attempt_count=row.attempt_count,
            cash_flow_quality=row.cash_flow_quality,
            failure_category="SYNC_FAILED" if row.status == "FAILED" else None,
        )


def _run_response(run: BrokerSyncRun) -> BrokerSyncRunResponse:
    return BrokerSyncRunResponse.model_validate(
        {
            "broker_sync_run_id": run.id,
            "portfolio_id": run.portfolio_id,
            "status": run.status,
            "trigger_kind": run.trigger_kind,
            "accepted_at": run.accepted_at,
            "completed_at": run.completed_at,
            "attempt_count": run.attempt_count,
            "cash_flow_quality": run.cash_flow_quality,
            "failure_category": "SYNC_FAILED" if run.status == "FAILED" else None,
        }
    )


def _not_found(resource: str, resource_id: UUID) -> NexusError:
    return NexusError(
        ErrorCode.NOT_FOUND,
        f"The {resource} was not found.",
        status_code=404,
        details={"id": str(resource_id)},
    )
