"""One explicit immutable initial-baseline approval; no execution side effects."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_synthesis_contracts import canonical_portfolio_synthesis_hash
from nexus.audit.canonical import Hash64
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_POLICY_HASH
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.paper import PaperAccountRecord, PaperInitialTargetApprovalRecord
from nexus.persistence.models.portfolio import Account
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.risk.canonical import canonical_paper_projection_hash
from nexus.risk.models import PaperPortfolioProjection

_APPROVAL_ROUTE = "paper-initial-target-approval-v1"


class InitialTargetApprovalResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    approval_id: UUID
    account_id: UUID
    portfolio_id: UUID
    projection_as_of: datetime
    projection_hash: Hash64
    approved_by: str
    approved_at: datetime
    approval_hash: Hash64


async def lock_portfolio_account(session: AsyncSession, account_id: UUID) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1818))"),
        {"scope": f"internal-paper:{account_id}"},
    )


def require_unoccupied_projection(projection: PaperPortfolioProjection) -> None:
    if (
        projection.open_orders
        or projection.reserved_cash != 0
        or any(position.reserved_quantity != 0 for position in projection.positions)
    ):
        raise ValueError("INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN")
    if canonical_paper_projection_hash(projection) != projection.projection_hash:
        raise ValueError("INITIAL_PROJECTION_HASH_INVALID")


def initial_approval_result(
    record: PaperInitialTargetApprovalRecord,
) -> InitialTargetApprovalResult:
    return InitialTargetApprovalResult(
        approval_id=record.id,
        account_id=record.account_id,
        portfolio_id=record.portfolio_id,
        projection_as_of=record.projection_as_of,
        projection_hash=record.projection_hash,
        approved_by=record.approved_by,
        approved_at=record.approved_at,
        approval_hash=record.approval_hash,
    )


def initial_approval_hash(result: InitialTargetApprovalResult) -> str:
    return canonical_portfolio_synthesis_hash(
        {
            "schema_version": "paper_initial_target_approval_v1",
            **result.model_dump(mode="json", exclude={"approval_hash", "approval_id"}),
            "allocation_policy_version": "portfolio_allocation_v1",
            "allocation_policy_hash": PORTFOLIO_ALLOCATION_POLICY_HASH,
        }
    )


async def approve_initial_target_projection(
    session: AsyncSession,
    *,
    account_id: UUID,
    projection_as_of: datetime,
    expected_projection_hash: str,
    approved_by: str,
    approved_at: datetime,
    idempotency_key: str,
) -> InitialTargetApprovalResult:
    projection_as_of, approved_at = require_utc(projection_as_of), require_utc(approved_at)
    if not 1 <= len(idempotency_key) <= 255 or not approved_by.strip() or len(approved_by) > 100:
        raise ValueError("INITIAL_APPROVAL_INVALID")
    await lock_portfolio_account(session, account_id)
    request_hash = canonical_portfolio_synthesis_hash(
        {
            "account_id": account_id,
            "projection_as_of": projection_as_of,
            "projection_hash": expected_projection_hash,
            "approved_by": approved_by,
            "approved_at": approved_at,
            "policy_hash": PORTFOLIO_ALLOCATION_POLICY_HASH,
        }
    )
    claim = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == f"internal-paper:{account_id}",
            IdempotencyRecord.route == _APPROVAL_ROUTE,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
    )
    existing = await session.scalar(
        select(PaperInitialTargetApprovalRecord).where(
            PaperInitialTargetApprovalRecord.account_id == account_id
        )
    )
    if claim is not None:
        if claim.request_hash != request_hash or existing is None:
            raise ValueError("INITIAL_APPROVAL_IDEMPOTENCY_CONFLICT")
        result = initial_approval_result(existing)
        if (
            InitialTargetApprovalResult.model_validate(claim.response_payload) != result
            or initial_approval_hash(result) != existing.approval_hash
        ):
            raise ValueError("INITIAL_APPROVAL_IDEMPOTENCY_CONFLICT")
        return result
    if existing is not None:
        raise ValueError("INITIAL_APPROVAL_ALREADY_EXISTS_CONFLICT")
    account = await session.get(PaperAccountRecord, account_id, populate_existing=True)
    underlying = (
        None
        if account is None
        else await session.get(Account, account.account_id, populate_existing=True)
    )
    if (
        account is None
        or underlying is None
        or underlying.provider != "internal"
        or underlying.status != "ACTIVE"
        or account.capability != "simulation_write_internal"
        or account.currency != "USD"
        or not account.created_at <= projection_as_of <= approved_at
        or underlying.created_at > projection_as_of
    ):
        raise ValueError("INITIAL_APPROVAL_ACCOUNT_OR_TIME_INVALID")
    projection = await load_paper_projection(session, account_id=account_id, as_of=projection_as_of)
    current = await load_paper_projection(session, account_id=account_id, as_of=approved_at)
    require_unoccupied_projection(projection)
    require_unoccupied_projection(current)
    if (
        projection.projection_hash != expected_projection_hash
        or projection.portfolio_id != account.portfolio_id
        or projection.source_facts_hash != current.source_facts_hash
        or projection.model_dump(exclude={"as_of", "projection_hash"})
        != current.model_dump(exclude={"as_of", "projection_hash"})
    ):
        raise ValueError("INITIAL_PROJECTION_STALE_OR_HASH_CONFLICT")
    result = InitialTargetApprovalResult(
        approval_id=new_uuid7(),
        account_id=account_id,
        portfolio_id=account.portfolio_id,
        projection_as_of=projection_as_of,
        projection_hash=projection.projection_hash,
        approved_by=approved_by,
        approved_at=approved_at,
        approval_hash="0" * 64,
    )
    result = result.model_copy(update={"approval_hash": initial_approval_hash(result)})
    session.add(
        PaperInitialTargetApprovalRecord(
            id=result.approval_id,
            account_id=account_id,
            portfolio_id=account.portfolio_id,
            projection_as_of=projection_as_of,
            projection_hash=projection.projection_hash,
            allocation_policy_version="portfolio_allocation_v1",
            allocation_policy_hash=PORTFOLIO_ALLOCATION_POLICY_HASH,
            approved_by=approved_by,
            approved_at=approved_at,
            approval_hash=result.approval_hash,
            created_at=approved_at,
        )
    )
    session.add(
        IdempotencyRecord(
            actor_id=f"internal-paper:{account_id}",
            route=_APPROVAL_ROUTE,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            schema_version="initial_target_approval_v1",
            response_payload=result.model_dump(mode="json"),
            created_at=approved_at,
        )
    )
    await session.flush()
    return result
