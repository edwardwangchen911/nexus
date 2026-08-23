"""Transaction-scoped proposal baseline eligibility boundary."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import desc, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.integrations.service import CapabilityProjection
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.reconciliation.persistence_errors import SnapshotNotProposalEligible

_OPERATIONS = ("fosun_list_orders", "fosun_sync_portfolio")


async def require_proposal_baseline(
    session: AsyncSession,
    *,
    portfolio_id: UUID,
    snapshot_id: UUID,
    now: datetime,
) -> UUID:
    """Hold the same account lock as admission until the caller's transaction ends."""
    now = require_utc(now)
    account_id = await session.scalar(
        select(Portfolio.account_id).where(Portfolio.id == portfolio_id)
    )
    if account_id is None:
        raise SnapshotNotProposalEligible("portfolio is not available")
    account = await session.scalar(
        select(Account).where(Account.id == account_id).with_for_update()
    )
    if account is None or account.status != "ACTIVE":
        raise SnapshotNotProposalEligible("broker account is not active")
    active = await session.scalar(
        select(BrokerSyncRun.id).where(
            BrokerSyncRun.portfolio_id == portfolio_id,
            BrokerSyncRun.status == "ACTIVE",
        )
    )
    if active is not None:
        raise SnapshotNotProposalEligible("an active broker sync blocks proposals")
    latest = await session.scalar(
        select(PortfolioSnapshotRecord)
        .where(PortfolioSnapshotRecord.portfolio_id == portfolio_id)
        .order_by(desc(PortfolioSnapshotRecord.created_at), desc(PortfolioSnapshotRecord.id))
        .limit(1)
    )
    if latest is None or latest.id != snapshot_id or latest.status != "READY":
        raise SnapshotNotProposalEligible("latest broker snapshot is not READY")
    status, authorities = await _lock_fosun_authority(session, now)
    if (
        status is None
        or status.status != "read_only"
        or any(
            authority is None
            or authority.schema_version != "1"
            or status.admitted_contract_hash != authority.contract_hash
            or (
                status.capability_invalidated_at is not None
                and authority.created_at <= status.capability_invalidated_at
            )
            for authority in authorities
        )
    ):
        raise SnapshotNotProposalEligible("broker read capability is not currently valid")
    return latest.id


async def _lock_fosun_authority(
    session: AsyncSession,
    now: datetime,
) -> tuple[IntegrationStatus | None, list[ProviderCapability | None]]:
    for operation in _OPERATIONS:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
            {
                "scope": (
                    "provider-capability-scope:v1:fosun:READ_ONLY:"
                    f"{operation}:BROKER"
                )
            },
        )
    status = await session.scalar(
        select(IntegrationStatus)
        .where(IntegrationStatus.provider == "fosun")
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    authorities: list[ProviderCapability | None] = []
    for operation in _OPERATIONS:
        authority = await session.scalar(
            select(ProviderCapability)
            .where(
                ProviderCapability.provider == "fosun",
                ProviderCapability.capability == "READ_ONLY",
                ProviderCapability.payload["operation"].as_string() == operation,
                ProviderCapability.payload["market_scope"].as_string() == "BROKER",
            )
            .order_by(desc(ProviderCapability.created_at), desc(ProviderCapability.id))
            .limit(1)
            .with_for_update()
        )
        authorities.append(CapabilityProjection(authority, now).valid)
    return status, authorities
