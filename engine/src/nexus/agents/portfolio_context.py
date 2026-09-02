"""Minimal, redacted portfolio evidence stored inline in the research snapshot."""

import hashlib
import json
from datetime import datetime
from decimal import Decimal, localcontext
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.models import OptimizationNeed
from nexus.domain.decimal import DecimalString, decimal_to_string
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.models.research import ResearchEvidenceRef

SOURCE = "nexus_portfolio_context"


class ContextDiagnostic(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    code: str = Field(max_length=200)
    severity: Literal["info", "warning", "critical"]
    actionability: Literal["monitor", "research", "reduce_only", "blocked"]
    unknown_reason: str | None = Field(max_length=200)


class PortfolioResearchContext(BaseModel):
    """No account identities, cash amounts, quantities, order IDs or other holdings."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    instrument_id: UUID
    portfolio_snapshot_id: UUID
    snapshot_as_of: datetime
    research_as_of: datetime
    source: Literal["fosun"] = "fosun"
    currency: str = Field(pattern="^[A-Z]{3}$")
    position_weight: DecimalString | None
    cash_weight: DecimalString | None
    valuation_reason: Literal["INCOMPLETE_CURRENCY_VALUATION"] | None
    snapshot_decision_ready: bool
    execution_locked: Literal[True] = True
    diagnostic_run_id: UUID | None
    optimization_need: (
        Literal["BLOCKED", "RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE", "NO_ACTION"] | None
    )
    diagnostic_input_hash: str | None = Field(pattern="^[0-9a-f]{64}$")
    diagnostics: tuple[ContextDiagnostic, ...] = Field(max_length=100)
    diagnostics_truncated: bool
    evidence_ref_id: UUID | None = None
    content_hash: str | None = Field(default=None, pattern="^[0-9a-f]{64}$")


def _bytes(context: PortfolioResearchContext) -> bytes:
    return json.dumps(
        context.model_dump(mode="json", exclude={"evidence_ref_id", "content_hash"}),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


async def freeze_portfolio_context(
    session: AsyncSession,
    instrument_id: UUID,
    as_of: datetime,
) -> tuple[PortfolioResearchContext, ResearchEvidenceRef] | None:
    """Use the sole account's latest visible snapshot; never fall back to an older READY one."""
    candidates = list(
        await session.execute(
            select(PortfolioSnapshotRecord, BrokerSyncRun)
            .join(BrokerSyncRun, BrokerSyncRun.id == PortfolioSnapshotRecord.sync_run_id)
            .join(Portfolio, Portfolio.id == PortfolioSnapshotRecord.portfolio_id)
            .join(Account, Account.id == Portfolio.account_id)
            .where(
                Account.provider == "fosun",
                Account.status == "ACTIVE",
                PortfolioSnapshotRecord.created_at <= as_of,
                PortfolioSnapshotRecord.as_of <= as_of,
                BrokerSyncRun.created_at <= as_of,
            )
            .distinct(PortfolioSnapshotRecord.portfolio_id)
            .order_by(
                PortfolioSnapshotRecord.portfolio_id,
                PortfolioSnapshotRecord.created_at.desc(),
                PortfolioSnapshotRecord.id.desc(),
            )
            .limit(2)
        )
    )
    if len(candidates) != 1:
        return None
    snapshot, sync = candidates[0]
    positions = list(
        await session.scalars(
            select(PositionSnapshot).where(
                PositionSnapshot.portfolio_snapshot_id == snapshot.id,
            )
        )
    )
    held = next((p for p in positions if p.instrument_id == instrument_id), None)
    if held is None or held.created_at > as_of:
        return None
    currency_positions = [p for p in positions if p.currency == held.currency]
    cash = await session.scalar(
        select(CashSnapshot).where(
            CashSnapshot.portfolio_snapshot_id == snapshot.id,
            CashSnapshot.currency == held.currency,
        )
    )
    valid = (
        cash is not None
        and cash.created_at <= as_of
        and cash.cash_balance >= 0
        and (cash.provider_as_of is None or cash.provider_as_of <= snapshot.as_of)
        and all(
            p.created_at <= as_of
            and p.current_price is not None
            and p.current_price > 0
            and p.quantity > 0
            and (p.provider_as_of is None or p.provider_as_of <= snapshot.as_of)
            for p in currency_positions
        )
    )
    position_weight = cash_weight = None
    if valid:
        assert cash is not None and held.current_price is not None
        with localcontext() as decimal_context:
            decimal_context.prec = 28
            total = cash.cash_balance + sum(
                (p.quantity * cast(Decimal, p.current_price) for p in currency_positions),
                Decimal(0),
            )
            position_weight = decimal_to_string(held.quantity * held.current_price / total)
            cash_weight = decimal_to_string(cash.cash_balance / total)
    ready = snapshot.status == "READY" and sync.cash_flow_quality == "PASS"
    report = (
        await session.scalar(
            select(PortfolioDiagnosticReportRecord)
            .where(
                PortfolioDiagnosticReportRecord.portfolio_snapshot_id == snapshot.id,
                PortfolioDiagnosticReportRecord.snapshot_as_of == snapshot.as_of,
                PortfolioDiagnosticReportRecord.created_at <= as_of,
            )
            .order_by(
                PortfolioDiagnosticReportRecord.created_at.desc(),
                PortfolioDiagnosticReportRecord.id.desc(),
            )
            .limit(1)
        )
        if ready
        else None
    )
    items = (
        list(
            await session.scalars(
                select(PortfolioDiagnosticItemRecord)
                .where(
                    PortfolioDiagnosticItemRecord.report_id == report.id,
                    PortfolioDiagnosticItemRecord.created_at <= as_of,
                )
                .order_by(
                    PortfolioDiagnosticItemRecord.diagnostic_code, PortfolioDiagnosticItemRecord.id
                )
                .limit(101)
            )
        )
        if report
        else []
    )
    context = PortfolioResearchContext(
        instrument_id=instrument_id,
        portfolio_snapshot_id=snapshot.id,
        snapshot_as_of=snapshot.as_of,
        research_as_of=as_of,
        currency=held.currency,
        position_weight=position_weight,
        cash_weight=cash_weight,
        valuation_reason=None if valid else "INCOMPLETE_CURRENCY_VALUATION",
        snapshot_decision_ready=ready,
        diagnostic_run_id=report.id if report else None,
        optimization_need=cast(OptimizationNeed, report.optimization_need) if report else None,
        diagnostic_input_hash=report.diagnostic_input_hash if report else None,
        diagnostics=tuple(
            ContextDiagnostic(
                code=item.diagnostic_code,
                severity=cast(Literal["info", "warning", "critical"], item.severity),
                actionability=cast(
                    Literal["monitor", "research", "reduce_only", "blocked"], item.actionability
                ),
                unknown_reason=item.unknown_reason,
            )
            for item in items[:100]
        ),
        diagnostics_truncated=len(items) > 100,
    )
    content = _bytes(context)
    digest = hashlib.sha256(content).hexdigest()
    object_id = f"{snapshot.id}:{instrument_id}"
    # The canonical bytes are retained inline in DataSnapshot.payload, not in a new service.
    await session.execute(
        insert(ResearchEvidenceRef)
        .values(
            source=SOURCE,
            provider_object_id=object_id,
            revision=digest,
            available_at=as_of,
            content_hash=digest,
            byte_length=len(content),
            location=f"memory://sha256/{digest}",
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == object_id,
            ResearchEvidenceRef.revision == digest,
        )
    )
    assert evidence is not None
    return context.model_copy(
        update={"evidence_ref_id": evidence.id, "content_hash": digest}
    ), evidence


async def read_portfolio_context(
    session: AsyncSession,
    snapshot_id: UUID,
    instrument_id: UUID,
    as_of: datetime,
) -> PortfolioResearchContext | None:
    """Only the exact inline content bound into this snapshot is readable, not current holdings."""
    snapshot = await session.get(DataSnapshot, snapshot_id)
    if (
        snapshot is None
        or snapshot.source != "nexus_research"
        or snapshot.provider_object_id != str(instrument_id)
        or snapshot.available_at > as_of
        or snapshot.received_at > as_of
    ):
        return None
    if (
        hashlib.sha256(json.dumps(snapshot.payload, sort_keys=True).encode()).hexdigest()
        != snapshot.revision
    ):
        return None
    try:
        context = PortfolioResearchContext.model_validate(snapshot.payload.get("portfolio_context"))
        if (
            context.instrument_id != instrument_id
            or context.research_as_of > as_of
            or context.snapshot_as_of > context.research_as_of
        ):
            return None
    except (ValidationError, TypeError):
        return None
    content = _bytes(context)
    digest = hashlib.sha256(content).hexdigest()
    if context.content_hash != digest:
        return None
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.id == context.evidence_ref_id,
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.content_hash == digest,
            ResearchEvidenceRef.revision == digest,
            ResearchEvidenceRef.provider_object_id
            == f"{context.portfolio_snapshot_id}:{instrument_id}",
            ResearchEvidenceRef.available_at == context.research_as_of,
            ResearchEvidenceRef.location == f"memory://sha256/{digest}",
            ResearchEvidenceRef.byte_length == len(content),
        )
    )
    return context if evidence else None
