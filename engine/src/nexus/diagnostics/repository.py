"""Atomic immutable storage and redacted retrieval for diagnostic reports."""
# ruff: noqa: E501

from datetime import datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.diagnostics.policy import (
    REAL_ADVISORY_POLICY_ID,
    load_real_advisory_policy_v1,
)
from nexus.domain.decimal import decimal_to_string
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord


async def persist_report(
    session: AsyncSession, *, portfolio_id: UUID, report: PortfolioDiagnosticReport
) -> UUID:
    """Insert one report graph atomically after deriving snapshot ownership from the database."""
    policy = await load_real_advisory_policy_v1(session)
    snapshot = await session.scalar(
        select(PortfolioSnapshotRecord).where(
            PortfolioSnapshotRecord.id == report.portfolio_snapshot_id,
            PortfolioSnapshotRecord.portfolio_id == portfolio_id,
        )
    )
    if snapshot is None or report.policy_version != policy.version:
        raise ValueError("diagnostic report snapshot or policy authority is invalid")
    stored = PortfolioDiagnosticReportRecord(
        portfolio_id=snapshot.portfolio_id,
        portfolio_snapshot_id=snapshot.id,
        trading_day=report.trading_day,
        policy_id=REAL_ADVISORY_POLICY_ID,
        policy_version=report.policy_version,
        optimization_need=report.optimization_need,
    )
    session.add(stored)
    await session.flush()
    for item in report.items:
        stored_item = PortfolioDiagnosticItemRecord(
            report_id=stored.id,
            diagnostic_code=item.diagnostic_code,
            metric_value=_decimal(item.metric_value),
            unit=item.unit,
            reference_limit=_decimal(item.reference_limit),
            policy_scope=item.policy_scope,
            severity=item.severity,
            actionability=item.actionability,
            unknown_reason=item.unknown_reason,
        )
        session.add(stored_item)
        await session.flush()
        session.add_all(
            PortfolioDiagnosticEvidenceRefRecord(
                item_id=stored_item.id,
                source=ref.source,
                object_id=ref.object_id,
                available_at=ref.available_at.isoformat(),
                location=ref.location,
                content_hash=ref.content_hash,
            )
            for ref in item.evidence_refs
        )
    return stored.id


async def latest_report(
    session: AsyncSession, *, portfolio_id: UUID
) -> PortfolioDiagnosticReport | None:
    record = await session.scalar(
        select(PortfolioDiagnosticReportRecord)
        .where(PortfolioDiagnosticReportRecord.portfolio_id == portfolio_id)
        .order_by(desc(PortfolioDiagnosticReportRecord.created_at), desc(PortfolioDiagnosticReportRecord.id))
        .limit(1)
    )
    if record is None:
        return None
    rows = (await session.execute(
        select(PortfolioDiagnosticItemRecord).where(PortfolioDiagnosticItemRecord.report_id == record.id)
        .order_by(PortfolioDiagnosticItemRecord.id)
    )).scalars().all()
    items = []
    for row in rows:
        refs = (await session.execute(
            select(PortfolioDiagnosticEvidenceRefRecord)
            .where(PortfolioDiagnosticEvidenceRefRecord.item_id == row.id)
            .order_by(PortfolioDiagnosticEvidenceRefRecord.id)
        )).scalars().all()
        items.append(DiagnosticItem(
            diagnostic_code=row.diagnostic_code,
            metric_value=_string(row.metric_value), unit=row.unit,
            reference_limit=_string(row.reference_limit), policy_scope=row.policy_scope,
            severity=cast(Literal["info", "warning", "critical"], row.severity),
            actionability=cast(Literal["monitor", "research", "reduce_only", "blocked"], row.actionability),
            unknown_reason=row.unknown_reason,
            evidence_refs=tuple(_evidence(ref) for ref in refs),
        ))
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=record.portfolio_snapshot_id,
        trading_day=record.trading_day,
        policy_version=cast(Literal["real_advisory_policy_v1"], record.policy_version),
        optimization_need=cast(Literal["BLOCKED", "RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE", "NO_ACTION"], record.optimization_need),
        items=tuple(items),
    )


def _decimal(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def _string(value: Decimal | None) -> str | None:
    return None if value is None else decimal_to_string(value)


def _evidence(record: PortfolioDiagnosticEvidenceRefRecord) -> EvidenceRef:
    return EvidenceRef(
        source=record.source, object_id=record.object_id,
        available_at=datetime.fromisoformat(record.available_at), location=record.location,
        content_hash=record.content_hash,
    )
