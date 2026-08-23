"""Atomic immutable storage and redacted retrieval for diagnostic reports."""

import json
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.models import (
    DiagnosticHistory,
    DiagnosticItem,
    InstrumentIdentity,
    PortfolioDiagnosticReport,
)
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
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.portfolio import (
    BrokerOrderSnapshot,
    CashSnapshot,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.providers.broker.base import (
    BrokerOrderFact,
    BrokerPosition,
    CashBalance,
    PortfolioSnapshot,
)


async def load_frozen_snapshot(
    session: AsyncSession, *, portfolio_id: UUID, snapshot_id: UUID
) -> PortfolioSnapshot:
    """Load the only production diagnostic snapshot from immutable database records."""
    record = await session.scalar(
        select(PortfolioSnapshotRecord).where(
            PortfolioSnapshotRecord.id == snapshot_id,
            PortfolioSnapshotRecord.portfolio_id == portfolio_id,
        )
    )
    if record is None:
        raise ValueError("portfolio snapshot was not found")
    cash = (
        await session.execute(
            select(CashSnapshot).where(CashSnapshot.portfolio_snapshot_id == record.id)
        )
    ).scalars()
    positions = (
        await session.execute(
            select(PositionSnapshot).where(PositionSnapshot.portfolio_snapshot_id == record.id)
        )
    ).scalars()
    orders = (
        await session.execute(
            select(BrokerOrderSnapshot).where(
                BrokerOrderSnapshot.portfolio_snapshot_id == record.id
            )
        )
    ).scalars()
    return PortfolioSnapshot(
        id=record.id,
        portfolio_id=record.portfolio_id,
        status=cast(Literal["READY", "PARTIAL", "STALE", "RECONCILIATION_REQUIRED"], record.status),
        cash=tuple(
            CashBalance(
                currency=row.currency,
                cash_balance=row.cash_balance,
                available_cash=row.available_cash,
                provider_as_of=row.provider_as_of,
            )
            for row in cash
        ),
        positions=tuple(
            BrokerPosition(
                instrument_id=row.instrument_id,
                currency=row.currency,
                quantity=row.quantity,
                available_quantity=row.available_quantity,
                frozen_quantity=row.frozen_quantity,
                avg_cost=row.avg_cost,
                diluted_cost=row.diluted_cost,
                current_price=row.current_price,
                provider_as_of=row.provider_as_of,
            )
            for row in positions
        ),
        open_orders=tuple(
            BrokerOrderFact(
                provider_order_id=row.provider_order_id,
                instrument_id=row.instrument_id,
                side=cast(Literal["buy", "sell"], row.side),
                quantity=row.quantity,
                filled_quantity=row.filled_quantity,
                price=row.price,
                status=cast(Literal["pending", "submitted", "partially_filled"], row.status),
                updated_at=row.updated_at,
            )
            for row in orders
        ),
        as_of=record.as_of,
        stale_from_snapshot_id=record.stale_from_snapshot_id,
        source_request_ids=tuple(record.source_request_ids),
    )


async def load_instrument_identities(
    session: AsyncSession, *, snapshot: PortfolioSnapshot
) -> dict[UUID, InstrumentIdentity]:
    """Mint canonical identities using only records visible at the frozen instant."""
    instrument_ids = {position.instrument_id for position in snapshot.positions}
    if not instrument_ids or snapshot.as_of is None:
        return {
            instrument_id: InstrumentIdentity.unresolved(instrument_id)
            for instrument_id in instrument_ids
        }
    instruments = (
        (
            await session.execute(
                select(Instrument).where(
                    Instrument.id.in_(instrument_ids),
                    Instrument.created_at <= snapshot.as_of,
                )
            )
        )
        .scalars()
        .all()
    )
    aliases = (
        (
            await session.execute(
                select(InstrumentAlias)
                .where(
                    InstrumentAlias.instrument_id.in_(instrument_ids),
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= snapshot.as_of.date(),
                    (
                        InstrumentAlias.valid_to.is_(None)
                        | (InstrumentAlias.valid_to >= snapshot.as_of.date())
                    ),
                    InstrumentAlias.created_at <= snapshot.as_of,
                )
                .order_by(
                    InstrumentAlias.instrument_id,
                    InstrumentAlias.valid_from.desc(),
                    InstrumentAlias.id.desc(),
                )
            )
        )
        .scalars()
        .all()
    )
    instruments_by_id = {record.id: record for record in instruments}
    aliases_by_id: dict[UUID, InstrumentAlias] = {}
    for alias in aliases:
        aliases_by_id.setdefault(alias.instrument_id, alias)
    result: dict[UUID, InstrumentIdentity] = {}
    for instrument_id in instrument_ids:
        record = instruments_by_id.get(instrument_id)
        selected_alias = aliases_by_id.get(instrument_id)
        if record is None or selected_alias is None:
            result[instrument_id] = InstrumentIdentity.unresolved(instrument_id)
            continue
        restriction = cast(
            Literal["STANDARD", "FOSUN_READ_ONLY", "UNRESOLVED"],
            "FOSUN_READ_ONLY"
            if record.asset_type == "equity"
            and record.exchange == "XHKG"
            and record.currency == "HKD"
            and selected_alias.symbol == "00656.HK"
            else "STANDARD",
        )
        result[instrument_id] = InstrumentIdentity(
            instrument_id=instrument_id,
            asset_type=record.asset_type,
            exchange=record.exchange,
            currency=record.currency,
            risk_bucket=record.risk_bucket,
            canonical_ticker=selected_alias.symbol,
            restriction=restriction,
        )
    return result


async def load_history(
    session: AsyncSession,
    *,
    portfolio_id: UUID,
    current_snapshot_id: UUID,
    current_snapshot_as_of: datetime,
    policy_version: str,
) -> DiagnosticHistory:
    """Load only canonical reports strictly before the current frozen snapshot."""
    records = (
        (
            await session.execute(
                select(PortfolioDiagnosticReportRecord)
                .where(
                    PortfolioDiagnosticReportRecord.portfolio_id == portfolio_id,
                    PortfolioDiagnosticReportRecord.portfolio_snapshot_id != current_snapshot_id,
                    PortfolioDiagnosticReportRecord.policy_id == REAL_ADVISORY_POLICY_ID,
                    PortfolioDiagnosticReportRecord.policy_version == policy_version,
                    PortfolioDiagnosticReportRecord.snapshot_as_of < current_snapshot_as_of,
                    PortfolioDiagnosticReportRecord.created_at <= current_snapshot_as_of,
                    PortfolioDiagnosticReportRecord.trading_day < current_snapshot_as_of.date(),
                )
                .order_by(
                    PortfolioDiagnosticReportRecord.snapshot_as_of,
                    PortfolioDiagnosticReportRecord.id,
                )
            )
        )
        .scalars()
        .all()
    )
    reports = [await _read_report(session, record) for record in records]
    refs = tuple(
        EvidenceRef(
            source="nexus_db",
            object_id=f"diagnostic-report:{record.id}",
            available_at=record.created_at,
            location=f"db://portfolio-diagnostic-reports/{record.id}",
            content_hash=_report_content_hash(record, report),
        )
        for record, report in zip(records, reports, strict=True)
    )
    return DiagnosticHistory(
        prior_reports=tuple(reports), authority_refs=refs, comparable_variants=()
    )


async def latest_report(
    session: AsyncSession, *, portfolio_id: UUID
) -> PortfolioDiagnosticReport | None:
    policy = await load_real_advisory_policy_v1(session)
    record = await session.scalar(
        select(PortfolioDiagnosticReportRecord)
        .where(PortfolioDiagnosticReportRecord.portfolio_id == portfolio_id)
        .order_by(
            desc(PortfolioDiagnosticReportRecord.created_at),
            desc(PortfolioDiagnosticReportRecord.id),
        )
        .limit(1)
    )
    if record is None:
        return None
    if record.policy_id != REAL_ADVISORY_POLICY_ID or record.policy_version != policy.version:
        raise ValueError("diagnostic report policy authority is invalid")
    return await _read_report(session, record)


async def _read_report(
    session: AsyncSession, record: PortfolioDiagnosticReportRecord
) -> PortfolioDiagnosticReport:
    rows = (
        (
            await session.execute(
                select(PortfolioDiagnosticItemRecord)
                .where(PortfolioDiagnosticItemRecord.report_id == record.id)
                .order_by(PortfolioDiagnosticItemRecord.id)
            )
        )
        .scalars()
        .all()
    )
    items = []
    for row in rows:
        refs = (
            (
                await session.execute(
                    select(PortfolioDiagnosticEvidenceRefRecord)
                    .where(PortfolioDiagnosticEvidenceRefRecord.item_id == row.id)
                    .order_by(PortfolioDiagnosticEvidenceRefRecord.id)
                )
            )
            .scalars()
            .all()
        )
        items.append(
            DiagnosticItem(
                diagnostic_code=row.diagnostic_code,
                metric_value=_string(row.metric_value),
                unit=row.unit,
                reference_limit=_string(row.reference_limit),
                policy_scope=row.policy_scope,
                severity=cast(Literal["info", "warning", "critical"], row.severity),
                actionability=cast(
                    Literal["monitor", "research", "reduce_only", "blocked"], row.actionability
                ),
                unknown_reason=row.unknown_reason,
                evidence_refs=tuple(_evidence(ref) for ref in refs),
            )
        )
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=record.portfolio_snapshot_id,
        snapshot_as_of=record.snapshot_as_of,
        trading_day=record.trading_day,
        policy_version=cast(Literal["real_advisory_policy_v1"], record.policy_version),
        optimization_need=cast(
            Literal["BLOCKED", "RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE", "NO_ACTION"],
            record.optimization_need,
        ),
        items=tuple(items),
    )


def _decimal(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)


def _string(value: Decimal | None) -> str | None:
    return None if value is None else decimal_to_string(value)


def _evidence(record: PortfolioDiagnosticEvidenceRefRecord) -> EvidenceRef:
    return EvidenceRef(
        source=record.source,
        object_id=record.object_id,
        available_at=record.available_at,
        location=record.location,
        content_hash=record.content_hash,
    )


def _reports_equivalent(
    stored: PortfolioDiagnosticReport, replay: PortfolioDiagnosticReport
) -> bool:
    if (
        stored.portfolio_snapshot_id != replay.portfolio_snapshot_id
        or stored.snapshot_as_of != replay.snapshot_as_of
        or stored.trading_day != replay.trading_day
        or stored.policy_version != replay.policy_version
        or stored.optimization_need != replay.optimization_need
        or len(stored.items) != len(replay.items)
    ):
        return False
    for left, right in zip(stored.items, replay.items, strict=True):
        if (
            left.diagnostic_code != right.diagnostic_code
            or _decimal(left.metric_value) != _decimal(right.metric_value)
            or left.unit != right.unit
            or _decimal(left.reference_limit) != _decimal(right.reference_limit)
            or left.policy_scope != right.policy_scope
            or left.severity != right.severity
            or left.actionability != right.actionability
            or left.unknown_reason != right.unknown_reason
            or left.evidence_refs != right.evidence_refs
        ):
            return False
    return True


def _report_content_hash(
    record: PortfolioDiagnosticReportRecord, report: PortfolioDiagnosticReport
) -> str:
    payload = {
        "id": str(record.id),
        "diagnostic_input_hash": record.diagnostic_input_hash,
        "report": report.model_dump(mode="json"),
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return sha256(encoded).hexdigest()
