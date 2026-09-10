"""Atomic immutable storage and redacted retrieval for diagnostic reports."""

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal, localcontext
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.models import (
    ComparableVariantEvidence,
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
        refs: tuple[EvidenceRef, ...] = ()
        if record is not None:
            refs = (
                _authority_ref(
                    "instrument", record.id, record.created_at, _instrument_input(record)
                ),
            )
        if selected_alias is not None:
            refs = (
                *refs,
                _authority_ref(
                    "instrument-alias",
                    selected_alias.id,
                    selected_alias.created_at,
                    _alias_input(selected_alias),
                ),
            )
        if record is None or selected_alias is None:
            result[instrument_id] = InstrumentIdentity.unresolved(
                instrument_id, authority_refs=refs
            )
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
            instrument_record_id=record.id,
            alias_record_id=selected_alias.id,
            instrument_created_at=record.created_at,
            alias_created_at=selected_alias.created_at,
            authority_refs=refs,
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
    reports = [await read_report(session, record) for record in records]
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
    comparisons = await _comparable_history(session, records, current_snapshot_as_of)
    return DiagnosticHistory(
        prior_reports=tuple(reports),
        authority_refs=(*refs, *(ref for item in comparisons for ref in item.evidence_refs)),
        comparable_variants=comparisons,
    )


async def _comparable_history(
    session: AsyncSession,
    reports: Sequence[PortfolioDiagnosticReportRecord],
    cutoff: datetime,
) -> tuple[ComparableVariantEvidence, ...]:
    """Read historical comparisons, never promote a draft or reuse another portfolio's proof."""
    from nexus.optimizer.authority import load_admission_authority
    from nexus.optimizer.repository import (
        _comparison_is_executable,
        has_executable_shadow_authority,
    )
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.optimization import (
        OptimizationAdmissionAuthorityRecord,
        OptimizationProposalGroupRecord,
        OptimizationProposalRecord,
        ProposalComparisonRecord,
    )

    if not reports:
        return ()
    groups = tuple(
        await session.scalars(
            select(OptimizationProposalGroupRecord)
            .where(
                OptimizationProposalGroupRecord.diagnostic_report_id.in_([r.id for r in reports]),
                OptimizationProposalGroupRecord.created_at <= cutoff,
                OptimizationProposalGroupRecord.baseline_ready.is_(True),
                OptimizationProposalGroupRecord.diagnostic_complete.is_(True),
                OptimizationProposalGroupRecord.partial.is_(False),
                OptimizationProposalGroupRecord.cost_complete.is_(True),
                OptimizationProposalGroupRecord.source_conflict.is_(False),
                OptimizationProposalGroupRecord.requires_fx.is_(False),
                OptimizationProposalGroupRecord.authority_stale.is_(False),
            )
            .order_by(OptimizationProposalGroupRecord.id)
        )
    )
    result = []
    for group in groups:
        authority = await load_admission_authority(
            session,
            portfolio_snapshot_id=group.baseline_portfolio_snapshot_id,
            data_snapshot_id=group.data_snapshot_id,
            cost_policy_id=group.cost_policy_id,
        )
        if (
            authority is None
            or authority.persistence.diagnostic_report_id != group.diagnostic_report_id
            or authority.persistence.diagnostic_input_hash != group.diagnostic_input_hash
            or authority.persistence.valuation_time != group.valuation_time
            or authority.persistence.portfolio_scope != group.portfolio_scope
            or authority.persistence.source_policy_id != group.source_policy_id
            or authority.persistence.optimizer_policy_id != group.policy_id
            or authority.persistence.selection_authority_id != group.selection_authority_id
            or authority.persistence.selection_evidence_hash != group.selection_evidence_hash
        ):
            continue
        admission = await session.get(
            OptimizationAdmissionAuthorityRecord, authority.admission_authority_id
        )
        data = await session.get(DataSnapshot, group.data_snapshot_id)
        if (
            admission is None
            or admission.created_at > cutoff
            or data is None
            or data.created_at > cutoff
        ):
            continue
        rows = (
            await session.execute(
                select(OptimizationProposalRecord, ProposalComparisonRecord)
                .join(ProposalComparisonRecord)
                .where(
                    OptimizationProposalRecord.proposal_group_id == group.id,
                    OptimizationProposalRecord.created_at <= cutoff,
                    ProposalComparisonRecord.created_at <= cutoff,
                )
                .order_by(OptimizationProposalRecord.variant)
            )
        ).all()
        if len(rows) != 3:
            continue
        hold = next((c for p, c in rows if p.variant == "hold_current"), None)
        if hold is None or not _comparison_is_executable(hold, group):
            continue
        for proposal, comparison in rows:
            if proposal.variant == "hold_current" or not _comparison_is_executable(
                comparison, group
            ):
                continue
            if proposal.variant == "strategy_target" and not await has_executable_shadow_authority(
                session, group
            ):
                continue
            digest = sha256(
                json.dumps(
                    {
                        "group_id": str(group.id),
                        "variant": proposal.variant,
                        "baseline_id": str(group.baseline_portfolio_snapshot_id),
                        "diagnostic_input_hash": group.diagnostic_input_hash,
                        "metrics": comparison.metrics,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            ref = EvidenceRef(
                source="nexus_db",
                object_id=f"proposal-comparison:{comparison.id}",
                available_at=max(
                    group.created_at,
                    proposal.created_at,
                    comparison.created_at,
                    admission.created_at,
                    data.created_at,
                ),
                location=f"db://proposal-comparisons/{comparison.id}",
                content_hash=digest,
            )
            result.append(
                ComparableVariantEvidence(
                    variant=cast(Literal["minimum_change", "strategy_target"], proposal.variant),
                    proposal_group_id=group.id,
                    evidence_refs=(ref,),
                )
            )
    return tuple(result)


async def latest_report(
    session: AsyncSession, *, portfolio_id: UUID
) -> PortfolioDiagnosticReport | None:
    persisted = await latest_report_with_id(session, portfolio_id=portfolio_id)
    return None if persisted is None else persisted[1]


async def latest_report_with_id(
    session: AsyncSession, *, portfolio_id: UUID
) -> tuple[UUID, PortfolioDiagnosticReport] | None:
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
    return record.id, await read_report(session, record)


async def read_report(
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


_read_report = read_report


def _stored_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 50
        return Decimal(value).quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP)


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


def _authority_ref(
    kind: str, record_id: UUID, available_at: datetime, payload: object
) -> EvidenceRef:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_authority_json,
    ).encode()
    return EvidenceRef(
        source="nexus_db",
        object_id=f"{kind}:{record_id}",
        available_at=available_at,
        location=f"db://{kind}/{record_id}",
        content_hash=sha256(encoded).hexdigest(),
    )


def _instrument_input(record: Instrument) -> dict[str, object]:
    return {
        "id": str(record.id),
        "asset_type": record.asset_type,
        "exchange": record.exchange,
        "currency": record.currency,
        "tick_size": None if record.tick_size is None else str(record.tick_size),
        "risk_bucket": record.risk_bucket,
        "created_at": _authority_json(record.created_at),
    }


def _alias_input(record: InstrumentAlias) -> dict[str, object]:
    return {
        "id": str(record.id),
        "instrument_id": str(record.instrument_id),
        "symbol": record.symbol,
        "alias_type": record.alias_type,
        "valid_from": record.valid_from.isoformat(),
        "valid_to": None if record.valid_to is None else record.valid_to.isoformat(),
        "created_at": _authority_json(record.created_at),
    }


def _authority_json(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal | UUID):
        return str(value)
    raise TypeError(f"unsupported identity authority value: {type(value).__name__}")


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
            or _stored_decimal(left.metric_value) != _stored_decimal(right.metric_value)
            or left.unit != right.unit
            or _stored_decimal(left.reference_limit) != _stored_decimal(right.reference_limit)
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
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return sha256(encoded).hexdigest()
