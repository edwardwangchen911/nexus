"""Deterministic diagnosis entry point with fail-closed readiness precedence."""

from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.artifacts import ArtifactByteReader
from nexus.diagnostics.assembler import assemble_diagnostic_evidence
from nexus.diagnostics.decision import optimization_need as _optimization_need
from nexus.diagnostics.evidence import EvidenceResolver, EvidenceResult
from nexus.diagnostics.input_hash import diagnostic_input_hash
from nexus.diagnostics.items import item as _item
from nexus.diagnostics.items import threshold_item as _threshold_item
from nexus.diagnostics.items import unknown_item as _unknown_item
from nexus.diagnostics.models import (
    DiagnosticHistory,
    DiagnosticItem,
    MarketEvidence,
    OptimizationNeed,
    PortfolioDiagnosticReport,
)
from nexus.diagnostics.policy import (
    REAL_ADVISORY_POLICY_ID,
    RealAdvisoryPolicy,
    load_real_advisory_policy_v1,
)
from nexus.diagnostics.portfolio_evaluator import evaluate_currency_scopes
from nexus.diagnostics.repository import (
    _read_report,
    _reports_equivalent,
    load_frozen_snapshot,
    load_history,
    load_instrument_identities,
)
from nexus.diagnostics.scope import (
    assert_fosun_read_only_scope as _assert_fosun_read_only_scope,
)
from nexus.diagnostics.scope import (
    has_future_session_conflict as _has_future_session_conflict,
)
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
from nexus.providers.broker.base import PortfolioSnapshot


def diagnose_portfolio(
    snapshot: PortfolioSnapshot,
    market: MarketEvidence,
    history: DiagnosticHistory,
    policy: RealAdvisoryPolicy,
) -> PortfolioDiagnosticReport:
    """Diagnose frozen broker facts without cross-currency aggregation or LLM input."""
    source_conflict = market.source_conflict or _has_future_session_conflict(snapshot, market)
    if snapshot.status != "READY" or source_conflict:
        return _report(snapshot, policy, "BLOCKED", (_blocked_item(market, source_conflict),))
    if market.quality != "PASS":
        return _report(snapshot, policy, "RESEARCH_REQUIRED", (_market_unknown_item(market),))
    resolver = EvidenceResolver(snapshot.as_of)
    snapshot_evidence = resolver.resolve(
        market.evidence.snapshot,
        "SNAPSHOT",
        expected_as_of=snapshot.as_of,
    )
    if snapshot_evidence.reason is not None:
        missing_items = [
            _unknown_item(
                "SNAPSHOT_EVIDENCE",
                "status",
                snapshot_evidence.reason,
                "portfolio",
                market,
            )
        ]
        missing_items.extend(
            _unknown_item(
                f"LIQUIDITY_DAYS_TO_EXIT:{position.instrument_id}",
                "trading_days",
                "ADV20_MISSING",
                f"currency:{position.currency}",
                market,
            )
            for position in snapshot.positions
            if market.adv20.get(position.instrument_id) is None
        )
        return _report(
            snapshot,
            policy,
            "RESEARCH_REQUIRED",
            tuple(missing_items),
        )
    items = evaluate_currency_scopes(snapshot, market, policy, resolver, snapshot_evidence.refs)
    currencies = {item.currency for item in snapshot.cash} | {
        item.currency for item in snapshot.positions
    }
    if len(currencies) > 1:
        items.append(
            _unknown_item(
                "CURRENCY_EXPOSURE",
                "ratio",
                "FX_QUALIFIED_VIEW_MISSING",
                "portfolio:unqualified_fx",
                market,
            )
        )
    elif currencies:
        items.append(
            _threshold_item(
                "CURRENCY_EXPOSURE",
                Decimal(1),
                "ratio",
                policy.currency_warning,
                f"currency:{next(iter(currencies))}",
                market,
                warning=True,
                evidence_refs=snapshot_evidence.refs,
            )
        )
    calendar = _calendar_evidence(snapshot, market, resolver)
    if calendar.reason is not None:
        items.append(
            _unknown_item(
                "DATA_COMPLETENESS_TRADING_CALENDAR",
                "sessions",
                calendar.reason,
                "portfolio",
                market,
            )
        )
    else:
        items.append(
            _item(
                "DATA_COMPLETENESS_TRADING_CALENDAR",
                Decimal(len(market.trading_sessions)),
                "sessions",
                None,
                "portfolio",
                "info",
                "monitor",
                None,
                market,
                evidence_refs=calendar.refs,
            )
        )
    _assert_fosun_read_only_scope(snapshot, market, items)
    trading_day = None if snapshot.as_of is None else snapshot.as_of.date()
    return _report(
        snapshot,
        policy,
        _optimization_need(
            items,
            history,
            trading_day,
            market.trading_sessions if calendar.reason is None else (),
        ),
        tuple(items),
    )


async def diagnose_and_persist(
    session: AsyncSession,
    *,
    portfolio_id: UUID,
    snapshot_id: UUID,
    artifact_reader: ArtifactByteReader | None = None,
) -> PortfolioDiagnosticReport:
    """Production entry point accepts IDs only and fails closed without stored market evidence."""
    policy = await load_real_advisory_policy_v1(session)
    snapshot = await load_frozen_snapshot(
        session, portfolio_id=portfolio_id, snapshot_id=snapshot_id
    )
    identities = await load_instrument_identities(session, snapshot=snapshot)
    if snapshot.as_of is None:
        raise ValueError("diagnostic snapshot timestamp is unavailable")
    history = await load_history(
        session,
        portfolio_id=portfolio_id,
        current_snapshot_id=snapshot.id,
        current_snapshot_as_of=snapshot.as_of,
        policy_version=policy.version,
    )
    assembly = await assemble_diagnostic_evidence(
        session,
        snapshot=snapshot,
        identities=identities,
        artifact_reader=artifact_reader,
    )
    report = diagnose_portfolio(snapshot, assembly.market, history, policy)
    input_hash = diagnostic_input_hash(
        snapshot=snapshot,
        assembly=assembly,
        history=history,
        policy_version=policy.version,
    )

    async def write_authoritative_result() -> UUID:
        locked_snapshot = await session.scalar(
            select(PortfolioSnapshotRecord)
            .where(
                PortfolioSnapshotRecord.id == snapshot_id,
                PortfolioSnapshotRecord.portfolio_id == portfolio_id,
            )
            .with_for_update()
        )
        if (
            locked_snapshot is None
            or locked_snapshot.as_of is None
            or locked_snapshot.as_of != snapshot.as_of
        ):
            raise ValueError("diagnostic snapshot authority changed during computation")
        if any(
            ref.available_at > locked_snapshot.as_of
            for item in report.items
            for ref in item.evidence_refs
        ):
            raise ValueError("diagnostic evidence is unavailable at frozen snapshot")
        existing = await session.scalar(
            select(PortfolioDiagnosticReportRecord).where(
                PortfolioDiagnosticReportRecord.portfolio_snapshot_id == snapshot.id,
                PortfolioDiagnosticReportRecord.policy_id == REAL_ADVISORY_POLICY_ID,
            )
        )
        if existing is not None:
            same_report = _reports_equivalent(await _read_report(session, existing), report)
            if existing.diagnostic_input_hash != input_hash or not same_report:
                raise ValueError("diagnostic report replay conflicts with immutable report")
            return existing.id
        stored = PortfolioDiagnosticReportRecord(
            portfolio_id=portfolio_id,
            portfolio_snapshot_id=snapshot.id,
            snapshot_as_of=locked_snapshot.as_of,
            trading_day=report.trading_day,
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version=report.policy_version,
            optimization_need=report.optimization_need,
            diagnostic_input_hash=input_hash,
        )
        session.add(stored)
        await session.flush()
        for diagnostic_item in report.items:
            stored_item = PortfolioDiagnosticItemRecord(
                report_id=stored.id,
                diagnostic_code=diagnostic_item.diagnostic_code,
                metric_value=(
                    None
                    if diagnostic_item.metric_value is None
                    else Decimal(diagnostic_item.metric_value)
                ),
                unit=diagnostic_item.unit,
                reference_limit=(
                    None
                    if diagnostic_item.reference_limit is None
                    else Decimal(diagnostic_item.reference_limit)
                ),
                policy_scope=diagnostic_item.policy_scope,
                severity=diagnostic_item.severity,
                actionability=diagnostic_item.actionability,
                unknown_reason=diagnostic_item.unknown_reason,
            )
            session.add(stored_item)
            await session.flush()
            session.add_all(
                PortfolioDiagnosticEvidenceRefRecord(
                    item_id=stored_item.id,
                    source=ref.source,
                    object_id=ref.object_id,
                    available_at=ref.available_at,
                    location=ref.location,
                    content_hash=ref.content_hash,
                )
                for ref in diagnostic_item.evidence_refs
            )
        return stored.id

    await write_authoritative_result()
    return report


def _calendar_evidence(
    snapshot: PortfolioSnapshot, market: MarketEvidence, resolver: EvidenceResolver
) -> EvidenceResult:
    expected_session = market.trading_sessions[-1] if market.trading_sessions else None
    result = resolver.resolve(
        market.evidence.trading_calendar,
        "TRADING_CALENDAR",
        expected_session=expected_session,
    )
    if (
        result.reason is None
        and snapshot.as_of is not None
        and any(session > snapshot.as_of.date() for session in market.trading_sessions)
    ):
        return EvidenceResult(reason="TRADING_CALENDAR_EVIDENCE_FUTURE")
    return result


def _report(
    snapshot: PortfolioSnapshot,
    policy: RealAdvisoryPolicy,
    optimization_need: OptimizationNeed,
    items: tuple[DiagnosticItem, ...],
) -> PortfolioDiagnosticReport:
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=snapshot.id,
        snapshot_as_of=snapshot.as_of,
        trading_day=None if snapshot.as_of is None else snapshot.as_of.date(),
        policy_version=policy.version,
        optimization_need=optimization_need,
        items=items,
    )


def _blocked_item(market: MarketEvidence, source_conflict: bool) -> DiagnosticItem:
    return _unknown_item(
        "SNAPSHOT_ADMISSION",
        "status",
        "SOURCE_CONFLICT" if source_conflict else "SNAPSHOT_NOT_READY",
        "portfolio",
        market,
        evidence_refs=market.conflict_refs,
    ).model_copy(update={"severity": "critical", "actionability": "blocked"})


def _market_unknown_item(market: MarketEvidence) -> DiagnosticItem:
    return _unknown_item(
        "MARKET_EVIDENCE", "status", "MARKET_EVIDENCE_UNAVAILABLE", "portfolio", market
    )
