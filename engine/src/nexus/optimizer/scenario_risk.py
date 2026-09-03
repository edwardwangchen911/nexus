"""Historical frozen-weight scenarios using existing qualified diagnostic primitives."""

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.artifacts import ArtifactByteReader
from nexus.diagnostics.assembler import assemble_diagnostic_evidence
from nexus.diagnostics.metrics import (
    annualized_volatility_by_session,
    beta_and_correlation_by_session,
    maximum_drawdown_by_session,
    qualified_session_closes,
)
from nexus.diagnostics.repository import load_instrument_identities
from nexus.domain.evidence import EvidenceRef
from nexus.optimizer.compare import TypedMetric
from nexus.persistence.models.market import SectorExposure
from nexus.providers.broker.base import PortfolioSnapshot


@dataclass(frozen=True)
class ScenarioEvidence:
    baseline_id: UUID
    valuation_time: datetime
    sessions: tuple[date, ...]
    closes: Mapping[UUID, Mapping[date, Decimal]]
    benchmark: Mapping[date, Decimal]
    sectors: Mapping[UUID, Mapping[str, Decimal]]
    refs: tuple[EvidenceRef, ...]


async def load_scenario_evidence(
    session: AsyncSession,
    snapshot: PortfolioSnapshot,
    artifact_reader: ArtifactByteReader | None = None,
) -> ScenarioEvidence:
    """Reuse DB-qualified price assembly and reviewed point-in-time sector facts."""
    if snapshot.as_of is None:
        raise ValueError("SCENARIO_VALUATION_TIME_MISSING")
    ids = {p.instrument_id for p in snapshot.positions}
    rows = tuple(
        await session.scalars(
            select(SectorExposure)
            .where(
                SectorExposure.instrument_id.in_(ids),
                SectorExposure.as_of <= snapshot.as_of.date(),
                SectorExposure.created_at <= snapshot.as_of,
                SectorExposure.reviewed.is_(True),
                SectorExposure.synthetic.is_(False),
            )
            .order_by(SectorExposure.instrument_id, SectorExposure.as_of.desc(), SectorExposure.id)
        )
    )
    sectors: dict[UUID, dict[str, Decimal]] = {}
    refs: list[EvidenceRef] = []
    for instrument_id in sorted(ids, key=str):
        candidates = [r for r in rows if r.instrument_id == instrument_id]
        if not candidates:
            continue
        latest = [r for r in candidates if r.as_of == candidates[0].as_of]
        weights = {r.sector_code: r.weight for r in latest}
        if len(weights) != len(latest) or sum(weights.values(), Decimal(0)) != 1:
            continue
        sectors[instrument_id] = weights
        refs.extend(
            EvidenceRef(
                source="nexus_db",
                object_id=f"sector-exposure:{r.id}",
                available_at=r.created_at,
                location=f"db://sector-exposures/{r.id}",
                content_hash=r.content_hash,
            )
            for r in latest
        )
    closes: dict[UUID, dict[date, Decimal]] = {}
    benchmark: dict[date, Decimal] = {}
    sessions: tuple[date, ...] = ()
    # The raw v2 boundary stays in the shared assembler. No local PASS override.
    currencies = {p.currency for p in snapshot.positions} | {
        c.currency for c in snapshot.cash if c.cash_balance != 0
    }
    if artifact_reader is not None and currencies == {"USD"}:
        identities = await load_instrument_identities(session, snapshot=snapshot)
        assembly = await assemble_diagnostic_evidence(
            session,
            snapshot=snapshot,
            identities=identities,
            artifact_reader=artifact_reader,
        )
        market = assembly.market
        if market.quality == "PASS" and not market.source_conflict:
            sessions = market.trading_sessions
            for instrument_id, series in market.prices.items():
                if series.adjustment != "all":
                    continue
                values = qualified_session_closes(series, snapshot.as_of)
                if values is not None:
                    closes[instrument_id] = values
                    refs.extend(market.evidence.price_series[instrument_id].refs)
            if market.benchmark is not None and market.benchmark.adjustment == "all":
                benchmark = qualified_session_closes(market.benchmark, snapshot.as_of) or {}
                if market.evidence.benchmark is not None:
                    refs.extend(market.evidence.benchmark.refs)
            if market.evidence.trading_calendar is not None:
                refs.extend(market.evidence.trading_calendar.refs)
    return ScenarioEvidence(
        snapshot.id, snapshot.as_of, sessions, closes, benchmark, sectors, tuple(refs)
    )


def scenario_metrics(
    weights: Mapping[UUID, Decimal],
    evidence: ScenarioEvidence,
) -> dict[str, TypedMetric]:
    """Rebase fixed shares at each metric's common published-window start.

    These are historical risk scenarios using today's proposed weights, not a
    strategy backtest or a prediction. Volatility uses 64 closes for 63 returns,
    correlation 127 closes for 126 returns, and drawdown 252 closes. Each needs
    its anchor prices and the existing 80% coverage; missing intermediate days
    never create cross-gap returns. Residual cash earns zero, with no daily
    rebalancing. Persisted old comparison dimensions are not recomputed here.
    """
    result = {
        name: TypedMetric.unknown(reason)
        for name, reason in (
            ("sector_exposure", "SECTOR_EXPOSURE_NOT_PROVEN"),
            ("volatility", "VOLATILITY_POST_PROPOSAL_NOT_PROVEN"),
            ("drawdown", "DRAWDOWN_POST_PROPOSAL_NOT_PROVEN"),
            ("correlation", "CORRELATION_POST_PROPOSAL_NOT_PROVEN"),
        )
    }
    positive = {i: w for i, w in weights.items() if w > 0}
    if any(ref.available_at > evidence.valuation_time for ref in evidence.refs):
        return result
    if positive and all(i in evidence.sectors for i in positive):
        totals: defaultdict[str, Decimal] = defaultdict(lambda: Decimal(0))
        for instrument, weight in positive.items():
            for sector, fraction in evidence.sectors[instrument].items():
                totals[sector] += weight * fraction
        result["sector_exposure"] = TypedMetric.known(max(totals.values()))
    sessions = evidence.sessions[-253:]
    if (
        not positive
        or not sessions
        or any(day > evidence.valuation_time.date() for day in sessions)
    ):
        return result
    volatility_sessions = sessions[-64:]
    correlation_sessions = sessions[-127:]
    drawdown_sessions = sessions[-252:]
    volatility = annualized_volatility_by_session(
        _rebased_nav(positive, evidence.closes, volatility_sessions), volatility_sessions, 63
    )
    drawdown = maximum_drawdown_by_session(
        _rebased_nav(positive, evidence.closes, drawdown_sessions), drawdown_sessions, 252
    )
    correlation = beta_and_correlation_by_session(
        _rebased_nav(positive, evidence.closes, correlation_sessions),
        evidence.benchmark,
        published_sessions=correlation_sessions,
    )
    for name, value in (
        ("volatility", volatility),
        ("drawdown", drawdown),
        ("correlation", None if correlation is None else correlation[1]),
    ):
        if value is not None:
            result[name] = TypedMetric.known(value)
    return result


def _rebased_nav(
    positive: Mapping[UUID, Decimal],
    closes: Mapping[UUID, Mapping[date, Decimal]],
    sessions: tuple[date, ...],
) -> dict[date, Decimal]:
    # The anchor depends on the published calendar, never on which prices a
    # particular proposal happens to have. Missing anchors stay unknown.
    if any(i not in closes or closes[i].get(sessions[0], 0) <= 0 for i in positive):
        return {}
    cash = Decimal(1) - sum(positive.values(), Decimal(0))
    return {
        day: cash
        + sum(
            (weight * closes[i][day] / closes[i][sessions[0]] for i, weight in positive.items()),
            Decimal(0),
        )
        for day in sessions
        if all(closes[i].get(day, 0) > 0 for i in positive)
    }
