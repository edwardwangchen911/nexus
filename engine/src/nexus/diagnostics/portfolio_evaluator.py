"""Evidence-qualified portfolio-value diagnostic evaluation."""

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from nexus.diagnostics.evidence import EvidenceResolver, EvidenceResult, combine_refs
from nexus.diagnostics.items import item, threshold_item, unknown_item, upper_threshold_item
from nexus.diagnostics.market_risk import market_risk_items
from nexus.diagnostics.metrics import days_to_exit, security_hhi, top_weights
from nexus.diagnostics.models import DiagnosticItem, EvidenceBinding, MarketEvidence
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.diagnostics.valuation import position_values
from nexus.domain.evidence import EvidenceRef
from nexus.providers.broker.base import BrokerPosition, PortfolioSnapshot


def evaluate_currency_scopes(
    snapshot: PortfolioSnapshot,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    resolver: EvidenceResolver,
    snapshot_refs: tuple[EvidenceRef, ...],
) -> list[DiagnosticItem]:
    positions_by_currency: dict[str, list[BrokerPosition]] = defaultdict(list)
    cash_by_currency: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    for cash in snapshot.cash:
        cash_by_currency[cash.currency] += cash.cash_balance
    for position in snapshot.positions:
        if position.quantity != 0:
            positions_by_currency[position.currency].append(position)
    items: list[DiagnosticItem] = []
    for currency in sorted(set(positions_by_currency) | set(cash_by_currency)):
        items.extend(
            _currency_items(
                currency,
                positions_by_currency[currency],
                cash_by_currency[currency],
                snapshot,
                market,
                policy,
                resolver,
                snapshot_refs,
            )
        )
    return items


def _currency_items(
    currency: str,
    positions: list[BrokerPosition],
    cash: Decimal,
    snapshot: PortfolioSnapshot,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    resolver: EvidenceResolver,
    snapshot_refs: tuple[EvidenceRef, ...],
) -> list[DiagnosticItem]:
    scope = f"currency:{currency}"
    results = {
        position.instrument_id: resolver.resolve(
            market.evidence.position_prices.get(position.instrument_id),
            "POSITION_PRICE",
            expected_as_of=position.provider_as_of,
        )
        for position in positions
    }
    invalid = tuple(
        (position, results[position.instrument_id])
        for position in positions
        if results[position.instrument_id].reason is not None
    )
    values, value_unknown = position_values(positions)
    items: list[DiagnosticItem] = []
    if value_unknown is not None:
        items.append(
            unknown_item(
                "POSITION_MARKET_VALUE",
                currency,
                value_unknown,
                scope,
                market,
                evidence_refs=_all_position_refs(positions, results, market, snapshot_refs),
            )
        )
    for position, result in invalid:
        assert result.reason is not None
        items.append(
            unknown_item(
                f"POSITION_MARKET_VALUE:{position.instrument_id}",
                currency,
                result.reason,
                scope,
                market,
                evidence_refs=_position_refs(position, result.refs, market, snapshot_refs),
            )
        )
    if values and not invalid and value_unknown is None:
        value_refs = _all_position_refs(
            [position for position, _ in values], results, market, snapshot_refs
        )
        items.extend(_concentration_items(values, scope, market, policy, value_refs))
        if market.quality == "PASS" and all(
            market.identities.get(position.instrument_id) is not None
            and market.identities[position.instrument_id].restriction == "STANDARD"
            for position, _ in values
        ):
            items.extend(_exposure_items(values, scope, market, policy, resolver, value_refs))
        # ADV uses qualified close * volume and does not require adjusted returns
        # or a benchmark. Its own evidence resolver still rejects missing/future inputs.
        if market.quality in {"PASS", "PARTIAL"}:
            items.extend(_liquidity_items(values, scope, market, policy, resolver, value_refs))
        if market.quality == "PASS":
            items.extend(market_risk_items(values, scope, market, policy, snapshot, resolver))
    total = cash + sum((value for _, value in values), Decimal(0))
    if total > 0:
        items.append(
            _cash_item(
                currency,
                cash,
                total,
                bool(invalid) or value_unknown is not None,
                snapshot,
                market,
                policy,
                resolver,
                snapshot_refs,
                _all_position_refs(positions, results, market, snapshot_refs),
            )
        )
    return items


def _cash_item(
    currency: str,
    cash: Decimal,
    total: Decimal,
    invalid_position: bool,
    snapshot: PortfolioSnapshot,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    resolver: EvidenceResolver,
    snapshot_refs: tuple[EvidenceRef, ...],
    position_refs: tuple[EvidenceRef, ...],
) -> DiagnosticItem:
    scope = f"currency:{currency}"
    rows = tuple(item for item in snapshot.cash if item.currency == currency)
    expected_as_of = rows[0].provider_as_of if len(rows) == 1 else None
    cash_binding = market.evidence.cash.get(currency)
    evidence = resolver.resolve(cash_binding, "CASH", expected_as_of=expected_as_of)
    cash_refs = _safe_binding_refs(cash_binding, snapshot.as_of)
    if evidence.reason is not None or invalid_position:
        return unknown_item(
            "CASH_RATIO",
            "ratio",
            evidence.reason or "POSITION_PRICE_EVIDENCE_MISSING",
            scope,
            market,
            evidence_refs=combine_refs(snapshot_refs, position_refs, evidence.refs, cash_refs),
        )
    ratio = cash / total
    return threshold_item(
        "CASH_RATIO",
        ratio,
        "ratio",
        policy.cash_minimum,
        scope,
        market,
        warning=ratio < policy.cash_minimum,
        evidence_refs=combine_refs(snapshot_refs, position_refs, evidence.refs, cash_refs),
    )


def _concentration_items(
    values: list[tuple[BrokerPosition, Decimal]],
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    refs: tuple[EvidenceRef, ...],
) -> list[DiagnosticItem]:
    amounts = tuple(value for _, value in values)
    weights = top_weights(amounts)
    hhi = security_hhi(amounts)
    if weights is None or hhi is None:
        return [unknown_item("CONCENTRATION_HHI", "ratio", "NO_SECURITIES", scope, market)]
    top3 = sum(weights[:3], Decimal(0))
    return [
        upper_threshold_item(
            "CONCENTRATION_TOP1",
            weights[0],
            policy.single_name_warning,
            policy.single_name_critical,
            scope,
            market,
            evidence_refs=refs,
        ),
        threshold_item(
            "CONCENTRATION_TOP3",
            top3,
            "ratio",
            policy.top3_warning,
            scope,
            market,
            warning=top3 > policy.top3_warning,
            evidence_refs=refs,
        ),
        item(
            "CONCENTRATION_HHI",
            hhi,
            "ratio",
            None,
            scope,
            "info",
            "monitor",
            None,
            market,
            evidence_refs=refs,
        ),
    ]


def _exposure_items(
    values: list[tuple[BrokerPosition, Decimal]],
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    resolver: EvidenceResolver,
    value_refs: tuple[EvidenceRef, ...],
) -> list[DiagnosticItem]:
    total = sum((value for _, value in values), Decimal(0))
    buckets: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    risk_refs: list[tuple[EvidenceRef, ...]] = []
    for position, value in values:
        definitions = market.risk_buckets.get(position.instrument_id)
        evidence = resolver.resolve(
            market.evidence.risk_buckets.get(position.instrument_id), "RISK_BUCKET"
        )
        if not definitions or evidence.reason is not None:
            return [
                unknown_item(
                    "RISK_BUCKET_EXPOSURE",
                    "ratio",
                    evidence.reason or "RISK_BUCKET_MISSING",
                    scope,
                    market,
                    evidence_refs=combine_refs(value_refs, evidence.refs),
                )
            ]
        risk_refs.append(evidence.refs)
        for bucket, bucket_weight in definitions.items():
            buckets[bucket] += (value / total) * bucket_weight
    refs = combine_refs(value_refs, *risk_refs)
    return [
        upper_threshold_item(
            f"RISK_BUCKET_EXPOSURE:{bucket}",
            weight,
            policy.sector_warning,
            policy.sector_critical,
            scope,
            market,
            evidence_refs=refs,
        )
        for bucket, weight in sorted(buckets.items())
    ]


def _liquidity_items(
    values: list[tuple[BrokerPosition, Decimal]],
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    resolver: EvidenceResolver,
    value_refs: tuple[EvidenceRef, ...],
) -> list[DiagnosticItem]:
    result: list[DiagnosticItem] = []
    for position, value in values:
        evidence = resolver.resolve(market.evidence.adv20.get(position.instrument_id), "ADV20")
        metric = days_to_exit(value, market.adv20.get(position.instrument_id))
        code = f"LIQUIDITY_DAYS_TO_EXIT:{position.instrument_id}"
        if metric is None or evidence.reason is not None:
            reason = "ADV20_MISSING" if metric is None else evidence.reason
            series = market.prices.get(position.instrument_id)
            if metric is None and series is not None and series.source == "fosun":
                reason = "ADV20_VOLUME_UNIT_UNVERIFIED"
            assert reason is not None
            result.append(
                unknown_item(
                    code,
                    "trading_days",
                    reason,
                    scope,
                    market,
                    evidence_refs=combine_refs(value_refs, evidence.refs),
                )
            )
        else:
            result.append(
                upper_threshold_item(
                    code,
                    metric,
                    policy.days_to_exit_warning,
                    policy.days_to_exit_critical,
                    scope,
                    market,
                    unit="trading_days",
                    evidence_refs=combine_refs(value_refs, evidence.refs),
                )
            )
    return result


def _all_position_refs(
    positions: list[BrokerPosition],
    results: dict[UUID, EvidenceResult],
    market: MarketEvidence,
    snapshot_refs: tuple[EvidenceRef, ...],
) -> tuple[EvidenceRef, ...]:
    return combine_refs(
        *(
            _position_refs(
                position,
                results[position.instrument_id].refs,
                market,
                snapshot_refs,
            )
            for position in positions
        )
    )


def _position_refs(
    position: BrokerPosition,
    price_refs: tuple[EvidenceRef, ...],
    market: MarketEvidence,
    snapshot_refs: tuple[EvidenceRef, ...],
) -> tuple[EvidenceRef, ...]:
    identity = market.identities.get(position.instrument_id)
    identity_refs = () if identity is None else identity.authority_refs
    binding = market.evidence.position_prices.get(position.instrument_id)
    snapshot_as_of = None if market.evidence.snapshot is None else market.evidence.snapshot.as_of
    return combine_refs(
        snapshot_refs,
        price_refs,
        _safe_binding_refs(binding, snapshot_as_of),
        identity_refs,
    )


def _safe_binding_refs(
    binding: EvidenceBinding | None, snapshot_as_of: datetime | None
) -> tuple[EvidenceRef, ...]:
    if binding is None or snapshot_as_of is None:
        return ()
    return tuple(ref for ref in binding.refs if ref.available_at <= snapshot_as_of)
