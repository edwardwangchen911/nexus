"""Versioned canonical hashes for paper decisions, risk, and accounting."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from nexus.paper.models import LedgerTransaction
from nexus.risk.models import (
    OrderCandidate,
    PaperPortfolioProjection,
    RiskEvaluation,
    RiskMarketSnapshot,
)
from nexus.strategy.forward import ForwardStrategyDecision

_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_QUANTUM = Decimal("0.000000000001")


def _decimal(value: Decimal) -> str:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("canonical financial values must be finite Decimal instances")
    canonical = value.quantize(_QUANTUM)
    if canonical != value:
        raise ValueError("canonical financial values must fit NUMERIC(38,12)")
    return format(canonical, ".12f")


def _uuid(value: UUID) -> str:
    return str(value).lower()


def _optional_uuid(value: UUID | None) -> str | None:
    return None if value is None else _uuid(value)


def _date(value: date) -> str:
    return value.isoformat()


def _utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("canonical timestamps must be timezone-aware UTC")
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("canonical timestamps must use UTC")
    normalized = value.astimezone(UTC)
    return normalized.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _hash(value: str) -> str:
    if _HASH_PATTERN.fullmatch(value) is None:
        raise ValueError("canonical content hash must be 64 lowercase hexadecimal characters")
    return value


def _digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_target_payload(decision: ForwardStrategyDecision) -> list[dict[str, Any]]:
    return [
        {
            "exit": target.exit,
            "instrument_id": _uuid(target.instrument_id),
            "reason": target.reason,
            "strategy_priority": target.strategy_priority,
            "target_weight": _decimal(target.target_weight),
        }
        for target in sorted(decision.targets, key=lambda item: item.instrument_id.int)
    ]


def canonical_forward_strategy_decision_hash(decision: ForwardStrategyDecision) -> str:
    """Hash a registry-bound forward decision excluding only its stored digest."""
    return _digest(
        {
            "schema": "forward_strategy_decision_sha256_v2",
            "account_id": _uuid(decision.account_id),
            "portfolio_id": _uuid(decision.portfolio_id),
            "strategy_id": decision.strategy_id,
            "strategy_version": decision.strategy_version,
            "strategy_definition_id": _uuid(decision.strategy_definition_id),
            "registry_event_id": _uuid(decision.registry_event_id),
            "champion_generation": decision.champion_generation,
            "decision_session": _date(decision.decision_session),
            "decision_time": _utc(decision.decision_time),
            "execution_session": _date(decision.execution_session),
            "source_policy_id": _uuid(decision.source_policy_id),
            "calendar_authority_id": _uuid(decision.calendar_authority_id),
            "universe_snapshot_id": _uuid(decision.universe_snapshot_id),
            "quality_report_id": _uuid(decision.quality_report_id),
            "portfolio_projection_hash": _hash(decision.portfolio_projection_hash),
            "authority_hash": _hash(decision.authority_hash),
            "strategy_parameters_hash": _hash(decision.strategy_parameters_hash),
            "cash_weight": _decimal(decision.cash_weight),
            "reason": decision.reason,
            "targets": canonical_target_payload(decision),
        }
    )


def canonical_legacy_forward_strategy_decision_hash(
    decision: ForwardStrategyDecision,
) -> str:
    """Recompute immutable pre-Phase-2 baseline hashes for migration reads only."""
    if decision.strategy_id != "baseline" or decision.strategy_version != "baseline_v1":
        raise ValueError("legacy forward hashes apply only to baseline_v1")
    return _digest(
        {
            "schema": "forward_strategy_decision_sha256_v1",
            "account_id": _uuid(decision.account_id),
            "portfolio_id": _uuid(decision.portfolio_id),
            "strategy_id": decision.strategy_id,
            "strategy_version": decision.strategy_version,
            "decision_session": _date(decision.decision_session),
            "decision_time": _utc(decision.decision_time),
            "execution_session": _date(decision.execution_session),
            "source_policy_id": _uuid(decision.source_policy_id),
            "calendar_authority_id": _uuid(decision.calendar_authority_id),
            "universe_snapshot_id": _uuid(decision.universe_snapshot_id),
            "quality_report_id": _uuid(decision.quality_report_id),
            "portfolio_projection_hash": _hash(decision.portfolio_projection_hash),
            "authority_hash": _hash(decision.authority_hash),
            "strategy_parameters_hash": _hash(decision.strategy_parameters_hash),
            "cash_weight": _decimal(decision.cash_weight),
            "reason": decision.reason,
            "targets": canonical_target_payload(decision),
        }
    )


def _position_payload(projection: PaperPortfolioProjection) -> list[dict[str, Any]]:
    return [
        {
            "cost_basis": _decimal(position.cost_basis),
            "instrument_id": _uuid(position.instrument_id),
            "quantity": _decimal(position.quantity),
            "reference_close": _decimal(position.reference_close),
            "reserved_quantity": _decimal(position.reserved_quantity),
        }
        for position in sorted(projection.positions, key=lambda item: item.instrument_id.int)
    ]


def _open_order_payload(projection: PaperPortfolioProjection) -> list[dict[str, Any]]:
    payloads = [
        {
            "instrument_id": _uuid(order.instrument_id),
            "quantity": _decimal(order.quantity),
            "reserved_cash": _decimal(order.reserved_cash),
            "reserved_quantity": _decimal(order.reserved_quantity),
            "side": order.side,
            "status": order.status,
            "worst_price": _decimal(order.worst_price),
        }
        for order in projection.open_orders
    ]
    return sorted(
        payloads,
        key=lambda item: (
            item["instrument_id"],
            item["side"],
            item["quantity"],
            item["status"],
            item["worst_price"],
        ),
    )


def canonical_paper_projection_hash(projection: PaperPortfolioProjection) -> str:
    """Hash the complete risk projection without random order root IDs."""
    return _digest(
        {
            "schema": "paper_projection_sha256_v1",
            "account_id": _uuid(projection.account_id),
            "portfolio_id": _uuid(projection.portfolio_id),
            "as_of": _utc(projection.as_of),
            "currency": projection.currency,
            "account_model": projection.account_model,
            "position_model": projection.position_model,
            "available_cash": _decimal(projection.available_cash),
            "reserved_cash": _decimal(projection.reserved_cash),
            "nav": _decimal(projection.nav),
            "peak_nav": _decimal(projection.peak_nav),
            "drawdown_state": projection.drawdown_state,
            "source_facts_hash": _hash(projection.source_facts_hash),
            "positions": _position_payload(projection),
            "open_orders": _open_order_payload(projection),
        }
    )


def _candidate_payload(candidate: OrderCandidate) -> dict[str, Any]:
    return {
        "asset_type": candidate.asset_type,
        "currency": candidate.currency,
        "desired_quantity": _decimal(candidate.desired_quantity),
        "instrument_id": _uuid(candidate.instrument_id),
        "reference_close": _decimal(candidate.reference_close),
        "sector_exposures": [
            {
                "authority_id": _uuid(exposure.authority_id),
                "sector": exposure.sector,
                "weight": _decimal(exposure.weight),
            }
            for exposure in sorted(
                candidate.sector_exposures,
                key=lambda item: (item.sector, item.authority_id.int),
            )
        ],
        "side": candidate.side,
        "strategy_priority": candidate.strategy_priority,
        "target_weight": _decimal(candidate.target_weight),
    }


def _market_payload(market: RiskMarketSnapshot) -> dict[str, Any]:
    return {
        "authority_hash": _hash(market.authority_hash),
        "calendar_authority_id": _uuid(market.calendar_authority_id),
        "decision_session": _date(market.decision_session),
        "decision_time": _utc(market.decision_time),
        "execution_session": _date(market.execution_session),
        "prices": [
            {
                "authority_id": _uuid(price.authority_id),
                "available_at": _utc(price.available_at),
                "close": _decimal(price.close),
                "delivery_mode": price.delivery_mode,
                "instrument_id": _uuid(price.instrument_id),
                "source": price.source,
            }
            for price in sorted(market.prices, key=lambda item: item.instrument_id.int)
        ],
        "quality_report_id": _optional_uuid(market.quality_report_id),
        "query_proofs": [
            {
                "authority_id": _uuid(proof.authority_id),
                "available_at": _utc(proof.available_at),
                "blocked": proof.blocked,
                "complete": proof.complete,
                "content_hash": _hash(proof.content_hash),
                "kind": proof.kind,
                **(
                    {
                        "required_instrument_ids": [
                            _uuid(value) for value in proof.required_instrument_ids
                        ]
                    }
                    if proof.required_instrument_ids is not None
                    else {}
                ),
            }
            for proof in sorted(
                market.query_proofs,
                key=lambda item: (
                    tuple(value.int for value in item.required_instrument_ids or ()),
                    item.kind,
                ),
            )
        ],
        "required_instrument_ids": sorted(
            _uuid(instrument_id) for instrument_id in market.required_instrument_ids
        ),
        "sector_exposures": [
            {
                "authority_id": _uuid(exposure.authority_id),
                "instrument_id": _uuid(exposure.instrument_id),
                "sector": exposure.sector,
                "weight": _decimal(exposure.weight),
            }
            for exposure in sorted(
                market.sector_exposures,
                key=lambda item: (item.instrument_id.int, item.sector, item.authority_id.int),
            )
        ],
        "source_policy_id": _uuid(market.source_policy_id),
        "universe_snapshot_id": _uuid(market.universe_snapshot_id),
        "whitelisted_instrument_ids": sorted(
            _uuid(instrument_id) for instrument_id in market.whitelisted_instrument_ids
        ),
    }


def canonical_risk_market_authority_hash(market: RiskMarketSnapshot) -> str:
    """Hash exact market authority facts without trusting their stored digest."""
    payload = _market_payload(market)
    payload.pop("authority_hash")
    price_hashes = {
        _uuid(price.instrument_id): _hash(price.content_hash) for price in market.prices
    }
    for price_payload in payload["prices"]:
        price = next(
            item
            for item in market.prices
            if _uuid(item.instrument_id) == price_payload["instrument_id"]
        )
        price_payload["market"] = price.market
        price_payload["currency"] = price.currency
        price_payload["asset_type"] = price.asset_type
        price_payload["content_hash"] = price_hashes[price_payload["instrument_id"]]
    sector_hashes = {
        (
            _uuid(exposure.instrument_id),
            exposure.sector,
            _uuid(exposure.authority_id),
        ): _hash(exposure.content_hash)
        for exposure in market.sector_exposures
    }
    for sector_payload in payload["sector_exposures"]:
        key = (
            sector_payload["instrument_id"],
            sector_payload["sector"],
            sector_payload["authority_id"],
        )
        sector_payload["content_hash"] = sector_hashes[key]
    return _digest(
        {
            "schema": "risk_market_authority_sha256_v1",
            "market": payload,
        }
    )


def _candidate_sort_key(candidate: OrderCandidate) -> tuple[int, Decimal, int]:
    return (
        candidate.strategy_priority,
        -candidate.target_weight,
        candidate.instrument_id.int,
    )


def canonical_risk_batch_input_hash(
    *,
    candidates: Sequence[OrderCandidate],
    portfolio: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
    policy_version: str,
    policy_hash: str,
) -> str:
    """Hash the full deterministic input independently of caller ordering."""
    return _digest(
        {
            "schema": "risk_batch_input_sha256_v1",
            "candidates": [
                _candidate_payload(candidate)
                for candidate in sorted(candidates, key=_candidate_sort_key)
            ],
            "market": _market_payload(market),
            "policy": {
                "content_hash": _hash(policy_hash),
                "version": policy_version,
            },
            "portfolio": {
                "content_hash": canonical_paper_projection_hash(portfolio),
                "stored_hash": _hash(portfolio.projection_hash),
            },
        }
    )


def _evaluation_payload(evaluation: RiskEvaluation) -> dict[str, Any]:
    return {
        "approved_quantity": _decimal(evaluation.approved_quantity),
        "desired_quantity": _decimal(evaluation.desired_quantity),
        "instrument_id": _uuid(evaluation.instrument_id),
        "outcome": evaluation.outcome,
        "projected_available_cash": _decimal(evaluation.projected_available_cash),
        "projected_cash_ratio": _decimal(evaluation.projected_cash_ratio),
        "projected_name_ratio": _decimal(evaluation.projected_name_ratio),
        "projected_sector_ratios": [
            {"ratio": _decimal(ratio), "sector": sector}
            for sector, ratio in sorted(evaluation.projected_sector_ratios)
        ],
        "reference_close": _decimal(evaluation.reference_close),
        "reserve_amount": _decimal(evaluation.reserve_amount),
        "rules": [
            {
                "authority_ids": sorted(_uuid(value) for value in rule.authority_ids),
                "limit": _decimal(rule.limit) if rule.limit is not None else None,
                "observed": _decimal(rule.observed) if rule.observed is not None else None,
                "passed": rule.passed,
                "remaining": _decimal(rule.remaining) if rule.remaining is not None else None,
                "rule_code": rule.rule_code,
            }
            for rule in sorted(evaluation.rules, key=lambda item: item.rule_code)
        ],
        "side": evaluation.side,
        "strategy_priority": evaluation.strategy_priority,
        "target_weight": _decimal(evaluation.target_weight),
        "worst_price": _decimal(evaluation.worst_price),
    }


def _evaluation_sort_key(evaluation: RiskEvaluation) -> tuple[int, Decimal, int]:
    return (
        evaluation.strategy_priority,
        -evaluation.target_weight,
        evaluation.instrument_id.int,
    )


def canonical_risk_evaluation_result_hash(
    evaluations: Sequence[RiskEvaluation],
) -> str:
    """Hash all candidate outcomes while excluding their stored digest fields."""
    return _digest(
        {
            "schema": "risk_evaluation_result_sha256_v1",
            "evaluations": [
                _evaluation_payload(evaluation)
                for evaluation in sorted(evaluations, key=_evaluation_sort_key)
            ],
        }
    )


def canonical_paper_ledger_transaction_hash(transaction: LedgerTransaction) -> str:
    """Hash accounting meaning while excluding random transaction/entry IDs."""
    return _digest(
        {
            "schema": "paper_ledger_transaction_sha256_v1",
            "account_id": _uuid(transaction.account_id),
            "transaction_type": transaction.transaction_type,
            "reference_type": transaction.reference_type,
            "reference_hash": _hash(transaction.reference_hash),
            "occurred_at": _utc(transaction.occurred_at),
            "entries": [
                {
                    "account_code": entry.account_code,
                    "credit_amount": _decimal(entry.credit_amount),
                    "currency": entry.currency,
                    "debit_amount": _decimal(entry.debit_amount),
                    "entry_seq": entry.entry_seq,
                }
                for entry in sorted(transaction.entries, key=lambda item: item.entry_seq)
            ],
        }
    )


def canonical_paper_order_batch_hash(
    *,
    account_id: UUID,
    decision_hash: str,
    risk_input_hash: str,
    evaluations: Sequence[RiskEvaluation],
) -> str:
    """Bind the paper batch to the exact decision, input, and ordered results."""
    return _digest(
        {
            "schema": "paper_order_batch_sha256_v1",
            "account_id": _uuid(account_id),
            "decision_hash": _hash(decision_hash),
            "risk_input_hash": _hash(risk_input_hash),
            "evaluations": [
                {
                    "instrument_id": _uuid(evaluation.instrument_id),
                    "result_hash": _hash(evaluation.result_hash),
                }
                for evaluation in sorted(evaluations, key=_evaluation_sort_key)
            ],
        }
    )
