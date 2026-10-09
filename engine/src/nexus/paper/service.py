"""Atomic internal-paper batch creation; no broker or live-order capability."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_candidate import require_candidate_consumer_profile
from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.paper.agent_decision import AgentPaperDecision
from nexus.paper.agent_decision_store import (
    load_paper_decision,
    load_portfolio_proposal,
    require_agent_decision_bindings,
    require_agent_source_policies,
)
from nexus.paper.errors import PaperBatchError, PaperBatchErrorCode
from nexus.paper.ledger import make_buy_reservation_transaction
from nexus.paper.notification import (
    build_order_decision_context,
    build_risk_rejected_snapshot,
    notification_snapshot_payload,
)
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_V1
from nexus.paper.portfolio_proposal import canonical_portfolio_target_content_hash
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    PaperAccountRecord,
    PaperDrawdownLockEventRecord,
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
    PaperTargetVersionRecord,
    RiskEvaluationPriceInputRecord,
    RiskEvaluationQueryProofRecord,
    RiskEvaluationRecord,
    RiskEvaluationRuleRecord,
    RiskEvaluationSectorInputRecord,
    RiskNoChangeEvaluationRecord,
    RiskPolicyRecord,
    StrategyDecisionRecord,
    StrategyDecisionTargetRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, OutboxEvent
from nexus.quality.source_policy import (
    SOURCE_POLICY_V2_ID,
    current_paper_order_price_authority_available,
    load_source_policy_v2,
)
from nexus.risk.authority import (
    load_risk_market_snapshot,
    prepare_schwab_realtime_risk_snapshot,
)
from nexus.risk.canonical import (
    canonical_paper_order_batch_hash,
    canonical_risk_batch_input_hash,
    canonical_risk_evaluation_result_hash,
)
from nexus.risk.evaluate import (
    RiskBatchRejected,
    derive_order_candidates,
    evaluate_order_batch,
    target_whole_share_quantity,
)
from nexus.risk.models import (
    OrderCandidate,
    PaperDecisionAuthority,
    PaperPortfolioProjection,
    RiskEvaluation,
    RiskMarketSnapshot,
    RiskRuleEvidence,
)
from nexus.risk.policy import RiskPolicy, select_active_policy
from nexus.strategy.forward import ForwardStrategyTarget


def meets_portfolio_minimum_notional(candidate: OrderCandidate) -> bool:
    """Apply the portfolio policy floor equally to buy and sell candidates."""
    return (
        candidate.desired_quantity * candidate.reference_close
        >= PORTFOLIO_ALLOCATION_V1.minimum_notional_usd
    )


_ROUTE = "paper.order_batch.create"
_ACTOR = "system:task18"


@dataclass(frozen=True)
class PaperBatchHooks:
    """Optional deterministic phase observer used for tracing and fault tests."""

    after_phase: Callable[[str], None]


@dataclass(frozen=True)
class _NoChangeEvidence:
    instrument_id: UUID
    current_quantity: Decimal
    target_quantity: Decimal
    target_weight: Decimal
    strategy_priority: int
    reference_close: Decimal
    price_authority_id: UUID
    authority_ids: tuple[UUID, ...]


class PaperOrderBatchResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    batch_id: UUID
    account_id: UUID
    strategy_decision_id: UUID
    decision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    projection_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    risk_input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    batch_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evaluated_at: datetime
    order_ids: tuple[UUID, ...]
    evaluations: tuple[RiskEvaluation, ...]

    @field_validator("evaluations", mode="before")
    @classmethod
    def _stored_evaluations_are_exact(cls, value: object) -> object:
        if not isinstance(value, tuple | list):
            return value
        decimal_fields = (
            "desired_quantity",
            "approved_quantity",
            "target_weight",
            "reference_close",
            "worst_price",
            "reserve_amount",
            "projected_available_cash",
            "projected_cash_ratio",
            "projected_name_ratio",
        )

        def stored_decimal(item: object) -> object:
            if not isinstance(item, str):
                return item
            try:
                decimal = Decimal(item)
            except (InvalidOperation, ValueError) as error:
                raise ValueError("stored batch decimal is invalid") from error
            if decimal == 0 and item.startswith("-"):
                raise ValueError("stored batch decimal cannot be negative zero")
            return decimal

        evaluations: list[object] = []
        for raw in value:
            if isinstance(raw, RiskEvaluation) or not isinstance(raw, Mapping):
                evaluations.append(raw)
                continue
            evaluation = dict(raw)
            for field in decimal_fields:
                if field in evaluation:
                    evaluation[field] = stored_decimal(evaluation[field])
            ratios = evaluation.get("projected_sector_ratios")
            if isinstance(ratios, tuple | list):
                evaluation["projected_sector_ratios"] = tuple(
                    (sector, stored_decimal(ratio)) for sector, ratio in ratios
                )
            rules = evaluation.get("rules")
            if isinstance(rules, tuple | list):
                converted_rules = []
                for raw_rule in rules:
                    if not isinstance(raw_rule, Mapping):
                        converted_rules.append(raw_rule)
                        continue
                    rule = dict(raw_rule)
                    for field in ("limit", "observed", "remaining"):
                        if field in rule:
                            rule[field] = stored_decimal(rule[field])
                    converted_rules.append(rule)
                evaluation["rules"] = tuple(converted_rules)
            evaluations.append(evaluation)
        return tuple(evaluations)

    @field_validator("evaluated_at")
    @classmethod
    def _time_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("evaluated_at must use UTC")
        return value


class _PortfolioDecisionAuthority(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cycle_id: UUID
    account_id: UUID
    portfolio_id: UUID
    decision_session: date
    decision_time: datetime
    execution_session: date
    source_policy_id: UUID
    execution_source_policy_id: UUID
    calendar_authority_id: UUID
    universe_snapshot_id: UUID
    quality_report_id: UUID | None
    portfolio_projection_hash: str
    authority_hash: str
    decision_hash: str
    cash_weight: Decimal
    targets: tuple[ForwardStrategyTarget, ...]


def _subtract_portfolio_risk_remainders(
    candidates: Sequence[OrderCandidate],
    remainder_quantities: Mapping[tuple[UUID, str], Decimal],
) -> tuple[OrderCandidate, ...]:
    """Subtract only fixed same-side unapproved quantities from current deltas."""
    uncovered: list[OrderCandidate] = []
    for candidate in candidates:
        covered_quantity = remainder_quantities.get(
            (candidate.instrument_id, candidate.side), Decimal(0)
        )
        desired_quantity = candidate.desired_quantity - min(
            candidate.desired_quantity, covered_quantity
        )
        if desired_quantity > 0:
            uncovered.append(candidate.model_copy(update={"desired_quantity": desired_quantity}))
    return tuple(uncovered)


def _observe(hooks: PaperBatchHooks | None, phase: str) -> None:
    if hooks is not None:
        hooks.after_phase(phase)


def _utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise PaperBatchError("STRATEGY_DECISION_INVALID")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def canonical_paper_batch_request_hash(
    *,
    account_id: UUID,
    strategy_decision_id: UUID,
    request_time: datetime,
    target_version_id: UUID | None = None,
) -> str:
    payload = {
        "schema": "paper_order_batch_request_sha256_v1",
        "account_id": str(account_id).lower(),
        "strategy_decision_id": str(strategy_decision_id).lower(),
        "request_time": _utc(request_time),
    }
    if target_version_id is not None:
        payload["target_version_id"] = str(target_version_id).lower()
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _fact_hash(kind: str, payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            {"schema": "paper_fact_payload_sha256_v1", "kind": kind, "payload": payload},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _decimal(value: Decimal) -> str:
    return format(value, ".12f")


def _no_change_evidence_hash(
    *,
    account_id: UUID,
    strategy_decision_id: UUID,
    risk_input_hash: str,
    evidence: _NoChangeEvidence,
) -> str:
    return _fact_hash(
        "risk_no_change_evaluation",
        {
            "account_id": str(account_id).lower(),
            "strategy_decision_id": str(strategy_decision_id).lower(),
            "instrument_id": str(evidence.instrument_id).lower(),
            "current_quantity": _decimal(evidence.current_quantity),
            "target_quantity": _decimal(evidence.target_quantity),
            "target_weight": _decimal(evidence.target_weight),
            "strategy_priority": evidence.strategy_priority,
            "reference_close": _decimal(evidence.reference_close),
            "price_authority_id": str(evidence.price_authority_id).lower(),
            "authority_ids": [str(value).lower() for value in evidence.authority_ids],
            "reason_code": "NO_CHANGE",
            "input_hash": risk_input_hash,
        },
    )


def _derive_no_change_evidence(
    decision: PaperDecisionAuthority,
    portfolio: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
) -> tuple[_NoChangeEvidence, ...]:
    positions = {position.instrument_id: position.quantity for position in portfolio.positions}
    prices = {price.instrument_id: price for price in market.prices}
    sectors: dict[UUID, list[UUID]] = {}
    for exposure in market.sector_exposures:
        sectors.setdefault(exposure.instrument_id, []).append(exposure.authority_id)
    common_authorities = {
        authority_id
        for authority_id in (
            market.calendar_authority_id,
            market.universe_snapshot_id,
            market.quality_report_id,
            *(proof.authority_id for proof in market.query_proofs),
        )
        if authority_id is not None
    }
    evidence: list[_NoChangeEvidence] = []
    for target in decision.targets:
        price = prices[target.instrument_id]
        current_quantity = positions.get(target.instrument_id, Decimal(0))
        target_quantity = target_whole_share_quantity(
            target=target,
            current_quantity=current_quantity,
            nav=portfolio.nav,
            price=price.close,
        )
        if target_quantity != current_quantity:
            continue
        authority_ids = tuple(
            sorted(
                {
                    *common_authorities,
                    price.authority_id,
                    *sectors[target.instrument_id],
                },
                key=lambda value: value.int,
            )
        )
        evidence.append(
            _NoChangeEvidence(
                instrument_id=target.instrument_id,
                current_quantity=current_quantity,
                target_quantity=target_quantity,
                target_weight=target.target_weight,
                strategy_priority=target.strategy_priority,
                reference_close=price.close,
                price_authority_id=price.authority_id,
                authority_ids=authority_ids,
            )
        )
    return tuple(
        sorted(
            evidence,
            key=lambda item: (
                item.strategy_priority,
                -item.target_weight,
                item.instrument_id.int,
            ),
        )
    )


def _map_error(code: str) -> PaperBatchError:
    allowed: set[str] = {
        "PAPER_ACCOUNT_INVALID",
        "STRATEGY_DECISION_INVALID",
        "RISK_POLICY_INVALID",
        "RISK_AUTHORITY_INCOMPLETE",
        "PAPER_PROJECTION_INVALID",
        "PAPER_BATCH_CONFLICT",
        "IDEMPOTENCY_CONFLICT",
    }
    return PaperBatchError(
        cast(PaperBatchErrorCode, code if code in allowed else "PAPER_BATCH_CONFLICT")
    )


async def _lock_account_graph(
    session: AsyncSession,
    *,
    account_id: UUID,
    hooks: PaperBatchHooks | None,
) -> tuple[PaperAccountRecord, tuple[PaperLedgerAccountRecord, ...]]:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1818))"),
        {"scope": f"internal-paper:{account_id}"},
    )
    _observe(hooks, "account_locked")
    account = await session.scalar(
        select(PaperAccountRecord).where(PaperAccountRecord.id == account_id).with_for_update()
    )
    if account is None or account.capability != "simulation_write_internal":
        raise PaperBatchError("PAPER_ACCOUNT_INVALID")
    await session.scalars(
        select(PaperDrawdownLockEventRecord)
        .where(PaperDrawdownLockEventRecord.account_id == account_id)
        .order_by(PaperDrawdownLockEventRecord.event_seq)
        .with_for_update()
    )
    _observe(hooks, "drawdown_locked")
    await session.scalars(
        select(PaperOrderRecord)
        .where(PaperOrderRecord.account_id == account_id)
        .order_by(PaperOrderRecord.id)
        .with_for_update()
    )
    _observe(hooks, "orders_locked")
    await session.scalars(
        select(PaperReservationRecord)
        .where(PaperReservationRecord.account_id == account_id)
        .order_by(PaperReservationRecord.id)
        .with_for_update()
    )
    _observe(hooks, "reservations_locked")
    ledger_accounts = tuple(
        (
            await session.scalars(
                select(PaperLedgerAccountRecord)
                .where(PaperLedgerAccountRecord.paper_account_id == account_id)
                .order_by(PaperLedgerAccountRecord.code)
                .with_for_update()
            )
        ).all()
    )
    _observe(hooks, "ledger_accounts_locked")
    return account, ledger_accounts


async def _claim_idempotency(
    session: AsyncSession,
    *,
    account_id: UUID,
    idempotency_key: str,
    request_hash: str,
) -> tuple[IdempotencyRecord, bool]:
    actor_id = f"internal-paper:{account_id}"
    inserted_id = (
        await session.execute(
            insert(IdempotencyRecord)
            .values(
                actor_id=actor_id,
                route=_ROUTE,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                schema_version="paper_order_batch_result_v1",
                response_payload={},
            )
            .on_conflict_do_nothing(
                index_elements=[
                    IdempotencyRecord.actor_id,
                    IdempotencyRecord.route,
                    IdempotencyRecord.idempotency_key,
                ]
            )
            .returning(IdempotencyRecord.id)
        )
    ).scalar_one_or_none()
    record = await session.scalar(
        select(IdempotencyRecord)
        .where(
            IdempotencyRecord.actor_id == actor_id,
            IdempotencyRecord.route == _ROUTE,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
        .with_for_update()
    )
    if record is None:
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    if record.request_hash != request_hash:
        raise PaperBatchError("IDEMPOTENCY_CONFLICT")
    if inserted_id is not None:
        return record, True
    if not record.response_payload:
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    return record, False


async def _load_policy(session: AsyncSession) -> tuple[UUID, RiskPolicy]:
    rows = tuple((await session.scalars(select(RiskPolicyRecord))).all())
    try:
        policies = tuple(
            RiskPolicy(
                version=cast(Literal["risk_policy_v1"], row.version),
                active=row.active,
                single_name_limit=row.single_name_limit,
                sector_limit=row.sector_limit,
                minimum_cash_ratio=row.minimum_cash_ratio,
                drawdown_lock_ratio=row.drawdown_lock_ratio,
                drawdown_recovery_ratio=row.drawdown_recovery_ratio,
                minimum_buy_notional=row.minimum_buy_notional,
                buy_reserve_markup=row.buy_reserve_markup,
                commission=row.commission,
                supported_market=cast(Literal["US"], row.supported_market),
                supported_currency=cast(Literal["USD"], row.supported_currency),
                supported_asset_types=cast(
                    tuple[Literal["ETF", "common_stock"], ...],
                    tuple(row.supported_asset_types),
                ),
                content_hash=row.content_hash,
            )
            for row in rows
        )
        policy = select_active_policy(policies)
    except ValueError as error:
        raise PaperBatchError("RISK_POLICY_INVALID") from error
    matching = [row.id for row in rows if row.active and row.content_hash == policy.content_hash]
    if len(matching) != 1:
        raise PaperBatchError("RISK_POLICY_INVALID")
    return matching[0], policy


async def load_paper_order_batch_result(
    session: AsyncSession,
    *,
    account_id: UUID,
    strategy_decision_id: UUID,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> PaperOrderBatchResult | None:
    if type(formal_stage_capacity) is not bool or (
        formal_stage_capacity and candidate_stage_capacity is not False
    ):
        raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    batch = await session.scalar(
        select(PaperOrderBatchRecord).where(
            PaperOrderBatchRecord.account_id == account_id,
            PaperOrderBatchRecord.strategy_decision_id == strategy_decision_id,
        )
    )
    if batch is None:
        return None
    idempotency = await session.get(IdempotencyRecord, batch.idempotency_record_id)
    decision = await session.get(StrategyDecisionRecord, strategy_decision_id)
    if (
        decision is None
        or batch.account_id != account_id
        or batch.strategy_decision_id != strategy_decision_id
        or idempotency is None
        or idempotency.actor_id != f"internal-paper:{account_id}"
        or idempotency.route != _ROUTE
        or idempotency.schema_version != "paper_order_batch_result_v1"
        or idempotency.request_hash != batch.request_hash
        or batch.request_hash
        != canonical_paper_batch_request_hash(
            account_id=account_id,
            strategy_decision_id=strategy_decision_id,
            request_time=batch.evaluated_at,
            target_version_id=batch.target_version_id,
        )
    ):
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    try:
        stored_result = PaperOrderBatchResult.model_validate(idempotency.response_payload)
        if idempotency.response_payload != stored_result.model_dump(mode="json"):
            raise ValueError("stored batch result is not canonical")
        if decision.strategy_version == "agent_portfolio_proposal_v1":
            proposal = await load_portfolio_proposal(
                session,
                account_id=account_id,
                decision_id=strategy_decision_id,
                candidate_stage_capacity=candidate_stage_capacity,
                formal_stage_capacity=formal_stage_capacity,
            )
            if proposal is None:
                raise ValueError("portfolio proposal is missing")
            current_decision_hash = proposal.proposal.decision_hash
        else:
            persisted = await load_paper_decision(
                session,
                account_id=account_id,
                decision_id=strategy_decision_id,
            )
            if persisted is None:
                raise ValueError("paper decision is missing")
            current_decision_hash = persisted.decision.decision_hash
    except ValueError as error:
        raise PaperBatchError("PAPER_BATCH_CONFLICT") from error
    if (
        decision.decision_hash != current_decision_hash
        or stored_result.decision_hash != current_decision_hash
    ):
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    orders = tuple(
        (
            await session.scalars(
                select(PaperOrderRecord)
                .where(PaperOrderRecord.batch_id == batch.id)
                .order_by(
                    PaperOrderRecord.target_weight.desc(),
                    PaperOrderRecord.instrument_id,
                )
            )
        ).all()
    )
    evaluations: list[RiskEvaluation] = []
    risk_rows = tuple(
        (
            await session.scalars(
                select(RiskEvaluationRecord)
                .where(RiskEvaluationRecord.batch_id == batch.id)
                .order_by(RiskEvaluationRecord.order_id)
            )
        ).all()
    )
    if {row.order_id for row in risk_rows} != {order.id for order in orders} or len(
        risk_rows
    ) != len(orders):
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    risk_by_order = {row.order_id: row for row in risk_rows}
    for order in orders:
        evaluation = risk_by_order.get(order.id)
        if evaluation is None:
            raise PaperBatchError("PAPER_BATCH_CONFLICT")
        rules = tuple(
            (
                await session.scalars(
                    select(RiskEvaluationRuleRecord)
                    .where(RiskEvaluationRuleRecord.risk_evaluation_id == evaluation.id)
                    .order_by(RiskEvaluationRuleRecord.rule_seq)
                )
            ).all()
        )
        domain = RiskEvaluation(
            instrument_id=order.instrument_id,
            side=cast(Literal["buy", "sell"], order.side),
            outcome=cast(Literal["approved", "reduced", "rejected"], evaluation.outcome),
            desired_quantity=evaluation.desired_quantity,
            approved_quantity=evaluation.approved_quantity,
            target_weight=evaluation.target_weight,
            strategy_priority=evaluation.strategy_priority,
            reference_close=evaluation.reference_close,
            worst_price=evaluation.worst_price,
            reserve_amount=evaluation.reserve_amount,
            projected_available_cash=evaluation.projected_available_cash,
            projected_cash_ratio=evaluation.projected_cash_ratio,
            projected_name_ratio=evaluation.projected_name_ratio,
            projected_sector_ratios=tuple(
                (sector, Decimal(value))
                for sector, value in sorted(evaluation.projected_sector_ratios.items())
            ),
            rules=tuple(
                RiskRuleEvidence(
                    rule_code=row.rule_code,
                    passed=row.passed,
                    limit=row.limit_value,
                    observed=row.observed_value,
                    remaining=row.remaining_value,
                    authority_ids=tuple(row.authority_ids),
                )
                for row in rules
            ),
            result_hash=evaluation.result_hash,
        )
        if (
            evaluation.account_id != account_id
            or evaluation.risk_policy_id != batch.risk_policy_id
            or evaluation.input_hash != batch.risk_input_hash
            or order.account_id != account_id
            or order.batch_id != batch.id
            or order.strategy_decision_id != strategy_decision_id
            or order.instrument_id != domain.instrument_id
            or order.side != domain.side
            or order.risk_outcome != domain.outcome
            or order.quantity
            != (
                domain.desired_quantity
                if domain.outcome == "rejected"
                else domain.approved_quantity
            )
            or order.target_weight != domain.target_weight
            or order.reference_close != domain.reference_close
            or order.worst_price != domain.worst_price
            or any(
                row.rule_seq != index or row.account_id != account_id
                for index, row in enumerate(rules, start=1)
            )
            or canonical_risk_evaluation_result_hash((domain,)) != domain.result_hash
        ):
            raise PaperBatchError("PAPER_BATCH_CONFLICT")
        evaluations.append(domain)
    ordered = tuple(
        sorted(
            evaluations,
            key=lambda item: (
                item.strategy_priority,
                -item.target_weight,
                item.instrument_id.int,
            ),
        )
    )
    if canonical_risk_evaluation_result_hash(ordered) != batch.result_hash:
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    if (
        canonical_paper_order_batch_hash(
            account_id=account_id,
            decision_hash=current_decision_hash,
            risk_input_hash=batch.risk_input_hash,
            evaluations=ordered,
        )
        != batch.batch_hash
    ):
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    no_change_rows = tuple(
        (
            await session.scalars(
                select(RiskNoChangeEvaluationRecord)
                .where(RiskNoChangeEvaluationRecord.batch_id == batch.id)
                .order_by(
                    RiskNoChangeEvaluationRecord.strategy_priority,
                    RiskNoChangeEvaluationRecord.target_weight.desc(),
                    RiskNoChangeEvaluationRecord.instrument_id,
                )
            )
        ).all()
    )
    for row in no_change_rows:
        evidence = _NoChangeEvidence(
            instrument_id=row.instrument_id,
            current_quantity=row.current_quantity,
            target_quantity=row.target_quantity,
            target_weight=row.target_weight,
            strategy_priority=row.strategy_priority,
            reference_close=row.reference_close,
            price_authority_id=row.price_authority_id,
            authority_ids=tuple(row.authority_ids),
        )
        if row.input_hash != batch.risk_input_hash or row.evidence_hash != (
            _no_change_evidence_hash(
                account_id=account_id,
                strategy_decision_id=strategy_decision_id,
                risk_input_hash=batch.risk_input_hash,
                evidence=evidence,
            )
        ):
            raise PaperBatchError("PAPER_BATCH_CONFLICT")
    order_by_instrument = {order.instrument_id: order for order in orders}
    order_ids = tuple(order_by_instrument[item.instrument_id].id for item in ordered)
    result = PaperOrderBatchResult(
        batch_id=batch.id,
        account_id=batch.account_id,
        strategy_decision_id=batch.strategy_decision_id,
        decision_hash=current_decision_hash,
        projection_hash=batch.projection_hash,
        request_hash=batch.request_hash,
        risk_input_hash=batch.risk_input_hash,
        result_hash=batch.result_hash,
        batch_hash=batch.batch_hash,
        evaluated_at=batch.evaluated_at,
        order_ids=order_ids,
        evaluations=ordered,
    )
    if stored_result != result:
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    return result


def _event_record(
    *,
    order_id: UUID,
    account_id: UUID,
    event_seq: int,
    from_status: str | None,
    to_status: str,
    occurred_at: datetime,
    reason_code: str,
) -> PaperOrderEventRecord:
    payload = {
        "order_id": str(order_id),
        "event_seq": event_seq,
        "from_status": from_status,
        "to_status": to_status,
        "occurred_at": _utc(occurred_at),
        "reason_code": reason_code,
    }
    return PaperOrderEventRecord(
        id=new_uuid7(),
        order_id=order_id,
        account_id=account_id,
        event_seq=event_seq,
        from_status=from_status,
        to_status=to_status,
        actor_kind="task18",
        reason_code=reason_code,
        occurred_at=occurred_at,
        payload_hash=_fact_hash("paper_order_event", payload),
    )


def _event_pairs(
    *,
    order_id: UUID,
    account_id: UUID,
    outcome: str,
    occurred_at: datetime,
) -> tuple[PaperOrderEventRecord, ...]:
    transitions: tuple[tuple[str | None, str, str], ...]
    if outcome == "rejected":
        transitions = (
            (None, "proposed", "PAPER_ORDER_PROPOSED"),
            ("proposed", "risk_rejected", "PAPER_RISK_REJECTED"),
        )
    else:
        transitions = (
            (None, "proposed", "PAPER_ORDER_PROPOSED"),
            ("proposed", "risk_approved", "PAPER_RISK_APPROVED"),
            ("risk_approved", "submitted", "PAPER_ORDER_SUBMITTED"),
        )
    return tuple(
        _event_record(
            order_id=order_id,
            account_id=account_id,
            event_seq=index,
            from_status=source,
            to_status=target,
            occurred_at=occurred_at,
            reason_code=reason,
        )
        for index, (source, target, reason) in enumerate(transitions, start=1)
    )


async def _add_audit_event(
    session: AsyncSession,
    *,
    action: str,
    payload: dict[str, object],
    occurred_at: datetime,
) -> None:
    await append_audit_event(
        session,
        actor_id=_ACTOR,
        action=action,
        schema_version="paper_event_v1",
        payload=payload,
        occurred_at=occurred_at,
    )


async def _add_event_pair(
    session: AsyncSession,
    *,
    action: str,
    aggregate_id: UUID,
    payload: dict[str, object],
    occurred_at: datetime,
) -> None:
    await _add_audit_event(
        session,
        action=action,
        payload=payload,
        occurred_at=occurred_at,
    )
    session.add(
        OutboxEvent(
            id=new_uuid7(),
            event_type=action,
            aggregate_id=aggregate_id,
            schema_version="paper_event_v1",
            payload=payload,
            published_at=None,
        )
    )


async def _persist_new_batch(
    session: AsyncSession,
    *,
    account_id: UUID,
    strategy_decision_id: UUID,
    target_version_id: UUID | None,
    decision_hash: str,
    policy_id: UUID,
    idempotency_record: IdempotencyRecord,
    request_hash: str,
    request_time: datetime,
    projection_hash: str,
    risk_input_hash: str,
    result_hash: str,
    batch_hash: str,
    decision_session: date,
    market: RiskMarketSnapshot,
    portfolio: PaperPortfolioProjection,
    no_changes: Sequence[_NoChangeEvidence],
    evaluations: Sequence[RiskEvaluation],
    ledger_accounts: Sequence[PaperLedgerAccountRecord],
    idempotency_key: str,
    hooks: PaperBatchHooks | None,
) -> PaperOrderBatchResult:
    batch_id = new_uuid7()
    batch = PaperOrderBatchRecord(
        id=batch_id,
        account_id=account_id,
        strategy_decision_id=strategy_decision_id,
        target_version_id=target_version_id,
        risk_policy_id=policy_id,
        idempotency_record_id=idempotency_record.id,
        request_hash=request_hash,
        projection_hash=projection_hash,
        risk_input_hash=risk_input_hash,
        result_hash=result_hash,
        batch_hash=batch_hash,
        evaluated_at=request_time,
    )
    session.add(batch)
    await session.flush()
    _observe(hooks, "batch_persisted")

    session.add_all(
        RiskNoChangeEvaluationRecord(
            id=new_uuid7(),
            batch_id=batch_id,
            account_id=account_id,
            strategy_decision_id=strategy_decision_id,
            instrument_id=evidence.instrument_id,
            current_quantity=evidence.current_quantity,
            target_quantity=evidence.target_quantity,
            target_weight=evidence.target_weight,
            strategy_priority=evidence.strategy_priority,
            reference_close=evidence.reference_close,
            price_authority_id=evidence.price_authority_id,
            authority_ids=list(evidence.authority_ids),
            reason_code="NO_CHANGE",
            input_hash=risk_input_hash,
            evidence_hash=_no_change_evidence_hash(
                account_id=account_id,
                strategy_decision_id=strategy_decision_id,
                risk_input_hash=risk_input_hash,
                evidence=evidence,
            ),
        )
        for evidence in no_changes
    )
    await session.flush()

    order_pairs: list[tuple[PaperOrderRecord, RiskEvaluation]] = []
    order_events: list[PaperOrderEventRecord] = []
    for evaluation in evaluations:
        order_id = new_uuid7()
        quantity = (
            evaluation.desired_quantity
            if evaluation.outcome == "rejected"
            else evaluation.approved_quantity
        )
        order = PaperOrderRecord(
            id=order_id,
            batch_id=batch_id,
            account_id=account_id,
            strategy_decision_id=strategy_decision_id,
            client_order_id=(f"paper-v1-{decision_hash[:12]}-{evaluation.instrument_id.hex}"),
            instrument_id=evaluation.instrument_id,
            side=evaluation.side,
            quantity=quantity,
            target_weight=evaluation.target_weight,
            reference_close=evaluation.reference_close,
            worst_price=evaluation.worst_price,
            risk_outcome=evaluation.outcome,
        )
        session.add(order)
        order_events.extend(
            _event_pairs(
                order_id=order_id,
                account_id=account_id,
                outcome=evaluation.outcome,
                occurred_at=request_time,
            )
        )
        order_pairs.append((order, evaluation))
    await session.flush()
    session.add_all(order_events)
    await session.flush()
    _observe(hooks, "orders_persisted")

    for order, evaluation in order_pairs:
        evaluation_id = new_uuid7()
        session.add(
            RiskEvaluationRecord(
                id=evaluation_id,
                order_id=order.id,
                batch_id=batch_id,
                account_id=account_id,
                risk_policy_id=policy_id,
                outcome=evaluation.outcome,
                desired_quantity=evaluation.desired_quantity,
                approved_quantity=evaluation.approved_quantity,
                target_weight=evaluation.target_weight,
                strategy_priority=evaluation.strategy_priority,
                reference_close=evaluation.reference_close,
                worst_price=evaluation.worst_price,
                reserve_amount=evaluation.reserve_amount,
                projected_available_cash=evaluation.projected_available_cash,
                projected_cash_ratio=evaluation.projected_cash_ratio,
                projected_name_ratio=evaluation.projected_name_ratio,
                projected_sector_ratios={
                    sector: _decimal(ratio) for sector, ratio in evaluation.projected_sector_ratios
                },
                input_hash=risk_input_hash,
                result_hash=evaluation.result_hash,
            )
        )
        await session.flush()
        session.add_all(
            RiskEvaluationRuleRecord(
                id=new_uuid7(),
                risk_evaluation_id=evaluation_id,
                account_id=account_id,
                rule_seq=index,
                rule_code=rule.rule_code,
                passed=rule.passed,
                limit_value=rule.limit,
                observed_value=rule.observed,
                remaining_value=rule.remaining,
                authority_ids=list(rule.authority_ids),
            )
            for index, rule in enumerate(evaluation.rules, start=1)
        )
        session.add_all(
            RiskEvaluationPriceInputRecord(
                id=new_uuid7(),
                risk_evaluation_id=evaluation_id,
                account_id=account_id,
                instrument_id=price.instrument_id,
                price_authority_id=price.authority_id,
                market=price.market,
                currency=price.currency,
                asset_type=price.asset_type,
                close=price.close,
                source=price.source,
                delivery_mode=price.delivery_mode,
                available_at=price.available_at,
                content_hash=price.content_hash,
            )
            for price in market.prices
        )
        session.add_all(
            RiskEvaluationSectorInputRecord(
                id=new_uuid7(),
                risk_evaluation_id=evaluation_id,
                account_id=account_id,
                instrument_id=exposure.instrument_id,
                sector_exposure_id=exposure.authority_id,
                sector=exposure.sector,
                weight=exposure.weight,
                content_hash=exposure.content_hash,
            )
            for exposure in market.sector_exposures
        )
        bound_proofs = tuple(
            proof
            for proof in market.query_proofs
            if proof.required_instrument_ids is None
            or proof.required_instrument_ids == (evaluation.instrument_id,)
        )
        if any(proof.required_instrument_ids is not None for proof in market.query_proofs):
            if len(bound_proofs) != 3 or any(
                proof.required_instrument_ids != (evaluation.instrument_id,)
                for proof in bound_proofs
            ):
                raise PaperBatchError("RISK_AUTHORITY_INCOMPLETE")
        elif len(market.required_instrument_ids) != 1:
            raise PaperBatchError("RISK_AUTHORITY_INCOMPLETE")
        session.add_all(
            RiskEvaluationQueryProofRecord(
                id=new_uuid7(),
                risk_evaluation_id=evaluation_id,
                account_id=account_id,
                kind=proof.kind,
                authority_id=proof.authority_id,
                complete=proof.complete,
                blocked=proof.blocked,
                available_at=proof.available_at,
                content_hash=proof.content_hash,
            )
            for proof in bound_proofs
        )
    await session.flush()
    _observe(hooks, "risk_persisted")

    ledger_by_code = {account.code: account for account in ledger_accounts}
    reservation_events: list[PaperReservationEventRecord] = []
    reservation_event_hash_by_order: dict[UUID, str] = {}
    ledger_transaction_hash_by_order: dict[UUID, str] = {}
    for order, evaluation in order_pairs:
        if evaluation.outcome == "rejected":
            continue
        reservation_id = new_uuid7()
        ledger_transaction_id: UUID | None = None
        if evaluation.side == "buy":
            ledger_transaction_id = new_uuid7()
            transaction = make_buy_reservation_transaction(
                transaction_id=ledger_transaction_id,
                debit_entry_id=new_uuid7(),
                credit_entry_id=new_uuid7(),
                paper_account_id=account_id,
                reference_hash=evaluation.result_hash,
                amount=evaluation.reserve_amount,
                occurred_at=request_time,
            )
            ledger_transaction_hash_by_order[order.id] = transaction.transaction_hash
            session.add(
                PaperLedgerTransactionRecord(
                    id=transaction.transaction_id,
                    account_id=account_id,
                    order_id=order.id,
                    transaction_type=transaction.transaction_type,
                    reference_type=transaction.reference_type,
                    reference_hash=transaction.reference_hash,
                    occurred_at=transaction.occurred_at,
                    transaction_hash=transaction.transaction_hash,
                )
            )
            await session.flush()
            session.add_all(
                PaperLedgerEntryRecord(
                    id=entry.entry_id,
                    transaction_id=transaction.transaction_id,
                    account_id=account_id,
                    entry_seq=entry.entry_seq,
                    ledger_account_id=ledger_by_code[entry.account_code].id,
                    account_code=entry.account_code,
                    currency=entry.currency,
                    debit_amount=entry.debit_amount,
                    credit_amount=entry.credit_amount,
                )
                for entry in transaction.entries
            )
            await session.flush()
        amount = (
            evaluation.reserve_amount if evaluation.side == "buy" else evaluation.approved_quantity
        )
        session.add(
            PaperReservationRecord(
                id=reservation_id,
                order_id=order.id,
                account_id=account_id,
                ledger_transaction_id=ledger_transaction_id,
                reservation_type="cash" if evaluation.side == "buy" else "position",
                currency="USD" if evaluation.side == "buy" else None,
                instrument_id=None if evaluation.side == "buy" else evaluation.instrument_id,
                amount=amount,
            )
        )
        await session.flush()
        event_payload = {
            "client_order_id": order.client_order_id,
            "event_seq": 1,
            "event_type": "reserved",
            "amount": _decimal(amount),
            "occurred_at": _utc(request_time),
        }
        reservation_event = PaperReservationEventRecord(
            id=new_uuid7(),
            reservation_id=reservation_id,
            account_id=account_id,
            event_seq=1,
            event_type="reserved",
            amount=amount,
            occurred_at=request_time,
            payload_hash=_fact_hash("paper_reservation_event", event_payload),
        )
        reservation_events.append(reservation_event)
        reservation_event_hash_by_order[order.id] = reservation_event.payload_hash
    session.add_all(reservation_events)
    await session.flush()
    _observe(hooks, "reservations_and_ledger_persisted")

    idempotency_key_hash = hashlib.sha256(idempotency_key.encode()).hexdigest()
    common = {
        "account_id": str(account_id),
        "strategy_decision_id": str(strategy_decision_id),
        "batch_id": str(batch_id),
        "batch_hash": batch_hash,
    }
    await _add_audit_event(
        session,
        action="strategy_decision.persisted",
        payload={**common, "decision_hash": decision_hash},
        occurred_at=request_time,
    )
    await _add_audit_event(
        session,
        action="paper.order_batch.evaluated",
        payload={
            **common,
            "request_hash": request_hash,
            "result_hash": result_hash,
            "idempotency_key_hash": idempotency_key_hash,
        },
        occurred_at=request_time,
    )
    for order, evaluation in order_pairs:
        action = (
            "paper.order.risk_rejected"
            if evaluation.outcome == "rejected"
            else "paper.order.submitted"
        )
        order_event_payload: dict[str, object] = {
            **common,
            "order_id": str(order.id),
            "instrument_id": str(order.instrument_id),
            "result_hash": evaluation.result_hash,
        }
        if evaluation.outcome == "rejected":
            try:
                context = await build_order_decision_context(
                    session,
                    strategy_decision_id=strategy_decision_id,
                    instrument_id=order.instrument_id,
                    decision_session=decision_session,
                )
                snapshot = build_risk_rejected_snapshot(
                    order=order,
                    evaluation=evaluation,
                    portfolio=portfolio,
                    context=context,
                    occurred_at=request_time,
                )
                order_event_payload.update(notification_snapshot_payload(snapshot))
            except ValueError:
                order_event_payload["notification_delivery"] = "unavailable"
        await _add_event_pair(
            session,
            action=action,
            aggregate_id=order.id,
            payload=order_event_payload,
            occurred_at=request_time,
        )
        if evaluation.outcome != "rejected":
            await _add_event_pair(
                session,
                action="paper.reservation.created",
                aggregate_id=order.id,
                payload={
                    **common,
                    "order_id": str(order.id),
                    "result_hash": evaluation.result_hash,
                    "reservation_event_hash": reservation_event_hash_by_order[order.id],
                },
                occurred_at=request_time,
            )
            if evaluation.side == "buy":
                await _add_event_pair(
                    session,
                    action="paper.ledger_transaction.posted",
                    aggregate_id=order.id,
                    payload={
                        **common,
                        "order_id": str(order.id),
                        "result_hash": evaluation.result_hash,
                        "transaction_hash": ledger_transaction_hash_by_order[order.id],
                    },
                    occurred_at=request_time,
                )
    await session.flush()
    _observe(hooks, "audit_outbox_persisted")

    result = PaperOrderBatchResult(
        batch_id=batch_id,
        account_id=account_id,
        strategy_decision_id=strategy_decision_id,
        decision_hash=decision_hash,
        projection_hash=projection_hash,
        request_hash=request_hash,
        risk_input_hash=risk_input_hash,
        result_hash=result_hash,
        batch_hash=batch_hash,
        evaluated_at=request_time,
        order_ids=tuple(order.id for order, _ in order_pairs),
        evaluations=tuple(evaluations),
    )
    idempotency_record.response_payload = result.model_dump(mode="json")
    await session.flush()
    _observe(hooks, "idempotency_completed")
    return result


async def create_paper_order_batch(
    session: AsyncSession,
    *,
    account_id: UUID,
    strategy_decision_id: UUID,
    idempotency_key: str,
    request_time: datetime,
    target_version_id: UUID | None = None,
    hooks: PaperBatchHooks | None = None,
) -> PaperOrderBatchResult:
    """Create a legacy batch; portfolio batches require the activation coordinator."""
    if target_version_id is not None:
        raise PaperBatchError("PORTFOLIO_ACTIVATION_REQUIRED")
    result = await _create_paper_order_batch_impl(
        session,
        account_id=account_id,
        strategy_decision_id=strategy_decision_id,
        idempotency_key=idempotency_key,
        request_time=request_time,
        target_version_id=None,
        portfolio_activation=False,
        hooks=hooks,
    )
    assert result is not None
    return result


async def _create_target_aware_paper_order_batch(
    session: AsyncSession,
    *,
    account_id: UUID,
    strategy_decision_id: UUID,
    idempotency_key: str,
    request_time: datetime,
    target_version_id: UUID,
    hooks: PaperBatchHooks | None = None,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> PaperOrderBatchResult | None:
    """Private activation-only target batch entrypoint; caller owns the transaction."""
    return await _create_paper_order_batch_impl(
        session,
        account_id=account_id,
        strategy_decision_id=strategy_decision_id,
        idempotency_key=idempotency_key,
        request_time=request_time,
        target_version_id=target_version_id,
        portfolio_activation=True,
        hooks=hooks,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )


async def _create_paper_order_batch_impl(
    session: AsyncSession,
    *,
    account_id: UUID,
    strategy_decision_id: UUID,
    idempotency_key: str,
    request_time: datetime,
    target_version_id: UUID | None,
    portfolio_activation: bool,
    hooks: PaperBatchHooks | None,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> PaperOrderBatchResult | None:
    """Shared deterministic batch implementation behind the two permission boundaries."""
    if type(formal_stage_capacity) is not bool or (
        formal_stage_capacity and candidate_stage_capacity is not False
    ):
        raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    if formal_stage_capacity and (not portfolio_activation or target_version_id is None):
        raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
    if candidate_stage_capacity and (not portfolio_activation or target_version_id is None):
        raise ValueError("CODEX_CANDIDATE_ISOLATION_REQUIRED")
    if not idempotency_key or len(idempotency_key) > 255:
        raise PaperBatchError("IDEMPOTENCY_CONFLICT")
    request_hash = canonical_paper_batch_request_hash(
        account_id=account_id,
        strategy_decision_id=strategy_decision_id,
        request_time=request_time,
        target_version_id=target_version_id,
    )
    account, ledger_accounts = await _lock_account_graph(
        session,
        account_id=account_id,
        hooks=hooks,
    )
    existing = await load_paper_order_batch_result(
        session,
        account_id=account_id,
        strategy_decision_id=strategy_decision_id,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )
    if existing is not None:
        existing_target = await session.scalar(
            select(PaperOrderBatchRecord.target_version_id).where(
                PaperOrderBatchRecord.id == existing.batch_id
            )
        )
        if existing_target is not None and not portfolio_activation:
            raise PaperBatchError("PORTFOLIO_ACTIVATION_REQUIRED")
        if existing_target != target_version_id:
            raise PaperBatchError("PAPER_BATCH_CONFLICT")
        idempotency, idempotency_is_new = await _claim_idempotency(
            session,
            account_id=account_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )
        _observe(hooks, "idempotency_claimed")
        if idempotency_is_new:
            idempotency.response_payload = existing.model_dump(mode="json")
            await session.flush()
        return existing

    decision_record = await session.get(StrategyDecisionRecord, strategy_decision_id)
    if decision_record is None or decision_record.account_id != account_id:
        raise PaperBatchError("STRATEGY_DECISION_INVALID")
    newer_decision_id = await session.scalar(
        select(StrategyDecisionRecord.id)
        .where(
            StrategyDecisionRecord.account_id == account_id,
            StrategyDecisionRecord.decision_session > decision_record.decision_session,
        )
        .limit(1)
    )
    if newer_decision_id is not None:
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    portfolio = decision_record.strategy_version == "agent_portfolio_proposal_v1"
    if portfolio and not portfolio_activation:
        raise PaperBatchError("PORTFOLIO_ACTIVATION_REQUIRED")
    decision: PaperDecisionAuthority
    try:
        if portfolio:
            if target_version_id is None:
                raise ValueError("portfolio batch requires target")
            proposal = await load_portfolio_proposal(
                session,
                account_id=account_id,
                decision_id=strategy_decision_id,
                candidate_stage_capacity=candidate_stage_capacity,
                formal_stage_capacity=formal_stage_capacity,
            )
            if proposal is None:
                raise ValueError("portfolio proposal missing")
            target = await session.scalar(
                select(PaperTargetVersionRecord).where(
                    PaperTargetVersionRecord.id == target_version_id,
                    PaperTargetVersionRecord.account_id == account_id,
                )
            )
            current_target_id = await session.scalar(
                select(PaperTargetVersionRecord.id)
                .where(PaperTargetVersionRecord.account_id == account_id)
                .order_by(PaperTargetVersionRecord.version_seq.desc())
                .limit(1)
            )
            proposal_target_hash = canonical_portfolio_target_content_hash(proposal.proposal)
            if (
                target is None
                or target.id != current_target_id
                or target.target_content_hash != proposal_target_hash
            ):
                raise ValueError("portfolio target mismatch")
            rows = tuple(
                await session.scalars(
                    select(StrategyDecisionTargetRecord)
                    .where(
                        StrategyDecisionTargetRecord.strategy_decision_id == strategy_decision_id
                    )
                    .order_by(StrategyDecisionTargetRecord.instrument_id)
                )
            )
            if (
                decision_record.execution_source_policy_id is None
                or decision_record.agent_paper_cycle_id is None
            ):
                raise ValueError("portfolio execution authority missing")
            decision = _PortfolioDecisionAuthority(
                cycle_id=decision_record.agent_paper_cycle_id,
                account_id=decision_record.account_id,
                portfolio_id=decision_record.portfolio_id,
                decision_session=decision_record.decision_session,
                decision_time=decision_record.decision_time,
                execution_session=decision_record.execution_session,
                source_policy_id=decision_record.source_policy_id,
                execution_source_policy_id=decision_record.execution_source_policy_id,
                calendar_authority_id=decision_record.calendar_authority_id,
                universe_snapshot_id=decision_record.universe_snapshot_id,
                quality_report_id=decision_record.quality_report_id,
                portfolio_projection_hash=decision_record.portfolio_projection_hash,
                authority_hash=decision_record.authority_hash,
                decision_hash=decision_record.decision_hash,
                cash_weight=decision_record.cash_weight,
                targets=tuple(
                    ForwardStrategyTarget(
                        instrument_id=row.instrument_id,
                        target_weight=row.target_weight,
                        exit=row.exit,
                        strategy_priority=row.strategy_priority,
                        reason=row.reason,
                    )
                    for row in rows
                ),
            )
        else:
            if target_version_id is not None:
                raise ValueError("legacy batch cannot bind target")
            persisted = await load_paper_decision(
                session,
                account_id=account_id,
                decision_id=strategy_decision_id,
            )
            if persisted is None or persisted.decision_id != strategy_decision_id:
                raise ValueError("paper decision missing")
            decision = cast(PaperDecisionAuthority, persisted.decision)
    except ValueError as error:
        raise PaperBatchError("STRATEGY_DECISION_INVALID") from error
    if request_time < decision.decision_time or account.portfolio_id != decision.portfolio_id:
        raise PaperBatchError("STRATEGY_DECISION_INVALID")
    try:
        if isinstance(decision, AgentPaperDecision) or portfolio:
            await require_agent_source_policies(session, decision)  # type: ignore[arg-type]
            if isinstance(decision, AgentPaperDecision):
                await require_agent_decision_bindings(session, decision)
        else:
            if decision.source_policy_id != SOURCE_POLICY_V2_ID:
                raise ValueError("registered source policy is invalid")
            source_policy = await load_source_policy_v2(
                session,
                instrument_tick_size=None,
                decision_time=decision.decision_time,
            )
            if source_policy.record_id != decision.source_policy_id:
                raise ValueError("registered source policy is invalid")
            if not current_paper_order_price_authority_available():
                raise ValueError("registered order price authority is unavailable")
    except ValueError as error:
        raise PaperBatchError("STRATEGY_DECISION_INVALID") from error
    idempotency, idempotency_is_new = await _claim_idempotency(
        session,
        account_id=account_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    _observe(hooks, "idempotency_claimed")
    if not idempotency_is_new:
        raise PaperBatchError("PAPER_BATCH_CONFLICT")
    try:
        projection = await load_paper_projection(
            session,
            account_id=account_id,
            as_of=request_time if portfolio else decision.decision_time,
        )
    except ValueError as error:
        raise PaperBatchError("PAPER_PROJECTION_INVALID") from error
    required = {
        *(target.instrument_id for target in decision.targets),
        *(position.instrument_id for position in projection.positions),
        *(order.instrument_id for order in projection.open_orders),
    }
    try:
        if (
            isinstance(decision, AgentPaperDecision) or portfolio
        ) and decision.execution_source_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID:
            cycle_id = (
                decision.cycle_id
                if isinstance(decision, AgentPaperDecision | _PortfolioDecisionAuthority)
                else None
            )
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            if cycle is None:
                raise ValueError("Schwab Agent cycle is missing")
            market = await prepare_schwab_realtime_risk_snapshot(
                session,
                cycle=cycle,
                required_instrument_ids=tuple(sorted(required, key=lambda value: value.int)),
                max_instruments=len(required),
            )
            if set(market.required_instrument_ids) != required:
                raise ValueError("Schwab Agent risk scope is incomplete")
        else:
            market = await load_risk_market_snapshot(
                session,
                decision=decision,
                required_instrument_ids=tuple(sorted(required, key=lambda value: value.int)),
            )
        if (
            portfolio
            and len(market.required_instrument_ids) > 1
            and any(proof.required_instrument_ids is None for proof in market.query_proofs)
        ):
            raise ValueError("portfolio risk proofs are ambiguously unbound")
    except ValueError as error:
        raise PaperBatchError("RISK_AUTHORITY_INCOMPLETE") from error
    if portfolio:
        assert isinstance(decision, _PortfolioDecisionAuthority)
        decision = decision.model_copy(
            update={
                "portfolio_projection_hash": projection.projection_hash,
                "authority_hash": market.authority_hash,
            }
        )
    policy_id, policy = await _load_policy(session)
    _observe(hooks, "inputs_loaded")
    try:
        candidates = derive_order_candidates(decision, projection, market)
        if portfolio:
            candidates = tuple(filter(meets_portfolio_minimum_notional, candidates))
        if portfolio and target_version_id is not None:
            from nexus.paper.target_service import load_risk_remainder_coverage

            remainder_coverage = await load_risk_remainder_coverage(
                session,
                account_id=account_id,
                target_version_id=target_version_id,
            )
            candidates = _subtract_portfolio_risk_remainders(
                candidates,
                {key: value.quantity for key, value in remainder_coverage.items()},
            )
            candidates = tuple(filter(meets_portfolio_minimum_notional, candidates))
        if portfolio and not candidates:
            await session.delete(idempotency)
            await session.flush()
            return None
        no_changes = _derive_no_change_evidence(decision, projection, market)
        evaluations = evaluate_order_batch(candidates, projection, market, policy)
    except RiskBatchRejected as error:
        raise _map_error(error.reason_code) from error
    risk_input_hash = canonical_risk_batch_input_hash(
        candidates=candidates,
        portfolio=projection,
        market=market,
        policy_version=policy.version,
        policy_hash=policy.content_hash,
    )
    result_hash = canonical_risk_evaluation_result_hash(evaluations)
    batch_hash = canonical_paper_order_batch_hash(
        account_id=account_id,
        decision_hash=decision.decision_hash,
        risk_input_hash=risk_input_hash,
        evaluations=evaluations,
    )
    _observe(hooks, "risk_evaluated")
    return await _persist_new_batch(
        session,
        account_id=account_id,
        strategy_decision_id=strategy_decision_id,
        target_version_id=target_version_id,
        decision_hash=decision.decision_hash,
        policy_id=policy_id,
        idempotency_record=idempotency,
        request_hash=request_hash,
        request_time=request_time,
        projection_hash=projection.projection_hash,
        risk_input_hash=risk_input_hash,
        result_hash=result_hash,
        batch_hash=batch_hash,
        decision_session=decision.decision_session,
        market=market,
        portfolio=projection,
        no_changes=no_changes,
        evaluations=evaluations,
        ledger_accounts=ledger_accounts,
        idempotency_key=idempotency_key,
        hooks=hooks,
    )
