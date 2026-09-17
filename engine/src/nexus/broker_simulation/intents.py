"""Preparation and human approval services for broker-simulation intents."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Annotated, Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.broker_simulation.domain import (
    BrokerIntentDocument,
    BrokerOrderDocument,
    IntentStatus,
    canonical_document_hash,
    effective_approval_expiry,
    require_intent_transition,
)
from nexus.broker_simulation.risk import (
    BrokerRiskContext,
    BrokerRiskInstrument,
    BrokerRiskPosition,
    evaluate_broker_order_batch,
)
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.broker_simulation import (
    BrokerAccountBindingRecord,
    BrokerBatchApprovalRecord,
    BrokerExecutionIntentOrderRecord,
    BrokerExecutionIntentRecord,
)
from nexus.persistence.models.market import Instrument, LatestQuote
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperExecutionModelRecord,
    PaperFillRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    RiskEvaluationRecord,
    RiskPolicyRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.portfolio import (
    CashSnapshot,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class BrokerIntentError(ValueError):
    """A stable broker intent admission failure."""


class BrokerIntentAuthorityChanged(BrokerIntentError):
    """One or more independently loaded authority facts drifted."""


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BrokerIntentPreparation(_FrozenModel):
    proposal_group_id: UUID
    strategy_decision_id: UUID
    portfolio_snapshot_id: UUID
    evidence_hashes: tuple[Hash64, ...]
    risk_policy_hash: Hash64
    execution_model_hash: Hash64
    account_binding_id: UUID
    orders: tuple[BrokerOrderDocument, ...]
    risk_context: BrokerRiskContext
    prepared_at: datetime
    quote_expires_at: datetime
    session_close_at: datetime

    @field_validator("prepared_at", "quote_expires_at", "session_close_at")
    @classmethod
    def _times_are_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class BrokerApprovalAuthority(_FrozenModel):
    account_binding_id: UUID
    account_binding_version: int = Field(ge=1)
    account_snapshot_identity_hash: Hash64
    capability_subject: str = Field(min_length=1, max_length=255)
    evidence_hashes: tuple[Hash64, ...]
    risk_policy_hash: Hash64
    execution_model_hash: Hash64
    orders: tuple[BrokerOrderDocument, ...]


class AgentPaperIntentSourceOrder(_FrozenModel):
    paper_order_id: UUID
    instrument_id: UUID
    side: Literal["buy", "sell"]
    quantity: int = Field(gt=0)
    risk_evaluation_id: UUID
    risk_result_hash: Hash64
    fill_hash: Hash64


class AgentPaperIntentSource(_FrozenModel):
    cycle_id: UUID
    strategy_decision_id: UUID
    paper_order_batch_id: UUID
    execution_locked: Literal[True]
    risk_policy_hash: Hash64
    execution_model_hash: Hash64
    session_close_at: datetime
    evidence_hashes: tuple[Hash64, ...]
    orders: tuple[AgentPaperIntentSourceOrder, ...]

    @field_validator("session_close_at")
    @classmethod
    def _session_close_is_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


@dataclass(frozen=True)
class PreparedExecutionIntent:
    intent_id: UUID
    status: str
    version: int
    body_hash: str
    expires_at: datetime
    replayed: bool


@dataclass(frozen=True)
class ApprovedExecutionIntent:
    intent_id: UUID
    approval_id: UUID
    status: str
    version: int
    body_hash: str
    expires_at: datetime
    replayed: bool


async def load_agent_paper_intent_source(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    now: datetime,
) -> AgentPaperIntentSource:
    """Freeze a filled Agent Paper batch without creating broker-side facts."""
    now = require_utc(now)
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if cycle is None or not cycle.execution_locked:
        raise BrokerIntentError("AGENT_PAPER_SOURCE_NOT_READY")
    decision = await session.scalar(
        select(StrategyDecisionRecord).where(
            StrategyDecisionRecord.agent_paper_cycle_id == cycle.id,
            StrategyDecisionRecord.decision_origin == "agent_research",
        )
    )
    if decision is None:
        raise BrokerIntentError("AGENT_PAPER_SOURCE_NOT_READY")
    batch = await session.scalar(
        select(PaperOrderBatchRecord).where(
            PaperOrderBatchRecord.account_id == cycle.account_id,
            PaperOrderBatchRecord.strategy_decision_id == decision.id,
        )
    )
    if batch is None:
        raise BrokerIntentError("AGENT_PAPER_SOURCE_NOT_READY")
    policy = await session.get(RiskPolicyRecord, batch.risk_policy_id)
    execution_model = await session.scalar(
        select(PaperExecutionModelRecord).where(
            PaperExecutionModelRecord.version == "execution_model_v1"
        )
    )
    if policy is None or execution_model is None:
        raise BrokerIntentError("AGENT_PAPER_SOURCE_NOT_READY")
    calendar_session = await session.get(
        ExchangeCalendarSessionRecord,
        (decision.calendar_authority_id, decision.execution_session),
    )
    if calendar_session is None or calendar_session.market_close_at is None:
        raise BrokerIntentError("AGENT_PAPER_SOURCE_NOT_READY")
    paper_orders = tuple(
        await session.scalars(
            select(PaperOrderRecord)
            .where(PaperOrderRecord.batch_id == batch.id)
            .order_by(PaperOrderRecord.instrument_id)
        )
    )
    if not paper_orders:
        raise BrokerIntentError("AGENT_PAPER_SOURCE_NOT_READY")

    source_orders: list[AgentPaperIntentSourceOrder] = []
    evidence_hashes = {
        cycle.candidate_hash,
        decision.portfolio_projection_hash,
        decision.authority_hash,
        decision.strategy_parameters_hash,
        decision.decision_hash,
        batch.request_hash,
        batch.projection_hash,
        batch.risk_input_hash,
        batch.result_hash,
        batch.batch_hash,
        policy.content_hash,
        execution_model.content_hash,
    }
    for order in paper_orders:
        risk = await session.scalar(
            select(RiskEvaluationRecord).where(RiskEvaluationRecord.order_id == order.id)
        )
        latest_event = await session.scalar(
            select(PaperOrderEventRecord)
            .where(
                PaperOrderEventRecord.order_id == order.id,
                PaperOrderEventRecord.occurred_at <= now,
                PaperOrderEventRecord.created_at <= now,
            )
            .order_by(PaperOrderEventRecord.event_seq.desc())
            .limit(1)
        )
        if (
            risk is None
            or risk.outcome not in {"approved", "reduced"}
            or risk.approved_quantity != order.quantity
            or risk.worst_price != order.worst_price
            or order.risk_outcome != risk.outcome
            or latest_event is None
            or latest_event.to_status != "filled"
            or latest_event.source_fill_id is None
        ):
            raise BrokerIntentError("AGENT_PAPER_SOURCE_NOT_READY")
        fill = await session.get(PaperFillRecord, latest_event.source_fill_id)
        if (
            fill is None
            or fill.order_id != order.id
            or fill.account_id != cycle.account_id
            or fill.quantity != order.quantity
        ):
            raise BrokerIntentError("AGENT_PAPER_SOURCE_NOT_READY")
        evidence_hashes.update((risk.input_hash, risk.result_hash, fill.payload_hash))
        source_orders.append(
            AgentPaperIntentSourceOrder(
                paper_order_id=order.id,
                instrument_id=order.instrument_id,
                side=cast(Literal["buy", "sell"], order.side),
                quantity=int(order.quantity),
                risk_evaluation_id=risk.id,
                risk_result_hash=risk.result_hash,
                fill_hash=fill.payload_hash,
            )
        )
    return AgentPaperIntentSource(
        cycle_id=cycle.id,
        strategy_decision_id=decision.id,
        paper_order_batch_id=batch.id,
        execution_locked=True,
        risk_policy_hash=policy.content_hash,
        execution_model_hash=execution_model.content_hash,
        session_close_at=calendar_session.market_close_at,
        evidence_hashes=tuple(sorted(evidence_hashes)),
        orders=tuple(source_orders),
    )


def canonical_broker_account_snapshot_identity(
    snapshot: PortfolioSnapshotRecord,
    cash_rows: tuple[CashSnapshot, ...],
) -> str:
    return canonical_document_hash(
        {
            "schema": "broker_account_snapshot_identity_v1",
            "portfolio_snapshot_id": snapshot.id,
            "portfolio_id": snapshot.portfolio_id,
            "as_of": snapshot.as_of,
            "position_hash": snapshot.position_hash,
            "order_hash": snapshot.order_hash,
            "cash": tuple(
                {
                    "currency": row.currency,
                    "cash_balance": row.cash_balance,
                    "available_cash": row.available_cash,
                    "provider_as_of": row.provider_as_of,
                }
                for row in sorted(cash_rows, key=lambda item: item.currency)
            ),
        }
    )


async def prepare_agent_paper_execution_intent(
    session: AsyncSession,
    *,
    source: AgentPaperIntentSource,
    account_binding_id: UUID,
    portfolio_snapshot_id: UUID,
    now: datetime,
) -> PreparedExecutionIntent:
    """Re-risk one frozen Agent Paper batch and persist it as not-sent only."""
    now = require_utc(now)
    binding = await session.get(BrokerAccountBindingRecord, account_binding_id)
    snapshot = await session.get(PortfolioSnapshotRecord, portfolio_snapshot_id)
    if (
        binding is None
        or binding.locked
        or binding.reconciliation_status != "matched"
        or binding.provider != "fosun"
        or binding.environment != "simulation"
        or binding.account_type != "simulation"
        or snapshot is None
        or snapshot.status != "READY"
        or snapshot.as_of is None
        or snapshot.as_of > now
        or snapshot.as_of <= now - timedelta(minutes=15)
        or source.session_close_at <= now
    ):
        raise BrokerIntentError("ACCOUNT_BINDING_NOT_READY")
    cash_rows = tuple(
        await session.scalars(
            select(CashSnapshot)
            .where(CashSnapshot.portfolio_snapshot_id == snapshot.id)
            .order_by(CashSnapshot.currency)
        )
    )
    if (
        not cash_rows
        or canonical_broker_account_snapshot_identity(snapshot, cash_rows)
        != binding.snapshot_identity_hash
    ):
        raise BrokerIntentError("ACCOUNT_SNAPSHOT_INCOMPLETE")
    usd_cash = tuple(row for row in cash_rows if row.currency == "USD")
    if len(usd_cash) != 1:
        raise BrokerIntentError("ACCOUNT_SNAPSHOT_INCOMPLETE")
    cash = usd_cash[0]
    positions = tuple(
        await session.scalars(
            select(PositionSnapshot)
            .where(PositionSnapshot.portfolio_snapshot_id == snapshot.id)
            .order_by(PositionSnapshot.instrument_id)
        )
    )
    instrument_ids = {item.instrument_id for item in positions} | {
        item.instrument_id for item in source.orders
    }
    instruments = {
        item.id: item
        for item in await session.scalars(
            select(Instrument).where(Instrument.id.in_(instrument_ids))
        )
    }
    if set(instruments) != instrument_ids:
        raise BrokerIntentError("INSTRUMENT_AUTHORITY_UNAVAILABLE")
    positions_by_instrument = {item.instrument_id: item for item in positions}
    sector_notionals: dict[str, Decimal] = {}
    market_value = Decimal(0)
    for position in positions:
        instrument = instruments[position.instrument_id]
        if position.currency != "USD" or position.current_price is None:
            raise BrokerIntentError("ACCOUNT_SNAPSHOT_INCOMPLETE")
        notional = position.quantity * position.current_price
        market_value += notional
        sector_notionals[instrument.risk_bucket] = (
            sector_notionals.get(instrument.risk_bucket, Decimal(0)) + notional
        )

    orders: list[BrokerOrderDocument] = []
    quotes: list[LatestQuote] = []
    for ordinal, item in enumerate(source.orders, start=1):
        instrument = instruments[item.instrument_id]
        quote = await session.scalar(
            select(LatestQuote)
            .where(
                LatestQuote.instrument_id == item.instrument_id,
                LatestQuote.available_at <= now,
                LatestQuote.available_at > now - timedelta(minutes=15),
            )
            .order_by(LatestQuote.available_at.desc(), LatestQuote.id.desc())
            .limit(1)
        )
        price = None if quote is None else quote.ask if item.side == "buy" else quote.bid
        if (
            quote is None
            or price is None
            or quote.quality_flags
            or quote.evidence_status not in {"verified", "fixture"}
            or quote.delivery_mode not in {"real_time", "fixture"}
            or instrument.tick_size is None
            or price % instrument.tick_size != 0
        ):
            raise BrokerIntentError("PRICE_AUTHORITY_UNAVAILABLE")
        quotes.append(quote)
        orders.append(
            BrokerOrderDocument(
                instrument_id=item.instrument_id,
                side=item.side,
                quantity=item.quantity,
                limit_price=price,
                sort_key=ordinal,
            )
        )
    nav = cash.cash_balance + market_value
    risk_context = BrokerRiskContext(
        available_cash=cash.available_cash,
        nav=nav,
        drawdown_locked=False,
        minimum_cash_ratio=Decimal("0.10"),
        single_name_limit=Decimal("0.20"),
        sector_limit=Decimal("0.40"),
        positions=tuple(
            BrokerRiskPosition(
                instrument_id=position.instrument_id,
                quantity=position.quantity,
            )
            for position in positions
        ),
        instruments=tuple(
            BrokerRiskInstrument(
                instrument_id=item.instrument_id,
                asset_type=(
                    "ETF"
                    if instruments[item.instrument_id].asset_type.lower() == "etf"
                    else "common_stock"
                ),
                sector=instruments[item.instrument_id].risk_bucket,
                whitelisted=(
                    instruments[item.instrument_id].exchange in {"XNYS", "XNAS"}
                    and instruments[item.instrument_id].currency == "USD"
                    and instruments[item.instrument_id].asset_type
                    in {"equity", "common_stock", "etf"}
                ),
                existing_notional=(
                    Decimal(0)
                    if positions_by_instrument.get(item.instrument_id) is None
                    else positions_by_instrument[item.instrument_id].quantity
                    * cast(
                        Decimal,
                        positions_by_instrument[item.instrument_id].current_price,
                    )
                ),
            )
            for item in source.orders
        ),
        sector_notionals=sector_notionals,
    )
    evidence_hashes = tuple(
        sorted(
            {
                *source.evidence_hashes,
                binding.snapshot_identity_hash,
                snapshot.position_hash,
                snapshot.order_hash,
                *(quote.content_hash for quote in quotes),
            }
        )
    )
    return await prepare_execution_intent(
        session,
        BrokerIntentPreparation(
            proposal_group_id=source.cycle_id,
            strategy_decision_id=source.strategy_decision_id,
            portfolio_snapshot_id=snapshot.id,
            evidence_hashes=evidence_hashes,
            risk_policy_hash=canonical_document_hash(
                {
                    "policy": "broker_hard_risk_v1",
                    "minimum_cash_ratio": "0.10",
                    "single_name_limit": "0.20",
                    "sector_limit": "0.40",
                    "agent_paper_risk_policy_hash": source.risk_policy_hash,
                }
            ),
            execution_model_hash=canonical_document_hash(
                {
                    "model": "limit_day_regular_whole_share_v1",
                    "agent_paper_execution_model_hash": source.execution_model_hash,
                }
            ),
            account_binding_id=binding.id,
            orders=tuple(orders),
            risk_context=risk_context,
            prepared_at=now,
            quote_expires_at=min(
                quote.available_at + timedelta(minutes=15) for quote in quotes
            ),
            session_close_at=source.session_close_at,
        ),
    )


async def prepare_execution_intent(
    session: AsyncSession,
    authority: BrokerIntentPreparation,
) -> PreparedExecutionIntent:
    """Persist a server-owned batch only after deterministic risk and binding checks."""
    binding = await session.scalar(
        select(BrokerAccountBindingRecord)
        .where(BrokerAccountBindingRecord.id == authority.account_binding_id)
        .with_for_update()
    )
    if binding is None:
        raise BrokerIntentError("ACCOUNT_BINDING_NOT_FOUND")
    if (
        binding.locked
        or binding.reconciliation_status != "matched"
        or binding.provider != "fosun"
        or binding.environment != "simulation"
        or binding.account_type != "simulation"
    ):
        raise BrokerIntentError("ACCOUNT_BINDING_NOT_READY")
    if binding.snapshot_identity_hash not in authority.evidence_hashes:
        raise BrokerIntentError("BINDING_SNAPSHOT_NOT_EVIDENCED")
    expires_at = effective_approval_expiry(
        prepared_at=authority.prepared_at,
        quote_expires_at=authority.quote_expires_at,
        capability_expires_at=binding.capability_expires_at,
        session_close_at=authority.session_close_at,
    )
    if expires_at <= authority.prepared_at:
        raise BrokerIntentError("INTENT_AUTHORITY_EXPIRED")
    risk = evaluate_broker_order_batch(authority.orders, authority.risk_context)
    if not risk.passed:
        raise BrokerIntentError("RISK_REJECTED:" + ",".join(risk.reason_codes))
    total_notional = sum(
        (order.limit_price * order.quantity for order in authority.orders),
        start=authority.orders[0].limit_price * 0,
    )
    document = BrokerIntentDocument(
        proposal_group_id=authority.proposal_group_id,
        strategy_decision_id=authority.strategy_decision_id,
        portfolio_snapshot_id=authority.portfolio_snapshot_id,
        evidence_hashes=authority.evidence_hashes,
        risk_policy_hash=authority.risk_policy_hash,
        execution_model_hash=authority.execution_model_hash,
        account_binding_id=authority.account_binding_id,
        orders=authority.orders,
        total_notional=total_notional,
    )
    body_hash = canonical_document_hash(document)
    existing = await session.scalar(
        select(BrokerExecutionIntentRecord).where(
            BrokerExecutionIntentRecord.body_hash == body_hash
        )
    )
    if existing is not None:
        return PreparedExecutionIntent(
            intent_id=existing.id,
            status=existing.status,
            version=existing.version,
            body_hash=existing.body_hash,
            expires_at=existing.expires_at,
            replayed=True,
        )
    intent = BrokerExecutionIntentRecord(
        id=new_uuid7(),
        account_binding_id=authority.account_binding_id,
        proposal_group_id=authority.proposal_group_id,
        strategy_decision_id=authority.strategy_decision_id,
        portfolio_snapshot_id=authority.portfolio_snapshot_id,
        evidence_hashes=list(authority.evidence_hashes),
        risk_policy_hash=authority.risk_policy_hash,
        execution_model_hash=authority.execution_model_hash,
        status=IntentStatus.PREPARED.value,
        version=1,
        body_hash=body_hash,
        total_notional=total_notional,
        prepared_at=authority.prepared_at,
        expires_at=expires_at,
    )
    session.add(intent)
    await session.flush()
    for ordinal, order in enumerate(authority.orders, start=1):
        session.add(
            BrokerExecutionIntentOrderRecord(
                id=new_uuid7(),
                intent_id=intent.id,
                ordinal=ordinal,
                instrument_id=order.instrument_id,
                side=order.side,
                quantity=order.quantity,
                limit_price=order.limit_price,
                currency=order.currency,
                order_type=order.order_type,
                time_in_force=order.time_in_force,
                session=order.session,
                status="not_sent",
                order_hash=canonical_document_hash(order),
            )
        )
    await session.flush()
    return PreparedExecutionIntent(
        intent_id=intent.id,
        status=intent.status,
        version=intent.version,
        body_hash=intent.body_hash,
        expires_at=intent.expires_at,
        replayed=False,
    )


async def approve_execution_intent(
    session: AsyncSession,
    *,
    intent_id: UUID,
    actor_id: str,
    expected_version: int,
    if_match: str,
    request_body_hash: str,
    idempotency_key: str,
    approved_at: datetime,
    authority: BrokerApprovalAuthority,
) -> ApprovedExecutionIntent:
    """Approve one exact server batch; caller-provided order facts are not accepted."""
    approved_at = require_utc(approved_at)
    intent = await session.scalar(
        select(BrokerExecutionIntentRecord)
        .where(BrokerExecutionIntentRecord.id == intent_id)
        .with_for_update()
    )
    if intent is None:
        raise BrokerIntentError("INTENT_NOT_FOUND")
    existing = await session.scalar(
        select(BrokerBatchApprovalRecord).where(
            BrokerBatchApprovalRecord.intent_id == intent_id,
            BrokerBatchApprovalRecord.actor_id == actor_id,
            BrokerBatchApprovalRecord.idempotency_key == idempotency_key,
        )
    )
    if existing is not None:
        if (
            existing.request_body_hash != request_body_hash
            or existing.intent_body_hash != if_match
        ):
            raise BrokerIntentError("IDEMPOTENCY_CONFLICT")
        return _approved_result(intent, existing, replayed=True)
    if intent.status != IntentStatus.PREPARED.value:
        raise BrokerIntentError("INTENT_NOT_PREPARED")
    if intent.version != expected_version or intent.body_hash != if_match:
        raise BrokerIntentError("INTENT_VERSION_CONFLICT")
    if approved_at >= intent.expires_at:
        raise BrokerIntentError("INTENT_EXPIRED")
    binding = await session.scalar(
        select(BrokerAccountBindingRecord)
        .where(BrokerAccountBindingRecord.id == intent.account_binding_id)
        .with_for_update()
    )
    orders = tuple(
        _order_document(record)
        for record in await session.scalars(
            select(BrokerExecutionIntentOrderRecord)
            .where(BrokerExecutionIntentOrderRecord.intent_id == intent.id)
            .order_by(BrokerExecutionIntentOrderRecord.ordinal)
        )
    )
    _require_current_authority(intent, binding, orders, authority, approved_at)
    document = BrokerIntentDocument(
        proposal_group_id=intent.proposal_group_id,
        strategy_decision_id=intent.strategy_decision_id,
        portfolio_snapshot_id=intent.portfolio_snapshot_id,
        evidence_hashes=tuple(intent.evidence_hashes),
        risk_policy_hash=intent.risk_policy_hash,
        execution_model_hash=intent.execution_model_hash,
        account_binding_id=intent.account_binding_id,
        orders=orders,
        total_notional=intent.total_notional,
    )
    if canonical_document_hash(document) != intent.body_hash:
        raise BrokerIntentAuthorityChanged("INTENT_AUTHORITY_CHANGED")
    approval = BrokerBatchApprovalRecord(
        id=new_uuid7(),
        intent_id=intent.id,
        intent_version=intent.version,
        intent_body_hash=intent.body_hash,
        request_body_hash=request_body_hash,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        approved_at=approved_at,
        expires_at=intent.expires_at,
    )
    session.add(approval)
    require_intent_transition(IntentStatus.PREPARED, IntentStatus.APPROVED)
    intent.status = IntentStatus.APPROVED.value
    intent.version += 1
    await session.flush()
    return _approved_result(intent, approval, replayed=False)


async def reject_execution_intent(
    session: AsyncSession,
    *,
    intent_id: UUID,
    expected_version: int,
    reason: Literal["user_rejected", "authority_changed"],
) -> PreparedExecutionIntent:
    del reason
    intent = await session.scalar(
        select(BrokerExecutionIntentRecord)
        .where(BrokerExecutionIntentRecord.id == intent_id)
        .with_for_update()
    )
    if intent is None:
        raise BrokerIntentError("INTENT_NOT_FOUND")
    if intent.version != expected_version or intent.status not in {"draft", "prepared"}:
        raise BrokerIntentError("INTENT_VERSION_CONFLICT")
    require_intent_transition(IntentStatus(intent.status), IntentStatus.REJECTED)
    intent.status = IntentStatus.REJECTED.value
    intent.version += 1
    await session.flush()
    return PreparedExecutionIntent(
        intent_id=intent.id,
        status=intent.status,
        version=intent.version,
        body_hash=intent.body_hash,
        expires_at=intent.expires_at,
        replayed=False,
    )


async def expire_execution_intents(session: AsyncSession, *, now: datetime) -> int:
    now = require_utc(now)
    intents = tuple(
        await session.scalars(
            select(BrokerExecutionIntentRecord)
            .where(
                BrokerExecutionIntentRecord.status.in_(("prepared", "approved")),
                BrokerExecutionIntentRecord.expires_at <= now,
            )
            .with_for_update(skip_locked=True)
        )
    )
    for intent in intents:
        require_intent_transition(IntentStatus(intent.status), IntentStatus.EXPIRED)
        intent.status = IntentStatus.EXPIRED.value
        intent.version += 1
    await session.flush()
    return len(intents)


async def revalidate_approved_intent(
    session: AsyncSession,
    *,
    intent_id: UUID,
    authority: BrokerApprovalAuthority,
    at: datetime,
) -> tuple[
    BrokerExecutionIntentRecord,
    BrokerAccountBindingRecord,
    tuple[BrokerOrderDocument, ...],
]:
    """Reload and lock every persisted authority immediately before a Gateway attempt."""
    at = require_utc(at)
    intent = await session.scalar(
        select(BrokerExecutionIntentRecord)
        .where(BrokerExecutionIntentRecord.id == intent_id)
        .with_for_update()
    )
    if intent is None or intent.status not in {"approved", "dispatching"}:
        raise BrokerIntentError("INTENT_NOT_DISPATCHABLE")
    if at >= intent.expires_at:
        raise BrokerIntentError("INTENT_EXPIRED")
    binding = await session.scalar(
        select(BrokerAccountBindingRecord)
        .where(BrokerAccountBindingRecord.id == intent.account_binding_id)
        .with_for_update()
    )
    orders = tuple(
        _order_document(record)
        for record in await session.scalars(
            select(BrokerExecutionIntentOrderRecord)
            .where(BrokerExecutionIntentOrderRecord.intent_id == intent.id)
            .order_by(BrokerExecutionIntentOrderRecord.ordinal)
        )
    )
    _require_current_authority(intent, binding, orders, authority, at)
    if binding is None:
        raise BrokerIntentAuthorityChanged("INTENT_AUTHORITY_CHANGED")
    document = BrokerIntentDocument(
        proposal_group_id=intent.proposal_group_id,
        strategy_decision_id=intent.strategy_decision_id,
        portfolio_snapshot_id=intent.portfolio_snapshot_id,
        evidence_hashes=tuple(intent.evidence_hashes),
        risk_policy_hash=intent.risk_policy_hash,
        execution_model_hash=intent.execution_model_hash,
        account_binding_id=intent.account_binding_id,
        orders=orders,
        total_notional=intent.total_notional,
    )
    if canonical_document_hash(document) != intent.body_hash:
        raise BrokerIntentAuthorityChanged("INTENT_AUTHORITY_CHANGED")
    return intent, binding, orders


def _require_current_authority(
    intent: BrokerExecutionIntentRecord,
    binding: BrokerAccountBindingRecord | None,
    orders: tuple[BrokerOrderDocument, ...],
    authority: BrokerApprovalAuthority,
    approved_at: datetime,
) -> None:
    if (
        binding is None
        or binding.id != authority.account_binding_id
        or binding.id != intent.account_binding_id
        or binding.version != authority.account_binding_version
        or binding.snapshot_identity_hash != authority.account_snapshot_identity_hash
        or binding.snapshot_identity_hash not in intent.evidence_hashes
        or binding.capability_subject != authority.capability_subject
        or binding.capability_expires_at <= approved_at
        or binding.locked
        or binding.reconciliation_status != "matched"
        or tuple(intent.evidence_hashes) != authority.evidence_hashes
        or intent.risk_policy_hash != authority.risk_policy_hash
        or intent.execution_model_hash != authority.execution_model_hash
        or orders != authority.orders
    ):
        raise BrokerIntentAuthorityChanged("INTENT_AUTHORITY_CHANGED")


def _order_document(record: BrokerExecutionIntentOrderRecord) -> BrokerOrderDocument:
    return BrokerOrderDocument.model_validate(
        {
            "instrument_id": record.instrument_id,
            "side": record.side,
            "quantity": int(record.quantity),
            "limit_price": record.limit_price,
            "currency": record.currency,
            "order_type": record.order_type,
            "time_in_force": record.time_in_force,
            "session": record.session,
            "sort_key": record.ordinal,
        }
    )


def _approved_result(
    intent: BrokerExecutionIntentRecord,
    approval: BrokerBatchApprovalRecord,
    *,
    replayed: bool,
) -> ApprovedExecutionIntent:
    return ApprovedExecutionIntent(
        intent_id=intent.id,
        approval_id=approval.id,
        status=intent.status,
        version=intent.version,
        body_hash=intent.body_hash,
        expires_at=approval.expires_at,
        replayed=replayed,
    )
