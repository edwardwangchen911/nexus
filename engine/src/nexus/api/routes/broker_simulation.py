"""Authenticated, redacted API for the offline broker-simulation workbench."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.portfolios import _utc_now
from nexus.api.schemas.broker_simulation import (
    BrokerAccountResponse,
    BrokerActionEnvelope,
    BrokerActionResponse,
    BrokerAvailableActionsResponse,
    BrokerCancelRequest,
    BrokerCapabilityResponse,
    BrokerEventResponse,
    BrokerIntentApprovalRequest,
    BrokerIntentEnvelope,
    BrokerIntentPrepareRequest,
    BrokerIntentRejectRequest,
    BrokerIntentResponse,
    BrokerModifyRequest,
    BrokerOrderResponse,
    BrokerReconciliationAcknowledgeRequest,
    BrokerReconciliationResponse,
    BrokerSimulationEnvelope,
    BrokerSimulationResponse,
)
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.broker_simulation.domain import BrokerOrderDocument, canonical_document_hash
from nexus.broker_simulation.intents import (
    BrokerIntentError,
    BrokerIntentPreparation,
    approve_execution_intent,
    prepare_execution_intent,
    reject_execution_intent,
)
from nexus.broker_simulation.jobs import load_current_approval_authority
from nexus.broker_simulation.reconciliation import (
    ReconciliationError,
    acknowledge_reconciliation,
)
from nexus.broker_simulation.risk import (
    BrokerRiskContext,
    BrokerRiskInstrument,
    BrokerRiskPosition,
)
from nexus.domain.decimal import decimal_to_string
from nexus.jobs.worker import UnsafeJobError
from nexus.persistence.models.broker_simulation import (
    BrokerAccountBindingRecord,
    BrokerExecutionIntentOrderRecord,
    BrokerExecutionIntentRecord,
    BrokerOrderEventRecord,
    BrokerReconciliationDiffRecord,
    BrokerReconciliationRunRecord,
)
from nexus.persistence.models.market import Instrument, LatestQuote
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.models.portfolio import CashSnapshot, PositionSnapshot
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.quality.source_policy import current_paper_order_price_authority_available
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import (
    BrokerSyncPersistenceError,
    IdempotencyConflict,
)
from pydantic import BaseModel, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_BASE = "/internal/v1/broker-simulation"
_MAX_BODY_BYTES = 64 * 1024
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 404, 409, 422)
}
_IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]
_IfMatch = Annotated[str, Header(alias="If-Match", min_length=1, max_length=64)]


async def _bounded_json[Payload: BaseModel](request: Request, model: type[Payload]) -> Payload:
    content_type = request.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise _validation("Content-Type must be application/json.")
    raw_length = request.headers.get("Content-Length")
    if raw_length is not None and (
        not raw_length.isascii()
        or not raw_length.isdecimal()
        or (raw_length != "0" and raw_length.startswith("0"))
        or int(raw_length) > _MAX_BODY_BYTES
    ):
        raise _validation("Request body is invalid or too large.")
    body = bytearray()
    async for chunk in request.stream():
        if len(chunk) > _MAX_BODY_BYTES - len(body):
            raise _validation("Request body is too large.")
        body.extend(chunk)
    raw = bytes(body)
    setattr(request, "_body", raw)
    try:
        return model.model_validate_json(raw)
    except ValidationError as error:
        raise _validation("Request body is invalid.") from error


async def _prepare_body(request: Request) -> BrokerIntentPrepareRequest:
    return await _bounded_json(request, BrokerIntentPrepareRequest)


async def _approve_body(request: Request) -> BrokerIntentApprovalRequest:
    return await _bounded_json(request, BrokerIntentApprovalRequest)


async def _reject_body(request: Request) -> BrokerIntentRejectRequest:
    return await _bounded_json(request, BrokerIntentRejectRequest)


async def _modify_body(request: Request) -> BrokerModifyRequest:
    return await _bounded_json(request, BrokerModifyRequest)


async def _cancel_body(request: Request) -> BrokerCancelRequest:
    return await _bounded_json(request, BrokerCancelRequest)


async def _ack_body(request: Request) -> BrokerReconciliationAcknowledgeRequest:
    return await _bounded_json(request, BrokerReconciliationAcknowledgeRequest)


@router.get(
    _BASE,
    operation_id="getBrokerSimulation",
    response_model=BrokerSimulationEnvelope,
    responses=_ERROR_RESPONSES,
)
async def get_broker_simulation(
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BrokerSimulationEnvelope:
    now = _utc_now(request)
    return BrokerSimulationEnvelope(
        data=await _load_workbench(session, now=now),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


@router.post(
    f"{_BASE}/intents",
    operation_id="prepareBrokerSimulationIntent",
    response_model=BrokerIntentEnvelope,
    responses=_ERROR_RESPONSES,
)
async def prepare_broker_intent(
    request: Request,
    idempotency_key: _IdempotencyKey,
    payload: BrokerIntentPrepareRequest = Depends(_prepare_body),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BrokerIntentEnvelope:
    now = _utc_now(request)
    request_hash = hashlib.sha256(await request.body()).hexdigest()
    record, owner = await _claim_action(
        session,
        actor_id=actor.actor_id,
        route="broker-simulation:prepare",
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        schema_version="broker_sim_prepare_v1",
    )
    if not owner:
        replay_id = _replay_uuid(record, "intent_id")
        return BrokerIntentEnvelope(
            data=await _intent_response(session, replay_id),
            meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
        )
    try:
        authority = await _preparation_from_proposal(session, payload=payload, now=now)
        prepared = await prepare_execution_intent(session, authority)
    except BrokerIntentError as error:
        raise _domain_error(error) from error
    intent = await _intent_response(session, prepared.intent_id)
    record.response_payload = {"intent_id": str(intent.intent_id)}
    return BrokerIntentEnvelope(
        data=intent,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


@router.post(
    f"{_BASE}/intents/{{intent_id}}/approve",
    operation_id="approveBrokerSimulationIntent",
    response_model=BrokerActionEnvelope,
    responses=_ERROR_RESPONSES,
)
async def approve_broker_intent(
    intent_id: UUID,
    request: Request,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    payload: BrokerIntentApprovalRequest = Depends(_approve_body),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BrokerActionEnvelope:
    now = _utc_now(request)
    try:
        authority = await load_current_approval_authority(session, intent_id=intent_id)
        result = await approve_execution_intent(
            session,
            intent_id=intent_id,
            actor_id=actor.actor_id,
            expected_version=payload.version,
            if_match=if_match,
            request_body_hash=hashlib.sha256(await request.body()).hexdigest(),
            idempotency_key=idempotency_key,
            approved_at=now,
            authority=authority,
        )
    except (BrokerIntentError, UnsafeJobError) as error:
        raise _domain_error(error) from error
    return _action_envelope(
        actor,
        now,
        resource_id=result.intent_id,
        action="approve",
        status=result.status,
        version=result.version,
        replayed=result.replayed,
    )


@router.post(
    f"{_BASE}/intents/{{intent_id}}/reject",
    operation_id="rejectBrokerSimulationIntent",
    response_model=BrokerActionEnvelope,
    responses=_ERROR_RESPONSES,
)
async def reject_broker_intent(
    intent_id: UUID,
    request: Request,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    payload: BrokerIntentRejectRequest = Depends(_reject_body),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BrokerActionEnvelope:
    _require_version_etag(if_match, payload.version)
    now = _utc_now(request)
    request_hash = hashlib.sha256(await request.body()).hexdigest()
    record, owner = await _claim_action(
        session,
        actor_id=actor.actor_id,
        route=f"broker-simulation:reject:{intent_id}",
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        schema_version="broker_sim_reject_v1",
    )
    if not owner:
        return _replay_action(actor, now, record, action="reject")
    try:
        result = await reject_execution_intent(
            session,
            intent_id=intent_id,
            expected_version=payload.version,
            reason=payload.reason,
        )
    except BrokerIntentError as error:
        raise _domain_error(error) from error
    envelope = _action_envelope(
        actor,
        now,
        resource_id=result.intent_id,
        action="reject",
        status=result.status,
        version=result.version,
        replayed=False,
    )
    record.response_payload = envelope.data.model_dump(mode="json")
    return envelope


@router.post(
    f"{_BASE}/orders/{{order_id}}/modify",
    operation_id="modifyBrokerSimulationOrder",
    response_model=BrokerActionEnvelope,
    responses=_ERROR_RESPONSES,
)
async def modify_broker_order(
    order_id: UUID,
    request: Request,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    payload: BrokerModifyRequest = Depends(_modify_body),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BrokerActionEnvelope:
    del order_id, request, idempotency_key, if_match, payload, actor, session
    raise _simulation_write_disabled()


@router.post(
    f"{_BASE}/orders/{{order_id}}/cancel",
    operation_id="cancelBrokerSimulationOrder",
    response_model=BrokerActionEnvelope,
    responses=_ERROR_RESPONSES,
)
async def cancel_broker_order(
    order_id: UUID,
    request: Request,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    payload: BrokerCancelRequest = Depends(_cancel_body),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BrokerActionEnvelope:
    del order_id, request, idempotency_key, if_match, payload, actor, session
    raise _simulation_write_disabled()


@router.post(
    f"{_BASE}/reconciliation/{{run_id}}/acknowledge",
    operation_id="acknowledgeBrokerSimulationReconciliation",
    response_model=BrokerActionEnvelope,
    responses=_ERROR_RESPONSES,
)
async def acknowledge_broker_reconciliation(
    run_id: UUID,
    request: Request,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    payload: BrokerReconciliationAcknowledgeRequest = Depends(_ack_body),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BrokerActionEnvelope:
    del payload
    run = await session.get(BrokerReconciliationRunRecord, run_id)
    if run is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Reconciliation was not found.", status_code=404)
    if if_match != f'"{run.status}"':
        raise NexusError(
            ErrorCode.VERSION_CONFLICT,
            "Reconciliation authority changed.",
            status_code=409,
        )
    now = _utc_now(request)
    request_hash = hashlib.sha256(await request.body()).hexdigest()
    record, owner = await _claim_action(
        session,
        actor_id=actor.actor_id,
        route=f"broker-simulation:reconciliation:{run_id}:acknowledge",
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        schema_version="broker_sim_ack_v1",
    )
    if not owner:
        return _replay_action(actor, now, record, action="acknowledge")
    try:
        result = await acknowledge_reconciliation(
            session, run_id=run_id, actor_id=actor.actor_id, acknowledged_at=now
        )
    except ReconciliationError as error:
        raise _domain_error(error) from error
    envelope = _action_envelope(
        actor,
        now,
        resource_id=result.run_id,
        action="acknowledge",
        status=result.status,
        version=None,
        replayed=False,
    )
    record.response_payload = envelope.data.model_dump(mode="json")
    return envelope


async def _load_workbench(session: AsyncSession, *, now: datetime) -> BrokerSimulationResponse:
    binding = await session.scalar(
        select(BrokerAccountBindingRecord)
        .order_by(
            BrokerAccountBindingRecord.created_at.desc(), BrokerAccountBindingRecord.id.desc()
        )
        .limit(1)
    )
    intent = await session.scalar(
        select(BrokerExecutionIntentRecord)
        .order_by(
            BrokerExecutionIntentRecord.created_at.desc(), BrokerExecutionIntentRecord.id.desc()
        )
        .limit(1)
    )
    events: tuple[BrokerEventResponse, ...] = ()
    reconciliation = None
    if binding is not None:
        event_records = tuple(
            await session.scalars(
                select(BrokerOrderEventRecord)
                .where(BrokerOrderEventRecord.account_binding_id == binding.id)
                .order_by(BrokerOrderEventRecord.event_sequence.desc())
                .limit(200)
            )
        )
        events = tuple(
            BrokerEventResponse(
                sequence=event.event_sequence,
                order_id=event.intent_order_id,
                source=event.source,  # type: ignore[arg-type]
                status=event.order_status,
                occurred_at=event.occurred_at,
            )
            for event in reversed(event_records)
        )
        run = await session.scalar(
            select(BrokerReconciliationRunRecord)
            .where(BrokerReconciliationRunRecord.account_binding_id == binding.id)
            .order_by(
                BrokerReconciliationRunRecord.created_at.desc(),
                BrokerReconciliationRunRecord.id.desc(),
            )
            .limit(1)
        )
        if run is not None:
            diff_count = await session.scalar(
                select(func.count(BrokerReconciliationDiffRecord.id)).where(
                    BrokerReconciliationDiffRecord.run_id == run.id
                )
            )
            reconciliation = BrokerReconciliationResponse(
                run_id=run.id,
                status=run.status,  # type: ignore[arg-type]
                diff_count=int(diff_count or 0),
                started_at=run.started_at,
                completed_at=run.completed_at,
            )
    intent_response = None if intent is None else await _intent_response(session, intent.id)
    return BrokerSimulationResponse(
        capability=BrokerCapabilityResponse(
            status="offline_ready",
            external_activation="external_activation_blocked",
            live_trading=False,
            simulation_write_enabled=False,
        ),
        account=(
            None
            if binding is None
            else BrokerAccountResponse(
                binding_id=binding.id,
                redacted_label=binding.redacted_label,
                reconciliation_status=binding.reconciliation_status,  # type: ignore[arg-type]
                locked=binding.locked,
                capability_expires_at=binding.capability_expires_at,
            )
        ),
        intent=intent_response,
        events=events,
        reconciliation=reconciliation,
        actions=BrokerAvailableActionsResponse(
            can_prepare=binding is not None and not binding.locked,
            can_approve=(
                intent is not None and intent.status == "prepared" and intent.expires_at > now
            ),
            can_reject=intent is not None and intent.status in {"draft", "prepared"},
            can_dispatch=False,
            can_modify=False,
            can_cancel=False,
        ),
    )


async def _intent_response(session: AsyncSession, intent_id: UUID) -> BrokerIntentResponse:
    intent = await session.get(BrokerExecutionIntentRecord, intent_id)
    if intent is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Intent was not found.", status_code=404)
    orders = tuple(
        await session.scalars(
            select(BrokerExecutionIntentOrderRecord)
            .where(BrokerExecutionIntentOrderRecord.intent_id == intent.id)
            .order_by(BrokerExecutionIntentOrderRecord.ordinal)
        )
    )
    return BrokerIntentResponse(
        intent_id=intent.id,
        status=intent.status,  # type: ignore[arg-type]
        version=intent.version,
        body_hash=intent.body_hash,
        total_notional=decimal_to_string(intent.total_notional),
        prepared_at=intent.prepared_at,
        expires_at=intent.expires_at,
        evidence_hashes=tuple(intent.evidence_hashes),
        orders=tuple(
            BrokerOrderResponse(
                order_id=order.id,
                instrument_id=order.instrument_id,
                side=order.side,  # type: ignore[arg-type]
                quantity=decimal_to_string(order.quantity),
                limit_price=decimal_to_string(order.limit_price),
                currency=cast(Any, order.currency),
                order_type=cast(Any, order.order_type),
                time_in_force=cast(Any, order.time_in_force),
                session=cast(Any, order.session),
                status=order.status,  # type: ignore[arg-type]
            )
            for order in orders
        ),
    )


async def _preparation_from_proposal(
    session: AsyncSession, *, payload: BrokerIntentPrepareRequest, now: datetime
) -> BrokerIntentPreparation:
    if not current_paper_order_price_authority_available():
        raise BrokerIntentError("PRICE_AUTHORITY_UNAVAILABLE")
    group = await session.get(OptimizationProposalGroupRecord, payload.proposal_group_id)
    proposal = await session.get(OptimizationProposalRecord, payload.proposal_id)
    if (
        group is None
        or proposal is None
        or proposal.proposal_group_id != group.id
        or group.status != "validated"
        or group.portfolio_scope != "fosun_simulation"
        or proposal.portfolio_scope != "fosun_simulation"
        or proposal.analysis_only
        or not proposal.strategy_evidence_complete
        or group.partial
        or group.source_conflict
        or group.authority_stale
    ):
        raise BrokerIntentError("PROPOSAL_NOT_EXECUTABLE")
    comparison = await session.scalar(
        select(ProposalComparisonRecord).where(ProposalComparisonRecord.proposal_id == proposal.id)
    )
    if comparison is None or comparison.non_executable or not comparison.cost_complete:
        raise BrokerIntentError("PROPOSAL_NOT_EXECUTABLE")
    binding = await session.scalar(
        select(BrokerAccountBindingRecord)
        .where(
            BrokerAccountBindingRecord.locked.is_(False),
            BrokerAccountBindingRecord.reconciliation_status == "matched",
        )
        .order_by(BrokerAccountBindingRecord.created_at.desc())
        .limit(1)
    )
    if binding is None:
        raise BrokerIntentError("ACCOUNT_BINDING_NOT_READY")
    cash = await session.scalar(
        select(CashSnapshot).where(
            CashSnapshot.portfolio_snapshot_id == group.baseline_portfolio_snapshot_id,
            CashSnapshot.currency == "USD",
        )
    )
    if cash is None:
        raise BrokerIntentError("ACCOUNT_SNAPSHOT_INCOMPLETE")
    current_positions = tuple(
        await session.scalars(
            select(PositionSnapshot).where(
                PositionSnapshot.portfolio_snapshot_id == group.baseline_portfolio_snapshot_id
            )
        )
    )
    positions_by_instrument = {position.instrument_id: position for position in current_positions}
    proposal_positions = tuple(
        await session.scalars(
            select(ProposalPositionRecord)
            .where(ProposalPositionRecord.proposal_id == proposal.id)
            .order_by(ProposalPositionRecord.instrument_id)
        )
    )
    if not proposal_positions:
        raise BrokerIntentError("PROPOSAL_NOT_EXECUTABLE")
    instruments = {
        instrument.id: instrument
        for instrument in await session.scalars(
            select(Instrument).where(
                Instrument.id.in_(item.instrument_id for item in proposal_positions)
            )
        )
    }
    quote_map: dict[UUID, LatestQuote] = {}
    for item in proposal_positions:
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
        if quote is None or quote.last is None or quote.quality_flags:
            raise BrokerIntentError("PRICE_AUTHORITY_UNAVAILABLE")
        quote_map[item.instrument_id] = quote
    nav = cash.cash_balance + sum(
        (
            position.quantity * cast(Decimal, position.current_price)
            for position in current_positions
            if position.current_price is not None
        ),
        start=Decimal("0"),
    )
    orders: list[BrokerOrderDocument] = []
    for item in proposal_positions:
        quote = quote_map[item.instrument_id]
        instrument = instruments.get(item.instrument_id)
        if instrument is None or instrument.tick_size is None:
            raise BrokerIntentError("INSTRUMENT_AUTHORITY_UNAVAILABLE")
        current = positions_by_instrument.get(item.instrument_id)
        current_notional = (
            Decimal("0")
            if current is None
            else current.quantity * cast(Decimal, current.current_price or quote.last)
        )
        delta = nav * item.target_weight - current_notional
        price = cast(
            Decimal,
            quote.ask
            if delta > 0 and quote.ask is not None
            else quote.bid
            if delta < 0 and quote.bid is not None
            else quote.last,
        )
        quantity = int((abs(delta) / price).to_integral_value(rounding=ROUND_DOWN))
        if delta < 0 and current is not None:
            quantity = min(quantity, int(current.available_quantity))
        if quantity <= 0:
            continue
        limit_price = (price / instrument.tick_size).to_integral_value(
            rounding=ROUND_DOWN
        ) * instrument.tick_size
        orders.append(
            BrokerOrderDocument(
                instrument_id=item.instrument_id,
                side="buy" if delta > 0 else "sell",
                quantity=quantity,
                limit_price=limit_price,
                sort_key=len(orders) + 1,
            )
        )
    if not orders:
        raise BrokerIntentError("NO_EXECUTABLE_ORDERS")
    evidence_hashes = tuple(
        sorted(
            {
                binding.snapshot_identity_hash,
                group.diagnostic_input_hash,
                *(
                    canonical_document_hash(
                        {
                            "instrument_id": quote.instrument_id,
                            "source": quote.source,
                            "feed": quote.feed,
                            "available_at": quote.available_at,
                            "revision": quote.revision,
                        }
                    )
                    for quote in quote_map.values()
                ),
            }
        )
    )
    risk_instruments = tuple(
        BrokerRiskInstrument(
            instrument_id=order.instrument_id,
            asset_type=(
                "ETF"
                if instruments[order.instrument_id].asset_type.lower() == "etf"
                else "common_stock"
            ),
            sector=instruments[order.instrument_id].risk_bucket,
            whitelisted=(
                instruments[order.instrument_id].exchange in {"XNYS", "XNAS"}
                and instruments[order.instrument_id].currency == "USD"
                and instruments[order.instrument_id].asset_type in {"equity", "common_stock", "etf"}
            ),
            existing_notional=(
                Decimal("0")
                if positions_by_instrument.get(order.instrument_id) is None
                else positions_by_instrument[order.instrument_id].quantity
                * cast(
                    Decimal,
                    positions_by_instrument[order.instrument_id].current_price
                    or quote_map[order.instrument_id].last,
                )
            ),
        )
        for order in orders
    )
    risk_context = BrokerRiskContext(
        available_cash=cash.available_cash,
        nav=nav,
        drawdown_locked=False,
        minimum_cash_ratio=Decimal("0.10"),
        single_name_limit=Decimal("0.20"),
        sector_limit=Decimal("0.40"),
        positions=tuple(
            BrokerRiskPosition(instrument_id=position.instrument_id, quantity=position.quantity)
            for position in current_positions
        ),
        instruments=risk_instruments,
        sector_notionals={},
    )
    return BrokerIntentPreparation(
        proposal_group_id=group.id,
        strategy_decision_id=proposal.id,
        portfolio_snapshot_id=group.baseline_portfolio_snapshot_id,
        evidence_hashes=evidence_hashes,
        risk_policy_hash=canonical_document_hash(
            {
                "policy": "broker_hard_risk_v1",
                "min_cash": "0.10",
                "single": "0.20",
                "sector": "0.40",
            }
        ),
        execution_model_hash=canonical_document_hash({"model": "limit_day_regular_whole_share_v1"}),
        account_binding_id=binding.id,
        orders=tuple(orders),
        risk_context=risk_context,
        prepared_at=now,
        quote_expires_at=min(
            quote.available_at + timedelta(minutes=15) for quote in quote_map.values()
        ),
        session_close_at=now + timedelta(hours=6),
    )


def _action_envelope(
    actor: VerifiedServiceActor,
    now: datetime,
    *,
    resource_id: UUID,
    action: Literal["prepare", "approve", "reject", "acknowledge"],
    status: str,
    version: int | None,
    replayed: bool,
) -> BrokerActionEnvelope:
    return BrokerActionEnvelope(
        data=BrokerActionResponse(
            resource_id=resource_id,
            action=action,
            status=status,
            version=version,
            replayed=replayed,
            dispatch_created=False,
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


async def _claim_action(
    session: AsyncSession,
    *,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    schema_version: str,
) -> tuple[IdempotencyRecord, bool]:
    try:
        record, owner = await claim_or_lock(
            session,
            actor_id,
            route,
            idempotency_key,
            request_hash,
            schema_version,
        )
    except (IdempotencyConflict, BrokerSyncPersistenceError) as error:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency authority conflicts with this request.",
            status_code=409,
        ) from error
    if not owner and (
        record.request_hash != request_hash or record.schema_version != schema_version
    ):
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency authority conflicts with this request.",
            status_code=409,
        )
    return record, owner


def _replay_uuid(record: IdempotencyRecord, key: str) -> UUID:
    value = record.response_payload.get(key)
    if not isinstance(value, str):
        raise NexusError(
            ErrorCode.AUTHORITY_UNAVAILABLE,
            "Idempotency response authority is unavailable.",
            status_code=409,
        )
    try:
        return UUID(value)
    except ValueError as error:
        raise NexusError(
            ErrorCode.AUTHORITY_UNAVAILABLE,
            "Idempotency response authority is unavailable.",
            status_code=409,
        ) from error


def _replay_action(
    actor: VerifiedServiceActor,
    now: datetime,
    record: IdempotencyRecord,
    *,
    action: Literal["reject", "acknowledge"],
) -> BrokerActionEnvelope:
    try:
        data = BrokerActionResponse.model_validate(record.response_payload)
    except ValidationError as error:
        raise NexusError(
            ErrorCode.AUTHORITY_UNAVAILABLE,
            "Idempotency response authority is unavailable.",
            status_code=409,
        ) from error
    if data.action != action:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency authority conflicts with this request.",
            status_code=409,
        )
    return BrokerActionEnvelope(
        data=data.model_copy(update={"replayed": True}),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


def _require_version_etag(if_match: str, version: int) -> None:
    if if_match != f'"{version}"':
        raise NexusError(ErrorCode.VERSION_CONFLICT, "Resource version changed.", status_code=409)


def _validation(message: str) -> NexusError:
    return NexusError(ErrorCode.VALIDATION_ERROR, message, status_code=422)


def _simulation_write_disabled() -> NexusError:
    return NexusError(
        ErrorCode.SIMULATION_WRITE_DISABLED,
        "External simulation writes are disabled.",
        status_code=409,
    )


def _domain_error(error: Exception) -> NexusError:
    reason = str(error)
    if reason.endswith("NOT_FOUND"):
        return NexusError(ErrorCode.NOT_FOUND, "Broker resource was not found.", status_code=404)
    if "VERSION" in reason or "IDEMPOTENCY" in reason or "AUTHORITY_CHANGED" in reason:
        code = (
            ErrorCode.IDEMPOTENCY_CONFLICT
            if "IDEMPOTENCY" in reason
            else ErrorCode.VERSION_CONFLICT
        )
        return NexusError(code, "Broker authority changed.", status_code=409)
    if "RISK" in reason:
        return NexusError(
            ErrorCode.RISK_REJECTED, "Deterministic risk rejected the intent.", status_code=409
        )
    return NexusError(
        ErrorCode.PROPOSAL_NOT_EXECUTABLE,
        "Broker simulation action is not executable.",
        status_code=409,
    )
