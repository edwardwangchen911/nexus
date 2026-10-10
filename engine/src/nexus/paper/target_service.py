"""One explicit immutable initial-baseline approval; no execution side effects."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from nexus.paper.service import PaperOrderBatchResult

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_candidate import require_candidate_consumer_profile
from nexus.agents.portfolio_synthesis_contracts import canonical_portfolio_synthesis_hash
from nexus.audit.canonical import Hash64
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.paper.portfolio_policy import (
    PORTFOLIO_ALLOCATION_POLICY_HASH,
    PORTFOLIO_ALLOCATION_V1,
)
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    PaperInitialTargetApprovalRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperRequestIdempotencyRecord,
    PaperTargetVersionRecord,
    RiskEvaluationRecord,
)
from nexus.persistence.models.portfolio import Account
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.risk.canonical import canonical_paper_projection_hash
from nexus.risk.models import PaperPortfolioProjection

_APPROVAL_ROUTE = "paper-initial-target-approval-v1"
_ACTIVATION_ROUTE = "paper-target-activation-v1"


class InitialTargetApprovalResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    approval_id: UUID
    account_id: UUID
    portfolio_id: UUID
    projection_as_of: datetime
    projection_hash: Hash64
    approved_by: str
    approved_at: datetime
    approval_hash: Hash64


class TargetActivationResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    account_id: UUID
    strategy_decision_id: UUID
    target_version_id: UUID
    target_created: bool
    batch_id: UUID | None
    result_code: str
    activated_at: datetime


class TargetActivationBlockedError(ValueError):
    """Activation stopped by the exact persisted plan occupying the account."""

    def __init__(self, *, blocking_plan_id: UUID) -> None:
        super().__init__("INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN")
        self.blocking_plan_id = blocking_plan_id


class RiskRemainderCoverage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    quantity: Decimal


def requires_portfolio_plan(
    gaps: tuple[Decimal, ...],
    *,
    target_created: bool,
) -> bool:
    """Apply the target-change threshold before the same-target drift threshold."""
    threshold = (
        PORTFOLIO_ALLOCATION_V1.minimum_target_delta
        if target_created
        else PORTFOLIO_ALLOCATION_V1.rebalance_drift
    )
    return any(gap >= threshold for gap in gaps)


async def load_risk_remainder_coverage(
    session: AsyncSession,
    *,
    account_id: UUID,
    target_version_id: UUID,
    reconsider_cash: bool = False,
) -> dict[tuple[UUID, str], RiskRemainderCoverage]:
    """Return only still-unapproved same-side quantities for one target."""
    rows = tuple(
        (
            await session.execute(
                select(PaperOrderRecord, RiskEvaluationRecord)
                .join(
                    RiskEvaluationRecord,
                    RiskEvaluationRecord.order_id == PaperOrderRecord.id,
                )
                .join(
                    PaperOrderBatchRecord,
                    PaperOrderBatchRecord.id == PaperOrderRecord.batch_id,
                )
                .where(
                    PaperOrderBatchRecord.account_id == account_id,
                    PaperOrderBatchRecord.target_version_id == target_version_id,
                    RiskEvaluationRecord.outcome.in_(("reduced", "rejected")),
                )
                .order_by(RiskEvaluationRecord.created_at, RiskEvaluationRecord.id)
            )
        ).tuples()
    )
    quantities: dict[tuple[UUID, str], Decimal] = {}
    for order, evaluation in rows:
        if reconsider_cash and order.side == "buy":
            from nexus.persistence.models.paper import RiskEvaluationRuleRecord

            cash_failure = await session.scalar(
                select(RiskEvaluationRuleRecord.id).where(
                    RiskEvaluationRuleRecord.risk_evaluation_id == evaluation.id,
                    RiskEvaluationRuleRecord.rule_code == "CASH_FLOOR",
                    RiskEvaluationRuleRecord.passed.is_(False),
                )
            )
            if cash_failure is not None:
                # The original rejection stays immutable. This explicit continuation
                # runs every original risk rule against actual post-sale projection.
                continue
        remainder = max(
            Decimal(0),
            evaluation.desired_quantity - evaluation.approved_quantity,
        )
        if remainder > 0:
            key = (order.instrument_id, order.side)
            quantities[key] = quantities.get(key, Decimal(0)) + remainder
    return {key: RiskRemainderCoverage(quantity=quantity) for key, quantity in quantities.items()}


def canonical_target_activation_hash(
    *,
    account_id: UUID,
    portfolio_id: UUID,
    version_seq: int,
    strategy_decision_id: UUID,
    previous_target_version_id: UUID | None,
    target_content_hash: str,
    activated_at: datetime,
) -> str:
    return canonical_portfolio_synthesis_hash(
        {
            "schema_version": "paper_target_activation_v1",
            "account_id": account_id,
            "portfolio_id": portfolio_id,
            "version_seq": version_seq,
            "strategy_decision_id": strategy_decision_id,
            "previous_target_version_id": previous_target_version_id,
            "target_content_hash": target_content_hash,
            "activated_at": require_utc(activated_at),
        }
    )


async def lock_portfolio_account(session: AsyncSession, account_id: UUID) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1818))"),
        {"scope": f"internal-paper:{account_id}"},
    )


def require_unoccupied_projection(projection: PaperPortfolioProjection) -> None:
    if (
        projection.open_orders
        or projection.reserved_cash != 0
        or any(position.reserved_quantity != 0 for position in projection.positions)
    ):
        raise ValueError("INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN")
    if canonical_paper_projection_hash(projection) != projection.projection_hash:
        raise ValueError("INITIAL_PROJECTION_HASH_INVALID")


async def require_no_unknown_cancel_requests(
    session: AsyncSession,
    *,
    account_id: UUID,
    as_of: datetime,
) -> None:
    """Keep the account occupied until every persisted cancellation is proven."""
    # Import locally because read_service also imports target-service helpers.
    # This keeps activation and approval aligned with the public cancel-result
    # authority instead of maintaining a weaker second validator here.
    from nexus.paper.read_service import PaperReadService

    claims = tuple(
        (
            await session.scalars(
                select(PaperRequestIdempotencyRecord).where(
                    PaperRequestIdempotencyRecord.account_id == account_id,
                    PaperRequestIdempotencyRecord.operation == "cancel",
                    PaperRequestIdempotencyRecord.created_at <= as_of,
                )
            )
        ).all()
    )
    reader = PaperReadService(session, as_of)
    for claim in claims:
        result = await reader.cancel_result(claim.order_id, claim.idempotency_key)
        if result is None or result.status != "COMMITTED":
            raise ValueError("INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN")


async def require_activation_projection_unoccupied(
    session: AsyncSession,
    *,
    account_id: UUID,
    projection: PaperPortfolioProjection,
) -> None:
    """Fail activation with an exact occupying plan when the projection proves one."""
    occupied = bool(
        projection.open_orders
        or projection.reserved_cash != 0
        or any(position.reserved_quantity != 0 for position in projection.positions)
    )
    if not occupied:
        await require_no_unknown_cancel_requests(
            session,
            account_id=account_id,
            as_of=projection.as_of,
        )
        require_unoccupied_projection(projection)
        return
    order_ids = tuple(order.order_id for order in projection.open_orders)
    rows = (
        ()
        if not order_ids
        else tuple(
            (
                await session.execute(
                    select(PaperOrderRecord.id, PaperOrderRecord.batch_id).where(
                        PaperOrderRecord.account_id == account_id,
                        PaperOrderRecord.id.in_(order_ids),
                    )
                )
            ).all()
        )
    )
    batch_ids = {batch_id for _, batch_id in rows}
    if len(rows) != len(order_ids) or len(batch_ids) != 1:
        raise ValueError("INITIAL_PROJECTION_OCCUPANCY_AUTHORITY_CONFLICT")
    raise TargetActivationBlockedError(blocking_plan_id=next(iter(batch_ids)))


def initial_approval_result(
    record: PaperInitialTargetApprovalRecord,
) -> InitialTargetApprovalResult:
    return InitialTargetApprovalResult(
        approval_id=record.id,
        account_id=record.account_id,
        portfolio_id=record.portfolio_id,
        projection_as_of=record.projection_as_of,
        projection_hash=record.projection_hash,
        approved_by=record.approved_by,
        approved_at=record.approved_at,
        approval_hash=record.approval_hash,
    )


def initial_approval_hash(result: InitialTargetApprovalResult) -> str:
    return canonical_portfolio_synthesis_hash(
        {
            "schema_version": "paper_initial_target_approval_v1",
            **result.model_dump(mode="json", exclude={"approval_hash", "approval_id"}),
            "allocation_policy_version": "portfolio_allocation_v1",
            "allocation_policy_hash": PORTFOLIO_ALLOCATION_POLICY_HASH,
        }
    )


async def load_initial_approval_projection(
    session: AsyncSession,
    *,
    approval: PaperInitialTargetApprovalRecord,
    portfolio_id: UUID,
    decision_time: datetime,
    now: datetime | None,
) -> PaperPortfolioProjection:
    """The one-time baseline keeps its own timestamp, independent of later research."""
    if (
        approval.portfolio_id != portfolio_id
        or approval.projection_as_of > decision_time
        or approval.projection_as_of > approval.approved_at
        or approval.allocation_policy_hash != PORTFOLIO_ALLOCATION_POLICY_HASH
        or initial_approval_hash(initial_approval_result(approval)) != approval.approval_hash
        or now is not None
        and max(approval.approved_at, approval.created_at) > now
    ):
        raise ValueError("PORTFOLIO_PROPOSAL_INITIAL_APPROVAL_CONFLICT")
    projection = await load_paper_projection(
        session, account_id=approval.account_id, as_of=approval.projection_as_of
    )
    approved = await load_paper_projection(
        session, account_id=approval.account_id, as_of=approval.approved_at
    )
    require_unoccupied_projection(projection)
    require_unoccupied_projection(approved)
    if (
        projection.portfolio_id != portfolio_id
        or projection.projection_hash != approval.projection_hash
        or projection.source_facts_hash != approved.source_facts_hash
        or projection.model_dump(exclude={"as_of", "projection_hash"})
        != approved.model_dump(exclude={"as_of", "projection_hash"})
    ):
        raise ValueError("PORTFOLIO_PROPOSAL_INITIAL_APPROVAL_CONFLICT")
    return projection


async def approve_initial_target_projection(
    session: AsyncSession,
    *,
    account_id: UUID,
    projection_as_of: datetime,
    expected_projection_hash: str,
    approved_by: str,
    approved_at: datetime,
    idempotency_key: str,
) -> InitialTargetApprovalResult:
    projection_as_of, approved_at = require_utc(projection_as_of), require_utc(approved_at)
    if not 1 <= len(idempotency_key) <= 255 or not approved_by.strip() or len(approved_by) > 100:
        raise ValueError("INITIAL_APPROVAL_INVALID")
    await lock_portfolio_account(session, account_id)
    request_hash = canonical_portfolio_synthesis_hash(
        {
            "account_id": account_id,
            "projection_as_of": projection_as_of,
            "projection_hash": expected_projection_hash,
            "approved_by": approved_by,
            "approved_at": approved_at,
            "policy_hash": PORTFOLIO_ALLOCATION_POLICY_HASH,
        }
    )
    claim = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == f"internal-paper:{account_id}",
            IdempotencyRecord.route == _APPROVAL_ROUTE,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
    )
    existing = await session.scalar(
        select(PaperInitialTargetApprovalRecord).where(
            PaperInitialTargetApprovalRecord.account_id == account_id
        )
    )
    if claim is not None:
        if claim.request_hash != request_hash or existing is None:
            raise ValueError("INITIAL_APPROVAL_IDEMPOTENCY_CONFLICT")
        result = initial_approval_result(existing)
        if (
            InitialTargetApprovalResult.model_validate(claim.response_payload) != result
            or initial_approval_hash(result) != existing.approval_hash
        ):
            raise ValueError("INITIAL_APPROVAL_IDEMPOTENCY_CONFLICT")
        return result
    if existing is not None:
        raise ValueError("INITIAL_APPROVAL_ALREADY_EXISTS_CONFLICT")
    account = await session.get(PaperAccountRecord, account_id, populate_existing=True)
    underlying = (
        None
        if account is None
        else await session.get(Account, account.account_id, populate_existing=True)
    )
    if (
        account is None
        or underlying is None
        or underlying.provider != "internal"
        or underlying.status != "ACTIVE"
        or account.capability != "simulation_write_internal"
        or account.currency != "USD"
        or not account.created_at <= projection_as_of <= approved_at
        or underlying.created_at > projection_as_of
    ):
        raise ValueError("INITIAL_APPROVAL_ACCOUNT_OR_TIME_INVALID")
    projection = await load_paper_projection(session, account_id=account_id, as_of=projection_as_of)
    current = await load_paper_projection(session, account_id=account_id, as_of=approved_at)
    await require_no_unknown_cancel_requests(session, account_id=account_id, as_of=projection_as_of)
    await require_no_unknown_cancel_requests(session, account_id=account_id, as_of=approved_at)
    require_unoccupied_projection(projection)
    require_unoccupied_projection(current)
    if (
        projection.projection_hash != expected_projection_hash
        or projection.portfolio_id != account.portfolio_id
        or projection.source_facts_hash != current.source_facts_hash
        or projection.model_dump(exclude={"as_of", "projection_hash"})
        != current.model_dump(exclude={"as_of", "projection_hash"})
    ):
        raise ValueError("INITIAL_PROJECTION_STALE_OR_HASH_CONFLICT")
    result = InitialTargetApprovalResult(
        approval_id=new_uuid7(),
        account_id=account_id,
        portfolio_id=account.portfolio_id,
        projection_as_of=projection_as_of,
        projection_hash=projection.projection_hash,
        approved_by=approved_by,
        approved_at=approved_at,
        approval_hash="0" * 64,
    )
    result = result.model_copy(update={"approval_hash": initial_approval_hash(result)})
    session.add(
        PaperInitialTargetApprovalRecord(
            id=result.approval_id,
            account_id=account_id,
            portfolio_id=account.portfolio_id,
            projection_as_of=projection_as_of,
            projection_hash=projection.projection_hash,
            allocation_policy_version="portfolio_allocation_v1",
            allocation_policy_hash=PORTFOLIO_ALLOCATION_POLICY_HASH,
            approved_by=approved_by,
            approved_at=approved_at,
            approval_hash=result.approval_hash,
            created_at=approved_at,
        )
    )
    session.add(
        IdempotencyRecord(
            actor_id=f"internal-paper:{account_id}",
            route=_APPROVAL_ROUTE,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            schema_version="initial_target_approval_v1",
            response_payload=result.model_dump(mode="json"),
            created_at=approved_at,
        )
    )
    await session.flush()
    return result


async def activate_portfolio_target(
    session: AsyncSession,
    *,
    account_id: UUID,
    strategy_decision_id: UUID,
    request_time: datetime,
    idempotency_key: str,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> TargetActivationResult:
    """Activate one immutable proposal, leaving execution locked and fail closed."""
    from nexus.paper.agent_decision_store import load_portfolio_proposal
    from nexus.paper.portfolio_proposal import canonical_portfolio_target_content_hash

    if type(formal_stage_capacity) is not bool or (
        formal_stage_capacity and candidate_stage_capacity is not False
    ):
        raise ValueError("CODEX_FORMAL_CYCLE_PROFILE_INVALID")
    await require_candidate_consumer_profile(
        session, candidate_stage_capacity=candidate_stage_capacity
    )
    request_time = require_utc(request_time)
    if not 1 <= len(idempotency_key) <= 255:
        raise ValueError("TARGET_ACTIVATION_IDEMPOTENCY_INVALID")
    await lock_portfolio_account(session, account_id)
    persisted = await load_portfolio_proposal(
        session,
        account_id=account_id,
        decision_id=strategy_decision_id,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )
    if persisted is None:
        raise ValueError("PORTFOLIO_PROPOSAL_NOT_FOUND")

    request_hash = canonical_portfolio_synthesis_hash(
        {
            "schema_version": "paper_target_activation_request_v1",
            "account_id": account_id,
            "strategy_decision_id": strategy_decision_id,
            "request_time": request_time,
        }
    )
    claim = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == f"internal-paper:{account_id}",
            IdempotencyRecord.route == _ACTIVATION_ROUTE,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
    )
    if claim is not None:
        if claim.request_hash != request_hash or not claim.response_payload:
            raise ValueError("TARGET_ACTIVATION_IDEMPOTENCY_CONFLICT")
        return TargetActivationResult.model_validate(claim.response_payload)

    historical_claim = await session.scalar(
        select(IdempotencyRecord)
        .where(
            IdempotencyRecord.actor_id == f"internal-paper:{account_id}",
            IdempotencyRecord.route == _ACTIVATION_ROUTE,
            IdempotencyRecord.response_payload["strategy_decision_id"].as_string()
            == str(strategy_decision_id),
        )
        .order_by(IdempotencyRecord.created_at, IdempotencyRecord.id)
        .limit(1)
    )
    if historical_claim is not None:
        historical = TargetActivationResult.model_validate(historical_claim.response_payload)
        if (
            historical.account_id != account_id
            or historical.strategy_decision_id != strategy_decision_id
        ):
            raise ValueError("TARGET_ACTIVATION_IDEMPOTENCY_CONFLICT")
        session.add(
            IdempotencyRecord(
                actor_id=f"internal-paper:{account_id}",
                route=_ACTIVATION_ROUTE,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                schema_version="target_activation_result_v1",
                response_payload=historical.model_dump(mode="json"),
                created_at=request_time,
            )
        )
        await session.flush()
        return historical

    activated = await session.scalar(
        select(PaperTargetVersionRecord).where(
            PaperTargetVersionRecord.account_id == account_id,
            PaperTargetVersionRecord.strategy_decision_id == strategy_decision_id,
        )
    )
    if activated is not None:
        batch_id = await session.scalar(
            select(PaperOrderBatchRecord.id).where(
                PaperOrderBatchRecord.account_id == account_id,
                PaperOrderBatchRecord.strategy_decision_id == strategy_decision_id,
                PaperOrderBatchRecord.target_version_id == activated.id,
                PaperOrderBatchRecord.previous_batch_id.is_(None),
            )
        )
        historical = TargetActivationResult(
            account_id=account_id,
            strategy_decision_id=strategy_decision_id,
            target_version_id=activated.id,
            target_created=True,
            batch_id=batch_id,
            result_code="PLAN_CREATED" if batch_id is not None else "NO_ORDER_REQUIRED",
            activated_at=activated.activated_at,
        )
        session.add(
            IdempotencyRecord(
                actor_id=f"internal-paper:{account_id}",
                route=_ACTIVATION_ROUTE,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                schema_version="target_activation_result_v1",
                response_payload=historical.model_dump(mode="json"),
                created_at=request_time,
            )
        )
        await session.flush()
        return historical

    proposal = persisted.proposal
    if request_time >= proposal.expires_at:
        raise ValueError("PORTFOLIO_PROPOSAL_EXPIRED")
    if proposal.policy_hash != PORTFOLIO_ALLOCATION_POLICY_HASH:
        raise ValueError("POLICY_UNCONFIRMED")

    current = await session.scalar(
        select(PaperTargetVersionRecord)
        .where(PaperTargetVersionRecord.account_id == account_id)
        .order_by(PaperTargetVersionRecord.version_seq.desc())
        .limit(1)
    )
    if proposal.baseline.target_version_id != (None if current is None else current.id):
        raise ValueError("PORTFOLIO_PROPOSAL_BASELINE_CONFLICT")
    if current is None:
        approval = (
            None
            if proposal.baseline.initial_approval_id is None
            else await session.get(
                PaperInitialTargetApprovalRecord,
                proposal.baseline.initial_approval_id,
                populate_existing=True,
            )
        )
        if approval is None or approval.account_id != account_id:
            raise ValueError("POLICY_UNCONFIRMED")

    projection = await load_paper_projection(session, account_id=account_id, as_of=request_time)
    await require_activation_projection_unoccupied(
        session,
        account_id=account_id,
        projection=projection,
    )
    content_hash = canonical_portfolio_target_content_hash(proposal)
    created = current is None or current.target_content_hash != content_hash
    if created:
        sequence = 1 if current is None else current.version_seq + 1
        target = PaperTargetVersionRecord(
            id=new_uuid7(),
            account_id=account_id,
            portfolio_id=projection.portfolio_id,
            version_seq=sequence,
            strategy_decision_id=strategy_decision_id,
            previous_target_version_id=None if current is None else current.id,
            target_content_hash=content_hash,
            activated_at=request_time,
            activation_hash=canonical_target_activation_hash(
                account_id=account_id,
                portfolio_id=projection.portfolio_id,
                version_seq=sequence,
                strategy_decision_id=strategy_decision_id,
                previous_target_version_id=None if current is None else current.id,
                target_content_hash=content_hash,
                activated_at=request_time,
            ),
            created_at=request_time,
        )
        session.add(target)
        await session.flush()
    else:
        assert current is not None
        target = current

    actual_weights = {
        position.instrument_id: position.quantity * position.reference_close / projection.nav
        for position in projection.positions
    }
    gaps: list[Decimal] = []
    for instrument_id in set(proposal.weights) | set(actual_weights):
        delta = proposal.weights.get(instrument_id, Decimal(0)) - actual_weights.get(
            instrument_id, Decimal(0)
        )
        if delta == 0:
            continue
        gaps.append(abs(delta))
    needs_plan = requires_portfolio_plan(tuple(gaps), target_created=created)
    batch_id = None
    if needs_plan:
        from nexus.paper.service import _create_target_aware_paper_order_batch

        batch = await _create_target_aware_paper_order_batch(
            session,
            account_id=account_id,
            strategy_decision_id=strategy_decision_id,
            target_version_id=target.id,
            idempotency_key=idempotency_key,
            request_time=request_time,
            candidate_stage_capacity=candidate_stage_capacity,
            formal_stage_capacity=formal_stage_capacity,
        )
        batch_id = None if batch is None else batch.batch_id
    result = TargetActivationResult(
        account_id=account_id,
        strategy_decision_id=strategy_decision_id,
        target_version_id=target.id,
        target_created=created,
        batch_id=batch_id,
        result_code="PLAN_CREATED" if batch_id is not None else "NO_ORDER_REQUIRED",
        activated_at=target.activated_at,
    )
    session.add(
        IdempotencyRecord(
            actor_id=f"internal-paper:{account_id}",
            route=_ACTIVATION_ROUTE,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            schema_version="target_activation_result_v1",
            response_payload=result.model_dump(mode="json"),
            created_at=request_time,
        )
    )
    await session.flush()
    return result


async def require_plan_continuation_parent(
    session: AsyncSession, *, account_id: UUID, previous_batch_id: UUID, now: datetime
) -> PaperOrderBatchRecord:
    """No expected proceeds: every approved parent order must have real complete fills."""
    from sqlalchemy import func

    from nexus.persistence.models.paper import (
        PaperFillRecord,
        PaperOrderEventRecord,
    )

    parent = await session.get(PaperOrderBatchRecord, previous_batch_id)
    if parent is None or parent.account_id != account_id or parent.target_version_id is None:
        raise ValueError("PORTFOLIO_PLAN_NOT_READY")
    current_target = await session.scalar(
        select(PaperTargetVersionRecord.id)
        .where(PaperTargetVersionRecord.account_id == account_id)
        .order_by(PaperTargetVersionRecord.version_seq.desc())
        .limit(1)
    )
    if current_target != parent.target_version_id or now < parent.evaluated_at:
        raise ValueError("PORTFOLIO_PLAN_NOT_READY")
    orders = tuple(
        await session.scalars(
            select(PaperOrderRecord).where(PaperOrderRecord.batch_id == parent.id)
        )
    )
    has_sale = False
    for order in orders:
        event = await session.scalar(
            select(PaperOrderEventRecord)
            .where(PaperOrderEventRecord.order_id == order.id)
            .order_by(PaperOrderEventRecord.event_seq.desc())
            .limit(1)
        )
        if event is None or max(event.occurred_at, event.created_at) > now:
            raise ValueError("PORTFOLIO_PLAN_NOT_READY")
        if order.risk_outcome == "rejected":
            if event.to_status != "risk_rejected":
                raise ValueError("PORTFOLIO_PLAN_NOT_READY")
            continue
        fill = (
            await session.get(PaperFillRecord, event.source_fill_id)
            if event.source_fill_id
            else None
        )
        total = await session.scalar(
            select(func.sum(PaperFillRecord.quantity)).where(PaperFillRecord.order_id == order.id)
        )
        if (
            event.to_status != "filled"
            or fill is None
            or fill.order_id != order.id
            or fill.account_id != account_id
            or total != order.quantity
            or max(fill.evaluated_at, fill.created_at) > now
        ):
            raise ValueError("PORTFOLIO_PLAN_NOT_READY")
        has_sale |= order.side == "sell"
    if not orders or not has_sale:
        raise ValueError("PORTFOLIO_PLAN_NOT_READY")
    projection = await load_paper_projection(session, account_id=account_id, as_of=now)
    require_unoccupied_projection(projection)
    await require_no_unknown_cancel_requests(session, account_id=account_id, as_of=now)
    return parent


async def continue_portfolio_plan(
    session: AsyncSession,
    *,
    account_id: UUID,
    previous_batch_id: UUID,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> PaperOrderBatchResult | None:
    """Normal continuation; DB clock, exact parent key, own cash, unchanged deterministic risk."""
    from sqlalchemy import func

    from nexus.paper.agent_decision_store import load_portfolio_proposal
    from nexus.paper.service import (
        _create_target_aware_paper_order_batch,
        load_paper_order_batch_result,
    )

    await lock_portfolio_account(session, account_id)
    parent = await session.get(PaperOrderBatchRecord, previous_batch_id)
    if parent is None or parent.account_id != account_id or parent.target_version_id is None:
        raise ValueError("PORTFOLIO_PLAN_NOT_READY")
    child = await session.scalar(
        select(PaperOrderBatchRecord.id).where(
            PaperOrderBatchRecord.previous_batch_id == previous_batch_id
        )
    )
    if child is not None:
        return await load_paper_order_batch_result(
            session,
            account_id=account_id,
            strategy_decision_id=parent.strategy_decision_id,
            batch_id=child,
            candidate_stage_capacity=candidate_stage_capacity,
            formal_stage_capacity=formal_stage_capacity,
        )
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("PORTFOLIO_PLAN_NOT_READY")
    await require_plan_continuation_parent(
        session, account_id=account_id, previous_batch_id=previous_batch_id, now=now
    )
    proposal = await load_portfolio_proposal(
        session,
        account_id=account_id,
        decision_id=parent.strategy_decision_id,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )
    if proposal is None or now >= proposal.proposal.expires_at:
        raise ValueError("PORTFOLIO_PROPOSAL_EXPIRED")
    return await _create_target_aware_paper_order_batch(
        session,
        account_id=account_id,
        strategy_decision_id=parent.strategy_decision_id,
        target_version_id=parent.target_version_id,
        previous_batch_id=parent.id,
        request_time=now,
        idempotency_key=f"portfolio-plan-continue:{parent.id}",
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
    )
