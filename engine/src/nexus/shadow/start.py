"""Atomic, idempotent startup of static three-way Proposal Shadows."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.optimizer.repository import has_executable_shadow_authority
from nexus.persistence.models.optimization import CostPolicyRecord
from nexus.persistence.models.runtime import OutboxEvent
from nexus.persistence.models.shadow import ShadowGroupRecord, ShadowPortfolioRecord
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.shadow.canonical import (
    proposal_bundle_content_hash,
    target_weights_content_hash,
)
from nexus.shadow.cost_policy import cost_model_from_policy
from nexus.shadow.models import (
    INITIAL_NAV,
    ProposalVariantHashInput,
    ShadowVariant,
)
from nexus.shadow.repository import (
    load_frozen_calendar,
    load_proposals_and_positions,
    lock_proposal_group,
    target_weight_for,
)
from nexus.simulation.models import TargetWeight

_SCHEMA_VERSION = "proposal_shadow_start_v1"
_NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class ShadowStartResult:
    proposal_group_id: UUID
    shadow_group_id: UUID
    status: Literal["shadowing"]
    version: int
    start_session: date
    evaluation_end_session: date
    grace_end_session: date


async def start_proposal_shadow(
    session: AsyncSession,
    *,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    group_id: UUID,
    expected_version: int,
) -> ShadowStartResult:
    """Create one complete Shadow graph and lifecycle CAS in the caller transaction."""
    claim, owner = await claim_or_lock(
        session,
        actor_id,
        f"optimization-proposal-shadow:{group_id}",
        idempotency_key,
        request_hash,
        _SCHEMA_VERSION,
    )
    if not owner:
        if claim.request_hash != request_hash:
            raise IdempotencyConflict(
                "idempotency key Shadow request conflicts with prior request"
            )
        return _replayed_start(claim.response_payload, group_id)

    group = await lock_proposal_group(session, group_id)
    if group is None or group.version != expected_version:
        raise ValueError("VERSION_CONFLICT")
    if group.status != "compared":
        raise ValueError("PROPOSAL_STATE_INVALID")

    started_at = await session.scalar(select(func.transaction_timestamp()))
    if not isinstance(started_at, datetime):
        raise RuntimeError("database transaction time is unavailable")
    started_at = started_at.astimezone(UTC)
    if not await has_executable_shadow_authority(session, group):
        raise ValueError("PROPOSAL_NOT_EXECUTABLE")

    cost_policy = await session.get(CostPolicyRecord, group.cost_policy_id)
    if cost_policy is None:
        raise ValueError("PROPOSAL_NOT_EXECUTABLE")
    cost_model_from_policy(cost_policy, started_at=started_at)

    proposals, positions = await load_proposals_and_positions(session, group.id)
    hash_inputs: list[ProposalVariantHashInput] = []
    target_hashes: dict[ShadowVariant, str] = {}
    for proposal in proposals:
        variant = cast(ShadowVariant, proposal.variant)
        targets = tuple(
            TargetWeight(
                instrument_id=position.instrument_id,
                weight=target_weight_for(variant, position),
            )
            for position in positions[proposal.id]
        )
        cash_weight = Decimal(1) - sum(
            (target.weight for target in targets), start=Decimal(0)
        )
        hash_input = ProposalVariantHashInput(
            proposal_id=proposal.id,
            variant=variant,
            targets=targets,
            cash_weight=cash_weight,
        )
        hash_inputs.append(hash_input)
        target_hashes[variant] = target_weights_content_hash(targets, cash_weight)
    proposal_bundle_hash = proposal_bundle_content_hash(tuple(hash_inputs))  # type: ignore[arg-type]

    market_date = started_at.astimezone(_NEW_YORK).date()
    calendar, sessions = await load_frozen_calendar(
        session,
        market_date=market_date,
        started_at=started_at,
    )
    shadow_group_id = new_uuid7()
    shadow_group = ShadowGroupRecord(
        id=shadow_group_id,
        proposal_group_id=group.id,
        baseline_portfolio_snapshot_id=group.baseline_portfolio_snapshot_id,
        proposal_bundle_hash=proposal_bundle_hash,
        source_policy_id=group.source_policy_id,
        cost_policy_id=group.cost_policy_id,
        calendar_authority_id=calendar.id,
        calendar_version=calendar.version,
        calendar_hash=calendar.content_hash,
        signal_session=group.valuation_time.astimezone(_NEW_YORK).date(),
        started_at=started_at,
        start_session=sessions[0],
        evaluation_end_session=sessions[62],
        grace_end_session=sessions[67],
        evaluation_sessions=list(sessions[:63]),
        grace_sessions=list(sessions[63:]),
        initial_nav=INITIAL_NAV,
        schema_version="proposal_shadow_v1",
    )
    session.add(shadow_group)
    for proposal in proposals:
        variant = cast(ShadowVariant, proposal.variant)
        session.add(
            ShadowPortfolioRecord(
                id=new_uuid7(),
                shadow_group_id=shadow_group_id,
                proposal_group_id=group.id,
                proposal_id=proposal.id,
                variant=variant,
                target_weights_hash=target_hashes[variant],
                initial_nav=INITIAL_NAV,
                initial_cash=INITIAL_NAV,
            )
        )
    await session.flush()

    group.status = "shadowing"
    group.version = expected_version + 1
    await session.flush()

    response_payload = {
        "proposal_group_id": str(group.id),
        "shadow_group_id": str(shadow_group_id),
        "status": "shadowing",
        "version": group.version,
        "start_session": sessions[0].isoformat(),
        "evaluation_end_session": sessions[62].isoformat(),
        "grace_end_session": sessions[67].isoformat(),
    }
    claim.response_payload = response_payload
    event_payload = {
        "proposal_group_id": str(group.id),
        "shadow_group_id": str(shadow_group_id),
        "version": group.version,
        "start_session": sessions[0].isoformat(),
        "evaluation_end_session": sessions[62].isoformat(),
        "grace_end_session": sessions[67].isoformat(),
    }
    await append_audit_event(
        session,
        actor_id=actor_id,
        action="shadow.started",
        schema_version="1",
        payload=event_payload,
        occurred_at=started_at,
    )
    session.add(
        OutboxEvent(
            event_type="shadow.started",
            aggregate_id=shadow_group_id,
            schema_version="1",
            payload=event_payload,
            published_at=None,
        )
    )
    await session.flush()
    return ShadowStartResult(
        proposal_group_id=group.id,
        shadow_group_id=shadow_group_id,
        status="shadowing",
        version=group.version,
        start_session=sessions[0],
        evaluation_end_session=sessions[62],
        grace_end_session=sessions[67],
    )


def _replayed_start(payload: dict[str, object], group_id: UUID) -> ShadowStartResult:
    try:
        proposal_group_id = UUID(cast(str, payload["proposal_group_id"]))
        shadow_group_id = UUID(cast(str, payload["shadow_group_id"]))
        status = payload["status"]
        version = payload["version"]
        start_session = date.fromisoformat(cast(str, payload["start_session"]))
        evaluation_end = date.fromisoformat(
            cast(str, payload["evaluation_end_session"])
        )
        grace_end = date.fromisoformat(cast(str, payload["grace_end_session"]))
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("Shadow idempotency response is incomplete") from None
    if (
        proposal_group_id != group_id
        or status != "shadowing"
        or not isinstance(version, int)
    ):
        raise RuntimeError("Shadow idempotency response is incomplete")
    return ShadowStartResult(
        proposal_group_id=proposal_group_id,
        shadow_group_id=shadow_group_id,
        status="shadowing",
        version=version,
        start_session=start_session,
        evaluation_end_session=evaluation_end,
        grace_end_session=grace_end,
    )
