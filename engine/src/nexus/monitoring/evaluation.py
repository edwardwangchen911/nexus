"""Pure evaluation and durable episode transition rules."""

from collections.abc import Mapping
from datetime import datetime, timedelta
from decimal import Decimal, localcontext
from typing import Literal
from uuid import UUID

from pydantic import Field

from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.monitoring.contracts import (
    MonitoringModel,
    PriceMonitoringProposal,
    PriceMonitoringReview,
    PriceObservation,
    PriceReviewCondition,
)

Result = Literal["true", "false", "unknown"]
Cause = Literal["initial_match", "crossed"]


class PriceEvaluation(MonitoringModel):
    result: Result
    reason: (
        Literal["observation_missing", "authority_mismatch", "time_unproven", "price_missing"]
        | None
    )


class EpisodeState(MonitoringModel):
    sequence: int = Field(default=0, ge=0)
    latched: bool = False
    observed_false: bool = False


def evaluate_price(
    condition: PriceReviewCondition,
    observation: PriceObservation | None,
    *,
    instrument_id: UUID,
    now: datetime,
) -> PriceEvaluation:
    now = require_utc(now)
    if observation is None:
        return PriceEvaluation(result="unknown", reason="observation_missing")
    if (
        observation.instrument_id != instrument_id
        or observation.source != condition.source
        or observation.feed != condition.feed
        or observation.currency != condition.currency
        or observation.price_basis != condition.price_basis
        or observation.delivery_mode != "real_time"
        or observation.evidence_status != "verified"
    ):
        return PriceEvaluation(result="unknown", reason="authority_mismatch")
    if not (
        observation.observed_at <= observation.available_at <= observation.received_at <= now
        and now - observation.observed_at <= timedelta(seconds=condition.max_age_seconds)
    ):
        return PriceEvaluation(result="unknown", reason="time_unproven")
    bid, ask = observation.bid, observation.ask
    if bid is None or ask is None or not (Decimal(0) < bid <= ask):
        return PriceEvaluation(result="unknown", reason="price_missing")
    with localcontext() as context:
        context.prec = 50
        midpoint = (bid + ask) / Decimal(2)
    threshold = Decimal(condition.threshold)
    matched = midpoint >= threshold if condition.operator == "gte" else midpoint <= threshold
    return PriceEvaluation(result="true" if matched else "false", reason=None)


def advance_episode(state: EpisodeState, result: Result) -> tuple[EpisodeState, Cause | None]:
    if result == "unknown":
        return state, None
    if result == "false":
        return state.model_copy(update={"latched": False, "observed_false": True}), None
    if state.latched:
        return state, None
    return (
        state.model_copy(update={"sequence": state.sequence + 1, "latched": True}),
        "crossed" if state.observed_false else "initial_match",
    )


def compile_proposal(
    proposal: PriceMonitoringProposal,
    reviews: Mapping[str, PriceMonitoringReview],
    *,
    now: datetime,
    evidence_refs: tuple[EvidenceRef, ...],
) -> PriceMonitoringProposal:
    """Caller supplies original, persisted role outputs; synthesis cannot self-approve."""
    now = require_utc(now)
    if (
        set(reviews) != {"bear", "evidence_audit"}
        or not now < proposal.review_due_at <= proposal.valid_until
        or not proposal.rationale.strip()
        or any(ref not in evidence_refs or ref.available_at > now for ref in proposal.evidence_refs)
        or any(
            review.decision != "accepted"
            or review.proposal_hash != proposal.content_hash
            or not review.rationale.strip()
            or any(
                ref not in evidence_refs or ref.available_at > now for ref in review.evidence_refs
            )
            for review in reviews.values()
        )
    ):
        raise ValueError("MONITOR_PROPOSAL_NOT_APPROVED")
    return proposal
