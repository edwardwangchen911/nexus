# ruff: noqa: E501
"""Strict research intent contract; it deliberately has no order or weight fields."""

from collections.abc import Mapping
from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    StrictBool,
    field_validator,
    model_serializer,
    model_validator,
)

from nexus.domain.decimal import DecimalString
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc

_UNSAFE_TEXT = (
    "http://",
    "https://",
    "file://",
    "../",
    "\\",
    ";",
    "|",
    "&&",
    "`",
    "select ",
    "insert ",
    "update ",
    "delete ",
    "import ",
    "exec(",
    "eval(",
    "curl ",
    "wget ",
    "bash ",
    "sh ",
)
_FORBIDDEN_FIELD_TOKENS = (
    "weight",
    "quantity",
    "amount",
    "order",
    "code",
    "url",
    "sql",
    "shell",
    "path",
)


class _IntentModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _safe_unstructured_content(cls, value: object) -> object:
        _assert_safe(value)
        return value


def _assert_safe(value: object) -> None:
    if isinstance(value, float):
        raise TypeError("research intents must not contain binary floats")
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower().replace("_", "")
            if any(token in normalized for token in _FORBIDDEN_FIELD_TOKENS):
                raise ValueError("research intent contains a forbidden executable or numeric field")
            _assert_safe(nested)
    elif isinstance(value, list | tuple | set | frozenset):
        for nested in value:
            _assert_safe(nested)
    elif isinstance(value, str) and any(token in value.lower() for token in _UNSAFE_TEXT):
        raise ValueError("research intent contains unsafe text")


class ResearchClaim(_IntentModel):
    text: str = Field(min_length=1, max_length=1000)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1)


class SynthesisClaimReview(_IntentModel):
    claim_id: str = Field(pattern=r"^(bear|evidence_audit):[0-9]+$")
    status: Literal["accepted", "rejected", "unresolved"]
    rationale: str = Field(min_length=1, max_length=500)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=3)
    resolved: StrictBool
    missing_evidence: tuple[str, ...] = Field(max_length=4)
    prevented_conclusion: str | None = Field(max_length=500)

    @model_validator(mode="after")
    def _resolution_is_explicit(self) -> "SynthesisClaimReview":
        if not self.rationale.strip():
            raise ValueError("review rationale is required")
        if self.resolved:
            if (
                self.status == "unresolved"
                or self.missing_evidence
                or self.prevented_conclusion is not None
            ):
                raise ValueError("resolved review contradicts unresolved evidence")
        elif (
            not self.missing_evidence
            or any(not value.strip() for value in self.missing_evidence)
            or not self.prevented_conclusion
            or not self.prevented_conclusion.strip()
        ):
            raise ValueError("unresolved review requires missing evidence and its consequence")
        return self


class SynthesisAssessment(_IntentModel):
    stance: Literal["buy_candidate", "reduce_candidate", "hold", "watch"]
    claims: tuple[ResearchClaim, ...] = Field(min_length=1, max_length=2)
    reviews: tuple[SynthesisClaimReview, ...] = Field(min_length=1, max_length=4)
    review_conclusion: Literal["sufficient", "limited", "insufficient"]
    uncertainties: tuple[str, ...] = Field(max_length=5)

    @model_validator(mode="after")
    def _consistent_review(self) -> "SynthesisAssessment":
        ids = tuple(review.claim_id for review in self.reviews)
        if len(set(ids)) != len(ids):
            raise ValueError("review claim IDs must be unique")
        if self.review_conclusion == "sufficient" and any(
            not review.resolved for review in self.reviews
        ):
            raise ValueError("sufficient review cannot contain unresolved issues")
        return self


class ResearchCondition(_IntentModel):
    """A deterministic signal predicate, never an executable expression."""

    signal: str = Field(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_.:-]*$")
    operator: Literal["lt", "lte", "eq", "gte", "gt", "is_true", "is_false"]
    threshold: DecimalString | StrictBool
    unit: Literal["ratio", "basis_points", "days", "currency", "boolean", "count", "state"]
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=10)

    @model_validator(mode="after")
    def _operator_matches_threshold(self) -> "ResearchCondition":
        boolean_operator = self.operator in {"is_true", "is_false"}
        if boolean_operator != isinstance(self.threshold, bool):
            raise ValueError(
                "boolean operators require booleans; comparison operators require decimals"
            )
        if boolean_operator != (self.unit == "boolean"):
            raise ValueError("boolean predicates require boolean units")
        return self


class ResearchConditionEvaluation(_IntentModel):
    """A frozen research observation, never an entry/exit or execution grant."""

    schema_version: Literal["research_condition_evaluation_v1"] = "research_condition_evaluation_v1"
    purpose: Literal["research_observation"] = "research_observation"
    condition: ResearchCondition
    result: Literal["true", "false", "unknown"]
    reason: Literal["frozen_concentration_unavailable"] | None = None
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _closed_observation(self) -> "ResearchConditionEvaluation":
        if (
            self.condition.signal not in {
                "portfolio.top1_warning_exceeded", "portfolio.top1_critical_exceeded"
            }
            or self.condition.operator != "is_true" or self.condition.threshold is not True
            or self.condition.unit != "boolean"
            or ((self.result == "unknown") != (self.reason is not None))
        ):
            raise ValueError("invalid frozen research observation")
        return self


class RoleVote(_IntentModel):
    role: Literal[
        "orchestrator",
        "fundamental",
        "market_state",
        "macro_sector",
        "bull",
        "bear",
        "evidence_audit",
        "portfolio",
    ]
    vote: Literal["support", "oppose", "abstain"]


class ResearchIntent(_IntentModel):
    synthesis_assessment: SynthesisAssessment | None = None
    condition_evaluations: tuple[ResearchConditionEvaluation, ...] | None = Field(
        default=None, min_length=2, max_length=2
    )

    @model_serializer(mode="wrap")
    # A return annotation would replace the closed model serialization schema.
    def _legacy_payload(self, handler: SerializerFunctionWrapHandler):  # type: ignore[no-untyped-def]
        payload = cast(dict[str, object], handler(self))
        if self.synthesis_assessment is None:
            payload.pop("synthesis_assessment", None)
        if self.condition_evaluations is None:
            payload.pop("condition_evaluations", None)
        return payload

    schema_version: Literal["research_intent_v1"]
    decision_id: UUID
    instrument_id: UUID
    display_symbol: str = Field(min_length=1, max_length=32, pattern=r"^[A-Z0-9.\-]+$")
    decision_time: datetime
    data_snapshot_id: UUID
    horizon: Literal["short_term", "medium_term", "long_term"]
    stance: Literal["buy_candidate", "reduce_candidate", "hold", "watch"]
    exposure_action: Literal["none", "reduce", "maintain", "eligible_for_increase"]
    theses: tuple[ResearchClaim, ...] = Field(max_length=5)
    counter_theses: tuple[ResearchClaim, ...] = Field(max_length=5)
    uncertainties: tuple[str, ...] = Field(max_length=10)
    invalidation_conditions: tuple[ResearchCondition, ...] = Field(max_length=10)
    entry_conditions: tuple[ResearchCondition, ...] = Field(max_length=10)
    exit_conditions: tuple[ResearchCondition, ...] = Field(max_length=10)
    valid_until: datetime
    quality_status: Literal["complete", "partial"]
    evidence_audit: Literal["complete", "partial", "failed"]
    role_votes: tuple[RoleVote, ...] = Field(min_length=1, max_length=8)
    disagreement: str = Field(min_length=1, max_length=500)
    strategy_version: str = Field(min_length=1, max_length=100)
    model_version: str = Field(min_length=1, max_length=100)
    prompt_version: str = Field(min_length=1, max_length=100)
    data_version: str = Field(min_length=1, max_length=100)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1)

    @field_validator("decision_time", "valid_until")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _point_in_time_and_complete_evidence(self) -> "ResearchIntent":
        if self.condition_evaluations is not None and (
            len({item.condition.signal for item in self.condition_evaluations}) != 2
            or any(item.observed_at > self.decision_time for item in self.condition_evaluations)
            or any(ref not in self.evidence_refs for item in self.condition_evaluations
                   for ref in item.condition.evidence_refs)
        ):
            raise ValueError("research observations must bind original evidence and time")
        if self.valid_until <= self.decision_time:
            raise ValueError("valid_until must follow decision_time")
        if any(ref.available_at > self.decision_time for ref in self.evidence_refs):
            raise ValueError("research evidence cannot become available after decision_time")
        if any(
            ref.available_at > self.decision_time
            for claim in (*self.theses, *self.counter_theses)
            for ref in claim.evidence_refs
        ):
            raise ValueError("claim evidence cannot become available after decision_time")
        if len({vote.role for vote in self.role_votes}) != len(self.role_votes):
            raise ValueError("research roles must vote at most once")
        if any(
            ref.available_at > self.decision_time
            for condition in (
                *self.invalidation_conditions,
                *self.entry_conditions,
                *self.exit_conditions,
            )
            for ref in condition.evidence_refs
        ):
            raise ValueError("condition evidence cannot become available after decision_time")
        return self
