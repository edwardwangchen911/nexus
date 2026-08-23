# ruff: noqa: E501
"""Strict research intent contract; it deliberately has no order or weight fields."""

from collections.abc import Mapping
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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


class ResearchIntent(_IntentModel):
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
    invalidation_conditions: tuple[str, ...] = Field(max_length=10)
    entry_conditions: tuple[str, ...] = Field(max_length=10)
    exit_conditions: tuple[str, ...] = Field(max_length=10)
    valid_until: datetime
    quality_status: Literal["complete", "partial"]
    evidence_audit: Literal["complete", "partial", "failed"]
    role_votes: Mapping[str, Literal["support", "oppose", "abstain"]]
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
        return self
