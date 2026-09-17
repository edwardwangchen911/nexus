"""Strict read-only contract for live-trading evaluation gates."""

from typing import Literal

from nexus.api.schemas.integrations import SuccessMetaResponse
from pydantic import BaseModel, ConfigDict, Field


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class LiveEvaluationEvidenceCountResponse(_StrictSchema):
    key: str = Field(min_length=1, max_length=64)
    count: int = Field(ge=0)


class LiveEvaluationGateResponse(_StrictSchema):
    key: Literal[
        "internal_paper_execution",
        "oos_agent_version",
        "broker_simulation_lifecycle",
        "broker_fee_variance",
        "broker_recovery_drill",
    ]
    status: Literal["ready", "not_ready", "unknown"]
    reason_code: str = Field(min_length=1, max_length=100)
    evidence: tuple[LiveEvaluationEvidenceCountResponse, ...] = Field(
        min_length=1,
        max_length=8,
    )


class LiveEvaluationResponse(_StrictSchema):
    status: Literal["not_ready", "eligible_for_user_review"]
    execution_locked: Literal[True] = True
    live_write: Literal[False] = False
    user_decision_required: Literal[True] = True
    gates: tuple[LiveEvaluationGateResponse, ...] = Field(min_length=5, max_length=5)


class LiveEvaluationEnvelope(_StrictSchema):
    data: LiveEvaluationResponse
    meta: SuccessMetaResponse
