"""Typed success envelopes for optimization admission and lifecycle writes."""

from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.optimizer.models import ProposalGroupStatus
from pydantic import BaseModel, ConfigDict, Field


class _StrictOptimizationSchema(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class OptimizationJobData(_StrictOptimizationSchema):
    job_id: UUID
    status: Literal["QUEUED"]


class OptimizationJobEnvelope(_StrictOptimizationSchema):
    data: OptimizationJobData
    meta: SuccessMetaResponse


class OptimizationLifecycleData(_StrictOptimizationSchema):
    proposal_group_id: UUID
    status: ProposalGroupStatus
    version: int = Field(ge=0)


class OptimizationLifecycleEnvelope(_StrictOptimizationSchema):
    data: OptimizationLifecycleData
    meta: SuccessMetaResponse
