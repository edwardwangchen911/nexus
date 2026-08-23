"""Stable public schemas for persisted deterministic diagnostics."""

from datetime import date
from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from nexus.domain.evidence import EvidenceRef
from pydantic import BaseModel, ConfigDict


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DiagnosticItemResponse(_StrictSchema):
    diagnostic_code: str
    metric_value: DecimalString | None
    unit: str
    reference_limit: DecimalString | None
    policy_scope: str
    severity: Literal["info", "warning", "critical"]
    actionability: Literal["monitor", "research", "reduce_only", "blocked"]
    unknown_reason: str | None
    evidence_refs: tuple[EvidenceRef, ...]


class PortfolioDiagnosticResponse(_StrictSchema):
    portfolio_snapshot_id: UUID
    trading_day: date | None
    policy_version: Literal["real_advisory_policy_v1"]
    optimization_need: Literal[
        "BLOCKED", "RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE", "NO_ACTION"
    ]
    items: tuple[DiagnosticItemResponse, ...]


class PortfolioDiagnosticEnvelope(_StrictSchema):
    data: PortfolioDiagnosticResponse
    meta: SuccessMetaResponse
