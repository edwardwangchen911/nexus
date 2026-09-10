"""Stable public schemas for persisted deterministic diagnostics."""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.domain.decimal import DecimalString, decimal_to_string
from nexus.domain.evidence import EvidenceRef
from pydantic import BaseModel, ConfigDict


class _StrictSchema(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class DiagnosticItemResponse(_StrictSchema):
    diagnostic_code: str
    metric_value: DecimalString | None
    unit: str
    reference_limit: DecimalString | None
    headroom: DecimalString | None
    headroom_reason: str | None
    policy_scope: str
    severity: Literal["info", "warning", "critical"]
    actionability: Literal["monitor", "research", "reduce_only", "blocked"]
    unknown_reason: str | None
    evidence_refs: tuple[EvidenceRef, ...]

    @classmethod
    def from_item(cls, item: DiagnosticItem) -> "DiagnosticItemResponse":
        headroom, headroom_reason = _headroom(item)
        return cls(
            **item.model_dump(), headroom=headroom, headroom_reason=headroom_reason
        )


class PortfolioDiagnosticResponse(_StrictSchema):
    diagnostic_report_id: UUID | None
    portfolio_snapshot_id: UUID
    snapshot_as_of: datetime
    trading_day: date | None
    policy_version: Literal["real_advisory_policy_v1"]
    optimization_need: Literal[
        "BLOCKED", "RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE", "NO_ACTION"
    ]
    items: tuple[DiagnosticItemResponse, ...]

    @classmethod
    def from_report(
        cls,
        report: PortfolioDiagnosticReport,
        *,
        diagnostic_report_id: UUID | None,
    ) -> "PortfolioDiagnosticResponse":
        if report.snapshot_as_of is None:
            raise ValueError("persisted diagnostic snapshot time is unavailable")
        return cls(
            diagnostic_report_id=diagnostic_report_id,
            portfolio_snapshot_id=report.portfolio_snapshot_id,
            snapshot_as_of=report.snapshot_as_of,
            trading_day=report.trading_day,
            policy_version=report.policy_version,
            optimization_need=report.optimization_need,
            items=tuple(DiagnosticItemResponse.from_item(item) for item in report.items),
        )


class PortfolioDiagnosticEnvelope(_StrictSchema):
    data: PortfolioDiagnosticResponse
    meta: SuccessMetaResponse


_UPPER_LIMIT_CODES = (
    "CONCENTRATION_TOP1",
    "CONCENTRATION_TOP3",
    "CORRELATION_CLUSTER",
    "CURRENCY_EXPOSURE",
    "RISK_BUCKET_EXPOSURE:",
    "LIQUIDITY_DAYS_TO_EXIT:",
    "DRAWDOWN_252:",
)


def _headroom(item: DiagnosticItem) -> tuple[DecimalString | None, str | None]:
    if item.metric_value is None or item.reference_limit is None:
        return None, item.unknown_reason or "HEADROOM_UNAVAILABLE"
    value = Decimal(item.metric_value)
    limit = Decimal(item.reference_limit)
    if item.diagnostic_code == "CASH_RATIO":
        return decimal_to_string(value - limit), None
    if item.diagnostic_code.startswith(_UPPER_LIMIT_CODES):
        return decimal_to_string(limit - value), None
    return None, "HEADROOM_DIRECTION_UNKNOWN"
