"""Read-only construction inputs are not formal comparison admission authority."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord


@dataclass(frozen=True)
class AnalysisConstructionInputs:
    portfolio_id: UUID
    diagnostic_report_id: UUID
    valuation_time: datetime


async def load_analysis_construction_inputs(
    session: AsyncSession,
    *,
    portfolio_snapshot_id: UUID,
    data_snapshot_id: UUID,
    cost_policy_id: UUID,
    now: datetime,
) -> AnalysisConstructionInputs | None:
    """Accept only a produced draft index bound to its unchanged snapshot and diagnosis."""
    data = await session.get(DataSnapshot, data_snapshot_id)
    snapshot = await session.get(PortfolioSnapshotRecord, portfolio_snapshot_id)
    if (
        data is None
        or snapshot is None
        or snapshot.status != "READY"
        or snapshot.as_of is None
        or snapshot.as_of > now
        or snapshot.created_at > now
        or data.source != "nexus_optimizer_draft"
        or data.schema_version != "optimization_draft_inputs_v1"
        or data.observed_at != snapshot.as_of
        or data.available_at > now
        or data.received_at > now
        or data.created_at > now
        or cost_policy_id != COST_POLICY_ID
    ):
        return None
    payload = data.payload
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if (
        data.revision != digest
        or payload.get("portfolio_snapshot_id") != str(snapshot.id)
        or payload.get("cost_policy_id") != str(cost_policy_id)
    ):
        return None
    group = await session.scalar(
        select(OptimizationProposalGroupRecord)
        .where(
            OptimizationProposalGroupRecord.baseline_portfolio_snapshot_id == snapshot.id,
            OptimizationProposalGroupRecord.data_snapshot_id == data.id,
            OptimizationProposalGroupRecord.cost_policy_id == cost_policy_id,
            OptimizationProposalGroupRecord.portfolio_scope == "real_read_only",
            OptimizationProposalGroupRecord.created_at <= now,
        )
        .order_by(OptimizationProposalGroupRecord.id)
        .limit(1)
    )
    if group is None:
        return None
    report = await session.get(PortfolioDiagnosticReportRecord, group.diagnostic_report_id)
    if (
        report is None
        or report.portfolio_id != snapshot.portfolio_id
        or report.portfolio_snapshot_id != snapshot.id
        or report.snapshot_as_of != snapshot.as_of
        or report.created_at > now
        or report.policy_id != REAL_ADVISORY_POLICY_ID
        or report.policy_version != "real_advisory_policy_v1"
        or report.optimization_need not in {"RESEARCH_REQUIRED", "OPTIMIZATION_CANDIDATE"}
        or payload.get("diagnostic_report_id") != str(report.id)
        or payload.get("diagnostic_input_hash") != report.diagnostic_input_hash
        or group.diagnostic_input_hash != report.diagnostic_input_hash
        or group.valuation_time != snapshot.as_of
        or payload.get("source_policy_id") != str(group.source_policy_id)
        or payload.get("optimizer_policy_id") != str(group.policy_id)
        or data.provider_object_id != f"{snapshot.id}:{report.id}"
    ):
        return None
    return AnalysisConstructionInputs(snapshot.portfolio_id, report.id, snapshot.as_of)
