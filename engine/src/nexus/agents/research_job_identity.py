"""Frozen input identity for native agent_research_job_v1 jobs."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from nexus.jobs.models import job_input_hash

if TYPE_CHECKING:
    from nexus.persistence.models.agents import AgentResearchRunRecord


def research_job_input_hash(
    *,
    instrument_id: UUID,
    data_snapshot_id: UUID,
    decision_time: datetime,
    request_hash: str,
    schema_hash: str,
    model_hash: str,
    prompt_hash: str,
    evidence_hash: str,
    tool_hash: str,
    pricing_version: str,
) -> str:
    return job_input_hash(
        {
            "instrument_id": str(instrument_id),
            "data_snapshot_id": str(data_snapshot_id),
            "decision_time": decision_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "request_hash": request_hash,
            "schema_hash": schema_hash,
            "model_hash": model_hash,
            "prompt_hash": prompt_hash,
            "evidence_hash": evidence_hash,
            "tool_hash": tool_hash,
            "pricing_version": pricing_version,
        }
    )


def research_run_job_input_hash(
    run: AgentResearchRunRecord, *, decision_time: datetime | None = None
) -> str:
    """Rebuild the Job identity from a Run using the reader's original time convention."""
    return research_job_input_hash(
        instrument_id=run.instrument_id,
        data_snapshot_id=run.data_snapshot_id,
        decision_time=run.decision_time if decision_time is None else decision_time,
        request_hash=run.input_hash,
        schema_hash=run.schema_hash,
        model_hash=run.model_hash,
        prompt_hash=run.prompt_hash,
        evidence_hash=run.evidence_hash,
        tool_hash=run.tool_hash,
        pricing_version=run.pricing_version,
    )
