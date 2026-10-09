"""Pending formal stage-capacity identity, separate from frozen candidate and v6."""

import hashlib
import json
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.jobs.models import JobType, QueueName, RetryPolicy, job_input_hash
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job


def formal_stage_config() -> dict[str, str | int]:
    from nexus.agents.codex_sdk import MODEL_ID

    return {
        "model_id": MODEL_ID,
        "version": FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
        "max_input_tokens": 96000,
        "max_output_tokens": 8192,
        "max_tool_calls": 0,
        "input_usd_per_million": "0",
        "output_usd_per_million": "0",
        "pricing_available_at": "2026-09-02T00:00:00+00:00",
        "pricing_expires_at": "2027-09-02T00:00:00+00:00",
        "billing_mode": "chatgpt_subscription",
        "max_sdk_turns": 3,
        "max_wire_bytes": 64000,
        "inline_citations": True,
        "stage_time_budget": json.dumps(
            {
                "job_absolute_seconds": 180,
                "handler_absolute_seconds": 195,
                "lease_seconds": 210,
                "terminal_reserve_seconds": 5,
                "sdk_close_reserve_seconds": 7,
                "stage_caps_seconds": {"research": 60, "critique": 45, "synthesis": 45},
                "later_phase_reserve_seconds": {"research": 104, "critique": 52, "synthesis": 0},
            },
            sort_keys=True,
        ),
    }


def formal_stage_config_hash() -> str:
    return hashlib.sha256(json.dumps(formal_stage_config(), sort_keys=True).encode()).hexdigest()


def formal_stage_model_matches(model: AgentModelConfigRecord) -> bool:
    config = formal_stage_config()
    return (
        model.version == config["version"]
        and model.config_hash == formal_stage_config_hash()
        and model.model_id == config["model_id"]
        and model.max_input_tokens == config["max_input_tokens"]
        and model.max_output_tokens == config["max_output_tokens"]
        and model.max_tool_calls == 0
        and model.input_usd_per_million == 0
        and model.output_usd_per_million == 0
        and model.pricing_available_at
        == datetime.fromisoformat(str(config["pricing_available_at"]))
        and model.pricing_expires_at == datetime.fromisoformat(str(config["pricing_expires_at"]))
    )


async def formal_stage_job_qualified(
    session: AsyncSession,
    job: Job,
    *,
    claimed: bool = False,
) -> bool:
    """Re-read frozen authority under the caller's selected Job lock, before writes."""
    if (
        job.job_type != JobType.AGENT_PAPER_RESEARCH.value
        or job.queue != QueueName.GENERAL.value
        or job.schema_version != "agent_research_job_v1"
        or job.retry_policy != RetryPolicy.AUTOMATIC_SAFE.value
        or job.max_attempts != 1
        or type(claimed) is not bool
        or job.status != ("running" if claimed else "queued")
        or job.attempt_count != (1 if claimed else 0)
        or job.manual_retry_count != 0
        or set(job.payload) != {"run_id"}
        or type(job.payload.get("run_id")) is not str
    ):
        return False
    try:
        run_id = UUID(cast(str, job.payload["run_id"]))
    except (ValueError, TypeError):
        return False
    run = await session.scalar(
        select(AgentResearchRunRecord)
        .where(
            AgentResearchRunRecord.id == run_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run is None or run.status != "prepared" or run.retry_count != 0:
        return False
    model = await session.scalar(
        select(AgentModelConfigRecord)
        .where(
            AgentModelConfigRecord.id == run.model_config_id,
        )
        .execution_options(populate_existing=True)
    )
    checked_at = await session.scalar(select(func.clock_timestamp()))
    if (
        model is None
        or not isinstance(checked_at, datetime)
        or not model.pricing_available_at <= checked_at < model.pricing_expires_at
        or not formal_stage_model_matches(model)
        or run.model_hash != model.config_hash
        or run.pricing_version != model.version
        or job.scope_kind != "instrument"
        or job.scope_id != str(run.instrument_id)
        or job.effective_at != run.decision_time
    ):
        return False
    return job.input_hash == _formal_stage_job_hash(run)


def _formal_stage_job_hash(run: AgentResearchRunRecord) -> str:
    return job_input_hash(
        {
            "instrument_id": str(run.instrument_id),
            "data_snapshot_id": str(run.data_snapshot_id),
            "decision_time": run.decision_time.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "request_hash": run.input_hash,
            "schema_hash": run.schema_hash,
            "model_hash": run.model_hash,
            "prompt_hash": run.prompt_hash,
            "evidence_hash": run.evidence_hash,
            "tool_hash": run.tool_hash,
            "pricing_version": run.pricing_version,
        }
    )
