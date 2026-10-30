"""Bounded preparation only. No dispatch, consent, business admission or budget bypass."""

import json
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.comparison_manifest import ACTOR, PURPOSE, ComparisonManifest
from nexus.agents.persistence import reserve_daily_budget
from nexus.jobs.models import job_input_hash
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import Job


async def register_comparison(session: AsyncSession, manifest: ComparisonManifest) -> None:
    """Caller owns commit. Same purpose/id/hash may be read back; never reset positions."""
    manifest = ComparisonManifest.model_validate(manifest.model_dump())
    await session.execute(
        text("""INSERT INTO research_comparison_manifest
      (id,purpose,manifest_hash,payload) VALUES (:id,:purpose,:hash,CAST(:payload AS jsonb))
      ON CONFLICT DO NOTHING"""),
        {
            "id": manifest.experiment_id,
            "purpose": PURPOSE,
            "hash": manifest.digest,
            "payload": manifest.model_dump_json(),
        },
    )
    row = (
        (
            await session.execute(
                text("""SELECT id,manifest_hash FROM
      research_comparison_manifest WHERE purpose=:purpose FOR UPDATE"""),
                {"purpose": PURPOSE},
            )
        )
        .mappings()
        .one()
    )
    if row["id"] != manifest.experiment_id or row["manifest_hash"] != manifest.digest:
        raise ValueError("COMPARISON_REGISTRATION_CONFLICT")


async def reserve_comparison_chain(
    session: AsyncSession,
    *,
    experiment_id: UUID,
    ordinal: int,
    run_id: UUID,
    job_id: UUID,
    daily_limit_usd: Decimal,
) -> UUID:
    """Bind a fresh native Run/Job to one fixed position and the real shared budget.

    A returned reservation is not permission to run. Re-entry always rejects;
    recovery reads the immutable registry and native facts without reserving again.
    Historical decision time never selects the budget date.
    """
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError("COMPARISON_TRANSACTION_INVALID")
    stored = (
        (
            await session.execute(
                text("""SELECT payload,manifest_hash FROM research_comparison_manifest
      WHERE id=:id FOR UPDATE"""),
                {"id": experiment_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if stored is None:
        raise ValueError("COMPARISON_REGISTRATION_REQUIRED")
    manifest = ComparisonManifest.model_validate_json(json.dumps(stored["payload"]))
    if manifest.experiment_id != experiment_id or manifest.digest != stored["manifest_hash"]:
        raise ValueError("COMPARISON_REGISTRATION_CONFLICT")
    expected_input = manifest.input_hash_for(ordinal)
    symbol = manifest.positions[ordinal - 1][1]
    instrument_id = manifest.instrument_ids[("AAPL", "AVGO", "NVDA").index(symbol)]
    used = await session.scalar(
        text("""SELECT EXISTS(SELECT 1 FROM research_comparison_chain
      WHERE experiment_id=:id AND ordinal=:ordinal)"""),
        {"id": experiment_id, "ordinal": ordinal},
    )
    if used:
        raise ValueError("COMPARISON_POSITION_CONSUMED")
    run = await session.get(AgentResearchRunRecord, run_id)
    job = await session.get(Job, job_id)
    if (
        run is None
        or job is None
        or run.actor_id != ACTOR
        or run.status != "prepared"
        or run.retry_count != 0
        or run.idempotency_key != f"comparison:{experiment_id}:{ordinal}"
        or job.job_key != f"comparison:{experiment_id}:{ordinal}"
        or job.schema_version != "comparison-v1"
        or job.scope_kind != "instrument"
        or job.scope_id != str(run.instrument_id)
        or job.effective_at != run.decision_time
        or job.job_type != "agent.research.comparison"
        or job.queue != "strategy_rnd"
        or job.status != "queued"
        or job.attempt_count != 0
        or job.max_attempts != 1
        or job.payload != {"run_id": str(run_id)}
        or job.retry_policy != "never"
        or job.manual_retry_count != 0
        or job.input_hash != job_input_hash(job.payload)
        or run.instrument_id != instrument_id
        or run.input_hash != expected_input
    ):
        raise ValueError("COMPARISON_NATIVE_BINDING_INVALID")
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    expected_config = (
        manifest.sdk_config_hash
        if manifest.positions[ordinal - 1][2] == "sdk"
        else manifest.api_config_hash
    )
    if (
        model is None
        or model.config_hash != expected_config
        or run.model_hash != expected_config
        or run.pricing_version != model.version
        or model.model_id != "gpt-5.6-terra"
        or model.max_input_tokens != 96000
        or model.max_output_tokens != 8192
        or model.max_tool_calls != 0
        or run.reserved_cost_usd != model.worst_case_cost_usd
    ):
        raise ValueError("COMPARISON_CONFIG_INVALID")
    if (
        await session.scalar(
            select(AgentDailyBudgetReservationRecord.id).where(
                AgentDailyBudgetReservationRecord.run_id == run_id
            )
        )
        is not None
    ):
        raise ValueError("COMPARISON_RESERVATION_ALREADY_PRESENT")
    now = await session.scalar(text("SELECT clock_timestamp()"))
    budget = await reserve_daily_budget(
        session,
        run_id=run_id,
        now=now,
        daily_limit_usd=daily_limit_usd,
        worst_case_usd=model.worst_case_cost_usd,
        pricing_version=run.pricing_version,
        pricing_fresh=model.pricing_available_at <= now < model.pricing_expires_at,
    )
    await session.execute(
        text("""INSERT INTO research_comparison_chain
      (experiment_id,ordinal,run_id,job_id,reserved_id,input_hash)
      VALUES (:id,:ordinal,:run,:job,:budget,:hash)"""),
        {
            "id": experiment_id,
            "ordinal": ordinal,
            "run": run_id,
            "job": job_id,
            "budget": budget.id,
            "hash": expected_input,
        },
    )
    return budget.id
