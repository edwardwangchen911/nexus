"""Non-model archive consumption through durable Jobs and business read models.

This entry is intentionally limited to isolated recovery databases. It never
renews research authority, reserves model budget, or invokes a provider.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from uuid import UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_synthesis_contracts import canonical_portfolio_synthesis_hash as digest
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy
from nexus.jobs.registry import JobHandlerContext, JobHandlerResult, JobHandlerSpec, JobRegistry
from nexus.jobs.repository import enqueue_job
from nexus.paper.historical_assessment_source import MODEL_HASH, PROFILE, project_archive
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.historical_assessment import HistoricalAssessmentSourceRecord
from nexus.persistence.models.paper import AgentPaperCycleRecord

SCHEMA = "historical_archive_job_v1"


async def require_isolated(session: AsyncSession) -> None:
    name = await session.scalar(text("SELECT current_database()"))
    if not isinstance(name, str) or not name.startswith(("nexus_recovery_", "nexus_test_")):
        raise ValueError("ARCHIVE_ISOLATED_DATABASE_REQUIRED")


def copy_columns(row: Any, **overrides: Any) -> dict[str, Any]:
    return {
        **{column.name: getattr(row, column.name) for column in row.__table__.columns},
        **overrides,
    }


async def source_for_cycle(
    session: AsyncSession, cycle_id: UUID
) -> HistoricalAssessmentSourceRecord | None:
    return cast(
        HistoricalAssessmentSourceRecord | None,
        await session.scalar(
            select(HistoricalAssessmentSourceRecord).where(
                HistoricalAssessmentSourceRecord.cycle_id == cycle_id
            )
        ),
    )


async def validate_source(
    session: AsyncSession, source: HistoricalAssessmentSourceRecord, *, at: datetime
) -> dict[str, Any]:
    payload = cast(dict[str, Any], source.payload)
    if at < source.imported_at or digest(payload) != source.content_hash:
        raise ValueError("ARCHIVE_SOURCE_BINDING_INVALID")
    archive, projection = payload["archive"], payload["projection"]
    synth = projection["synthesis"]
    rebuilt = project_archive(
        archive,
        cycle_id=source.cycle_id,
        model_id=UUID(synth["frozen_input"]["model_config_id"]),
        imported_at=source.imported_at,
    )
    if rebuilt != projection or archive["archive_hash"] != source.archive_hash:
        raise ValueError("ARCHIVE_SOURCE_BINDING_INVALID")
    if (
        source.source_cycle_id != UUID(archive["case"]["source_cycle_id"])
        or source.evidence_at != datetime.fromisoformat(archive["case"]["evidence_as_of"])
        or source.observed_started_at != datetime.fromisoformat(archive["observed_started_at"])
        or source.observed_finished_at != datetime.fromisoformat(archive["observed_finished_at"])
    ):
        raise ValueError("ARCHIVE_SOURCE_BINDING_INVALID")
    run = await session.get(AgentPortfolioSynthesisRunRecord, UUID(synth["run_id"]))
    if (
        run is None
        or run.cycle_id != source.cycle_id
        or run.frozen_input != synth["frozen_input"]
        or run.input_hash != synth["input_hash"]
        or run.model_hash != MODEL_HASH
        or run.pricing_version != PROFILE
        or run.created_at < source.imported_at
        or run.created_at > at
    ):
        raise ValueError("ARCHIVE_SYNTHESIS_BINDING_INVALID")
    actual_outputs = tuple(
        await session.scalars(
            select(AgentPortfolioSynthesisOutputRecord).where(
                AgentPortfolioSynthesisOutputRecord.run_id == run.id
            )
        )
    )
    if (
        len(actual_outputs) > 1
        or (actual_outputs and actual_outputs[0].id != UUID(synth["output_id"]))
        or (run.status == "completed" and not actual_outputs)
    ):
        raise ValueError("ARCHIVE_SYNTHESIS_BINDING_INVALID")
    output = actual_outputs[0] if actual_outputs else None
    if output is not None and (
        run.status != "completed"
        or run.completed_at is None
        or run.completed_at < output.created_at
        or run.completed_at > at
        or output.run_id != run.id
        or output.status != "completed"
        or output.intent != synth["intent"]
        or output.content_hash != synth["content_hash"]
        or output.binding_hash != synth["binding_hash"]
        or output.created_at < source.imported_at
        or output.created_at > at
    ):
        raise ValueError("ARCHIVE_SYNTHESIS_BINDING_INVALID")
    return cast(dict[str, Any], projection)


async def admit_archive(
    session: AsyncSession, archive: dict[str, Any]
) -> tuple[UUID, tuple[UUID, ...]]:
    await require_isolated(session)
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("ARCHIVE_CLOCK_INVALID")
    source_id = UUID(archive["case"]["completion"]["id"])
    if await session.get(HistoricalAssessmentSourceRecord, source_id) is not None:
        raise ValueError("ARCHIVE_ALREADY_CONSUMED")
    original = await session.get(AgentPaperCycleRecord, UUID(archive["case"]["source_cycle_id"]))
    if original is None or original.execution_locked is not True:
        raise ValueError("ARCHIVE_SOURCE_CYCLE_MISSING")
    cycle_id = uuid4()
    model_id = uuid5(source_id, PROFILE)
    model = AgentModelConfigRecord(
        id=model_id,
        model_id=f"archive-no-model:{source_id}",
        version=PROFILE,
        config_hash=MODEL_HASH,
        max_input_tokens=1,
        max_tool_calls=0,
        max_output_tokens=1,
        input_usd_per_million=Decimal(0),
        output_usd_per_million=Decimal(0),
        pricing_available_at=now,
        pricing_expires_at=now + timedelta(days=1),
        created_at=now,
    )
    cycle = AgentPaperCycleRecord(
        **copy_columns(
            original,
            id=cycle_id,
            decision_slot_at=now,
            formal_budget_binding_id=None,
            created_at=now,
        )
    )
    session.add_all([model, cycle])
    await session.flush()
    projection = project_archive(archive, cycle_id=cycle_id, model_id=model_id, imported_at=now)
    payload = dict(archive=archive, projection=projection)
    source = HistoricalAssessmentSourceRecord(
        id=source_id,
        cycle_id=cycle_id,
        source_cycle_id=original.id,
        archive_hash=archive["archive_hash"],
        payload=payload,
        content_hash=digest(payload),
        evidence_at=datetime.fromisoformat(archive["case"]["evidence_as_of"]),
        observed_started_at=datetime.fromisoformat(archive["observed_started_at"]),
        observed_finished_at=datetime.fromisoformat(archive["observed_finished_at"]),
        imported_at=now,
    )
    session.add(source)
    await session.flush()
    jobs = []
    from nexus.paper.agent_cycle import _events, _fact_uuids

    original_run_ids = _fact_uuids(await _events(session, original.id), "research_run_ids")
    for row in projection["research"]:
        prior = await session.get(AgentResearchRunRecord, UUID(row["source_run_id"]))
        member = next(
            member
            for member in archive["case"]["members"]
            if member["source_run_id"] == row["source_run_id"]
        )
        if (
            prior is None
            or str(prior.instrument_id) != row["instrument_id"]
            or prior.decision_time != source.evidence_at
            or str(prior.data_snapshot_id) != member["request"]["data_snapshot_id"]
            or prior.id not in original_run_ids
        ):
            raise ValueError("ARCHIVE_RESEARCH_SOURCE_MISSING")
        run = AgentResearchRunRecord(
            **copy_columns(
                prior,
                id=UUID(row["run_id"]),
                model_config_id=model_id,
                model_hash=MODEL_HASH,
                actor_id=PROFILE,
                idempotency_key=f"agent-paper:{cycle_id}:{prior.instrument_id}",
                status="prepared",
                retry_count=0,
                reserved_cost_usd=Decimal(0),
                settled_cost_usd=None,
                pricing_version=PROFILE,
                input_tokens=None,
                output_tokens=None,
                input_hash=digest(row),
                created_at=now,
            )
        )
        session.add(run)
        await session.flush()
        job = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.AGENT_PAPER_RESEARCH,
                scope_kind="instrument",
                scope_id=str(run.instrument_id),
                effective_at=now,
                input_hash=run.input_hash,
            ),
            schema_version=SCHEMA,
            payload={"run_id": str(run.id)},
            queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.NEVER,
            max_attempts=1,
        )
        jobs.append(job.id)
    frozen = projection["synthesis"]["frozen_input"]
    syn = projection["synthesis"]
    synthesis_run = AgentPortfolioSynthesisRunRecord(
        id=UUID(syn["run_id"]),
        cycle_id=cycle_id,
        candidate_batch_id=original.candidate_batch_id,
        research_output_ids=[UUID(row["output_id"]) for row in projection["research"]],
        active_target_version_id=UUID(frozen["active_target_version_id"])
        if frozen["active_target_version_id"]
        else None,
        projection_hash=frozen["projection_hash"],
        prompt_version_id=UUID(frozen["prompt_version_id"]),
        model_config_id=model_id,
        prompt_hash=frozen["prompt_hash"],
        model_hash=MODEL_HASH,
        input_hash=syn["input_hash"],
        frozen_input=frozen,
        idempotency_key=f"portfolio-synthesis:{cycle_id}",
        status="prepared",
        pricing_version=PROFILE,
        decision_time=source.evidence_at,
        created_at=now,
    )
    session.add(synthesis_run)
    await session.flush()
    job = await enqueue_job(
        session,
        key=JobKeyV1(
            job_type=JobType.AGENT_PORTFOLIO_SYNTHESIS,
            scope_kind="portfolio",
            scope_id=str(cycle_id),
            effective_at=now,
            input_hash=synthesis_run.input_hash,
        ),
        schema_version=SCHEMA,
        payload={"run_id": str(synthesis_run.id)},
        queue=QueueName.GENERAL,
        retry_policy=RetryPolicy.NEVER,
        max_attempts=1,
    )
    jobs.append(job.id)
    from nexus.paper.agent_cycle import _append_event

    await _append_event(session, cycle, None, "prepared", "CYCLE_PREPARED", {}, occurred_at=now)
    await _append_event(
        session,
        cycle,
        "prepared",
        "researching",
        "RESEARCH_JOBS_PREPARED",
        {"research_run_ids": [row["run_id"] for row in projection["research"]]},
        occurred_at=now,
    )
    return cycle_id, tuple(jobs)


class ArchivePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: UUID


async def consume_archive(context: JobHandlerContext) -> JobHandlerResult:
    session = context.session
    await require_isolated(session)
    if not isinstance(context.payload, ArchivePayload):
        raise ValueError("ARCHIVE_PAYLOAD_INVALID")
    run_id = context.payload.run_id
    run: AgentResearchRunRecord | AgentPortfolioSynthesisRunRecord | None
    sources = (await session.scalars(select(HistoricalAssessmentSourceRecord))).all()
    matching = [
        source
        for source in sources
        if str(run_id)
        in {
            *(
                row["run_id"]
                for row in cast(dict[str, Any], source.payload)["projection"]["research"]
            ),
            cast(dict[str, Any], source.payload)["projection"]["synthesis"]["run_id"],
        }
    ]
    if len(matching) != 1:
        raise ValueError("ARCHIVE_JOB_SOURCE_INVALID")
    source = matching[0]
    now = context.finished_at()
    projection = await validate_source(session, source, at=now)
    row = next((row for row in projection["research"] if row["run_id"] == str(run_id)), None)
    if row is not None:
        run = await session.get(AgentResearchRunRecord, run_id)
        if run is None or run.status != "prepared" or run.actor_id != PROFILE:
            raise ValueError("ARCHIVE_JOB_ALREADY_CONSUMED")
        for status in ("running", "validated", "audited", "synthesized"):
            run.status = status
            await session.flush()
        output = AgentResearchOutputRecord(
            id=UUID(row["output_id"]),
            run_id=run_id,
            sequence=1,
            status="completed",
            role=None,
            prompt_hash=None,
            content_hash=row["content_hash"],
            payload=row["payload"],
            intent=row["intent"],
            created_at=now,
        )
        session.add(output)
        await session.flush()
        from nexus.persistence.models.research import ResearchEvidenceRef

        for reference_id in row["evidence_ids"]:
            ref = await session.get(ResearchEvidenceRef, UUID(reference_id))
            if ref is None:
                raise ValueError("ARCHIVE_EVIDENCE_MISSING")
            session.add(
                AgentResearchOutputEvidenceRecord(
                    output_id=output.id,
                    evidence_ref_id=ref.id,
                    source=ref.source,
                    provider_object_id=ref.provider_object_id,
                    revision=ref.revision,
                    available_at=ref.available_at,
                    location=ref.location,
                    content_hash=ref.content_hash,
                    created_at=now,
                )
            )
        run.status = "completed"
    else:
        await validate_research(session, source, at=now)
        row = projection["synthesis"]
        run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
        if run is None or run.status != "prepared":
            raise ValueError("ARCHIVE_JOB_ALREADY_CONSUMED")
        run.status = "running"
        await session.flush()
        session.add(
            AgentPortfolioSynthesisOutputRecord(
                id=UUID(row["output_id"]),
                run_id=run_id,
                status="completed",
                intent=row["intent"],
                content_hash=row["content_hash"],
                binding_hash=row["binding_hash"],
                created_at=now,
            )
        )
        run.status = "completed"
        run.completed_at = now
    # Zero new model use for normalization only; original usage stays in immutable payload.
    run.settled_cost_usd = Decimal(0)
    run.input_tokens = run.output_tokens = 0
    await session.flush()
    return JobHandlerResult("succeeded", "ARCHIVE_CONSUMED_NO_MODEL")


async def validate_research(
    session: AsyncSession, source: HistoricalAssessmentSourceRecord, *, at: datetime
) -> dict[str, Any]:
    projection = await validate_source(session, source, at=at)
    for row in projection["research"]:
        run = await session.get(AgentResearchRunRecord, UUID(row["run_id"]))
        output = await session.get(AgentResearchOutputRecord, UUID(row["output_id"]))
        if (
            run is None
            or output is None
            or run.status != "completed"
            or run.actor_id != PROFILE
            or output.run_id != run.id
            or output.created_at > at
            or output.created_at < source.imported_at
            or output.content_hash != row["content_hash"]
            or output.intent != row["intent"]
            or output.payload != row["payload"]
            or run.decision_time != source.evidence_at
        ):
            raise ValueError("ARCHIVE_RESEARCH_BINDING_INVALID")
        member = next(
            member
            for member in cast(dict[str, Any], source.payload)["archive"]["case"]["members"]
            if member["source_run_id"] == row["source_run_id"]
        )
        if (
            str(run.instrument_id) != row["instrument_id"]
            or str(run.data_snapshot_id) != member["request"]["data_snapshot_id"]
            or run.model_hash != MODEL_HASH
            or run.pricing_version != PROFILE
            or run.input_hash != digest(row)
            or output.status != "completed"
            or output.role is not None
            or run.created_at < source.imported_at
            or run.created_at > at
        ):
            raise ValueError("ARCHIVE_RESEARCH_BINDING_INVALID")
        bound = (
            await session.scalars(
                select(AgentResearchOutputEvidenceRecord).where(
                    AgentResearchOutputEvidenceRecord.output_id == output.id
                )
            )
        ).all()
        from nexus.agents.execution_authority import load_frozen_evidence
        from nexus.agents.synthesis import domain_evidence

        original_run = await session.get(AgentResearchRunRecord, UUID(row["source_run_id"]))
        if original_run is None:
            raise ValueError("ARCHIVE_EVIDENCE_BINDING_INVALID")
        originals = await load_frozen_evidence(session, original_run, allow_fixture_evidence=False)
        if [str(ref.id) for ref in originals] != member["evidence_ids"] or [
            domain_evidence(ref).model_dump(mode="json") for ref in originals
        ] != member["evidence"]:
            raise ValueError("ARCHIVE_EVIDENCE_BINDING_INVALID")
        expected = {ref.id: ref for ref in originals}
        if {ref.evidence_ref_id for ref in bound} != set(expected) or len(bound) != len(expected):
            raise ValueError("ARCHIVE_EVIDENCE_BINDING_INVALID")
        for ref in bound:
            original_ref = expected[ref.evidence_ref_id]
            for key in (
                "source",
                "provider_object_id",
                "revision",
                "location",
                "content_hash",
                "available_at",
            ):
                if getattr(ref, key) != getattr(original_ref, key):
                    raise ValueError("ARCHIVE_EVIDENCE_BINDING_INVALID")
    return cast(dict[str, Any], projection)


SAFE_ARCHIVE_CODES = frozenset(
    {
        "ARCHIVE_ISOLATED_DATABASE_REQUIRED",
        "ARCHIVE_SOURCE_BINDING_INVALID",
        "ARCHIVE_SYNTHESIS_BINDING_INVALID",
        "ARCHIVE_CLOCK_INVALID",
        "ARCHIVE_ALREADY_CONSUMED",
        "ARCHIVE_SOURCE_CYCLE_MISSING",
        "ARCHIVE_RESEARCH_SOURCE_MISSING",
        "ARCHIVE_PAYLOAD_INVALID",
        "ARCHIVE_JOB_SOURCE_INVALID",
        "ARCHIVE_JOB_ALREADY_CONSUMED",
        "ARCHIVE_EVIDENCE_MISSING",
        "ARCHIVE_RESEARCH_BINDING_INVALID",
        "ARCHIVE_EVIDENCE_BINDING_INVALID",
        "ARCHIVE_CHANGED_DURING_READ",
        "ARCHIVE_ASSESSMENT_REQUIRED",
        "ARCHIVE_NONEXECUTING_RESULT_REQUIRED",
        "ARCHIVE_AVAILABILITY_INVALID",
        "ARCHIVE_JOB_FAILED",
        "ARCHIVE_CYCLE_MISSING",
        "ARCHIVE_SOURCE_MISSING",
        "ARCHIVE_CLOSEOUT_INCOMPLETE",
        "ARCHIVE_ATTEMPT_BINDING_INVALID",
        "ARCHIVE_HANDLER_FAILED",
    }
)


async def guarded_archive_consumer(context: JobHandlerContext) -> JobHandlerResult:
    from nexus.jobs.errors import UnsafeJobError

    try:
        return await consume_archive(context)
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) else "ARCHIVE_HANDLER_FAILED"
        raise UnsafeJobError(
            code if code in SAFE_ARCHIVE_CODES else "ARCHIVE_HANDLER_FAILED"
        ) from error


def archive_registry() -> JobRegistry:
    return JobRegistry(
        [
            JobHandlerSpec(
                job_type=kind,
                schema_version=SCHEMA,
                payload_model=ArchivePayload,
                queue=QueueName.GENERAL,
                timeout_seconds=45,
                retry_policy=RetryPolicy.NEVER,
                max_attempts=1,
                idempotency_namespace="agent.archive",
                handler=guarded_archive_consumer,
            )
            for kind in (JobType.AGENT_PAPER_RESEARCH, JobType.AGENT_PORTFOLIO_SYNTHESIS)
        ]
    )
