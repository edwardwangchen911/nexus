"""One bounded local proposal handler for the existing general Job worker."""

from typing import cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

from nexus.diagnostics.artifacts import ArtifactByteReader
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.registry import JobHandlerContext, JobHandlerResult, JobHandlerSpec, JobRegistry
from nexus.optimizer.authority import load_admission_authority
from nexus.optimizer.construction import load_analysis_construction_inputs
from nexus.optimizer.formal_inputs import FormalOptimizationJobBinding
from nexus.optimizer.initial_draft import create_initial_draft
from nexus.optimizer.production import build_and_persist_proposal_group
from nexus.persistence.models.runtime import Job
from nexus.strategy.intents import ResearchIntent


class OptimizationConstructionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    portfolio_snapshot_id: UUID
    data_snapshot_id: UUID
    cost_policy_id: UUID
    formal_binding: FormalOptimizationJobBinding | None = None

    @model_validator(mode="before")
    @classmethod
    def _explicit_binding(cls, value: object) -> object:
        if isinstance(value, dict) and "formal_binding" in value and value["formal_binding"] is None:
            raise ValueError("OPTIMIZATION_FORMAL_BINDING_INVALID")
        return value

    @model_validator(mode="after")
    def _formal_identity(self) -> "OptimizationConstructionPayload":
        if (
            self.formal_binding is not None
            and self.formal_binding.data_snapshot_id != self.data_snapshot_id
        ):
            raise ValueError("OPTIMIZATION_FORMAL_BINDING_INVALID")
        return self

    @model_serializer(mode="wrap")
    def _legacy_payload(self, handler: SerializerFunctionWrapHandler) -> dict[str, object]:
        result = handler(self)
        if self.formal_binding is None:
            result.pop("formal_binding", None)
        return cast(dict[str, object], result)


async def _construct(
    context: JobHandlerContext,
    artifact_reader: ArtifactByteReader | None,
) -> JobHandlerResult:
    payload = OptimizationConstructionPayload.model_validate(context.payload.model_dump())
    job = await context.session.get(Job, context.claim.job_id)
    if job is None:
        raise ValueError("OPTIMIZATION_JOB_UNAVAILABLE")
    loaded = await load_admission_authority(
        context.session,
        portfolio_snapshot_id=payload.portfolio_snapshot_id,
        data_snapshot_id=payload.data_snapshot_id,
        cost_policy_id=payload.cost_policy_id,
    )
    actor_id = f"optimizer-job:{job.id}"
    research: tuple[ResearchIntent, ...] = ()
    if payload.formal_binding is not None:
        from nexus.optimizer.formal_inputs import load_formal_research, read_formal_inputs
        from nexus.persistence.models.data import DataSnapshot

        binding = payload.formal_binding
        data = await context.session.get(DataSnapshot, binding.data_snapshot_id)
        if (
            loaded is None
            or data is None
            or loaded.admission_authority_id != binding.admission_authority_id
            or data.revision != binding.data_snapshot_hash
            or loaded.persistence.decision_cutoff_at != binding.decision_cutoff_at
        ):
            raise ValueError("OPTIMIZATION_FORMAL_BINDING_INVALID")
        research = await load_formal_research(context.session, read_formal_inputs(data))
    elif loaded is not None and loaded.persistence.decision_cutoff_at is not None:
        raise ValueError("OPTIMIZATION_FORMAL_BINDING_REQUIRED")
    if loaded is not None:
        if (
            str(loaded.portfolio_id) != job.scope_id
            or loaded.persistence.valuation_time != job.effective_at
        ):
            raise ValueError("OPTIMIZATION_JOB_BINDING_INVALID")
        await build_and_persist_proposal_group(
            context.session,
            actor_id=actor_id,
            idempotency_key=str(job.id),
            request_hash=job.input_hash,
            admission_authority_id=loaded.admission_authority_id,
            research=research,
            artifact_reader=artifact_reader,
        )
        return JobHandlerResult(outcome="succeeded", result_code="PROPOSAL_BUILT")
    inputs = await load_analysis_construction_inputs(
        context.session,
        **payload.model_dump(),
        now=context.finished_at(),
    )
    if (
        inputs is None
        or str(inputs.portfolio_id) != job.scope_id
        or inputs.valuation_time != job.effective_at
    ):
        raise ValueError("OPTIMIZATION_CONSTRUCTION_INPUTS_UNAVAILABLE")
    await create_initial_draft(
        context.session,
        portfolio_snapshot_id=payload.portfolio_snapshot_id,
        diagnostic_report_id=inputs.diagnostic_report_id,
        actor_id=actor_id,
        idempotency_key=str(job.id),
        request_hash=job.input_hash,
        now=context.finished_at(),
        expected_data_snapshot_id=payload.data_snapshot_id,
        artifact_reader=artifact_reader,
    )
    return JobHandlerResult(outcome="succeeded", result_code="ANALYSIS_PROPOSAL_BUILT")


def build_optimization_registry(
    *,
    artifact_reader: ArtifactByteReader | None = None,
) -> JobRegistry:
    """Register construction only; no scheduling, provider, model, or execution capability."""

    async def construct(context: JobHandlerContext) -> JobHandlerResult:
        return await _construct(context, artifact_reader)

    return JobRegistry(
        (
            JobHandlerSpec(
                job_type=JobType.OPTIMIZATION_PROPOSE,
                schema_version="optimization_job_v1",
                payload_model=OptimizationConstructionPayload,
                queue=QueueName.GENERAL,
                timeout_seconds=30,
                retry_policy=RetryPolicy.MANUAL_SAFE,
                max_attempts=3,
                idempotency_namespace="optimization-construction",
                handler=construct,
            ),
        )
    )
