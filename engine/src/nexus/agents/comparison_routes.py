"""Read exact experimental ownership; no claims, credential use or send authority."""

from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.comparison_manifest import ACTORS, parse_comparison_manifest
from nexus.jobs.models import job_input_hash
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.runtime import Job


async def comparison_owner(
    session: AsyncSession, run: AgentResearchRunRecord, job: Job
) -> UUID | None:
    if run.actor_id not in ACTORS and job.job_type != "agent.research.comparison":
        return None
    row = (
        (
            await session.execute(
                text("""SELECT m.id,m.payload,m.manifest_hash,
              research_comparison_binding(:run,:job,m.id) AS bound
              FROM research_comparison_chain c JOIN research_comparison_manifest m
              ON m.id=c.experiment_id WHERE c.run_id=:run AND c.job_id=:job"""),
                {"run": run.id, "job": job.id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or not row["bound"] or job.input_hash != job_input_hash(job.payload):
        raise ValueError("COMPARISON_NATIVE_BINDING_INVALID")
    manifest = parse_comparison_manifest(row["payload"])
    if manifest.experiment_id != row["id"] or manifest.digest != row["manifest_hash"]:
        raise ValueError("COMPARISON_REGISTRATION_CONFLICT")
    return manifest.experiment_id
