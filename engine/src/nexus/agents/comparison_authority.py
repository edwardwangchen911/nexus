"""Read experimental frozen context without manufacturing a business request."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.comparison_inputs import _hash, _validate
from nexus.agents.comparison_manifest import ACTORS, PURPOSES
from nexus.agents.comparison_routes import comparison_owner
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.runtime import Job


async def comparison_context_qualified(
    session: AsyncSession, run: AgentResearchRunRecord
) -> bool | None:
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    marked = (
        run.actor_id in ACTORS
        or snapshot is not None
        and (
            snapshot.source == "nexus_research_comparison"
            or snapshot.schema_version == "comparison-input-v1"
            or snapshot.payload.get("purpose") in PURPOSES
        )
    )
    if not marked:
        return None
    if snapshot is None or run.actor_id not in ACTORS:
        return False
    jobs = list(
        await session.scalars(select(Job).where(Job.payload["run_id"].astext == str(run.id)))
    )
    if len(jobs) != 1:
        return False
    try:
        experiment = await comparison_owner(session, run, jobs[0])
        payload = snapshot.payload
        frozen = payload["frozen"]
        if not isinstance(frozen, dict):
            return False
        context = _validate(frozen)
        return bool(
            experiment is not None
            and payload["experiment_id"] == str(experiment)
            and snapshot.source == "nexus_research_comparison"
            and snapshot.schema_version == "comparison-input-v1"
            and snapshot.revision == _hash(payload)
            and _hash(frozen) == run.input_hash
            and context.instrument_id == run.instrument_id
            and context.decision_time == run.decision_time
            and frozen["request"]["data_snapshot_id"] == str(run.data_snapshot_id)
        )
    except (KeyError, TypeError, ValueError):
        return False
