"""Read experimental frozen context without manufacturing a business request."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.comparison_inputs import _hash, _validate
from nexus.agents.comparison_manifest import ACTORS, PURPOSES
from nexus.agents.comparison_routes import comparison_owner
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.runtime import Job


async def require_comparison_observation_basis(
    session: AsyncSession, run: AgentResearchRunRecord, observation_ids: tuple[UUID, ...]
) -> None:
    """Resolve an exact experimental wrapper back to its frozen observation basis."""
    from nexus.agents.research_only import require_research_only_snapshot
    from nexus.persistence.models.research import ResearchEvidenceRef

    invalid = "COMPARISON_OBSERVATION_BASIS_INVALID"
    if await comparison_context_qualified(session, run) is not True:
        raise ValueError(invalid)
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    if snapshot is None or not isinstance(snapshot.payload.get("frozen"), dict):
        raise ValueError(invalid)
    frozen = snapshot.payload["frozen"]
    assert isinstance(frozen, dict)
    context = _validate(frozen)
    basis = await session.get(DataSnapshot, context.source_snapshot_id)
    require_research_only_snapshot(basis)
    assert basis is not None
    ids = basis.payload.get("evidence_ids")
    allowed = {
        row["id"]
        for row in run.evidence_manifest
        if row["source"] != "nexus_independent_monitor_research"
    }
    if (
        basis.revision != context.source_snapshot_hash
        or basis.provider_object_id != str(run.instrument_id)
        or basis.observed_at != run.decision_time
        or not isinstance(ids, list)
        or not all(isinstance(value, str) for value in ids)
        or len(set(ids)) != len(ids)
        or not set(ids).issubset(allowed)
        or not {str(value) for value in observation_ids}.issubset(ids)
    ):
        raise ValueError(invalid)
    originals = set(
        await session.scalars(
            select(ResearchEvidenceRef.id).where(
                ResearchEvidenceRef.id.in_([UUID(value) for value in ids]),
                ResearchEvidenceRef.source == "research_observation_identity",
            )
        )
    )
    if originals != set(observation_ids):
        raise ValueError(invalid)


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
