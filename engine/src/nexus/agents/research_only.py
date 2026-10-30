"""Explicit historical-observation purpose; never formal execution authority."""

import json
from collections.abc import Mapping
from hashlib import sha256
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import ROLE_ORDER, AgentRole
from nexus.persistence.models.agents import AgentPromptVersionRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot

PURPOSE = "research_only_v2"
SOURCE = "nexus_research_only"
OBSERVATION_SOURCE = "research_observation_identity"
PROMPT_PREFIX = "research-only-v2-"
RESEARCH_ONLY_INSTRUCTIONS = (
    "Purpose: research_only_v2. Analyze historical observations available at the frozen "
    "research cutoff. Observation identity establishes a historical security/issuer binding, "
    "not current identity; remote latest status is unverified. Source receipt time is not "
    "source effective time. Do not claim current listing, tradability or current mapping. "
    "No portfolio or Paper context applies to this research. Do not invent holdings, cash, "
    "current quotes or risk authority. Complete research, critique and single-security "
    "synthesis; assess disclosures, trends and uncertainty within their dated scope. "
    "Missing optional macro evidence limits macro conclusions. Missing portfolio context "
    "is not itself a company research defect. No allocation, target weights, plans, orders "
    "or execution permission; execution remains locked. Conditional research stance cannot "
    "be consumed as a formal investment decision."
)


def is_research_only(snapshot: DataSnapshot | None, manifest: object = None) -> bool:
    return bool(
        snapshot is not None
        and (
            snapshot.source == SOURCE
            or snapshot.schema_version == PURPOSE
            or snapshot.payload.get("purpose") == PURPOSE
        )
        or isinstance(manifest, list)
        and any(
            isinstance(item, dict) and item.get("source") == OBSERVATION_SOURCE for item in manifest
        )
    )


def require_research_only_snapshot(snapshot: DataSnapshot | None) -> None:
    if (
        snapshot is None
        or snapshot.source != SOURCE
        or snapshot.schema_version != PURPOSE
        or snapshot.payload.get("purpose") != PURPOSE
        or "portfolio_context" in snapshot.payload
        or "agent_paper_context" in snapshot.payload
        or snapshot.payload.get("as_of") != snapshot.observed_at.isoformat()
        or snapshot.revision
        != sha256(json.dumps(snapshot.payload, sort_keys=True).encode()).hexdigest()
    ):
        raise ValueError("RESEARCH_ONLY_SNAPSHOT_INVALID")


async def require_formal_research(session: AsyncSession, run: AgentResearchRunRecord) -> None:
    from nexus.agents.comparison_manifest import require_business_run

    require_business_run(run.actor_id)
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    require_business_run(run.actor_id, snapshot)
    from nexus.monitoring.context import is_monitor_snapshot
    from nexus.monitoring.independent_context import is_independent_snapshot

    if is_independent_snapshot(snapshot, run.evidence_manifest):
        raise ValueError("INDEPENDENT_MONITOR_FORMAL_CONSUMPTION_FORBIDDEN")
    if is_monitor_snapshot(snapshot):
        raise ValueError("MONITOR_FORMAL_CONSUMPTION_FORBIDDEN")
    if is_research_only(snapshot, run.evidence_manifest):
        raise ValueError("RESEARCH_ONLY_FORMAL_CONSUMPTION_FORBIDDEN")


async def ensure_research_only_prompts(
    session: AsyncSession,
    *,
    orchestrator_id: UUID | None = None,
) -> dict[AgentRole, AgentPromptVersionRecord]:
    """Register a new immutable challenger bundle before the shared research cutoff."""
    from nexus.agents.persistence import load_complete_prompt_authority

    base = await load_complete_prompt_authority(session, orchestrator_id=orchestrator_id)
    if base is None:
        raise ValueError("RESEARCH_ONLY_PROMPT_UNAVAILABLE")
    if research_only_prompts_valid(base):
        return base
    contents = {
        role: base[role].content + "\n\n" + RESEARCH_ONLY_INSTRUCTIONS for role in ROLE_ORDER
    }
    digest = sha256(json.dumps(contents, sort_keys=True).encode()).hexdigest()
    version = PROMPT_PREFIX + digest
    for role in ROLE_ORDER:
        await session.execute(
            insert(AgentPromptVersionRecord)
            .values(
                role=role,
                version=version,
                content=contents[role],
                content_hash=sha256(contents[role].encode()).hexdigest(),
                release_channel="challenger",
            )
            .on_conflict_do_nothing(index_elements=["role", "version"])
        )
    records = list(
        await session.scalars(
            select(AgentPromptVersionRecord).where(
                AgentPromptVersionRecord.version == version,
            )
        )
    )
    result = {role: next(row for row in records if row.role == role) for role in ROLE_ORDER}
    if not research_only_prompts_valid(result):
        raise ValueError("RESEARCH_ONLY_PROMPT_INVALID")
    return result


def research_only_prompts_valid(prompts: Mapping[AgentRole, AgentPromptVersionRecord]) -> bool:
    return set(prompts) == set(ROLE_ORDER) and all(
        row.version.startswith(PROMPT_PREFIX)
        and row.content.endswith(RESEARCH_ONLY_INSTRUCTIONS)
        and sha256(row.content.encode()).hexdigest() == row.content_hash
        for row in prompts.values()
    )


def require_observation_snapshot(snapshot: DataSnapshot | None) -> None:
    """Dispatch by exact non-trading purpose; the v2 validator stays unchanged."""
    from nexus.monitoring.independent_context import (
        is_independent_snapshot,
        require_independent_snapshot,
    )

    if is_independent_snapshot(snapshot):
        require_independent_snapshot(snapshot)
    else:
        require_research_only_snapshot(snapshot)
