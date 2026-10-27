"""Immutable prompt authority for non-trading independent monitor research."""

from collections.abc import Mapping
from hashlib import sha256
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import ROLE_ORDER, AgentRole
from nexus.persistence.models.agents import AgentPromptVersionRecord

PROMPT_VERSION = "independent-monitor-research-v1"
INSTRUCTIONS = (
    "Root purpose: independent_monitor_research_v1. Research the single security using only "
    "observations available at the frozen cutoff and their original dated scope. "
    "Observation identity is not current listing or trading permission. Historical "
    "prices retain their session, currency, adjustment and original source times; they "
    "are not live L1 quotes or current trigger evaluations. Missing portfolio context "
    "is not a company evidence gap. Perform the research, critique and single-security "
    "synthesis stages, preserving evidence audit veto. The portfolio role synthesizes "
    "the security and adopts only exactly hashed, independently audited price or SEC "
    "conditions. Each kind is optional and retains its own evidence requirements and "
    "expiry; never invent a condition to fill a coverage gap. Do not infer that a "
    "split-adjusted historical price is comparable to an unadjusted live threshold. "
    "The independent_monitor_context freezes the request, full candidate set, original "
    "snapshot and explicit per-kind predecessors. The independent root grants no daily "
    "or mixed pending event consumption. A monitoring descendant may review and consume "
    "only events explicitly bound by its current frozen monitor_context; inherited root "
    "research never authorizes consuming unrelated or historical pending events. "
    "Do not submit daily_pending proposals. No portfolio allocation, "
    "target weights, orders or execution permission; watch remains defer, and this "
    "research and its monitoring descendants are forbidden formal decision sources."
)


def prompts_valid(prompts: Mapping[AgentRole, AgentPromptVersionRecord]) -> bool:
    return set(prompts) == set(ROLE_ORDER) and all(
        p.version == PROMPT_VERSION
        and p.content.endswith(INSTRUCTIONS)
        and sha256(p.content.encode()).hexdigest() == p.content_hash
        for p in prompts.values()
    )


async def ensure_prompts(
    session: AsyncSession, *, orchestrator_id: UUID | None = None
) -> dict[AgentRole, AgentPromptVersionRecord]:
    from nexus.agents.committee import DISCLOSURE_SCOPE_PROMPT_VERSION
    from nexus.agents.persistence import load_complete_prompt_authority

    base = await load_complete_prompt_authority(session, orchestrator_id=orchestrator_id)
    if base is None:
        raise ValueError("INDEPENDENT_MONITOR_PROMPT_UNAVAILABLE")
    if prompts_valid(base):
        return base
    if base["portfolio"].version != DISCLOSURE_SCOPE_PROMPT_VERSION:
        raise ValueError("INDEPENDENT_MONITOR_PROMPT_UNAVAILABLE")
    contents = {role: base[role].content + "\n\n" + INSTRUCTIONS for role in ROLE_ORDER}
    for role in ROLE_ORDER:
        await session.execute(
            insert(AgentPromptVersionRecord)
            .values(
                role=role,
                version=PROMPT_VERSION,
                content=contents[role],
                content_hash=sha256(contents[role].encode()).hexdigest(),
                release_channel="challenger",
            )
            .on_conflict_do_nothing(index_elements=["role", "version"])
        )
    records = list(
        await session.scalars(
            select(AgentPromptVersionRecord).where(
                AgentPromptVersionRecord.version == PROMPT_VERSION
            )
        )
    )
    result = {role: next(p for p in records if p.role == role) for role in ROLE_ORDER}
    if not prompts_valid(result) or any(
        result[role].content != contents[role] for role in ROLE_ORDER
    ):
        raise ValueError("INDEPENDENT_MONITOR_PROMPT_INVALID")
    return result
