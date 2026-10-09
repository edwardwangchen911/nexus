"""Read exact published authorities; never upgrade a historical model or activate a lane."""

from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import DEADLINE_BUDGET_CONFIG_VERSION
from nexus.agents.persistence import _shared_daily_budget_used
from nexus.agents.portfolio_synthesis_contracts import SYNTHESIS_PROMPT_VERSION
from nexus.agents.portfolio_synthesis_worker import CodexSynthesisTransport
from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.paper.agent_challenger import (
    PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
    PortfolioChallengerPublicationV1,
)
from nexus.paper.universe_scope import is_formal_agent_paper_universe
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord
from nexus.persistence.models.market import UniverseSnapshot
from nexus.persistence.models.paper import AgentPaperCandidateBatchRecord
from nexus.persistence.models.runtime import AuditEvent

PairReason = Literal[
    "CURRENT_MODEL_UNAVAILABLE",
    "MODEL_SPEC_UNSUPPORTED",
    "MODEL_PRICING_UNAVAILABLE",
    "PUBLICATION_UNAVAILABLE",
    "PROMPT_PAIR_INVALID",
    "PUBLICATION_MODEL_UNAVAILABLE",
    "MODEL_REVISION_MISMATCH",
    "VERSIONS_MATCH_AT_OBSERVATION",
]


class PairInspection(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    reason_code: PairReason
    current_model_version: str | None
    publication_model_version: str | None
    configured_research_cost_usd: Decimal | None
    activation_permitted: Literal[False] = False
    economic_status: Literal["not_evaluated"] = "not_evaluated"


def inspect_pair(
    current: AgentModelConfigRecord | None,
    incumbent: AgentPromptVersionRecord | None,
    challenger: AgentPromptVersionRecord | None,
    publication: PortfolioChallengerPublicationV1 | None,
    *,
    published_model: AgentModelConfigRecord | None,
    now: datetime,
) -> PairInspection:
    def result(code: PairReason, cost: Decimal | None = None) -> PairInspection:
        return PairInspection(
            reason_code=code,
            current_model_version=None if current is None else current.version,
            publication_model_version=None if published_model is None else published_model.version,
            configured_research_cost_usd=cost,
        )

    if current is None:
        return result("CURRENT_MODEL_UNAVAILABLE")
    if current.version != DEADLINE_BUDGET_CONFIG_VERSION or not CodexSynthesisTransport().supports(
        current
    ):
        return result("MODEL_SPEC_UNSUPPORTED")
    if not current.pricing_available_at <= now < current.pricing_expires_at:
        return result("MODEL_PRICING_UNAVAILABLE")
    cost = current.worst_case_cost_usd
    if publication is None:
        return result("PUBLICATION_UNAVAILABLE", cost)
    if (
        incumbent is None
        or challenger is None
        or incumbent.id != publication.incumbent_prompt_version_id
        or incumbent.role != "portfolio_decision"
        or incumbent.version != SYNTHESIS_PROMPT_VERSION
        or incumbent.release_channel != "active"
        or challenger.id != publication.challenger_prompt_version_id
        or challenger.role != "portfolio_decision"
        or challenger.release_channel != "challenger"
        or sha256(incumbent.content.encode()).hexdigest() != incumbent.content_hash
        or sha256(challenger.content.encode()).hexdigest() != challenger.content_hash
        or challenger.content_hash != publication.challenger_prompt_hash
        or challenger.content_hash == incumbent.content_hash
    ):
        return result("PROMPT_PAIR_INVALID", cost)
    if published_model is None or published_model.id != publication.challenger_model_config_id:
        return result("PUBLICATION_MODEL_UNAVAILABLE", cost)
    if published_model.id != current.id or published_model.config_hash != current.config_hash:
        return result("MODEL_REVISION_MISMATCH", cost)
    return result("VERSIONS_MATCH_AT_OBSERVATION", cost)


class EconomicAuthorityInspection(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    as_of: datetime
    formal_batch_status: Literal["recognized_scope", "unavailable"]
    formal_candidate_count: int | None
    pair: PairInspection
    configured_global_daily_limit_usd: Decimal
    shared_used_or_reserved_usd: Decimal
    atomic_evaluation_budget_reserved: Literal[False] = False
    common_execution_window_verified: Literal[False] = False
    execution_locked: Literal[True] = True


async def inspect_authorities(
    session: AsyncSession,
    *,
    batch_id: UUID,
    proposal_hash: str,
    daily_limit_usd: Decimal,
) -> EconomicAuthorityInspection:
    if len(proposal_hash) != 64 or any(c not in "0123456789abcdef" for c in proposal_hash):
        raise ValueError("ECONOMIC_AUTHORITY_PROPOSAL_INVALID")
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    current_rows = tuple(
        await session.scalars(
            select(AgentModelConfigRecord)
            .where(
                AgentModelConfigRecord.version == DEADLINE_BUDGET_CONFIG_VERSION,
                AgentModelConfigRecord.created_at <= now,
            )
            .limit(2)
        )
    )
    incumbent_rows = tuple(
        await session.scalars(
            select(AgentPromptVersionRecord)
            .where(
                AgentPromptVersionRecord.role == "portfolio_decision",
                AgentPromptVersionRecord.version == SYNTHESIS_PROMPT_VERSION,
                AgentPromptVersionRecord.created_at <= now,
            )
            .limit(2)
        )
    )
    events = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
                AuditEvent.payload["proposal_hash"].as_string() == proposal_hash,
                AuditEvent.occurred_at <= now,
                AuditEvent.created_at <= now,
            )
            .limit(2)
        )
    )
    publication = None
    challenger = published_model = None
    if len(events) == 1:
        try:
            event = events[0]
            canonical = canonical_audit_event(
                AuditChainInput(
                    event_seq=event.event_seq,
                    occurred_at=event.occurred_at,
                    actor_id=event.actor_id,
                    action=event.action,
                    schema_version=event.schema_version,
                    payload=event.payload,
                )
            )
            if (
                canonical != event.canonical_event
                or audit_event_hash(event.previous_hash, canonical) != event.event_hash
                or event.schema_version != "portfolio_challenger_pub_v1"
            ):
                raise ValueError("ECONOMIC_AUTHORITY_PUBLICATION_INVALID")
            publication = PortfolioChallengerPublicationV1.model_validate(event.payload)
            challenger = await session.get(
                AgentPromptVersionRecord, publication.challenger_prompt_version_id
            )
            published_model = await session.get(
                AgentModelConfigRecord, publication.challenger_model_config_id
            )
            if challenger is not None and challenger.created_at > now:
                challenger = None
            if published_model is not None and published_model.created_at > now:
                published_model = None
        except ValueError:
            publication = None
    batch = await session.get(AgentPaperCandidateBatchRecord, batch_id)
    universe = (
        None if batch is None else await session.get(UniverseSnapshot, batch.universe_snapshot_id)
    )
    recognized = bool(
        batch is not None
        and universe is not None
        and batch.created_at <= now
        and universe.created_at <= now
        and universe.published_at is not None
        and universe.published_at <= now
        and is_formal_agent_paper_universe(universe)
    )
    return EconomicAuthorityInspection(
        as_of=now,
        formal_batch_status="recognized_scope" if recognized else "unavailable",
        formal_candidate_count=len(batch.candidate_instrument_ids)
        if recognized and batch is not None
        else None,
        pair=inspect_pair(
            current_rows[0] if len(current_rows) == 1 else None,
            incumbent_rows[0] if len(incumbent_rows) == 1 else None,
            challenger,
            publication,
            published_model=published_model,
            now=now,
        ),
        configured_global_daily_limit_usd=daily_limit_usd,
        shared_used_or_reserved_usd=await _shared_daily_budget_used(
            session, business_date=now.date()
        ),
    )
