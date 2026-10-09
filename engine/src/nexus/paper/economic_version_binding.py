"""Freeze exact same-model prompt identities, without publishing or activating them."""

from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import Field
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import DEADLINE_BUDGET_CONFIG_VERSION
from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.paper.agent_challenger import (
    PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
    PortfolioChallengerPublicationV1,
)
from nexus.paper.economic_authorities import inspect_authorities
from nexus.paper.portfolio_economic_preflight import StrictModel, _hash
from nexus.paper.portfolio_economic_protocol import read_protocol, require_isolated_database
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord
from nexus.persistence.models.runtime import AuditEvent

ACTION = "portfolio.economic.versions_bound"
SCHEMA = "portfolio_economic_binding_v1"
ACTOR = "local-economic-version-binding-cli"


class VersionBinding(StrictModel):
    schema_version: Literal["portfolio_economic_binding_v1"] = "portfolio_economic_binding_v1"
    status: Literal["versions_bound_not_activated"] = "versions_bound_not_activated"
    protocol_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    publication_event_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    shared_model_id: UUID
    shared_model_version: str
    shared_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    incumbent_prompt_id: UUID
    incumbent_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenger_prompt_id: UUID
    challenger_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    pricing_available_at: datetime
    pricing_expires_at: datetime
    configured_cost_usd: Decimal = Field(ge=0, allow_inf_nan=False)
    activation_permitted: Literal[False] = False
    economic_status: Literal["not_evaluated"] = "not_evaluated"
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


def decode_binding(event: AuditEvent) -> VersionBinding:
    binding = VersionBinding.model_validate(event.payload)
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
        event.action != ACTION
        or event.schema_version != "portfolio_economic_binding_v1"
        or event.actor_id != ACTOR
        or canonical != event.canonical_event
        or audit_event_hash(event.previous_hash, canonical) != event.event_hash
        or _hash(binding.model_dump(mode="json", exclude={"content_hash"})) != binding.content_hash
        or binding.incumbent_prompt_id == binding.challenger_prompt_id
        or binding.incumbent_prompt_hash == binding.challenger_prompt_hash
        or not binding.pricing_available_at <= event.occurred_at < binding.pricing_expires_at
    ):
        raise ValueError("ECONOMIC_VERSION_BINDING_INVALID")
    return binding


async def bind_versions(
    session: AsyncSession,
    *,
    protocol_hash: str,
    proposal_hash: str,
    expected_database: str,
) -> AuditEvent:
    await require_isolated_database(session, expected=expected_database)
    for value in (protocol_hash, proposal_hash):
        if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("ECONOMIC_VERSION_BINDING_INPUT_INVALID")
    # Same singleton lock as protocol registration: no parallel protocol/version mutation.
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock(hashtextextended('portfolio-economic-protocol-v1', 2026))"
        )
    )
    existing = tuple(
        await session.scalars(select(AuditEvent).where(AuditEvent.action == ACTION).limit(2))
    )
    if existing:
        if len(existing) != 1:
            raise ValueError("ECONOMIC_VERSION_BINDING_INVALID")
        frozen = decode_binding(existing[0])
        if frozen.protocol_hash != protocol_hash or frozen.proposal_hash != proposal_hash:
            raise ValueError("ECONOMIC_VERSION_BINDING_CONFLICT")
        return existing[0]
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    _, _, protocol = await read_protocol(session, as_of=now)
    if (
        protocol is None
        or protocol.content_hash != protocol_hash
        or now >= protocol.slots[0].decision_at
    ):
        raise ValueError("ECONOMIC_VERSION_PROTOCOL_UNAVAILABLE")
    observation = await inspect_authorities(
        session,
        batch_id=UUID(int=0),
        proposal_hash=proposal_hash,
        daily_limit_usd=protocol.request.global_daily_limit_usd,
    )
    if observation.pair.reason_code != "VERSIONS_MATCH_AT_OBSERVATION":
        raise ValueError(observation.pair.reason_code)
    # The inspector established uniqueness and validated exact publication integrity.
    event = await session.scalar(
        select(AuditEvent).where(
            AuditEvent.action == PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
            AuditEvent.payload["proposal_hash"].as_string() == proposal_hash,
            AuditEvent.occurred_at <= observation.as_of,
            AuditEvent.created_at <= observation.as_of,
        )
    )
    assert event is not None
    publication = PortfolioChallengerPublicationV1.model_validate(event.payload)
    model = await session.get(AgentModelConfigRecord, publication.challenger_model_config_id)
    incumbent = await session.get(AgentPromptVersionRecord, publication.incumbent_prompt_version_id)
    challenger = await session.get(
        AgentPromptVersionRecord, publication.challenger_prompt_version_id
    )
    assert model is not None and incumbent is not None and challenger is not None
    assert model.version == DEADLINE_BUDGET_CONFIG_VERSION
    if (
        protocol.request.research_cost_usd != model.worst_case_cost_usd
        or protocol.request.synthesis_cost_usd != model.worst_case_cost_usd
    ):
        raise ValueError("ECONOMIC_VERSION_COST_CLAIM_MISMATCH")
    body = dict(
        schema_version=SCHEMA,
        status="versions_bound_not_activated",
        protocol_hash=protocol_hash,
        proposal_hash=proposal_hash,
        publication_event_hash=event.event_hash,
        shared_model_id=str(model.id),
        shared_model_version=model.version,
        shared_model_hash=model.config_hash,
        incumbent_prompt_id=str(incumbent.id),
        incumbent_prompt_hash=incumbent.content_hash,
        challenger_prompt_id=str(challenger.id),
        challenger_prompt_hash=challenger.content_hash,
        pricing_available_at=model.pricing_available_at.isoformat(),
        pricing_expires_at=model.pricing_expires_at.isoformat(),
        configured_cost_usd=str(model.worst_case_cost_usd),
        activation_permitted=False,
        economic_status="not_evaluated",
    )
    # Canonicalize through the typed DTO before hashing dates/Decimal representation.
    preliminary = VersionBinding.model_validate({**body, "content_hash": "0" * 64})
    binding = preliminary.model_copy(
        update={
            "content_hash": _hash(preliminary.model_dump(mode="json", exclude={"content_hash"}))
        }
    )
    return await append_audit_event(
        session,
        actor_id=ACTOR,
        action=ACTION,
        schema_version=SCHEMA,
        payload=binding.model_dump(mode="json"),
        occurred_at=observation.as_of,
    )


async def read_binding(
    session: AsyncSession,
    *,
    as_of: datetime,
) -> tuple[str, VersionBinding | None]:
    events = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == ACTION,
                AuditEvent.occurred_at <= as_of,
                AuditEvent.created_at <= as_of,
            )
            .limit(2)
        )
    )
    if not events:
        return "unbound", None
    try:
        if len(events) != 1:
            raise ValueError("multiple bindings")
        return "versions_bound_not_activated", decode_binding(events[0])
    except ValueError:
        return "unavailable", None
