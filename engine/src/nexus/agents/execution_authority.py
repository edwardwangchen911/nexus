"""Frozen prompt, evidence, and instrument authority for durable research runs."""

import hashlib
import json
from collections.abc import Mapping
from typing import cast
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import ROLE_ORDER, AgentRole
from nexus.persistence.models.agents import AgentPromptVersionRecord, AgentResearchRunRecord
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.current_price_trends_storage import read_current_price_trends
from nexus.research.news_evidence import read_news_evidence

MAX_FROZEN_EVIDENCE = 100


async def load_frozen_evidence(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    allow_fixture_evidence: bool,
) -> tuple[ResearchEvidenceRef, ...]:
    manifest = run.evidence_manifest
    if not isinstance(manifest, list) or not manifest or len(manifest) > MAX_FROZEN_EVIDENCE:
        return ()
    canonical_hash = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if canonical_hash != run.evidence_hash:
        return ()
    evidence_ref_ids: list[UUID] = []
    try:
        for item in manifest:
            if not isinstance(item, dict):
                return ()
            evidence_ref_ids.append(UUID(cast(str, item.get("id"))))
    except (TypeError, ValueError, AttributeError):
        return ()
    if len(set(evidence_ref_ids)) != len(evidence_ref_ids):
        return ()
    records = list(
        await session.scalars(
            select(ResearchEvidenceRef).where(ResearchEvidenceRef.id.in_(evidence_ref_ids))
        )
    )
    by_id = {record.id: record for record in records}
    if set(by_id) != set(evidence_ref_ids):
        return ()
    ordered = tuple(by_id[evidence_id] for evidence_id in evidence_ref_ids)
    if any(
        record.available_at > run.decision_time
        or (record.source == "fixture" and not allow_fixture_evidence)
        or not _evidence_matches_manifest(record, manifest[index])
        for index, record in enumerate(ordered)
    ):
        return ()
    trend_ids = tuple(record.id for record in ordered if record.source == "alpaca_current_trends")
    if trend_ids and set(
        await read_current_price_trends(
            session,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
            evidence_ids=trend_ids,
        )
    ) != set(trend_ids):
        return ()
    news_ids = tuple(record.id for record in ordered if record.source == "alpaca_news")
    if news_ids and set(
        await read_news_evidence(
            session,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
            evidence_ids=news_ids,
        )
    ) != set(news_ids):
        return ()
    return ordered


async def load_frozen_prompts(
    session: AsyncSession,
    run: AgentResearchRunRecord,
) -> Mapping[AgentRole, AgentPromptVersionRecord] | None:
    manifest = run.prompt_manifest
    if not isinstance(manifest, dict) or set(manifest) != set(ROLE_ORDER):
        return None
    if _hash(cast(Mapping[str, object], manifest)) != run.prompt_hash:
        return None
    prompt_ids: list[UUID] = []
    try:
        for role in ROLE_ORDER:
            item = manifest[role]
            if not isinstance(item, dict):
                return None
            prompt_ids.append(UUID(cast(str, item.get("id"))))
    except (TypeError, ValueError, AttributeError):
        return None
    records = list(
        await session.scalars(
            select(AgentPromptVersionRecord).where(AgentPromptVersionRecord.id.in_(prompt_ids))
        )
    )
    by_id = {record.id: record for record in records}
    prompts: dict[AgentRole, AgentPromptVersionRecord] = {}
    for role, prompt_id in zip(ROLE_ORDER, prompt_ids, strict=True):
        record = by_id.get(prompt_id)
        item = manifest[role]
        if (
            record is None
            or record.role != role
            or not isinstance(item, dict)
            or item.get("version") != record.version
            or item.get("content_hash") != record.content_hash
            or hashlib.sha256(record.content.encode()).hexdigest() != record.content_hash
        ):
            return None
        prompts[role] = record
    return prompts


async def load_frozen_symbol(session: AsyncSession, run: AgentResearchRunRecord) -> str | None:
    value = await session.scalar(
        select(InstrumentAlias.symbol)
        .where(
            InstrumentAlias.instrument_id == run.instrument_id,
            InstrumentAlias.valid_from <= run.decision_time.date(),
            or_(
                InstrumentAlias.valid_to.is_(None),
                InstrumentAlias.valid_to >= run.decision_time.date(),
            ),
        )
        .order_by(InstrumentAlias.valid_from.desc(), InstrumentAlias.symbol)
        .limit(1)
    )
    return cast(str | None, value)


def _evidence_matches_manifest(record: ResearchEvidenceRef, item: dict[str, object]) -> bool:
    return item == {
        "id": str(record.id),
        "source": record.source,
        "provider_object_id": record.provider_object_id,
        "revision": record.revision,
        "available_at": record.available_at.isoformat().replace("+00:00", "Z"),
        "location": record.location,
        "content_hash": record.content_hash,
    }


def _hash(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
