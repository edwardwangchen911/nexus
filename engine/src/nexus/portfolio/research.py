"""Bulk read-only index connecting held instruments to frozen Agent research."""

from datetime import datetime
from typing import cast
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select, true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from nexus.agents.observation_readback import ObservationReadback, readable_semantics
from nexus.agents.synthesis import STAGED_SEMANTICS_UNCERTAINTY
from nexus.api.schemas.portfolio import PositionResearchResponse, PositionResearchStatus
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.strategy.intents import ResearchIntent


async def read_position_research(
    session: AsyncSession,
    instrument_ids: tuple[UUID, ...],
    *,
    read_at: datetime,
) -> dict[UUID, PositionResearchResponse]:
    """Batch local reads; no model/provider calls, writes, or readiness promotion."""
    if not instrument_ids:
        return {}
    binding = AgentDataSnapshotEvidenceRecord
    inputs = (
        await session.execute(
            select(binding.instrument_id, DataSnapshot.id, DataSnapshot.available_at)
            .join(DataSnapshot, DataSnapshot.id == binding.data_snapshot_id)
            .where(
                binding.instrument_id.in_(instrument_ids),
                binding.authority_kind == "research",
                binding.available_at <= read_at,
                DataSnapshot.available_at <= read_at,
                DataSnapshot.received_at <= read_at,
                DataSnapshot.created_at <= read_at,
            )
            .distinct(binding.instrument_id)
            .order_by(
                binding.instrument_id,
                DataSnapshot.available_at.desc(),
                DataSnapshot.received_at.desc(),
                DataSnapshot.id.desc(),
            )
            .limit(len(instrument_ids))
        )
    ).all()
    latest_inputs = {row.instrument_id: row for row in inputs}
    run = AgentResearchRunRecord
    output = (
        select(AgentResearchOutputRecord)
        .where(
            AgentResearchOutputRecord.run_id == run.id,
            AgentResearchOutputRecord.status == "completed",
            AgentResearchOutputRecord.created_at <= read_at,
        )
        .order_by(AgentResearchOutputRecord.sequence.desc())
        .limit(1)
        .lateral()
    )
    selected_output = aliased(AgentResearchOutputRecord, output)
    records = (
        await session.execute(
            select(run, selected_output)
            .outerjoin(output, true())
            .where(
                run.instrument_id.in_(instrument_ids),
                run.decision_time <= read_at,
                run.created_at <= read_at,
            )
            .distinct(run.instrument_id)
            .order_by(run.instrument_id, run.decision_time.desc(), run.id.desc())
            .limit(len(instrument_ids))
        )
    ).all()
    result = {
        instrument_id: PositionResearchResponse(
            read_at=read_at,
            status="not_started",
            data_snapshot_id=latest_inputs[instrument_id].id
            if instrument_id in latest_inputs
            else None,
            decision_time=latest_inputs[instrument_id].available_at
            if instrument_id in latest_inputs
            else None,
        )
        for instrument_id in instrument_ids
    }
    output_ids = [item.id for _, item in records if item is not None]
    bound: dict[UUID, list[AgentResearchOutputEvidenceRecord]] = {}
    if output_ids:
        for output_binding in await session.scalars(
            select(AgentResearchOutputEvidenceRecord).where(
                AgentResearchOutputEvidenceRecord.output_id.in_(output_ids)
            )
        ):
            bound.setdefault(output_binding.output_id, []).append(output_binding)
    reader = ObservationReadback(session, read_at=read_at)
    await reader.prime([record for record, _ in records])
    for record, selected in records:
        raw_intent = None if selected is None else selected.intent
        intent = None
        if record.status == "completed" and raw_intent is not None:
            try:
                parsed = ResearchIntent.model_validate(raw_intent)
                if (
                    parsed.instrument_id == record.instrument_id
                    and parsed.data_snapshot_id == record.data_snapshot_id
                    and parsed.decision_time == record.decision_time
                ):
                    intent = parsed
            except ValidationError:
                pass
        observation_verified = await reader.verified(
            record, selected, () if selected is None else bound.get(selected.id, ())
        )
        semantics_invalid = intent is not None and not readable_semantics(
            intent, record.pricing_version, observation_verified=observation_verified
        )
        if semantics_invalid:
            intent = None
        latest = latest_inputs.get(record.instrument_id)
        result[record.instrument_id] = PositionResearchResponse(
            read_at=read_at,
            status=cast(PositionResearchStatus, record.status),
            run_id=record.id,
            data_snapshot_id=record.data_snapshot_id,
            decision_time=record.decision_time,
            quality_status="partial"
            if semantics_invalid
            else (intent.quality_status if intent else None),
            summary=intent.theses[0].text[:400] if intent and intent.theses else None,
            uncertainties=(STAGED_SEMANTICS_UNCERTAINTY,)
            if semantics_invalid
            else (tuple(item[:400] for item in intent.uncertainties[:3]) if intent else ()),
            newer_inputs_available=(
                latest is not None
                and latest.id != record.data_snapshot_id
                and latest.available_at > record.decision_time
            ),
        )
    return result
