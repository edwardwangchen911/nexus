"""Read exact SEC index differences without collection or investment interpretation."""

from datetime import datetime, timedelta
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.monitoring.contracts import DisclosureReviewCondition
from nexus.monitoring.disclosure_contracts import DisclosureMonitoringProposal
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.research.sec_disclosure_index import SecDisclosureIndex, SecIndexEntry
from nexus.research.sec_exhibit_storage import ExhibitStore, configured_exhibit_store
from nexus.research.sec_index_storage import SOURCE, IndexCapture, read_index_capture


class DisclosureObservation(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    instrument_id: UUID
    plan_id: UUID
    baseline_snapshot_id: UUID
    baseline_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    current_snapshot_id: UUID | None
    current_hash: str | None = Field(pattern=r"^[0-9a-f]{64}$")
    issuer_cik: str | None = Field(pattern=r"^[0-9]{10}$")
    baseline_as_of: datetime
    received_at: datetime | None
    available_at: datetime | None
    result: Literal["true", "false", "unknown"]
    reason: str | None = Field(max_length=64)
    history_coverage: Literal["complete", "partial", "unknown"]
    added: tuple[SecIndexEntry, ...] = Field(max_length=1000)

    @field_validator("baseline_as_of", "received_at", "available_at")
    @classmethod
    def utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)

    @model_validator(mode="after")
    def coherent_result(self) -> Self:
        current = (self.current_snapshot_id, self.current_hash, self.received_at, self.available_at)
        if any(value is None for value in current) and any(value is not None for value in current):
            raise ValueError("SEC_OBSERVATION_INVALID")
        if self.result == "unknown":
            if self.added or not self.reason:
                raise ValueError("SEC_OBSERVATION_INVALID")
        elif (
            self.reason is not None
            or self.current_snapshot_id is None
            or self.issuer_cik is None
            or self.history_coverage == "unknown"
            or (self.result == "true") != bool(self.added)
            or self.result == "false"
            and self.history_coverage != "complete"
        ):
            raise ValueError("SEC_OBSERVATION_INVALID")
        if len({item.accession_number for item in self.added}) != len(self.added):
            raise ValueError("SEC_OBSERVATION_INVALID")
        return self


async def _bound_proposal(
    session: AsyncSession, plan: MonitorPlan, now: datetime
) -> DisclosureMonitoringProposal:
    try:
        if (
            plan.kind != "disclosure"
            or plan.baseline_snapshot_id is None
            or plan.baseline_as_of is None
        ):
            raise ValueError
        condition = DisclosureReviewCondition.model_validate(plan.condition)
        output = await session.get(AgentResearchOutputRecord, plan.source_output_id)
        run = await session.get(AgentResearchRunRecord, plan.source_run_id)
        if (
            condition.semantic_hash != plan.condition_hash
            or output is None
            or run is None
            or output.status != "completed"
            or output.role is not None
            or output.run_id != run.id
            or run.status != "completed"
            or output.content_hash != plan.source_hash
            or not isinstance(output.intent, dict)
            or run.instrument_id != plan.instrument_id
            or run.data_snapshot_id != plan.data_snapshot_id
            or run.decision_time != plan.baseline_as_of
            or max(output.created_at, run.created_at, run.decision_time) > now
            or not plan.baseline_as_of <= plan.effective_at < plan.review_due_at <= plan.valid_until
        ):
            raise ValueError
        proposal = DisclosureMonitoringProposal.model_validate(
            output.intent.get("disclosure_proposal")
        )
        if (
            proposal.condition != condition
            or proposal.baseline_snapshot_id != plan.baseline_snapshot_id
            or proposal.baseline_as_of != plan.baseline_as_of
            or proposal.review_due_at != plan.review_due_at
            or proposal.valid_until != plan.valid_until
        ):
            raise ValueError
        return proposal
    except SQLAlchemyError:
        raise
    except Exception:
        raise ValueError("MONITOR_DISCLOSURE_PLAN_INVALID") from None


async def _capture(
    session: AsyncSession, store: ExhibitStore, *, snapshot_id: UUID, as_of: datetime
) -> tuple[DataSnapshot, SecDisclosureIndex]:
    row, index = await read_index_capture(session, store, snapshot_id=snapshot_id, as_of=as_of)
    for binding in IndexCapture.model_validate(row.payload).originals:
        artifact = await session.get(DataArtifact, binding.artifact_id)
        if (
            artifact is None
            or max(
                artifact.observed_at,
                artifact.available_at,
                artifact.received_at,
                artifact.created_at,
            )
            > as_of
        ):
            raise ValueError("SEC_INDEX_ARTIFACT_INVALID")
    return row, index


async def read_disclosure_observation(
    session: AsyncSession, *, plan: MonitorPlan, now: datetime
) -> DisclosureObservation:
    now = require_utc(now)
    proposal = await _bound_proposal(session, plan, now)
    values = dict(
        instrument_id=plan.instrument_id,
        plan_id=plan.id,
        baseline_snapshot_id=proposal.baseline_snapshot_id,
        baseline_hash=proposal.baseline_hash,
        baseline_as_of=proposal.baseline_as_of,
        issuer_cik=proposal.issuer_cik,
        current_snapshot_id=None,
        current_hash=None,
        received_at=None,
        available_at=None,
        history_coverage="unknown",
        added=(),
    )

    def unknown(reason: str) -> DisclosureObservation:
        return DisclosureObservation.model_validate(
            {**values, "result": "unknown", "reason": reason}
        )

    try:
        store = configured_exhibit_store(session)
        baseline_row, baseline = await _capture(
            session, store, snapshot_id=proposal.baseline_snapshot_id, as_of=proposal.baseline_as_of
        )
        if (
            baseline_row.revision != proposal.baseline_hash
            or baseline.instrument_id != plan.instrument_id
            or baseline.issuer_cik != proposal.issuer_cik
        ):
            return unknown("SEC_BASELINE_INVALID")
    except SQLAlchemyError:
        raise
    except Exception:
        return unknown("SEC_BASELINE_INVALID")

    candidate = await session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.source == SOURCE,
            or_(
                DataSnapshot.provider_object_id.startswith(f"{plan.instrument_id}:"),
                DataSnapshot.payload["index"]["instrument_id"].astext == str(plan.instrument_id),
            ),
            DataSnapshot.observed_at <= now,
            DataSnapshot.available_at <= now,
            DataSnapshot.received_at <= now,
            DataSnapshot.created_at <= now,
        )
        .order_by(
            DataSnapshot.observed_at.desc(),
            DataSnapshot.available_at.desc(),
            DataSnapshot.received_at.desc(),
            DataSnapshot.created_at.desc(),
            DataSnapshot.id.desc(),
        )
        .limit(1)
    )
    if candidate is None:
        return unknown("SEC_INDEX_CAPTURE_UNAVAILABLE")
    try:
        current_row, current = await _capture(session, store, snapshot_id=candidate.id, as_of=now)
        if (current.instrument_id, current.issuer_cik) != (
            baseline.instrument_id,
            baseline.issuer_cik,
        ):
            return unknown("SEC_INDEX_IDENTITY_INVALID")
    except SQLAlchemyError:
        raise
    except Exception:
        return unknown("SEC_INDEX_CAPTURE_INVALID")

    values.update(
        current_snapshot_id=current_row.id,
        current_hash=current_row.revision,
        received_at=current.received_at,
        available_at=current_row.available_at,
        history_coverage="complete" if current.history_complete else "partial",
    )
    if current.received_at < baseline.received_at or now - current.received_at > timedelta(
        seconds=300
    ):
        return unknown("SEC_INDEX_STALE")
    previous = {entry.accession_number: entry for entry in baseline.entries}
    if any(
        entry.accession_number in previous and previous[entry.accession_number] != entry
        for entry in current.entries
    ):
        return unknown("SEC_INDEX_EVENT_CONFLICT")
    added = tuple(entry for entry in current.entries if entry.accession_number not in previous)
    if not added and not current.history_complete:
        return unknown("SEC_INDEX_COVERAGE_UNAVAILABLE")
    values["added"] = added
    return DisclosureObservation.model_validate(
        {**values, "result": "true" if added else "false", "reason": None}
    )
