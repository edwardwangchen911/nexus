"""Freeze a verified SEC index baseline at research T0, without collecting anything."""

from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.research.sec_index_storage import SOURCE, IndexCapture, read_index_capture


class FrozenDisclosureBaseline(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    schema_version: Literal["sec_monitor_baseline_v1"] = "sec_monitor_baseline_v1"
    instrument_id: UUID
    snapshot_id: UUID
    snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    research_as_of: datetime

    @field_validator("research_as_of")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @field_validator("issuer_cik")
    @classmethod
    def nonzero_cik(cls, value: str) -> str:
        if int(value) == 0:
            raise ValueError("SEC_BASELINE_INVALID")
        return value


class DisclosureMonitoringContext(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    schema_version: Literal["disclosure_monitor_context_v1"] = "disclosure_monitor_context_v1"
    instrument_id: UUID
    research_as_of: datetime
    baseline: FrozenDisclosureBaseline | None
    gap_reason: Literal["SEC_BASELINE_MISSING", "SEC_BASELINE_INVALID"] | None

    @field_validator("research_as_of")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def bound_baseline_or_gap(self) -> Self:
        if (self.baseline is None) == (self.gap_reason is None):
            raise ValueError("SEC_BASELINE_INVALID")
        if self.baseline is not None and (
            self.baseline.instrument_id != self.instrument_id
            or self.baseline.research_as_of != self.research_as_of
        ):
            raise ValueError("SEC_BASELINE_INVALID")
        return self

    @property
    def ready(self) -> bool:
        return self.baseline is not None and self.gap_reason is None


async def _exact_baseline(
    session: AsyncSession, *, snapshot_id: UUID, instrument_id: UUID, as_of: datetime
) -> FrozenDisclosureBaseline:
    row, index = await read_index_capture(
        session, configured_exhibit_store(session), snapshot_id=snapshot_id, as_of=as_of
    )
    if index.instrument_id != instrument_id:
        raise ValueError("SEC_BASELINE_INVALID")
    # The shared reader verifies hashes, original receipts, issuer mapping and
    # local custody. Include original observation time in this T0 boundary too.
    capture = IndexCapture.model_validate(row.payload)
    for binding in capture.originals:
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
            raise ValueError("SEC_BASELINE_INVALID")
    return FrozenDisclosureBaseline(
        instrument_id=instrument_id,
        snapshot_id=row.id,
        snapshot_hash=row.revision,
        issuer_cik=index.issuer_cik,
        research_as_of=as_of,
    )


async def freeze_disclosure_baseline(
    session: AsyncSession, *, instrument_id: UUID, as_of: datetime
) -> DisclosureMonitoringContext:
    """Select newest visible network capture once; a broken newest capture is a gap."""
    as_of = require_utc(as_of)
    candidate = await session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.source == SOURCE,
            or_(
                DataSnapshot.provider_object_id.startswith(f"{instrument_id}:"),
                DataSnapshot.payload["index"]["instrument_id"].astext == str(instrument_id),
            ),
            DataSnapshot.observed_at <= as_of,
            DataSnapshot.available_at <= as_of,
            DataSnapshot.received_at <= as_of,
            DataSnapshot.created_at <= as_of,
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
    baseline = None
    reason: Literal["SEC_BASELINE_MISSING", "SEC_BASELINE_INVALID"] | None = "SEC_BASELINE_MISSING"
    if candidate is not None:
        try:
            baseline = await _exact_baseline(
                session, snapshot_id=candidate.id, instrument_id=instrument_id, as_of=as_of
            )
            reason = None
        except SQLAlchemyError:
            raise
        except Exception:
            # Store failures and malformed archives expose only a fixed gap;
            # cancellation propagates and no older candidate is retried.
            reason = "SEC_BASELINE_INVALID"
    return DisclosureMonitoringContext(
        instrument_id=instrument_id, research_as_of=as_of, baseline=baseline, gap_reason=reason
    )


async def validate_disclosure_baseline(
    session: AsyncSession, context: DisclosureMonitoringContext
) -> bool:
    """Validate the frozen package, not readiness; an explicit gap remains a gap."""
    try:
        context = DisclosureMonitoringContext.model_validate(context.model_dump())
        if not context.ready:
            return True
        assert context.baseline is not None
        return context.baseline == await _exact_baseline(
            session,
            snapshot_id=context.baseline.snapshot_id,
            instrument_id=context.instrument_id,
            as_of=context.research_as_of,
        )
    except SQLAlchemyError:
        raise
    except Exception:
        return False
