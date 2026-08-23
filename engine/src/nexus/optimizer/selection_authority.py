"""Load only a DB-proven frozen baseline_v1 selection authority."""

from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.optimizer.strategy_target import (
    DeterministicSelectionAuthority,
    SelectionCandidate,
)
from nexus.persistence.models.optimization import BaselineSelectionAuthorityRecord


async def load_selection_authority(
    session: AsyncSession,
    selection_authority_id: UUID,
    *,
    data_snapshot_id: UUID,
    valuation_time: datetime,
) -> DeterministicSelectionAuthority:
    """Project a strict immutable record only when it predates the valuation."""
    record = await session.get(BaselineSelectionAuthorityRecord, selection_authority_id)
    if (
        record is None
        or record.data_snapshot_id != data_snapshot_id
        or record.available_at > valuation_time
    ):
        raise ValueError("baseline selection authority is missing or unavailable")
    try:
        candidates = tuple(
            SelectionCandidate(
                UUID(cast(str, item["instrument_id"])),
                tuple(cast(list[str], item["provenance_refs"])),
            )
            for item in record.candidates
        )
        authority = DeterministicSelectionAuthority(
            policy_version=record.policy_version,
            decision_at=record.decision_at,
            data_snapshot_id=record.data_snapshot_id,
            available_at=record.available_at,
            evidence_refs=tuple(record.evidence_refs),
            candidates=candidates,
            selected_instrument_ids=tuple(UUID(value) for value in record.selected_instrument_ids),
            evidence_hash=record.evidence_hash,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("baseline selection authority is malformed") from error
    if authority.decision_at > valuation_time:
        raise ValueError("baseline selection decision is after valuation")
    return authority
