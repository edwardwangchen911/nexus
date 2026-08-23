"""Real PostgreSQL boundaries for frozen baseline selection authority."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from helpers.portfolio_api import settings
from nexus.optimizer.selection_authority import load_selection_authority
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.optimization import BaselineSelectionAuthorityRecord
from nexus.persistence.session import session_factory
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

_CLOCK = datetime(2026, 8, 24, tzinfo=UTC)
_DATA_ID = UUID("0198d500-1000-7000-8000-000000000a01")
_AUTHORITY_ID = UUID("0198d500-1000-7000-8000-000000000a02")
_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000a03")


async def _seed_data(factory) -> None:
    async with factory() as session, session.begin():
        session.add(
            DataSnapshot(
                id=_DATA_ID,
                source="fixture",
                provider_object_id="selection-authority",
                revision="1",
                observed_at=_CLOCK,
                available_at=_CLOCK,
                received_at=_CLOCK,
                schema_version="1",
                payload={},
            )
        )


def _record(**updates: object) -> BaselineSelectionAuthorityRecord:
    values: dict[str, object] = {
        "id": _AUTHORITY_ID,
        "policy_version": "baseline_v1",
        "data_snapshot_id": _DATA_ID,
        "decision_at": _CLOCK,
        "available_at": _CLOCK,
        "evidence_refs": ["selection:fixture"],
        "candidates": [
            {
                "instrument_id": str(_INSTRUMENT_ID),
                "provenance_refs": ["candidate:fixture"],
            }
        ],
        "selected_instrument_ids": [str(_INSTRUMENT_ID)],
        "evidence_hash": "a" * 64,
    }
    values.update(updates)
    return BaselineSelectionAuthorityRecord(**values)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("evidence_hash", "A" * 64),
        ("evidence_refs", []),
        ("candidates", []),
        ("selected_instrument_ids", []),
        ("selected_instrument_ids", [str(_INSTRUMENT_ID)] * 2),
        ("selected_instrument_ids", ["0198d500-1000-7000-8000-000000000aff"]),
        (
            "candidates",
            [{"instrument_id": str(_INSTRUMENT_ID), "provenance_refs": []}],
        ),
    ),
)
async def test_database_rejects_noncanonical_selection_authority(
    migrated_database_url: str, field: str, value: object
) -> None:
    """Malformed or unevidenced selection rows must fail before a loader can trust them."""
    factory = session_factory(settings(migrated_database_url))
    await _seed_data(factory)
    with pytest.raises(IntegrityError):
        async with factory() as session, session.begin():
            session.add(_record(**{field: value}))


@pytest.mark.asyncio
async def test_selection_authority_is_immutable_and_point_in_time(
    migrated_database_url: str,
) -> None:
    """A valid row loads exactly once; future use, update, and delete all fail closed."""
    factory = session_factory(settings(migrated_database_url))
    await _seed_data(factory)
    async with factory() as session, session.begin():
        session.add(_record())
    async with factory() as session:
        authority = await load_selection_authority(
            session, _AUTHORITY_ID, data_snapshot_id=_DATA_ID, valuation_time=_CLOCK
        )
        assert authority.selected_instrument_ids == (_INSTRUMENT_ID,)
        with pytest.raises(ValueError, match="missing or unavailable"):
            await load_selection_authority(
                session,
                _AUTHORITY_ID,
                data_snapshot_id=_DATA_ID,
                valuation_time=_CLOCK - timedelta(seconds=1),
            )
    async with factory() as session:
        with pytest.raises(IntegrityError, match="optimization record is immutable"):
            async with session.begin():
                await session.execute(
                    text(
                        "UPDATE baseline_selection_authorities "
                        "SET evidence_hash = :digest WHERE id = :authority"
                    ),
                    {"digest": "b" * 64, "authority": _AUTHORITY_ID},
                )
        with pytest.raises(IntegrityError, match="optimization record is immutable"):
            async with session.begin():
                await session.execute(
                    text("DELETE FROM baseline_selection_authorities WHERE id = :authority"),
                    {"authority": _AUTHORITY_ID},
                )
