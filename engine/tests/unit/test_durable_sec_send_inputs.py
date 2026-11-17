from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.monitoring import durable_sec_send_inputs as inputs
from nexus.research.sec_disclosure_index import SecIndexEntry
from nexus.research.sec_listed_history import SecMetadataEntry


@pytest.mark.asyncio
@pytest.mark.parametrize("listed", [False, True])
@pytest.mark.parametrize("copies", [1, 2])
async def test_current_html_metadata_matches_across_versioned_reader_models(
    monkeypatch, listed, copies
):
    instrument, snapshot = UUID(int=1), UUID(int=2)
    at = datetime(2026, 11, 12, 15, tzinfo=UTC)
    entry = SecIndexEntry(
        accession_number="0000123456-26-000001",
        form="8-K",
        accepted_at=at,
        filed_at=at.replace(hour=0),
        primary_document="release.htm",
    )
    current = SecMetadataEntry.model_validate(entry.model_dump()) if listed else entry
    scope = dict(
        account_id=UUID(int=3),
        instrument_id=instrument,
        plan_id=None,
        event_key=f"sec:0000123456:{entry.accession_number}",
        entry=entry.model_dump(mode="json"),
    )
    index = SimpleNamespace(
        instrument_id=instrument, issuer_cik="0000123456", entries=(current,) * copies
    )
    rows = SimpleNamespace(mappings=lambda: [scope])
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[rows, None]),
        scalar=AsyncMock(side_effect=[True, at, snapshot, at, snapshot]),
        get=AsyncMock(return_value=SimpleNamespace(source="sec_disclosure_listed_index")),
    )
    monkeypatch.setattr(inputs, "configured_exhibit_store", lambda session: object())
    monkeypatch.setattr(inputs, "read_monitor_index", AsyncMock(return_value=(object(), index)))
    if copies == 1:
        await inputs.verify_send_inputs(session, UUID(int=4))
    else:
        with pytest.raises(ValueError, match="SEC_RESEARCH_PERMISSION_INVALID"):
            await inputs.verify_send_inputs(session, UUID(int=4))
