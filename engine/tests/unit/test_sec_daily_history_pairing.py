"""Historical daily content must pair with its own verified event and frozen cutoff."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus.monitoring import durable_sec_daily as daily
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.sec_monitor import SecDiscoveryEvent, SecMonitorRule
from nexus.research.sec_disclosure_index import SecIndexEntry


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", (None, "instrument", "issuer", "entry", "future"))
async def test_historical_daily_content_exact_event_pair(monkeypatch, mutation):
    now = datetime(2026, 11, 12, 15, tzinfo=UTC)
    account, instrument, rule_id, event_id, cycle_id = (uuid4() for _ in range(5))
    entry = SecIndexEntry(
        accession_number="0000123456-26-000001",
        form="8-K",
        accepted_at=now - timedelta(seconds=10),
        filed_at=now.replace(hour=0),
        primary_document="entry.htm",
    )
    event = SimpleNamespace(
        account_id=account,
        instrument_id=instrument,
        rule_id=rule_id,
        event_key="sec:0000123456:" + entry.accession_number,
        metadata_hash="a" * 64,
        entry=entry.model_dump(mode="json"),
        first_seen_at=now,
    )
    rule = SimpleNamespace(account_id=account, instrument_id=instrument, issuer_cik="0000123456")
    archive = SimpleNamespace(instrument_id=instrument, issuer_cik=rule.issuer_cik, entry=entry)
    if mutation == "instrument":
        archive.instrument_id = uuid4()
    if mutation == "issuer":
        archive.issuer_cik = "0000654321"
    if mutation == "entry":
        archive.entry = entry.model_copy(update={"primary_document": "other.htm"})
    if mutation == "future":
        event.first_seen_at = now + timedelta(seconds=1)
    content = SimpleNamespace(snapshot_id=uuid4(), archive=archive)
    item = SimpleNamespace(
        event_id=event_id,
        rule_id=rule_id,
        event_key=event.event_key,
        metadata_hash=event.metadata_hash,
        entry=event.entry,
        first_seen_at=event.first_seen_at,
        capture_id=uuid4(),
        capture_hash="b" * 64,
        content=content,
    )
    context = SimpleNamespace(
        cycle_id=cycle_id,
        account_id=account,
        instrument_id=instrument,
        decision_time=now,
        events=(item,),
        issues=(),
    )

    async def get(model, identity):
        return {
            AgentPaperCycleRecord: SimpleNamespace(account_id=account),
            SecDiscoveryEvent: event,
            SecMonitorRule: rule,
        }[model]

    session = SimpleNamespace(get=AsyncMock(side_effect=get))
    capture = SimpleNamespace(
        index=SimpleNamespace(
            instrument_id=instrument, issuer_cik=rule.issuer_cik, received_at=now, entries=(entry,)
        ),
        issues=(),
    )
    monkeypatch.setattr(
        "nexus.research.durable_sec_index_storage.read_durable_index_capture",
        AsyncMock(return_value=(SimpleNamespace(revision="b" * 64), capture)),
    )
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.configured_exhibit_store", lambda _: object()
    )
    monkeypatch.setattr(daily, "read_filing_content", AsyncMock(return_value=content))
    assert await daily.validate_durable_sec_pending(session, context) is (mutation is None)
