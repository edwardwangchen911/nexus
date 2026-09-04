"""Legacy batch collection cannot silently skip a different date with the same revision."""

import json

import pytest
from integration.test_provider_collection_pg import (
    NOW,
    FixtureFredClient,
    FixtureSecClient,
    InMemoryMarketObjectStore,
    InMemoryRawEvidenceStore,
    RecordingMarketProvider,
    _bundle,
    _context,
    _payload,
    _seed_universe,
)
from nexus.jobs.models import JobType
from nexus.jobs.provider_collection import build_provider_collection_registry
from nexus.persistence.models.data import MacroObservation
from nexus.persistence.session import session_factory
from nexus.providers.fred import FredRawDocument
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_legacy_batch_rejects_same_revision_on_distinct_dates(migrated_database_url):
    class ReusedRevision(FixtureFredClient):
        async def fetch_alfred_series(self, series, real_time_start, real_time_end):
            original = await super().fetch_alfred_series(series, real_time_start, real_time_end)
            payload = json.loads(original.raw_payload)
            first = payload["observations"][0]
            payload["observations"].append({**first, "date": "2026-08-26"})
            return FredRawDocument(json.dumps(payload).encode(), original.received_at)

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as db, db.begin():
        await _seed_universe(db)
    client = ReusedRevision()
    raw_store = InMemoryRawEvidenceStore()
    spec = build_provider_collection_registry(
        _bundle(RecordingMarketProvider(), sec_client=FixtureSecClient(), fred_client=client),
        InMemoryMarketObjectStore(),
        raw_store,
        lambda: NOW,
    ).resolve(JobType.COLLECT_SEC_MACRO.value)
    assert spec is not None
    with pytest.raises(RuntimeError, match="FRED_OBSERVATION_IDENTITY_COLLISION"):
        async with factory() as db, db.begin():
            await spec.handler(_context(db, _payload(), job_type=JobType.COLLECT_SEC_MACRO))
    assert len(client.calls) == 1
    assert any(key.startswith("raw/fred/") for key in raw_store.objects)
    async with factory() as db:
        assert await db.scalar(select(func.count()).select_from(MacroObservation)) == 0
