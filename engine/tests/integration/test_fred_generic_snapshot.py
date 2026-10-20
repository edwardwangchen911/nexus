"""Generic preparation must not starve monthly series behind daily history."""

import json
import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from helpers.portfolio_api import settings
from helpers.research_snapshots import seed_filing
from integration.test_fred_research_context import _macro_collection
from integration.test_fred_vintage_storage import MemoryStore
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.openai_local import prepare_local_research
from nexus.persistence.models.data import DataSnapshot, MacroObservation
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.research.fred_observations import (
    FredCapture,
    FredVintageRequest,
    parse_collected_vintage,
)
from nexus.research.fred_storage import import_fred_vintage, read_fred_observations
from sqlalchemy import select

SERIES = frozenset({"CPIAUCSL", "UNRATE", "DFF", "DGS10", "DGS2", "VIXCLS"})


def collections():
    archive = os.environ.get("NEXUS_FRED_SNAPSHOT_ARCHIVE")
    if archive:
        for record in json.loads(Path(archive).read_text()):
            request = FredVintageRequest.model_validate(record["request"])
            captures = tuple(
                FredCapture(
                    "series" if index == 0 else "observations",
                    item["receipt"]["offset"],
                    item["raw_text"].encode(),
                    datetime.fromisoformat(item["receipt"]["received_at"]),
                )
                for index, item in enumerate(record["captures"])
            )
            yield parse_collected_vintage(request, captures[0], captures[1:])
        return
    # Twenty daily archives yield 640 rows newer than the two monthly observations.
    for series in sorted(SERIES):
        monthly = series in {"CPIAUCSL", "UNRATE"}
        for offset in range(1 if monthly else 5):
            vintage = date(2026, 9, 10) + timedelta(days=offset)
            base = _macro_collection(
                series, vintage=vintage, received_at=datetime(2026, 9, 15, tzinfo=UTC)
            )
            if monthly:
                observed = date(2026, 8, 1)
                request = base.request.model_copy(
                    update={"observation_start": observed, "observation_end": observed}
                )
                payload = json.loads(base.pages[0].raw_payload)
                payload["observation_start"] = payload["observation_end"] = observed.isoformat()
                payload["observations"][0]["date"] = observed.isoformat()
                page = FredCapture(
                    "observations", 0, json.dumps(payload).encode(), base.received_at
                )
                yield parse_collected_vintage(request, base.metadata, (page,))
                continue
            start = vintage - timedelta(days=31)
            request = base.request.model_copy(update={"observation_start": start})
            template = json.loads(base.pages[0].raw_payload)
            observations = [
                dict(template["observations"][0], date=(start + timedelta(days=i)).isoformat())
                for i in range(32)
            ]
            pages = []
            for page_offset in (0, 16):
                payload = dict(
                    template,
                    observation_start=start.isoformat(),
                    count=32,
                    offset=page_offset,
                    observations=observations[page_offset : page_offset + 16],
                )
                pages.append(
                    FredCapture(
                        "observations", page_offset, json.dumps(payload).encode(), base.received_at
                    )
                )
            yield parse_collected_vintage(request, base.metadata, tuple(pages))


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol,identity", [("AAPL", 101), ("AVGO", 102), ("NVDA", 103)])
async def test_generic_snapshot_keeps_six_series_and_source_visibility(
    migrated_database_url, symbol, identity
):
    instrument = UUID(int=identity)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as db, db.begin():
        db.add(
            Instrument(
                id=instrument,
                asset_type="equity",
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="us_equity",
            )
        )
        await db.flush()
        db.add(
            InstrumentAlias(
                instrument_id=instrument,
                symbol=symbol,
                alias_type="ticker",
                valid_from=date(2026, 1, 1),
            )
        )
        await seed_filing(db, instrument_id=instrument, seed_instrument=False)
        old = await prepare_research_snapshot(db, instrument, as_of=datetime.now(UTC))
        assert old is not None
        old_id, old_payload = old.id, dict(old.payload)
    async with factory() as db, db.begin():
        store = MemoryStore()
        for collection in collections():
            await import_fred_vintage(db, collection=collection, store=store)
    at = datetime.now(UTC)
    result = await prepare_local_research(settings(migrated_database_url), instrument, as_of=at)
    assert result["status"] == "RESEARCH_INPUTS_PREPARED"
    assert result["model_requests"] == result["provider_requests"] == 0
    assert result["execution_locked"] is True
    async with factory() as db:
        capped = await read_fred_observations(db, as_of=at)
        assert not {"CPIAUCSL", "UNRATE"} & {value.series for value in capped.values()}
        reference = {}
        for series in SERIES:
            reference.update(await read_fred_observations(db, as_of=at, series=frozenset({series})))
        from nexus.agents.snapshots import _macro_context_ids

        expected = _macro_context_ids(reference)
        selected = await db.get(DataSnapshot, UUID(result["data_snapshot_id"]))
        assert selected is not None
        bound = set(selected.payload["evidence_ids"])
        future = set(
            await db.scalars(
                select(MacroObservation.evidence_ref_id).where(MacroObservation.available_at > at)
            )
        )
        assert not bound & {str(key) for key in future}
        assert (
            await read_fred_observations(db, as_of=at - timedelta(days=1), latest_per_series=True)
            == {}
        )
        assert {str(value) for value in expected} <= bound
        assert {value.series for key, value in reference.items() if str(key) in bound} == SERIES
        assert all(
            value.available_at <= at for key, value in reference.items() if str(key) in bound
        )
        old = await db.get(DataSnapshot, old_id)
        assert old.payload == old_payload
        assert not set(old.payload["evidence_ids"]) & {str(key) for key in reference}
