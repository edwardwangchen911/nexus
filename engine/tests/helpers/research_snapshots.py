"""Synthetic filing authority shared by research snapshot path tests."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import Filing, ResearchEvidenceRef

NOW = datetime(2026, 9, 2, 4, tzinfo=UTC)
INSTRUMENT = UUID("0198d500-1000-7000-8000-000000002001")


async def seed_filing(
    session,
    *,
    source="sec",
    accepted_at=NOW,
    created_at=NOW,
    instrument_id=INSTRUMENT,
    seed_instrument=True,
    provider_object_id="filing:test",
    accession_number="0000000001-26-000001",
):
    if seed_instrument:
        session.add(
            Instrument(
                id=instrument_id,
                asset_type="equity",
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="us_equity",
            )
        )
        await session.flush()
        session.add(
            InstrumentAlias(
                instrument_id=instrument_id,
                symbol="TEST",
                alias_type="ticker",
                valid_from=NOW.date(),
                valid_to=None,
            )
        )
    evidence = ResearchEvidenceRef(
        source=source,
        provider_object_id=provider_object_id,
        revision="test-v1",
        available_at=NOW,
        created_at=created_at,
        content_hash="a" * 64,
        location="memory://sha256/" + "a" * 64,
        byte_length=10,
    )
    session.add(evidence)
    await session.flush()
    session.add(
        Filing(
            instrument_id=instrument_id,
            evidence_ref_id=evidence.id,
            source=source,
            provider_object_id=evidence.provider_object_id,
            revision=evidence.revision,
            accession_number=accession_number,
            form="10-Q",
            filed_at=NOW,
            accepted_at=accepted_at,
            created_at=created_at,
        )
    )
    await session.flush()
    return evidence
