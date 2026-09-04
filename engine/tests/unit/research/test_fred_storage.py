"""Storage rejects caller-supplied normalized facts that disagree with originals."""

from dataclasses import replace

import pytest


@pytest.mark.asyncio
async def test_import_reparses_original_instead_of_trusting_collection_fields():
    from nexus.research.fred_storage import import_fred_vintage

    # Kept separate from the independently deliverable collection tests.
    from unit.research.test_fred_observations import parse

    class Forbidden:
        def __getattr__(self, name):
            raise AssertionError("invalid collection must not touch DB or store")

    with pytest.raises(ValueError, match="FRED_COLLECTION_MISMATCH"):
        await import_fred_vintage(
            Forbidden(), collection=replace(parse(), unit="lin"), store=Forbidden()
        )
