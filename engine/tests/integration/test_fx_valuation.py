"""Historical FX vintages keep publication and local knowledge times distinct."""

from datetime import date, timedelta
from decimal import Decimal
from hashlib import sha256

import pytest
from helpers.portfolio_api import CLOCK, settings
from nexus.persistence.session import session_factory
from nexus.research.fx import import_dexhkus_vintage, load_dexhkus_valuation


class MemoryStore:
    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        return f"memory://sha256/{sha256(content).hexdigest()}"


_RAW = b"observation_date,DEXHKUS_20260820\n2026-08-19,7.8400\n2026-08-20,7.8500\n"
_URL = "https://alfred.stlouisfed.org/graph/alfredgraph.csv?id=DEXHKUS&cosd=2026-08-19&coed=2026-08-20&vintage_date=2026-08-20"


@pytest.mark.asyncio
async def test_fx_import_replay_and_two_time_cutoffs(migrated_database_url: str) -> None:
    factory = session_factory(settings(migrated_database_url))
    async with factory() as db, db.begin():
        ids = await import_dexhkus_vintage(
            db, payload=_RAW, url=_URL, received_at=CLOCK, store=MemoryStore()
        )
        assert len(ids) == 2
    async with factory() as db, db.begin():
        replay = await import_dexhkus_vintage(
            db, payload=_RAW, url=_URL, received_at=CLOCK, store=MemoryStore()
        )
        assert replay == ids
        quote = await load_dexhkus_valuation(
            db, valuation_time=CLOCK - timedelta(minutes=1), known_at=CLOCK
        )
        assert quote is not None
        assert quote.rate_hkd_per_usd == Decimal("7.85")
        assert quote.observed_on == date(2026, 8, 20)
        assert quote.received_at == CLOCK
        assert quote.available_at < CLOCK
        assert (
            await load_dexhkus_valuation(
                db,
                valuation_time=CLOCK - timedelta(minutes=1),
                known_at=CLOCK - timedelta(seconds=1),
            )
            is None
        )
        assert (
            await load_dexhkus_valuation(
                db, valuation_time=CLOCK - timedelta(days=4), known_at=CLOCK
            )
            is None
        )
    async with factory() as db, db.begin():
        with pytest.raises(ValueError, match="conflict"):
            await import_dexhkus_vintage(
                db,
                payload=_RAW.replace(b"7.8500", b"7.8600"),
                url=_URL,
                received_at=CLOCK,
                store=MemoryStore(),
            )
