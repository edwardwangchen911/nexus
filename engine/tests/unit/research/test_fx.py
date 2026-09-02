"""Public FX parsing cannot infer a vintage from request parameters alone."""

from datetime import date
from decimal import Decimal

import pytest
from nexus.research.fx import parse_dexhkus_vintage

URL = "https://alfred.stlouisfed.org/graph/alfredgraph.csv?id=DEXHKUS&cosd=2026-08-24&coed=2026-08-28&vintage_date=2026-08-31"
RAW = b"observation_date,DEXHKUS_20260831\n2026-08-28,7.8400\n"


def test_vintage_is_verified_in_publisher_bytes() -> None:
    vintage, observations = parse_dexhkus_vintage(RAW, URL)
    assert vintage == date(2026, 8, 31)
    assert observations == ((date(2026, 8, 28), Decimal("7.8400")),)


@pytest.mark.parametrize(
    "payload,url",
    [
        (RAW.replace(b"20260831", b"20260901"), URL),
        (RAW.replace(b"DEXHKUS", b"DEXUSUK"), URL),
        (RAW, URL + "&transformation=pch"),
        (RAW, URL.replace("alfred.stlouisfed.org", "example.com")),
        (RAW + b"2026-08-28,7.8400\n", URL),
        (RAW.replace(b"7.8400", b"-1"), URL),
        (RAW.replace(b"7.8400", b"NaN"), URL),
        (RAW.replace(b"2026-08-28", b"2026-09-01"), URL),
    ],
)
def test_unproved_or_transformed_fx_is_rejected(payload: bytes, url: str) -> None:
    with pytest.raises(ValueError):
        parse_dexhkus_vintage(payload, url)
