"""No-socket Engine -> signed Gateway -> normalized snapshot composition proof."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from nexus.providers.base import InstrumentId
from nexus.providers.market.fosun import FosunSnapshotProvider
from nexus.providers.market.fosun_gateway import (
    FosunMarketGatewayClient,
    FosunMarketGatewayIdentity,
)
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    SnapshotInstrument,
)
from nexus_fosun_gateway.domain import GatewayPage, QuoteItem
from nexus_fosun_gateway.main import create_gateway_app
from nexus_fosun_gateway.settings import GatewaySettings
from pydantic import SecretStr


class _Resolver:
    async def resolve(self, instrument_id: InstrumentId, as_of: date) -> str:
        del instrument_id, as_of
        return "AAPL"


class _QuoteTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def execute(
        self,
        operation: str,
        payload: dict[str, object],
    ) -> GatewayPage[QuoteItem]:
        self.calls.append((operation, payload))
        received_at = datetime.now(UTC)
        return GatewayPage[QuoteItem](
            items=(
                QuoteItem(
                    instrument_code="usAAPL",
                    price=Decimal("227.1600"),
                    as_of=received_at,
                ),
            ),
            source_request_id="synthetic-provider-request",
            has_more=False,
            next_cursor=None,
            as_of=received_at,
            quality="PARTIAL",
        )


@pytest.mark.asyncio
async def test_engine_snapshot_round_trips_through_signed_gateway_without_socket(
    tmp_path: Path,
) -> None:
    app = create_gateway_app(GatewaySettings.for_test())
    transport = _QuoteTransport()
    app.state.transport = transport
    http_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://fosun-gateway.internal",
    )
    gateway = FosunMarketGatewayClient(
        FosunMarketGatewayIdentity(
            origin="https://fosun-gateway.internal",
            key_id="fixture-market-gateway-current",
            secret=SecretStr("fixture-market-gateway-current-secret-32-bytes"),
            client_cert=tmp_path / "market.crt",
            client_key=tmp_path / "market.key",
            client_ca=tmp_path / "ca.crt",
        ),
        client=http_client,
    )
    provider = FosunSnapshotProvider(
        gateway,
        symbol_resolver=_Resolver(),
        delivery_mode="unknown",
    )
    now = datetime.now(UTC)
    bucket_start = now.replace(second=(now.second // 15) * 15, microsecond=0)
    request = IntradaySnapshotRequest(
        instruments=(
            SnapshotInstrument(
                instrument_id=InstrumentId(
                    UUID("019d0000-0000-7000-8000-0000000000a1")
                ),
                symbol="AAPL",
            ),
        ),
        symbol_as_of=now.date(),
        bucket_start=bucket_start,
        include_minute_bars=False,
        feed="l1",
        delivery_mode="unknown",
    )
    try:
        envelope = await provider.fetch_raw_snapshots(request)
        batch = provider.normalize_raw_snapshots(envelope, request)
    finally:
        await gateway.aclose()
        await http_client.aclose()

    assert transport.calls == [("fosun_get_quotes", {"codes": ["usAAPL"]})]
    assert len(batch.snapshots) == 1
    assert batch.snapshots[0].trade is not None
    assert batch.snapshots[0].trade.price == Decimal("227.1600")
    assert batch.snapshots[0].quote is None
    assert batch.provenance.source == "fosun"
