"""No-socket Engine -> signed Gateway -> normalized snapshot composition proof."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.providers.market.fosun import (
    FosunMarketDataProvider,
    FosunSnapshotProvider,
    decode_fosun_snapshot_evidence,
)
from nexus.providers.market.fosun_gateway import (
    FosunMarketGatewayClient,
    FosunMarketGatewayIdentity,
    FosunSecurityClassification,
    classify_fosun_security_family,
    parse_quote_page,
)
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    SnapshotInstrument,
)
from nexus_fosun_gateway.adapter.normalization import normalize_provider_response
from nexus_fosun_gateway.domain import GatewayPage, QuoteItem
from nexus_fosun_gateway.main import create_gateway_app
from nexus_fosun_gateway.settings import GatewaySettings
from pydantic import SecretStr


class _Resolver:
    async def resolve(self, instrument_id: InstrumentId, as_of: date) -> str:
        del instrument_id, as_of
        return "AAPL"


@pytest.mark.parametrize(
    ("levels", "equity_complete", "net_asset_complete", "expected"),
    [
        ((1, 101, 10105), True, False, "company_equity_like"),
        ((2, 202, 20202), False, True, "fund_or_structured_like"),
        ((2, 202, 20202), True, True, "unknown"),
        ((2, 202, 20202), False, False, "unknown"),
        ((9, 9, 9), False, True, "unknown"),
        ((2, None, None), False, True, "unknown"),
    ],
)
def test_fosun_security_family_classifier_fails_closed(
    levels: tuple[int | None, int | None, int | None],
    equity_complete: bool,
    net_asset_complete: bool,
    expected: str,
) -> None:
    evidence = FosunSecurityClassification(
        level_1=levels[0],
        level_2=levels[1],
        level_3=levels[2],
        equity_profile_fields_complete=equity_complete,
        net_asset_profile_fields_complete=net_asset_complete,
        fund_asset_type_chs="",
        fund_asset_type_cht="",
        fund_asset_type_eng="",
    )

    assert classify_fosun_security_family(evidence) == expected


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
                    provider_name="Apple Inc.",
                    provider_classification={
                        "level_1": 1,
                        "level_2": 101,
                        "level_3": 10105,
                        "equity_profile_fields_complete": True,
                        "net_asset_profile_fields_complete": False,
                    },
                    provider_reported_delay=True,
                ),
            ),
            source_request_id="synthetic-provider-request",
            has_more=False,
            next_cursor=None,
            as_of=received_at,
            quality="PARTIAL",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("turnover", [None, 0, 900719925474099312345])
async def test_bar_source_metadata_survives_signed_gateway_and_provider_archive(tmp_path, turnover):
    class BarTransport:
        calls = 0

        async def execute(self, operation, payload):
            self.calls += 1
            assert operation == "fosun_get_bars"
            rows = [
                {
                    "time": day,
                    "open": 1234,
                    "high": 1234,
                    "low": 1234,
                    "close": 1234,
                    "vol": 7,
                    "turnover": turnover,
                }
                for day in (20260827, 20260828)
            ][-payload["limit"] :]
            return normalize_provider_response(
                operation,
                {
                    "code": 0,
                    "requestId": "synthetic-bars",
                    "data": {
                        "power": 2,
                        "total": len(rows),
                        "data": rows,
                    },
                },
                received_at=datetime.now(UTC),
                context=payload,
            )

    app = create_gateway_app(GatewaySettings.for_test())
    transport = BarTransport()
    app.state.transport = transport
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="https://fosun-gateway.internal",
    ) as client:
        gateway = FosunMarketGatewayClient(
            FosunMarketGatewayIdentity(
                origin="https://fosun-gateway.internal",
                key_id="fixture-market-gateway-current",
                secret=SecretStr("fixture-market-gateway-current-secret-32-bytes"),
                client_cert=tmp_path / "market.crt",
                client_key=tmp_path / "market.key",
                client_ca=tmp_path / "ca.crt",
            ),
            client=client,
        )
        provider = FosunMarketDataProvider(
            gateway, symbol_resolver=_Resolver(), delivery_mode="unknown"
        )
        request = BarsRequest(
            instrument_id=InstrumentId(UUID(int=1)),
            interval="1Day",
            start=datetime(2026, 8, 28, tzinfo=UTC),
            end=datetime(2026, 8, 29, tzinfo=UTC),
            adjustment="raw",
        )
        raw = await provider.fetch_raw_bars(request)
        page = await provider.normalize_bars(request, raw)
        assert page.raw_payload == raw.raw_payload
        archived = json.loads(page.raw_payload)
        assert archived["items"][0]["provider_raw_turnover"] == turnover
        assert archived["provider_scale_power"] == 2
        assert page.items[0].close == Decimal("12.34")
        assert page.items[0].volume == Decimal(7)
        assert page.provenance.delivery_mode == "unknown"
        assert "currency" not in archived["items"][0]
        assert "volume_unit" not in archived["items"][0]
        assert transport.calls == 1


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
                instrument_id=InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a1")),
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
    responses = decode_fosun_snapshot_evidence(
        envelope.raw_payload,
        expected_codes=("usAAPL",),
        include_minute_bars=False,
        bucket_start=bucket_start,
    )
    quote_page = parse_quote_page(responses[0][1], expected_codes=("usAAPL",))
    assert quote_page.items[0].provider_name == "Apple Inc."
    assert quote_page.items[0].provider_reported_delay is True
    assert (
        classify_fosun_security_family(quote_page.items[0].provider_classification)
        == "company_equity_like"
    )
