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
    encode_fosun_snapshot_evidence,
)
from nexus.providers.market.fosun_gateway import (
    FosunMarketGatewayClient,
    FosunMarketGatewayIdentity,
    FosunSecurityClassification,
    RawGatewayResponse,
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
        return normalize_provider_response(
            "fosun_get_quotes",
            {
                "code": 0,
                "requestId": "synthetic-provider-request",
                "data": [
                    {
                        "mkt": "us",
                        "rawSymbol": "AAPL",
                        "price": 2271600,
                        "power": 4,
                        "name": "Apple Inc.",
                        "first": 1,
                        "second": 101,
                        "third": 10105,
                        "mktCap": 1,
                        "floatMktCap": 1,
                        "pb": 1,
                        "peStatic": 1,
                        "peTTM": 1,
                        "totalShares": 1,
                        "floatShares": 1,
                        "minTradeUnit": 1,
                        "delay": True,
                        "qtDate": 20261008,
                        "qtTime": 160004506,
                    }
                ],
            },
            received_at=received_at,
            context={"codes": tuple(payload["codes"])},
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
    assert batch.snapshots[0].trade.quality_flags == (
        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
        "INTRADAY_BID_ASK_UNAVAILABLE",
    )
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
    assert quote_page.items[0].provider_quote_date == 20261008
    assert quote_page.items[0].provider_quote_time == 160004506
    assert (
        classify_fosun_security_family(quote_page.items[0].provider_classification)
        == "company_equity_like"
    )


@pytest.mark.parametrize(
    "provider_date,provider_time", [(20261008, 160004506), (0, 0), (None, None)]
)
def test_quote_source_clock_survives_normalization_archive_and_engine(provider_date, provider_time):
    received_at = datetime(2026, 10, 8, 20, tzinfo=UTC)
    row = {"mkt": "us", "rawSymbol": "AAPL", "price": 12345, "power": 2}
    if provider_date is not None:
        row.update(qtDate=provider_date, qtTime=provider_time)
    page = normalize_provider_response(
        "fosun_get_quotes",
        {"code": 0, "data": [row], "requestId": "synthetic-clock"},
        received_at=received_at,
        context={"codes": ("usAAPL",)},
    )
    document = page.model_dump(mode="json")
    if provider_date is None:
        document["items"][0].pop("provider_quote_date")
        document["items"][0].pop("provider_quote_time")
    response = RawGatewayResponse(
        operation="fosun_get_quotes",
        gateway_request_id=UUID("0198d400-3000-7000-8000-000000000012"),
        request_body_sha256="a" * 64,
        raw_payload=json.dumps(document).encode(),
        sent_at=received_at,
        received_at=received_at,
    )
    archive = encode_fosun_snapshot_evidence(((("usAAPL",), response),))
    restored = decode_fosun_snapshot_evidence(archive)[0][1]
    parsed = parse_quote_page(restored, expected_codes=("usAAPL",))
    assert parsed.items[0].provider_quote_date == provider_date
    assert parsed.items[0].provider_quote_time == provider_time
    assert parsed.items[0].as_of == received_at
    assert parsed.quality == "PARTIAL"
    assert restored.raw_payload == response.raw_payload


@pytest.mark.parametrize("field", ["qtDate", "qtTime"])
@pytest.mark.parametrize("value", [True, "20261008", 1.5, -1, 2**63])
def test_quote_source_clock_rejects_malformed_present_values(field, value):
    from nexus_fosun_gateway.domain import GatewayError

    with pytest.raises(GatewayError) as error:
        normalize_provider_response(
            "fosun_get_quotes",
            {
                "code": 0,
                "data": [
                    {"mkt": "us", "rawSymbol": "AAPL", "price": 12345, "power": 2, field: value}
                ],
                "requestId": "synthetic-clock",
            },
            received_at=datetime(2026, 10, 8, 20, tzinfo=UTC),
            context={"codes": ("usAAPL",)},
        )
    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"
