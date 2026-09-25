"""Literal FRED wire examples, not synthetic provider revision extensions."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr


def captured_payloads():
    return (
        {
            "realtime_start": "2026-08-28",
            "realtime_end": "2026-08-28",
            "seriess": [
                {
                    "id": "DGS10",
                    "realtime_start": "2026-08-28",
                    "realtime_end": "2026-08-28",
                    "units": "Percent",
                    "units_short": "%",
                    "frequency": "Daily",
                    "seasonal_adjustment": "Not Seasonally Adjusted",
                }
            ],
        },
        {
            "realtime_start": "2026-08-28",
            "realtime_end": "2026-08-28",
            "units": "lin",
            "output_type": 1,
            "count": 3,
            "offset": 0,
            "limit": 16,
            "observation_start": "2026-08-24",
            "observation_end": "2026-08-28",
            "order_by": "observation_date",
            "sort_order": "asc",
            "observations": [
                {
                    "date": "2026-08-24",
                    "realtime_start": "2026-08-28",
                    "realtime_end": "2026-08-28",
                    "value": "4.123456789012",
                },
                {
                    "date": "2026-08-25",
                    "realtime_start": "2026-08-28",
                    "realtime_end": "2026-08-28",
                    "value": ".",
                },
                {
                    "date": "2026-08-26",
                    "realtime_start": "2026-08-28",
                    "realtime_end": "2026-08-28",
                    "value": "0",
                },
            ],
        },
    )


def parse(metadata=None, page=None):
    from nexus.research.fred_observations import (
        FredCapture,
        FredVintageRequest,
        parse_collected_vintage,
    )

    original_meta, original_page = captured_payloads()
    request = FredVintageRequest(
        series="DGS10",
        observation_start="2026-08-24",
        observation_end="2026-08-28",
        vintage="2026-08-28",
    )
    at = datetime(2026, 9, 4, 12, tzinfo=UTC)
    metadata_capture = FredCapture(
        kind="series",
        offset=None,
        raw_payload=json.dumps(original_meta if metadata is None else metadata).encode(),
        received_at=at,
    )
    page_capture = FredCapture(
        kind="observations",
        offset=0,
        raw_payload=json.dumps(original_page if page is None else page).encode(),
        received_at=at,
    )
    return parse_collected_vintage(request, metadata_capture, (page_capture,))


def test_wire_unit_is_metadata_unit_and_missing_is_not_zero():
    result = parse()
    assert result.unit == "Percent"
    assert result.observations == (
        (date(2026, 8, 24), Decimal("4.123456789012")),
        (date(2026, 8, 26), Decimal("0")),
    )
    assert result.missing_dates == (date(2026, 8, 25),)
    assert result.received_at == datetime(2026, 9, 4, 12, tzinfo=UTC)


@pytest.mark.parametrize(
    "defect", ["foreign", "transformed", "truncated", "duplicate", "precision", "vintage"]
)
def test_invalid_source_contract_is_rejected(defect):
    metadata, page = captured_payloads()
    if defect == "foreign":
        metadata["seriess"][0]["id"] = "DGS2"
    elif defect == "transformed":
        page["units"] = "pch"
    elif defect == "truncated":
        page["count"] = 4
    elif defect == "duplicate":
        page["observations"][2]["date"] = "2026-08-24"
    elif defect == "precision":
        page["observations"][0]["value"] = "4.1234567890123"
    else:
        page["observations"][0]["realtime_start"] = "2026-08-27"
    with pytest.raises(ValueError, match="FRED_VINTAGE_INVALID"):
        parse(metadata, page)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_page", [False, True])
async def test_normal_collector_archives_real_http_bytes_and_safe_receipt(tmp_path: Path, bad_page):
    from nexus.providers.fred_http import FredHttpTransport
    from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
    from nexus.research.fred_collection import collect_fred_vintage, load_fred_collection

    collection = parse()
    calls = []
    metadata, page = captured_payloads()
    originals = [
        json.dumps(metadata).encode(),
        b"bad-json" if bad_page else json.dumps(page).encode(),
    ]

    def handler(request):
        calls.append(request)
        if len(calls) == 2:
            assert (tmp_path / "000.raw.json").read_bytes() == originals[0]
            assert (tmp_path / "000.receipt.json").exists()
        return httpx.Response(200, content=originals[len(calls) - 1])

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        transport = FredHttpTransport(
            FixedOriginHttpClient(
                provider_name="fred",
                origin=AllowedOrigin(scheme="https", host="api.stlouisfed.org"),
                policy=HttpRequestPolicy(max_attempts=1),
                client=client,
            ),
            SecretStr("private-test-key"),
        )
        if bad_page:
            with pytest.raises(ValueError, match="FRED_VINTAGE_INVALID"):
                await collect_fred_vintage(
                    collection.request,
                    transport=transport,
                    output_dir=tmp_path,
                    clock=lambda: collection.received_at,
                )
            assert not (tmp_path / "complete.json").exists()
        else:
            result = await collect_fred_vintage(
                collection.request,
                transport=transport,
                output_dir=tmp_path,
                clock=lambda: collection.received_at,
            )
            assert result == load_fred_collection(tmp_path)
            assert result.observations == collection.observations
    assert len(calls) == 2
    assert [request.url.path for request in calls] == ["/fred/series", "/fred/series/observations"]
    assert calls[1].url.params["limit"] == "16"
    assert calls[1].url.params["realtime_start"] == "2026-08-28"
    for index in range(2):
        assert (tmp_path / f"{index:03}.raw.json").read_bytes() == originals[index]
        receipt = (tmp_path / f"{index:03}.receipt.json").read_bytes()
        assert b"private-test-key" not in receipt and b"api_key" not in receipt
        assert b"2026-09-04T12:00:00+00:00" in receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("wire_code", "receipt_code"),
    [
        ("RETRY_BUDGET_EXHAUSTED", "RETRY_BUDGET_EXHAUSTED"),
        ("STATUS_REJECTED private-test-key", "FRED_HTTP_FAILURE_UNKNOWN"),
    ],
)
async def test_partial_fred_archive_records_safe_failure_stage_without_retry(
    tmp_path: Path, wire_code: str, receipt_code: str
):
    from nexus.providers.fred_http import FredHttpTransport
    from nexus.providers.http import HttpPolicyError
    from nexus.research.fred_collection import collect_fred_vintage

    collection = parse()
    metadata, page = captured_payloads()
    page["observations"] = page["observations"][:2]

    class Wire:
        calls = 0

        async def get_bytes(self, *_args):
            self.calls += 1
            if self.calls == 3:
                raise HttpPolicyError(wire_code)
            return json.dumps(metadata if self.calls == 1 else page).encode()

    wire = Wire()
    with pytest.raises(HttpPolicyError):
        await collect_fred_vintage(
            collection.request,
            transport=FredHttpTransport(wire, SecretStr("private-test-key")),
            output_dir=tmp_path,
            clock=lambda: collection.received_at,
        )

    assert wire.calls == 3
    assert not (tmp_path / "complete.json").exists()
    assert (tmp_path / "001.raw.json").exists()
    receipt_bytes = (tmp_path / "failure.json").read_bytes()
    assert b"private-test-key" not in receipt_bytes and b"api_key" not in receipt_bytes
    assert json.loads(receipt_bytes) == {
        "schema": "fred_collection_failure_v1",
        "stage": "observations_fetch",
        "offset": 2,
        "code": receipt_code,
        "capture_count": 2,
    }


@pytest.mark.asyncio
async def test_collector_timeout_receipt_is_safe_to_classify_as_unavailable(tmp_path: Path):
    from nexus.providers.fred_http import FredHttpTransport
    from nexus.research.fred_collection import (
        collect_fred_vintage,
        has_archived_timeout_failure,
    )

    class Wire:
        calls = 0

        async def get_bytes(self, *_args):
            self.calls += 1
            raise TimeoutError

    wire = Wire()
    with pytest.raises(TimeoutError):
        await collect_fred_vintage(
            parse().request,
            transport=FredHttpTransport(wire, SecretStr("private-test-key")),
            output_dir=tmp_path,
        )

    assert wire.calls == 1
    assert has_archived_timeout_failure(tmp_path)
    assert not (tmp_path / "complete.json").exists()
    receipt_path = tmp_path / "failure.json"
    receipt = json.loads(receipt_path.read_bytes())
    assert receipt == {
        "schema": "fred_collection_failure_v1",
        "stage": "metadata_fetch",
        "offset": None,
        "code": "FRED_COLLECTION_TIMEOUT",
        "capture_count": 0,
    }
    receipt["code"] = "FRED_COLLECTION_FAILURE_UNKNOWN"
    receipt_path.write_bytes(json.dumps(receipt).encode())
    assert not has_archived_timeout_failure(tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect", ["metadata_id", "metadata_unit", "metadata_vintage", "page_value"]
)
async def test_first_semantic_error_stops_before_next_http(tmp_path, defect):
    from nexus.providers.fred_http import FredHttpTransport
    from nexus.research.fred_collection import collect_fred_vintage

    collection = parse()
    metadata, page = captured_payloads()
    if defect == "metadata_id":
        metadata["seriess"][0]["id"] = "DGS2"
    elif defect == "metadata_unit":
        metadata["seriess"][0]["units"] = ""
    elif defect == "metadata_vintage":
        metadata["seriess"][0]["realtime_start"] = "2026-08-27"
    else:
        page["count"] = 4
        page["observations"][0]["value"] = "NaN"

    class Wire:
        calls = 0

        async def get_bytes(self, *args):
            self.calls += 1
            if self.calls > (2 if defect == "page_value" else 1):
                raise AssertionError("unexpected later HTTP")
            return json.dumps(metadata if self.calls == 1 else page).encode()

    wire = Wire()
    with pytest.raises(ValueError, match="FRED_VINTAGE_INVALID"):
        await collect_fred_vintage(
            collection.request,
            transport=FredHttpTransport(wire, SecretStr("test")),
            output_dir=tmp_path,
            clock=lambda: collection.received_at,
        )
    assert wire.calls == (2 if defect == "page_value" else 1)
    assert (tmp_path / "000.receipt.json").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_second", [False, True])
async def test_short_page_is_not_exhaustion_and_replay_revalidates_receipts(tmp_path, bad_second):
    from nexus.providers.fred_http import FredHttpTransport
    from nexus.research.fred_collection import collect_fred_vintage, load_fred_collection

    collection = parse()
    metadata, first = captured_payloads()
    _, second = captured_payloads()
    first["observations"] = first["observations"][:2]
    second["observations"] = second["observations"][2:]
    second["offset"] = 2
    if bad_second:
        second["count"] = 4

    class Wire:
        calls = []

        async def get_bytes(self, path, query, headers):
            self.calls.append(dict(query))
            return json.dumps([metadata, first, second][len(self.calls) - 1]).encode()

    wire = Wire()
    if bad_second:
        with pytest.raises(ValueError, match="FRED_VINTAGE_INVALID"):
            await collect_fred_vintage(
                collection.request,
                transport=FredHttpTransport(wire, SecretStr("test")),
                output_dir=tmp_path,
                clock=lambda: collection.received_at,
            )
        assert not (tmp_path / "complete.json").exists()
    else:
        result = await collect_fred_vintage(
            collection.request,
            transport=FredHttpTransport(wire, SecretStr("test")),
            output_dir=tmp_path,
            clock=lambda: collection.received_at,
        )
        assert result.observations == collection.observations
        assert load_fred_collection(tmp_path) == result
        receipt_path = tmp_path / "001.receipt.json"
        receipt = json.loads(receipt_path.read_bytes())
        receipt["query"]["series_id"] = "DGS2"
        receipt_path.write_text(json.dumps(receipt))
        with pytest.raises(ValueError, match="FRED_ARCHIVE_INVALID"):
            load_fred_collection(tmp_path)
    assert len(wire.calls) == 3
    assert wire.calls[2]["offset"] == "2"
    assert all("order_by" not in call for call in wire.calls)


@pytest.mark.parametrize(
    "change", ["bool_output", "bool_count", "numeric_value", "negative_precision", "extra_page"]
)
def test_strict_json_and_exhausted_tail(change):
    from nexus.research.fred_observations import FredCapture, parse_collected_vintage

    collection = parse()
    metadata, page = captured_payloads()
    if change == "bool_output":
        page["output_type"] = True
    elif change == "bool_count":
        page["count"] = True
    elif change == "numeric_value":
        page["observations"][0]["value"] = 4.2
    elif change == "negative_precision":
        page["observations"][0]["value"] = "-0.0000000000001"
    else:
        page["observations"] = []
        page["offset"] = 3
        extra = FredCapture("observations", 3, json.dumps(page).encode(), collection.received_at)
        with pytest.raises(ValueError, match="FRED_VINTAGE_INVALID"):
            parse_collected_vintage(
                collection.request, collection.metadata, (*collection.pages, extra)
            )
        return
    with pytest.raises(ValueError, match="FRED_VINTAGE_INVALID"):
        parse(metadata, page)


def test_exact_representable_trailing_zero_precision_is_preserved():
    metadata, page = captured_payloads()
    page["observations"][0]["value"] = "-0.0000000000010000"
    result = parse(metadata, page)
    assert str(result.observations[0][1]) == "-1.0000E-12"
