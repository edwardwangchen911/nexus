"""The dedicated request pins historical identity and archives before parsing."""

import json
from datetime import UTC, date, datetime
from hashlib import sha256
from uuid import UUID

import pytest
from pydantic import SecretStr


@pytest.mark.parametrize(
    "day,start,end",
    [
        (date(2026, 3, 8), "2026-03-08T05:00:00+00:00", "2026-03-09T03:59:59+00:00"),
        (date(2026, 11, 1), "2026-11-01T04:00:00+00:00", "2026-11-02T04:59:59+00:00"),
    ],
)
def test_daily_window_uses_new_york_midnight_across_dst(day, start, end):
    from nexus.shadow.price_collection import daily_request

    request = daily_request(UUID(int=1), day, datetime(2027, 1, 1, tzinfo=UTC))
    assert request.start.isoformat() == start
    assert request.end.isoformat() == end
    assert (request.interval, request.adjustment) == ("1Day", "raw")


def test_unfinished_new_york_day_is_rejected():
    from nexus.shadow.price_collection import daily_request

    with pytest.raises(ValueError, match="SHADOW_PRICE_DAY_NOT_CLOSED"):
        daily_request(UUID(int=1), date(2026, 9, 2), datetime(2026, 9, 3, 3, tzinfo=UTC))


@pytest.mark.parametrize("day", [None, "2026-09-02", datetime(2026, 9, 2, tzinfo=UTC)])
def test_identity_day_must_be_explicit_date(day):
    from nexus.shadow.price_collection import daily_request

    with pytest.raises(ValueError, match="SHADOW_PRICE_DAY_NOT_CLOSED"):
        daily_request(UUID(int=1), day, datetime(2026, 9, 3, 5, tzinfo=UTC))


@pytest.mark.asyncio
async def test_transport_and_original_reader_keep_historical_identity_through_pagination():
    from nexus.shadow.price_collection import ShadowPriceTransport, daily_request
    from nexus.shadow.source_queries import InstrumentPriceReceipt, verify_original_pages

    at = datetime(2026, 9, 3, 5, tzinfo=UTC)
    # Historical DST boundary: neither UTC request.end nor receipt date is the identity day.
    day = date(2026, 3, 8)
    base = (
        ("adjustment", "raw"), ("asof", "2026-03-08"), ("currency", "USD"),
        ("end", "2026-03-09T03:59:59Z"), ("feed", "sip"), ("limit", "10000"),
        ("sort", "asc"), ("start", "2026-03-08T05:00:00Z"), ("timeframe", "1Day"),
    )
    raw_pages = (
        b'{"symbol":"TEST","bars":[],"next_page_token":"next"}',
        b'{"symbol":"TEST","bars":[],"next_page_token":null}',
    )
    queries, archived = [], []

    class Http:
        async def get_bytes(self, path_segments, query, headers):
            queries.append(query)
            return raw_pages[len(queries) - 1]

    async def archive(raw, received):
        archived.append((raw, received))
        return UUID(int=10 + len(archived))

    transport = ShadowPriceTransport(
        Http(), symbol="TEST", session_date=day, feed="sip",
        key_id=SecretStr("test"), secret=SecretStr("test"), archive=archive, now=lambda: at,
    )
    pages = []
    for index, cursor in enumerate((None, "next")):
        response = await transport.fetch_bars(daily_request(UUID(int=1), day, at), cursor)
        pages.append({
            "raw_receipt_id": UUID(int=20 + index),
            "raw_artifact_id": transport.raw_artifact_id,
            "raw_hash": sha256(response.raw_payload).hexdigest(),
            "raw_byte_length": len(response.raw_payload), "received_at": at,
            "path": ("v2", "stocks", "TEST", "bars"), "query": transport.query,
            "input_cursor": cursor, "output_cursor": "next" if index == 0 else None,
            "manifest_id": None, "bar_set_id": None, "member_ids": (),
        })
    assert queries == [base, (*base, ("page_token", "next"))]
    assert archived == [(raw_pages[0], at), (raw_pages[1], at)]
    receipt = InstrumentPriceReceipt.model_validate({
        "instrument_id": UUID(int=1), "alias_id": UUID(int=2), "symbol": "TEST",
        "session_date": day, "feed": "sip", "delivery_mode": "delayed", "pages": pages,
    })

    async def read_raw(page):
        return raw_pages[0 if page.input_cursor is None else 1]

    verified = await verify_original_pages(receipt, read_raw)
    assert len(verified) == 2 and all(page.items == () for page in verified)
    assert [page.next_cursor for page in verified] == ["next", None]


@pytest.mark.asyncio
async def test_actual_transport_pins_asof_and_archives_malformed_original_first():
    from nexus.providers.market.alpaca import MarketProviderPayloadError
    from nexus.shadow.price_collection import ShadowPriceTransport, daily_request

    received = datetime(2026, 9, 3, 5, tzinfo=UTC)
    events = []

    class Http:
        async def get_bytes(self, path_segments, query, headers):
            assert path_segments == ("v2", "stocks", "TEST", "bars")
            assert dict(query) == {
                "adjustment": "raw",
                "currency": "USD",
                "start": "2026-09-02T04:00:00Z",
                "end": "2026-09-03T03:59:59Z",
                "asof": "2026-09-02",
                "feed": "sip",
                "limit": "10000",
                "sort": "asc",
                "timeframe": "1Day",
            }
            assert len([key for key, _ in query if key == "asof"]) == 1
            events.append("http")
            return b"malformed original"

    async def archive(raw, at):
        assert raw == b"malformed original" and at == received
        events.append("archive")
        return UUID(int=2)

    transport = ShadowPriceTransport(
        Http(),
        symbol="TEST",
        session_date=date(2026, 9, 2),
        feed="sip",
        key_id=SecretStr("test"),
        secret=SecretStr("test"),
        archive=archive,
        now=lambda: received,
    )
    with pytest.raises(MarketProviderPayloadError):
        await transport.fetch_bars(daily_request(UUID(int=1), date(2026, 9, 2), received), None)
    assert events == ["http", "archive"]


@pytest.mark.asyncio
@pytest.mark.parametrize("timestamp", ["2026-09-02T20:00:00Z", "2026-09-03T04:00:00Z"])
async def test_receipt_validator_rejects_not_day_start_or_other_session(timestamp):
    from nexus.shadow.price_collection import daily_request
    from nexus.shadow.source_queries import normalize_original

    at = datetime(2026, 9, 3, 5, tzinfo=UTC)
    raw = json.dumps(
        {
            "symbol": "TEST",
            "bars": [
                {
                    "t": timestamp,
                    "o": "100",
                    "h": "101",
                    "l": "99",
                    "c": "100",
                    "v": "10",
                }
            ],
            "next_page_token": None,
        }
    ).encode()
    with pytest.raises(ValueError, match="SHADOW_PRICE_ORIGINAL_INVALID"):
        await normalize_original(
            raw,
            daily_request(UUID(int=1), date(2026, 9, 2), at),
            received_at=at,
            feed="sip",
            delivery_mode="delayed",
            symbol="TEST",
        )


@pytest.mark.asyncio
async def test_missing_cursor_never_proves_empty_return_set():
    from nexus.shadow.price_collection import daily_request
    from nexus.shadow.source_queries import normalize_original

    at = datetime(2026, 9, 3, 5, tzinfo=UTC)
    with pytest.raises(ValueError, match="SHADOW_PRICE_ORIGINAL_INVALID"):
        await normalize_original(
            b'{"symbol":"TEST","bars": []}',
            daily_request(UUID(int=1), date(2026, 9, 2), at),
            received_at=at,
            feed="sip",
            delivery_mode="delayed",
            symbol="TEST",
        )


@pytest.mark.asyncio
async def test_normalized_facts_come_only_from_archived_raw_bytes():
    from nexus.shadow.price_collection import daily_request
    from nexus.shadow.source_queries import normalize_original

    at = datetime(2026, 9, 3, 5, tzinfo=UTC)
    page = await normalize_original(
        b'{"symbol":"TEST","bars":[{"t":"2026-09-02T04:00:00Z","o":"100","h":"101",'
        b'"l":"99","c":"100.25","v":"10"}],"next_page_token":null}',
        daily_request(UUID(int=1), date(2026, 9, 2), at),
        received_at=at,
        feed="sip",
        delivery_mode="delayed",
        symbol="TEST",
    )
    assert str(page.items[0].close) == "100.25"
    assert page.items[0].available_at == at
    assert page.items[0].session == date(2026, 9, 2)
    assert page.next_cursor is None


def test_v2_hash_binds_original_receipts_not_just_member_ids():
    from nexus.shadow.canonical import shadow_price_query_content_hash

    payload = {
        "schema": "shadow_source_query_v2",
        "member_ids": [],
        "receipts": [{"raw_hash": "a" * 64}],
    }
    digest = shadow_price_query_content_hash(payload)
    changed = {**payload, "receipts": [{"raw_hash": "b" * 64}]}
    assert shadow_price_query_content_hash(changed) != digest


def test_collection_request_cannot_supply_complete_or_cursor_state():
    from nexus.shadow.price_collection import ShadowPriceQueryRequest

    with pytest.raises(ValueError):
        ShadowPriceQueryRequest.model_validate(
            {
                "shadow_group_id": str(UUID(int=1)),
                "session_date": "2026-09-02",
                "idempotency_key": "request-1",
                "feed": "sip",
                "delivery_mode": "delayed",
                "complete": True,
            }
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", [None, "digest", "terminal", "query", "missing_page"])
async def test_claimed_receipt_cannot_override_original_query_or_page_chain(defect):
    from nexus.shadow.source_queries import InstrumentPriceReceipt, verify_original_pages

    raw = b'{"symbol":"TEST","bars":[],"next_page_token":null}'
    at = datetime(2026, 9, 3, 5, tzinfo=UTC)
    receipt = {
        "instrument_id": str(UUID(int=1)),
        "alias_id": str(UUID(int=2)),
        "symbol": "TEST",
        "session_date": "2026-09-02",
        "feed": "sip",
        "delivery_mode": "delayed",
        "pages": [
            {
                "raw_receipt_id": str(UUID(int=4)),
                "raw_artifact_id": str(UUID(int=3)),
                "raw_hash": sha256(raw).hexdigest(),
                "raw_byte_length": len(raw),
                "received_at": at.isoformat(),
                "path": ["v2", "stocks", "TEST", "bars"],
                "query": [
                    ["adjustment", "raw"],
                    ["asof", "2026-09-02"],
                    ["currency", "USD"],
                    ["end", "2026-09-03T03:59:59Z"],
                    ["feed", "sip"],
                    ["limit", "10000"],
                    ["sort", "asc"],
                    ["start", "2026-09-02T04:00:00Z"],
                    ["timeframe", "1Day"],
                ],
                "input_cursor": None,
                "output_cursor": None,
                "manifest_id": None,
                "bar_set_id": None,
                "member_ids": [],
            }
        ],
    }
    page = receipt["pages"][0]
    if defect == "digest":
        page["raw_hash"] = "0" * 64
    elif defect == "terminal":
        page["output_cursor"] = "invented"
    elif defect == "query":
        page["query"][1][1] = "2026-09-03"
    elif defect == "missing_page":
        raw = b'{"symbol":"TEST","bars":[],"next_page_token":"next"}'
        page.update(
            raw_hash=sha256(raw).hexdigest(), raw_byte_length=len(raw), output_cursor="next"
        )

    async def read_raw(_page):
        return raw

    if defect is None:
        pages = await verify_original_pages(
            InstrumentPriceReceipt.model_validate(receipt), read_raw
        )
        assert len(pages) == 1 and pages[0].items == () and pages[0].next_cursor is None
    else:
        with pytest.raises(ValueError, match="SHADOW_PRICE_RECEIPT_INVALID"):
            await verify_original_pages(InstrumentPriceReceipt.model_validate(receipt), read_raw)


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol", [None, "OTHER"])
async def test_original_must_identify_requested_symbol(symbol):
    from nexus.shadow.price_collection import daily_request
    from nexus.shadow.source_queries import normalize_original

    at = datetime(2026, 9, 3, 5, tzinfo=UTC)
    payload = {"bars": [], "next_page_token": None}
    if symbol is not None:
        payload["symbol"] = symbol
    with pytest.raises(ValueError, match="SHADOW_PRICE_ORIGINAL_INVALID"):
        await normalize_original(
            json.dumps(payload).encode(),
            daily_request(UUID(int=1), date(2026, 9, 2), at),
            received_at=at,
            feed="sip",
            delivery_mode="delayed",
            symbol="TEST",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extension,value",
    [
        ("available_at", "2020-01-01T00:00:00Z"),
        ("revision", "source-invented"),
        ("quality_flags", []),
    ],
)
async def test_undeclared_source_extensions_cannot_backdate_or_relabel(extension, value):
    from nexus.shadow.price_collection import daily_request
    from nexus.shadow.source_queries import normalize_original

    at = datetime(2026, 9, 3, 5, tzinfo=UTC)
    payload = {
        "symbol": "TEST",
        "next_page_token": None,
        "bars": [
            {
                "t": "2026-09-02T04:00:00Z",
                "o": 1,
                "h": 1,
                "l": 1,
                "c": 1,
                "v": 1,
                extension: value,
            }
        ],
    }
    with pytest.raises(ValueError, match="SHADOW_PRICE_ORIGINAL_INVALID"):
        await normalize_original(
            json.dumps(payload).encode(),
            daily_request(UUID(int=1), date(2026, 9, 2), at),
            received_at=at,
            feed="sip",
            delivery_mode="delayed",
            symbol="TEST",
        )


@pytest.mark.asyncio
async def test_null_bars_remains_unverified_not_negative_evidence():
    from nexus.shadow.price_collection import daily_request
    from nexus.shadow.source_queries import normalize_original

    at = datetime(2026, 9, 3, 5, tzinfo=UTC)
    with pytest.raises(ValueError, match="SHADOW_PRICE_ORIGINAL_INVALID"):
        await normalize_original(
            b'{"symbol":"TEST","bars":null,"next_page_token":null}',
            daily_request(UUID(int=1), date(2026, 9, 2), at),
            received_at=at,
            feed="sip",
            delivery_mode="delayed",
            symbol="TEST",
        )
