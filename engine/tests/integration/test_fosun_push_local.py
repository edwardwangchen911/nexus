"""Prepared Fosun push authority is explicit, bounded, and archive-bindable."""

import base64
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.jobs.daily_payloads import instrument_alias_identity_sha256
from nexus.market.service import MarketService
from nexus.ops.fosun_local_refresh import _ensure_market_bindings, _ensure_market_universe
from nexus.ops.fosun_push_local import import_push_quote, prepare_push_authority
from nexus.persistence.models.data import DataArtifact, ProviderCapability
from nexus.persistence.models.market import Instrument, InstrumentAlias, LatestQuote
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

PREPARED_AT = datetime(2026, 9, 9, 9, 0, tzinfo=UTC)
INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-0000000000a1")
ALIAS_ID = UUID("019d0000-0000-7000-8000-0000000000a2")
ALIAS_CREATED_AT = PREPARED_AT - timedelta(seconds=2)


async def _seed_identity(session) -> None:
    session.add(
        Instrument(
            id=INSTRUMENT_ID,
            asset_type="equity",
            exchange="XNAS",
            currency="USD",
            tick_size=Decimal("0.01"),
            risk_bucket="us_equity",
            created_at=ALIAS_CREATED_AT,
        )
    )
    await session.flush()
    await _ensure_market_universe(
        session,
        instrument_ids=(INSTRUMENT_ID,),
        published_at=ALIAS_CREATED_AT,
        effective_from=date(2020, 1, 1),
    )
    session.add(
        InstrumentAlias(
            id=ALIAS_ID,
            instrument_id=INSTRUMENT_ID,
            symbol="AAPL",
            alias_type="ticker",
            valid_from=date(2020, 1, 1),
            valid_to=None,
            created_at=ALIAS_CREATED_AT,
        )
    )


@pytest.mark.asyncio
async def test_prepare_push_authority_adds_only_subscription_capability(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_identity(session)
        await _ensure_market_bindings(
            session,
            authorized_at=PREPARED_AT - timedelta(seconds=1),
            delivery_mode="unknown",
        )
    async with factory() as session, session.begin():
        prepared = await prepare_push_authority(session, code="usAAPL", prepared_at=PREPARED_AT)

    document = json.loads(prepared.payload)
    assert prepared.sha256 == sha256(prepared.payload).hexdigest()
    assert document == {
        "capability_expires_at": "2027-09-09T09:00:00Z",
        "capability_id": document["capability_id"],
        "contract_hash": sha256(b"nexus.mac-local-fosun-read-only.v2").hexdigest(),
        "delivery_mode": "unknown",
        "instrument_alias": {
            "alias_id": str(ALIAS_ID),
            "alias_identity_sha256": instrument_alias_identity_sha256(
                alias_id=str(ALIAS_ID),
                instrument_id=str(INSTRUMENT_ID),
                symbol="AAPL",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
                created_at=ALIAS_CREATED_AT,
            ),
            "alias_type": "ticker",
            "created_at": "2026-09-09T08:59:58Z",
            "instrument_id": str(INSTRUMENT_ID),
            "symbol": "AAPL",
            "valid_from": "2020-01-01",
            "valid_to": None,
        },
        "operation": "fosun_subscribe_quotes",
        "prepared_at": "2026-09-09T09:00:00Z",
        "provider": "fosun",
        "schema": "nexus_fosun_push_authority_v1",
        "status_id": document["status_id"],
        "status_version": 1,
    }
    async with factory() as session:
        operations = tuple(
            await session.scalars(
                select(ProviderCapability.payload["operation"].as_string()).order_by(
                    ProviderCapability.payload["operation"].as_string()
                )
            )
        )
        count = await session.scalar(select(func.count()).select_from(ProviderCapability))
    assert count == 3
    assert operations == ("fosun_get_bars", "fosun_get_quotes", "fosun_subscribe_quotes")


@pytest.mark.asyncio
async def test_prepare_push_authority_requires_existing_read_only_status(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_identity(session)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="FOSUN_PUSH_AUTHORITY_REQUIRED"):
            await prepare_push_authority(session, code="usAAPL", prepared_at=PREPARED_AT)

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(ProviderCapability)) == 0


@pytest.mark.asyncio
async def test_import_push_quote_persists_verified_display_snapshot_idempotently(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_identity(session)
        await _ensure_market_bindings(
            session,
            authorized_at=PREPARED_AT - timedelta(seconds=1),
            delivery_mode="unknown",
        )
    async with factory() as session, session.begin():
        prepared = await prepare_push_authority(session, code="usAAPL", prepared_at=PREPARED_AT)

    started_at = PREPARED_AT + timedelta(seconds=1)
    frame_at = PREPARED_AT + timedelta(seconds=2)
    completed_at = PREPARED_AT + timedelta(seconds=3)
    subscription_frame = json.dumps(
        {
            "topic": "subrep",
            "requestid": "synthetic-request",
            "subRep": [{"topic": "qt", "scode": ["usAAPL"]}],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    dynamic_quote = {
        "qtTime": 1788927000000,
        "qtDate": 20260909,
        "power": 3,
        "tradeStatus": 1,
        "price": 201125,
        "chgVal": 1,
        "chgPct": 1,
        "pClose": 200000,
        "open": 200000,
        "high": 202000,
        "low": 199000,
        "vol": 100,
        "turnover": 20112500,
    }
    quote_frame = json.dumps(
        {
            "topic": "qt",
            "id": 1,
            "items": [
                {
                    "code": "usAAPL",
                    "msg": json.dumps(dynamic_quote, sort_keys=True, separators=(",", ":")),
                }
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    frames = (subscription_frame, quote_frame)
    receipt = json.dumps(
        {
            "schema": "nexus_fosun_push_receipt_v3",
            "source": "fosun",
            "transport": "websocket",
            "origin": "wss://openapi-push.fosunxcz.com/msg/v1/push",
            "topic": "qt",
            "status": "bounded_complete",
            "code": "usAAPL",
            "request_id": "synthetic-request",
            "started_at": started_at.isoformat(),
            "ended_at": completed_at.isoformat(),
            "requested_duration_seconds": 20.0,
            "requested_max_frames": 2,
            "heartbeat_interval_seconds": 15.0,
            "elapsed_monotonic_seconds": 3.0,
            "subscription_confirmed": True,
            "stop_reason": "FRAME_LIMIT",
            "frame_count": 2,
            "quote_message_count": 1,
            "heartbeat_sent_count": 0,
            "heartbeat_ack_count": 0,
            "heartbeats": [],
            "outbound_send_timed_out": False,
            "frames": [
                {
                    "file": f"frame-{index:05}.json",
                    "received_at": received_at.isoformat(),
                    "raw_sha256": sha256(frame).hexdigest(),
                    "raw_byte_length": len(frame),
                }
                for index, (received_at, frame) in enumerate(
                    zip((started_at, frame_at), frames, strict=True)
                )
            ],
            "authority_sha256": prepared.sha256,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    raw_archive = json.dumps(
        {
            "schema": "fosun_push_quote_archive_v1",
            "receipt_base64": base64.b64encode(receipt).decode(),
            "frames_base64": [base64.b64encode(frame).decode() for frame in frames],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    raw_digest = sha256(raw_archive).hexdigest()
    handoff = json.dumps(
        {
            "schema": "nexus_fosun_push_quote_handoff_v1",
            "code": "usAAPL",
            "price": "201.125",
            "provider_quote_date": 20260909,
            "provider_quote_time": 1788927000000,
            "archive_started_at": started_at.isoformat(),
            "frame_received_at": frame_at.isoformat().replace("+00:00", "Z"),
            "archive_completed_at": completed_at.isoformat().replace("+00:00", "Z"),
            "authority_sha256": prepared.sha256,
            "raw_archive_sha256": raw_digest,
            "raw_archive_base64": base64.b64encode(raw_archive).decode(),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()

    class Store:
        def __init__(self) -> None:
            self.objects: dict[str, bytes] = {}

        async def archive_raw(self, key: str, payload: bytes) -> str:
            self.objects.setdefault(key, payload)
            return f"memory://{key}"

        async def read_raw(self, key: str, *, uri: str) -> bytes:
            assert uri == f"memory://{key}"
            return self.objects[key]

    store = Store()
    invalid_time = json.loads(handoff)
    invalid_time["archive_started_at"] = (
        (PREPARED_AT - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    )
    with pytest.raises(ValueError, match="FOSUN_PUSH_TIME_ORDER_INVALID"):
        async with factory() as session, session.begin():
            await import_push_quote(
                session,
                store,
                handoff_payload=json.dumps(
                    invalid_time, sort_keys=True, separators=(",", ":")
                ).encode(),
                authority_payload=prepared.payload,
                imported_at=completed_at + timedelta(seconds=1),
            )
    assert store.objects == {}
    invalid_price = json.loads(handoff)
    invalid_price["price"] = "999.99"
    with pytest.raises(ValueError, match="FOSUN_PUSH_HANDOFF_ARCHIVE_MISMATCH"):
        async with factory() as session, session.begin():
            await import_push_quote(
                session,
                store,
                handoff_payload=json.dumps(
                    invalid_price, sort_keys=True, separators=(",", ":")
                ).encode(),
                authority_payload=prepared.payload,
                imported_at=completed_at + timedelta(seconds=1),
            )
    assert store.objects == {}

    for replay in range(2):
        async with factory() as session, session.begin():
            result = await import_push_quote(
                session,
                store,
                handoff_payload=handoff,
                authority_payload=prepared.payload,
                imported_at=completed_at + timedelta(seconds=1 + replay),
            )
        assert result.instrument_id == INSTRUMENT_ID
        assert result.raw_sha256 == raw_digest

    async with factory() as session:
        artifact = await session.scalar(select(DataArtifact))
        quote = await session.scalar(select(LatestQuote))
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 1
    assert artifact is not None
    assert artifact.schema_version == "fosun_push_quote_archive_v1"
    assert artifact.sha256 == raw_digest
    assert quote is not None
    assert quote.instrument_id == INSTRUMENT_ID
    assert quote.source == "fosun"
    assert quote.feed == "l1"
    assert quote.delivery_mode == "unknown"
    assert quote.event_kind == "snapshot"
    assert quote.last == Decimal("201.125")
    assert quote.observed_at == frame_at
    assert quote.available_at == completed_at
    assert quote.received_at == completed_at
    assert quote.revision == raw_digest
    assert quote.quality_flags == [
        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
        "INTRADAY_BID_ASK_UNAVAILABLE",
    ]
    assert quote.evidence_status == "verified"
    assert quote.data_artifact_id == artifact.id
    assert quote.provider_authorized_at == completed_at + timedelta(seconds=1)
    async with factory() as session:
        page, _ = await MarketService(
            session,
            completed_at + timedelta(seconds=10),
            cursor_signing_secrets=("synthetic-test-secret",),
        ).market_page(
            limit=10,
            cursor=None,
            watchlist=None,
            selected_instrument_id=None,
        )
    assert len(page.items) == 1
    assert page.items[0].price == "201.125000000000"
    assert page.items[0].source == "fosun"
    assert page.items[0].delivery_mode == "unknown"
