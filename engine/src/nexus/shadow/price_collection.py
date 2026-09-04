"""One explicit, raw-price query; no Shadow lifecycle or trading effects."""

from asyncio import timeout
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.jobs.daily_pipeline import assert_daily_pipeline_schema_ready
from nexus.market.ingestion import MarketObjectStore, ingest_price_page, object_uri_matches_key
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.data import DataArtifact, DataSnapshot, PriceBarRecord
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.shadow import ShadowGroupRecord
from nexus.providers.base import BarsRequest, InstrumentId
from nexus.providers.market.alpaca import AlpacaRawResponse, MarketProviderPayloadError
from nexus.providers.market.alpaca_http import (
    FixedOriginBytes,
    _bars_query,
    _optional_request_id,
    _validated_us_symbol,
    load_exact_json,
)

_NEW_YORK = ZoneInfo("America/New_York")


async def collect_shadow_prices(
    factory: async_sessionmaker[AsyncSession],
    request: "ShadowPriceQueryRequest",
    *,
    client: FixedOriginBytes,
    store: MarketObjectStore,
    key_id: SecretStr,
    secret: SecretStr,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, object]:
    from nexus.shadow.authority import LockedShadowRoot, load_shadow_session_input
    from nexus.shadow.canonical import shadow_price_query_content_hash
    from nexus.shadow.service import _load_frozen_targets
    from nexus.shadow.source_queries import (
        InstrumentPriceReceipt,
        PricePageReceipt,
        PriceQueryProof,
        normalize_original,
        raw_object_key,
        received_page_payload,
        verify_original_pages,
        verify_price_query,
    )

    async with timeout(120), factory() as session, session.begin():
        await assert_daily_pipeline_schema_ready(session)
        object_id = f"shadow-price:{request.shadow_group_id}:{request.idempotency_key}"
        locked = await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": object_id}
        )
        if not locked:
            raise ValueError("SHADOW_PRICE_QUERY_BUSY")
        existing = list(
            await session.scalars(
                select(DataSnapshot).where(
                    DataSnapshot.source == "alpaca",
                    DataSnapshot.provider_object_id == object_id,
                )
            )
        )
        root = await session.get(ShadowGroupRecord, request.shadow_group_id)
        clock = require_utc(now())
        if (
            root is None
            or root.started_at > clock
            or request.session_date not in root.evaluation_sessions
        ):
            raise ValueError("SHADOW_PRICE_ROOT_INVALID")
        _, _, instruments = await _load_frozen_targets(session, root.proposal_group_id, root.id)
        if not 1 <= len(instruments) <= 10:
            raise ValueError("SHADOW_PRICE_SCOPE_LIMIT")
        daily_request(instruments[0], request.session_date, clock)
        if existing:
            if len(existing) != 1:
                raise ValueError("SHADOW_PRICE_REPLAY_CONFLICT")
            record = existing[0]
            proof = await verify_price_query(session, record, store, as_of=clock)
            if (
                proof.session != request.session_date
                or proof.shadow_group_id != request.shadow_group_id
                or proof.idempotency_key != request.idempotency_key
                or any(
                    item.feed != request.feed or item.delivery_mode != request.delivery_mode
                    for item in proof.receipts
                )
            ):
                raise ValueError("SHADOW_PRICE_REPLAY_CONFLICT")
        else:
            partial_receipt = await session.scalar(
                select(DataSnapshot.id)
                .where(
                    DataSnapshot.source == "alpaca",
                    DataSnapshot.schema_version == "shadow_price_receipt_v1",
                    DataSnapshot.payload["shadow_group_id"].astext == str(request.shadow_group_id),
                    DataSnapshot.payload["idempotency_key"].astext == request.idempotency_key,
                )
                .limit(1)
            )
            if partial_receipt is not None:
                raise ValueError("SHADOW_PRICE_INCOMPLETE_REPLAY")
            receipts: list[InstrumentPriceReceipt] = []
            all_members: list[UUID] = []
            negative: list[UUID] = []
            remaining = 10
            for instrument_id in instruments:
                instrument = await session.get(Instrument, instrument_id)
                aliases = list(
                    await session.scalars(
                        select(InstrumentAlias).where(
                            InstrumentAlias.instrument_id == instrument_id,
                            InstrumentAlias.alias_type == "ticker",
                            InstrumentAlias.valid_from <= request.session_date,
                            or_(
                                InstrumentAlias.valid_to.is_(None),
                                InstrumentAlias.valid_to >= request.session_date,
                            ),
                            InstrumentAlias.created_at <= clock,
                        )
                    )
                )
                if (
                    instrument is None
                    or instrument.currency != "USD"
                    or instrument.exchange not in US_EQUITY_EXCHANGES
                    or instrument.asset_type not in {"equity", "common_stock", "equity_etf"}
                    or instrument.created_at > clock
                    or len(aliases) != 1
                ):
                    raise ValueError("SHADOW_PRICE_SYMBOL_INVALID")
                alias = aliases[0]
                raw_receipt_id: UUID | None = None

                async def archive(raw: bytes, received: datetime) -> UUID:
                    nonlocal raw_receipt_id
                    if not 0 < len(raw) <= 8 * 1024 * 1024:
                        raise ValueError("SHADOW_PRICE_RAW_LIMIT")
                    digest = sha256(raw).hexdigest()
                    key = raw_object_key(instrument_id, received, digest)
                    uri = await store.archive_raw(key, raw)
                    if (
                        not object_uri_matches_key(uri, key)
                        or await store.read_raw(key, uri=uri) != raw
                    ):
                        raise ValueError("SHADOW_PRICE_ARCHIVE_INVALID")
                    async with factory() as raw_session, raw_session.begin():
                        await raw_session.execute(
                            insert(DataArtifact)
                            .values(
                                id=new_uuid7(),
                                uri=uri,
                                sha256=digest,
                                byte_size=len(raw),
                                mime_type="application/json",
                                schema_version="shadow_price_raw_v1",
                                source="alpaca",
                                observed_at=received,
                                available_at=received,
                                received_at=received,
                            )
                            .on_conflict_do_nothing(index_elements=[DataArtifact.sha256])
                        )
                        artifact = await raw_session.scalar(
                            select(DataArtifact).where(DataArtifact.sha256 == digest)
                        )
                        if (
                            artifact is None
                            or artifact.uri != uri
                            or artifact.byte_size != len(raw)
                            or (
                                artifact.schema_version != "shadow_price_raw_v1"
                                or artifact.source != "alpaca"
                                or artifact.received_at > received
                            )
                        ):
                            raise ValueError("SHADOW_PRICE_ARCHIVE_CONFLICT")
                        raw_receipt_id = new_uuid7()
                        payload = received_page_payload(
                            root_id=root.id,
                            idempotency_key=request.idempotency_key,
                            instrument_id=instrument_id,
                            alias_id=alias.id,
                            symbol=alias.symbol,
                            session_date=request.session_date,
                            feed=request.feed,
                            delivery_mode=request.delivery_mode,
                            artifact_id=artifact.id,
                            digest=digest,
                            byte_length=len(raw),
                            received_at=received,
                            path=("v2", "stocks", alias.symbol, "bars"),
                            query=transport.query,
                            cursor=cursor,
                        )
                        raw_session.add(
                            DataSnapshot(
                                id=raw_receipt_id,
                                source="alpaca",
                                provider_object_id=f"shadow-price-page:{raw_receipt_id}",
                                revision=shadow_price_query_content_hash(payload),
                                schema_version="shadow_price_receipt_v1",
                                payload=payload,
                                observed_at=received,
                                available_at=received,
                                received_at=received,
                            )
                        )
                        return artifact.id

                transport = ShadowPriceTransport(
                    client,
                    symbol=alias.symbol,
                    session_date=request.session_date,
                    feed=request.feed,
                    key_id=key_id,
                    secret=secret,
                    archive=archive,
                    now=now,
                )
                query = daily_request(instrument_id, request.session_date, clock)
                cursor: str | None = None
                pages: list[PricePageReceipt] = []
                raw_pages: dict[UUID, bytes] = {}
                while True:
                    if remaining == 0:
                        raise ValueError("SHADOW_PRICE_PAGE_LIMIT")
                    remaining -= 1
                    response = await transport.fetch_bars(query, cursor)
                    received, artifact_id = transport.received_at, transport.raw_artifact_id
                    assert received is not None and artifact_id is not None
                    assert raw_receipt_id is not None
                    page = await normalize_original(
                        response.raw_payload,
                        query,
                        received_at=received,
                        feed=request.feed,
                        delivery_mode=request.delivery_mode,
                        symbol=alias.symbol,
                    )
                    manifest_id = bar_set_id = None
                    members: tuple[UUID, ...] = ()
                    if page.items:
                        ingested = await ingest_price_page(session, store, page, commit=False)
                        if ingested.price_authority is None:
                            raise ValueError("SHADOW_PRICE_BAR_AUTHORITY_INVALID")
                        manifest_id, bar_set_id = ingested.manifest.id, ingested.price_authority.id
                        members = tuple(
                            await session.scalars(
                                select(PriceBarRecord.id)
                                .where(
                                    PriceBarRecord.price_series_manifest_id == manifest_id,
                                )
                                .order_by(PriceBarRecord.id)
                            )
                        )
                    pages.append(
                        PricePageReceipt(
                            raw_receipt_id=raw_receipt_id,
                            raw_artifact_id=artifact_id,
                            raw_hash=sha256(response.raw_payload).hexdigest(),
                            raw_byte_length=len(response.raw_payload),
                            received_at=received,
                            path=("v2", "stocks", alias.symbol, "bars"),
                            query=transport.query,
                            input_cursor=cursor,
                            output_cursor=page.next_cursor,
                            manifest_id=manifest_id,
                            bar_set_id=bar_set_id,
                            member_ids=members,
                        )
                    )
                    raw_pages[artifact_id] = response.raw_payload
                    if page.next_cursor is None:
                        break
                    if page.next_cursor in {item.input_cursor for item in pages}:
                        raise ValueError("SHADOW_PRICE_CURSOR_CYCLE")
                    cursor = page.next_cursor
                receipt = InstrumentPriceReceipt(
                    instrument_id=instrument_id,
                    alias_id=alias.id,
                    symbol=alias.symbol,
                    session_date=request.session_date,
                    feed=request.feed,
                    delivery_mode=request.delivery_mode,
                    pages=tuple(pages),
                )

                async def read_original(item: PricePageReceipt) -> bytes:
                    return raw_pages[item.raw_artifact_id]

                await verify_original_pages(receipt, read_original)
                instrument_members = [member for item in pages for member in item.member_ids]
                all_members.extend(instrument_members)
                if not instrument_members:
                    negative.append(instrument_id)
                receipts.append(receipt)
            proof = PriceQueryProof.model_validate(
                {
                    "schema": "shadow_source_query_v2",
                    "shadow_group_id": root.id,
                    "session": request.session_date,
                    "kind": "price_bars",
                    "source": "alpaca",
                    "delivery_mode": request.delivery_mode,
                    "instrument_ids": instruments,
                    "complete": True,
                    "member_ids": sorted(all_members, key=lambda item: item.int),
                    "negative_instrument_ids": negative,
                    "idempotency_key": request.idempotency_key,
                    "calendar_authority_id": root.calendar_authority_id,
                    "calendar_version": root.calendar_version,
                    "calendar_hash": root.calendar_hash,
                    "proposal_bundle_hash": root.proposal_bundle_hash,
                    "receipts": receipts,
                    "query_hash": "0" * 64,
                }
            )
            payload = proof.model_dump(mode="json", by_alias=True)
            payload["query_hash"] = shadow_price_query_content_hash(payload)
            clock = require_utc(now())
            record = DataSnapshot(
                source="alpaca",
                provider_object_id=object_id,
                revision=payload["query_hash"],
                schema_version="shadow_source_query_v2",
                payload=payload,
                observed_at=clock,
                available_at=clock,
                received_at=clock,
            )
            session.add(record)
            await session.flush()
            await verify_price_query(session, record, store, as_of=clock)
        ordinal = root.evaluation_sessions.index(request.session_date) + 1
        authority = await load_shadow_session_input(
            session,
            root=LockedShadowRoot.from_record(root),
            session_date=request.session_date,
            ordinal=ordinal,
            observed_at=clock,
            required_instruments=instruments,
            initial_execution_instruments=instruments if ordinal == 1 else (),
            query_artifact_reader=store,
        )
        result: dict[str, object] = {
            "query_snapshot_id": str(record.id),
            "shadow_group_id": str(root.id),
            "price_return_set_complete": True,
            "price_use_authorized": False,
            "execution_locked": True,
            "shadow_input_status": authority.status,
        }
    return result


class ShadowPriceQueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    shadow_group_id: UUID
    session_date: date
    idempotency_key: str = Field(pattern=r"^[A-Za-z0-9._-]{1,64}$")
    feed: Literal["iex", "sip"]
    delivery_mode: Literal["real_time", "delayed"]


def daily_request(instrument_id: UUID, session_date: date, now: datetime) -> BarsRequest:
    now = require_utc(now)
    if type(session_date) is not date or session_date >= now.astimezone(_NEW_YORK).date():
        raise ValueError("SHADOW_PRICE_DAY_NOT_CLOSED")
    start = datetime.combine(session_date, time.min, _NEW_YORK).astimezone(UTC)
    end = datetime.combine(session_date + timedelta(days=1), time.min, _NEW_YORK).astimezone(UTC)
    return BarsRequest(
        instrument_id=InstrumentId(instrument_id),
        start=start,
        end=end - timedelta(seconds=1),
        interval="1Day",
        adjustment="raw",
    )


class ShadowPriceTransport:
    """Opt-in historical symbol mapping; ordinary Alpaca transport remains unchanged."""

    def __init__(
        self,
        client: FixedOriginBytes,
        *,
        symbol: str,
        session_date: date,
        feed: Literal["iex", "sip"],
        key_id: SecretStr,
        secret: SecretStr,
        archive: Callable[[bytes, datetime], Awaitable[UUID]],
        now: Callable[[], datetime],
    ) -> None:
        if not key_id.get_secret_value().strip() or not secret.get_secret_value().strip():
            raise ValueError("SHADOW_PRICE_CREDENTIALS_REQUIRED")
        self.client, self.symbol, self.session_date = (
            client,
            _validated_us_symbol(symbol),
            session_date,
        )
        self.feed, self.key_id, self.secret = feed, key_id, secret
        self.archive, self.now = archive, now
        self.received_at: datetime | None = None
        self.raw_artifact_id: UUID | None = None
        self.query: tuple[tuple[str, str], ...] = ()

    async def fetch_bars(self, request: BarsRequest, cursor: str | None) -> AlpacaRawResponse:
        expected = daily_request(request.instrument_id, self.session_date, self.now())
        if request != expected:
            raise ValueError("SHADOW_PRICE_REQUEST_MISMATCH")
        self.query = (
            *_bars_query(request, cursor, self.feed),
            ("asof", self.session_date.isoformat()),
        )
        raw = await self.client.get_bytes(
            ("v2", "stocks", self.symbol, "bars"),
            self.query,
            (
                ("APCA-API-KEY-ID", self.key_id.get_secret_value()),
                ("APCA-API-SECRET-KEY", self.secret.get_secret_value()),
            ),
        )
        self.received_at = require_utc(self.now())
        self.raw_artifact_id = await self.archive(raw, self.received_at)
        value = load_exact_json(raw)
        if not isinstance(value, Mapping):
            raise MarketProviderPayloadError("ALPACA_OBJECT_REQUIRED")
        return AlpacaRawResponse(
            payload=value,
            raw_payload=raw,
            request_id=_optional_request_id(value.get("request_id")),
        )
