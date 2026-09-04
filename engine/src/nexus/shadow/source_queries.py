"""Receipt-bound Shadow price queries; completeness describes returned pages only."""

import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.ingestion import MarketObjectStore, object_uri_matches_key
from nexus.market.price_bar_authority import prepare_price_bar_set
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.persistence.models.data import (
    DataArtifact,
    DataSnapshot,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.shadow import ShadowGroupRecord
from nexus.providers.base import BarsRequest, PriceBar, ProviderPage
from nexus.providers.market.alpaca import (
    AlpacaMarketDataProvider,
    AlpacaRawResponse,
    MarketProviderPayloadError,
)
from nexus.providers.market.alpaca_http import _bars_query, load_exact_json
from nexus.shadow.canonical import shadow_price_query_content_hash


class _Receipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class PricePageReceipt(_Receipt):
    raw_receipt_id: UUID
    raw_artifact_id: UUID
    raw_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_byte_length: int = Field(strict=True, gt=0, le=8 * 1024 * 1024)
    received_at: datetime
    path: tuple[str, ...]
    query: tuple[tuple[str, str], ...]
    input_cursor: str | None
    output_cursor: str | None
    manifest_id: UUID | None
    bar_set_id: UUID | None
    member_ids: tuple[UUID, ...] = Field(max_length=10000)

    @field_validator("received_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class InstrumentPriceReceipt(_Receipt):
    instrument_id: UUID
    alias_id: UUID
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,14}$")
    session_date: date
    feed: Literal["iex", "sip"]
    delivery_mode: Literal["real_time", "delayed"]
    pages: tuple[PricePageReceipt, ...] = Field(min_length=1, max_length=10)


class PriceQueryProof(_Receipt):
    schema_version: Literal["shadow_source_query_v2"] = Field(alias="schema")
    shadow_group_id: UUID
    session: date
    kind: Literal["price_bars"]
    source: Literal["alpaca"]
    delivery_mode: Literal["real_time", "delayed"]
    instrument_ids: tuple[UUID, ...] = Field(min_length=1, max_length=10)
    complete: Literal[True]
    member_ids: tuple[UUID, ...] = Field(max_length=10)
    negative_instrument_ids: tuple[UUID, ...] = Field(max_length=10)
    idempotency_key: str = Field(pattern=r"^[A-Za-z0-9._-]{1,64}$")
    calendar_authority_id: UUID
    calendar_version: str
    calendar_hash: str
    proposal_bundle_hash: str
    receipts: tuple[InstrumentPriceReceipt, ...] = Field(min_length=1, max_length=10)
    query_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


def raw_object_key(instrument_id: UUID, received_at: datetime, digest: str) -> str:
    # Content identity is global in DataArtifact, including identical empty pages.
    return f"raw/shadow-price/{digest}.json"


def received_page_payload(
    *,
    root_id: UUID,
    idempotency_key: str,
    instrument_id: UUID,
    alias_id: UUID,
    symbol: str,
    session_date: date,
    feed: str,
    delivery_mode: str,
    artifact_id: UUID,
    digest: str,
    byte_length: int,
    received_at: datetime,
    path: tuple[str, ...],
    query: tuple[tuple[str, str], ...],
    cursor: str | None,
) -> dict[str, object]:
    """Pre-parse receipt: identifies bytes and the request, never their completeness."""
    return {
        "schema": "shadow_price_receipt_v1",
        "shadow_group_id": str(root_id),
        "idempotency_key": idempotency_key,
        "instrument_id": str(instrument_id),
        "alias_id": str(alias_id),
        "symbol": symbol,
        "session": session_date.isoformat(),
        "feed": feed,
        "delivery_mode": delivery_mode,
        "raw_artifact_id": str(artifact_id),
        "raw_hash": digest,
        "raw_byte_length": byte_length,
        "received_at": received_at.isoformat(),
        "path": list(path),
        "query": [list(pair) for pair in query],
        "input_cursor": cursor,
    }


async def verify_price_query(
    session: AsyncSession,
    record: DataSnapshot,
    store: MarketObjectStore,
    *,
    as_of: datetime,
) -> PriceQueryProof:
    """Verify v2 against original bytes, immutable identities and actual normalized rows."""
    from nexus.shadow.service import _load_frozen_targets

    try:
        proof = PriceQueryProof.model_validate(record.payload)
        if (
            record.schema_version != proof.schema_version
            or record.source != proof.source
            or json.dumps(record.payload, sort_keys=True)
            != json.dumps(proof.model_dump(mode="json", by_alias=True), sort_keys=True)
            or record.provider_object_id
            != f"shadow-price:{proof.shadow_group_id}:{proof.idempotency_key}"
            or record.revision != proof.query_hash
            or proof.query_hash != shadow_price_query_content_hash(record.payload)
            or not record.observed_at <= record.available_at <= record.received_at <= as_of
            or record.created_at > as_of
            or sum(len(item.pages) for item in proof.receipts) > 10
        ):
            raise ValueError
        root = await session.get(ShadowGroupRecord, proof.shadow_group_id)
        if (
            root is None
            or root.started_at > as_of
            or (
                proof.session not in root.evaluation_sessions
                or proof.calendar_authority_id != root.calendar_authority_id
                or proof.calendar_version != root.calendar_version
                or proof.calendar_hash != root.calendar_hash
                or proof.proposal_bundle_hash != root.proposal_bundle_hash
            )
        ):
            raise ValueError
        saved_receipt_ids = set(
            await session.scalars(
                select(DataSnapshot.id).where(
                    DataSnapshot.source == "alpaca",
                    DataSnapshot.schema_version == "shadow_price_receipt_v1",
                    DataSnapshot.payload["shadow_group_id"].astext == str(proof.shadow_group_id),
                    DataSnapshot.payload["idempotency_key"].astext == proof.idempotency_key,
                )
            )
        )
        bound_receipt_ids = [page.raw_receipt_id for item in proof.receipts for page in item.pages]
        if (
            len(set(bound_receipt_ids)) != len(bound_receipt_ids)
            or set(bound_receipt_ids) != saved_receipt_ids
        ):
            raise ValueError
        _, _, instruments = await _load_frozen_targets(
            session,
            root.proposal_group_id,
            root.id,
        )
        if (
            proof.instrument_ids != instruments
            or tuple(item.instrument_id for item in proof.receipts) != instruments
        ):
            raise ValueError
        members: list[UUID] = []
        negative: list[UUID] = []
        for receipt in proof.receipts:
            if (
                receipt.session_date != proof.session
                or receipt.delivery_mode != proof.delivery_mode
            ):
                raise ValueError
            alias = await session.get(InstrumentAlias, receipt.alias_id)
            instrument = await session.get(Instrument, receipt.instrument_id)
            if (
                alias is None
                or instrument is None
                or (
                    alias.instrument_id != receipt.instrument_id
                    or alias.symbol != receipt.symbol
                    or alias.alias_type != "ticker"
                    or alias.valid_from > proof.session
                    or (alias.valid_to is not None and alias.valid_to < proof.session)
                    or alias.created_at > receipt.pages[0].received_at
                    or instrument.created_at > receipt.pages[0].received_at
                    or instrument.currency != "USD"
                    or instrument.exchange not in US_EQUITY_EXCHANGES
                    or instrument.asset_type not in {"equity", "common_stock", "equity_etf"}
                )
            ):
                raise ValueError

            async def read_raw(page: PricePageReceipt) -> bytes:
                original_receipt = await session.get(DataSnapshot, page.raw_receipt_id)
                expected = received_page_payload(
                    root_id=proof.shadow_group_id,
                    idempotency_key=proof.idempotency_key,
                    instrument_id=receipt.instrument_id,
                    alias_id=receipt.alias_id,
                    symbol=receipt.symbol,
                    session_date=proof.session,
                    feed=receipt.feed,
                    delivery_mode=receipt.delivery_mode,
                    artifact_id=page.raw_artifact_id,
                    digest=page.raw_hash,
                    byte_length=page.raw_byte_length,
                    received_at=page.received_at,
                    path=page.path,
                    query=page.query,
                    cursor=page.input_cursor,
                )
                if original_receipt is None or (
                    original_receipt.schema_version != "shadow_price_receipt_v1"
                    or original_receipt.source != "alpaca"
                    or original_receipt.provider_object_id
                    != f"shadow-price-page:{page.raw_receipt_id}"
                    or original_receipt.payload != expected
                    or original_receipt.revision != shadow_price_query_content_hash(expected)
                    or original_receipt.observed_at != page.received_at
                    or original_receipt.available_at != page.received_at
                    or original_receipt.received_at != page.received_at
                    or original_receipt.created_at > as_of
                ):
                    raise ValueError
                artifact = await session.get(DataArtifact, page.raw_artifact_id)
                if artifact is None or (
                    artifact.sha256 != page.raw_hash
                    or artifact.byte_size != page.raw_byte_length
                    or artifact.source != "alpaca"
                    or artifact.schema_version != "shadow_price_raw_v1"
                    or artifact.created_at > as_of
                    or page.received_at > record.available_at
                    or max(artifact.observed_at, artifact.available_at, artifact.received_at)
                    > page.received_at
                ):
                    raise ValueError
                key = raw_object_key(receipt.instrument_id, page.received_at, page.raw_hash)
                if not object_uri_matches_key(artifact.uri, key):
                    raise ValueError
                return await store.read_raw(key, uri=artifact.uri)

            pages = await verify_original_pages(receipt, read_raw)
            if not any(page.items for page in pages):
                negative.append(receipt.instrument_id)
            for captured, page in zip(receipt.pages, pages, strict=True):
                if not page.items:
                    if captured.member_ids or captured.manifest_id or captured.bar_set_id:
                        raise ValueError
                    continue
                manifest = await session.get(PriceSeriesManifest, captured.manifest_id)
                authority = await session.get(PriceBarSetAuthority, captured.bar_set_id)
                if (
                    manifest is None
                    or authority is None
                    or (
                        manifest.instrument != str(receipt.instrument_id)
                        or manifest.source != "alpaca"
                        or manifest.feed != receipt.feed
                        or manifest.adjustment != "raw"
                        or manifest.interval != "1Day"
                        or manifest.delivery_mode != receipt.delivery_mode
                        or manifest.created_at > as_of
                        or authority.created_at > as_of
                        or authority.price_series_manifest_id != manifest.id
                    )
                ):
                    raise ValueError
                prepared = await prepare_price_bar_set(session, manifest.id)
                if prepared is None or authority.content_hash != prepared.content_hash:
                    raise ValueError
                rows = list(
                    await session.scalars(
                        select(PriceBarRecord)
                        .where(
                            PriceBarRecord.price_series_manifest_id == manifest.id,
                        )
                        .order_by(PriceBarRecord.id)
                    )
                )
                if tuple(row.id for row in rows) != captured.member_ids or len(rows) != 1:
                    raise ValueError
                row, value = rows[0], page.items[0]
                if (
                    any(
                        getattr(row, field) != getattr(value, field)
                        for field in (
                            "instrument_id",
                            "session",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "observed_at",
                            "available_at",
                            "received_at",
                            "revision",
                        )
                    )
                    or row.quality_flags != list(value.quality_flags)
                    or row.created_at > as_of
                ):
                    raise ValueError
                members.extend(captured.member_ids)
        if tuple(sorted(members, key=lambda item: item.int)) != proof.member_ids or (
            tuple(negative) != proof.negative_instrument_ids
        ):
            raise ValueError
        return proof
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("SHADOW_PRICE_PROOF_INVALID") from error


async def verify_original_pages(
    receipt: InstrumentPriceReceipt,
    read_raw: Callable[[PricePageReceipt], Awaitable[bytes]],
) -> tuple[ProviderPage[PriceBar], ...]:
    from nexus.shadow.price_collection import daily_request

    result: list[ProviderPage[PriceBar]] = []
    cursor: str | None = None
    seen: set[str] = set()
    last_received: datetime | None = None
    try:
        for index, stored in enumerate(receipt.pages):
            request = daily_request(receipt.instrument_id, receipt.session_date, stored.received_at)
            query = (
                *_bars_query(request, cursor, receipt.feed),
                ("asof", receipt.session_date.isoformat()),
            )
            if (
                stored.input_cursor != cursor
                or stored.query != query
                or stored.path != ("v2", "stocks", receipt.symbol, "bars")
                or (last_received is not None and stored.received_at < last_received)
            ):
                raise ValueError
            raw = await read_raw(stored)
            if len(raw) != stored.raw_byte_length or sha256(raw).hexdigest() != stored.raw_hash:
                raise ValueError
            page = await normalize_original(
                raw,
                request,
                received_at=stored.received_at,
                feed=receipt.feed,
                delivery_mode=receipt.delivery_mode,
                symbol=receipt.symbol,
            )
            if page.next_cursor != stored.output_cursor or (
                (index == len(receipt.pages) - 1) != (page.next_cursor is None)
            ):
                raise ValueError
            cursor, last_received = page.next_cursor, stored.received_at
            if cursor is not None:
                if cursor in seen:
                    raise ValueError
                seen.add(cursor)
            result.append(page)
        # A single daily query cannot stitch repeated/corrected bars across pages.
        if sum(len(page.items) for page in result) > 1:
            raise ValueError
        return tuple(result)
    except (ValueError, MarketProviderPayloadError) as error:
        raise ValueError("SHADOW_PRICE_RECEIPT_INVALID") from error


async def normalize_original(
    raw: bytes,
    request: BarsRequest,
    *,
    received_at: datetime,
    feed: Literal["iex", "sip"],
    delivery_mode: Literal["real_time", "delayed"],
    symbol: str,
) -> ProviderPage[PriceBar]:
    """Re-run the actual provider over exact archived bytes, never a caller payload."""
    try:
        require_utc(received_at)
        value = load_exact_json(raw)
        if not isinstance(value, Mapping) or value.get("symbol") != symbol:
            raise ValueError
        raw_bars = value.get("bars")
        if not isinstance(raw_bars, list) or any(
            not isinstance(bar, Mapping)
            or any(field in bar for field in ("available_at", "revision", "quality_flags"))
            for bar in raw_bars
        ):
            # These fixture extensions are not part of this source contract.
            raise ValueError

        class ArchivedTransport:
            async def fetch_bars(
                self,
                request: BarsRequest,
                cursor: str | None,
            ) -> AlpacaRawResponse:
                return AlpacaRawResponse(
                    payload=cast(Mapping[str, object], value),
                    raw_payload=raw,
                    request_id=None,
                )

        page = await AlpacaMarketDataProvider(
            ArchivedTransport(),
            feed=feed,
            delivery_mode=delivery_mode,
            now=lambda: received_at,
        ).fetch_bars(request)
        if any(
            item.observed_at != request.start or item.available_at > received_at
            for item in page.items
        ):
            raise ValueError
        return page
    except (ValueError, MarketProviderPayloadError, RecursionError) as error:
        raise ValueError("SHADOW_PRICE_ORIGINAL_INVALID") from error
