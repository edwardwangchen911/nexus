"""Immutable, receipt-bound news observations; no research or trading authority."""

import json
import os
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.providers.base import InstrumentId
from nexus.providers.market.alpaca_http import SqlInstrumentSymbolResolver
from nexus.providers.news import NewsArticle, _unique, parse_news
from nexus.research.fmp_profile_archive import _read

SCHEMA = "nexus_news_v1"
STREAM_SCHEMA = "nexus_news_stream_v1"
SOURCE = "alpaca_news"


class NewsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instrument_id: UUID
    status: Literal["saved", "empty", "unavailable", "invalid"]
    symbol: str | None = None
    received_at: datetime | None = None
    window_start: datetime | None = None
    window_end: datetime | None = None
    content_hash: str | None = None
    has_more: bool = False
    articles: tuple[NewsArticle, ...] = Field(default=(), max_length=50)
    source: Literal["alpaca"] = "alpaca"
    delivery: Literal["delayed_query_latency_unverified", "stream_bounded"] = (
        "delayed_query_latency_unverified"
    )
    coverage: Literal["single_source_bounded"] = "single_source_bounded"
    execution_locked: Literal[True] = True


def validate_archive(raw: bytes, receipt_raw: bytes, *, now: datetime) -> dict[str, object]:
    receipt = json.loads(receipt_raw, object_pairs_hook=_unique)
    if not isinstance(receipt, dict) or set(receipt) != {
        "schema",
        "source",
        "symbol",
        "start",
        "end",
        "received_at",
        "raw_sha256",
        "raw_byte_length",
        "has_more",
    }:
        raise ValueError("NEWS_RECEIPT_INVALID")
    received = require_utc(datetime.fromisoformat(str(receipt.get("received_at", ""))))
    start = require_utc(datetime.fromisoformat(str(receipt.get("start", ""))))
    end = require_utc(datetime.fromisoformat(str(receipt.get("end", ""))))
    symbol = receipt.get("symbol")
    if (
        receipt.get("schema") != "nexus_news_receipt_v1"
        or receipt.get("source") != "alpaca"
        or not isinstance(symbol, str)
        or receipt.get("raw_sha256") != sha256(raw).hexdigest()
        or type(receipt.get("raw_byte_length")) is not int
        or receipt.get("raw_byte_length") != len(raw)
        or not start < end <= received - timedelta(minutes=15)
        or end - start > timedelta(days=7)
        or received > now
    ):
        raise ValueError("NEWS_RECEIPT_INVALID")
    page = parse_news(raw, symbol=symbol, received_at=received)
    if receipt.get("has_more") is not page.has_more or any(
        not start <= article.updated_at <= end for article in page.articles
    ):
        raise ValueError("NEWS_WINDOW_INVALID")
    return receipt


async def import_news(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    instrument_id: UUID,
    directory: Path,
) -> UUID:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if os.fstat(fd).st_mode & 0o077 or set(os.listdir(fd)) != {"raw.json", "receipt.json"}:
            raise ValueError("NEWS_ARCHIVE_INVALID")
        raw, receipt_raw = _read(fd, "raw.json", 8 * 1024 * 1024), _read(fd, "receipt.json", 8192)
    finally:
        os.close(fd)
    receipt = validate_archive(raw, receipt_raw, now=datetime.now(UTC))
    received = datetime.fromisoformat(str(receipt["received_at"]))
    symbol = await SqlInstrumentSymbolResolver(session).resolve(
        InstrumentId(instrument_id), received.date()
    )
    if symbol != receipt["symbol"]:
        raise ValueError("NEWS_IDENTITY_INVALID")
    for content, schema in ((raw, "news_raw_v1"), (receipt_raw, "news_receipt_v1")):
        digest = sha256(content).hexdigest()
        key = f"raw/alpaca/news/{schema}/{digest}.json"
        uri = await store.put_if_absent(key=key, content=content)
        if await store.read_raw(key, uri=uri) != content:
            raise ValueError("NEWS_OBJECT_INVALID")
        await session.execute(
            insert(DataArtifact)
            .values(
                uri=uri,
                sha256=digest,
                byte_size=len(content),
                mime_type="application/json",
                schema_version=schema,
                source=SOURCE,
                observed_at=received,
                available_at=received,
                received_at=received,
            )
            .on_conflict_do_nothing(index_elements=["sha256"])
        )
        artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
        if artifact is None or (
            artifact.uri != uri
            or artifact.source != SOURCE
            or artifact.schema_version != schema
            or artifact.byte_size != len(content)
            or artifact.received_at > received
        ):
            raise ValueError("NEWS_ARTIFACT_CONFLICT")
    identity = dict(
        source=SOURCE,
        provider_object_id=f"news:{instrument_id}",
        revision=sha256(receipt_raw).hexdigest(),
    )
    payload = {"instrument_id": str(instrument_id), "receipt": receipt}
    await session.execute(
        insert(DataSnapshot)
        .values(
            **identity,
            observed_at=received,
            available_at=received,
            received_at=received,
            schema_version=SCHEMA,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    row = await session.scalar(select(DataSnapshot).filter_by(**identity))
    if (
        row is None
        or row.payload != payload
        or row.received_at != received
        or row.schema_version != SCHEMA
    ):
        raise ValueError("NEWS_SNAPSHOT_CONFLICT")
    return row.id


async def import_stream_news(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    instrument_id: UUID,
    directory: Path,
) -> UUID:
    """Persist one prevalidated bounded stream archive; never opens a socket."""
    from nexus.ops.news_stream import load_stream_archive

    archive = load_stream_archive(directory)
    receipt = archive["receipt"]
    receipt_raw = archive["receipt_raw"]
    frames = archive["frames"]
    if (
        not isinstance(receipt, dict)
        or not isinstance(receipt_raw, bytes)
        or not isinstance(frames, tuple)
    ):
        raise ValueError("NEWS_STREAM_ARCHIVE_INVALID")
    if archive["status"] != "bounded_complete":
        raise ValueError("NEWS_STREAM_ARCHIVE_INVALID")
    if len(archive["articles"]) > 50:
        raise ValueError("NEWS_STREAM_ARTICLE_LIMIT")
    ended = require_utc(datetime.fromisoformat(str(receipt.get("ended_at", ""))))
    if ended > datetime.now(UTC) or not archive["subscription_confirmed"]:
        raise ValueError("NEWS_STREAM_ARCHIVE_INVALID")
    symbol = await SqlInstrumentSymbolResolver(session).resolve(
        InstrumentId(instrument_id), ended.date()
    )
    if symbol != receipt.get("symbol"):
        raise ValueError("NEWS_IDENTITY_INVALID")
    entries = receipt.get("frames")
    if not isinstance(entries, list) or len(entries) != len(frames):
        raise ValueError("NEWS_STREAM_ARCHIVE_INVALID")
    artifacts: list[tuple[bytes, str, datetime]] = [(receipt_raw, "news_stream_receipt_v1", ended)]
    for entry, frame in zip(entries, frames, strict=True):
        if not isinstance(entry, dict) or not isinstance(frame, bytes):
            raise ValueError("NEWS_STREAM_ARCHIVE_INVALID")
        artifacts.append(
            (
                frame,
                "news_stream_frame_v1",
                require_utc(datetime.fromisoformat(str(entry.get("received_at", "")))),
            )
        )
    for content, schema, received in artifacts:
        digest = sha256(content).hexdigest()
        key = f"raw/alpaca/news/{schema}/{digest}.json"
        uri = await store.put_if_absent(key=key, content=content)
        if await store.read_raw(key, uri=uri) != content:
            raise ValueError("NEWS_OBJECT_INVALID")
        await session.execute(
            insert(DataArtifact)
            .values(
                uri=uri,
                sha256=digest,
                byte_size=len(content),
                mime_type="application/json",
                schema_version=schema,
                source=SOURCE,
                observed_at=received,
                available_at=received,
                received_at=received,
            )
            .on_conflict_do_nothing(index_elements=["sha256"])
        )
        artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
        if artifact is None or (
            artifact.uri != uri
            or artifact.source != SOURCE
            or artifact.schema_version != schema
            or artifact.byte_size != len(content)
            or artifact.received_at > received
        ):
            raise ValueError("NEWS_ARTIFACT_CONFLICT")
    receipt_hash = sha256(receipt_raw).hexdigest()
    frame_hashes = [sha256(frame).hexdigest() for frame in frames]
    identity = dict(
        source=SOURCE, provider_object_id=f"news:{instrument_id}", revision=receipt_hash
    )
    payload = {
        "instrument_id": str(instrument_id),
        "receipt": receipt,
        "frame_hashes": frame_hashes,
    }
    await session.execute(
        insert(DataSnapshot)
        .values(
            **identity,
            observed_at=ended,
            available_at=ended,
            received_at=ended,
            schema_version=STREAM_SCHEMA,
            payload=payload,
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    row = await session.scalar(select(DataSnapshot).filter_by(**identity))
    if (
        row is None
        or row.payload != payload
        or row.received_at != ended
        or row.schema_version != STREAM_SCHEMA
    ):
        raise ValueError("NEWS_SNAPSHOT_CONFLICT")
    return row.id


async def read_news(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    instrument_id: UUID,
    now: datetime,
    snapshot_id: UUID | None = None,
) -> NewsResponse:
    row = await session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.provider_object_id == f"news:{instrument_id}",
            DataSnapshot.schema_version.in_((SCHEMA, STREAM_SCHEMA)),
            DataSnapshot.available_at <= now,
            DataSnapshot.created_at <= now,
            *(() if snapshot_id is None else (DataSnapshot.id == snapshot_id,)),
        )
        .order_by(DataSnapshot.received_at.desc(), DataSnapshot.created_at.desc())
        .limit(1)
    )
    if row is None:
        return NewsResponse(instrument_id=instrument_id, status="unavailable")
    if row.schema_version == STREAM_SCHEMA:
        return await _read_stream_news(
            row, session=session, store=store, instrument_id=instrument_id, now=now
        )
    try:
        receipt = row.payload["receipt"]
        if not isinstance(receipt, dict) or row.payload.get("instrument_id") != str(instrument_id):
            raise ValueError
        originals = []
        for digest, schema in (
            (str(receipt["raw_sha256"]), "news_raw_v1"),
            (row.revision, "news_receipt_v1"),
        ):
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == digest)
            )
            if (
                artifact is None
                or artifact.source != SOURCE
                or artifact.schema_version != schema
                or max(artifact.available_at, artifact.received_at, artifact.observed_at)
                > row.received_at
                or artifact.created_at > now
            ):
                raise ValueError
            content = await store.read_raw(
                f"raw/alpaca/news/{schema}/{digest}.json", uri=artifact.uri
            )
            if sha256(content).hexdigest() != digest or len(content) != artifact.byte_size:
                raise ValueError
            originals.append(content)
        validated = validate_archive(*originals, now=now)
        if (
            validated != receipt
            or datetime.fromisoformat(str(receipt["received_at"])) != row.received_at
        ):
            raise ValueError
        page = parse_news(originals[0], symbol=str(receipt["symbol"]), received_at=row.received_at)
        return NewsResponse(
            instrument_id=instrument_id,
            status="saved" if page.articles else "empty",
            symbol=str(receipt["symbol"]),
            received_at=row.received_at,
            window_start=datetime.fromisoformat(str(receipt["start"])),
            window_end=datetime.fromisoformat(str(receipt["end"])),
            content_hash=str(receipt["raw_sha256"]),
            has_more=page.has_more,
            articles=page.articles,
        )
    except Exception:
        return NewsResponse(instrument_id=instrument_id, status="invalid")


async def _read_stream_news(
    row: DataSnapshot,
    *,
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    instrument_id: UUID,
    now: datetime,
) -> NewsResponse:
    from nexus.ops.news_stream import parse_stream_archive

    try:
        payload = row.payload
        receipt = payload["receipt"]
        frame_hashes = payload["frame_hashes"]
        if (
            not isinstance(receipt, dict)
            or not isinstance(frame_hashes, list | tuple)
            or payload.get("instrument_id") != str(instrument_id)
        ):
            raise ValueError
        originals: list[bytes] = []
        for digest, schema in (
            (row.revision, "news_stream_receipt_v1"),
            *((str(value), "news_stream_frame_v1") for value in frame_hashes),
        ):
            artifact = await session.scalar(
                select(DataArtifact).where(DataArtifact.sha256 == digest)
            )
            if (
                artifact is None
                or artifact.source != SOURCE
                or artifact.schema_version != schema
                or max(artifact.available_at, artifact.received_at, artifact.observed_at)
                > row.received_at
                or artifact.created_at > now
            ):
                raise ValueError
            content = await store.read_raw(
                f"raw/alpaca/news/{schema}/{digest}.json", uri=artifact.uri
            )
            if sha256(content).hexdigest() != digest or len(content) != artifact.byte_size:
                raise ValueError
            originals.append(content)
        stored_receipt = json.loads(originals[0], object_pairs_hook=_unique)
        validated = parse_stream_archive(stored_receipt, originals[1:])
        ended = require_utc(datetime.fromisoformat(str(stored_receipt["ended_at"])))
        started = require_utc(datetime.fromisoformat(str(stored_receipt["started_at"])))
        if (
            sha256(originals[0]).hexdigest() != row.revision
            or stored_receipt != receipt
            or ended != row.received_at
            or ended > now
            or validated["status"] != "bounded_complete"
            or len(validated["articles"]) > 50
        ):
            raise ValueError
        return NewsResponse(
            instrument_id=instrument_id,
            status="saved" if validated["articles"] else "empty",
            symbol=str(stored_receipt["symbol"]),
            received_at=ended,
            window_start=started,
            window_end=ended,
            content_hash=row.revision,
            has_more=False,
            articles=validated["articles"],
            delivery="stream_bounded",
        )
    except Exception:
        return NewsResponse(instrument_id=instrument_id, status="invalid")
