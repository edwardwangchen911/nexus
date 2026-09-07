"""Optional exact-news research observations, separate from the live message view."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.news import NewsArticle, parse_news
from nexus.research.news import SOURCE, read_news, validate_archive

SCHEMA = "news_research_v1"


class NewsObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["news_research_v1"] = "news_research_v1"
    instrument_id: UUID
    news_snapshot_id: UUID
    symbol: str
    available_at: datetime
    received_at: datetime
    window_start: datetime
    window_end: datetime
    selection: Literal["latest_updated_5_v1"] = "latest_updated_5_v1"
    articles: tuple[NewsArticle, ...] = Field(min_length=1, max_length=5)
    collected_count: int = Field(ge=1, le=50)
    has_more: bool
    raw_hash: str
    receipt_hash: str
    raw_text: str
    receipt_text: str
    primary_source_verified: Literal[False] = False
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    @property
    def object_id(self) -> str:
        return f"news-research:{self.instrument_id}:{self.news_snapshot_id}:latest5-v1"

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


def news_tool_payload(document: NewsObservation) -> dict[str, object]:
    return {
        **document.model_dump(mode="json", exclude={"raw_text", "receipt_text"}),
        "scope": (
            "供应商新闻标题及摘要，未独立核实；按更新时间选取本页最新5条，非全面覆盖。"
            "缺少摘要时不能推测正文；发布时间不等于本地可用时间。"
        ),
    }


def _validate_document(document: NewsObservation) -> None:
    raw, receipt_raw = document.raw_text.encode(), document.receipt_text.encode()
    receipt = validate_archive(raw, receipt_raw, now=document.available_at)
    page = parse_news(raw, symbol=document.symbol, received_at=document.received_at)
    selected = tuple(sorted(page.articles, key=lambda a: (a.updated_at, a.id), reverse=True)[:5])
    if (
        sha256(raw).hexdigest() != document.raw_hash
        or sha256(receipt_raw).hexdigest() != document.receipt_hash
        or receipt["symbol"] != document.symbol
        or datetime.fromisoformat(str(receipt["received_at"])) != document.received_at
        or datetime.fromisoformat(str(receipt["start"])) != document.window_start
        or datetime.fromisoformat(str(receipt["end"])) != document.window_end
        or selected != document.articles
        or len(page.articles) != document.collected_count
        or page.has_more != document.has_more
    ):
        raise ValueError("NEWS_RESEARCH_ORIGINAL_INVALID")


async def read_news_evidence(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...],
) -> dict[UUID, NewsObservation]:
    """Only requested frozen references; no latest selection or fallback."""
    if not evidence_ids:
        return {}
    rows = (
        await session.execute(
            select(ResearchEvidenceRef, DataSnapshot)
            .join(
                DataSnapshot,
                (DataSnapshot.source == ResearchEvidenceRef.source)
                & (DataSnapshot.provider_object_id == ResearchEvidenceRef.provider_object_id)
                & (DataSnapshot.revision == ResearchEvidenceRef.revision),
            )
            .where(
                ResearchEvidenceRef.id.in_(evidence_ids),
                ResearchEvidenceRef.source == SOURCE,
                ResearchEvidenceRef.available_at <= as_of,
                ResearchEvidenceRef.created_at <= as_of,
                DataSnapshot.schema_version == SCHEMA,
                DataSnapshot.created_at <= as_of,
            )
        )
    ).all()
    result = {}
    for reference, row in rows:
        try:
            doc = NewsObservation.model_validate(row.payload)
            content = doc.canonical_bytes()
            if not (
                doc.instrument_id == instrument_id
                and doc.object_id == row.provider_object_id
                and sha256(content).hexdigest() == row.revision == reference.content_hash
                and len(content) == reference.byte_length
                and doc.available_at
                == row.available_at
                == row.observed_at
                == row.received_at
                == reference.available_at
                and doc.received_at <= doc.available_at <= as_of
            ):
                continue
            original = await session.get(DataSnapshot, doc.news_snapshot_id)
            if original is None or not (
                original.source == SOURCE
                and original.schema_version == "nexus_news_v1"
                and original.provider_object_id == f"news:{instrument_id}"
                and original.revision == doc.receipt_hash
                and original.payload
                == {"instrument_id": str(instrument_id), "receipt": json.loads(doc.receipt_text)}
                and original.received_at == original.available_at == doc.received_at
                and original.created_at <= doc.available_at
            ):
                continue
            _validate_document(doc)
            checks = []
            for digest, size, schema in (
                (doc.raw_hash, len(doc.raw_text.encode()), "news_raw_v1"),
                (doc.receipt_hash, len(doc.receipt_text.encode()), "news_receipt_v1"),
                (row.revision, len(content), SCHEMA),
            ):
                art = await session.scalar(
                    select(DataArtifact).where(DataArtifact.sha256 == digest)
                )
                checks.append(
                    art is not None
                    and art.source == SOURCE
                    and art.schema_version == schema
                    and art.byte_size == size
                    and max(art.available_at, art.received_at, art.observed_at) <= doc.available_at
                    and art.created_at <= as_of
                    and (schema != SCHEMA or art.uri == reference.location)
                )
            if all(checks):
                result[reference.id] = doc
        except (ValueError, TypeError, KeyError):
            continue
    return result


async def register_news_evidence(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    instrument_id: UUID,
    news_snapshot_id: UUID,
) -> ResearchEvidenceRef:
    """Materialize only the named saved page; source/model requests are never issued."""
    object_id = f"news-research:{instrument_id}:{news_snapshot_id}:latest5-v1"
    existing = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == object_id,
        )
    )
    now = datetime.now(UTC)
    if existing is not None:
        if existing.id not in await read_news_evidence(
            session, instrument_id=instrument_id, as_of=now, evidence_ids=(existing.id,)
        ):
            raise ValueError("NEWS_RESEARCH_EXISTING_INVALID")
        return existing
    saved = await read_news(
        session, store, instrument_id=instrument_id, now=now, snapshot_id=news_snapshot_id
    )
    original = await session.get(DataSnapshot, news_snapshot_id)
    if saved.status != "saved" or original is None:
        raise ValueError("NEWS_RESEARCH_SNAPSHOT_UNAVAILABLE")
    original_bytes = []
    for digest, schema in (
        (str(saved.content_hash), "news_raw_v1"),
        (original.revision, "news_receipt_v1"),
    ):
        art = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
        if art is None:
            raise ValueError("NEWS_RESEARCH_ARTIFACT_UNAVAILABLE")
        original_bytes.append(
            await store.read_raw(f"raw/alpaca/news/{schema}/{digest}.json", uri=art.uri)
        )
    doc = NewsObservation.model_validate(
        {
            "instrument_id": instrument_id,
            "news_snapshot_id": news_snapshot_id,
            "symbol": saved.symbol,
            "available_at": datetime.now(UTC),
            "received_at": saved.received_at,
            "window_start": saved.window_start,
            "window_end": saved.window_end,
            "articles": tuple(
                sorted(saved.articles, key=lambda a: (a.updated_at, a.id), reverse=True)[:5]
            ),
            "collected_count": len(saved.articles),
            "has_more": saved.has_more,
            "raw_hash": saved.content_hash,
            "receipt_hash": original.revision,
            "raw_text": original_bytes[0].decode(),
            "receipt_text": original_bytes[1].decode(),
        }
    )
    _validate_document(doc)
    content = doc.canonical_bytes()
    digest = sha256(content).hexdigest()
    key = f"evidence/alpaca-news/sha256/{digest}.json"
    uri = await store.put_if_absent(key=key, content=content)
    if await store.read_evidence(key, uri=uri) != content:
        raise ValueError("NEWS_RESEARCH_OBJECT_INVALID")
    session.add(
        DataArtifact(
            uri=uri,
            sha256=digest,
            byte_size=len(content),
            mime_type="application/json",
            source=SOURCE,
            schema_version=SCHEMA,
            observed_at=doc.available_at,
            available_at=doc.available_at,
            received_at=doc.available_at,
        )
    )
    await session.flush()
    session.add(
        DataSnapshot(
            source=SOURCE,
            provider_object_id=object_id,
            revision=digest,
            observed_at=doc.available_at,
            available_at=doc.available_at,
            received_at=doc.available_at,
            schema_version=SCHEMA,
            payload=doc.model_dump(mode="json"),
        )
    )
    reference = ResearchEvidenceRef(
        source=SOURCE,
        provider_object_id=object_id,
        revision=digest,
        available_at=doc.available_at,
        location=uri,
        content_hash=digest,
        byte_length=len(content),
    )
    session.add(reference)
    await session.flush()
    return reference
