"""Prepare one explicit read-only authority before a bounded Fosun push probe."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import os
import stat
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Literal, NoReturn, Protocol, cast
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.jobs.daily_payloads import (
    instrument_alias_identity_sha256,
    is_canonical_us_ticker_symbol,
)
from nexus.jobs.market_authority import (
    FrozenFosunMarketAuthority,
    acquire_fosun_market_authority_locks,
    binding_at,
    load_frozen_fosun_market_authority,
)
from nexus.market.ingestion import object_uri_matches_key
from nexus.market.intraday_ingestion import canonical_quote_observation
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.data import DataArtifact, ProviderCapability
from nexus.persistence.models.market import Instrument, InstrumentAlias, LatestQuote
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import load_fosun_market_admission
from nexus.settings import EngineSettings

_OPERATION: Literal["fosun_subscribe_quotes"] = "fosun_subscribe_quotes"
_MARKET_SCOPE = "US"
_CAPABILITY_DAYS = 365
_PUSH_URL = "wss://openapi-push.fosunxcz.com/msg/v1/push"
_MAX_FRAME_BYTES = 2 * 1024 * 1024
_MAX_TOTAL_FRAME_BYTES = 8 * 1024 * 1024
_MAX_ARCHIVE_BYTES = 12 * 1024 * 1024
_MAX_HANDOFF_BYTES = 16 * 1024 * 1024
_MAX_FRAMES = 100
_DYNAMIC_QUOTE_FIELDS = frozenset(
    {
        "qtTime",
        "qtDate",
        "power",
        "tradeStatus",
        "price",
        "chgVal",
        "chgPct",
        "pClose",
        "open",
        "high",
        "low",
        "vol",
        "turnover",
    }
)


@dataclass(frozen=True)
class PreparedPushAuthority:
    """Canonical authority document and the digest bound into a v3 archive."""

    payload: bytes
    sha256: str


@dataclass(frozen=True)
class ImportedPushQuote:
    instrument_id: UUID
    raw_sha256: str


@dataclass(frozen=True)
class _DerivedPushQuote:
    price: Decimal
    provider_quote_date: int
    provider_quote_time: int
    frame_received_at: datetime


class PushArchiveStore(Protocol):
    async def archive_raw(self, key: str, payload: bytes) -> str: ...

    async def read_raw(self, key: str, *, uri: str) -> bytes: ...


def _utc_text(value: datetime) -> str:
    return require_utc(value).isoformat().replace("+00:00", "Z")


async def prepare_push_authority(
    session: AsyncSession,
    *,
    code: str,
    prepared_at: datetime,
) -> PreparedPushAuthority:
    """Create/reuse subscription authority before opening the WebSocket."""
    checked_at = require_utc(prepared_at)
    alias, _ = await _load_us_identity(session, code=code, as_of=checked_at)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": (f"provider-capability-scope:v1:fosun:READ_ONLY:{_OPERATION}:{_MARKET_SCOPE}")},
    )
    status_id = await session.scalar(
        select(IntegrationStatus.id).where(IntegrationStatus.provider == "fosun")
    )
    if status_id is None:
        raise ValueError("FOSUN_PUSH_AUTHORITY_REQUIRED")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"fosun-integration-status:{status_id}"},
    )
    status = await session.scalar(
        select(IntegrationStatus)
        .where(IntegrationStatus.id == status_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if status is None or (
        status.status != "read_only"
        or status.created_at > checked_at
        or status.mode_changed_at > checked_at
        or status.admitted_contract_hash is None
        or status.capability_invalidated_at is not None
    ):
        raise ValueError("FOSUN_PUSH_AUTHORITY_REQUIRED")

    capability = await session.scalar(
        select(ProviderCapability)
        .where(
            ProviderCapability.provider == "fosun",
            ProviderCapability.capability == "READ_ONLY",
            ProviderCapability.contract_hash == status.admitted_contract_hash,
            ProviderCapability.payload["operation"].as_string() == _OPERATION,
            ProviderCapability.payload["market_scope"].as_string() == _MARKET_SCOPE,
        )
        .order_by(ProviderCapability.created_at.desc(), ProviderCapability.id.desc())
        .limit(1)
    )
    if capability is not None and (
        capability.schema_version != "1"
        or capability.created_at > checked_at
        or capability.expires_at <= checked_at
        or capability.payload.get("delivery_mode") != "unknown"
    ):
        raise ValueError("FOSUN_PUSH_AUTHORITY_INVALID")
    if capability is None:
        capability = ProviderCapability(
            provider="fosun",
            capability="READ_ONLY",
            contract_hash=status.admitted_contract_hash,
            expires_at=checked_at + timedelta(days=_CAPABILITY_DAYS),
            schema_version="1",
            payload={
                "operation": _OPERATION,
                "market_scope": _MARKET_SCOPE,
                "delivery_mode": "unknown",
            },
            created_at=checked_at,
        )
        session.add(capability)
        await session.flush()

    admission = await load_fosun_market_admission(
        session,
        decision_time=checked_at,
        operation=_OPERATION,
        market_scope=_MARKET_SCOPE,
    )
    if admission is None or admission.capability_id != capability.id:
        raise ValueError("FOSUN_PUSH_AUTHORITY_REQUIRED")
    document = {
        "schema": "nexus_fosun_push_authority_v1",
        "provider": "fosun",
        "operation": _OPERATION,
        "delivery_mode": admission.delivery_mode,
        "instrument_alias": _alias_document(alias),
        "capability_id": str(admission.capability_id),
        "capability_expires_at": _utc_text(admission.expires_at),
        "status_id": str(admission.status_id),
        "status_version": admission.status_version,
        "contract_hash": admission.contract_hash,
        "prepared_at": _utc_text(checked_at),
    }
    payload = canonical_json(document).encode()
    return PreparedPushAuthority(payload=payload, sha256=sha256(payload).hexdigest())


async def import_push_quote(
    session: AsyncSession,
    store: PushArchiveStore,
    *,
    handoff_payload: bytes,
    authority_payload: bytes,
    imported_at: datetime,
) -> ImportedPushQuote:
    """Persist one Gateway-validated bounded push quote for display only."""
    checked_at = require_utc(imported_at)
    handoff = _exact_object(handoff_payload, "FOSUN_PUSH_HANDOFF_INVALID")
    required = {
        "schema",
        "code",
        "price",
        "provider_quote_date",
        "provider_quote_time",
        "archive_started_at",
        "frame_received_at",
        "archive_completed_at",
        "authority_sha256",
        "raw_archive_sha256",
        "raw_archive_base64",
    }
    if set(handoff) != required or handoff.get("schema") != "nexus_fosun_push_quote_handoff_v1":
        raise ValueError("FOSUN_PUSH_HANDOFF_INVALID")
    authority_digest = sha256(authority_payload).hexdigest()
    if handoff.get("authority_sha256") != authority_digest:
        raise ValueError("FOSUN_PUSH_AUTHORITY_MISMATCH")
    authority = _exact_object(authority_payload, "FOSUN_PUSH_AUTHORITY_INVALID")
    if (
        set(authority)
        != {
            "schema",
            "provider",
            "operation",
            "delivery_mode",
            "instrument_alias",
            "capability_id",
            "capability_expires_at",
            "status_id",
            "status_version",
            "contract_hash",
            "prepared_at",
        }
        or canonical_json(authority).encode() != authority_payload
    ):
        raise ValueError("FOSUN_PUSH_AUTHORITY_INVALID")
    if (
        authority.get("schema") != "nexus_fosun_push_authority_v1"
        or authority.get("provider") != "fosun"
        or authority.get("operation") != _OPERATION
        or authority.get("delivery_mode") != "unknown"
    ):
        raise ValueError("FOSUN_PUSH_AUTHORITY_INVALID")
    code = handoff.get("code")
    if not isinstance(code, str):
        raise ValueError("FOSUN_PUSH_HANDOFF_INVALID")
    prepared_at = _parse_utc(authority.get("prepared_at"), "FOSUN_PUSH_AUTHORITY_INVALID")
    started_at = _parse_utc(handoff.get("archive_started_at"), "FOSUN_PUSH_HANDOFF_INVALID")
    frame_at = _parse_utc(handoff.get("frame_received_at"), "FOSUN_PUSH_HANDOFF_INVALID")
    completed_at = _parse_utc(handoff.get("archive_completed_at"), "FOSUN_PUSH_HANDOFF_INVALID")
    if not prepared_at <= started_at <= frame_at <= completed_at <= checked_at:
        raise ValueError("FOSUN_PUSH_TIME_ORDER_INVALID")

    raw_archive = _decode_base64(handoff.get("raw_archive_base64"))
    raw_digest = sha256(raw_archive).hexdigest()
    if handoff.get("raw_archive_sha256") != raw_digest:
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
    derived = _validate_raw_archive(
        raw_archive,
        code=code,
        authority_sha256=authority_digest,
        started_at=started_at,
        completed_at=completed_at,
    )
    price = _positive_decimal(handoff.get("price"))
    if (
        type(handoff.get("provider_quote_date")) is not int
        or type(handoff.get("provider_quote_time")) is not int
    ):
        raise ValueError("FOSUN_PUSH_HANDOFF_INVALID")
    if (
        price != derived.price
        or handoff.get("provider_quote_date") != derived.provider_quote_date
        or handoff.get("provider_quote_time") != derived.provider_quote_time
        or frame_at != derived.frame_received_at
    ):
        raise ValueError("FOSUN_PUSH_HANDOFF_ARCHIVE_MISMATCH")

    alias, instrument = await _load_us_identity(session, code=code, as_of=prepared_at)
    if authority.get("instrument_alias") != _alias_document(alias):
        raise ValueError("FOSUN_PUSH_IDENTITY_CHANGED")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"fosun-push-import:{raw_digest}"},
    )
    current = await load_frozen_fosun_market_authority(
        session,
        check_time=checked_at,
        operation=_OPERATION,
        delivery_mode="unknown",
    )
    if current is None:
        raise ValueError("FOSUN_PUSH_AUTHORITY_REQUIRED")
    await acquire_fosun_market_authority_locks(session, (current,))
    rechecked = await load_frozen_fosun_market_authority(
        session,
        check_time=checked_at,
        operation=_OPERATION,
        delivery_mode="unknown",
    )
    if rechecked != current or not _authority_matches(authority, current, completed_at):
        raise ValueError("FOSUN_PUSH_AUTHORITY_CHANGED")

    date_path = completed_at.astimezone(UTC).strftime("%Y/%m/%d")
    raw_key = f"raw/market/fosun/push/{date_path}/{raw_digest}.json"
    raw_uri = await store.archive_raw(raw_key, raw_archive)
    if not object_uri_matches_key(raw_uri, raw_key):
        raise RuntimeError("FOSUN_PUSH_OBJECT_URI_MISMATCH")
    if await store.read_raw(raw_key, uri=raw_uri) != raw_archive:
        raise RuntimeError("FOSUN_PUSH_OBJECT_READBACK_MISMATCH")

    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == raw_digest))
    artifact_values = {
        "uri": raw_uri,
        "sha256": raw_digest,
        "byte_size": len(raw_archive),
        "mime_type": "application/json",
        "schema_version": "fosun_push_quote_archive_v1",
        "source": "fosun",
        "observed_at": frame_at,
        "available_at": completed_at,
        "received_at": completed_at,
    }
    if artifact is None:
        artifact = DataArtifact(**artifact_values)
        session.add(artifact)
        await session.flush()
    elif any(getattr(artifact, field) != value for field, value in artifact_values.items()):
        raise RuntimeError("FOSUN_PUSH_ARTIFACT_REPLAY_CONFLICT")

    flags = (
        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
        "INTRADAY_BID_ASK_UNAVAILABLE",
    )
    canonical = canonical_quote_observation(
        instrument_id=instrument.id,
        source="fosun",
        feed="l1",
        delivery_mode="unknown",
        event_kind="snapshot",
        bid=None,
        ask=None,
        last=price,
        observed_at=frame_at,
        available_at=completed_at,
        received_at=completed_at,
        revision=raw_digest,
        quality_flags=flags,
    )
    content_hash = sha256(canonical).hexdigest()
    quote = await session.scalar(
        select(LatestQuote).where(
            LatestQuote.instrument_id == instrument.id,
            LatestQuote.source == "fosun",
            LatestQuote.feed == "l1",
            LatestQuote.event_kind == "snapshot",
            LatestQuote.observed_at == frame_at,
            LatestQuote.content_hash == content_hash,
        )
    )
    authorized_at = checked_at
    if quote is not None:
        existing_authorized_at = quote.provider_authorized_at
        if (
            existing_authorized_at is None
            or not completed_at <= existing_authorized_at <= checked_at
        ):
            raise RuntimeError("FOSUN_PUSH_QUOTE_REPLAY_CONFLICT")
        authorized_at = existing_authorized_at
    binding = binding_at(current, authorized_at=authorized_at)
    quote_values = {
        "instrument_id": instrument.id,
        "source": "fosun",
        "feed": "l1",
        "delivery_mode": "unknown",
        "bid": None,
        "ask": None,
        "last": price,
        "observed_at": frame_at,
        "available_at": completed_at,
        "received_at": completed_at,
        "revision": raw_digest,
        "quality_flags": list(flags),
        "event_kind": "snapshot",
        "content_hash": content_hash,
        "evidence_status": "verified",
        "data_artifact_id": artifact.id,
        **binding.model_dump(),
    }
    if quote is None:
        session.add(LatestQuote(id=new_uuid7(), **quote_values))
        await session.flush()
    elif any(getattr(quote, field) != value for field, value in quote_values.items()):
        raise RuntimeError("FOSUN_PUSH_QUOTE_REPLAY_CONFLICT")
    return ImportedPushQuote(instrument_id=instrument.id, raw_sha256=raw_digest)


async def _load_us_identity(
    session: AsyncSession, *, code: str, as_of: datetime
) -> tuple[InstrumentAlias, Instrument]:
    if not code.startswith("us") or not is_canonical_us_ticker_symbol(code[2:]):
        raise ValueError("FOSUN_PUSH_CODE_UNSUPPORTED")
    rows = (
        await session.execute(
            select(InstrumentAlias, Instrument)
            .join(Instrument, Instrument.id == InstrumentAlias.instrument_id)
            .where(
                InstrumentAlias.symbol == code[2:],
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= as_of.date(),
                (InstrumentAlias.valid_to.is_(None) | (InstrumentAlias.valid_to >= as_of.date())),
                InstrumentAlias.created_at <= as_of,
                Instrument.currency == "USD",
                Instrument.exchange.in_(US_EQUITY_EXCHANGES),
            )
            .order_by(InstrumentAlias.id)
            .limit(2)
        )
    ).all()
    if len(rows) != 1:
        raise ValueError("FOSUN_PUSH_IDENTITY_REQUIRED")
    return cast(tuple[InstrumentAlias, Instrument], rows[0])


def _alias_document(alias: InstrumentAlias) -> dict[str, object]:
    return {
        "alias_id": str(alias.id),
        "instrument_id": str(alias.instrument_id),
        "symbol": alias.symbol,
        "alias_type": alias.alias_type,
        "valid_from": alias.valid_from.isoformat(),
        "valid_to": None if alias.valid_to is None else alias.valid_to.isoformat(),
        "created_at": _utc_text(alias.created_at),
        "alias_identity_sha256": instrument_alias_identity_sha256(
            alias_id=str(alias.id),
            instrument_id=str(alias.instrument_id),
            symbol=alias.symbol,
            alias_type=alias.alias_type,
            valid_from=alias.valid_from,
            valid_to=alias.valid_to,
            created_at=alias.created_at,
        ),
    }


def _exact_object(payload: bytes, code: str) -> dict[str, object]:
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(code)
            result[key] = value
        return result

    try:
        value = json.loads(payload, object_pairs_hook=unique)
    except (UnicodeError, json.JSONDecodeError, ValueError):
        raise ValueError(code) from None
    if not isinstance(value, dict):
        raise ValueError(code)
    return cast(dict[str, object], value)


def _parse_utc(value: object, code: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(code)
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00" if value.endswith("Z") else value)
        return require_utc(parsed).astimezone(UTC)
    except ValueError:
        raise ValueError(code) from None


def _decode_base64(value: object, *, max_bytes: int | None = None) -> bytes:
    if not isinstance(value, str) or (
        max_bytes is not None and len(value) > ((max_bytes + 2) // 3) * 4
    ):
        raise ValueError("FOSUN_PUSH_HANDOFF_INVALID")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error):
        raise ValueError("FOSUN_PUSH_HANDOFF_INVALID") from None
    if max_bytes is not None and len(decoded) > max_bytes:
        raise ValueError("FOSUN_PUSH_HANDOFF_INVALID")
    return decoded


def _positive_decimal(value: object) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("FOSUN_PUSH_HANDOFF_INVALID")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise ValueError("FOSUN_PUSH_HANDOFF_INVALID") from None
    if not parsed.is_finite() or parsed <= 0 or str(parsed) != value:
        raise ValueError("FOSUN_PUSH_HANDOFF_INVALID")
    return parsed


def _validate_raw_archive(
    payload: bytes,
    *,
    code: str,
    authority_sha256: str,
    started_at: datetime,
    completed_at: datetime,
) -> _DerivedPushQuote:
    if len(payload) > _MAX_ARCHIVE_BYTES:
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
    archive = _exact_object(payload, "FOSUN_PUSH_ARCHIVE_MISMATCH")
    if (
        set(archive) != {"schema", "receipt_base64", "frames_base64"}
        or archive.get("schema") != "fosun_push_quote_archive_v1"
    ):
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
    encoded_frames = archive.get("frames_base64")
    if not isinstance(encoded_frames, list) or not 1 <= len(encoded_frames) <= _MAX_FRAMES:
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
    frames = tuple(_decode_base64(frame, max_bytes=_MAX_FRAME_BYTES) for frame in encoded_frames)
    if sum(map(len, frames)) > _MAX_TOTAL_FRAME_BYTES:
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
    receipt = _exact_object(
        _decode_base64(archive.get("receipt_base64"), max_bytes=256 * 1024),
        "FOSUN_PUSH_ARCHIVE_MISMATCH",
    )
    expected_receipt_keys = {
        "schema",
        "source",
        "transport",
        "origin",
        "topic",
        "code",
        "request_id",
        "started_at",
        "ended_at",
        "requested_duration_seconds",
        "requested_max_frames",
        "heartbeat_interval_seconds",
        "elapsed_monotonic_seconds",
        "subscription_confirmed",
        "status",
        "stop_reason",
        "frame_count",
        "quote_message_count",
        "heartbeat_sent_count",
        "heartbeat_ack_count",
        "heartbeats",
        "outbound_send_timed_out",
        "frames",
        "authority_sha256",
    }
    duration = receipt.get("requested_duration_seconds")
    max_frames = receipt.get("requested_max_frames")
    elapsed = receipt.get("elapsed_monotonic_seconds")
    entries = receipt.get("frames")
    heartbeats = receipt.get("heartbeats")
    quote_message_count = receipt.get("quote_message_count")
    heartbeat_sent_count = receipt.get("heartbeat_sent_count")
    heartbeat_ack_expected = receipt.get("heartbeat_ack_count")
    if (
        type(duration) is not float
        or type(max_frames) is not int
        or type(elapsed) is not float
        or type(quote_message_count) is not int
        or type(heartbeat_sent_count) is not int
        or type(heartbeat_ack_expected) is not int
        or not isinstance(entries, list)
        or not isinstance(heartbeats, list)
    ):
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
    if (
        set(receipt) != expected_receipt_keys
        or receipt.get("schema") != "nexus_fosun_push_receipt_v3"
        or receipt.get("source") != "fosun"
        or receipt.get("transport") != "websocket"
        or receipt.get("origin") != _PUSH_URL
        or receipt.get("topic") != "qt"
        or receipt.get("status") != "bounded_complete"
        or receipt.get("subscription_confirmed") is not True
        or receipt.get("stop_reason") not in {"DURATION_LIMIT", "FRAME_LIMIT"}
        or receipt.get("outbound_send_timed_out") is not False
        or receipt.get("code") != code
        or receipt.get("authority_sha256") != authority_sha256
        or not 0 < duration <= 300
        or not 1 <= max_frames <= _MAX_FRAMES
        or not 0 <= elapsed <= duration + 1
        or receipt.get("heartbeat_interval_seconds") != 15.0
        or len(entries) != len(frames)
        or receipt.get("frame_count") != len(frames)
        or len(frames) > max_frames
        or quote_message_count < 1
        or heartbeat_sent_count < 0
        or heartbeat_ack_expected < 0
        or heartbeat_sent_count != len(heartbeats)
        or _parse_utc(receipt.get("started_at"), "FOSUN_PUSH_ARCHIVE_MISMATCH") != started_at
        or _parse_utc(receipt.get("ended_at"), "FOSUN_PUSH_ARCHIVE_MISMATCH") != completed_at
        or completed_at - started_at > timedelta(seconds=duration + 1)
        or receipt.get("stop_reason") == "FRAME_LIMIT"
        and len(frames) != max_frames
        or receipt.get("stop_reason") == "DURATION_LIMIT"
        and elapsed < duration
    ):
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")

    request_id = receipt.get("request_id")
    if not isinstance(request_id, str) or not request_id:
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
    subscribed = False
    previous_received: datetime | None = None
    previous_quote_id: int | None = None
    quote_count = 0
    heartbeat_ack_count = 0
    latest: _DerivedPushQuote | None = None
    for index, (entry, raw) in enumerate(zip(entries, frames, strict=True)):
        if (
            not isinstance(entry, dict)
            or set(entry) != {"file", "received_at", "raw_sha256", "raw_byte_length"}
            or entry.get("file") != f"frame-{index:05}.json"
            or entry.get("raw_sha256") != sha256(raw).hexdigest()
            or entry.get("raw_byte_length") != len(raw)
        ):
            raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
        received_at = _parse_utc(entry.get("received_at"), "FOSUN_PUSH_ARCHIVE_MISMATCH")
        if not started_at <= received_at <= completed_at or (
            previous_received is not None and received_at < previous_received
        ):
            raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
        previous_received = received_at
        message = _exact_object(raw, "FOSUN_PUSH_ARCHIVE_MISMATCH")
        if any(
            field in message and type(message[field]) is not int
            for field in ("id", "time", "cache")
        ) or ("requestid" in message and not isinstance(message["requestid"], str)):
            raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
        if not subscribed:
            _validate_subscription_frame(message, code=code, request_id=request_id)
            subscribed = True
            continue
        if message.get("topic") == "hb":
            if type(message.get("hbValue")) is not int or _has_error(message):
                raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
            heartbeat_ack_count += 1
            continue
        outer_id = message.get("id")
        items = message.get("items")
        if (
            message.get("topic") != "qt"
            or "subRep" in message
            or _has_error(message)
            or not isinstance(items, list)
            or not items
            or outer_id is not None
            and type(outer_id) is not int
            or previous_quote_id is not None
            and outer_id is not None
            and outer_id <= previous_quote_id
        ):
            raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
        if outer_id is not None:
            previous_quote_id = cast(int, outer_id)
        assert isinstance(items, list)
        for item in items:
            if (
                not isinstance(item, dict)
                or set(item) != {"code", "msg"}
                or item.get("code") != code
                or not isinstance(item.get("msg"), str)
            ):
                raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
            quote = _exact_object(item["msg"].encode(), "FOSUN_PUSH_ARCHIVE_MISMATCH")
            if (
                _has_error(quote)
                or not _DYNAMIC_QUOTE_FIELDS.issubset(quote)
                or any(type(quote[field]) is not int for field in _DYNAMIC_QUOTE_FIELDS)
            ):
                raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
            raw_price = cast(int, quote["price"])
            power = cast(int, quote["power"])
            quote_date = cast(int, quote["qtDate"])
            quote_time = cast(int, quote["qtTime"])
            if raw_price <= 0 or not 0 <= power <= 12:
                raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
            latest = _DerivedPushQuote(
                price=Decimal(raw_price) / (Decimal(10) ** power),
                provider_quote_date=quote_date,
                provider_quote_time=quote_time,
                frame_received_at=received_at,
            )
            quote_count += 1
    if (
        not subscribed
        or latest is None
        or receipt.get("quote_message_count") != quote_count
        or receipt.get("heartbeat_ack_count") != heartbeat_ack_count
    ):
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
    return latest


def _has_error(message: Mapping[str, object]) -> bool:
    value = message.get("errMsg")
    return "errMsg" in message and not (value is None or value == "" or value == {})


def _validate_subscription_frame(
    message: Mapping[str, object], *, code: str, request_id: str
) -> None:
    reply = message.get("subRep")
    if (
        message.get("topic") not in {"qt", "subrep"}
        or message.get("requestid") != request_id
        or not isinstance(reply, list)
        or len(reply) != 1
        or not isinstance(reply[0], dict)
        or _has_error(message)
    ):
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")
    item = reply[0]
    collections = {
        name: item[name] if name in item else [] for name in ("scode", "fcode", "invalid", "denied")
    }
    if (
        item.get("topic") != "qt"
        or _has_error(item)
        or any(not isinstance(values, list) for values in collections.values())
        or collections != {"scode": [code], "fcode": [], "invalid": [], "denied": []}
    ):
        raise ValueError("FOSUN_PUSH_ARCHIVE_MISMATCH")


def _authority_matches(
    authority: Mapping[str, object],
    current: FrozenFosunMarketAuthority,
    completed_at: datetime,
) -> bool:
    expires_at = _parse_utc(authority.get("capability_expires_at"), "FOSUN_PUSH_AUTHORITY_INVALID")
    return bool(
        authority.get("capability_id") == str(current.capability_id)
        and authority.get("status_id") == str(current.status_id)
        and authority.get("status_version") == current.status_version
        and authority.get("contract_hash") == current.contract_hash
        and expires_at == current.capability_expires_at
        and current.capability_created_at <= completed_at < current.capability_expires_at
        and current.status_created_at <= completed_at
        and current.status_mode_changed_at <= completed_at
        and current.capability_invalidated_at is None
    )


def _require_local_services(settings: EngineSettings) -> None:
    database = make_url(settings.database_url) if settings.database_url else None
    endpoint = urlsplit(settings.object_store_endpoint or "")
    local = {"localhost", "127.0.0.1", "::1"}
    if (
        database is None
        or database.drivername != "postgresql+asyncpg"
        or database.host not in local
        or database.query
        or endpoint.scheme not in {"http", "https"}
        or endpoint.hostname not in local
        or endpoint.username
        or endpoint.password
        or endpoint.path
        or endpoint.query
        or endpoint.fragment
    ):
        raise ValueError("FOSUN_PUSH_LOCAL_SERVICES_REQUIRED")


async def _require_schema_head(session: AsyncSession) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
        {"key": SCHEMA_ADMISSION_LOCK_KEY},
    )
    head = await session.scalar(
        text("SELECT CASE WHEN COUNT(*) = 1 THEN MIN(version_num) END FROM public.alembic_version")
    )
    if head != CURRENT_MIGRATION_HEAD:
        raise ValueError("FOSUN_PUSH_SCHEMA_NOT_READY")


async def prepare_local(settings: EngineSettings, *, code: str) -> PreparedPushAuthority:
    """Prepare authority using the normal local configuration and database clock."""
    _require_local_services(settings)
    factory = session_factory(settings)
    async with asyncio.timeout(30):
        async with factory.begin() as session:
            await _require_schema_head(session)
            prepared_at = cast(datetime, await session.scalar(select(func.clock_timestamp())))
            return await prepare_push_authority(session, code=code, prepared_at=prepared_at)


async def import_local(
    settings: EngineSettings,
    *,
    handoff_path: Path,
    authority_path: Path,
) -> dict[str, object]:
    """Import a completed private handoff without calling Gateway or a model."""
    _require_local_services(settings)
    handoff_payload = _read_private_bounded(handoff_path, _MAX_HANDOFF_BYTES)
    authority_payload = _read_private_bounded(authority_path, 64 * 1024)
    store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    async with asyncio.timeout(120):
        async with factory.begin() as session:
            await _require_schema_head(session)
            imported_at = cast(datetime, await session.scalar(select(func.clock_timestamp())))
            result = await import_push_quote(
                session,
                store,
                handoff_payload=handoff_payload,
                authority_payload=authority_payload,
                imported_at=imported_at,
            )
    return {
        "status": "stored_display_quote",
        "instrument_id": str(result.instrument_id),
        "raw_sha256": result.raw_sha256,
        "execution_locked": True,
        "provider_called": False,
        "model_called": False,
        "research_eligible": False,
    }


def _read_private_bounded(path: Path, max_bytes: int) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o077
            or not 0 < metadata.st_size <= max_bytes
        ):
            raise ValueError("FOSUN_PUSH_PRIVATE_INPUT_REQUIRED")
        with os.fdopen(descriptor, "rb") as source:
            payload = source.read(max_bytes + 1)
            descriptor = -1
    except OSError:
        raise ValueError("FOSUN_PUSH_PRIVATE_INPUT_REQUIRED") from None
    finally:
        if "descriptor" in locals() and descriptor >= 0:
            os.close(descriptor)
    if len(payload) != metadata.st_size:
        raise ValueError("FOSUN_PUSH_PRIVATE_INPUT_REQUIRED")
    return payload


def _require_private_new_output(path: Path) -> None:
    if path.exists() or path.is_symlink():
        raise ValueError("FOSUN_PUSH_OUTPUT_EXISTS")
    metadata = path.parent.stat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o077:
        raise ValueError("FOSUN_PUSH_PRIVATE_OUTPUT_REQUIRED")


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        raise ValueError("FOSUN_PUSH_REQUEST_INVALID")


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    prepare_parser.add_argument("--code", required=True)
    prepare_parser.add_argument("--output", type=Path, required=True)
    import_parser = subparsers.add_parser("import")
    import_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    import_parser.add_argument("--handoff", type=Path, required=True)
    import_parser.add_argument("--authority", type=Path, required=True)
    import_parser.add_argument("--output", type=Path, required=True)
    started = False
    try:
        args = parser.parse_args(argv)
        _require_private_new_output(args.output)
        settings = EngineSettings.model_validate(load_local_environment(args.repo_root, os.environ))
        if args.command == "prepare":
            started = True
            prepared = asyncio.run(prepare_local(settings, code=args.code))
            write_private(args.output, prepared.payload)
            summary = {
                "status": "prepared",
                "authority_sha256": prepared.sha256,
                "execution_locked": True,
                "provider_called": False,
            }
        else:
            descriptor = os.open(
                args.output,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
            )
            with os.fdopen(descriptor, "w") as output:
                json.dump({"status": "started"}, output)
                output.flush()
                os.fsync(output.fileno())
                started = True
                summary = asyncio.run(
                    import_local(
                        settings,
                        handoff_path=args.handoff,
                        authority_path=args.authority,
                    )
                )
                output.seek(0)
                json.dump(summary, output, sort_keys=True)
                output.truncate()
                output.flush()
                os.fsync(output.fileno())
        print(json.dumps(summary, sort_keys=True))
        return 0
    except KeyboardInterrupt:
        code = "FOSUN_PUSH_CHECK_PERSISTED_STATE" if started else "FOSUN_PUSH_INTERRUPTED"
    except Exception:
        code = "FOSUN_PUSH_CHECK_PERSISTED_STATE" if started else "FOSUN_PUSH_FAILED"
    print(
        json.dumps({"status": "unknown" if started else "rejected", "code": code}),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
