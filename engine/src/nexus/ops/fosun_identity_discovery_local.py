"""Bounded classification discovery for existing private broker identity gaps."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local_refresh import (
    LocalFosunRefreshSettings,
    _active_holdings,
    _broker_settings,
    _load_local_broker_identity,
    _market_identity,
    _persist_pending_instrument_discoveries,
    _raw_key,
    _read_quote_snapshot,
)
from nexus.persistence.models.data import DataArtifact
from nexus.providers.broker.fosun_contracts import AccountItem, PositionItem
from nexus.providers.broker.gateway_client import FosunGatewayClient
from nexus.providers.market.fosun import encode_fosun_snapshot_evidence
from nexus.providers.market.fosun_gateway import (
    FosunMarketGatewayClient,
    RawGatewayResponse,
    parse_quote_page,
)

MAX_DISCOVERIES_PER_RUN = 3


class _GapItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    provider_code: str = Field(pattern=r"^(us[A-Z][A-Z0-9.-]{0,14}|hk[0-9]{5})$")
    currency: Literal["USD", "HKD"]


class _GapDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["fosun_broker_identity_gap_diagnostic_v1"] = Field(alias="schema")
    identity_authority: Literal[False]
    observed_at: AwareDatetime
    source_page_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    unresolved: list[_GapItem] = Field(min_length=1, max_length=100)

    @field_validator("identity_authority", mode="before")
    @classmethod
    def _no_identity_authority(cls, value: object) -> object:
        if value is not False:
            raise ValueError("diagnostic authority must be false")
        return value


def _unique_json(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def read_gap_selection(
    repo_root: Path, gap_file: Path, item_indices: tuple[int, ...]
) -> tuple[str, ...]:
    """Use diagnostic codes only as selectors; live holdings must corroborate them later."""
    if (
        not item_indices
        or any(type(index) is not int or index < 0 for index in item_indices)
        or len(set(item_indices)) != len(item_indices)
    ):
        raise ValueError("LOCAL_IDENTITY_DISCOVERY_SELECTION_INVALID")
    if len(item_indices) > MAX_DISCOVERIES_PER_RUN:
        raise ValueError("LOCAL_IDENTITY_DISCOVERY_BATCH_LIMIT")
    root = repo_root.absolute()
    expected_parent = root / ".secrets/nexus-fosun-local/identity-gaps"
    path = gap_file if gap_file.is_absolute() else root / gap_file
    try:
        if (
            path.parent != expected_parent
            or str(UUID(path.stem)) != path.stem
            or path.suffix != ".json"
        ):
            raise ValueError("invalid path")
        for directory in (root / ".secrets", expected_parent.parent, expected_parent):
            info = directory.lstat()
            if (
                not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o700
            ):
                raise ValueError("unsafe directory")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_size > 65536
            ):
                raise ValueError("unsafe file")
            with os.fdopen(descriptor, "r") as source:
                descriptor = -1
                document = _GapDocument.model_validate(
                    json.load(source, object_pairs_hook=_unique_json)
                )
        finally:
            if descriptor != -1:
                os.close(descriptor)
        items = [(item.provider_code, item.currency) for item in document.unresolved]
        if (
            items != sorted(set(items))
            or len({code for code, _ in items}) != len(items)
            or document.observed_at > datetime.now(UTC)
        ):
            raise ValueError("invalid document")
    except (OSError, ValueError, TypeError):
        raise ValueError("LOCAL_IDENTITY_DISCOVERY_GAP_INVALID") from None
    if any(index >= len(items) for index in item_indices):
        raise ValueError("LOCAL_IDENTITY_DISCOVERY_SELECTION_INVALID")
    selected = tuple(sorted(items[index] for index in item_indices))
    if any(not code.startswith("us") or currency != "USD" for code, currency in selected):
        raise ValueError("LOCAL_IDENTITY_DISCOVERY_SCOPE_UNSUPPORTED")
    return tuple(code for code, _ in selected)


async def _persist_discovery_batch(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    codes: tuple[str, ...],
    holdings: tuple[PositionItem, ...],
    holdings_observed_at: datetime,
    quote_raw: RawGatewayResponse,
) -> dict[str, int]:
    page = parse_quote_page(quote_raw, expected_codes=codes)
    evidence = encode_fosun_snapshot_evidence(((codes, quote_raw),))
    digest = hashlib.sha256(evidence).hexdigest()
    key = _raw_key(quote_raw, digest)
    uri = await store.archive_raw(key, evidence)
    if await store.read_raw(key, uri=uri) != evidence:
        raise ValueError("LOCAL_IDENTITY_DISCOVERY_RAW_READBACK_FAILED")
    values = dict(
        uri=uri,
        sha256=digest,
        byte_size=len(evidence),
        mime_type="application/json",
        schema_version="fosun_gateway_market_snapshot_v1",
        source="fosun",
        observed_at=quote_raw.received_at,
        available_at=quote_raw.received_at,
        received_at=quote_raw.received_at,
    )
    await session.execute(
        insert(DataArtifact)
        .values(**values)
        .on_conflict_do_nothing(index_elements=[DataArtifact.sha256])
    )
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if artifact is None or any(getattr(artifact, name) != value for name, value in values.items()):
        raise ValueError("LOCAL_IDENTITY_DISCOVERY_RAW_BINDING_INVALID")
    return await _persist_pending_instrument_discoveries(
        session,
        store,
        quote_items=page.items,
        holdings=holdings,
        unresolved_codes=codes,
        holdings_observed_at=holdings_observed_at,
        quote_raw=quote_raw,
        quote_artifact=artifact,
    )


async def discover_identity_gaps(
    settings: LocalFosunRefreshSettings,
    *,
    repo_root: Path,
    gap_file: Path,
    item_indices: tuple[int, ...],
) -> dict[str, object]:
    codes = read_gap_selection(repo_root, gap_file, item_indices)
    broker = FosunGatewayClient(_broker_settings(settings))
    market = None
    engine = None
    try:
        accounts_page = await broker.read_accounts()
        if (
            accounts_page.quality != "PASS"
            or accounts_page.has_more
            or len(accounts_page.items) != 1
        ):
            raise ValueError("LOCAL_IDENTITY_DISCOVERY_ACCOUNT_INVALID")
        try:
            account = AccountItem.model_validate(accounts_page.items[0])
        except Exception:
            raise ValueError("LOCAL_IDENTITY_DISCOVERY_ACCOUNT_INVALID") from None
        holdings_page = await broker.read_holdings(
            account.external_account_id, cursor=None, limit=100
        )
        if (
            holdings_page.quality != "PASS"
            or holdings_page.has_more
            or len(holdings_page.items) > 100
        ):
            raise ValueError("LOCAL_IDENTITY_DISCOVERY_HOLDINGS_INVALID")
        try:
            holdings = _active_holdings(
                tuple(PositionItem.model_validate(item) for item in holdings_page.items)
            )
        except Exception:
            raise ValueError("LOCAL_IDENTITY_DISCOVERY_HOLDINGS_INVALID") from None
        if any(item.external_account_id != account.external_account_id for item in holdings):
            raise ValueError("LOCAL_IDENTITY_DISCOVERY_ACCOUNT_MISMATCH")
        held = {(item.instrument_code, item.currency) for item in holdings}
        if any((code, "USD") not in held for code in codes):
            raise ValueError("LOCAL_IDENTITY_DISCOVERY_SELECTION_UNAVAILABLE")
        engine = create_async_engine(settings.database_url, pool_pre_ping=True)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            identity = await _load_local_broker_identity(
                session,
                holdings=holdings,
                allow_unresolved=True,
                identity_store_factory=lambda: S3ProviderEvidenceStore(settings),
            )
        if any(code not in identity.unresolved_codes for code in codes):
            raise ValueError("LOCAL_IDENTITY_DISCOVERY_SELECTION_UNAVAILABLE")
        market = FosunMarketGatewayClient(_market_identity(settings))
        quote_raw = await _read_quote_snapshot(market, codes=codes)
        store = S3ProviderEvidenceStore(settings)
        async with sessions() as session, session.begin():
            counts = await _persist_discovery_batch(
                session,
                store,
                codes=codes,
                holdings=holdings,
                holdings_observed_at=holdings_page.as_of,
                quote_raw=quote_raw,
            )
        return {
            "status": "classification_saved",
            "discovered": len(codes),
            "remaining_identity_gaps": len(identity.unresolved_codes),
            "identity_route_counts": counts,
            "canonical_identities_created": 0,
            "account_snapshots_created": 0,
            "provider_requests": {"accounts": 1, "holdings": 1, "quotes": 1, "total": 3},
            "execution_locked": True,
        }
    finally:
        if market is not None:
            await market.aclose()
        await broker.aclose()
        if engine is not None:
            await engine.dispose()
