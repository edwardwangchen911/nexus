"""PostgreSQL tests for raw-first immutable intraday ingestion."""

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from nexus.jobs.market_authority import ProviderMarketBinding
from nexus.market.intraday_ingestion import (
    archive_intraday_snapshot_raw,
    ingest_intraday_snapshot,
)
from nexus.market.object_store import ProviderObjectIOUnavailable
from nexus.persistence.models.data import (
    DataArtifact,
    IntradayBarSetAuthority,
    PriceBarRecord,
    PriceSeriesManifest,
    ProviderCapability,
)
from nexus.persistence.models.market import Instrument, LatestQuote
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.providers.base import InstrumentId, MarketDataProvenance, PriceBar
from nexus.providers.market.snapshot_contracts import (
    MarketSnapshot,
    RawSnapshotEnvelope,
    SnapshotBatch,
    SnapshotMinuteBar,
    SnapshotQuote,
    SnapshotTrade,
)
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

AAPL_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a1"))
MSFT_ID = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a2"))
RECEIVED_AT = datetime(2026, 8, 28, 14, 30, 16, tzinfo=UTC)


@dataclass
class _ObjectStore:
    fail_at: str | None = None
    claim_wrong_uri_at: str | None = None
    objects: dict[str, bytes] = field(default_factory=dict)
    temporary: dict[str, bytes] = field(default_factory=dict)
    archived_payloads: list[bytes] = field(default_factory=list)
    normalized_writes: list[str] = field(default_factory=list)
    temporary_writes: int = 0

    async def archive_raw(self, key: str, payload: bytes) -> str:
        if self.fail_at == "archive_raw":
            raise OSError("fixture archive failure")
        if key not in self.objects:
            self.objects[key] = payload
            self.archived_payloads.append(payload)
        if self.claim_wrong_uri_at == "archive_raw":
            return f"memory://wrong-object?claimed={key}"
        return f"memory://{key}"

    async def put_temporary(self, key: str, payload: bytes) -> None:
        self.temporary_writes += 1
        if self.fail_at == "put_temporary":
            raise OSError("fixture temporary failure")
        self.temporary[key] = payload

    async def promote(self, temporary_key: str, final_key: str) -> str:
        if self.fail_at == "promote":
            raise OSError("fixture promotion failure")
        self.objects.setdefault(final_key, self.temporary[temporary_key])
        if self.claim_wrong_uri_at == "promote":
            return f"memory://wrong-object?claimed={final_key}"
        return f"memory://{final_key}"

    async def delete_temporary(self, key: str) -> None:
        self.temporary.pop(key, None)

    async def put_normalized(self, key: str, payload: bytes) -> str:
        if self.fail_at == "put_normalized":
            raise OSError("fixture normalized failure")
        self.objects.setdefault(key, payload)
        self.normalized_writes.append(key)
        if self.claim_wrong_uri_at == "put_normalized":
            return f"memory://wrong-object?claimed={key}"
        return f"memory://{key}"


def _batch(
    *,
    aapl_close: str = "227.16",
    received_at: datetime = RECEIVED_AT,
) -> SnapshotBatch:
    return SnapshotBatch(
        snapshots=(
            _snapshot(AAPL_ID, "AAPL", aapl_close, received_at=received_at),
            _snapshot(MSFT_ID, "MSFT", "510.25", received_at=received_at),
        ),
        raw_payload=f'{{"aapl_close":"{aapl_close}","fixture":"intraday"}}'.encode(),
        received_at=received_at,
        provenance=MarketDataProvenance(
            source="alpaca",
            feed="iex",
            adjustment="raw",
            interval="1Min",
            mode="read_only",
            delivery_mode="real_time",
        ),
    )


def _snapshot(
    instrument_id: InstrumentId,
    symbol: str,
    close: str,
    *,
    received_at: datetime,
) -> MarketSnapshot:
    observed = received_at.replace(second=0, microsecond=0) - timedelta(minutes=1)
    price = Decimal(close)
    return MarketSnapshot(
        instrument_id=instrument_id,
        symbol=symbol,
        trade=SnapshotTrade(
            price=price,
            observed_at=RECEIVED_AT.replace(second=14, microsecond=400_000),
            provider_trade_id=f"{symbol}-trade",
        ),
        quote=SnapshotQuote(
            bid=price - Decimal("0.02"),
            ask=price + Decimal("0.02"),
            observed_at=RECEIVED_AT.replace(second=14, microsecond=500_000),
        ),
        minute_bar=SnapshotMinuteBar(
            bar=PriceBar(
                instrument_id=instrument_id,
                session=date(2026, 8, 28),
                open=price - Decimal("0.16"),
                high=price + Decimal("0.04"),
                low=price - Decimal("0.18"),
                close=price,
                volume=Decimal("1200"),
                observed_at=observed,
                available_at=received_at,
                received_at=received_at,
                revision="snapshot-v1",
            )
        ),
    )


async def _seed_instruments(database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session, session.begin():
        session.add_all(
            Instrument(
                id=instrument_id,
                asset_type="equity",
                exchange="NASDAQ",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="large_cap",
            )
            for instrument_id in (AAPL_ID, MSFT_ID)
        )


@pytest.mark.asyncio
async def test_intraday_ingestion_is_raw_first_and_replay_idempotent(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore()
    async with factory() as session, session.begin():
        first = await ingest_intraday_snapshot(session, store, _batch(), commit=False)
    async with factory() as session, session.begin():
        replay = await ingest_intraday_snapshot(session, store, _batch(), commit=False)

    assert first.created_trade_observations == 2
    assert first.created_quote_observations == 2
    assert first.created_bar_authorities == 2
    assert first.replayed == 0
    assert replay.created_trade_observations == 0
    assert replay.created_quote_observations == 0
    assert replay.created_bar_authorities == 0
    assert replay.replayed == 6
    assert store.archived_payloads == [_batch().raw_payload]
    assert store.temporary == {}
    assert store.temporary_writes == 0
    assert len(store.normalized_writes) == 4

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 4
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 3
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 2
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 2
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 2


@pytest.mark.asyncio
async def test_alpaca_distinct_raw_snapshots_do_not_collapse_identical_minute_bars(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore()
    first_batch = _batch()
    second_batch = first_batch.model_copy(
        update={"raw_payload": b'{"fixture":"intraday","quote_revision":2}'}
    )
    first_raw_digest = sha256(first_batch.raw_payload).hexdigest()
    second_raw_digest = sha256(second_batch.raw_payload).hexdigest()
    assert first_raw_digest != second_raw_digest

    async with factory() as session, session.begin():
        first = await ingest_intraday_snapshot(session, store, first_batch, commit=False)
    async with factory() as session, session.begin():
        second = await ingest_intraday_snapshot(session, store, second_batch, commit=False)

    assert first.created_bar_authorities == 2
    assert second.created_bar_authorities == 2
    async with factory() as session:
        revisions = set(await session.scalars(select(PriceBarRecord.revision)))
        assert revisions == {first_raw_digest, second_raw_digest}
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 4
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 4


@pytest.mark.asyncio
async def test_malformed_raw_envelope_is_archived_without_business_facts(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore()
    envelope = RawSnapshotEnvelope(
        raw_payload=b'{"AAPL":{},"AAPL":{}}',
        received_at=RECEIVED_AT,
        provenance=MarketDataProvenance(
            source="alpaca",
            feed="iex",
            adjustment="raw",
            interval="1Min",
            mode="read_only",
            delivery_mode="real_time",
        ),
    )

    async with factory() as session, session.begin():
        archived = await archive_intraday_snapshot_raw(
            session,
            store,
            envelope,
            commit=False,
        )

    assert archived.raw_artifact.sha256 in archived.raw_uri
    assert store.archived_payloads == [envelope.raw_payload]
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 0
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_fosun_intraday_facts_persist_exact_binding_and_v2_authority(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    status_id = UUID("019d0000-0000-7000-8000-0000000000f0")
    quote_capability_id = UUID("019d0000-0000-7000-8000-0000000000f1")
    bars_capability_id = UUID("019d0000-0000-7000-8000-0000000000f2")
    contract_hash = "a" * 64
    created_at = RECEIVED_AT - timedelta(days=1)
    authorized_at = RECEIVED_AT + timedelta(seconds=1)
    async with factory() as session, session.begin():
        session.add(
            IntegrationStatus(
                id=status_id,
                provider="fosun",
                status="read_only",
                last_success_at=created_at,
                version=1,
                mode_changed_at=created_at,
                capability_invalidated_at=None,
                admitted_contract_hash=contract_hash,
                created_at=created_at,
            )
        )
        session.add_all(
            ProviderCapability(
                id=capability_id,
                provider="fosun",
                capability="READ_ONLY",
                contract_hash=contract_hash,
                expires_at=datetime(2027, 1, 1, tzinfo=UTC),
                schema_version="1",
                payload={
                    "operation": operation,
                    "market_scope": "US",
                    "delivery_mode": "unknown",
                },
                created_at=created_at,
            )
            for capability_id, operation in (
                (quote_capability_id, "fosun_get_quotes"),
                (bars_capability_id, "fosun_get_bars"),
            )
        )

    price = Decimal("227.16")
    observed = RECEIVED_AT.replace(second=0, microsecond=0) - timedelta(minutes=1)
    batch = SnapshotBatch(
        snapshots=(
            MarketSnapshot(
                instrument_id=AAPL_ID,
                symbol="AAPL",
                trade=SnapshotTrade(
                    price=price,
                    observed_at=RECEIVED_AT,
                    event_kind="snapshot",
                    quality_flags=(
                        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
                        "INTRADAY_BID_ASK_UNAVAILABLE",
                    ),
                ),
                quote=None,
                minute_bar=SnapshotMinuteBar(
                    bar=PriceBar(
                        instrument_id=AAPL_ID,
                        session=observed.date(),
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                        volume=Decimal("1"),
                        observed_at=observed,
                        available_at=RECEIVED_AT,
                        received_at=RECEIVED_AT,
                        revision="b" * 64,
                    )
                ),
            ),
        ),
        raw_payload=b'{"schema":"fosun_gateway_market_snapshot_v1"}',
        received_at=RECEIVED_AT,
        provenance=MarketDataProvenance(
            source="fosun",
            feed="l1",
            adjustment="raw",
            interval="1Min",
            mode="read_only",
            delivery_mode="unknown",
        ),
    )
    quote_binding = ProviderMarketBinding(
        provider_capability_id=quote_capability_id,
        provider_status_id=status_id,
        provider_status_version=1,
        provider_contract_hash=contract_hash,
        provider_authorized_at=authorized_at,
    )
    bars_binding = quote_binding.model_copy(update={"provider_capability_id": bars_capability_id})

    async with factory() as session, session.begin():
        result = await ingest_intraday_snapshot(
            session,
            _ObjectStore(),
            batch,
            quote_binding=quote_binding,
            bar_binding=bars_binding,
            commit=False,
        )

    assert result.created_trade_observations == 1
    assert result.created_quote_observations == 0
    assert result.created_bar_authorities == 1
    async with factory() as session:
        quote = await session.scalar(select(LatestQuote).where(LatestQuote.source == "fosun"))
        manifest = await session.scalar(
            select(PriceSeriesManifest).where(PriceSeriesManifest.source == "fosun")
        )
        authority = await session.scalar(
            select(IntradayBarSetAuthority).where(IntradayBarSetAuthority.source == "fosun")
        )
        raw = await session.scalar(
            select(DataArtifact).where(
                DataArtifact.schema_version == "fosun_gateway_market_snapshot_v1"
            )
        )
    assert quote is not None and manifest is not None and authority is not None and raw is not None
    assert quote.event_kind == "snapshot"
    assert quote.quality_flags == [
        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
        "INTRADAY_BID_ASK_UNAVAILABLE",
    ]
    assert quote.provider_capability_id == quote_capability_id
    assert manifest.provider_capability_id == bars_capability_id
    assert authority.hash_version == "intraday_bar_set_sha256_v2"
    assert authority.policy_version == "intraday_monitor_authority_v2"


@pytest.mark.asyncio
async def test_changed_snapshot_raw_creates_new_batch_bar_authorities_without_rewrite(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore()
    async with factory() as session, session.begin():
        await ingest_intraday_snapshot(session, store, _batch(), commit=False)
    async with factory() as session, session.begin():
        changed = await ingest_intraday_snapshot(
            session, store, _batch(aapl_close="227.17"), commit=False
        )

    # One raw Snapshot envelope is the lineage root for every bar in the batch,
    # so a new envelope revision creates a new immutable authority for each.
    assert changed.created_bar_authorities == 2
    async with factory() as session:
        rows = (
            await session.scalars(
                select(IntradayBarSetAuthority).order_by(
                    IntradayBarSetAuthority.first_observed_at,
                    IntradayBarSetAuthority.created_at,
                )
            )
        ).all()
        assert len(rows) == 4
        assert len({row.content_hash for row in rows}) == 4


@pytest.mark.asyncio
async def test_identical_raw_bytes_reuse_content_object_across_delivery_buckets(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore()
    async with factory() as session, session.begin():
        first = await ingest_intraday_snapshot(session, store, _batch(), commit=False)
    async with factory() as session, session.begin():
        replayed_content = await ingest_intraday_snapshot(
            session,
            store,
            _batch(received_at=RECEIVED_AT + timedelta(seconds=15)),
            commit=False,
        )

    assert replayed_content.raw_artifact.id == first.raw_artifact.id
    assert replayed_content.raw_uri == first.raw_uri
    assert store.archived_payloads == [_batch().raw_payload]


@pytest.mark.asyncio
async def test_published_intraday_manifest_rejects_late_bar_append(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await ingest_intraday_snapshot(session, _ObjectStore(), _batch(), commit=False)
    async with factory() as session:
        manifest = await session.scalar(
            select(PriceSeriesManifest).where(
                PriceSeriesManifest.instrument == str(AAPL_ID),
                PriceSeriesManifest.interval == "1Min",
            )
        )
    assert manifest is not None
    late_at = RECEIVED_AT + timedelta(minutes=1)

    with pytest.raises(DBAPIError, match="frozen price series"):
        async with factory() as session, session.begin():
            session.add(
                PriceBarRecord(
                    price_series_manifest_id=manifest.id,
                    instrument_id=AAPL_ID,
                    session=late_at.date(),
                    open=Decimal("228"),
                    high=Decimal("229"),
                    low=Decimal("227"),
                    close=Decimal("228"),
                    volume=Decimal("100"),
                    observed_at=late_at,
                    available_at=late_at,
                    received_at=late_at,
                    revision="late-v1",
                    quality_flags=[],
                )
            )
            await session.flush()

    with pytest.raises(DBAPIError, match="authority record is immutable"):
        async with factory() as session, session.begin():
            await session.execute(
                text("UPDATE price_series_manifests SET feed='tampered' WHERE id=:id"),
                {"id": manifest.id},
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_kind", ("in_progress", "quality_flags"))
async def test_intraday_authority_rejects_incomplete_or_unbounded_bar_facts(
    migrated_database_url: str,
    invalid_kind: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    batch = _batch()
    first = batch.snapshots[0]
    assert first.minute_bar is not None
    bar = first.minute_bar.bar
    invalid_bar = bar.model_copy(
        update=(
            {
                "observed_at": batch.received_at.replace(second=0, microsecond=0),
            }
            if invalid_kind == "in_progress"
            else {"quality_flags": tuple(f"FLAG_{index}" for index in range(17))}
        )
    )
    invalid_snapshot = first.model_copy(
        update={"minute_bar": first.minute_bar.model_copy(update={"bar": invalid_bar})}
    )
    invalid_batch = batch.model_copy(update={"snapshots": (invalid_snapshot, *batch.snapshots[1:])})
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    with pytest.raises(DBAPIError, match="intraday bar review authority is invalid"):
        async with factory() as session, session.begin():
            await ingest_intraday_snapshot(
                session,
                _ObjectStore(),
                invalid_batch,
                commit=False,
            )


@pytest.mark.asyncio
async def test_intraday_authority_rejects_non_array_quality_flags_deterministically(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    artifact_id = uuid4()
    evidence_id = uuid4()
    manifest_id = uuid4()
    digest = "a" * 64
    observed_at = RECEIVED_AT.replace(second=0, microsecond=0) - timedelta(minutes=1)
    async with factory() as session, session.begin():
        session.add(
            DataArtifact(
                id=artifact_id,
                uri=f"memory://sha256/{digest}",
                sha256=digest,
                byte_size=1,
                mime_type="application/json",
                schema_version="test_v1",
                source="alpaca",
                observed_at=observed_at,
                available_at=RECEIVED_AT,
                received_at=RECEIVED_AT,
            )
        )
        session.add(
            ResearchEvidenceRef(
                id=evidence_id,
                source="alpaca",
                provider_object_id="quality-flags-scalar",
                revision="revision-v1",
                available_at=RECEIVED_AT,
                location=f"memory://sha256/{digest}",
                content_hash=digest,
                byte_length=1,
            )
        )
        await session.flush()
        session.add(
            PriceSeriesManifest(
                id=manifest_id,
                evidence_ref_id=evidence_id,
                data_artifact_id=artifact_id,
                instrument=str(AAPL_ID),
                source="alpaca",
                feed="iex",
                delivery_mode="real_time",
                adjustment="raw",
                interval="1Min",
                start_at=observed_at,
                end_at=observed_at,
                row_count=1,
            )
        )
        await session.flush()
        await session.execute(
            text(
                "INSERT INTO price_bar_records "
                "(id,price_series_manifest_id,instrument_id,session,open,high,low,close,"
                "volume,observed_at,available_at,received_at,revision,quality_flags) "
                "VALUES (:id,:manifest_id,:instrument_id,:session,100,101,99,100,1000,"
                ":observed_at,:received_at,:received_at,'revision-v1','{}'::jsonb)"
            ),
            {
                "id": uuid4(),
                "manifest_id": manifest_id,
                "instrument_id": AAPL_ID,
                "session": observed_at.date(),
                "observed_at": observed_at,
                "received_at": RECEIVED_AT,
            },
        )
        with pytest.raises(DBAPIError, match="intraday bar review authority is invalid") as error:
            async with session.begin_nested():
                await session.execute(
                    text(
                        "INSERT INTO intraday_bar_set_authorities "
                        "(id,price_series_manifest_id,evidence_ref_id,source_policy_id,source,"
                        "provider_object_id,revision,hash_version,policy_version,content_hash,"
                        "byte_length,bar_count,first_observed_at,last_observed_at,available_at) "
                        "VALUES (:id,:manifest_id,:evidence_id,:policy_id,'alpaca',"
                        "'quality-flags-scalar','revision-v1','intraday_bar_set_sha256_v1',"
                        "'intraday_monitor_authority_v1',:digest,1,1,:observed_at,"
                        ":observed_at,:received_at)"
                    ),
                    {
                        "id": uuid4(),
                        "manifest_id": manifest_id,
                        "evidence_id": evidence_id,
                        "policy_id": UUID("8fab80c9-5b6a-5b2b-8f36-3b22319b54ef"),
                        "digest": digest,
                        "observed_at": observed_at,
                        "received_at": RECEIVED_AT,
                    },
                )

    assert error.value.orig.sqlstate == "23514"


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", ("archive_raw", "put_normalized"))
async def test_object_store_failure_leaves_no_readable_intraday_authority(
    migrated_database_url: str,
    fail_at: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore(fail_at=fail_at)
    async with factory() as session:
        with pytest.raises(OSError):
            await ingest_intraday_snapshot(session, store, _batch())

    assert store.temporary_writes == 0
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 0
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["OBJECT_IO_SATURATED", "OBJECT_IO_DEADLINE_EXCEEDED"],
)
async def test_typed_normalized_write_rolls_back_before_error(
    migrated_database_url: str,
    error_code: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    class _NormalizedUnavailableStore(_ObjectStore):
        async def put_normalized(self, key: str, payload: bytes) -> str:
            del key, payload
            raise ProviderObjectIOUnavailable(error_code)

    store = _NormalizedUnavailableStore()
    async with factory() as session:
        with pytest.raises(ProviderObjectIOUnavailable, match=error_code):
            await ingest_intraday_snapshot(session, store, _batch())
        # Prove the public commit=True path cleaned its failed transaction;
        # catching the error and reusing the same session cannot leak facts.
        await session.commit()
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 0
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 0

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 0
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("claim_at", "error_code"),
    (
        ("archive_raw", "INTRADAY_RAW_OBJECT_URI_MISMATCH"),
        ("put_normalized", "INTRADAY_NORMALIZED_OBJECT_URI_MISMATCH"),
    ),
)
async def test_query_only_object_uri_claim_publishes_zero_intraday_facts(
    migrated_database_url: str,
    claim_at: str,
    error_code: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore(claim_wrong_uri_at=claim_at)

    async with factory() as session:
        with pytest.raises(RuntimeError, match=error_code):
            await ingest_intraday_snapshot(session, store, _batch())

    assert store.temporary == {}
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_detached_raw_artifact_identity_drift_publishes_zero_intraday_facts(
    migrated_database_url: str,
) -> None:
    await _seed_instruments(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _ObjectStore()
    batch = _batch()
    envelope = RawSnapshotEnvelope(
        raw_payload=batch.raw_payload,
        received_at=batch.received_at,
        provenance=batch.provenance,
    )
    async with factory() as session:
        archived = await archive_intraday_snapshot_raw(session, store, envelope)

    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE data_artifacts DISABLE TRIGGER USER"))
        await session.execute(
            text("UPDATE data_artifacts SET uri = :uri WHERE id = :artifact_id"),
            {
                "artifact_id": archived.raw_artifact.id,
                "uri": "memory://raw/market/intraday/sha256/00/" + "0" * 64 + ".json",
            },
        )
        await session.execute(text("ALTER TABLE data_artifacts ENABLE TRIGGER USER"))

    async with factory() as session:
        with pytest.raises(RuntimeError, match="INTRADAY_RAW_ARCHIVE_MISMATCH"):
            await ingest_intraday_snapshot(
                session,
                store,
                batch,
                raw_artifact=archived.raw_artifact,
            )

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(IntradayBarSetAuthority)) == 0
