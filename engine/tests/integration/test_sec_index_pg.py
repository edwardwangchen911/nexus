"""Normal SEC index collector to immutable local evidence; no real HTTP or models."""

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID

import httpx
import pytest
from helpers.current_identity import CIK, MemoryStore, archives
from nexus.ops.instrument_identity_local import import_identity
from nexus.ops.sec_index_local import collect_index, import_index, inspect_index
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.runtime import Job, OutboxEvent
from nexus.persistence.session import session_factory
from nexus.research.sec_index_storage import compare_index_captures, read_index_capture
from nexus.settings import EngineSettings
from sqlalchemy import func, select


async def seed(database_url, tmp_path):
    settings = EngineSettings(
        database_url=database_url, object_store_endpoint="http://127.0.0.1:9000"
    )
    store = MemoryStore()
    fmp, sec = archives(tmp_path / "identity")
    identity = await import_identity(
        settings, symbol="AAPL", fmp_archive=fmp, sec_archive=sec, store=store
    )
    parent = tmp_path / "captures"
    parent.mkdir(mode=0o700)
    return settings, store, identity, parent


def submissions():
    accepted = datetime.now(UTC) - timedelta(days=1)
    return {
        "cik": int(CIK),
        "tickers": ["AAPL"],
        "exchanges": ["NYSE"],
        "filings": {
            "files": [],
            "recent": {
                "accessionNumber": [f"{CIK}-26-000001"],
                "form": ["8-K"],
                "acceptanceDateTime": [accepted.isoformat()],
                "filingDate": [accepted.date().isoformat()],
                "primaryDocument": ["first.htm"],
            },
        },
    }


def response(metadata, requests):
    def handle(request):
        requests.append(str(request.url))
        if request.url.host == "www.sec.gov":
            assert request.url.path == "/files/company_tickers_exchange.json"
            return httpx.Response(
                200,
                json={
                    "fields": ["cik", "name", "ticker", "exchange"],
                    "data": [[int(CIK), "Synthetic", "AAPL", "NYSE"]],
                },
            )
        assert str(request.url) == f"https://data.sec.gov/submissions/CIK{CIK}.json"
        return httpx.Response(200, json=metadata)

    return handle


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["same", "new", "new_partial", "partial", "conflict", "future"])
async def test_normal_sec_index_capture_readback_and_exact_new_rows(
    migrated_database_url, tmp_path, mode
):
    settings, store, identity, parent = await seed(migrated_database_url, tmp_path)
    metadata, requests = submissions(), []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(response(metadata, requests))
    ) as client:
        first = await collect_index(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archive_dir=parent / "first",
            contact="test@example.invalid",
            client=client,
            store=store,
        )
        if mode in {"new", "new_partial"}:
            recent = metadata["filings"]["recent"]
            for key, value in {
                "accessionNumber": f"{CIK}-26-000002",
                "form": "8-K/A",
                "acceptanceDateTime": recent["acceptanceDateTime"][0],
                "filingDate": recent["filingDate"][0],
                "primaryDocument": "second.htm",
            }.items():
                recent[key].append(value)
        elif mode == "partial":
            metadata["filings"]["files"] = [
                {
                    "name": "older.json",
                    "filingCount": 100,
                    "filingFrom": "2020-01-01",
                    "filingTo": "2024-12-31",
                }
            ]
        elif mode == "conflict":
            metadata["filings"]["recent"]["form"] = ["8-K/A"]
        if mode == "new_partial":
            metadata["filings"]["files"] = [{"name": "older.json"}]
        second = await collect_index(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archive_dir=parent / "second",
            contact="test@example.invalid",
            client=client,
            store=store,
        )
    assert first["provider_requests"] == second["provider_requests"] == 2
    assert len(requests) == 4 and first["snapshot_id"] != second["snapshot_id"]
    repeated = await import_index(
        settings,
        identity_snapshot_id=identity.snapshot_id,
        archive_dir=parent / "first",
        store=store,
    )
    assert repeated["status"] == "existing" and repeated["snapshot_id"] == first["snapshot_id"]
    assert repeated["available_at"] == first["available_at"]
    verified = await inspect_index(settings, snapshot_id=UUID(first["snapshot_id"]), store=store)
    assert verified["status"] == "verified" and verified["entries"] == 1
    baseline_as_of = datetime.fromisoformat(first["available_at"])
    if mode == "future":
        baseline_as_of = datetime.now(UTC) + timedelta(hours=1)
        with pytest.raises(ValueError, match="SEC_INDEX_BASELINE_INVALID"):
            await inspect_index(
                settings,
                snapshot_id=UUID(second["snapshot_id"]),
                baseline_id=UUID(first["snapshot_id"]),
                baseline_as_of=baseline_as_of,
                store=store,
            )
    else:
        result = await inspect_index(
            settings,
            snapshot_id=UUID(second["snapshot_id"]),
            baseline_id=UUID(first["snapshot_id"]),
            baseline_as_of=baseline_as_of,
            store=store,
        )
        assert (
            result["result"]
            == {
                "same": "false",
                "new": "true",
                "new_partial": "true",
                "partial": "unknown",
                "conflict": "unknown",
            }[mode]
        )
        assert result["event_ids"] == (
            [f"sec:{CIK}:{CIK}-26-000002"] if mode in {"new", "new_partial"} else []
        )
        assert result["model_requests"] == result["provider_requests"] == 0
        if mode == "new_partial":
            assert result["history_coverage"] == "partial"
        if mode == "conflict":
            assert result["reason"] == "SEC_INDEX_EVENT_CONFLICT"
    factory = session_factory(settings)
    async with factory() as session:
        baseline, old = await read_index_capture(
            session, store, snapshot_id=UUID(first["snapshot_id"]), as_of=datetime.now(UTC)
        )
        assert len(old.entries) == 1 and old.entries[0].form == "8-K"
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
        # Imported evidence is unavailable before actual local custody even though
        # its public accepted timestamp is older.
        with pytest.raises(ValueError, match="SEC_INDEX_CAPTURE_UNAVAILABLE"):
            await read_index_capture(
                session,
                store,
                snapshot_id=baseline.id,
                as_of=baseline.available_at - timedelta(minutes=1),
            )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_index_original_tampering_and_stale_source_remain_unknown(
    migrated_database_url, tmp_path
):
    settings, store, identity, parent = await seed(migrated_database_url, tmp_path)
    metadata, requests = submissions(), []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(response(metadata, requests))
    ) as client:
        first = await collect_index(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archive_dir=parent / "first",
            contact="test@example.invalid",
            client=client,
            store=store,
        )
    factory = session_factory(settings)
    async with factory() as session:
        row = await session.get(DataSnapshot, UUID(first["snapshot_id"]))
        result = await compare_index_captures(
            session,
            store,
            baseline_id=row.id,
            current_id=row.id,
            baseline_as_of=row.available_at,
            now=row.available_at + timedelta(minutes=6),
        )
        assert result["result"] == "unknown" and result["reason"] == "SEC_INDEX_STALE"
        binding = row.payload["originals"][2]
        key = binding["uri"].split("s3://synthetic/", 1)[1]
        store.objects[key] = b"{}"
        with pytest.raises(ValueError, match="SEC_INDEX_ARTIFACT_INVALID"):
            await read_index_capture(session, store, snapshot_id=row.id, as_of=datetime.now(UTC))
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_existing_hash_with_lost_original_cannot_create_a_broken_capture(
    migrated_database_url, tmp_path
):
    settings, store, identity, parent = await seed(migrated_database_url, tmp_path)
    metadata, requests = submissions(), []
    raw = json.dumps(metadata, separators=(",", ":")).encode()
    key = "raw/sec/earlier-source.json"
    uri = await store.put_if_absent(key=key, content=raw)
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        at = await session.scalar(select(func.clock_timestamp()))
        session.add(
            DataArtifact(
                uri=uri,
                sha256=sha256(raw).hexdigest(),
                byte_size=len(raw),
                source="sec",
                schema_version="sec_index_raw_v1",
                mime_type="application/json",
                observed_at=at,
                available_at=at,
                received_at=at,
            )
        )
    store.objects[key] = b"{}"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(response(metadata, requests))
    ) as client:
        with pytest.raises(ValueError, match="SEC_INDEX_ARTIFACT_INVALID"):
            await collect_index(
                settings,
                identity_snapshot_id=identity.snapshot_id,
                archive_dir=parent / "fresh",
                contact="test@example.invalid",
                client=client,
                store=store,
            )
    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataSnapshot)
                .where(DataSnapshot.source == "sec_disclosure_index")
            )
            == 0
        )
    await factory.kw["bind"].dispose()
