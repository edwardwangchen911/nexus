"""Persisted automatic Schwab intraday job path with only provider I/O replaced."""

import hashlib
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import httpx
import pytest
from nexus.jobs.intraday_collection import build_intraday_collection_registry
from nexus.jobs.intraday_schedule import schedule_due_intraday_snapshot
from nexus.jobs.models import QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.market.database_series import load_intraday_series
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.runtime import Job, JobAttempt
from nexus.persistence.session import session_factory
from nexus.providers.market.schwab_snapshot import SchwabSnapshotProvider
from nexus.providers.settings import ProviderSettings
from nexus.settings import EngineSettings
from sqlalchemy import func, select

from tests.integration.test_intraday_collection_pg import AAPL_ID, NOW, _seed_schedule


class _Store:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def archive_raw(self, key: str, payload: bytes) -> str:
        self.objects.setdefault(key, payload)
        return f"memory://{key}"

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        assert uri == f"memory://{key}"
        return self.objects[key]

    async def put_normalized(self, key: str, payload: bytes) -> str:
        self.objects.setdefault(key, payload)
        return f"memory://{key}"


def _settings() -> ProviderSettings:
    return ProviderSettings(
        runtime_mode="production",
        market_provider="fosun",
        intraday_provider="schwab",
        intraday_monitor_mode="production",
        intraday_max_symbols=30,
        fosun_market_gateway_origin="https://fosun-gateway.internal:8443",
        fosun_market_gateway_key_id="synthetic-market-current",
        fosun_market_gateway_secret="synthetic-market-secret",
        fosun_market_gateway_client_cert="/synthetic/market.crt",
        fosun_market_gateway_client_key="/synthetic/market.key",
        fosun_market_gateway_client_ca="/synthetic/ca.crt",
        sec_contact="owner@example.com",
        fred_api_key="fred-canary",
    )


def _login(millis: int) -> dict[str, object]:
    return {
        "response": [
            {
                "service": "ADMIN",
                "command": "LOGIN",
                "requestid": "1",
                "timestamp": millis,
                "content": {"code": 0, "msg": "LOGIN command succeeded"},
            }
        ]
    }


def _subscription(millis: int) -> dict[str, object]:
    return {
        "response": [
            {
                "service": "LEVELONE_EQUITIES",
                "command": "SUBS",
                "requestid": "2",
                "timestamp": millis,
                "content": {"code": 0, "msg": "SUBS command succeeded"},
            }
        ]
    }


def _data(millis: int) -> dict[str, object]:
    return {
        "data": [
            {
                "service": "LEVELONE_EQUITIES",
                "timestamp": millis,
                "command": "SUBS",
                "content": [
                    {
                        "key": symbol,
                        "1": bid,
                        "2": ask,
                        "3": last,
                        "34": millis,
                        "35": millis,
                        "37": millis,
                        "38": millis,
                        "delayed": False,
                        "assetMainType": "EQUITY",
                    }
                    for symbol, bid, ask, last in (
                        ("AAPL", "227.14", "227.18", "227.15"),
                        ("MSFT", "509.90", "510.10", "510.00"),
                    )
                ],
            }
        ]
    }


def _write_archive(directory: Path, *, complete: bool) -> None:
    started_at = NOW - timedelta(seconds=2)
    millis = int((NOW - timedelta(seconds=1)).timestamp() * 1000)
    values = [_login(millis - 2), _subscription(millis - 1)]
    if complete:
        values.append(_data(millis))
    frames = [json.dumps(value, separators=(",", ":")).encode() for value in values]
    entries = []
    for index, raw in enumerate(frames):
        name = f"frame-{index:05}.json"
        path = directory / name
        path.write_bytes(raw)
        path.chmod(0o600)
        entries.append(
            {
                "file": name,
                "received_at": (started_at + timedelta(milliseconds=500 * (index + 1))).isoformat(),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "raw_byte_length": len(raw),
            }
        )
    receipt = {
        "schema": "nexus_provider_market_stream_receipt_v1",
        "source": "schwab",
        "transport": "websocket",
        "origin": "wss://streamer-api.schwab.com/ws",
        "symbols": ["AAPL", "MSFT"],
        "started_at": started_at.isoformat(),
        "ended_at": NOW.isoformat(),
        "authenticated": True,
        "subscription_confirmed": True,
        "status": "bounded_complete" if complete else "partial",
        "stop_reason": "DURATION_LIMIT" if complete else "DISCONNECTED",
        "frames": entries,
        "observation_count": 2 if complete else 0,
        "requested_duration_seconds": 15.0,
        "requested_max_frames": 100,
        "elapsed_monotonic_seconds": 2.0,
        "frame_count": len(frames),
    }
    receipt_path = directory / "receipt.json"
    receipt_path.write_text(json.dumps(receipt, sort_keys=True))
    receipt_path.chmod(0o600)


@pytest.mark.asyncio
@pytest.mark.parametrize("complete", (True, False))
async def test_automatic_schwab_job_freezes_raw_before_publish_or_failure(
    migrated_database_url: str,
    tmp_path: Path,
    complete: bool,
) -> None:
    await _seed_schedule(migrated_database_url)
    settings = _settings()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        job = await schedule_due_intraday_snapshot(session, now=NOW, settings=settings)
    assert job is not None and job.payload["source"] == "schwab"

    async def capture(arguments, _client, *, now=None):
        assert arguments.symbol == ["AAPL", "MSFT"]
        assert now == NOW
        _write_archive(arguments.output_dir, complete=complete)
        return {"status": "bounded_complete" if complete else "partial"}

    store = _Store()
    async with httpx.AsyncClient() as client:
        provider = SchwabSnapshotProvider(
            tmp_path,
            client,
            now=lambda: NOW,
            capture=capture,
        )
        registry = build_intraday_collection_registry(
            lambda _session: provider,
            cast(object, store),
            lambda: NOW,
            settings=settings,
            raw_archive_session_factory=factory,
        )
        assert await run_job_worker_once(
            factory,
            registry=registry,
            queue=QueueName.PROVIDER,
            worker_id=f"schwab-automatic-{complete}",
            worker_started_at=NOW,
            process_version="test-v1",
            migration_head=CURRENT_MIGRATION_HEAD,
            clock=lambda: NOW,
            claim_registered_only=True,
        )

    async with factory() as session:
        persisted = await session.get(Job, job.id)
        attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
        artifact = await session.scalar(select(DataArtifact))
        quote_count = await session.scalar(select(func.count()).select_from(LatestQuote))
        assert persisted is not None and attempt is not None and artifact is not None
        assert artifact.source == "schwab"
        assert artifact.schema_version == "schwab_level_one_stream_v1"
        if complete:
            view = await load_intraday_series(session, AAPL_ID, NOW + timedelta(seconds=1))
            assert persisted.status == "succeeded"
            assert persisted.result_code == "INTRADAY_SNAPSHOT_COLLECTED"
            assert attempt.outcome == "succeeded"
            assert quote_count == 4
            assert view.trade is not None and view.trade.last == Decimal("227.15")
            assert view.quote is not None and view.quote.bid == Decimal("227.14")
            assert view.quote.ask == Decimal("227.18")
        else:
            assert persisted.status == "dead_letter"
            assert persisted.last_error_code == "INTRADAY_PROVIDER_UNAVAILABLE"
            assert attempt.outcome == "failed"
            assert attempt.error_code == "INTRADAY_PROVIDER_UNAVAILABLE"
            assert quote_count == 0

    assert len(store.objects) == 1
    assert next(iter(store.objects)).startswith("raw/market/intraday/")
