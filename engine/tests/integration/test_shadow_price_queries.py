"""HTTP-only doubles drive actual receipt ingestion and the Shadow authority reader."""

import asyncio
import json
import sys
from copy import deepcopy
from datetime import UTC, datetime, time, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
import pytest
from helpers.portfolio_api import settings
from helpers.shadow import seed_shadow_query_proof, seed_started_shadow
from nexus.persistence.models.data import DataArtifact, DataSnapshot, PriceBarRecord
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.shadow import ShadowGroupRecord, ShadowSessionAuthorityRecord
from nexus.persistence.session import session_factory
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.shadow.authority import LockedShadowRoot, load_shadow_session_input
from sqlalchemy import func, select, text


class ObjectClient:
    def __init__(self):
        self.objects = {}

    async def archive_raw(self, key, payload):
        self.objects.setdefault(key, payload)
        assert self.objects[key] == payload
        return f"s3://test/{key}?versionId=fixture"

    async def put_normalized(self, key, payload):
        return await self.archive_raw(key, payload)

    async def read_raw(self, key, *, uri):
        assert uri == f"s3://test/{key}?versionId=fixture"
        return self.objects[key]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    ["bars", "empty", "pages", "missing_cursor", "wrong_symbol", "cli", "cycle", "page_limit"],
)
async def test_http_collection_persists_exact_proof_and_replays_without_http(
    migrated_database_url,
    scenario,
    monkeypatch,
    tmp_path,
    capsys,
):
    from helpers import portfolio_api
    from nexus.shadow.price_collection import ShadowPriceQueryRequest, collect_shadow_prices

    def synthetic_us_master(**values):
        return Instrument(**{**values, "exchange": "XNYS", "currency": "USD"})

    def synthetic_us_alias(**values):
        symbol = "TEST" if values["symbol"] == "00656.HK" else "OTHER"
        return InstrumentAlias(**{**values, "symbol": symbol})

    # Construct synthetic US facts before freezing; no saved master is updated.
    with monkeypatch.context() as setup:
        setup.setattr(portfolio_api, "Instrument", synthetic_us_master)
        setup.setattr(portfolio_api, "InstrumentAlias", synthetic_us_alias)
        seeded = await seed_started_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    day = seeded.start.start_session
    day_start = datetime.combine(day, time.min, ZoneInfo("America/New_York")).astimezone(UTC)
    now = day_start + timedelta(days=2)
    async with factory() as session, session.begin():
        for index, instrument in enumerate(seeded.eligible.instrument_ids):
            if not await session.scalar(
                select(InstrumentAlias.id).where(
                    InstrumentAlias.instrument_id == instrument,
                    InstrumentAlias.alias_type == "ticker",
                )
            ):
                session.add(
                    InstrumentAlias(
                        instrument_id=instrument,
                        symbol=f"TST{chr(65 + index)}",
                        alias_type="ticker",
                        valid_from=day,
                        created_at=day_start,
                    )
                )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=seeded.start.shadow_group_id,
            session_date=day,
            kind="corporate_actions",
            source="fixture",
            delivery_mode="fixture",
            instrument_ids=seeded.eligible.instrument_ids,
            complete=True,
            available_at=day_start,  # Closed synthetic event universe, not Alpaca observations.
        )
    calls = []
    empty = scenario == "empty"

    async def response(request):
        calls.append(request)
        assert request.url.params["asof"] == day.isoformat()
        assert request.url.params["adjustment"] == "raw"
        assert request.url.params["feed"] == "sip"
        if scenario == "missing_cursor" and len(calls) == 2:
            return httpx.Response(200, json={"symbol": request.url.path.split("/")[-2], "bars": []})
        if scenario == "wrong_symbol":
            return httpx.Response(
                200, json={"symbol": "WRONG", "bars": [], "next_page_token": None}
            )
        if scenario in {"cycle", "page_limit"}:
            return httpx.Response(
                200,
                json={
                    "symbol": request.url.path.split("/")[-2],
                    "bars": [],
                    "next_page_token": "cycle" if scenario == "cycle" else str(len(calls)),
                },
            )
        first_page = (
            scenario in {"pages", "missing_cursor"} and "page_token" not in request.url.params
        )
        if scenario == "pages" and not first_page:
            assert request.url.params["page_token"] == "next"
        return httpx.Response(
            200,
            json={
                "symbol": request.url.path.split("/")[-2],
                "bars": []
                if empty or (scenario == "pages" and not first_page)
                else [
                    {
                        "t": day_start.isoformat(),
                        "o": 100,
                        "h": 101,
                        "l": 99,
                        "c": 100,
                        "v": 1000,
                    }
                ],
                "next_page_token": "next" if first_page else None,
            },
        )

    store = ObjectClient()
    request = ShadowPriceQueryRequest(
        shadow_group_id=seeded.start.shadow_group_id,
        session_date=day,
        idempotency_key="source-query",
        feed="sip",
        delivery_mode="delayed",
    )
    from pydantic import SecretStr

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        client = FixedOriginHttpClient(
            provider_name="alpaca",
            origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
            policy=HttpRequestPolicy(max_attempts=1),
            client=http,
        )
        if scenario == "bars":
            async with factory() as holder, holder.begin():
                await holder.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                    {"key": f"shadow-price:{request.shadow_group_id}:{request.idempotency_key}"},
                )
                with pytest.raises(ValueError, match="SHADOW_PRICE_QUERY_BUSY"):
                    await collect_shadow_prices(
                        factory,
                        request,
                        client=client,
                        store=store,
                        key_id=SecretStr("test"),
                        secret=SecretStr("test"),
                        now=lambda: now,
                    )
                assert calls == []
        if scenario in {"missing_cursor", "wrong_symbol", "cycle", "page_limit"}:
            code = {
                "cycle": "SHADOW_PRICE_CURSOR_CYCLE",
                "page_limit": "SHADOW_PRICE_PAGE_LIMIT",
            }.get(scenario, "SHADOW_PRICE_ORIGINAL_INVALID")
            with pytest.raises(ValueError, match=code):
                await collect_shadow_prices(
                    factory,
                    request,
                    client=client,
                    store=store,
                    key_id=SecretStr("test"),
                    secret=SecretStr("test"),
                    now=lambda: now,
                )
            first_calls = len(calls)
            with pytest.raises(ValueError, match="SHADOW_PRICE_INCOMPLETE_REPLAY"):
                await collect_shadow_prices(
                    factory,
                    request,
                    client=client,
                    store=store,
                    key_id=SecretStr("test"),
                    secret=SecretStr("test"),
                    now=lambda: now,
                )
            assert len(calls) == first_calls
            async with factory() as session:
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(DataSnapshot)
                        .where(DataSnapshot.schema_version == "shadow_source_query_v2")
                    )
                    == 0
                )
                assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
                assert await session.scalar(
                    select(func.count())
                    .select_from(DataArtifact)
                    .where(DataArtifact.schema_version == "shadow_price_raw_v1")
                ) == (1 if scenario == "cycle" else len(calls))
                saved_receipts = list(
                    await session.scalars(
                        select(DataSnapshot)
                        .where(DataSnapshot.schema_version == "shadow_price_receipt_v1")
                        .order_by(DataSnapshot.id)
                    )
                )
                assert len(saved_receipts) == len(calls)
                for saved, actual_request in zip(saved_receipts, calls, strict=True):
                    assert saved.payload["shadow_group_id"] == str(request.shadow_group_id)
                    assert saved.payload["idempotency_key"] == request.idempotency_key
                    assert saved.payload["path"] == actual_request.url.path.strip("/").split("/")
                    assert dict(saved.payload["query"]) == dict(actual_request.url.params)
                    assert saved.payload["input_cursor"] == actual_request.url.params.get(
                        "page_token"
                    )
                    assert "complete" not in saved.payload and "output_cursor" not in saved.payload
            assert (
                len(calls)
                == {"missing_cursor": 2, "wrong_symbol": 1, "cycle": 2, "page_limit": 10}[scenario]
            )
            return
        if scenario == "cli":
            from nexus.ops import shadow_price_query as cli
            from nexus.shadow import price_collection

            class ReceiptClock(datetime):
                @classmethod
                def now(cls, tz=None):
                    return now

            original_client = httpx.AsyncClient

            def external_client(**kwargs):
                return original_client(**kwargs, transport=httpx.MockTransport(response))

            config = {
                "DATABASE_URL": migrated_database_url,
                "OBJECT_STORE_ENDPOINT": "http://localhost:9000",
                "OBJECT_STORE_REGION": "us-east-1",
                "OBJECT_STORE_BUCKET": "test",
                "OBJECT_STORE_ACCESS_KEY_ID": "test",
                "OBJECT_STORE_SECRET_ACCESS_KEY": "test",
                "ALPACA_API_KEY_ID": "test",
                "ALPACA_API_SECRET_KEY": "test",
            }
            (tmp_path / "infra").mkdir()
            (tmp_path / "infra" / ".env.local").write_text(
                "\n".join(f"{key}={value}" for key, value in config.items())
            )
            with monkeypatch.context() as boundary:
                boundary.setattr(price_collection, "datetime", ReceiptClock)
                boundary.setattr(cli.httpx, "AsyncClient", external_client)
                boundary.setattr(cli, "S3ProviderEvidenceStore", lambda *args, **kwargs: store)
                boundary.setattr(
                    sys,
                    "argv",
                    [
                        "shadow-price-query",
                        "--repo-root",
                        str(tmp_path),
                        "--shadow-group-id",
                        str(request.shadow_group_id),
                        "--session-date",
                        day.isoformat(),
                        "--idempotency-key",
                        request.idempotency_key,
                        "--feed",
                        "sip",
                        "--delivery-mode",
                        "delayed",
                    ],
                )
                await asyncio.to_thread(cli.main)
                result = json.loads(capsys.readouterr().out)
        else:
            result = await collect_shadow_prices(
                factory,
                request,
                client=client,
                store=store,
                key_id=SecretStr("test"),
                secret=SecretStr("test"),
                now=lambda: now,
            )
        count = len(calls)
        replayed = await collect_shadow_prices(
            factory,
            request,
            client=client,
            store=store,
            key_id=SecretStr("test"),
            secret=SecretStr("test"),
            now=lambda: now,
        )
        with pytest.raises(ValueError, match="SHADOW_PRICE_REPLAY_CONFLICT"):
            await collect_shadow_prices(
                factory,
                request.model_copy(update={"feed": "iex"}),
                client=client,
                store=store,
                key_id=SecretStr("test"),
                secret=SecretStr("test"),
                now=lambda: now,
            )
    assert result["query_snapshot_id"] == replayed["query_snapshot_id"]
    instrument_count = len(seeded.eligible.instrument_ids)
    assert len(calls) == count == instrument_count * (2 if scenario == "pages" else 1)
    assert result["price_return_set_complete"] is True
    assert result["execution_locked"] is True
    async with factory() as session:
        proof = await session.get(DataSnapshot, UUID(result["query_snapshot_id"]))
        assert proof.schema_version == "shadow_source_query_v2"
        assert len(proof.payload["negative_instrument_ids"]) == (instrument_count if empty else 0)
        assert len(proof.payload["member_ids"]) == (0 if empty else instrument_count)
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) >= count
        root = LockedShadowRoot.from_record(
            await session.get(ShadowGroupRecord, request.shadow_group_id)
        )
        authority = await load_shadow_session_input(
            session,
            root=root,
            session_date=day,
            ordinal=1,
            observed_at=now,
            required_instruments=seeded.eligible.instrument_ids,
            initial_execution_instruments=seeded.eligible.instrument_ids,
            query_artifact_reader=store,
        )
        assert authority.status == "complete"
        if scenario == "bars":
            from nexus.shadow.canonical import shadow_price_query_content_hash

            late = await load_shadow_session_input(
                session,
                root=root,
                session_date=day,
                ordinal=1,
                observed_at=now - timedelta(microseconds=1),
                required_instruments=seeded.eligible.instrument_ids,
                initial_execution_instruments=seeded.eligible.instrument_ids,
                query_artifact_reader=store,
            )
            assert late.status != "complete"
            for defect in ("member_ids", "manifest_id"):
                payload = deepcopy(proof.payload)
                page = payload["receipts"][0]["pages"][0]
                page[defect] = (
                    [str(UUID(int=999))] if defect == "member_ids" else str(UUID(int=999))
                )
                payload["query_hash"] = shadow_price_query_content_hash(payload)
                nested = await session.begin_nested()
                session.add(
                    DataSnapshot(
                        source="alpaca",
                        provider_object_id=proof.provider_object_id,
                        revision=payload["query_hash"],
                        schema_version="shadow_source_query_v2",
                        payload=payload,
                        observed_at=now,
                        available_at=now,
                        received_at=now,
                    )
                )
                await session.flush()
                try:
                    forged = await load_shadow_session_input(
                        session,
                        root=root,
                        session_date=day,
                        ordinal=1,
                        observed_at=now,
                        required_instruments=seeded.eligible.instrument_ids,
                        initial_execution_instruments=seeded.eligible.instrument_ids,
                        query_artifact_reader=store,
                    )
                    assert forged.status != "complete"
                finally:
                    await nested.rollback()
        unavailable = await load_shadow_session_input(
            session,
            root=root,
            session_date=day,
            ordinal=1,
            observed_at=now,
            required_instruments=seeded.eligible.instrument_ids,
            initial_execution_instruments=seeded.eligible.instrument_ids,
        )
        assert unavailable.status != "complete"
        raw_key = next(key for key in store.objects if key.startswith("raw/shadow-price/"))
        store.objects[raw_key] = b"tampered-original"
        invalid = await load_shadow_session_input(
            session,
            root=root,
            session_date=day,
            ordinal=1,
            observed_at=now,
            required_instruments=seeded.eligible.instrument_ids,
            initial_execution_instruments=seeded.eligible.instrument_ids,
            query_artifact_reader=store,
        )
        assert invalid.status != "complete"
        assert (
            await session.scalar(select(func.count()).select_from(ShadowSessionAuthorityRecord))
            == 0
        )
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        client = FixedOriginHttpClient(
            provider_name="alpaca",
            origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
            policy=HttpRequestPolicy(max_attempts=1),
            client=http,
        )
        with pytest.raises(ValueError, match="SHADOW_PRICE_PROOF_INVALID"):
            await collect_shadow_prices(
                factory,
                request,
                client=client,
                store=store,
                key_id=SecretStr("test"),
                secret=SecretStr("test"),
                now=lambda: now,
            )
        assert len(calls) == count
