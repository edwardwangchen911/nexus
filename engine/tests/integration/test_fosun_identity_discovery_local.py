import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest
from nexus.domain.ids import new_uuid7
from nexus.market.fosun_instrument_discovery_storage import read_pending_instrument_discoveries
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_identity_discovery_local import _persist_discovery_batch
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias, LatestQuote
from nexus.persistence.models.portfolio import BrokerSyncRun, PortfolioSnapshotRecord
from nexus.persistence.session import session_factory
from nexus.providers.broker.fosun_contracts import PositionItem
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from unit.market.test_object_store import FakeS3Client


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", [False, True])
async def test_discovery_raw_readback_replay_and_no_identity_or_account_facts(
    migrated_database_url, monkeypatch, corrupt
):
    url = make_url(migrated_database_url)
    assert url.database.startswith("nexus_test_") and url.port == 55448
    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_region": "us-east-1",
            "object_store_bucket": "synthetic",
            "object_store_prefix": "discovery",
            "object_store_access_key_id": SecretStr("synthetic"),
            "object_store_secret_access_key": SecretStr("synthetic"),
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    now = datetime.now(UTC) - timedelta(minutes=1)
    codes = ("usAAA", "usBBB")
    body = json.dumps(
        {
            "items": [
                {
                    "kind": "quote",
                    "instrument_code": code,
                    "price": "1",
                    "as_of": now.isoformat(),
                    "provider_classification": {
                        "level_1": 1,
                        "level_2": 101,
                        "level_3": 10105,
                        "equity_profile_fields_complete": True,
                        "net_asset_profile_fields_complete": False,
                    },
                }
                for code in codes
            ],
            "source_request_id": "synthetic",
            "has_more": False,
            "next_cursor": None,
            "as_of": now.isoformat(),
            "quality": "PARTIAL",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    raw = RawGatewayResponse(
        operation="fosun_get_quotes",
        gateway_request_id=new_uuid7(),
        request_body_sha256=hashlib.sha256(body).hexdigest(),
        raw_payload=body,
        sent_at=now,
        received_at=now,
    )
    holdings = tuple(
        PositionItem(
            kind="position",
            external_account_id="synthetic",
            instrument_code=code,
            currency="USD",
            quantity="1",
            available_quantity="1",
            frozen_quantity="0",
            average_cost=None,
            diluted_cost=None,
            current_price=None,
            provider_as_of=now,
        )
        for code in codes
    )
    if corrupt:

        async def broken_read(*args, **kwargs):
            return b"corrupt synthetic readback"

        monkeypatch.setattr(store, "read_raw", broken_read)
    try:
        if corrupt:
            with pytest.raises(ValueError, match="^LOCAL_IDENTITY_DISCOVERY_RAW_READBACK_FAILED$"):
                async with factory() as session, session.begin():
                    await _persist_discovery_batch(
                        session,
                        store,
                        codes=codes,
                        holdings=holdings,
                        holdings_observed_at=now,
                        quote_raw=raw,
                    )
        else:
            for _ in range(2):
                async with factory() as session, session.begin():
                    counts = await _persist_discovery_batch(
                        session,
                        store,
                        codes=codes,
                        holdings=holdings,
                        holdings_observed_at=now,
                        quote_raw=raw,
                    )
                    assert counts == {
                        "current_held_common_stock": 2,
                        "current_held_fund_or_structured": 0,
                        "manual_review": 0,
                    }
        async with factory() as session:
            pending = await read_pending_instrument_discoveries(session, as_of=datetime.now(UTC))
            assert len(pending) == (0 if corrupt else 2)
            assert all(
                doc.identity_authoritative is False
                and doc.decision_eligible is False
                and doc.execution_locked is True
                for doc in pending.values()
            )
            for model in (
                Instrument,
                InstrumentAlias,
                LatestQuote,
                BrokerSyncRun,
                PortfolioSnapshotRecord,
            ):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == (
                0 if corrupt else 2
            )
            assert await session.scalar(select(func.count()).select_from(DataArtifact)) == (
                0 if corrupt else 3
            )
    finally:
        await factory.kw["bind"].dispose()
