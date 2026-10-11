import hashlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

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
from nexus.providers.broker.gateway_contracts import GatewayResponsePage
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from nexus.settings import EngineSettings
from nexus_fosun_gateway.adapter.normalization import normalize_holdings_response
from pydantic import SecretStr
from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from unit.market.test_object_store import FakeS3Client


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", [False, True])
@pytest.mark.parametrize("normal_entry", [False, True])
async def test_discovery_raw_readback_replay_and_no_identity_or_account_facts(
    migrated_database_url, monkeypatch, tmp_path, corrupt, normal_entry
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
    if normal_entry:
        from nexus.ops import fosun_identity_discovery_local as module

        directory = tmp_path / ".secrets/nexus-fosun-local/identity-gaps"
        for path in (tmp_path / ".secrets", directory.parent, directory):
            path.mkdir(mode=0o700)
        gap_file = directory / f"{new_uuid7()}.json"
        gap_file.write_text(
            json.dumps(
                {
                    "schema": "fosun_broker_identity_gap_diagnostic_v1",
                    "identity_authority": False,
                    "observed_at": now.isoformat(),
                    "source_page_sha256": "a" * 64,
                    "unresolved": [{"provider_code": code, "currency": "USD"} for code in codes],
                }
            )
        )
        gap_file.chmod(0o600)
        requests = []

        class Broker:
            async def read_accounts(self):
                requests.append("accounts")
                return GatewayResponsePage(
                    items=(
                        {
                            "kind": "account",
                            "external_account_id": "synthetic",
                            "base_currency": None,
                            "status": "active",
                        },
                    ),
                    source_request_id="synthetic",
                    has_more=False,
                    next_cursor=None,
                    as_of=now,
                    quality="PASS",
                )

            async def read_holdings(self, account, *, cursor, limit):
                assert account == "synthetic" and cursor is None and limit == 100
                requests.append("holdings")
                page = normalize_holdings_response(
                    {
                        "code": 0,
                        "requestId": "synthetic",
                        "data": {
                            "start": 0,
                            "count": 2,
                            "total": 2,
                            "list": [
                                {
                                    "marketCode": "us",
                                    "stockCode": code[2:],
                                    "currency": "USD",
                                    "quantity": "1",
                                    "quantityAvail": "1",
                                    "qtyOnHold": "0",
                                }
                                for code in codes
                            ],
                        },
                    },
                    account_id=account,
                    received_at=now,
                    context={"cursor": cursor, "limit": limit},
                )
                return GatewayResponsePage.model_validate(page.model_dump(mode="json"))

            async def aclose(self):
                pass

        async def read_quotes(market, *, codes):
            assert codes == ("usAAA", "usBBB")
            requests.append("quotes")
            return raw

        monkeypatch.setattr(module, "_broker_settings", lambda _: object())
        monkeypatch.setattr(module, "_market_identity", lambda _: object())
        monkeypatch.setattr(module, "FosunGatewayClient", lambda _: Broker())
        monkeypatch.setattr(module, "FosunMarketGatewayClient", lambda _: Broker())
        monkeypatch.setattr(module, "_read_quote_snapshot", read_quotes)
        monkeypatch.setattr(module, "S3ProviderEvidenceStore", lambda _: store)

    async def run():
        if normal_entry:
            result = await module.discover_identity_gaps(
                SimpleNamespace(database_url=migrated_database_url),
                repo_root=tmp_path,
                gap_file=gap_file,
                item_indices=(0, 1),
            )
            assert result["provider_requests"]["total"] == 3
            assert (
                result["canonical_identities_created"] == result["account_snapshots_created"] == 0
            )
            return result["identity_route_counts"]
        async with factory() as session, session.begin():
            return await _persist_discovery_batch(
                session,
                store,
                codes=codes,
                holdings=holdings,
                holdings_observed_at=now,
                quote_raw=raw,
            )

    if corrupt:

        async def broken_read(*args, **kwargs):
            return b"corrupt synthetic readback"

        monkeypatch.setattr(store, "read_raw", broken_read)
    try:
        if corrupt:
            with pytest.raises(ValueError, match="^LOCAL_IDENTITY_DISCOVERY_RAW_READBACK_FAILED$"):
                await run()
        else:
            for _ in range(2):
                counts = await run()
                assert counts == {
                    "current_held_common_stock": 2,
                    "current_held_fund_or_structured": 0,
                    "manual_review": 0,
                }
        if normal_entry:
            assert requests == ["accounts", "holdings", "quotes"] * (1 if corrupt else 2)
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
