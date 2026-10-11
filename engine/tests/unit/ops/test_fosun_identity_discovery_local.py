import importlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.providers.broker.fosun_contracts import PositionItem
from nexus.providers.broker.gateway_contracts import GatewayResponsePage
from nexus_fosun_gateway.adapter.normalization import normalize_holdings_response

NOW = datetime(2026, 10, 11, 12, tzinfo=UTC)


def _gap(root: Path):
    directory = root / ".secrets/nexus-fosun-local/identity-gaps"
    for path in (root / ".secrets", directory.parent, directory):
        path.mkdir(mode=0o700)
    path = directory / f"{UUID(int=1)}.json"
    path.write_text(
        json.dumps(
            {
                "schema": "fosun_broker_identity_gap_diagnostic_v1",
                "identity_authority": False,
                "observed_at": NOW.isoformat(),
                "source_page_sha256": "a" * 64,
                "unresolved": [
                    {"provider_code": f"us{code}", "currency": "USD"}
                    for code in ("AAA", "BBB", "CCC", "DDD")
                ],
            }
        )
    )
    path.chmod(0o600)
    return path


def test_gap_selection_is_bounded_and_uses_private_archive(tmp_path):
    module = importlib.import_module("nexus.ops.fosun_identity_discovery_local")
    path = _gap(tmp_path)
    assert module.read_gap_selection(tmp_path, path, (1, 0)) == ("usAAA", "usBBB")


@pytest.mark.parametrize(
    ("indices", "code"),
    [
        ((), "LOCAL_IDENTITY_DISCOVERY_SELECTION_INVALID"),
        ((0, 0), "LOCAL_IDENTITY_DISCOVERY_SELECTION_INVALID"),
        ((-1,), "LOCAL_IDENTITY_DISCOVERY_SELECTION_INVALID"),
        ((4,), "LOCAL_IDENTITY_DISCOVERY_SELECTION_INVALID"),
        ((0, 1, 2, 3), "LOCAL_IDENTITY_DISCOVERY_BATCH_LIMIT"),
    ],
)
def test_gap_selection_rejects_invalid_indices(tmp_path, indices, code):
    module = importlib.import_module("nexus.ops.fosun_identity_discovery_local")
    path = _gap(tmp_path)
    with pytest.raises(ValueError, match=f"^{code}$"):
        module.read_gap_selection(tmp_path, path, indices)


@pytest.mark.parametrize(
    "unsafe", ["mode", "symlink", "authority", "duplicate", "unknown_field", "duplicate_json_key"]
)
def test_gap_reader_rejects_unsafe_or_forged_diagnostic(tmp_path, unsafe):
    module = importlib.import_module("nexus.ops.fosun_identity_discovery_local")
    path = _gap(tmp_path)
    data = json.loads(path.read_text())
    if unsafe == "mode":
        path.chmod(0o644)
    elif unsafe == "symlink":
        actual = path.with_suffix(".raw")
        path.rename(actual)
        path.symlink_to(actual)
    elif unsafe == "authority":
        data["identity_authority"] = True
        path.write_text(json.dumps(data))
    elif unsafe == "duplicate":
        data["unresolved"].append(data["unresolved"][0])
        path.write_text(json.dumps(data))
    elif unsafe == "unknown_field":
        data["account"] = "synthetic forbidden field"
        path.write_text(json.dumps(data))
    else:
        path.write_text(
            path.read_text().replace(
                '"identity_authority": false',
                '"identity_authority": false, "identity_authority": true',
            )
        )
    with pytest.raises(ValueError, match="^LOCAL_IDENTITY_DISCOVERY_GAP_INVALID$"):
        module.read_gap_selection(tmp_path, path, (0,))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reject",
    [None, "not_held", "already_resolved", "currency", "pagination", "quality", "schema"],
)
async def test_discovery_checks_live_holdings_before_one_bounded_quote(
    tmp_path, monkeypatch, reject
):
    module = importlib.import_module("nexus.ops.fosun_identity_discovery_local")
    path = _gap(tmp_path)
    calls = []
    codes = ("AAA", "BBB", "CCC", "DDD") + tuple(f"EXTRA{i}" for i in range(22))
    positions = tuple(
        PositionItem(
            kind="position",
            external_account_id="synthetic-account",
            instrument_code=f"us{code}",
            currency="HKD" if reject == "currency" and code == "AAA" else "USD",
            quantity=Decimal("1"),
            available_quantity=Decimal("1"),
            frozen_quantity=Decimal("0"),
            average_cost=Decimal("1"),
            diluted_cost=None,
            current_price=None,
            provider_as_of=NOW,
        )
        for code in codes
        if not (reject == "not_held" and code == "AAA")
    )

    class Broker:
        async def read_accounts(self):
            calls.append("accounts")
            return GatewayResponsePage(
                items=(
                    {
                        "kind": "account",
                        "external_account_id": "synthetic-account",
                        "base_currency": "USD",
                        "status": "active",
                    },
                ),
                source_request_id="synthetic",
                has_more=False,
                next_cursor=None,
                as_of=NOW,
                quality="PASS",
            )

        async def read_holdings(self, account, *, cursor, limit):
            assert account == "synthetic-account" and cursor is None and limit == 100
            calls.append("holdings")
            # Exercise the producer's actual contract, including its PARTIAL quality
            # and unknown provider clock, instead of inventing a PASS holdings page.
            normalized = normalize_holdings_response(
                {
                    "code": 0,
                    "requestId": "synthetic",
                    "data": {
                        "start": 0,
                        "count": len(positions),
                        "total": len(positions) + (1 if reject == "pagination" else 0),
                        "list": [
                            {
                                "marketCode": "us",
                                "stockCode": p.instrument_code[2:],
                                "currency": p.currency,
                                "quantity": str(p.quantity),
                                "quantityAvail": str(p.available_quantity),
                                "qtyOnHold": str(p.frozen_quantity),
                                "avgCost": "1",
                            }
                            for p in positions
                        ],
                    },
                },
                account_id=account,
                received_at=NOW,
                context={"cursor": None, "limit": limit},
            )
            page = GatewayResponsePage.model_validate(normalized.model_dump(mode="json"))
            if reject == "quality":
                page = page.model_copy(update={"quality": "PASS"})
            elif reject == "schema":
                items = list(page.items)
                items[0] = {**items[0], "quantity": "financial-fact-canary"}
                page = page.model_copy(update={"items": tuple(items)})
            return page

        async def aclose(self):
            calls.append("broker_closed")

    class Context:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def begin(self):
            return self

        async def execute(self, query):
            pass

    class Engine:
        async def dispose(self):
            calls.append("engine_closed")

    class Market:
        async def aclose(self):
            calls.append("market_closed")

    async def identity(*args, **kwargs):
        calls.append("identity")
        return SimpleNamespace(
            unresolved_codes=("usBBB", "usCCC", "usDDD")
            if reject == "already_resolved"
            else ("usAAA", "usBBB", "usCCC", "usDDD")
        )

    async def quotes(market, *, codes):
        calls.append(("quotes", codes))
        return object()

    async def persist(session, store, *, codes, holdings, holdings_observed_at, quote_raw):
        calls.append(("persist", codes))
        assert len(holdings) == 26 and holdings_observed_at == NOW
        return {
            "current_held_common_stock": 2,
            "current_held_fund_or_structured": 0,
            "manual_review": 0,
        }

    monkeypatch.setattr(module, "_broker_settings", lambda _: object())
    monkeypatch.setattr(module, "_market_identity", lambda _: object())
    monkeypatch.setattr(module, "FosunGatewayClient", lambda _: Broker())
    monkeypatch.setattr(module, "FosunMarketGatewayClient", lambda _: Market())
    monkeypatch.setattr(module, "create_async_engine", lambda *args, **kwargs: Engine())
    monkeypatch.setattr(module, "async_sessionmaker", lambda *args, **kwargs: Context)
    monkeypatch.setattr(module, "S3ProviderEvidenceStore", lambda _: object())
    monkeypatch.setattr(module, "_load_local_broker_identity", identity)
    monkeypatch.setattr(module, "_read_quote_snapshot", quotes)
    monkeypatch.setattr(module, "_persist_discovery_batch", persist)
    settings = SimpleNamespace(database_url="synthetic")
    if reject:
        with pytest.raises(ValueError, match="^LOCAL_IDENTITY_DISCOVERY_") as failure:
            await module.discover_identity_gaps(
                settings, repo_root=tmp_path, gap_file=path, item_indices=(0, 1)
            )
        assert not any(isinstance(c, tuple) for c in calls)
        assert "canary" not in str(failure.value)
        assert calls.count("accounts") == calls.count("holdings") == 1
    else:
        result = await module.discover_identity_gaps(
            settings, repo_root=tmp_path, gap_file=path, item_indices=(0, 1)
        )
        assert result["discovered"] == 2 and result["remaining_identity_gaps"] == 4
        assert result["canonical_identities_created"] == result["account_snapshots_created"] == 0
        assert result["provider_requests"] == {
            "accounts": 1,
            "holdings": 1,
            "quotes": 1,
            "total": 3,
        }
        assert result["execution_locked"] is True
        assert calls.count(("quotes", ("usAAA", "usBBB"))) == 1
        assert calls.count(("persist", ("usAAA", "usBBB"))) == 1
