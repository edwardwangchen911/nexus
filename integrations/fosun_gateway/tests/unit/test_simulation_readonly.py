"""Synthetic contract tests: no provider credentials or sockets."""

from copy import deepcopy
from datetime import date

import pytest
from nexus_fosun_gateway.adapter.simulation_readonly import SimulationReadOnlyTransport
from nexus_fosun_gateway.domain import GatewayError
from sdk_adapter_helpers import NoopRateLimiter
from sdk_adapter_helpers import client as sdk_client  # noqa: F401


@pytest.fixture(name="client")
def existing_client(request):
    return request.getfixturevalue("sdk_client")


def transport(client):
    return SimulationReadOnlyTransport(
        client, account_id="synthetic-account-001", rate_limiter=NoopRateLimiter()
    )


def prepare(client):
    client.account.list_accounts.response["data"]["subAccounts"][0]["subAccountType"] = 2
    for method in (client.portfolio.get_holdings, client.trade.list_orders):
        method.response["data"]["start"] = 0
        method.response["data"]["total"] = 1


@pytest.mark.asyncio
async def test_reads_only_verified_simulation_account_and_preserves_models(client):
    prepare(client)
    real = {"subAccountId": "synthetic-real", "subAccountType": 0, "status": 1}
    client.account.list_accounts.response["data"]["subAccounts"].append(real)
    result = await transport(client).collect(date(2026, 9, 1), date(2026, 9, 17))
    assert result["simulation_write_enabled"] is False
    assert result["execution_locked"] is True
    assert result["account_type"] == 2
    assert len(result["accounts"]["items"]) == 1
    assert "synthetic-real" not in str(result)
    assert result["cash"]["items"][0]["currency"] == "USD"
    assert result["holdings"][0]["items"][0]["quantity"] == "10"
    assert result["cash"]["quality"] == "PARTIAL"
    assert client.trade.list_orders.calls[0][1]["status_arr"] is None
    assert client.trade.list_orders.calls[0][1]["from_date"] == "2026-09-01"
    for method in (
        client.portfolio.get_assets_summary,
        client.portfolio.get_holdings,
        client.trade.list_orders,
        client.trade.get_cash_flows,
    ):
        assert method.calls[0][1]["sub_account_id"] == "synthetic-account-001"


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, 0, 1, 3, "2", True])
async def test_account_type_must_be_provider_integer_two(client, value):
    prepare(client)
    client.account.list_accounts.response["data"]["subAccounts"][0]["subAccountType"] = value
    with pytest.raises(GatewayError, match="SIMULATION_ACCOUNT_UNVERIFIED"):
        await transport(client).collect(date(2026, 9, 1), date(2026, 9, 17))
    assert not client.portfolio.get_assets_summary.calls
    assert not client.trade.list_orders.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["missing", "duplicate", "disabled", "unsuccessful"])
async def test_ambiguous_or_disabled_account_stops_before_asset_reads(client, case):
    prepare(client)
    raw = client.account.list_accounts.response
    rows = raw["data"]["subAccounts"]
    if case == "missing":
        rows.clear()
    elif case == "duplicate":
        rows.append(deepcopy(rows[0]))
    elif case == "disabled":
        rows[0]["status"] = 2
    else:
        raw["code"] = 40301
    with pytest.raises(GatewayError):
        await transport(client).collect(date(2026, 9, 1), date(2026, 9, 17))
    assert not client.portfolio.get_assets_summary.calls


@pytest.mark.asyncio
async def test_bounded_pagination_cannot_claim_complete_snapshot(client):
    prepare(client)
    client.portfolio.get_holdings.response["data"].update(total=2)
    with pytest.raises(GatewayError, match="SIMULATION_PAGE_LIMIT"):
        await transport(client).collect(date(2026, 9, 1), date(2026, 9, 17), max_pages=1)
    assert not client.trade.list_orders.calls


@pytest.mark.asyncio
async def test_multiple_pages_keep_source_metadata_and_advance_cursor(client):
    prepare(client)
    raw = deepcopy(client.portfolio.get_holdings.response)
    calls = []

    def holdings(**kwargs):
        calls.append(kwargs)
        page = deepcopy(raw)
        page["requestId"] = f"synthetic-holdings-{kwargs['start']}"
        page["data"].update(start=kwargs["start"], count=1, total=2)
        return page

    client.portfolio.get_holdings = holdings
    snapshot = await transport(client).collect(date(2026, 9, 1), date(2026, 9, 17))
    assert [call["start"] for call in calls] == [0, 1]
    assert [page["source_request_id"] for page in snapshot["holdings"]] == [
        "synthetic-holdings-0",
        "synthetic-holdings-1",
    ]


@pytest.mark.asyncio
async def test_provider_failure_is_not_retried_and_stops_following_reads(client):
    prepare(client)
    calls = []

    def unavailable(**kwargs):
        calls.append(kwargs)
        raise TimeoutError("synthetic-provider-error")

    client.portfolio.get_assets_summary = unavailable
    with pytest.raises(GatewayError):
        await transport(client).collect(date(2026, 9, 1), date(2026, 9, 17))
    assert len(calls) == 1
    assert not client.portfolio.get_holdings.calls


@pytest.mark.asyncio
async def test_cashflow_has_more_cannot_trigger_unsupported_pagination(client):
    prepare(client)
    client.trade.get_cash_flows.response["data"]["total"] = 2
    with pytest.raises(GatewayError, match="SIMULATION_PAGE_LIMIT"):
        await transport(client).collect(date(2026, 9, 1), date(2026, 9, 17))
    assert len(client.trade.get_cash_flows.calls) == 1


@pytest.mark.asyncio
async def test_invalid_window_does_not_call_provider(client):
    with pytest.raises(ValueError):
        await transport(client).collect(date(2026, 9, 17), date(2026, 9, 1))
    assert not client.account.list_accounts.calls


@pytest.mark.asyncio
async def test_transport_is_one_shot_even_after_failure(client):
    reader = transport(client)
    for _ in range(2):
        with pytest.raises(GatewayError):
            await reader.collect(date(2026, 9, 1), date(2026, 9, 17))
    assert len(client.account.list_accounts.calls) == 1


@pytest.mark.asyncio
async def test_frozen_sdk_factory_to_normalized_snapshot_without_network(client, monkeypatch):
    import os

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from fsopenapi.client import OpenAPIClient
    from nexus_fosun_gateway.simulation_readonly_cli import SimulationReadSettings, build_reader

    prepare(client)
    paths = {
        "/v1/account/Accounts": client.account.list_accounts.response,
        "/v1/portfolio/CashSummary": client.portfolio.get_assets_summary.response,
        "/v1/portfolio/Holdings": client.portfolio.get_holdings.response,
        "/v1/trade/OrderList": client.trade.list_orders.response,
        "/v1/trade/CashFlows": client.trade.get_cash_flows.response,
    }
    calls = []

    def fake_request(self, method, path, data=None, params=None):
        calls.append((method, path, data))
        assert method == "POST" and params is None
        assert (
            data == {}
            if path.endswith("Accounts")
            else (data["subAccountId"] == "synthetic-account-001")
        )
        return deepcopy(paths[path])

    monkeypatch.setattr(OpenAPIClient, "_request", fake_request)
    key = ec.generate_private_key(ec.SECP384R1())
    private = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    public = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    settings = SimulationReadSettings.model_validate(
        {
            "FOSUN_SIMULATION_API_KEY": "synthetic-only",
            "FOSUN_SIMULATION_CLIENT_PRIVATE_KEY": private,
            "FOSUN_SIMULATION_SERVER_PUBLIC_KEY": public,
            "FOSUN_SIMULATION_ACCOUNT_ID": "synthetic-account-001",
        }
    )
    previous = dict(os.environ)
    with build_reader(settings) as reader:
        assert dict(os.environ) == previous
        for operation in (
            reader._client.account.sim_account_create,
            lambda: reader._client.account.sim_account_reset("synthetic-account-001"),
        ):
            with pytest.raises(GatewayError, match="SIMULATION_OPERATION_REFUSED"):
                operation()
        assert calls == []
        reader._transport._broker_rate_limiter = NoopRateLimiter()
        result = await reader.collect(date(2026, 9, 1), date(2026, 9, 17))
    assert [path for _, path, _ in calls] == list(paths)
    assert calls[3][2]["fromDate"] == "2026-09-01"
    assert "statusArr" not in calls[3][2]
    assert result["execution_locked"] is True
