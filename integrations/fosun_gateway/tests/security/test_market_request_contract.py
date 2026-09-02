"""Market routes accept only canonical bounded Fosun request shapes."""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from nexus_fosun_gateway.main import create_gateway_app
from nexus_fosun_gateway.settings import GatewaySettings
from read_only_helpers import _post_signed


@pytest.fixture(name="gateway_client")
def _gateway_client() -> Iterator[TestClient]:
    with TestClient(create_gateway_app(GatewaySettings.for_test())) as client:
        yield client


@pytest.mark.parametrize(
    ("codes", "nonce"),
    [
        (["AAPL"], "0198f7a0-2222-7777-8222-222222222301"),
        (["usaapl"], "0198f7a0-2222-7777-8222-222222222302"),
        (["usAAPL", "usAAPL"], "0198f7a0-2222-7777-8222-222222222303"),
        (["usMSFT", "usAAPL"], "0198f7a0-2222-7777-8222-222222222304"),
    ],
)
def test_quotes_require_unique_canonical_sorted_us_codes(
    gateway_client: TestClient,
    codes: list[str],
    nonce: str,
) -> None:
    response = _post_signed(
        gateway_client,
        "fosun_get_quotes",
        {"codes": codes},
        nonce=nonce,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.parametrize(
    ("payload", "nonce"),
    [
        (
            {"code": "AAPL", "ktype": "day"},
            "0198f7a0-2222-7777-8222-222222222311",
        ),
        (
            {
                "code": "usAAPL",
                "ktype": "day",
                "start_time": "2026-08-01T00:00:00Z",
            },
            "0198f7a0-2222-7777-8222-222222222312",
        ),
        (
            {
                "code": "usAAPL",
                "ktype": "day",
                "start_time": "2026-08-22T00:00:00Z",
                "end_time": "2026-08-01T00:00:00Z",
            },
            "0198f7a0-2222-7777-8222-222222222313",
        ),
        (
            {"code": "usAAPL", "ktype": "minute", "limit": True},
            "0198f7a0-2222-7777-8222-222222222314",
        ),
    ],
)
def test_bars_require_canonical_code_paired_window_and_strict_limit(
    gateway_client: TestClient,
    payload: dict[str, object],
    nonce: str,
) -> None:
    response = _post_signed(
        gateway_client,
        "fosun_get_bars",
        payload,
        nonce=nonce,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.parametrize("right", ("noward", "forward", "backward"))
def test_bars_do_not_allow_clients_to_override_adjustment(
    gateway_client: TestClient, right: str
) -> None:
    response = _post_signed(
        gateway_client,
        "fosun_get_bars",
        {"code": "usAAPL", "ktype": "day", "right": right},
        nonce="0198f7a0-2222-7777-8222-222222222315",
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_bar_route_preserves_adapter_request_fact_and_legacy_unknown(gateway_client, monkeypatch):
    from nexus_fosun_gateway.domain import GatewayBarPage

    original = gateway_client.app.state.transport.execute

    async def with_request_fact(operation, payload):
        page = await original(operation, payload)
        return GatewayBarPage.model_validate(
            {
                **page.model_dump(),
                "sdk_requested_adjustment": "noward",
                "provider_reported_delay": False,
            }
        )

    legacy = _post_signed(
        gateway_client,
        "fosun_get_bars",
        {"code": "usAAPL", "ktype": "day"},
        nonce="0198f7a0-2222-7777-8222-222222222316",
    )
    assert legacy.status_code == 200
    assert legacy.json()["sdk_requested_adjustment"] is None
    assert legacy.json()["provider_reported_delay"] is None
    monkeypatch.setattr(gateway_client.app.state.transport, "execute", with_request_fact)
    current = _post_signed(
        gateway_client,
        "fosun_get_bars",
        {"code": "usAAPL", "ktype": "day"},
        nonce="0198f7a0-2222-7777-8222-222222222317",
    )
    assert current.status_code == 200
    assert current.json()["sdk_requested_adjustment"] == "noward"
    assert current.json()["provider_reported_delay"] is False
