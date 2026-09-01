"""Authentication, transport bounds, replay, and request-ID behavior."""

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from nexus_fosun_gateway.activation import RuntimeAuthorization
from nexus_fosun_gateway.main import create_gateway_app
from nexus_fosun_gateway.settings import GatewaySettings
from read_only_helpers import RecordingTransport, _post_signed
from read_only_helpers import gateway_client as gateway_client
from read_only_transport_cases import (
    test_bad_signature_does_not_consume_nonce as test_bad_signature_does_not_consume_nonce,
)
from read_only_transport_cases import (
    test_current_and_previous_gateway_key_domains_authenticate as test_current_and_previous_gateway_key_domains_authenticate,  # noqa: E501
)
from read_only_transport_cases import (
    test_future_dated_signature_is_rejected_without_consuming_nonce as test_future_dated_signature_is_rejected_without_consuming_nonce,  # noqa: E501
)
from read_only_transport_cases import (
    test_gateway_hmac_v2_binds_actor_and_request_id as test_gateway_hmac_v2_binds_actor_and_request_id,  # noqa: E501
)
from read_only_transport_cases import (
    test_invalid_secret_configuration_cannot_initialize_hmac_authentication as test_invalid_secret_configuration_cannot_initialize_hmac_authentication,  # noqa: E501
)
from read_only_transport_cases import (
    test_read_only_routes_reject_non_json_before_authentication as test_read_only_routes_reject_non_json_before_authentication,  # noqa: E501
)
from read_only_transport_cases import (
    test_read_only_routes_stream_reject_oversized_body_before_json_parsing as test_read_only_routes_stream_reject_oversized_body_before_json_parsing,  # noqa: E501
)
from read_only_transport_cases import (
    test_replay_and_expired_signatures_fail_closed as test_replay_and_expired_signatures_fail_closed,  # noqa: E501
)
from read_only_transport_cases import (
    test_replay_cache_capacity_rejects_churn_without_evicting_live_nonce as test_replay_cache_capacity_rejects_churn_without_evicting_live_nonce,  # noqa: E501
)
from read_only_transport_cases import (
    test_replay_capacity_is_reserved_independently_per_hmac_key as test_replay_capacity_is_reserved_independently_per_hmac_key,  # noqa: E501
)
from read_only_transport_cases import (
    test_restart_drain_rejects_requests_until_old_signatures_are_stale as test_restart_drain_rejects_requests_until_old_signatures_are_stale,  # noqa: E501
)
from read_only_transport_cases import (
    test_unauthorized_operation_does_not_consume_principal_replay_quota as test_unauthorized_operation_does_not_consume_principal_replay_quota,  # noqa: E501
)
from read_only_transport_cases import (
    test_unverified_request_id_header_is_not_reflected as test_unverified_request_id_header_is_not_reflected,  # noqa: E501
)
from read_only_transport_cases import (
    test_valid_uuid7_is_not_reflected_on_absent_route as test_valid_uuid7_is_not_reflected_on_absent_route,  # noqa: E501
)
from read_only_transport_cases import (
    test_valid_uuid7_is_not_reflected_when_hmac_authentication_fails as test_valid_uuid7_is_not_reflected_when_hmac_authentication_fails,  # noqa: E501
)
from read_only_transport_cases import (
    test_verified_request_id_is_reflected_on_post_auth_validation_error as test_verified_request_id_is_reflected_on_post_auth_validation_error,  # noqa: E501
)


def test_core_runtime_allows_accounts_cash_holdings_quotes_and_bars() -> None:
    app, recording = _core_runtime_app()

    with TestClient(app) as client:
        responses = (
            _post_signed(client, "fosun_sync_accounts", {}),
            _post_signed(
                client,
                "fosun_sync_portfolio",
                {"account_id": "synthetic-account-001", "read_kind": "cash"},
                nonce="0198f7a0-2222-7777-8222-000000000902",
            ),
            _post_signed(
                client,
                "fosun_sync_portfolio",
                {
                    "account_id": "synthetic-account-001",
                    "read_kind": "holdings",
                    "cursor": None,
                    "limit": 100,
                },
                nonce="0198f7a0-2222-7777-8222-000000000903",
            ),
            _post_signed(
                client,
                "fosun_get_quotes",
                {"codes": ["usAAPL"]},
                nonce="0198f7a0-2222-7777-8222-000000000904",
            ),
            _post_signed(
                client,
                "fosun_get_bars",
                {"code": "usAAPL", "ktype": "day"},
                nonce="0198f7a0-2222-7777-8222-000000000905",
            ),
        )

    assert [response.status_code for response in responses] == [200] * 5
    assert recording.calls == 5


def test_core_runtime_denies_orders_and_cash_flows_with_zero_transport() -> None:
    app, recording = _core_runtime_app()

    with TestClient(app) as client:
        orders = _post_signed(
            client,
            "fosun_list_orders",
            {"account_id": "synthetic-account-001", "cursor": None, "limit": 100},
        )
        cash_flows = _post_signed(
            client,
            "fosun_list_cash_flows",
            {
                "account_id": "synthetic-account-001",
                "cursor": None,
                "limit": 100,
                "date_from": "2026-08-01",
                "date_to": "2026-08-29",
            },
            nonce="0198f7a0-2222-7777-8222-000000000906",
        )

    assert orders.status_code == 404
    assert cash_flows.status_code == 404
    assert orders.json()["error"]["code"] == "OPERATION_NOT_ALLOWED"
    assert cash_flows.json()["error"]["code"] == "OPERATION_NOT_ALLOWED"
    assert recording.calls == 0


def test_portfolio_schema_precedes_derived_runtime_authorization() -> None:
    app, recording = _core_runtime_app(
        authorized_operation_set=frozenset({"fosun_sync_portfolio:cash"})
    )

    with TestClient(app) as client:
        invalid = _post_signed(
            client,
            "fosun_sync_portfolio",
            {"account_id": "synthetic-account-001", "read_kind": "orders"},
        )
        holdings = _post_signed(
            client,
            "fosun_sync_portfolio",
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": None,
                "limit": 100,
            },
            nonce="0198f7a0-2222-7777-8222-000000000907",
        )
        cash = _post_signed(
            client,
            "fosun_sync_portfolio",
            {"account_id": "synthetic-account-001", "read_kind": "cash"},
            nonce="0198f7a0-2222-7777-8222-000000000908",
        )

    assert invalid.status_code == 422
    assert holdings.status_code == 404
    assert cash.status_code == 200
    assert recording.calls == 1


def test_core_capabilities_are_local_and_scope_exact() -> None:
    app, recording = _core_runtime_app()

    with TestClient(app) as client:
        response = _post_signed(client, "fosun_capabilities", {})

    assert response.status_code == 200
    assert response.json()["items"] == [
        {
            "kind": "capability",
            "provider": "fosun",
            "mode": "read_only",
            "operations": [
                "fosun_get_bars",
                "fosun_get_quotes",
                "fosun_sync_accounts",
                "fosun_sync_portfolio:cash",
                "fosun_sync_portfolio:holdings",
            ],
        }
    ]
    assert recording.calls == 0


def _core_runtime_app(
    *,
    authorized_operation_set: frozenset[str] | None = None,
):
    app = create_gateway_app(GatewaySettings.for_test())
    recording = RecordingTransport()
    app.state.transport = recording
    app.state.settings = GatewaySettings.for_test().model_copy(
        update={"transport_mode": "read_only"}
    )
    app.state.runtime_authorization = RuntimeAuthorization(
        scope="core",
        scope_contract_version="nexus.fosun.read-only-scope.v1.core",
        run_id="readonly-core-run-0001",
        capability_expires_at=datetime.now(UTC) + timedelta(hours=1),
        authorized_operation_set=frozenset(
            {
                "fosun_sync_accounts",
                "fosun_sync_portfolio:cash",
                "fosun_sync_portfolio:holdings",
                "fosun_get_quotes",
                "fosun_get_bars",
            }
            if authorized_operation_set is None
            else authorized_operation_set
        ),
    )
    return app, recording
