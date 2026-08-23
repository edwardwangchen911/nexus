from uuid import UUID

import pytest
from nexus.api.schemas.portfolio import (
    BrokerSyncCreateRequest,
    PortfolioListItemResponse,
    PortfolioPageResponse,
    PortfolioSnapshotEnvelope,
    PortfolioSnapshotResponse,
    PositionResponse,
)
from nexus.main import create_engine_app
from nexus.settings import EngineSettings
from pydantic import ValidationError

_PORTFOLIO_ID = UUID("0198d500-0000-7000-8000-000000000001")
_INSTRUMENT_ID = UUID("0198d500-0000-7000-8000-000000000002")


def test_broker_sync_request_accepts_exactly_one_internal_portfolio_id() -> None:
    request = BrokerSyncCreateRequest.model_validate({"portfolio_id": str(_PORTFOLIO_ID)})
    assert request.portfolio_id == _PORTFOLIO_ID

    with pytest.raises(ValidationError):
        BrokerSyncCreateRequest.model_validate(
            {"portfolio_id": str(_PORTFOLIO_ID), "account_id": "external-canary"}
        )


def test_position_contract_preserves_both_costs_as_decimal_strings() -> None:
    position = PositionResponse.model_validate(
        {
            "instrument_id": str(_INSTRUMENT_ID),
            "symbol": "00656.HK",
            "exchange": "XHKG",
            "currency": "HKD",
            "quantity": "90000",
            "available_quantity": "90000",
            "frozen_quantity": "0",
            "avg_cost": "5.25",
            "diluted_cost": "4.95",
            "current_price": "6.125",
            "provider_as_of": None,
            "read_only": True,
        }
    )

    assert position.avg_cost == "5.25"
    assert position.diluted_cost == "4.95"
    assert position.read_only is True


def test_snapshot_contract_forbids_an_unadmitted_converted_view() -> None:
    payload = {
        "portfolio_snapshot_id": "0198d500-0000-7000-8000-000000000003",
        "portfolio_id": str(_PORTFOLIO_ID),
        "coverage_scope": "broker_account",
        "status": "PARTIAL",
        "cash": (),
        "positions": (),
        "open_orders": (),
        "converted_view": {"currency": "USD", "value": "1"},
        "as_of": None,
    }

    with pytest.raises(ValidationError):
        PortfolioSnapshotResponse.model_validate(payload)


def test_portfolio_contracts_publish_fixed_collection_bounds() -> None:
    page_schema = PortfolioPageResponse.model_json_schema()
    snapshot_schema = PortfolioSnapshotResponse.model_json_schema()

    assert page_schema["properties"]["items"]["maxItems"] == 50
    assert snapshot_schema["properties"]["cash"]["maxItems"] == 32
    assert snapshot_schema["properties"]["positions"]["maxItems"] == 2000
    assert snapshot_schema["properties"]["open_orders"]["maxItems"] == 2000


def test_safety_discriminators_and_nullable_symbols_are_required() -> None:
    list_required = set(PortfolioListItemResponse.model_json_schema()["required"])
    snapshot_schema = PortfolioSnapshotResponse.model_json_schema()
    snapshot_required = set(snapshot_schema["required"])
    position_schema = PositionResponse.model_json_schema()

    assert {"kind", "coverage_scope"} <= list_required
    assert {"coverage_scope", "converted_view"} <= snapshot_required
    assert {"symbol", "read_only"} <= set(position_schema["required"])
    assert position_schema["properties"]["quantity"]["maxLength"] == 40


def test_maximum_legal_snapshot_envelope_fits_the_bff_four_mebibyte_limit() -> None:
    decimal = "-12345678901234567890123456.123456789012"
    common = {
        "instrument_id": str(_INSTRUMENT_ID),
        "symbol": "X" * 32,
        "currency": "USD",
    }
    envelope = PortfolioSnapshotEnvelope.model_validate(
        {
            "data": {
                "portfolio_snapshot_id": "0198d500-0000-7000-8000-000000000003",
                "portfolio_id": str(_PORTFOLIO_ID),
                "coverage_scope": "broker_account",
                "status": "READY",
                "cash": (),
                "positions": (
                    {
                        **common,
                        "exchange": "X" * 20,
                        "quantity": decimal,
                        "available_quantity": decimal,
                        "frozen_quantity": decimal,
                        "avg_cost": decimal,
                        "diluted_cost": decimal,
                        "current_price": decimal,
                        "provider_as_of": "2026-08-23T08:00:00.123456Z",
                        "read_only": True,
                    },
                )
                * 2000,
                "open_orders": (
                    {
                        **common,
                        "broker_order_snapshot_id": "0198d500-0000-7000-8000-000000000004",
                        "side": "X" * 10,
                        "quantity": decimal,
                        "filled_quantity": decimal,
                        "price": decimal,
                        "status": "X" * 30,
                        "updated_at": "2026-08-23T08:00:00.123456Z",
                        "read_only": True,
                    },
                )
                * 2000,
                "converted_view": None,
                "as_of": "2026-08-23T08:00:00.123456Z",
            },
            "meta": {
                "request_id": "0198f7a0-1111-7777-8111-111111111114",
                "schema_version": "1",
                "as_of": "2026-08-23T08:00:00.123456Z",
            },
        }
    )

    assert len(envelope.model_dump_json().encode()) <= 4 * 1024 * 1024


def test_sync_admission_openapi_publishes_bff_location_header() -> None:
    document = create_engine_app(EngineSettings.for_test()).openapi()

    response = document["paths"]["/internal/v1/broker-sync-runs"]["post"]["responses"]["202"]
    assert response["headers"]["Location"]["schema"] == {"type": "string"}
