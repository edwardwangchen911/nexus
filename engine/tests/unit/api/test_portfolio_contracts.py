from uuid import UUID

import pytest
from nexus.api.schemas.portfolio import (
    BrokerSyncCreateRequest,
    PortfolioSnapshotResponse,
    PositionResponse,
)
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
