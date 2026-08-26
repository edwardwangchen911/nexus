from __future__ import annotations

from uuid import UUID

import pytest
from nexus.api.schemas.paper import (
    PaperCancelRequest,
    PaperOrderPageResponse,
    PaperPortfolioResponse,
)
from nexus.main import create_engine_app
from nexus.settings import EngineSettings
from pydantic import ValidationError

PORTFOLIO_ID = UUID("67000000-0000-0000-0000-000000000001")


def test_paper_cancel_request_is_strict_and_versioned() -> None:
    request = PaperCancelRequest.model_validate({"expected_event_seq": 4})
    assert request.expected_event_seq == 4

    with pytest.raises(ValidationError):
        PaperCancelRequest.model_validate(
            {"expected_event_seq": 4, "order_id": "broker-order-canary"}
        )
    with pytest.raises(ValidationError):
        PaperCancelRequest.model_validate({"expected_event_seq": 0})


def test_paper_read_contracts_are_bounded_and_do_not_expose_account_or_trade_creation() -> None:
    portfolio_schema = PaperPortfolioResponse.model_json_schema()
    page_schema = PaperOrderPageResponse.model_json_schema()

    assert portfolio_schema["properties"]["positions"]["maxItems"] == 500
    assert portfolio_schema["properties"]["attributions"]["maxItems"] == 500
    performance = portfolio_schema["$defs"]["PaperPerformanceResponse"]
    assert {"cash", "available_cash", "reserved_cash"} <= set(
        performance["required"]
    )
    assert page_schema["properties"]["items"]["maxItems"] == 50
    wire = str({"portfolio": portfolio_schema, "orders": page_schema}).lower()
    assert "account_id" not in wire
    assert "broker_order" not in wire
    assert "create_order" not in wire


def test_paper_openapi_exposes_reads_and_cancel_but_no_fill_or_order_creation() -> None:
    document = create_engine_app(EngineSettings.for_test()).openapi()
    paths = document["paths"]

    assert set(paths["/internal/v1/paper/portfolio"]) == {"get"}
    assert set(paths["/internal/v1/paper/orders"]) == {"get"}
    assert set(paths["/internal/v1/paper/orders/{order_id}/cancel"]) == {"post"}
    assert not any(
        forbidden in path
        for path in paths
        for forbidden in ("/fill", "/advance", "/execute", "/paper/orders/create")
    )
    cancel = paths["/internal/v1/paper/orders/{order_id}/cancel"]["post"]
    headers = {
        parameter["name"]: parameter
        for parameter in cancel["parameters"]
        if parameter["in"] == "header"
    }
    assert headers["Idempotency-Key"]["required"] is True
