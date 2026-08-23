from datetime import UTC, datetime

import pytest
from nexus.providers.broker.gateway_contracts import (
    GatewayClientError,
    GatewayResponsePage,
    is_exact_json_media_type,
    validate_account_id,
    validate_content_length,
    validate_pagination,
)
from pydantic import ValidationError

NOW = datetime(2026, 8, 23, 4, tzinfo=UTC)


def page(**updates: object) -> dict[str, object]:
    value: dict[str, object] = {
        "items": (),
        "source_request_id": "synthetic-request",
        "has_more": False,
        "next_cursor": None,
        "as_of": NOW,
        "quality": "PASS",
    }
    value.update(updates)
    return value


@pytest.mark.parametrize(
    "updates",
    (
        {"items": tuple({} for _ in range(101))},
        {"has_more": True, "next_cursor": None},
        {"has_more": True, "next_cursor": "-1"},
        {"as_of": datetime(2026, 8, 23, 4)},
        {"unexpected": True},
    ),
)
def test_gateway_page_rejects_unbounded_or_contradictory_contract(updates) -> None:
    with pytest.raises(ValidationError):
        GatewayResponsePage.model_validate(page(**updates))


@pytest.mark.parametrize("cursor,limit", ((None, True), (None, 0), ("-1", 100), ("1" * 13, 100)))
def test_gateway_pagination_is_fixed(cursor, limit) -> None:
    with pytest.raises(GatewayClientError, match="pagination"):
        validate_pagination(cursor, limit)


@pytest.mark.parametrize("account", ("", " padded ", "x" * 129))
def test_gateway_account_scope_is_bounded(account: str) -> None:
    with pytest.raises(GatewayClientError, match="account scope"):
        validate_account_id(account)


@pytest.mark.parametrize(
    "media_type",
    (
        None,
        "application/json-patch+json",
        "text/application/json",
        "application/json; profile=synthetic",
        "application/json; charset=latin-1",
    ),
)
def test_gateway_media_type_is_exact(media_type: str | None) -> None:
    assert is_exact_json_media_type(media_type) is False


def test_gateway_content_length_is_bounded() -> None:
    with pytest.raises(GatewayClientError, match="response rejected"):
        validate_content_length("1048577")
