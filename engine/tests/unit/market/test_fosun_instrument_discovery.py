from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.market.fosun_instrument_discovery import (
    FosunInstrumentDiscoveryDocument,
    recommended_identity_route,
)
from nexus.research.fosun_security_classification import (
    FosunClassificationLevels,
    FosunFundAssetType,
    FosunRawQuoteBinding,
)
from pydantic import ValidationError

NOW = datetime(2025, 9, 12, tzinfo=UTC)


def _document(**changes: object) -> FosunInstrumentDiscoveryDocument:
    values: dict[str, object] = {
        "provider_code": "usNEW",
        "provider_name": "New Company",
        "market_scope": "US",
        "currency": "USD",
        "provider_product_type": 6,
        "levels": FosunClassificationLevels(first=1, second=101, third=10105),
        "equity_profile_fields_complete": True,
        "net_asset_profile_fields_complete": False,
        "fund_asset_type": FosunFundAssetType(chs="", cht="", eng=""),
        "routing_family": "company_equity_like",
        "recommended_identity_route": "current_held_common_stock",
        "holding_observed_at": NOW - timedelta(seconds=1),
        "received_at": NOW,
        "available_at": NOW,
        "raw_quote": FosunRawQuoteBinding(
            artifact_sha256="a" * 64,
            artifact_byte_length=123,
            artifact_uri=f"s3://bucket/raw/fosun/sha256/{'a' * 64}.json",
            gateway_request_id=UUID("00000000-0000-7000-8000-000000000002"),
            request_body_sha256="b" * 64,
        ),
    }
    values.update(changes)
    return FosunInstrumentDiscoveryDocument.model_validate(values)


def test_document_is_locked_provider_scoped_discovery() -> None:
    document = _document()

    assert document.provider_object_id == "discovery:usNEW"
    assert document.canonical_instrument_id is None
    assert document.canonical_asset_type is None
    assert document.identity_authoritative is False
    assert document.decision_eligible is False
    assert document.execution_locked is True
    assert b'"execution_locked":true' in document.canonical_bytes()


@pytest.mark.parametrize(
    ("family", "route"),
    [
        ("company_equity_like", "current_held_common_stock"),
        ("fund_or_structured_like", "current_held_fund_or_structured"),
        ("unknown", "manual_review"),
    ],
)
def test_recommended_route_is_deterministic(family: str, route: str) -> None:
    assert recommended_identity_route(family) == route


def test_fund_or_structured_discovery_does_not_claim_etf_or_etn() -> None:
    document = _document(
        levels=FosunClassificationLevels(first=2, second=202, third=20202),
        equity_profile_fields_complete=False,
        net_asset_profile_fields_complete=True,
        routing_family="fund_or_structured_like",
        recommended_identity_route="current_held_fund_or_structured",
    )

    assert document.canonical_asset_type is None
    assert "etf" not in document.canonical_bytes().decode().lower()
    assert "exchange_traded_note" not in document.canonical_bytes().decode().lower()


@pytest.mark.parametrize(
    "changes",
    [
        {"provider_code": "NEW"},
        {"provider_code": "hk00656"},
        {"market_scope": "NASDAQ"},
        {"currency": "HKD"},
        {"recommended_identity_route": "manual_review"},
        {"holding_observed_at": NOW + timedelta(seconds=1)},
        {"available_at": NOW + timedelta(seconds=1)},
        {
            "received_at": datetime.now(UTC) + timedelta(days=1),
            "available_at": datetime.now(UTC) + timedelta(days=1),
        },
    ],
)
def test_document_rejects_invalid_identity_claim_or_clock(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _document(**changes)


def test_document_rejects_family_not_derived_from_fields() -> None:
    with pytest.raises(ValidationError, match="ROUTING_FAMILY_MISMATCH"):
        _document(routing_family="unknown", recommended_identity_route="manual_review")
