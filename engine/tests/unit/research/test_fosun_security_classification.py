from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.research.fosun_security_classification import (
    FosunClassificationLevels,
    FosunFundAssetType,
    FosunRawQuoteBinding,
    FosunSecurityClassificationDocument,
)
from pydantic import ValidationError

NOW = datetime(2025, 9, 12, tzinfo=UTC)
INSTRUMENT = UUID("00000000-0000-7000-8000-000000000001")


def _document(**changes: object) -> FosunSecurityClassificationDocument:
    values: dict[str, object] = {
        "instrument_id": INSTRUMENT,
        "provider_code": "usAAPL",
        "provider_name": "Apple",
        "levels": FosunClassificationLevels(first=1, second=101, third=10105),
        "equity_profile_fields_complete": True,
        "net_asset_profile_fields_complete": False,
        "fund_asset_type": FosunFundAssetType(chs="", cht=None, eng="Stock"),
        "routing_family": "company_equity_like",
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
    return FosunSecurityClassificationDocument.model_validate(values)


def test_document_has_deterministic_identity_and_bytes() -> None:
    document = _document()
    assert document.provider_object_id == f"classification:{INSTRUMENT}:usAAPL"
    assert document.canonical_bytes() == document.canonical_bytes()
    assert b'"execution_locked":true' in document.canonical_bytes()
    assert document.fund_asset_type.chs == ""


@pytest.mark.parametrize(
    ("levels", "equity", "net_asset", "family"),
    [
        ((1, 101, 10105), True, False, "company_equity_like"),
        ((2, 202, 20202), False, True, "fund_or_structured_like"),
        ((2, 202, 20202), True, True, "unknown"),
    ],
)
def test_document_requires_exact_derived_family(
    levels: tuple[int, int, int], equity: bool, net_asset: bool, family: str
) -> None:
    document = _document(
        levels=FosunClassificationLevels(
            first=levels[0], second=levels[1], third=levels[2]
        ),
        equity_profile_fields_complete=equity,
        net_asset_profile_fields_complete=net_asset,
        routing_family=family,
    )
    assert document.routing_family == family
    with pytest.raises(ValidationError, match="ROUTING_FAMILY_MISMATCH"):
        _document(
            levels=document.levels,
            equity_profile_fields_complete=equity,
            net_asset_profile_fields_complete=net_asset,
            routing_family="unknown" if family != "unknown" else "company_equity_like",
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"provider_code": "AAPL"},
        {"available_at": NOW + timedelta(seconds=1)},
        {
            "received_at": datetime.now(UTC) + timedelta(days=1),
            "available_at": datetime.now(UTC) + timedelta(days=1),
        },
        {
            "raw_quote": {
                "artifact_sha256": "BAD",
                "artifact_byte_length": 1,
                "artifact_uri": "s3://bucket/value",
                "gateway_request_id": "00000000-0000-7000-8000-000000000002",
                "request_body_sha256": "b" * 64,
            }
        },
    ],
)
def test_document_rejects_invalid_binding(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _document(**changes)
