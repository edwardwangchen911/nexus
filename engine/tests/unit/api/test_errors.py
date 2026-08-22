from uuid import uuid4

import pytest
from nexus.api.errors import ErrorCode, NexusError
from nexus.domain.ids import new_uuid7


def test_nexus_error_serializes_the_strict_error_envelope() -> None:
    """Clients can depend on the four documented fields without incidental metadata."""
    error = NexusError(
        ErrorCode.DATA_STALE,
        "Data is too old for this operation.",
        status_code=409,
        details={"source": "sec"},
    )

    request_id = new_uuid7()
    envelope = error.to_envelope(request_id)

    assert envelope == {
        "error": {
            "code": "DATA_STALE",
            "message": "Data is too old for this operation.",
            "request_id": str(request_id),
            "details": {"source": "sec"},
        }
    }


def test_nexus_error_rejects_non_uuid_request_id() -> None:
    """A string request ID could bypass the API's required UUIDv7 trace contract."""
    error = NexusError(ErrorCode.INTERNAL_ERROR, "Unexpected error.", status_code=500)

    with pytest.raises(TypeError, match="UUID"):
        error.to_envelope("0198620d-9a9e-7d9b-8a9c-0a543e5e47e1")  # type: ignore[arg-type]


def test_nexus_error_rejects_non_v7_request_id() -> None:
    """A UUID from another version is not a valid NEXUS request trace identifier."""
    error = NexusError(ErrorCode.INTERNAL_ERROR, "Unexpected error.", status_code=500)

    with pytest.raises(ValueError, match="UUIDv7"):
        error.to_envelope(uuid4())


def test_error_codes_include_all_stable_client_branches() -> None:
    """A missing stable code would force clients to misclassify a documented failure."""
    assert {code.value for code in ErrorCode} == {
        "UNAUTHENTICATED",
        "FORBIDDEN",
        "VALIDATION_ERROR",
        "NOT_FOUND",
        "VERSION_CONFLICT",
        "IDEMPOTENCY_CONFLICT",
        "DATA_PARTIAL",
        "DATA_STALE",
        "BROKER_STALE",
        "SOURCE_CONFLICT",
        "CAPABILITY_EXPIRED",
        "PROPOSAL_NOT_EXECUTABLE",
        "ACCOUNT_TYPE_MISMATCH",
        "APPROVAL_REQUIRED",
        "RISK_REJECTED",
        "DRAWDOWN_LOCKED",
        "RECONCILIATION_REQUIRED",
        "BUDGET_EXHAUSTED",
        "PROVIDER_UNAVAILABLE",
        "INTERNAL_ERROR",
    }
