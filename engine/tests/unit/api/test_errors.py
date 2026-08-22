from nexus.api.errors import ErrorCode, NexusError


def test_nexus_error_serializes_the_strict_error_envelope() -> None:
    """Clients can depend on the four documented fields without incidental metadata."""
    error = NexusError(
        ErrorCode.DATA_STALE,
        "Data is too old for this operation.",
        status_code=409,
        details={"source": "sec"},
    )

    envelope = error.to_envelope("0198620d-9a9e-7d9b-8a9c-0a543e5e47e1")

    assert envelope == {
        "error": {
            "code": "DATA_STALE",
            "message": "Data is too old for this operation.",
            "request_id": "0198620d-9a9e-7d9b-8a9c-0a543e5e47e1",
            "details": {"source": "sec"},
        }
    }


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
