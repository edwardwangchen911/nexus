"""Stable API error semantics."""

from collections.abc import Mapping
from enum import StrEnum
from uuid import UUID


class ErrorCode(StrEnum):
    """Client-visible error codes defined by the API contract."""

    UNAUTHENTICATED = "UNAUTHENTICATED"
    FORBIDDEN = "FORBIDDEN"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    DATA_PARTIAL = "DATA_PARTIAL"
    DATA_STALE = "DATA_STALE"
    BROKER_STALE = "BROKER_STALE"
    SOURCE_CONFLICT = "SOURCE_CONFLICT"
    CAPABILITY_EXPIRED = "CAPABILITY_EXPIRED"
    PROPOSAL_NOT_EXECUTABLE = "PROPOSAL_NOT_EXECUTABLE"
    ACCOUNT_TYPE_MISMATCH = "ACCOUNT_TYPE_MISMATCH"
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    RISK_REJECTED = "RISK_REJECTED"
    DRAWDOWN_LOCKED = "DRAWDOWN_LOCKED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    SIMULATION_WRITE_DISABLED = "SIMULATION_WRITE_DISABLED"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    RESEARCH_ACCOUNTING_BUSY = "RESEARCH_ACCOUNTING_BUSY"
    PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
    EVOLUTION_BLOCKED = "EVOLUTION_BLOCKED"
    AUTHORITY_UNAVAILABLE = "AUTHORITY_UNAVAILABLE"
    SCENARIO_BASELINE_NOT_READY = "SCENARIO_BASELINE_NOT_READY"
    SCENARIO_DIAGNOSTIC_MISMATCH = "SCENARIO_DIAGNOSTIC_MISMATCH"
    SCENARIO_AS_OF_MISMATCH = "SCENARIO_AS_OF_MISMATCH"
    SCENARIO_OBJECTIVE_UNSUPPORTED = "SCENARIO_OBJECTIVE_UNSUPPORTED"
    SCENARIO_INPUT_INVALID = "SCENARIO_INPUT_INVALID"
    SCENARIO_US_EQUITY_SCOPE_INVALID = "SCENARIO_US_EQUITY_SCOPE_INVALID"
    SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID = "SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID"
    PORTFOLIO_DECISION_AUTHORITY_MISMATCH = "PORTFOLIO_DECISION_AUTHORITY_MISMATCH"
    PORTFOLIO_DECISION_INPUT_UNSAFE = "PORTFOLIO_DECISION_INPUT_UNSAFE"
    PORTFOLIO_DECISION_BUDGET_EXHAUSTED = "PORTFOLIO_DECISION_BUDGET_EXHAUSTED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class NexusError(Exception):
    """An expected API failure with an HTTP status and stable code."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        status_code: int,
        details: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = dict(details or {})

    def to_envelope(self, request_id: UUID) -> dict[str, dict[str, object]]:
        """Return exactly the public error shape documented for API clients."""
        if not isinstance(request_id, UUID):
            raise TypeError("request_id must be a UUID")
        if request_id.version != 7:
            raise ValueError("request_id must be a UUIDv7")
        return {
            "error": {
                "code": self.code.value,
                "message": self.message,
                "request_id": str(request_id),
                "details": self.details,
            }
        }
