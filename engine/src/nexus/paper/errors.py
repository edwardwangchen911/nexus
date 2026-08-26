from typing import Literal

PaperBatchErrorCode = Literal[
    "PAPER_ACCOUNT_INVALID",
    "STRATEGY_DECISION_INVALID",
    "RISK_POLICY_INVALID",
    "RISK_AUTHORITY_INCOMPLETE",
    "PAPER_PROJECTION_INVALID",
    "PAPER_BATCH_CONFLICT",
    "IDEMPOTENCY_CONFLICT",
]

_SAFE_MESSAGES: dict[PaperBatchErrorCode, str] = {
    "PAPER_ACCOUNT_INVALID": "The internal paper account is unavailable.",
    "STRATEGY_DECISION_INVALID": "The strategy decision is not consumable.",
    "RISK_POLICY_INVALID": "The deterministic risk policy is unavailable.",
    "RISK_AUTHORITY_INCOMPLETE": "The point-in-time risk authority is incomplete.",
    "PAPER_PROJECTION_INVALID": "The internal paper projection is invalid.",
    "PAPER_BATCH_CONFLICT": "The paper order batch conflicts with existing facts.",
    "IDEMPOTENCY_CONFLICT": "The idempotency key was used for another request.",
}


class PaperBatchError(ValueError):
    """Stable fail-closed application error without provider or SQL details."""

    def __init__(self, code: PaperBatchErrorCode) -> None:
        self.code = code
        self.safe_message = _SAFE_MESSAGES[code]
        super().__init__(code)


PaperExecutionErrorCode = Literal[
    "PAPER_EXECUTION_INVALID",
    "PAPER_EXECUTION_CONFLICT",
    "PAPER_EXECUTION_AUTHORITY_INCOMPLETE",
    "PAPER_EXECUTION_RESERVATION_INVALID",
    "PAPER_EXECUTION_IDEMPOTENCY_CONFLICT",
]

_EXECUTION_SAFE_MESSAGES: dict[PaperExecutionErrorCode, str] = {
    "PAPER_EXECUTION_INVALID": "The internal paper order cannot be executed.",
    "PAPER_EXECUTION_CONFLICT": "The internal paper execution conflicts with existing facts.",
    "PAPER_EXECUTION_AUTHORITY_INCOMPLETE": "The paper execution authority is incomplete.",
    "PAPER_EXECUTION_RESERVATION_INVALID": "The paper order reservation is unavailable.",
    "PAPER_EXECUTION_IDEMPOTENCY_CONFLICT": (
        "The idempotency key was used for another paper execution request."
    ),
}


class PaperExecutionError(ValueError):
    """Stable Task 19 failure that does not expose SQL, provider, or account details."""

    def __init__(self, code: PaperExecutionErrorCode) -> None:
        self.code = code
        self.safe_message = _EXECUTION_SAFE_MESSAGES[code]
        super().__init__(code)
