from typing import Literal

PaperBatchErrorCode = Literal[
    "PAPER_ACCOUNT_INVALID",
    "STRATEGY_DECISION_INVALID",
    "RISK_POLICY_INVALID",
    "RISK_AUTHORITY_INCOMPLETE",
    "PAPER_PROJECTION_INVALID",
    "PAPER_BATCH_CONFLICT",
    "IDEMPOTENCY_CONFLICT",
    "PORTFOLIO_ACTIVATION_REQUIRED",
]

_SAFE_MESSAGES: dict[PaperBatchErrorCode, str] = {
    "PAPER_ACCOUNT_INVALID": "The internal paper account is unavailable.",
    "STRATEGY_DECISION_INVALID": "The strategy decision is not consumable.",
    "RISK_POLICY_INVALID": "The deterministic risk policy is unavailable.",
    "RISK_AUTHORITY_INCOMPLETE": "The point-in-time risk authority is incomplete.",
    "PAPER_PROJECTION_INVALID": "The internal paper projection is invalid.",
    "PAPER_BATCH_CONFLICT": "The paper order batch conflicts with existing facts.",
    "IDEMPOTENCY_CONFLICT": "The idempotency key was used for another request.",
    "PORTFOLIO_ACTIVATION_REQUIRED": (
        "Portfolio order batches must be created by target activation."
    ),
}


class PaperBatchError(ValueError):
    """Stable fail-closed application error without provider or SQL details."""

    def __init__(self, code: PaperBatchErrorCode) -> None:
        self.code = code
        self.safe_message = _SAFE_MESSAGES[code]
        super().__init__(code)


PaperExecutionErrorCode = Literal[
    "ECONOMIC_PAIR_CYCLE_NOT_BOUND",
    "ECONOMIC_WINDOW_REQUIRED",
    "ECONOMIC_WINDOW_INVALID",
    "ECONOMIC_WINDOW_MISSED",
    "ECONOMIC_WINDOW_ORDER_REQUIRED",
    "ECONOMIC_WINDOW_ARCHIVES_REQUIRED",
    "ECONOMIC_WINDOW_ARCHIVE_CONFLICT",
    "ECONOMIC_WINDOW_BINDING_REQUIRED",
    "ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE",
    "PAPER_EXECUTION_INVALID",
    "PAPER_EXECUTION_CONFLICT",
    "PAPER_EXECUTION_AUTHORITY_INCOMPLETE",
    "PAPER_EXECUTION_RESERVATION_INVALID",
    "PAPER_EXECUTION_IDEMPOTENCY_CONFLICT",
]

_EXECUTION_SAFE_MESSAGES: dict[PaperExecutionErrorCode, str] = {
    "ECONOMIC_PAIR_CYCLE_NOT_BOUND": "Economic common-window execution is unavailable.",
    "ECONOMIC_WINDOW_REQUIRED": "Economic common-window execution is unavailable.",
    "ECONOMIC_WINDOW_INVALID": "Economic common-window execution is unavailable.",
    "ECONOMIC_WINDOW_MISSED": "Economic common-window execution is unavailable.",
    "ECONOMIC_WINDOW_ORDER_REQUIRED": "Economic common-window execution is unavailable.",
    "ECONOMIC_WINDOW_ARCHIVES_REQUIRED": "Economic common-window execution is unavailable.",
    "ECONOMIC_WINDOW_ARCHIVE_CONFLICT": "Economic common-window execution is unavailable.",
    "ECONOMIC_WINDOW_BINDING_REQUIRED": "Economic common-window execution is unavailable.",
    "ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE": "Economic common-window execution is unavailable.",
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


PaperPerformanceErrorCode = Literal[
    "PAPER_PERFORMANCE_INVALID",
    "PAPER_PERFORMANCE_PRICE_INCOMPLETE",
    "PAPER_PERFORMANCE_PROJECTION_INVALID",
    "PAPER_PERFORMANCE_CONFLICT",
]

_PERFORMANCE_SAFE_MESSAGES: dict[PaperPerformanceErrorCode, str] = {
    "PAPER_PERFORMANCE_INVALID": "The internal paper performance request is invalid.",
    "PAPER_PERFORMANCE_PRICE_INCOMPLETE": ("The end-of-day price authority is incomplete."),
    "PAPER_PERFORMANCE_PROJECTION_INVALID": (
        "The internal paper performance projection is invalid."
    ),
    "PAPER_PERFORMANCE_CONFLICT": ("The internal paper performance conflicts with existing facts."),
}


class PaperPerformanceError(ValueError):
    """Stable performance publication error without account or source details."""

    def __init__(self, code: PaperPerformanceErrorCode) -> None:
        self.code = code
        self.safe_message = _PERFORMANCE_SAFE_MESSAGES[code]
        super().__init__(code)
