"""Fixed schemas for the committee's non-executable read-only data boundary."""

from collections.abc import Mapping
from datetime import date, datetime
from typing import Final, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from nexus.domain.time import require_utc

EXPECTED_READ_ONLY_TOOL_NAMES: Final[tuple[str, ...]] = (
    "get_security_snapshot",
    "get_financial_facts",
    "search_filing_evidence",
    "get_price_features",
    "get_macro_snapshot",
    "get_portfolio_exposure",
    "get_portfolio_diagnostics",
    "get_proposal_comparison",
)

_FINANCIAL_FIELDS: Final = (
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "operating_cash_flow",
    "capital_expenditure",
    "stockholders_equity",
    "diluted_shares",
    "diluted_eps",
)


class ToolInputError(ValueError):
    """Raised before an untyped or sensitive field crosses a tool boundary."""


class _ToolInput(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("as_of", check_fields=False)
    @classmethod
    def require_point_in_time(cls, value: datetime) -> datetime:
        return require_utc(value)


class _SnapshotInput(_ToolInput):
    instrument_id: UUID
    data_snapshot_id: UUID
    as_of: datetime


class _FinancialFactsInput(_SnapshotInput):
    fields: tuple[Literal[
        "revenue", "gross_profit", "operating_income", "net_income", "operating_cash_flow",
        "capital_expenditure", "stockholders_equity", "diluted_shares", "diluted_eps",
    ], ...] = Field(min_length=1, max_length=len(_FINANCIAL_FIELDS))


class _FilingEvidenceInput(_SnapshotInput):
    forms: tuple[Literal["10-K", "10-Q", "8-K"], ...] = Field(min_length=1, max_length=3)
    filed_from: date
    filed_to: date

    @model_validator(mode="after")
    def bounded_date_range(self) -> "_FilingEvidenceInput":
        if self.filed_to < self.filed_from or (self.filed_to - self.filed_from).days > 366:
            raise ValueError("filing date range must be ordered and no longer than 366 days")
        return self


class _PriceFeaturesInput(_SnapshotInput):
    features: tuple[Literal["close", "sma_20", "sma_200", "rsi_14", "volatility_20"], ...] = Field(
        min_length=1, max_length=5
    )


class _MacroSnapshotInput(_ToolInput):
    data_snapshot_id: UUID
    as_of: datetime
    series: tuple[
        Literal["DFF", "DGS2", "DGS10", "CPIAUCSL", "UNRATE", "INDPRO", "VIXCLS"], ...
    ] = Field(
        min_length=1, max_length=7
    )


class _PortfolioExposureInput(_ToolInput):
    portfolio_snapshot_id: UUID
    instrument_id: UUID | None = None
    as_of: datetime


class _DiagnosticsInput(_ToolInput):
    portfolio_snapshot_id: UUID
    diagnostic_run_id: UUID
    as_of: datetime


class _ProposalComparisonInput(_ToolInput):
    proposal_group_id: UUID
    as_of: datetime


_TOOL_INPUTS: Final[Mapping[str, type[_ToolInput]]] = {
    "get_security_snapshot": _SnapshotInput,
    "get_financial_facts": _FinancialFactsInput,
    "search_filing_evidence": _FilingEvidenceInput,
    "get_price_features": _PriceFeaturesInput,
    "get_macro_snapshot": _MacroSnapshotInput,
    "get_portfolio_exposure": _PortfolioExposureInput,
    "get_portfolio_diagnostics": _DiagnosticsInput,
    "get_proposal_comparison": _ProposalComparisonInput,
}


class ReadOnlyToolRegistry(Protocol):
    """The model receives a catalogue identity, never a gateway or client object."""

    names: tuple[str, ...]


class ReadOnlyTools:
    """Validate a fixed tool request without executing I/O or retaining the payload."""

    names: tuple[str, ...] = EXPECTED_READ_ONLY_TOOL_NAMES

    def validate(self, name: str, payload: Mapping[str, object]) -> dict[str, object]:
        """Return typed, JSON-safe fields or reject unknown/sensitive input before any lookup."""
        schema = _TOOL_INPUTS.get(name)
        if schema is None:
            raise ToolInputError("tool is not in the fixed read-only catalogue")
        try:
            return schema.model_validate(payload).model_dump(mode="python")
        except ValidationError as error:
            raise ToolInputError("tool payload is not an allowlisted typed request") from error


def response_function_tools() -> Mapping[str, dict[str, object]]:
    """Expose fixed custom-function schemas; no built-in or dynamically named tool is possible."""
    return {
        name: {
            "type": "function",
            "name": name,
            "description": "Read-only typed research lookup.",
            "parameters": schema.model_json_schema(),
            "strict": True,
        }
        for name, schema in _TOOL_INPUTS.items()
    }
