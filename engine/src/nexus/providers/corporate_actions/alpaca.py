"""Alpaca corporate-action normalization; this module never mutates a ledger."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from nexus.domain.evidence import EvidenceRef


class CorporateActionValidationError(ValueError):
    """An Alpaca event cannot safely feed a later, separate ledger consumer."""


@dataclass(frozen=True)
class CorporateAction:
    provider_event_id: str
    action_type: str
    status: str
    process_date: date
    ex_date: date | None
    record_date: date | None
    payable_date: date | None
    rate: Decimal | None
    revision: str
    evidence_ref: EvidenceRef

    @property
    def ledger_eligible(self) -> bool:
        """Eligibility only: all actual effects remain outside this task."""
        return self.status == "complete"


def normalize_alpaca_corporate_action(
    raw: Mapping[str, object], *, evidence_ref: EvidenceRef
) -> CorporateAction:
    """Preserve one provider event/revision as immutable evidence-bound data."""
    provider_event_id = _required_string(raw, "id")
    action_type = _required_string(raw, "type")
    status = _required_string(raw, "status")
    revision = _required_string(raw, "revision")
    rate_raw = raw.get("rate")
    if rate_raw is not None and not isinstance(rate_raw, str):
        raise CorporateActionValidationError("rate must be a decimal string")
    try:
        rate = None if rate_raw is None else Decimal(rate_raw)
    except Exception as error:
        raise CorporateActionValidationError("rate must be a decimal string") from error
    if rate is not None and not rate.is_finite():
        raise CorporateActionValidationError("rate must be a finite decimal string")
    process_date = _optional_date(raw, "process_date", required=True)
    if process_date is None:
        raise CorporateActionValidationError("process_date must be an ISO date")
    return CorporateAction(
        provider_event_id=provider_event_id,
        action_type=action_type,
        status=status,
        process_date=process_date,
        ex_date=_optional_date(raw, "ex_date"),
        record_date=_optional_date(raw, "record_date"),
        payable_date=_optional_date(raw, "payable_date"),
        rate=rate,
        revision=revision,
        evidence_ref=evidence_ref,
    )


def _required_string(raw: Mapping[str, object], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value:
        raise CorporateActionValidationError(f"{key} must be a non-empty string")
    return value


def _optional_date(raw: Mapping[str, object], key: str, *, required: bool = False) -> date | None:
    value = raw.get(key)
    if value is None and not required:
        return None
    if not isinstance(value, str):
        raise CorporateActionValidationError(f"{key} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise CorporateActionValidationError(f"{key} must be an ISO date") from error
