"""Display projection of an already validated, frozen action observation."""

from decimal import Decimal
from typing import Literal
from uuid import UUID

from nexus.api.schemas.agents import ResearchInputActionEvent, ResearchInputActionObservation
from nexus.domain.decimal import decimal_to_string
from nexus.research.alpaca_actions import AlpacaActionObservationDocument


def _display_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    sign, digits, exponent = value.as_tuple()
    # Canonical validation rejects non-finite values. Bound expansion BEFORE format().
    if not isinstance(exponent, int):
        return None
    count = len(digits)
    length = sign + (count + exponent if exponent >= 0 else max(count + 1, 2 - exponent))
    return decimal_to_string(value) if length <= 40 else None


def project_action_observation(
    document: AlpacaActionObservationDocument,
    *,
    evidence_ref_id: UUID,
    revision: str,
    content_hash: str,
) -> ResearchInputActionObservation:
    events = []
    names: tuple[Literal["rate", "old_rate", "new_rate"], ...] = ("rate", "old_rate", "new_rate")
    for event in document.events[:32]:
        numbers = {name: _display_decimal(getattr(event, name)) for name in names}
        unavailable = tuple(
            name for name in names if getattr(event, name) is not None and numbers[name] is None
        )
        events.append(
            ResearchInputActionEvent.model_validate(
                {
                    **event.model_dump(exclude={"rate", "old_rate", "new_rate"}),
                    **numbers,
                    "unavailable_numeric_fields": unavailable,
                }
            )
        )
    return ResearchInputActionObservation(
        evidence_ref_id=evidence_ref_id,
        source="alpaca_action_observation",
        revision=revision,
        content_hash=content_hash,
        symbol=document.symbol,
        query_start=document.query_start,
        query_end=document.query_end,
        available_at=document.available_at,
        manifest_content_hash=document.manifest_content_hash,
        page_count=len(document.pages),
        total_event_count=document.total_event_count,
        bound_event_count=len(document.events),
        unbound_event_count=document.unbound_event_count,
        truncated=len(document.events) > len(events),
        events=tuple(events),
        pagination_exhausted=document.pagination_exhausted,
        source_completeness=document.source_completeness,
        revision_semantics=document.revision_semantics,
        historical_backtest_eligible=document.historical_backtest_eligible,
        ledger_eligible=document.ledger_eligible,
        execution_locked=document.execution_locked,
        available_at_semantics="maximum_page_receipt_not_historical_availability",
    )
