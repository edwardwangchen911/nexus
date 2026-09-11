"""Frozen evidence contracts and pure spread-cost calculation for scenarios."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, localcontext
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.time import require_utc
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.scenario_contracts import (
    PortfolioScenarioEvaluationV2,
    canonical_scenario_hash,
)
from nexus.providers.broker.base import PortfolioSnapshot

_ZERO = Decimal("0")
_TWO = Decimal("2")
_HASH_PATTERN = r"^[0-9a-f]{64}$"


class _StrictSpreadModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    @model_validator(mode="before")
    @classmethod
    def _reject_binary_floats(cls, value: object) -> object:
        _reject_float(value)
        return value


def _reject_float(value: object) -> None:
    if isinstance(value, float):
        raise TypeError("scenario spread contracts reject binary floats")
    if isinstance(value, Mapping):
        for nested in value.values():
            _reject_float(nested)
    elif isinstance(value, list | tuple | set | frozenset):
        for nested in value:
            _reject_float(nested)


class ScenarioSpreadCalendarAuthorityV1(_StrictSpreadModel):
    library: Literal["exchange_calendars"]
    library_version: Literal["4.13.2"]
    calendar: Literal["XNYS"]
    session_window_contract: Literal["xnys_regular_open_closed_v1"]
    authority_hash: str = Field(pattern=_HASH_PATTERN)
    session_date: date
    session_open: datetime
    session_close: datetime

    @field_validator("session_open", "session_close")
    @classmethod
    def _canonical_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _ordered_window(self) -> ScenarioSpreadCalendarAuthorityV1:
        if self.session_close <= self.session_open:
            raise ValueError("scenario spread session close must follow open")
        return self


class SelectedSpreadQuoteV1(_StrictSpreadModel):
    instrument_id: UUID
    latest_quote_id: UUID
    content_hash: str = Field(pattern=_HASH_PATTERN)
    artifact_id: UUID
    artifact_hash: str = Field(pattern=_HASH_PATTERN)


class ScenarioSpreadEvidenceInputV1(_StrictSpreadModel):
    manifest_schema: Literal["scenario_spread_evidence_manifest_v1"]
    evidence_selected_at: datetime
    policy_status: Literal["available", "unavailable"]
    source_policy_id: UUID | None
    source_policy_version: Literal["scenario_spread_cost_policy_v1"] | None
    source_policy_payload_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    calendar_authority: ScenarioSpreadCalendarAuthorityV1 | None
    calculation_version: Literal["scenario_spread_cost_v1"]
    no_cost_candidate_hash: str = Field(pattern=_HASH_PATTERN)
    scenario_cost_basis_hash: str = Field(pattern=_HASH_PATTERN)
    regulatory_reference_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    selected_quotes: tuple[SelectedSpreadQuoteV1, ...]

    @field_validator("evidence_selected_at")
    @classmethod
    def _canonical_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @field_validator("selected_quotes")
    @classmethod
    def _canonical_quote_order(
        cls, value: tuple[SelectedSpreadQuoteV1, ...]
    ) -> tuple[SelectedSpreadQuoteV1, ...]:
        return tuple(sorted(value, key=lambda quote: quote.instrument_id.int))

    @model_validator(mode="after")
    def _policy_and_quotes_are_consistent(self) -> ScenarioSpreadEvidenceInputV1:
        quote_ids = tuple(quote.instrument_id for quote in self.selected_quotes)
        if len(set(quote_ids)) != len(quote_ids):
            raise ValueError("scenario spread quotes require unique instruments")
        policy_fields = (
            self.source_policy_id,
            self.source_policy_version,
            self.source_policy_payload_hash,
            self.calendar_authority,
        )
        if self.policy_status == "available" and any(value is None for value in policy_fields):
            raise ValueError("available scenario spread policy requires complete authority")
        if self.policy_status == "unavailable" and (
            any(value is not None for value in policy_fields) or self.selected_quotes
        ):
            raise ValueError("unavailable scenario spread policy cannot carry authority or quotes")
        return self


class ScenarioSpreadEvidenceManifestV1(ScenarioSpreadEvidenceInputV1):
    expected_result_hash: str = Field(pattern=_HASH_PATTERN)


class VerifiedSpreadQuoteV1(_StrictSpreadModel):
    instrument_id: UUID
    bid: Decimal
    ask: Decimal

    @model_validator(mode="after")
    def _valid_quote(self) -> VerifiedSpreadQuoteV1:
        if (
            not self.bid.is_finite()
            or not self.ask.is_finite()
            or self.bid <= _ZERO
            or self.ask <= _ZERO
            or self.bid > self.ask
        ):
            raise ValueError("scenario spread quote must be finite, positive, and uncrossed")
        return self


class ScenarioSpreadEvidenceV1(_StrictSpreadModel):
    input: ScenarioSpreadEvidenceInputV1
    quotes: tuple[VerifiedSpreadQuoteV1, ...]
    bundle_hash: str = Field(pattern=_HASH_PATTERN)
    available_at: datetime | None

    @field_validator("quotes")
    @classmethod
    def _canonical_quote_order(
        cls, value: tuple[VerifiedSpreadQuoteV1, ...]
    ) -> tuple[VerifiedSpreadQuoteV1, ...]:
        return tuple(sorted(value, key=lambda quote: quote.instrument_id.int))

    @field_validator("available_at")
    @classmethod
    def _canonical_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)

    @model_validator(mode="after")
    def _valid_bundle(self) -> ScenarioSpreadEvidenceV1:
        quote_ids = tuple(quote.instrument_id for quote in self.quotes)
        if len(set(quote_ids)) != len(quote_ids):
            raise ValueError("scenario spread quotes require unique instruments")
        selected_ids = tuple(quote.instrument_id for quote in self.input.selected_quotes)
        if quote_ids != selected_ids:
            raise ValueError("scenario spread runtime quotes must match the frozen manifest")
        if self.bundle_hash != scenario_spread_bundle_hash(self.input):
            raise ValueError("scenario spread bundle hash mismatch")
        if self.input.policy_status == "available" and self.available_at is None:
            raise ValueError("available scenario spread evidence requires available_at")
        if self.input.policy_status == "unavailable" and (
            self.quotes or self.available_at is not None
        ):
            raise ValueError("unavailable scenario spread evidence cannot carry quotes")
        return self


def canonical_scenario_spread_manifest_hash(
    manifest: ScenarioSpreadEvidenceManifestV1,
) -> str:
    """Hash the complete persisted manifest, including its expected result hash."""
    return canonical_scenario_hash(_spread_hash_payload(manifest))


def finalize_scenario_spread_manifest(
    evidence_input: ScenarioSpreadEvidenceInputV1,
    *,
    expected_result_hash: str,
) -> ScenarioSpreadEvidenceManifestV1:
    """Add the first calculation's result identity to the persisted manifest."""
    return ScenarioSpreadEvidenceManifestV1.model_validate(
        {**evidence_input.model_dump(mode="python"), "expected_result_hash": expected_result_hash}
    )


def scenario_spread_bundle_hash(evidence_input: ScenarioSpreadEvidenceInputV1) -> str:
    """Hash only frozen calculation inputs, avoiding the final-result hash cycle."""
    return canonical_scenario_hash(_spread_hash_payload(evidence_input))


def _spread_hash_payload(value: BaseModel) -> Mapping[str, object]:
    payload = value.model_dump(mode="python")
    calendar = payload.get("calendar_authority")
    if isinstance(calendar, dict) and isinstance(calendar.get("session_date"), date):
        calendar["session_date"] = calendar["session_date"].isoformat()
    return payload


def scenario_cost_basis_hash(
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
) -> str:
    """Bind spread calculation to snapshot marks, portfolio value, and target weights."""
    if (
        snapshot.id != base_result.portfolio_snapshot_id
        or snapshot.as_of is None
        or snapshot.as_of != base_result.valuation_time
    ):
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")

    snapshot_positions = {
        position.instrument_id: position
        for position in snapshot.positions
        if position.currency == "USD"
    }
    if len(snapshot_positions) != len(
        tuple(position for position in snapshot.positions if position.currency == "USD")
    ):
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")

    position_values: dict[UUID, Decimal] = {}
    basis_positions: list[dict[str, object]] = []
    for position in sorted(base_result.positions, key=lambda item: item.instrument_id.int):
        snapshot_position = snapshot_positions.get(position.instrument_id)
        if snapshot_position is None or snapshot_position.current_price is None:
            raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
        price = snapshot_position.current_price
        value = snapshot_position.quantity * price
        if not price.is_finite() or price <= _ZERO or not value.is_finite():
            raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
        position_values[position.instrument_id] = value
        basis_positions.append(
            {
                "instrument_id": position.instrument_id,
                "snapshot_current_price": price,
                "current_weight": position.current_weight,
                "target_weight": position.target_weight,
            }
        )

    cash = sum(
        (
            item.cash_balance
            for item in snapshot.cash
            if item.currency == "USD" and item.cash_balance.is_finite()
        ),
        _ZERO,
    )
    portfolio_value = cash + sum(position_values.values(), _ZERO)
    if not portfolio_value.is_finite() or portfolio_value <= _ZERO:
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
    return canonical_scenario_hash(
        {
            "schema_version": "scenario_cost_basis_hash_v1",
            "portfolio_snapshot_id": snapshot.id,
            "valuation_time": base_result.valuation_time,
            "portfolio_value": portfolio_value,
            "positions": tuple(basis_positions),
        }
    )


def _instrument_spread_ratio(
    *,
    sold_weight: Decimal,
    snapshot_current_price: Decimal,
    bid: Decimal,
    ask: Decimal,
) -> Decimal:
    midpoint = (bid + ask) / _TWO
    return sold_weight * (midpoint - bid) / snapshot_current_price


def apply_scenario_spread_cost(
    *,
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    evidence: ScenarioSpreadEvidenceV1,
) -> PortfolioScenarioEvaluationV2:
    """Apply an all-or-nothing frozen half-spread estimate to a no-cost scenario."""
    if snapshot.id != base_result.portfolio_snapshot_id or (
        scenario_cost_basis_hash(snapshot, base_result) != evidence.input.scenario_cost_basis_hash
    ):
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
    if canonical_scenario_hash(base_result) != evidence.input.no_cost_candidate_hash:
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")

    reduced = tuple(
        position
        for position in base_result.positions
        if position.target_weight < position.current_weight
    )
    quotes = {quote.instrument_id: quote for quote in evidence.quotes}
    spread = TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE")
    if (
        evidence.input.policy_status == "available"
        and reduced
        and set(quotes) == {position.instrument_id for position in reduced}
    ):
        snapshot_prices = {
            position.instrument_id: position.current_price
            for position in snapshot.positions
            if position.current_price is not None
        }
        with localcontext() as context:
            context.prec = 28
            context.rounding = ROUND_HALF_UP
            spread = TypedMetric.known(
                sum(
                    (
                        _instrument_spread_ratio(
                            sold_weight=position.current_weight - position.target_weight,
                            snapshot_current_price=snapshot_prices[position.instrument_id],
                            bid=quotes[position.instrument_id].bid,
                            ask=quotes[position.instrument_id].ask,
                        )
                        for position in reduced
                    ),
                    _ZERO,
                )
            )

    breakdown = base_result.cost_breakdown.model_copy(
        update={
            "evidence_version": (
                "scenario_cost_evidence_bundle_v1"
                if spread.status == "known"
                else base_result.cost_breakdown.evidence_version
            ),
            "evidence_hash": (
                evidence.bundle_hash
                if spread.status == "known"
                else base_result.cost_breakdown.evidence_hash
            ),
            "available_at": (
                evidence.available_at
                if spread.status == "known"
                else base_result.cost_breakdown.available_at
            ),
            "spread": spread,
        }
    )
    candidate = base_result.candidate.model_copy(
        update={"estimated_cost": TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE")}
    )
    updated = base_result.model_copy(update={"cost_breakdown": breakdown, "candidate": candidate})
    return updated.model_copy(update={"result_hash": canonical_scenario_hash(updated)})
