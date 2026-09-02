"""Validate that one broker sync result is safe to persist as an inseparable fact."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from nexus.domain.time import require_utc
from nexus.providers.broker.base import (
    BrokerOrderFact,
    BrokerPosition,
    BrokerSyncResult,
    CashBalance,
    SourceObservation,
)
from nexus.reconciliation.facts import (
    BrokerReadSet,
    DerivedCurrencyNav,
    compare_read_sets,
)

_PHASES = (
    "cash_a",
    "positions_a",
    "orders_a",
    "positions_b",
    "orders_b",
    "cash_b",
)


class InvalidBrokerSyncResult(ValueError):
    """The result does not contain the evidence required by its terminal status."""


def validate_broker_sync_result(result: BrokerSyncResult) -> None:
    snapshot = result.portfolio_snapshot
    if result.status != snapshot.status:
        raise InvalidBrokerSyncResult("result and snapshot status differ")
    if result.attempt_count not in (1, 2) or len(result.attempt_traces) != result.attempt_count:
        raise InvalidBrokerSyncResult("attempt count and traces differ")
    for expected, trace in enumerate(result.attempt_traces, start=1):
        if trace.get("attempt") != expected:
            raise InvalidBrokerSyncResult("attempt sequence is incomplete")
    if result.status != "READY":
        return
    if result.reconciliation_reasons:
        raise InvalidBrokerSyncResult("READY result must not retain mismatch reasons")
    if snapshot.as_of is None:
        raise InvalidBrokerSyncResult("READY snapshot requires a declared snapshot time")
    trace = result.attempt_traces[-1]
    if (
        trace.get("completed") is not True
        or trace.get("reason") != "reconciled"
        or trace.get("phase") != "cash_b"
    ):
        raise InvalidBrokerSyncResult("final attempt was not reconciled")
    raw = trace.get("raw_facts")
    if not isinstance(raw, dict):
        raise InvalidBrokerSyncResult("final attempt raw facts are absent")
    phases = tuple(key for key in raw if key != "_derived_nav")
    if phases != _PHASES:
        raise InvalidBrokerSyncResult("final attempt raw phase order is invalid")
    _validate_raw_snapshot(raw, result)
    observations = _validate_observations(trace, result)
    _validate_nav(raw, trace)
    _validate_fact_times(raw, observations, result)
    _validate_reconciliation(raw, observations)


def _validate_raw_snapshot(raw: dict[object, object], result: BrokerSyncResult) -> None:
    snapshot = result.portfolio_snapshot
    expected = {
        "cash_b": tuple(item.model_dump(mode="json") for item in snapshot.cash),
        "positions_b": tuple(item.model_dump(mode="json") for item in snapshot.positions),
        "orders_b": tuple(item.model_dump(mode="json") for item in snapshot.open_orders),
    }
    if any(raw.get(phase) != facts for phase, facts in expected.items()):
        raise InvalidBrokerSyncResult("final attempt facts do not match the snapshot")


def _validate_observations(
    trace: dict[str, object], result: BrokerSyncResult
) -> tuple[tuple[str, SourceObservation], ...]:
    observations = trace.get("source_observations")
    source_ids = trace.get("source_request_ids")
    if not isinstance(observations, tuple | list) or not isinstance(source_ids, tuple | list):
        raise InvalidBrokerSyncResult("final attempt source evidence is absent")
    phases_seen: set[str] = set()
    b_source_ids: list[str] = []
    b_observations: list[SourceObservation] = []
    observed_ids: list[str] = []
    typed: list[tuple[str, SourceObservation]] = []
    phase_sequence: list[str] = []
    for observation in observations:
        if not isinstance(observation, dict):
            raise InvalidBrokerSyncResult("source observation is invalid")
        phase = observation.get("phase")
        request_id = observation.get("source_request_id")
        response_as_of = observation.get("response_as_of")
        quality = observation.get("quality")
        if phase not in _PHASES or not isinstance(request_id, str) or not request_id:
            raise InvalidBrokerSyncResult("source observation is incomplete")
        if quality != "PASS":
            raise InvalidBrokerSyncResult("READY source observations must all be PASS")
        if not isinstance(response_as_of, str):
            raise InvalidBrokerSyncResult("source observation time is absent")
        try:
            parsed = SourceObservation.model_validate(
                {
                    "source_request_id": request_id,
                    "response_as_of": response_as_of,
                    "quality": quality,
                    "time_basis": observation.get("time_basis", "provider_event"),
                }
            )
        except ValueError:
            raise InvalidBrokerSyncResult("source observation time is invalid") from None
        phases_seen.add(phase)
        phase_sequence.append(phase)
        observed_ids.append(request_id)
        typed.append((phase, parsed))
        if phase in {"positions_b", "orders_b", "cash_b"}:
            b_source_ids.append(request_id)
            b_observations.append(parsed)
    phase_blocks = tuple(
        phase
        for index, phase in enumerate(phase_sequence)
        if index == 0 or phase != phase_sequence[index - 1]
    )
    phase_counts = {phase: phase_sequence.count(phase) for phase in _PHASES}
    if (
        phase_blocks != _PHASES
        or phase_counts["cash_a"] != 1
        or phase_counts["cash_b"] != 1
        or any(phase_counts[phase] < 1 for phase in _PHASES[1:5])
    ):
        raise InvalidBrokerSyncResult("six-phase source observation phase blocks are invalid")
    if phases_seen != set(_PHASES) or list(source_ids) != observed_ids:
        raise InvalidBrokerSyncResult("six-phase source evidence is incomplete")
    if tuple(b_source_ids) != result.portfolio_snapshot.source_request_ids:
        raise InvalidBrokerSyncResult("snapshot source IDs do not match final read B")
    if tuple(b_observations) != result.portfolio_snapshot.source_observations:
        raise InvalidBrokerSyncResult("snapshot source observations do not match final read B")
    if len({item.time_basis for _, item in typed}) != 1:
        raise InvalidBrokerSyncResult("broker snapshot time bases differ")
    if typed[0][1].time_basis == "gateway_observed":
        times = [item.response_as_of for _, item in typed]
        if times != sorted(times) or (times[-1] - times[0]).total_seconds() > 60:
            raise InvalidBrokerSyncResult("broker observation window is invalid")
        if len(set(observed_ids)) != len(observed_ids):
            raise InvalidBrokerSyncResult("broker observations reuse a request")
    return tuple(typed)


def _validate_fact_times(
    raw: dict[object, object],
    observations: tuple[tuple[str, SourceObservation], ...],
    result: BrokerSyncResult,
) -> None:
    observed = observations[0][1].time_basis == "gateway_observed"
    responses: dict[str, list[datetime]] = {phase: [] for phase in _PHASES}
    for phase, observation in observations:
        responses[phase].append(observation.response_as_of)
    b_fact_times: list[datetime] = []
    for suffix in ("a", "b"):
        phase_facts = (
            (
                f"cash_{suffix}",
                (item.provider_as_of for item in _cash_rows(raw, suffix)),
            ),
            (
                f"positions_{suffix}",
                (item.provider_as_of for item in _position_rows(raw, suffix)),
            ),
            (
                f"orders_{suffix}",
                (item.updated_at for item in _order_rows(raw, suffix)),
            ),
        )
        for phase, timestamps in phase_facts:
            phase_times = tuple(timestamps)
            if not observed and any(timestamp is None for timestamp in phase_times):
                raise InvalidBrokerSyncResult("READY broker facts require Provider times")
            typed_times = tuple(timestamp for timestamp in phase_times if timestamp is not None)
            if any(timestamp > max(responses[phase]) for timestamp in typed_times):
                raise InvalidBrokerSyncResult("broker fact is later than its phase response time")
            if suffix == "b":
                b_fact_times.extend(typed_times)
    snapshot_as_of = result.portfolio_snapshot.as_of
    if observed:
        b_fact_times = [item.response_as_of for phase, item in observations if phase.endswith("_b")]
    if not b_fact_times or snapshot_as_of != max(b_fact_times):
        raise InvalidBrokerSyncResult("snapshot as-of does not match final read B facts")


def _cash_rows(raw: dict[object, object], suffix: str) -> tuple[CashBalance, ...]:
    return tuple(CashBalance.model_validate(row) for row in _raw_rows(raw, f"cash_{suffix}"))


def _position_rows(raw: dict[object, object], suffix: str) -> tuple[BrokerPosition, ...]:
    return tuple(
        BrokerPosition.model_validate(row) for row in _raw_rows(raw, f"positions_{suffix}")
    )


def _order_rows(raw: dict[object, object], suffix: str) -> tuple[BrokerOrderFact, ...]:
    return tuple(BrokerOrderFact.model_validate(row) for row in _raw_rows(raw, f"orders_{suffix}"))


def _validate_reconciliation(
    raw: dict[object, object],
    observations: tuple[tuple[str, SourceObservation], ...],
) -> None:
    try:
        read_a = _rebuild_read_set(raw, observations, "a")
        read_b = _rebuild_read_set(raw, observations, "b")
        reconciled = compare_read_sets(read_a, read_b)
    except (KeyError, TypeError, ValueError):
        raise InvalidBrokerSyncResult("six-phase typed facts are invalid") from None
    if not reconciled.consistent:
        raise InvalidBrokerSyncResult("six-phase deterministic reconciliation failed")


def _rebuild_read_set(
    raw: dict[object, object],
    observations: tuple[tuple[str, SourceObservation], ...],
    suffix: str,
) -> BrokerReadSet:
    cash = tuple(CashBalance.model_validate(row) for row in _raw_rows(raw, f"cash_{suffix}"))
    positions = tuple(
        BrokerPosition.model_validate(row) for row in _raw_rows(raw, f"positions_{suffix}")
    )
    orders = tuple(
        BrokerOrderFact.model_validate(row) for row in _raw_rows(raw, f"orders_{suffix}")
    )
    read_observations = tuple(
        observation
        for phase, observation in observations
        if phase in {f"cash_{suffix}", f"positions_{suffix}", f"orders_{suffix}"}
    )
    return BrokerReadSet(
        cash=cash,
        positions=positions,
        open_orders=orders,
        completed_at=max(item.response_as_of for item in read_observations),
        source_request_ids=tuple(item.source_request_id for item in read_observations),
        source_observations=read_observations,
        nav_by_currency=_typed_nav(raw, "read_a" if suffix == "a" else "read_b"),
    )


def _raw_rows(raw: dict[object, object], phase: str) -> tuple[object, ...]:
    rows = raw.get(phase)
    if not isinstance(rows, tuple | list):
        raise ValueError("broker phase rows are absent")
    return tuple(rows)


def _typed_nav(raw: dict[object, object], label: str) -> tuple[DerivedCurrencyNav, ...]:
    derived = raw.get("_derived_nav")
    if not isinstance(derived, dict):
        raise ValueError("derived NAV rows are absent")
    rows = derived.get(label)
    if not isinstance(rows, tuple | list):
        raise ValueError("derived NAV rows are absent")
    result: list[DerivedCurrencyNav] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("derived NAV row is invalid")
        currency = row.get("currency")
        value = row.get("value")
        provider_as_of = row.get("provider_as_of")
        observed_at = row.get("observed_at")
        source_request_ids = row.get("source_request_ids")
        if (
            not isinstance(currency, str)
            or len(currency) != 3
            or not currency.isascii()
            or not currency.isalpha()
            or currency != currency.upper()
            or not isinstance(value, str)
            or not (
                (isinstance(provider_as_of, str) and observed_at is None)
                or (provider_as_of is None and isinstance(observed_at, str))
            )
            or not isinstance(source_request_ids, tuple | list)
        ):
            raise ValueError("derived NAV row is invalid")
        result.append(
            DerivedCurrencyNav(
                currency=currency,
                value=Decimal(value),
                provider_as_of=(
                    require_utc(datetime.fromisoformat(provider_as_of))
                    if isinstance(provider_as_of, str)
                    else None
                ),
                source_request_ids=tuple(source_request_ids),
                observed_at=(
                    require_utc(datetime.fromisoformat(observed_at))
                    if isinstance(observed_at, str)
                    else None
                ),
            )
        )
    return tuple(result)


def _validate_nav(raw: dict[object, object], trace: dict[str, object]) -> None:
    derived = raw.get("_derived_nav")
    if not isinstance(derived, dict) or set(derived) != {"read_a", "read_b"}:
        raise InvalidBrokerSyncResult("derived NAV evidence is absent")
    observations = trace.get("source_observations")
    if not isinstance(observations, tuple | list):
        raise InvalidBrokerSyncResult("derived NAV source observations are absent")
    for label, suffix in (("read_a", "a"), ("read_b", "b")):
        rows = derived[label]
        if not isinstance(rows, tuple | list) or not rows:
            raise InvalidBrokerSyncResult("derived NAV evidence is empty")
        read_observations = [
            item
            for item in observations
            if isinstance(item, dict)
            and item.get("phase") in {f"cash_{suffix}", f"positions_{suffix}"}
        ]
        observed_at = None
        if read_observations[0].get("time_basis", "provider_event") == "gateway_observed":
            observed_at = max(
                require_utc(datetime.fromisoformat(str(item["response_as_of"])))
                for item in read_observations
            )
        expected = _recompute_nav(raw, suffix, observed_at=observed_at)
        seen: set[str] = set()
        phase_source_ids = {
            observation.get("source_request_id")
            for observation in observations
            if isinstance(observation, dict)
            and observation.get("phase") in {f"cash_{suffix}", f"positions_{suffix}"}
        }
        for row in rows:
            if not isinstance(row, dict):
                raise InvalidBrokerSyncResult("derived NAV evidence is invalid")
            currency = row.get("currency")
            request_ids = row.get("source_request_ids")
            if (
                not isinstance(currency, str)
                or len(currency) != 3
                or currency != currency.upper()
                or not isinstance(request_ids, tuple | list)
                or not request_ids
                or not all(isinstance(item, str) and item for item in request_ids)
            ):
                raise InvalidBrokerSyncResult("derived NAV provenance is invalid")
            try:
                value = row["value"]
                as_of = row.get("observed_at") if observed_at else row.get("provider_as_of")
                if (observed_at and row.get("provider_as_of") is not None) or (
                    not observed_at and row.get("observed_at") is not None
                ):
                    raise ValueError
                if not isinstance(value, str) or not isinstance(as_of, str):
                    raise ValueError
                decimal_value = Decimal(value)
                parsed_as_of = require_utc(datetime.fromisoformat(as_of))
                expected_value = expected.get(currency)
                if (
                    not decimal_value.is_finite()
                    or expected_value is None
                    or decimal_value != expected_value[0]
                    or parsed_as_of != expected_value[1]
                    or not set(request_ids).issubset(phase_source_ids)
                ):
                    raise ValueError
                seen.add(currency)
            except (KeyError, ValueError):
                raise InvalidBrokerSyncResult("derived NAV value or time is invalid") from None
        if seen != set(expected):
            raise InvalidBrokerSyncResult("derived NAV currency coverage is invalid")


def _recompute_nav(
    raw: dict[object, object], suffix: str, *, observed_at: datetime | None = None
) -> dict[str, tuple[Decimal, datetime]]:
    cash_rows = raw.get(f"cash_{suffix}")
    position_rows = raw.get(f"positions_{suffix}")
    if not isinstance(cash_rows, tuple | list) or not isinstance(position_rows, tuple | list):
        raise InvalidBrokerSyncResult("derived NAV inputs are absent")
    try:
        cash = tuple(CashBalance.model_validate(row) for row in cash_rows)
        positions = tuple(BrokerPosition.model_validate(row) for row in position_rows)
    except ValueError:
        raise InvalidBrokerSyncResult("derived NAV inputs are invalid") from None
    values: dict[str, tuple[Decimal, datetime]] = {}
    for item in cash:
        as_of = observed_at or item.provider_as_of
        if item.currency in values or as_of is None:
            raise InvalidBrokerSyncResult("derived NAV cash inputs are ambiguous")
        values[item.currency] = (item.cash_balance, as_of)
    for position in positions:
        prior = values.get(position.currency)
        as_of = observed_at or position.provider_as_of
        if prior is None or position.current_price is None or as_of is None:
            raise InvalidBrokerSyncResult("derived NAV position inputs are incomplete")
        values[position.currency] = (
            prior[0] + position.quantity * position.current_price,
            max(prior[1], as_of),
        )
    return values
