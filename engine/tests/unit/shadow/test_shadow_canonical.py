"""Canonical content-addressing tests for append-only Proposal Shadow facts."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.shadow.canonical import (
    proposal_bundle_content_hash,
    shadow_session_content_hash,
    simulation_state_content_hash,
    target_weights_content_hash,
)
from nexus.shadow.models import (
    SHADOW_VARIANTS,
    CalendarHashBinding,
    EventInputHash,
    MissingExecutionInputHash,
    PriceInputHash,
    ProposalVariantHashInput,
    ShadowSessionHashInput,
)
from nexus.simulation.models import SimulationInstrumentState, SimulationState, TargetWeight


def _session_input(
    *,
    bars: tuple[PriceInputHash, ...] | None = None,
    missing: tuple[MissingExecutionInputHash, ...] = (),
    record_id: UUID | None = None,
    created_at: datetime | None = None,
) -> ShadowSessionHashInput:
    available_at = datetime(2026, 1, 5, 22, tzinfo=UTC)
    default_bars = (
        PriceInputHash(
            instrument_id=UUID(int=2),
            authority_id=UUID(int=102),
            source="alpaca",
            delivery_mode="delayed",
            revision="2",
            available_at=available_at,
            content_hash="b" * 64,
            purpose="mark",
        ),
        PriceInputHash(
            instrument_id=UUID(int=1),
            authority_id=UUID(int=101),
            source="alpaca",
            delivery_mode="delayed",
            revision="1",
            available_at=available_at,
            content_hash="a" * 64,
            purpose="execution_and_mark",
        ),
    )
    return ShadowSessionHashInput(
        shadow_group_id=UUID(int=10),
        session=date(2026, 1, 5),
        ordinal=1,
        calendar=CalendarHashBinding(
            authority_id=UUID(int=20),
            version="xnys_v1",
            content_hash="c" * 64,
        ),
        source_policy_id=UUID(int=30),
        cost_policy_id=UUID(int=40),
        bars=default_bars if bars is None else bars,
        missing_execution_inputs=missing,
        events=(
            EventInputHash(
                instrument_id=UUID(int=1),
                authority_id=UUID(int=201),
                event_kind="split",
                source="sec",
                provider_event_id="event-1",
                revision="1",
                available_at=datetime(2026, 1, 4, 20, tzinfo=UTC),
                evidence_hash="d" * 64,
            ),
        ),
        record_id=record_id,
        created_at=created_at,
    )


def test_shadow_session_hash_is_stable_under_member_insertion_order() -> None:
    original = _session_input()
    reversed_bars = _session_input(bars=tuple(reversed(original.bars)))

    assert shadow_session_content_hash(original) == shadow_session_content_hash(reversed_bars)


def test_shadow_session_hash_changes_for_revision_time_or_negative_evidence() -> None:
    original = _session_input()
    first = original.bars[0]
    revised = _session_input(
        bars=(
            PriceInputHash(
                instrument_id=first.instrument_id,
                authority_id=first.authority_id,
                source=first.source,
                delivery_mode=first.delivery_mode,
                revision="3",
                available_at=first.available_at + timedelta(seconds=1),
                content_hash=first.content_hash,
                purpose=first.purpose,
            ),
            original.bars[1],
        )
    )
    negative = _session_input(
        missing=(
            MissingExecutionInputHash(
                instrument_id=UUID(int=3),
                source="alpaca",
                query_hash="e" * 64,
                reason="BAR_NOT_FOUND_COMPLETE_QUERY",
            ),
        )
    )

    assert shadow_session_content_hash(original) != shadow_session_content_hash(revised)
    assert shadow_session_content_hash(original) != shadow_session_content_hash(negative)


def test_shadow_session_hash_excludes_random_row_metadata() -> None:
    first = _session_input(
        record_id=UUID(int=901),
        created_at=datetime(2026, 1, 6, tzinfo=UTC),
    )
    second = _session_input(
        record_id=UUID(int=902),
        created_at=datetime(2026, 1, 7, tzinfo=UTC),
    )

    assert shadow_session_content_hash(first) == shadow_session_content_hash(second)


def test_target_and_proposal_bundle_hashes_are_order_independent() -> None:
    weights = (
        TargetWeight(instrument_id=UUID(int=2), weight=Decimal("0.4")),
        TargetWeight(instrument_id=UUID(int=1), weight=Decimal("0.5")),
    )
    hold = ProposalVariantHashInput(
        proposal_id=UUID(int=501),
        variant="hold_current",
        targets=weights,
        cash_weight=Decimal("0.1"),
    )
    strategy = ProposalVariantHashInput(
        proposal_id=UUID(int=503),
        variant="strategy_target",
        targets=tuple(reversed(weights)),
        cash_weight=Decimal("0.1"),
    )
    minimum = ProposalVariantHashInput(
        proposal_id=UUID(int=502),
        variant="minimum_change",
        targets=weights,
        cash_weight=Decimal("0.1"),
    )

    assert target_weights_content_hash(weights, Decimal("0.1")) == target_weights_content_hash(
        tuple(reversed(weights)), Decimal("0.1")
    )
    assert proposal_bundle_content_hash((strategy, hold, minimum)) == proposal_bundle_content_hash(
        (hold, minimum, strategy)
    )


def test_target_hash_rejects_duplicate_or_unbalanced_weights() -> None:
    duplicate = TargetWeight(instrument_id=UUID(int=1), weight=Decimal("0.4"))

    with pytest.raises(ValueError, match="unique"):
        target_weights_content_hash((duplicate, duplicate), Decimal("0.2"))
    with pytest.raises(ValueError, match="sum to one"):
        target_weights_content_hash((duplicate,), Decimal("0.5"))


def test_proposal_bundle_hash_rejects_duplicate_proposal_ids() -> None:
    targets = (TargetWeight(instrument_id=UUID(int=1), weight=Decimal("0.9")),)
    values = tuple(
        ProposalVariantHashInput(
            proposal_id=UUID(int=501),
            variant=variant,
            targets=targets,
            cash_weight=Decimal("0.1"),
        )
        for variant in SHADOW_VARIANTS
    )

    with pytest.raises(ValueError, match="proposal identifiers must be unique"):
        proposal_bundle_content_hash(values)


def test_simulation_state_hash_uses_canonical_instrument_order() -> None:
    first = SimulationInstrumentState(
        instrument_id=UUID(int=1),
        quantity=Decimal("2"),
        cost_basis=Decimal("20"),
        realized_pnl=Decimal("0"),
        cumulative_cost=Decimal("0.1"),
        last_close=Decimal("11"),
        untradable=False,
    )
    second = SimulationInstrumentState(
        instrument_id=UUID(int=2),
        quantity=Decimal("1"),
        cost_basis=Decimal("9"),
        realized_pnl=Decimal("1"),
        cumulative_cost=Decimal("0.1"),
        last_close=Decimal("10"),
        untradable=False,
    )

    left = SimulationState(cash=Decimal("50"), instruments=(second, first), receivables=())
    right = SimulationState(cash=Decimal("50"), instruments=(first, second), receivables=())

    assert simulation_state_content_hash(left) == simulation_state_content_hash(right)
