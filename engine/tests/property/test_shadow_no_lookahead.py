"""Property contracts for deterministic point-in-time Shadow selection."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st
from nexus.shadow.authority import ShadowBarCandidate, select_shadow_bar_candidate
from nexus.shadow.canonical import shadow_source_query_content_hash

_OBSERVED_AT = datetime(2026, 8, 26, 21, 30, tzinfo=UTC)


def _candidate(
    identity: int,
    *,
    source: str,
    revision: str,
    available_delta: int,
) -> ShadowBarCandidate:
    return ShadowBarCandidate(
        price_bar_record_id=UUID(int=identity),
        price_bar_set_authority_id=UUID(int=identity + 100),
        instrument_id=UUID(int=1),
        source=source,
        delivery_mode="delayed" if source == "alpaca" else "fixture",
        revision=revision,
        available_at=_OBSERVED_AT + timedelta(minutes=available_delta),
        content_hash=f"{identity:064x}",
        open=Decimal(identity),
        close=Decimal(identity + 1),
    )


@given(st.permutations((0, 1, 2, 3)))
def test_selection_is_insertion_order_independent_and_excludes_future(
    order: list[int],
) -> None:
    candidates = (
        _candidate(10, source="alpaca", revision="1", available_delta=-20),
        _candidate(11, source="alpaca", revision="2", available_delta=-10),
        _candidate(12, source="alpaca", revision="3", available_delta=1),
        _candidate(13, source="fixture", revision="9", available_delta=-1),
    )

    selected = select_shadow_bar_candidate(
        tuple(candidates[index] for index in order), observed_at=_OBSERVED_AT
    )

    assert selected is not None
    assert selected.price_bar_record_id == UUID(int=11)
    assert selected.available_at <= _OBSERVED_AT


@given(st.integers(min_value=1, max_value=120))
def test_a_future_only_revision_is_never_selected(minutes_in_future: int) -> None:
    candidate = _candidate(
        20,
        source="alpaca",
        revision="future",
        available_delta=minutes_in_future,
    )

    assert (
        select_shadow_bar_candidate((candidate,), observed_at=_OBSERVED_AT) is None
    )


@given(st.permutations((1, 2, 3)))
def test_query_proof_hash_is_independent_of_member_order(order: list[int]) -> None:
    members = tuple(UUID(int=value) for value in order)
    instruments = tuple(UUID(int=value + 10) for value in reversed(order))

    digest = shadow_source_query_content_hash(
        shadow_group_id=UUID(int=99),
        session=_OBSERVED_AT.date(),
        kind="price_bars",
        source="alpaca",
        delivery_mode="delayed",
        instrument_ids=instruments,
        complete=True,
        member_ids=members,
        negative_instrument_ids=(),
    )
    canonical = shadow_source_query_content_hash(
        shadow_group_id=UUID(int=99),
        session=_OBSERVED_AT.date(),
        kind="price_bars",
        source="alpaca",
        delivery_mode="delayed",
        instrument_ids=tuple(sorted(instruments, key=lambda item: item.int)),
        complete=True,
        member_ids=tuple(sorted(members, key=lambda item: item.int)),
        negative_instrument_ids=(),
    )

    assert digest == canonical
