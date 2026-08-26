"""One-session Proposal Shadow orchestration stays static and atomic."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.shadow.authority import BoundShadowBar, ShadowSessionInput
from nexus.shadow.service import simulate_shadow_portfolio_session
from nexus.simulation.costs import CostModel
from nexus.simulation.models import SimulationInstrumentState, SimulationState, TargetWeight


def _authority(ordinal: int) -> ShadowSessionInput:
    session_date = date(2026, 1, 5 + ordinal - 1)
    bar = BoundShadowBar(
        price_bar_record_id=UUID(int=100 + ordinal),
        price_bar_set_authority_id=UUID(int=200 + ordinal),
        instrument_id=UUID(int=1),
        usage="execution_and_mark" if ordinal == 1 else "mark",
        source="fixture",
        delivery_mode="fixture",
        revision="1",
        available_at=datetime(2026, 1, 5 + ordinal - 1, 21, tzinfo=UTC),
        content_hash="a" * 64,
        open=Decimal("10"),
        close=Decimal("11"),
    )
    return ShadowSessionInput(
        status="complete",
        reason=None,
        session=session_date,
        ordinal=ordinal,
        observed_at=datetime(2026, 1, 5 + ordinal - 1, 22, tzinfo=UTC),
        late_backfill=False,
        authority_hash="b" * 64,
        bars=(bar,),
        missing_execution_inputs=(),
        events=(),
        source_summary=(),
        delivery_mode_summary=("fixture",),
        maximum_available_at=bar.available_at,
    )


def test_ordinal_one_executes_frozen_targets_once_per_variant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nexus.shadow import service

    calls = 0
    real_execute = service.execute_target_weights

    def counted_execute(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        return real_execute(*args, **kwargs)

    monkeypatch.setattr(service, "execute_target_weights", counted_execute)
    authority = _authority(1)
    for _ in range(3):
        result = simulate_shadow_portfolio_session(
            previous_state=SimulationState(cash=Decimal("100"), instruments=(), receivables=()),
            authority=authority,
            targets=(TargetWeight(instrument_id=UUID(int=1), weight=Decimal("0.5")),),
            cost_model=CostModel.zero(),
            signal_session=date(2026, 1, 2),
        )
        assert result.authority_hash == authority.authority_hash
        assert len(result.outcomes) == len(result.fills) == 1

    assert calls == 3


def test_ordinal_two_cannot_accept_targets_or_execute(monkeypatch: pytest.MonkeyPatch) -> None:
    from nexus.shadow import service

    def forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("ordinal 2 must not execute target weights")

    monkeypatch.setattr(service, "execute_target_weights", forbidden)
    previous = SimulationState(
        cash=Decimal("50"),
        instruments=(
            SimulationInstrumentState(
                instrument_id=UUID(int=1),
                quantity=Decimal("5"),
                cost_basis=Decimal("50"),
                realized_pnl=Decimal("0"),
                cumulative_cost=Decimal("0"),
                last_close=Decimal("10"),
                untradable=False,
            ),
        ),
        receivables=(),
    )

    result = simulate_shadow_portfolio_session(
        previous_state=previous,
        authority=_authority(2),
        targets=None,
        cost_model=CostModel.zero(),
        signal_session=date(2026, 1, 2),
    )

    assert result.outcomes == result.fills == ()
    with pytest.raises(ValueError, match="TARGETS_FORBIDDEN_AFTER_INITIAL_SESSION"):
        simulate_shadow_portfolio_session(
            previous_state=previous,
            authority=_authority(2),
            targets=(),
            cost_model=CostModel.zero(),
            signal_session=date(2026, 1, 2),
        )
