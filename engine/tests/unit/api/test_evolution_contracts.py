"""Closed browser-facing contracts for the strategy-evolution API."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.api.routes.evolution import parse_registry_etag
from nexus.api.schemas.evolution import (
    EvolutionCursorCodec,
    EvolutionCursorState,
    RegistryControlRequest,
    RollbackRequest,
    StartExperimentRequest,
    VetoExperimentRequest,
)
from pydantic import ValidationError

_NOW = datetime(2026, 8, 27, 12, tzinfo=UTC)
_EXPERIMENT_ID = UUID("019d0000-0000-7000-8000-000000009901")


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (StartExperimentRequest, {"strategy": {"version": "escape"}}),
        (VetoExperimentRequest, {"reason": "USER_VETO", "prompt": "escape"}),
        (RegistryControlRequest, {"action": "promote"}),
        (RollbackRequest, {"reason": "arbitrary"}),
    ],
)
def test_write_contracts_reject_unbounded_or_unknown_input(model, payload) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(payload)


def test_write_contracts_accept_only_closed_actions() -> None:
    assert StartExperimentRequest.model_validate({}).model_dump() == {}
    assert VetoExperimentRequest(reason="USER_VETO").reason == "USER_VETO"
    assert RegistryControlRequest(action="pause_auto_promotion").action == (
        "pause_auto_promotion"
    )
    assert RollbackRequest(reason="OPERATIONAL_RISK").reason == "OPERATIONAL_RISK"


def test_registry_etag_binds_generation_and_projection_version() -> None:
    assert parse_registry_etag('"g7-v11"') == (7, 11)
    for invalid in ('7', '"7"', '"g0-v1"', '"g1-v0"', 'W/"g1-v1"', '"g01-v1"'):
        with pytest.raises(ValueError):
            parse_registry_etag(invalid)


def test_evolution_cursor_is_signed_scoped_and_short_lived() -> None:
    state = EvolutionCursorState(
        decision_time=_NOW,
        after_created_at=datetime(2026, 8, 27, 10, tzinfo=UTC),
        after_experiment_id=_EXPERIMENT_ID,
    )
    encoded = EvolutionCursorCodec(("current-secret",), _NOW).encode(state)
    assert EvolutionCursorCodec(("current-secret",), _NOW).decode(encoded) == state

    with pytest.raises(ValueError, match="cursor"):
        EvolutionCursorCodec(("different-secret",), _NOW).decode(encoded)
    with pytest.raises(ValueError, match="cursor"):
        EvolutionCursorCodec(
            ("current-secret",), datetime(2026, 8, 27, 12, 6, tzinfo=UTC)
        ).decode(encoded)

