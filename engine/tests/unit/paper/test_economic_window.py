"""Account ownership gate; no fabricated execution evidence or real provider calls."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.paper.economic_window import require_economic_consumption


@pytest.mark.asyncio
@pytest.mark.parametrize("registered", [False, True])
async def test_no_cycle_is_ordinary_only_for_unregistered_account(registered):
    order_id, decision_id, account_id = uuid4(), uuid4(), uuid4()
    order = SimpleNamespace(strategy_decision_id=decision_id, account_id=account_id)
    decision = SimpleNamespace(agent_paper_cycle_id=None)

    class Session:
        async def get(self, model, key):
            assert key in {order_id, decision_id}
            return order if key == order_id else decision

        async def scalar(self, query):
            # The ownership lookup must happen even for a decision without cycle.
            assert "economic_evaluations" in str(query)
            return uuid4() if registered else None

    if registered:
        with pytest.raises(ValueError, match="ECONOMIC_PAIR_CYCLE_NOT_BOUND"):
            await require_economic_consumption(
                Session(),
                order_id=order_id,
                evaluated_at=datetime(2026, 10, 9, 14, tzinfo=UTC),
                market_window_id=None,
            )
    else:
        await require_economic_consumption(
            Session(),
            order_id=order_id,
            evaluated_at=datetime(2026, 10, 9, 14, tzinfo=UTC),
            market_window_id=None,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "archive_case,code",
    [
        ("complete_looking", "ECONOMIC_WINDOW_ARCHIVES_REQUIRED"),
        ("missing", "ECONOMIC_WINDOW_ARCHIVES_REQUIRED"),
        ("conflicting", "ECONOMIC_WINDOW_ARCHIVE_CONFLICT"),
    ],
)
async def test_bindings_never_grant_execution_before_shared_raw_validation(
    monkeypatch, archive_case, code
):
    import nexus.paper.economic_window as windows
    from nexus.paper.errors import PaperExecutionError

    order_id, decision_id, account_id, cycle_id, pair_id, iid = (uuid4() for _ in range(6))
    selected_id, artifact_id = uuid4(), uuid4()
    now = datetime(2026, 10, 9, 14, 10, tzinfo=UTC)
    order = SimpleNamespace(
        id=order_id, strategy_decision_id=decision_id, account_id=account_id, instrument_id=iid
    )
    decision = SimpleNamespace(agent_paper_cycle_id=cycle_id)
    bindings = [
        SimpleNamespace(order_id=order_id, instrument_id=iid),
        SimpleNamespace(order_id=uuid4(), instrument_id=iid),
    ]

    async def owner(*args, **kwargs):
        return SimpleNamespace(pair_slot_id=pair_id), None

    async def scope(*args, **kwargs):
        return SimpleNamespace(window_start=now, window_end=now)

    async def reject_unproven(*args, **kwargs):
        raise ValueError("ECONOMIC_WINDOW_ARCHIVES_REQUIRED")

    monkeypatch.setattr(windows, "_qualify_economic_consumption", reject_unproven)
    monkeypatch.setattr(windows, "read_economic_cycle", owner)
    monkeypatch.setattr(windows, "economic_market_scope", scope)

    class Session:
        calls = 0

        async def get(self, model, key):
            return order if key == order_id else decision

        async def scalar(self, query):
            self.calls += 1
            if self.calls == 1:
                return uuid4()  # registered account
            if self.calls == 2:
                return SimpleNamespace(id=uuid4())
            if archive_case == "missing":
                return None
            return SimpleNamespace(
                id=selected_id,
                evaluated_at=now,
                data_available_at=now,
                raw_artifact_id=uuid4() if archive_case == "conflicting" else artifact_id,
                raw_content_hash="a" * 64,
                source_content_hash="b" * 64,
            )

        async def scalars(self, query):
            return bindings

    with pytest.raises(PaperExecutionError) as error:
        await windows.require_economic_consumption(
            Session(), order_id=order_id, evaluated_at=now, market_window_id=selected_id
        )
    assert error.value.code == code
    assert str(account_id) not in error.value.safe_message
