from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.monitoring import durable_sec_host as host
from nexus.monitoring import durable_sec_research as bridge
from nexus.ops import monitor_disclosure_refresh as refresh

ACCOUNT, INSTRUMENT, RULE, BATCH, CALENDAR = (UUID(int=i) for i in range(1, 6))
NOW = datetime(2026, 11, 17, 15, tzinfo=UTC)


@asynccontextmanager
async def context(*args, **kwargs):
    yield


class Session:
    def __init__(self, *, commit_failure=None):
        self.scalar = AsyncMock(return_value=UUID(int=8))
        self.commit_failure = commit_failure

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    @asynccontextmanager
    async def begin(self):
        yield
        if self.commit_failure:
            raise self.commit_failure


class Factory:
    def __init__(self, session):
        self.session = session
        self.kw = {"bind": SimpleNamespace(dispose=AsyncMock())}

    def __call__(self):
        return self.session


def setup(monkeypatch):
    session = Session()
    factory = Factory(session)
    monkeypatch.setattr("nexus.persistence.session.session_factory", lambda _: factory)
    monkeypatch.setattr("nexus.paper.account_transaction_turn.account_transaction_turn", context)
    monkeypatch.setattr("nexus.agents.research_maintenance.require_research_open", lambda: None)
    monkeypatch.setattr("nexus.paper.host_probe_state.require_host_admitting", AsyncMock())
    monkeypatch.setattr(refresh, "_scope", AsyncMock(return_value=(NOW, (INSTRUMENT,))))
    monkeypatch.setattr(host, "registered_rule", AsyncMock(return_value=SimpleNamespace(id=RULE)))
    monkeypatch.setattr(host, "_lock", AsyncMock())
    monkeypatch.setattr(host, "rule_state", AsyncMock(return_value="active"))
    producer = AsyncMock(return_value=SimpleNamespace(id=BATCH))
    monkeypatch.setattr(bridge, "prepare_durable_sec_research_batch", producer)
    settings = SimpleNamespace(
        agent_paper_cycle_enabled=True,
        agent_paper_formal_model_config_id=UUID(int=9),
        agent_paper_account_id=ACCOUNT,
    )
    return settings, session, factory, producer


async def invoke(settings):
    return await host.prepare_registered_research(
        settings, account_id=ACCOUNT, calendar_authority_id=CALENDAR, scheduled_at=NOW
    )


@pytest.mark.asyncio
async def test_normal_helper_reuses_producer_without_dispatch(monkeypatch):
    settings, _, factory, producer = setup(monkeypatch)
    result = await invoke(settings)
    assert result["items"] == [{"rule_id": str(RULE), "status": "sealed", "batch_id": str(BATCH)}]
    assert result["model_requests"] == result["provider_requests"] == 0
    assert result["job_created"] is False
    producer.assert_awaited_once()
    factory.kw["bind"].dispose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["paused", "revoked"])
async def test_inactive_never_falls_back(monkeypatch, state):
    settings, _, _, producer = setup(monkeypatch)
    monkeypatch.setattr(host, "rule_state", AsyncMock(return_value=state))
    assert (await invoke(settings))["items"][0]["status"] == "inactive"
    producer.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_earlier_saved_event_has_no_producer(monkeypatch):
    settings, session, _, producer = setup(monkeypatch)
    session.scalar.return_value = None
    assert (await invoke(settings))["items"] == []
    producer.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_plan_blocks_bridge_only(monkeypatch):
    settings, _, _, producer = setup(monkeypatch)
    producer.side_effect = ValueError("MONITOR_AWAITING_RECONCILIATION")
    assert (await invoke(settings))["items"][0]["status"] == "blocked"


@pytest.mark.asyncio
async def test_commit_failure_is_never_expected_material_gap(monkeypatch):
    settings, session, factory, producer = setup(monkeypatch)

    async def committed(*args, **kwargs):
        session.commit_failure = ValueError("MONITOR_AWAITING_RECONCILIATION")
        return SimpleNamespace(id=BATCH)

    producer.side_effect = committed
    with pytest.raises(ValueError, match="MONITOR_AWAITING_RECONCILIATION"):
        await invoke(settings)
    factory.kw["bind"].dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_producer_error_propagates(monkeypatch):
    settings, _, _, producer = setup(monkeypatch)
    producer.side_effect = ValueError("DURABLE_SEC_PENDING_BATCH_CONFLICT")
    with pytest.raises(ValueError, match="DURABLE_SEC_PENDING_BATCH_CONFLICT"):
        await invoke(settings)


@pytest.mark.asyncio
async def test_no_rule_preserves_legacy_path(monkeypatch):
    settings, _, _, producer = setup(monkeypatch)
    monkeypatch.setattr(host, "registered_rule", AsyncMock(return_value=None))
    assert (await invoke(settings))["items"] == []
    producer.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("reconciliation_fails", [False, True])
async def test_normal_host_preparation_bridges_after_sec_before_price(
    monkeypatch, tmp_path, reconciliation_fails
):
    from datetime import timedelta

    from nexus.ops import monitor_price_refresh, research_monitor
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

    settings, session, factory, _ = setup(monkeypatch)
    settings.database_url = "postgresql+asyncpg://test:test@127.0.0.1:56279/test"
    session.execute = AsyncMock()
    session.scalar.side_effect = [CURRENT_MIGRATION_HEAD, NOW]
    monkeypatch.setattr(research_monitor, "session_factory", lambda _: factory)
    monkeypatch.setattr(
        "nexus.paper.agent_cycle_authority.load_agent_paper_service_calendar_authority",
        AsyncMock(
            return_value=SimpleNamespace(
                market_open_at=NOW - timedelta(hours=1), market_close_at=NOW + timedelta(hours=1)
            )
        ),
    )
    calls = []

    async def reconcile(current, args):
        assert current is settings and args.action == "reconcile"
        calls.append("reconcile")
        if reconciliation_fails:
            raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
        return {"status": "reconciled", "recorded": 6}

    async def sec(*args, **kwargs):
        calls.append("sec")
        return {"status": "unchanged", "provider_requests": 0}

    async def prepare(*args, **kwargs):
        calls.append("bridge")
        return {"items": [{"status": "blocked", "reason": "MONITOR_AWAITING_RECONCILIATION"}]}

    async def price(*args, **kwargs):
        calls.append("price")
        return {"status": "refreshed", "provider_requests": 0}

    monkeypatch.setattr(refresh, "refresh_disclosure_observations", sec)
    monkeypatch.setattr(host, "prepare_registered_research", prepare)
    monkeypatch.setattr(monitor_price_refresh, "refresh_price_observations", price)
    monkeypatch.setattr(research_monitor, "run", reconcile)
    if reconciliation_fails:
        with pytest.raises(ValueError, match="MONITOR_HOST_SCOPE_CHANGED"):
            await research_monitor.refresh_configured_observations(
                settings,
                calendar_authority_id=CALENDAR,
                environment={},
                repo_root=tmp_path,
                scheduled_at=NOW,
            )
        assert calls == ["reconcile"]
        return
    result = await research_monitor.refresh_configured_observations(
        settings,
        calendar_authority_id=CALENDAR,
        environment={},
        repo_root=tmp_path,
        scheduled_at=NOW,
    )
    assert calls == ["reconcile", "sec", "bridge", "price"]
    assert result["reconciliation"] == {"status": "reconciled", "recorded": 6}
    assert result["status"] == "refreshed"
    assert result["sources"]["disclosure"]["status"] == "unchanged"
    assert result["sources"]["disclosure"]["research_bridge"]["items"][0]["status"] == "blocked"
