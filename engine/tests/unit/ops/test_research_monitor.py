"""Normal monitoring entry uses protected configured scope and database time."""

import pytest
from nexus.ops.research_monitor import parser, run
from nexus.settings import EngineSettings


def test_install_requires_exact_output_and_does_not_accept_clock_or_account_override():
    for args in (
        ["install"],
        ["install", "--output-id", "00000000-0000-0000-0000-000000000001", "--now", "2026-10-09"],
        ["install", "--output-id", "00000000-0000-0000-0000-000000000001", "--account-id", "x"],
    ):
        with pytest.raises(SystemExit):
            parser().parse_args(args)


@pytest.mark.asyncio
async def test_unconfigured_scope_rejected_before_connection(monkeypatch):
    from nexus.ops import research_monitor

    def forbidden(*_args, **_kwargs):
        raise AssertionError("unconfigured monitor reached database")

    monkeypatch.setattr(research_monitor, "session_factory", forbidden)
    args = parser().parse_args(["install", "--output-id", "00000000-0000-0000-0000-000000000001"])
    with pytest.raises(ValueError, match="MONITOR_ACCOUNT_UNCONFIGURED"):
        await run(
            EngineSettings(database_url="postgresql+asyncpg://localhost/nexus_test_monitor"), args
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "before_time,advance",
    [("15:05:12", 4), ("15:05:12", 288), ("15:05:12", -1), ("20:59:58", 4), ("14:29:58", 4)],
)
async def test_scan_uses_post_reconciliation_clock_without_moving_original_bucket(
    monkeypatch, before_time, advance
):
    from contextlib import asynccontextmanager
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from uuid import UUID

    from nexus.monitoring import reconciliation
    from nexus.ops import research_monitor
    from nexus.paper import candidate_batch

    before = datetime.fromisoformat(f"2026-11-09T{before_time}").replace(tzinfo=UTC)
    after = before + timedelta(seconds=advance)
    account, batch_id, instrument, calendar = (UUID(int=n) for n in (1, 2, 3, 4))
    clocks = iter((before, after))

    async def scalar(statement):
        if "clock_timestamp()" in str(statement):
            return next(clocks)
        return research_monitor.CURRENT_MIGRATION_HEAD

    session = SimpleNamespace(execute=AsyncMock(), scalar=scalar)

    @asynccontextmanager
    async def opened():
        yield session

    session.begin = opened

    class Factory:
        kw = {"bind": SimpleNamespace(dispose=AsyncMock())}

        def __call__(self):
            return opened()

    factory = Factory()
    monkeypatch.setattr(research_monitor, "session_factory", lambda _: factory)
    batch = SimpleNamespace(account_id=account, created_at=before, instrument_ids=(instrument,))
    monkeypatch.setattr(
        candidate_batch, "load_agent_paper_candidate_batch", AsyncMock(return_value=batch)
    )
    reconciled = AsyncMock(return_value={"recorded": 1})
    monkeypatch.setattr(reconciliation, "reconcile_account_results", reconciled)
    scanned = AsyncMock(return_value={"status": "scanned", "checks": 1})
    monkeypatch.setattr(research_monitor, "scan_account", scanned)
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
        agent_paper_account_id=account,
        agent_paper_candidate_batch_id=batch_id,
    )
    args = parser().parse_args(["scan", "--calendar-authority-id", str(calendar)])
    if advance < 0:
        with pytest.raises(ValueError, match="MONITOR_CLOCK_UNAVAILABLE"):
            await research_monitor.run(settings, args)
        scanned.assert_not_awaited()
    else:
        result = await research_monitor.run(settings, args)
        assert result["reconciled_results"] == 1
        scheduled = before.replace(minute=before.minute // 5 * 5, second=0)
        if after.replace(minute=after.minute // 5 * 5, second=0) == scheduled:
            assert scanned.await_args.kwargs["now"] == after
            assert result["status"] == "scanned"
        else:
            scanned.assert_not_awaited()
            assert result["status"] == "missed_bucket" and result["checks"] == 0
            assert result["scheduled_at"] == scheduled.isoformat()
            assert result["provider_requests"] == result["model_requests"] == 0
    assert reconciled.await_args.kwargs["now"] == before


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["missing_apply", "disabled", "calendar_changed", "bound"])
async def test_spending_entry_uses_applied_scope_and_honors_producer_stop(monkeypatch, mode):
    from uuid import UUID

    from nexus.ops import formal_budget_protection, research_monitor

    raw = {
        "DATABASE_URL": "postgresql+asyncpg://localhost/nexus_test_monitor",
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
        "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(UUID(int=1)),
    }
    calls = []

    async def bind(settings, environment):
        assert environment == raw
        if mode == "missing_apply":
            raise ValueError("FORMAL_BUDGET_APPLY_REQUIRED")
        return {
            **raw,
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(UUID(int=2)),
            "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(UUID(int=3)),
            "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID": str(UUID(int=4)),
            "AGENT_DAILY_BUDGET_USD": "1.25",
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "false" if mode == "disabled" else "true",
        }, str(UUID(int=99 if mode == "calendar_changed" else 5))

    async def run(settings, args):
        assert settings.agent_paper_account_id == UUID(int=2)
        assert settings.agent_paper_candidate_batch_id == UUID(int=3)
        assert settings.agent_paper_formal_model_config_id == UUID(int=4)
        assert str(settings.agent_daily_budget_usd) == "1.25"
        calls.append(True)
        return {"status": "admitted"}

    monkeypatch.setattr(formal_budget_protection, "bind_runtime_environment", bind)
    monkeypatch.setattr(research_monitor, "run", run)
    args = parser().parse_args(
        ["admit", "--trigger-id", str(UUID(int=8)), "--calendar-authority-id", str(UUID(int=5))]
    )
    if mode == "bound":
        assert await research_monitor.run_configured(raw, args) == {"status": "admitted"}
        assert calls == [True]
    else:
        code = {
            "missing_apply": "FORMAL_BUDGET_APPLY_REQUIRED",
            "disabled": "MONITOR_HOST_DISABLED",
            "calendar_changed": "MONITOR_HOST_SCOPE_CHANGED",
        }[mode]
        with pytest.raises(ValueError, match=code):
            await research_monitor.run_configured(raw, args)
        assert calls == []


def test_finish_requires_only_the_original_trigger_identity():
    args = parser().parse_args(["finish", "--trigger-id", "00000000-0000-0000-0000-000000000001"])
    assert args.action == "finish"
    with pytest.raises(SystemExit):
        parser().parse_args(["finish", "--trigger-id", str(args.trigger_id), "--accept-unknown"])


def test_collect_content_uses_existing_contact_config_and_exact_identity(monkeypatch):
    from uuid import UUID

    from nexus.ops import research_monitor

    args = research_monitor.parser().parse_args(
        [
            "collect-content",
            "--index-snapshot-id",
            "01000000-0000-7000-8000-000000000001",
            "--accession",
            "0000123456-26-000002",
            "--archive-dir",
            "/private/synthetic-archive",
        ]
    )
    assert args.index_snapshot_id == UUID("01000000-0000-7000-8000-000000000001")
    with pytest.raises(SystemExit):
        research_monitor.parser().parse_args(
            [
                "collect-content",
                "--index-snapshot-id",
                str(args.index_snapshot_id),
                "--accession",
                args.accession,
                "--archive-dir",
                str(args.archive_dir),
                "--account-id",
                "other",
            ]
        )
