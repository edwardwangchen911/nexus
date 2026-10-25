"""Pure skip/config/time boundaries for the current price observation producer."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from nexus.ops import monitor_price_refresh as refresh
from nexus.settings import EngineSettings


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["disabled", "legacy", "account", "time", "bucket"])
async def test_ineligible_host_does_not_open_database_or_source(monkeypatch, mode):
    account, model = uuid4(), uuid4()
    settings = EngineSettings(
        agent_paper_cycle_enabled=mode != "disabled",
        agent_paper_account_id=account,
        agent_paper_formal_model_config_id=None if mode == "legacy" else model,
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("ineligible host opened database")

    monkeypatch.setattr(refresh, "session_factory", forbidden)
    now = datetime(2026, 10, 9, 14, tzinfo=UTC)
    result = await refresh.refresh_price_observations(
        settings,
        account_id=uuid4() if mode == "account" else account,
        calendar_authority_id=uuid4(),
        scheduled_at=now.replace(tzinfo=None)
        if mode == "time"
        else now + timedelta(seconds=1)
        if mode == "bucket"
        else now,
        environment={},
        repo_root=Path("/tmp"),
    )
    assert result["status"] == ("disabled" if mode in {"disabled", "legacy"} else "blocked")
    assert result["provider_requests"] == result["model_requests"] == 0
    assert result["job_created"] is False and result["execution_locked"] is True


def test_explicit_missing_config_cannot_fall_back_to_ambient_secrets(monkeypatch, tmp_path):
    monkeypatch.setenv("NEXUS_SCHWAB_APP_KEY", "synthetic")
    monkeypatch.setenv("NEXUS_SCHWAB_APP_SECRET", "synthetic")
    monkeypatch.setenv("NEXUS_SCHWAB_CALLBACK_URL", "https://127.0.0.1")
    with pytest.raises(ValueError, match="AGENT_PAPER_SCHWAB_APP_CONFIG_INVALID"):
        refresh._configured(EngineSettings(), {}, tmp_path)


def test_incomplete_object_archive_configuration_is_blocked_before_capture(tmp_path):
    settings = EngineSettings(
        database_url="postgresql+asyncpg://synthetic:synthetic@127.0.0.1:55441/synthetic",
        object_store_endpoint="http://127.0.0.1:9000",
        object_store_access_key_id=None,
        object_store_secret_access_key=None,
    ).model_copy(
        update={"object_store_access_key_id": None, "object_store_secret_access_key": None}
    )
    with pytest.raises(ValueError, match="MONITOR_PRICE_ARCHIVE_CONFIG_UNAVAILABLE"):
        refresh._configured(
            settings,
            {
                "NEXUS_SCHWAB_APP_KEY": "synthetic",
                "NEXUS_SCHWAB_APP_SECRET": "synthetic",
                "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
            },
            tmp_path,
        )
