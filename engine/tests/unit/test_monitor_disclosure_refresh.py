from datetime import UTC, datetime
from uuid import uuid4

import pytest
from nexus.ops.monitor_disclosure_refresh import refresh_disclosure_observations
from nexus.settings import EngineSettings


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled,formal", [(False, True), (True, False)])
async def test_disabled_or_legacy_has_no_database_or_provider_io(tmp_path, enabled, formal):
    account = uuid4()
    settings = EngineSettings().model_copy(
        update={
            "agent_paper_cycle_enabled": enabled,
            "agent_paper_account_id": account,
            "agent_paper_formal_model_config_id": uuid4() if formal else None,
        }
    )
    result = await refresh_disclosure_observations(
        settings,
        account_id=account,
        calendar_authority_id=uuid4(),
        scheduled_at=datetime(2026, 10, 9, 14, tzinfo=UTC),
        environment={},
        repo_root=tmp_path,
    )
    assert result["status"] == "disabled"
    assert result["provider_requests"] == result["model_requests"] == 0
    assert result["execution_locked"] is True
    assert not tuple(tmp_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["account", "bucket", "store"])
async def test_invalid_scope_rejected_before_io(tmp_path, fault):
    account = uuid4()
    settings = EngineSettings().model_copy(
        update={
            "agent_paper_cycle_enabled": True,
            "agent_paper_account_id": account,
            "agent_paper_formal_model_config_id": uuid4(),
            "database_url": "postgresql+asyncpg://synthetic:synthetic@127.0.0.1:55440/unused",
            "object_store_endpoint": "https://unapproved.invalid",
        }
    )
    result = await refresh_disclosure_observations(
        settings,
        account_id=uuid4() if fault == "account" else account,
        calendar_authority_id=uuid4(),
        scheduled_at=datetime(2026, 10, 9, 14, 1 if fault == "bucket" else 0, tzinfo=UTC),
        environment={"SEC_CONTACT": "test@example.invalid"},
        repo_root=tmp_path,
    )
    assert result["status"] == "blocked"
    assert result["provider_requests"] == result["model_requests"] == 0
    assert (
        result["reason"]
        == {
            "account": "MONITOR_SCOPE_INVALID",
            "bucket": "MONITOR_REFRESH_BUCKET_UNAVAILABLE",
            "store": "SEC_INDEX_LOCAL_STORE_REQUIRED",
        }[fault]
    )
    assert not tuple(tmp_path.iterdir())


@pytest.mark.parametrize(
    "receipt,extra,expected",
    [
        (None, None, None),
        ("history-result.json", 2, 4),
        ("history-failure.json", 1, 3),
        ("history-failure.json", 3, None),
    ],
)
def test_listed_request_count_requires_exact_separate_attempt_receipt(
    tmp_path, receipt, extra, expected
):
    import json

    from nexus.ops.monitor_disclosure_refresh import _request_count

    (tmp_path / "result.json").write_text(json.dumps({"requests": 2}))
    if receipt:
        (tmp_path / receipt).write_text(json.dumps({"requests": extra}))
    assert _request_count(tmp_path, listed_history=True) == expected
    assert _request_count(tmp_path) == 2
