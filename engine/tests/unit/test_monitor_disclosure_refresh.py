import json
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.ops import monitor_disclosure_refresh as refresh
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


def test_reuse_budget_covers_default_host_preparation_and_scan():
    from inspect import signature

    from nexus.monitoring.host import PriceMonitorCoordinator
    from nexus.ops.monitor_disclosure_refresh import REUSE_MAX_AGE_SECONDS, TOTAL_TIMEOUT_SECONDS

    parameters = signature(PriceMonitorCoordinator).parameters
    preparation = parameters["preparation_timeout_seconds"].default
    scan = parameters["timeout_seconds"].default
    assert TOTAL_TIMEOUT_SECONDS <= preparation
    assert 0 < REUSE_MAX_AGE_SECONDS + preparation + scan <= 300


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_point,failure_code",
    [("scope", "TIMEOUT"), ("claim", "VALIDATION_REJECTED"), ("cleanup", "OS_ERROR")],
)
async def test_refresh_reports_the_fixed_failure_point(
    tmp_path, monkeypatch, capsys, failure_point, failure_code
):
    account = uuid4()
    bucket = datetime(2026, 10, 9, 14, tzinfo=UTC)
    settings = EngineSettings(
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=account,
        agent_paper_formal_model_config_id=uuid4(),
        database_url="postgresql+asyncpg://localhost/nexus_test_monitor",
        object_store_endpoint="http://127.0.0.1:9000",
    )
    secret = "private-provider-canary"
    instrument, identity, capture = uuid4(), uuid4(), uuid4()
    receipts = []

    async def dispose():
        if failure_point == "cleanup":
            raise OSError(secret)

    async def clock(_statement):
        return bucket

    class Factory:
        kw = {"bind": SimpleNamespace(dispose=dispose)}

        def __call__(self):
            return nullcontext(SimpleNamespace(begin=nullcontext, scalar=clock))

    async def scope(*_args, **_kwargs):
        if failure_point == "scope":
            raise TimeoutError(secret)
        return bucket, (instrument,)

    async def claim(*_args, **_kwargs):
        if failure_point == "claim":
            raise ValueError(secret)
        return {
            "archive_dir": str(tmp_path / "archive"),
            "identity_snapshot_id": str(identity),
            "issuer_cik": "0000320193",
        }

    async def collect(*_args, **_kwargs):
        return {"snapshot_id": str(capture), "provider_requests": 2}

    async def read_capture(*_args, **_kwargs):
        return (
            SimpleNamespace(id=capture, revision="test-hash"),
            SimpleNamespace(
                instrument_id=instrument,
                issuer_cik="0000320193",
                identity_snapshot_id=identity,
                received_at=bucket,
            ),
        )

    async def append_receipt(_session, *, action, payload, **_kwargs):
        receipts.append((action, payload["status"], payload["provider_requests"]))

    monkeypatch.setattr(refresh, "session_factory", lambda _settings: Factory())
    monkeypatch.setattr(refresh, "account_transaction_turn", lambda _settings: nullcontext())
    monkeypatch.setattr(refresh, "_scope", scope)
    monkeypatch.setattr(refresh, "_claim", claim)
    monkeypatch.setattr(refresh, "collect_index", collect)
    monkeypatch.setattr(refresh, "_capture", read_capture)
    monkeypatch.setattr(refresh, "configured_exhibit_store", lambda _session: None)
    monkeypatch.setattr(refresh, "append_audit_event", append_receipt)
    action = refresh_disclosure_observations(
        settings,
        account_id=account,
        calendar_authority_id=uuid4(),
        scheduled_at=bucket,
        environment={},
        repo_root=tmp_path,
    )
    if failure_point == "cleanup":
        with pytest.raises(OSError, match=secret):
            await action
        assert receipts == [(refresh.RESULT, "refreshed", 2)]
    else:
        result = await action
        assert result["status"] == "unknown"
        assert result["provider_requests"] == (None if failure_point == "scope" else 0)
        assert result["model_requests"] == 0 and result["job_created"] is False
        assert result["execution_locked"] is True
    observed = capsys.readouterr()
    assert secret not in observed.out + observed.err
    assert [json.loads(line) for line in observed.err.splitlines()] == [
        {
            "event": "research.monitor.disclosure_refresh_failed",
            "scheduled_at": bucket.isoformat(),
            "failure_point": failure_point,
            "failure_code": failure_code,
        }
    ]
