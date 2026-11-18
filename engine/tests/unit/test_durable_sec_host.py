from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.monitoring import durable_sec_host as host
from nexus.monitoring import scanning
from nexus.ops import monitor_disclosure_refresh as refresh
from nexus.research import durable_sec_index_storage as reader

NOW = datetime(2026, 10, 9, 14, tzinfo=UTC)
ACCOUNT, INSTRUMENT, IDENTITY = (UUID(int=i) for i in range(1, 4))


def rule():
    return SimpleNamespace(
        id=UUID(int=4),
        account_id=ACCOUNT,
        instrument_id=INSTRUMENT,
        issuer_cik="0000000001",
        effective_at=NOW - timedelta(days=10),
        created_at=NOW - timedelta(days=10),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [None, {"status": "paused"}, {"status": "already_claimed"}])
async def test_normal_refresh_registered_rule_never_reads_old_plan(monkeypatch, tmp_path, outcome):
    monkeypatch.setattr(refresh, "_scope", AsyncMock(return_value=(NOW, (INSTRUMENT,))))
    monkeypatch.setattr(refresh, "lock_security", AsyncMock())
    monkeypatch.setattr("nexus.paper.host_probe_state.require_host_admitting", AsyncMock())
    monkeypatch.setattr(host, "registered_rule", AsyncMock(return_value=rule()))
    claim = AsyncMock(return_value=outcome)
    monkeypatch.setattr(host, "claim_refresh", claim)
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=AssertionError("old plan must not be read"))
    )
    got = await refresh._claim(
        session,
        object(),
        account_id=ACCOUNT,
        calendar_id=UUID(int=5),
        instrument_id=INSTRUMENT,
        scheduled=NOW,
        repo_root=tmp_path,
        contact="test@example.invalid",
    )
    assert got == outcome
    claim.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["paused", "revoked"])
async def test_inactive_scope_never_reads_identity_or_collects(monkeypatch, tmp_path, state):
    monkeypatch.setattr(host, "_lock", AsyncMock())
    monkeypatch.setattr(host, "rule_state", AsyncMock(return_value=state))
    identity = AsyncMock(side_effect=AssertionError("must not collect"))
    monkeypatch.setattr(host, "current_identity", identity)
    result = await host.claim_refresh(
        object(),
        rule=rule(),
        now=NOW,
        scheduled=NOW,
        repo_root=tmp_path,
        contact="test@example.invalid",
    )
    assert result["status"] == "paused"
    identity.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("age,reuse", [(52, True), (53, False), (299, False)])
async def test_registered_reuse_reserves_full_prep_budget(monkeypatch, tmp_path, age, reuse):
    monkeypatch.setattr(host, "_lock", AsyncMock())
    monkeypatch.setattr(host, "rule_state", AsyncMock(return_value="active"))
    monkeypatch.setattr(host, "current_identity", AsyncMock(return_value=IDENTITY))
    row = SimpleNamespace(id=UUID(int=6))
    monkeypatch.setattr(host, "latest_capture", AsyncMock(return_value=row))
    index = SimpleNamespace(
        instrument_id=INSTRUMENT,
        issuer_cik=rule().issuer_cik,
        identity_snapshot_id=IDENTITY,
        received_at=NOW - timedelta(seconds=age),
        entries=(),
    )
    monkeypatch.setattr(
        reader,
        "read_durable_index_capture",
        AsyncMock(return_value=(row, SimpleNamespace(index=index))),
    )
    monkeypatch.setattr(host, "configured_exhibit_store", lambda session: object())
    audit = AsyncMock()
    monkeypatch.setattr(host, "append_audit_event", audit)
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[None, NOW]))
    result = await host.claim_refresh(
        session,
        rule=rule(),
        now=NOW,
        scheduled=NOW,
        repo_root=tmp_path,
        contact="test@example.invalid",
    )
    if reuse:
        assert result is None
        audit.assert_not_awaited()
    else:
        assert result["durable_sec"] is True and result["identity_snapshot_id"] == str(IDENTITY)
        assert result["feed"] == refresh.FEED
        audit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identity_version", "capture_version", "expected_reads"),
    [("v1", "v1", 1), ("v1", "v2", 2), (None, None, 2)],
)
async def test_scan_registered_reuses_only_exact_versioned_originals_within_one_check(
    monkeypatch, identity_version, capture_version, expected_reads
):
    source = SimpleNamespace(read_raw=AsyncMock(return_value=b"original"))
    session = SimpleNamespace(
        info={"sec_exhibit_store": source}, scalar=AsyncMock(return_value=None)
    )
    capture = SimpleNamespace(id=UUID(int=6))
    check = SimpleNamespace(observation={"events": []})
    monkeypatch.setattr(host, "_lock", AsyncMock())
    monkeypatch.setattr(host, "rule_state", AsyncMock(return_value="active"))
    monkeypatch.setattr(host, "latest_capture", AsyncMock(return_value=capture))

    async def read_original(version):
        uri = "s3://test/raw/identity"
        if version is not None:
            uri += f"?versionId={version}"
        store = host.configured_exhibit_store(session)
        assert await store.read_raw("raw/identity", uri=uri) == b"original"

    async def verify_identity(*args):
        await read_original(identity_version)
        return IDENTITY

    async def verify_capture(*args, **kwargs):
        assert kwargs["capture_id"] == capture.id
        await read_original(capture_version)
        return check

    identity = AsyncMock(side_effect=verify_identity)
    discovery = AsyncMock(side_effect=verify_capture)
    monkeypatch.setattr(host, "current_identity", identity)
    monkeypatch.setattr(host, "record_discovery", discovery)

    for attempt in (1, 2):
        assert await host.scan_registered(session, rule(), NOW) == (check, 0)
        assert source.read_raw.await_count == attempt * expected_reads
        assert session.info["sec_exhibit_store"] is source
        assert identity.await_count == discovery.await_count == attempt


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("has_plan", [False, True])
async def test_normal_scan_expired_anchor_uses_durable_only(monkeypatch, active, has_plan):
    selected = SimpleNamespace(instrument_id=INSTRUMENT, valid_until=NOW - timedelta(days=1))
    session = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(capability="simulation_write_internal")),
        scalars=AsyncMock(side_effect=[[selected] if has_plan else [], [INSTRUMENT]]),
        scalar=AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        scanning,
        "load_agent_paper_service_calendar_authority",
        AsyncMock(
            return_value=SimpleNamespace(
                market_open_at=NOW - timedelta(hours=1), market_close_at=NOW + timedelta(hours=6)
            )
        ),
    )
    monkeypatch.setattr(scanning, "lock_security", AsyncMock())
    monkeypatch.setattr(host, "registered_rule", AsyncMock(return_value=rule()))
    checked = SimpleNamespace(result="true") if active else None
    scan = AsyncMock(return_value=(checked, 1 if active else 0))
    monkeypatch.setattr(host, "scan_registered", scan)
    report = await scanning.scan_account(
        session,
        account_id=ACCOUNT,
        calendar_authority_id=UUID(int=5),
        now=NOW,
        instrument_ids=(INSTRUMENT,),
    )
    assert report["checks"] == int(active) and report["notifications"] == int(active)
    assert report["expired"] == 0
    scan.assert_awaited_once()


@pytest.mark.asyncio
async def test_registered_and_plan_scope_union_still_limits_five_before_lock(monkeypatch):
    plans = [SimpleNamespace(instrument_id=UUID(int=20 + i)) for i in range(5)]
    session = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(capability="simulation_write_internal")),
        scalars=AsyncMock(side_effect=[plans, [UUID(int=99)]]),
    )
    monkeypatch.setattr(
        scanning,
        "load_agent_paper_service_calendar_authority",
        AsyncMock(
            return_value=SimpleNamespace(
                market_open_at=NOW - timedelta(hours=1), market_close_at=NOW + timedelta(hours=1)
            )
        ),
    )
    lock = AsyncMock()
    monkeypatch.setattr(scanning, "lock_security", lock)
    with pytest.raises(ValueError, match="MONITOR_SCOPE_EXCEEDED"):
        await scanning.scan_account(
            session, account_id=ACCOUNT, calendar_authority_id=UUID(int=5), now=NOW
        )
    lock.assert_not_awaited()
