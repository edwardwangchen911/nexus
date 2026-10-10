"""Runtime release identity stays canonical and visible without exposing configuration."""

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.settings import EngineSettings
from pydantic import ValidationError

_REVISION = "a" * 40


def test_engine_settings_accepts_only_canonical_release_revisions() -> None:
    settings = EngineSettings.model_validate({"NEXUS_RELEASE_REVISION": _REVISION})

    assert settings.release_revision == _REVISION
    with pytest.raises(ValidationError):
        EngineSettings.model_validate({"NEXUS_RELEASE_REVISION": "A" * 40})
    with pytest.raises(ValidationError):
        EngineSettings.model_validate({"NEXUS_RELEASE_REVISION": "abc123"})


def test_agent_paper_process_version_carries_the_release_revision() -> None:
    assert loop.agent_paper_process_version(_REVISION) == (
        f"{loop.AGENT_PAPER_LOOP_PROCESS_VERSION}+{_REVISION}"
    )
    assert loop.agent_paper_process_version(None) == loop.AGENT_PAPER_LOOP_PROCESS_VERSION


def test_agent_paper_process_version_parser_preserves_legacy_unknown() -> None:
    assert loop.parse_agent_paper_process_version(loop.AGENT_PAPER_LOOP_PROCESS_VERSION) == (
        loop.AGENT_PAPER_LOOP_PROCESS_VERSION,
        None,
    )
    assert loop.parse_agent_paper_process_version(
        f"{loop.AGENT_PAPER_LOOP_PROCESS_VERSION}+{_REVISION}"
    ) == (loop.AGENT_PAPER_LOOP_PROCESS_VERSION, _REVISION)
    assert loop.parse_agent_paper_process_version("agent-paper-loop-v2+invalid") == (None, None)


def test_manual_host_identity_is_admitted_only_for_same_process_and_scope() -> None:
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from uuid import UUID

    from nexus.paper.manual_entry import manual_host_scope, require_manual_host

    settings = EngineSettings(
        agent_runtime_mode="codex_sdk",
        release_revision=_REVISION,
        agent_paper_cycle_enabled=True,
        agent_paper_manual_enabled=True,
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=3),
    )
    worker = f"agent-paper-loop-{UUID(int=4)}"
    worker_id, version = loop.agent_paper_host_identity(
        settings, worker_id=worker, account_id=UUID(int=1)
    )
    now = datetime(2026, 10, 10, 14, tzinfo=UTC)
    row = SimpleNamespace(
        worker_id=worker_id,
        process_version=version,
        migration_head=loop.CURRENT_MIGRATION_HEAD,
        started_at=now,
        seen_at=now,
    )
    require_manual_host(
        [row], now=now, release_revision=_REVISION, scope_hash=manual_host_scope(settings)
    )
    result = loop._classify_agent_paper_service_heartbeat(now, row)
    assert result["status"] == "fresh"
    assert result["release_revision"] == _REVISION
    assert "worker_id" not in result
    changed = settings.model_copy(update={"agent_paper_candidate_batch_id": UUID(int=5)})
    with pytest.raises(ValueError, match="MANUAL_HOST_NOT_READY"):
        require_manual_host(
            [row], now=now, release_revision=_REVISION, scope_hash=manual_host_scope(changed)
        )
    with pytest.raises(ValueError, match="MANUAL_HOST_NOT_READY"):
        require_manual_host(
            [row], now=now, release_revision="b" * 40, scope_hash=manual_host_scope(settings)
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"agent_runtime_mode": "offline_fixture"},
        {"agent_paper_manual_enabled": False},
        {"agent_paper_cycle_enabled": False},
        {"formal_budget_protection_enabled": True},
        {"agent_paper_candidate_batch_id": None},
        {"agent_paper_formal_model_config_id": None},
        {"agent_paper_account_id": None},
        {"release_revision": None},
    ],
)
def test_manual_host_identity_never_advertises_unconsumable_scope(changes) -> None:
    from uuid import UUID

    settings = EngineSettings(
        agent_runtime_mode="codex_sdk",
        release_revision=_REVISION,
        agent_paper_cycle_enabled=True,
        agent_paper_manual_enabled=True,
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=3),
    ).model_copy(update=changes)
    worker = f"agent-paper-loop-{UUID(int=4)}"
    identity = loop.agent_paper_host_identity(settings, worker_id=worker, account_id=UUID(int=1))
    assert identity == (worker, loop.agent_paper_process_version(settings.release_revision))


def test_manual_host_identity_rejects_account_mismatch_and_protocol_worker_mismatch() -> None:
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from uuid import UUID

    settings = EngineSettings(
        agent_runtime_mode="codex_sdk",
        release_revision=_REVISION,
        agent_paper_cycle_enabled=True,
        agent_paper_manual_enabled=True,
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=3),
    )
    worker = f"agent-paper-loop-{UUID(int=4)}"
    assert loop.agent_paper_host_identity(settings, worker_id=worker, account_id=UUID(int=8)) == (
        worker,
        loop.agent_paper_process_version(_REVISION),
    )
    now = datetime(2026, 10, 10, 14, tzinfo=UTC)
    for worker_id, version in [
        (worker, f"nexus-manual-paper-v1+{_REVISION}"),
        (worker + ":manual:" + "c" * 64, loop.agent_paper_process_version(_REVISION)),
        (worker + ":manual:invalid", f"nexus-manual-paper-v1+{_REVISION}"),
    ]:
        row = SimpleNamespace(
            worker_id=worker_id,
            process_version=version,
            migration_head=loop.CURRENT_MIGRATION_HEAD,
            started_at=now,
            seen_at=now,
        )
        assert loop._classify_agent_paper_service_heartbeat(now, row)["status"] != "fresh"
