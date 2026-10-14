import json
from types import SimpleNamespace

import pytest
from nexus.ops import manual_paper


@pytest.mark.parametrize("file_revision", [None, "1" * 40])
def test_normal_cli_binds_host_version_to_requested_repo_head(
    tmp_path, monkeypatch, capsys, file_revision
):
    calls = []
    observed = []
    revision = "2" * 40
    environment = {} if file_revision is None else {"NEXUS_RELEASE_REVISION": file_revision}
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(
        manual_paper, "load_agent_paper_service_environment", lambda *a, **k: environment.copy()
    )

    def checkout(root):
        calls.append(root)
        return revision

    monkeypatch.setattr(manual_paper, "_checkout_revision", checkout, raising=False)

    async def run(settings, args):
        observed.append(settings.release_revision)
        return SimpleNamespace(model_dump_json=lambda: '{"status":"fixture"}')

    monkeypatch.setattr(manual_paper, "run", run)
    assert (
        manual_paper.main(
            [
                "--repo-root",
                str(tmp_path),
                "start",
                "--mode",
                "research_and_paper",
                "--idempotency-key",
                "fixture",
            ]
        )
        == 0
    )
    assert calls == [tmp_path.resolve()]
    assert observed == [revision]
    assert json.loads(capsys.readouterr().out) == {"status": "fixture"}


def test_missing_checkout_revision_does_not_trust_file_but_keeps_receipt_read(
    tmp_path, monkeypatch
):
    observed = []
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(
        manual_paper,
        "load_agent_paper_service_environment",
        lambda *a, **k: {"NEXUS_RELEASE_REVISION": "1" * 40},
    )
    monkeypatch.setattr(manual_paper, "_checkout_revision", lambda root: None, raising=False)

    async def run(settings, args):
        observed.append(settings.release_revision)
        assert args.action == "read"
        return SimpleNamespace(model_dump_json=lambda: '{"status":"fixture"}')

    monkeypatch.setattr(manual_paper, "run", run)
    assert (
        manual_paper.main(
            [
                "--repo-root",
                str(tmp_path),
                "read",
                "--request-id",
                "00000000-0000-0000-0000-000000000001",
            ]
        )
        == 0
    )
    assert observed == [None]


@pytest.mark.asyncio
@pytest.mark.parametrize("host", ["missing", "wrong-version"])
@pytest.mark.parametrize("action", ["cancel-unused-research"])
async def test_cancel_entry_rejects_bad_host_before_recovery(monkeypatch, host, action):
    from contextlib import asynccontextmanager
    from datetime import UTC, datetime, timedelta
    from unittest.mock import AsyncMock
    from uuid import UUID

    from nexus.paper.manual_entry import MANUAL_HOST_PROTOCOL, manual_host_scope
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
    from nexus.settings import EngineSettings

    now = datetime(2026, 10, 12, 14, tzinfo=UTC)
    settings = EngineSettings.for_test().model_copy(
        update={
            "agent_paper_account_id": UUID(int=1),
            "release_revision": "a" * 40,
        }
    )
    row = SimpleNamespace(
        worker_id=f"agent-paper-loop-{UUID(int=2)}:manual:{manual_host_scope(settings)}",
        seen_at=now,
        started_at=now - timedelta(minutes=1),
        process_version=f"{MANUAL_HOST_PROTOCOL}+{'b' * 40}",
        migration_head=CURRENT_MIGRATION_HEAD,
    )
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=now),
        scalars=AsyncMock(
            return_value=SimpleNamespace(all=lambda: [] if host == "missing" else [row])
        ),
    )

    @asynccontextmanager
    async def begin():
        yield session

    factory = SimpleNamespace(begin=begin, kw={"bind": SimpleNamespace(dispose=AsyncMock())})
    monkeypatch.setattr(manual_paper, "_validate_local_database", lambda url: None)
    monkeypatch.setattr(manual_paper, "session_factory", lambda config: factory)
    recover = AsyncMock()
    monkeypatch.setattr(manual_paper, "close_terminal_cycle_unused_research", recover)
    monkeypatch.setattr(manual_paper, "seal_failed_research", recover)
    with pytest.raises(ValueError, match="MANUAL_HOST_NOT_READY"):
        await manual_paper.run(settings, SimpleNamespace(action=action, cycle_id=UUID(int=3)))
    recover.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", ["current", "old", "missing"])
async def test_seal_maintenance_uses_actual_schema_without_dispatch_heartbeat(monkeypatch, schema):
    from contextlib import asynccontextmanager
    from datetime import UTC, datetime
    from pathlib import Path
    from unittest.mock import AsyncMock
    from uuid import UUID

    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
    from nexus.settings import EngineSettings

    now = datetime(2026, 10, 14, tzinfo=UTC)
    settings = EngineSettings.for_test().model_copy(
        update={"agent_paper_account_id": UUID(int=1), "release_revision": "a" * 40}
    )
    session = SimpleNamespace(
        scalar=AsyncMock(
            side_effect=[
                now,
                CURRENT_MIGRATION_HEAD
                if schema == "current"
                else ("old" if schema == "old" else None),
            ]
        ),
        execute=AsyncMock(),
        scalars=AsyncMock(side_effect=AssertionError("maintenance must not require live host")),
    )

    @asynccontextmanager
    async def begin():
        yield session

    factory = SimpleNamespace(begin=begin, kw={"bind": SimpleNamespace(dispose=AsyncMock())})
    monkeypatch.setattr(manual_paper, "session_factory", lambda config: factory)
    monkeypatch.setattr(manual_paper, "_validate_local_database", lambda url: None)
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *a: None, raising=False)
    recover = AsyncMock(return_value="sealed")
    monkeypatch.setattr(manual_paper, "seal_failed_research", recover)
    args = SimpleNamespace(
        action="seal-failed-research",
        repo_root=Path.cwd(),
        cycle_id=UUID(int=3),
        run_ids=(UUID(int=4),),
        accept_unknown_usage=True,
    )
    if schema == "current":
        assert await manual_paper.run(settings, args) == "sealed"
    else:
        with pytest.raises(ValueError, match="MANUAL_CONFIGURATION_UNAVAILABLE"):
            await manual_paper.run(settings, args)
        recover.assert_not_awaited()


@pytest.mark.parametrize("fault", [None, "wrong-root", "revision", "source", "inspection-source"])
def test_maintenance_code_binds_loaded_sources_to_exact_checkout(monkeypatch, tmp_path, fault):
    import subprocess
    from pathlib import Path

    root = Path(manual_paper.__file__).resolve().parents[4]
    revision = "a" * 40
    monkeypatch.setattr(manual_paper, "_checkout_revision", lambda path: revision)

    def git(command, **kwargs):
        relative = command[-1].split(":", 1)[1]
        content = (root / relative).read_bytes()
        if fault == "source" and relative.endswith("ops/manual_paper.py"):
            content += b"changed"
        if fault == "inspection-source" and relative.endswith(
            "ops/research_workload_inspection.py"
        ):
            content += b"changed"
        return subprocess.CompletedProcess(command, 0, content)

    monkeypatch.setattr(
        manual_paper,
        "subprocess",
        SimpleNamespace(run=git, SubprocessError=subprocess.SubprocessError),
        raising=False,
    )
    args = (
        tmp_path if fault == "wrong-root" else root,
        "b" * 40 if fault == "revision" else revision,
    )
    if fault is None:
        manual_paper._require_maintenance_code(*args)
    else:
        with pytest.raises(ValueError, match="MANUAL_CONFIGURATION_UNAVAILABLE"):
            manual_paper._require_maintenance_code(*args)


@pytest.mark.parametrize("missing", ["--run-id", "--accept-unknown-usage"])
def test_history_seal_cli_requires_exact_run_and_explicit_unknown_acceptance(missing):
    arguments = [
        "seal-failed-research",
        "--cycle-id",
        "00000000-0000-0000-0000-000000000001",
        "--run-id",
        "00000000-0000-0000-0000-000000000002",
        "--accept-unknown-usage",
    ]
    index = arguments.index(missing)
    del arguments[index : index + (2 if missing == "--run-id" else 1)]
    with pytest.raises(SystemExit):
        manual_paper.parser().parse_args(arguments)


def test_same_day_closure_requires_explicit_option_and_keeps_default():
    arguments = [
        "seal-failed-research",
        "--cycle-id",
        "00000000-0000-0000-0000-000000000001",
        "--run-id",
        "00000000-0000-0000-0000-000000000002",
        "--accept-unknown-usage",
    ]
    assert manual_paper.parser().parse_args(arguments).same_day is False
    assert manual_paper.parser().parse_args([*arguments, "--same-day"]).same_day is True
