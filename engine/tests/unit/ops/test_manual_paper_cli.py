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


def test_unstarted_preparation_closure_is_explicit_and_request_scoped():
    args = manual_paper.parser().parse_args(
        ["close-unstarted-preparation", "--request-id", "00000000-0000-0000-0000-000000000003"]
    )
    assert args.action == "close-unstarted-preparation"
    assert str(args.request_id).endswith("0003")


@pytest.mark.parametrize(
    "action",
    [
        "close-unstarted-preparation",
        "acknowledge-unknown-preparations",
        "check-preparation-acceptance",
    ],
)
@pytest.mark.asyncio
async def test_preparation_maintenance_holds_lifecycle_fence_through_commit(
    monkeypatch, tmp_path, action
):
    from argparse import Namespace
    from contextlib import contextmanager
    from datetime import UTC, datetime
    from uuid import UUID

    from nexus.paper import manual_preparation_close as close
    from nexus.settings import EngineSettings

    observed = []
    held = [False]
    code_checks = []

    @contextmanager
    def fence(*_args):
        held[0] = True
        observed.append("lock")
        try:
            yield
        finally:
            observed.append("unlock")
            held[0] = False

    class Transaction:
        async def __aenter__(self):
            assert held[0]
            return self

        async def __aexit__(self, *_args):
            assert held[0]
            observed.append("commit")

        async def scalar(self, query):
            if "clock_timestamp" in str(query):
                return datetime(2026, 10, 14, 18, tzinfo=UTC)
            return manual_paper.CURRENT_MIGRATION_HEAD

        async def execute(self, *_a, **_k):
            assert held[0]

    class Engine:
        async def dispose(self):
            observed.append("dispose")

    factory = SimpleNamespace(begin=Transaction, kw={"bind": Engine()})

    async def close_request(*_a, **_k):
        assert held[0]
        if action == "check-preparation-acceptance":
            return manual_paper.PreparationAcceptanceCheck(
                request_id=UUID(int=1),
                status="eligible_at_check",
                checked_at=datetime(2026, 10, 14, 18, tzinfo=UTC),
            )
        if action == "acknowledge-unknown-preparations":
            from nexus.paper.preparation_unknown_ack import UnknownPreparationReceipt

            return UnknownPreparationReceipt(request_id=UUID(int=1), replayed=False)
        return close.PreparationCloseReceipt(request_id=UUID(int=1), replayed=False)

    monkeypatch.setattr(manual_paper, "session_factory", lambda *_a: factory)
    monkeypatch.setattr(
        manual_paper, "_require_maintenance_code", lambda *_a: code_checks.append(True)
    )
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", fence)
    monkeypatch.setattr(close, "close_unstarted_preparation", close_request)
    monkeypatch.setattr(manual_paper, "check_current_unknown", close_request)
    from nexus.paper import preparation_unknown_ack

    monkeypatch.setattr(preparation_unknown_ack, "acknowledge_unknown_preparations", close_request)
    receipt = await manual_paper.run(
        EngineSettings(
            database_url="postgresql+asyncpg://fixture:fixture@127.0.0.1/nexus_test_fixture",
            agent_paper_account_id=UUID(int=2),
        ),
        Namespace(
            action=action,
            claim_ids=[UUID(int=i) for i in range(2, 6)],
            accept_unknown_effects=True,
            repo_root=tmp_path,
            env_file=tmp_path / "env",
            request_id=UUID(int=1),
        ),
    )
    assert receipt.execution_locked is True
    assert observed == ["lock", "commit", "dispose", "unlock"]
    if action == "check-preparation-acceptance":
        assert len(code_checks) == 3
        assert receipt.check_only


def test_unknown_preparation_ack_requires_explicit_five_item_selection():
    args = manual_paper.parser().parse_args(
        [
            "acknowledge-unknown-preparations",
            "--request-id",
            str(__import__("uuid").UUID(int=1)),
            *[
                part
                for i in range(2, 6)
                for part in ("--claim-id", str(__import__("uuid").UUID(int=i)))
            ],
            "--accept-unknown-effects",
        ]
    )
    assert len(args.claim_ids) == 4 and args.accept_unknown_effects


@pytest.mark.parametrize(
    "failure,stage,code",
    [
        ("source_timeout", "service_control_source", "INSPECTION_TIMEOUT"),
        ("source_mismatch", "service_control_source", "SOURCE_IDENTITY_INVALID"),
        ("source_nonzero", "service_control_source", "INSPECTION_FAILED"),
        ("source_oserror", "service_control_source", "INSPECTION_FAILED"),
        ("host_timeout", "host_status", "INSPECTION_TIMEOUT"),
        ("host_nonzero", "host_status", "INSPECTION_FAILED"),
        ("host_oserror", "host_status", "INSPECTION_FAILED"),
        ("host_json", "host_status", "STATUS_INVALID"),
        ("host_object", "host_status", "STATUS_INVALID"),
        ("host_running", "host_status", "HOST_NOT_INACTIVE"),
        ("research", "research_processes", "QUIESCENCE_UNCONFIRMED"),
    ],
)
def test_preparation_failures_report_safe_stage_through_cli(
    monkeypatch, tmp_path, capsys, failure, stage, code
):
    import subprocess

    from nexus.paper import manual_preparation_close as close

    secret = "private-response-must-not-escape"
    script = tmp_path / "scripts/macos/agent-paper-loop-service.py"
    script.parent.mkdir(parents=True)
    script.write_text("fixture")
    calls = []

    def command(argv, **kwargs):
        calls.append(argv[0])
        assert kwargs["timeout"] == (5 if failure.startswith("source") else 15)
        if failure.endswith("timeout"):
            raise subprocess.TimeoutExpired([secret], kwargs["timeout"], output=secret)
        if failure.endswith("oserror"):
            raise OSError(secret)
        stdout = {
            "source_mismatch": b"different",
            "source_nonzero": b"fixture",
            "host_nonzero": '{"status":"inactive"}',
            "host_json": secret,
            "host_object": "[]",
            "host_running": '{"status":"active"}',
            "research": '{"status":"inactive"}',
        }[failure]
        return subprocess.CompletedProcess(argv, int(failure.endswith("nonzero")), stdout, secret)

    def quiescent():
        assert failure == "research"
        raise ValueError(secret)

    monkeypatch.setattr(close.subprocess, "run", command)
    monkeypatch.setattr(close, "require_research_processes_quiescent", quiescent)
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(manual_paper, "load_agent_paper_service_environment", lambda *a, **k: {})
    monkeypatch.setattr(manual_paper, "_checkout_revision", lambda root: "a" * 40)

    async def run(settings, args):
        if failure.startswith("source"):
            close.preparation_service_control_lock(args.repo_root, settings.release_revision)
        else:
            close.require_preparation_producers_stopped(args.repo_root, args.env_file)
        raise AssertionError("failed inspection must block")

    monkeypatch.setattr(manual_paper, "run", run)
    assert (
        manual_paper.main(
            [
                "--repo-root",
                str(tmp_path),
                "recover-preparation",
                "--request-id",
                "00000000-0000-0000-0000-000000000001",
                "--accept-unknown-effects",
            ]
        )
        == 1
    )
    output = capsys.readouterr()
    assert output.err == "" and secret not in output.out
    assert json.loads(output.out) == {
        "status": "blocked",
        "error_code": "MANUAL_PREPARATION_CLOSE_BLOCKED",
        "execution_locked": True,
        "diagnostic_stage": stage,
        "diagnostic_code": code,
    }
    assert len(calls) == 1  # Observability must not add retries.


@pytest.mark.parametrize(
    "reason",
    [
        "AGENT_PAPER_SERVICE_CONTROL_BUSY",
        "AGENT_PAPER_SERVICE_CONTROL_LOCK_UNSAFE",
        "private-message",
    ],
)
def test_lifecycle_lock_failure_blocks_before_database_and_disposes(
    monkeypatch, tmp_path, capsys, reason
):
    from contextlib import contextmanager

    disposed = []

    @contextmanager
    def fence(*_):
        raise RuntimeError(reason)
        yield

    class Engine:
        async def dispose(self):
            disposed.append(True)

    def forbidden_transaction():
        raise AssertionError("failed fence must not enter database")

    monkeypatch.setattr(
        manual_paper,
        "session_factory",
        lambda *_: SimpleNamespace(begin=forbidden_transaction, kw={"bind": Engine()}),
    )
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", fence)
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(
        manual_paper,
        "load_agent_paper_service_environment",
        lambda *a, **k: {
            "DATABASE_URL": "postgresql+asyncpg://fixture:fixture@127.0.0.1/nexus_test_fixture"
        },
    )
    monkeypatch.setattr(manual_paper, "_checkout_revision", lambda root: "a" * 40)
    assert (
        manual_paper.main(
            [
                "--repo-root",
                str(tmp_path),
                "recover-preparation",
                "--request-id",
                "00000000-0000-0000-0000-000000000001",
                "--accept-unknown-effects",
            ]
        )
        == 1
    )
    output = capsys.readouterr()
    assert output.err == "" and "private-message" not in output.out
    report = json.loads(output.out)
    assert report["error_code"] == "MANUAL_PREPARATION_CLOSE_BLOCKED"
    assert report["diagnostic_stage"] == "service_control_lock"
    assert report["diagnostic_code"] == {
        "AGENT_PAPER_SERVICE_CONTROL_BUSY": "LOCK_BUSY",
        "AGENT_PAPER_SERVICE_CONTROL_LOCK_UNSAFE": "LOCK_UNSAFE",
    }.get(reason, "LOCK_UNAVAILABLE")
    assert disposed == [True]


@pytest.mark.parametrize("kind", ["untyped", "unsafe_stage", "unsafe_code"])
def test_preparation_cli_drops_untrusted_diagnostic_fields(monkeypatch, tmp_path, capsys, kind):
    from nexus.paper.manual_preparation_close import PreparationCloseDiagnosticError

    secret = "private-path-or-response"
    if kind == "untyped":
        error = ValueError("MANUAL_PREPARATION_CLOSE_BLOCKED")
        error.stage = "host_status"
        error.code = "INSPECTION_FAILED"
    else:
        error = PreparationCloseDiagnosticError(
            secret if kind == "unsafe_stage" else "host_status",
            secret if kind == "unsafe_code" else "INSPECTION_FAILED",
        )

    async def run(*_):
        raise error

    monkeypatch.setattr(manual_paper, "run", run)
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(manual_paper, "load_agent_paper_service_environment", lambda *a, **k: {})
    monkeypatch.setattr(manual_paper, "_checkout_revision", lambda root: "a" * 40)
    assert (
        manual_paper.main(
            [
                "--repo-root",
                str(tmp_path),
                "recover-preparation",
                "--request-id",
                "00000000-0000-0000-0000-000000000001",
                "--accept-unknown-effects",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert captured.err == "" and secret not in captured.out
    assert json.loads(captured.out) == {
        "status": "blocked",
        "error_code": "MANUAL_PREPARATION_CLOSE_BLOCKED",
        "execution_locked": True,
    }
