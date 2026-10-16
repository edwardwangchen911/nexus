"""Exercise normal entry points; diagnostics cannot change retries or expose data."""

import asyncio
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.ops import manual_paper
from sqlalchemy.exc import SQLAlchemyError
from unit.ops.test_agent_paper_loop import CALENDAR_AUTHORITY_ID, NOW, TEST_CANDIDATE
from unit.ops.test_manual_paper_cli import _SessionFactory


@pytest.mark.parametrize(
    "failure,stage,outcome",
    [
        ("open", "transaction_open", "unconfirmed"),
        ("body", "transaction_body", "unconfirmed"),
        ("commit", "transaction_commit", "unconfirmed"),
        ("dispose", "engine_dispose", "committed"),
        ("unlock", "lifecycle_unlock", "committed"),
        ("body_dispose_unlock", "transaction_body", "unconfirmed"),
        ("body_rollback", "transaction_body", "unconfirmed"),
    ],
)
def test_normal_maintenance_cli_distinguishes_failure_boundaries(
    monkeypatch, tmp_path, capsys, failure, stage, outcome
):
    calls = []
    private = "private SQL credentials and account must not escape"

    class Transaction:
        async def __aenter__(self):
            calls.append("open")
            if failure == "open":
                raise SQLAlchemyError(private)
            return self

        async def __aexit__(self, kind, error, traceback):
            calls.append("rollback" if error else "commit")
            if failure in {"commit", "body_rollback"}:
                raise SQLAlchemyError(private)

        async def scalar(self, query):
            return (
                datetime(2026, 10, 16, 8, tzinfo=UTC)
                if "clock_timestamp" in str(query)
                else manual_paper.CURRENT_MIGRATION_HEAD
            )

        async def execute(self, *_args, **_kwargs):
            pass

    async def dispose():
        calls.append("dispose")
        if "dispose" in failure:
            raise OSError(private)

    @contextmanager
    def fence(*_args):
        calls.append("lock")
        try:
            yield
        finally:
            calls.append("unlock")
            if "unlock" in failure:
                raise OSError(private)

    async def accept(*_args, **_kwargs):
        calls.append("accept")
        if failure.startswith("body"):
            raise SQLAlchemyError(private)
        return SimpleNamespace(model_dump_json=lambda: '{"status":"accepted"}')

    monkeypatch.setattr(
        manual_paper,
        "session_factory",
        lambda _: _SessionFactory(Transaction, SimpleNamespace(dispose=dispose)),
    )
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", fence)
    monkeypatch.setattr(manual_paper, "accept_current_unknown", accept)
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(manual_paper, "_checkout_revision", lambda _: "a" * 40)
    monkeypatch.setattr(
        manual_paper,
        "load_agent_paper_service_environment",
        lambda *a, **k: {
            "DATABASE_URL": "postgresql+asyncpg://fixture:fixture@127.0.0.1/nexus_test_fixture",
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(UUID(int=2)),
        },
    )
    assert (
        manual_paper.main(
            [
                "--repo-root",
                str(tmp_path),
                "recover-preparation",
                "--request-id",
                str(UUID(int=1)),
                "--accept-unknown-effects",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert report["error_code"] == "MANUAL_RESULT_UNKNOWN"
    assert report["operation_stage"] == stage
    assert report["operation_code"] == (
        "OS_ERROR" if failure in {"dispose", "unlock"} else "DATABASE_ERROR"
    )
    assert report["transaction_outcome"] == outcome
    assert report["execution_locked"] is True
    assert calls.count("accept") == (0 if failure == "open" else 1)
    assert calls[-2:] == ["dispose", "unlock"]
    if failure == "body_dispose_unlock":
        assert report["secondary_failures"] == [
            {"stage": "engine_dispose", "code": "OS_ERROR"},
            {"stage": "lifecycle_unlock", "code": "OS_ERROR"},
        ]
    if failure == "body_rollback":
        assert report["secondary_failures"] == [
            {"stage": "transaction_rollback", "code": "DATABASE_ERROR"}
        ]
    assert private not in captured.out + captured.err


@pytest.mark.parametrize("boundary", ["scope", "collect", "import", "snapshot", "readback"])
def test_trends_exception_reaches_host_with_safe_exact_step(
    monkeypatch, tmp_path, capsys, boundary
):
    calls = []
    private = "private response and SQL canary"

    async def step(name, result):
        calls.append(name)
        if boundary == name:
            raise SQLAlchemyError(private)
        return result

    async def inspect(*a, **k):
        name = "readback" if "scope" in calls else "scope"
        return await step(
            name,
            SimpleNamespace(
                instrument_id=TEST_CANDIDATE.instrument_id,
                prior_session=NOW.date(),
                start_session=NOW.date(),
                evidence_ready=False,
                snapshot_ready=False,
            ),
        )

    async def collect(*a, **k):
        return await step("collect", {"status": "collected", "readback_verified": True})

    async def importer(*a, **k):
        return await step(
            "import", {"status": "stored_context", "last_observed_on": NOW.date().isoformat()}
        )

    async def snapshot(*a, **k):
        return await step("snapshot", None)

    monkeypatch.setattr(loop, "_inspect_current_trends_scope", inspect)
    monkeypatch.setattr(loop, "collection_window", lambda *a, **k: (object(), {}))
    monkeypatch.setattr(loop, "collect_prices", collect)
    monkeypatch.setattr(loop, "import_trends", importer)
    monkeypatch.setattr(loop, "_prepare_current_trends_research_snapshot", snapshot)
    with pytest.raises(SQLAlchemyError) as caught:
        asyncio.run(
            loop.prepare_current_trends_once(
                SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=NOW,
                expected_calendar_authority_id=CALENDAR_AUTHORITY_ID,
                candidate=TEST_CANDIDATE,
            )
        )
    loop._emit_agent_paper_unknown_failure(
        "execute", NOW, action=loop.AgentPaperLoopAction.PREPARE_INPUTS, error=caught.value
    )
    output = capsys.readouterr().err
    report = json.loads(output)
    assert report["failure_phase"] == "current_trends"
    assert report["failure_point"] == boundary
    assert report["failure_code"] == "DATABASE_ERROR"
    assert calls[-1] == boundary
    assert all(calls.count(name) == 1 for name in calls)
    assert private not in output


def test_existing_archive_unavailable_is_visible_without_recollection(
    monkeypatch, tmp_path, capsys
):
    async def inspect(*a, **k):
        return SimpleNamespace(
            instrument_id=TEST_CANDIDATE.instrument_id,
            prior_session=NOW.date(),
            authority_ready=False,
        )

    async def forbidden(*a, **k):
        pytest.fail("must not recollect or import an existing attempt")

    monkeypatch.setattr(loop, "_inspect_prior_daily_scope", inspect)
    monkeypatch.setattr(loop, "collect_prices", forbidden)
    monkeypatch.setattr(loop, "import_adjusted_authority", forbidden)
    (
        tmp_path
        / ".secrets/nexus-agent-paper/daily"
        / f"{NOW.date()}-{TEST_CANDIDATE.instrument_id}"
    ).mkdir(parents=True)
    assert (
        asyncio.run(
            loop.prepare_prior_daily_once(
                SimpleNamespace(),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=NOW,
                expected_calendar_authority_id=CALENDAR_AUTHORITY_ID,
                candidate=TEST_CANDIDATE,
            )
        )
        == "unavailable"
    )
    report = json.loads(capsys.readouterr().err)
    assert report["event"] == "agent_paper_preparation_unavailable"
    assert report["failure_phase"] == "prior_daily"
    assert report["failure_point"] == "archive"
    assert report["failure_code"] == "ATTEMPT_EXISTS"


@pytest.mark.parametrize("phase", ["prior_daily", "macro"])
@pytest.mark.parametrize("boundary", ["scope", "collect", "import", "readback"])
def test_daily_and_macro_exceptions_keep_exact_step(monkeypatch, tmp_path, capsys, phase, boundary):
    calls = []
    private = "private upstream payload"

    async def step(name, value):
        calls.append(name)
        if name == boundary:
            raise SQLAlchemyError(private)
        return value

    async def inspect(*a, **k):
        return await step(
            "readback" if "scope" in calls else "scope",
            SimpleNamespace(
                instrument_id=TEST_CANDIDATE.instrument_id,
                prior_session=NOW.date(),
                authority_ready=False,
                ready_series=frozenset(),
                snapshot_ready=False,
                vintage=NOW.date(),
            ),
        )

    async def collect(*a, **k):
        return await step(
            "collect", {"status": "collected", "readback_verified": True, "observation_count": 1}
        )

    async def importer(*a, **k):
        return await step(
            "import",
            {
                "status": "stored_authority" if phase == "prior_daily" else "imported",
                "visible_observation_count": 1,
            },
        )

    async def snapshot(*a, **k):
        calls.append("snapshot")

    monkeypatch.setattr(loop, f"_inspect_{phase}_scope", inspect)
    monkeypatch.setattr(loop, "collection_window", lambda *a, **k: (object(), {}))
    monkeypatch.setattr(loop, "collect_prices", collect)
    monkeypatch.setattr(loop, "collect_fred", collect)
    monkeypatch.setattr(loop, "import_adjusted_authority", importer)
    monkeypatch.setattr(loop, "import_fred_collection", importer)
    monkeypatch.setattr(loop, "load_fred_collection", lambda *a: object())
    monkeypatch.setattr(loop, "validate_local_import", lambda *a: None)
    monkeypatch.setattr(loop, "_PAPER_MACRO_SERIES", frozenset({"CPIAUCSL"}))
    monkeypatch.setattr(loop, "_prepare_current_trends_research_snapshot", snapshot)
    kwargs = {"fred_key": object()} if phase == "macro" else {}
    with pytest.raises(SQLAlchemyError) as caught:
        asyncio.run(
            getattr(loop, f"prepare_{phase}_once")(
                SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=NOW,
                expected_calendar_authority_id=CALENDAR_AUTHORITY_ID,
                candidate=TEST_CANDIDATE,
                **kwargs,
            )
        )
    loop._emit_agent_paper_unknown_failure(
        "execute", NOW, action=loop.AgentPaperLoopAction.PREPARE_INPUTS, error=caught.value
    )
    output = capsys.readouterr().err
    report = json.loads(output)
    assert (report["failure_phase"], report["failure_point"], report["failure_code"]) == (
        phase,
        boundary,
        "DATABASE_ERROR",
    )
    assert calls[-1] == boundary and len(calls) == len(set(calls))
    assert private not in output


def test_daily_validation_failure_is_not_misreported_as_collection_window(
    monkeypatch, tmp_path, capsys
):
    async def inspect(*a, **k):
        return SimpleNamespace(
            instrument_id=TEST_CANDIDATE.instrument_id,
            prior_session=NOW.date(),
            authority_ready=False,
        )

    async def collect(*a, **k):
        return {"status": "collected", "readback_verified": True}

    async def reject_import(*a, **k):
        raise ValueError("private import validation detail")

    monkeypatch.setattr(loop, "_inspect_prior_daily_scope", inspect)
    monkeypatch.setattr(loop, "collection_window", lambda *a, **k: (object(), {}))
    monkeypatch.setattr(loop, "collect_prices", collect)
    monkeypatch.setattr(loop, "import_adjusted_authority", reject_import)
    assert (
        asyncio.run(
            loop.prepare_prior_daily_once(
                SimpleNamespace(alpaca_key=object(), alpaca_secret=object()),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=NOW,
                expected_calendar_authority_id=CALENDAR_AUTHORITY_ID,
                candidate=TEST_CANDIDATE,
            )
        )
        == "unavailable"
    )
    output = capsys.readouterr().err
    report = json.loads(output)
    assert report["failure_point"] == "import"
    assert report["failure_code"] == "VALIDATION_REJECTED"
    assert "private import" not in output


@pytest.mark.parametrize(
    "raw",
    [
        None,
        ("macro", "private", "DATABASE_ERROR"),
        ("macro", "collect", "private"),
        ["macro", "collect", "DATABASE_ERROR"],
        ({}, "collect", "DATABASE_ERROR"),
    ],
)
def test_host_drops_untrusted_diagnostic_metadata(capsys, raw):
    error = RuntimeError("private error")
    error._nexus_preparation_diagnostic = raw
    loop._emit_agent_paper_unknown_failure("execute", NOW, error=error)
    output = capsys.readouterr().err
    report = json.loads(output)
    assert "failure_point" not in report and "failure_code" not in report
    assert "private" not in output


def test_diagnostic_output_failure_does_not_change_unavailable(monkeypatch):
    from nexus.ops import preparation_diagnostics as diagnostic

    def broken(*a, **k):
        raise OSError("logging unavailable")

    monkeypatch.setattr(diagnostic, "print", broken, raising=False)
    assert diagnostic.preparation_unavailable("macro", "archive", "ATTEMPT_EXISTS") == "unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["close", "commit_rollback"])
async def test_real_sqlalchemy_transaction_cleanup_preserves_primary(monkeypatch, failure):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    calls = []

    class Session(AsyncSession):
        async def close(self):
            calls.append("close")
            await super().close()
            if failure == "close":
                raise OSError("private session close")

    state = manual_paper._OperationDiagnostic()
    factory = async_sessionmaker(class_=Session)
    with pytest.raises((OSError, SQLAlchemyError)) as caught:
        async with state.transaction(factory) as session:
            if failure == "commit_rollback":
                tx = session.sync_session.get_transaction()

                def commit(*a, **k):
                    calls.append("commit")
                    raise SQLAlchemyError("private original commit error")

                def rollback(*a, **k):
                    calls.append("rollback")
                    raise OSError("private secondary rollback error")

                monkeypatch.setattr(tx, "commit", commit)
                monkeypatch.setattr(tx, "rollback", rollback)
    fields = manual_paper._operation_fields(caught.value)
    assert fields["operation_stage"] == (
        "session_close" if failure == "close" else "transaction_commit"
    )
    assert fields["transaction_outcome"] == ("committed" if failure == "close" else "unconfirmed")
    if failure == "commit_rollback":
        assert isinstance(caught.value, SQLAlchemyError)
        assert fields["secondary_failures"] == [
            {"stage": "transaction_rollback", "code": "OS_ERROR"}
        ]
    assert calls.count("close") == 1


@pytest.mark.asyncio
async def test_driver_connection_refusal_is_not_a_filesystem_error():
    from nexus.ops.preparation_diagnostics import exception_code
    from sqlalchemy.ext.asyncio import create_async_engine

    async def refuse():
        raise ConnectionRefusedError("private server identity")

    engine = create_async_engine("postgresql+asyncpg://", async_creator=refuse)
    try:
        with pytest.raises(ConnectionRefusedError) as caught:
            async with engine.connect():
                pytest.fail("unreachable")
        assert exception_code(caught.value) == "CONNECTION_ERROR"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("close_fails", [False, True])
@pytest.mark.parametrize("cancel_at", ["body", "commit"])
async def test_cancellation_is_propagated_without_success_even_if_close_fails(
    close_fails, cancel_at
):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    calls = []

    class Session(AsyncSession):
        async def commit(self):
            if cancel_at == "body":
                pytest.fail("cancelled body must never attempt commit")
            calls.append("commit")
            raise original

        async def rollback(self):
            calls.append("rollback")
            await super().rollback()

        async def close(self):
            calls.append("close")
            await super().close()
            if close_fails:
                raise OSError("private cleanup")

    state = manual_paper._OperationDiagnostic()
    original = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError) as caught:
        async with state.transaction(async_sessionmaker(class_=Session)):
            if cancel_at == "body":
                raise original
    assert caught.value is original
    assert calls == (["commit"] if cancel_at == "commit" else []) + ["rollback", "close"]
    assert manual_paper._operation_fields(original)["transaction_outcome"] == "unconfirmed"


@pytest.mark.parametrize(
    "failure", ["candidates", "prior_daily", "macro", "research_identity", "current_trends"]
)
def test_normal_host_reports_invalid_preparation_and_stops_once(
    monkeypatch, tmp_path, capsys, failure
):
    from unit.ops.test_agent_paper_loop import enabled_environment, snapshot

    calls = []

    async def candidates(*a, **k):
        calls.append("candidates")
        if failure == "candidates":
            raise ConnectionRefusedError("private database host")
        return (TEST_CANDIDATE,)

    def preparer(phase):
        async def prepare(*a, **k):
            calls.append(phase)
            return "private invalid result" if failure == phase else "ready"

        return prepare

    monkeypatch.setattr(
        loop,
        "load_agent_paper_service_environment",
        lambda *a, **k: {
            **enabled_environment(),
            "ALPACA_API_KEY_ID": "key",
            "ALPACA_API_SECRET_KEY": "secret",
        },
    )
    monkeypatch.setattr(loop, "_load_loop_candidates", candidates)
    for phase, name in [
        ("prior_daily", "prepare_prior_daily_once"),
        ("macro", "prepare_macro_once"),
        ("research_identity", "prepare_agent_paper_identity_once"),
        ("current_trends", "prepare_current_trends_once"),
    ]:
        monkeypatch.setattr(loop, name, preparer(phase))
    operation = loop.execute_agent_paper_loop_action(
        loop.EngineSettings(),
        account_id=UUID(int=1),
        repo_root=tmp_path,
        action=loop.AgentPaperLoopAction.PREPARE_INPUTS,
        snapshot=snapshot(
            now=NOW.replace(minute=30, hour=13), market_open_at=NOW.replace(minute=30, hour=13)
        ),
    )
    if failure == "candidates":
        with pytest.raises(ConnectionRefusedError) as caught:
            asyncio.run(operation)
        loop._emit_agent_paper_unknown_failure(
            "execute", NOW, action=loop.AgentPaperLoopAction.PREPARE_INPUTS, error=caught.value
        )
    else:
        assert asyncio.run(operation) is loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    output = capsys.readouterr().err
    report = json.loads(output)
    assert report["failure_phase"] == ("configuration" if failure == "candidates" else failure)
    assert report["failure_code"] == (
        "CONNECTION_ERROR" if failure == "candidates" else "READBACK_MISMATCH"
    )
    assert report["failure_point"] == ("candidates" if failure == "candidates" else "readback")
    assert calls[-1] == failure and len(calls) == len(set(calls))
    assert "private" not in output


@pytest.mark.parametrize("http_status", [400, "private", True, 999])
def test_macro_policy_rejection_reports_only_valid_http_status(
    monkeypatch, tmp_path, capsys, http_status
):
    from nexus.providers.http import HttpPolicyError

    calls = []

    async def inspect(*a, **k):
        return SimpleNamespace(
            instrument_id=TEST_CANDIDATE.instrument_id, ready_series=frozenset(), vintage=NOW.date()
        )

    async def collect(*a, **k):
        calls.append("collect")
        error = HttpPolicyError("private upstream message")
        error.http_status = http_status
        raise error

    async def forbidden(*a, **k):
        pytest.fail("rejected collection must not be imported")

    monkeypatch.setattr(loop, "_inspect_macro_scope", inspect)
    monkeypatch.setattr(loop, "validate_local_import", lambda *a: None)
    monkeypatch.setattr(loop, "_PAPER_MACRO_SERIES", frozenset({"CPIAUCSL"}))
    monkeypatch.setattr(loop, "collect_fred", collect)
    monkeypatch.setattr(loop, "import_fred_collection", forbidden)
    assert (
        asyncio.run(
            loop.prepare_macro_once(
                SimpleNamespace(),
                fred_key=object(),
                account_id=UUID(int=1),
                repo_root=tmp_path,
                now=NOW,
                expected_calendar_authority_id=CALENDAR_AUTHORITY_ID,
                candidate=TEST_CANDIDATE,
            )
        )
        == "unavailable"
    )
    output = capsys.readouterr().err
    report = json.loads(output)
    assert report["failure_point"] == "collect" and report["failure_code"] == "HTTP_POLICY_REJECTED"
    if type(http_status) is int and http_status == 400:
        assert report["http_status"] == 400
    else:
        assert "http_status" not in report
    assert calls == ["collect"] and "private" not in output
