import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from nexus.ops import agent_paper_once as cli
from nexus.ops.agent_paper_once import (
    AgentPaperCollectWindowRequest,
    AgentPaperResumeRequest,
    AgentPaperRunRequest,
    AgentPaperTriggerRequest,
    main,
    parser,
    trigger_in_session,
)
from nexus.ops.schwab_market_stream import SchwabMarketStreamImportResult
from nexus.paper.agent_cycle import AgentPaperCycleError
from nexus.paper.agent_cycle_authority import CycleAuthority, ServiceCalendarAuthority
from nexus.paper.agent_cycle_models import AgentPaperCycleResult, AgentPaperTriggerReservation
from nexus.paper.agent_market import AgentPaperMarketScope
from nexus.persistence.models.paper import (
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
)
from nexus.providers.market.schwab_oauth import SchwabOAuthError
from nexus.providers.market.schwab_stream import SchwabStreamError
from nexus.risk.query_authority_producer import RiskQueryScope, RiskSourceProof
from nexus.settings import EngineSettings
from pydantic import SecretStr, ValidationError


def test_request_requires_explicit_utc_time() -> None:
    with pytest.raises(ValueError, match="explicit UTC time is required"):
        AgentPaperRunRequest(
            action="run",
            account_id=UUID(int=1),
            decision_time=datetime(2026, 8, 26, 21),
            idempotency_key="cycle-1",
        )


def test_disabled_command_emits_only_fixed_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "load_local_environment",
        lambda *_args: {"NEXUS_AGENT_PAPER_CYCLE_ENABLED": "false"},
    )

    exit_code = main(
        [
            "run",
            "--account-id",
            str(UUID(int=1)),
            "--decision-time",
            datetime(2026, 8, 26, 21, tzinfo=UTC).isoformat(),
            "--idempotency-key",
            "cycle-1",
        ]
    )

    assert exit_code == 1
    assert capsys.readouterr().out == '{"error_code":"AGENT_PAPER_CYCLE_DISABLED"}\n'


def test_non_internal_account_emits_stable_boundary_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        "nexus.ops.agent_paper_once.load_local_environment",
        lambda *_: {"NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true", "DATABASE_URL": "x"},
    )

    async def reject(*_args: object) -> None:
        raise AgentPaperCycleError("AGENT_PAPER_ACCOUNT_INVALID")

    monkeypatch.setattr("nexus.ops.agent_paper_once.run_once", reject)

    exit_code = main(
        [
            "run",
            "--account-id",
            str(UUID(int=1)),
            "--decision-time",
            datetime(2026, 8, 26, 21, tzinfo=UTC).isoformat(),
            "--idempotency-key",
            "cycle-invalid-account",
        ]
    )

    assert exit_code == 1
    assert capsys.readouterr().out == '{"error_code":"AGENT_PAPER_ACCOUNT_INVALID"}\n'


@pytest.mark.parametrize(
    "error_code",
    [
        "AGENT_PAPER_CALENDAR_UNAVAILABLE",
        "AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
        "AGENT_PAPER_MARKET_WINDOW_CLOSED",
    ],
)
def test_trigger_command_exposes_persisted_calendar_failure_codes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    error_code: str,
) -> None:
    monkeypatch.setattr(
        cli,
        "load_local_environment",
        lambda *_args: {
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
            "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1/nexus",
            "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
            "ALPACA_API_KEY_ID": "key",
            "ALPACA_API_SECRET_KEY": "secret",
        },
    )

    async def reject(*_args, **_kwargs):
        raise ValueError(error_code)

    monkeypatch.setattr(cli, "trigger_once", reject)

    exit_code = main(
        [
            "trigger",
            "--account-id",
            str(UUID(int=1)),
            "--idempotency-key",
            "calendar-failure",
        ]
    )

    assert exit_code == 1
    assert json.loads(capsys.readouterr().out) == {"error_code": error_code}


def test_feature_flag_is_disabled_by_default_and_can_be_explicitly_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NEXUS_AGENT_PAPER_CYCLE_ENABLED", raising=False)
    assert EngineSettings().agent_paper_cycle_enabled is False
    monkeypatch.setenv("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "true")
    assert EngineSettings().agent_paper_cycle_enabled is True


def test_parser_rejects_provider_and_credential_overrides() -> None:
    with pytest.raises(SystemExit):
        parser().parse_args(
            [
                "run",
                "--account-id",
                str(UUID(int=1)),
                "--decision-time",
                datetime(2026, 8, 26, 21, tzinfo=UTC).isoformat(),
                "--idempotency-key",
                "cycle-1",
                "--provider-url",
                "https://example.invalid",
            ]
        )


def test_collect_window_accepts_legacy_instrument_but_rejects_client_time() -> None:
    request = AgentPaperCollectWindowRequest.model_validate(
        {
            "action": "collect-window",
            "cycle_id": UUID(int=1),
            "instrument_id": UUID(int=2),
        }
    )
    assert request.instrument_id == UUID(int=2)
    with pytest.raises(ValidationError):
        AgentPaperCollectWindowRequest.model_validate(
            {
                "action": "collect-window",
                "cycle_id": UUID(int=1),
                "instrument_id": UUID(int=2),
                "window_start": datetime(2026, 8, 27, 13, 30, tzinfo=UTC),
            }
        )


def test_collect_window_parser_has_no_provider_or_credential_override() -> None:
    parsed = parser().parse_args(
        [
            "collect-window",
            "--cycle-id",
            str(UUID(int=1)),
            "--instrument-id",
            str(UUID(int=2)),
        ]
    )
    assert parsed.action == "collect-window"
    assert parsed.instrument_id == str(UUID(int=2))
    with pytest.raises(SystemExit):
        parser().parse_args(
            [
                "collect-window",
                "--cycle-id",
                str(UUID(int=1)),
                "--window-start",
                datetime(2026, 8, 27, 13, 30, tzinfo=UTC).isoformat(),
            ]
        )


def test_run_research_parser_accepts_only_cycle_id() -> None:
    cycle_id = UUID(int=1)

    parsed = parser().parse_args(["run-research", "--cycle-id", str(cycle_id)])

    assert parsed.action == "run-research"
    assert parsed.cycle_id == str(cycle_id)
    with pytest.raises(SystemExit):
        parser().parse_args(
            [
                "run-research",
                "--cycle-id",
                str(cycle_id),
                "--backend",
                "codex_sdk",
            ]
        )


def test_run_research_resolves_only_one_queued_cycle_bound_paper_job() -> None:
    run_id, job_id = UUID(int=10), UUID(int=11)
    events = (
        SimpleNamespace(
            sequence=1,
            to_status="researching",
            fact_ids={"research_run_ids": [str(run_id)]},
        ),
    )
    paper_job = SimpleNamespace(
        id=job_id,
        job_type="agent.paper.research",
        status="queued",
        attempt_count=0,
        manual_retry_count=0,
        max_attempts=1,
        started_at=None,
        finished_at=None,
        payload={"run_id": str(run_id)},
    )

    assert (
        cli.resolve_queued_cycle_research_job(events=events, jobs=(paper_job,), output_run_ids=())
        == job_id
    )

    ordinary_job = SimpleNamespace(
        id=job_id,
        job_type="agent.research",
        status="queued",
        payload={"run_id": str(run_id)},
    )
    for jobs, outputs in (
        ((ordinary_job,), ()),
        ((paper_job,), (run_id,)),
        ((paper_job, paper_job), ()),
        ((SimpleNamespace(**{**vars(paper_job), "status": "running"}),), ()),
        ((SimpleNamespace(**{**vars(paper_job), "attempt_count": 1}),), ()),
    ):
        with pytest.raises(ValueError, match="AGENT_PAPER_RESULT_UNKNOWN"):
            cli.resolve_queued_cycle_research_job(events=events, jobs=jobs, output_run_ids=outputs)
    with pytest.raises(ValueError, match="AGENT_PAPER_RESULT_UNKNOWN"):
        cli.resolve_queued_cycle_research_job(events=(), jobs=(), output_run_ids=())


def test_run_research_claims_only_resolved_job_and_exits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cycle_id, job_id = UUID(int=20), UUID(int=21)
    received: list[tuple[UUID, ...] | None] = []
    selected_backends: list[str] = []

    async def resolve(_database_url: str, *, requested_cycle_id: UUID) -> UUID:
        assert requested_cycle_id == cycle_id
        return job_id

    async def run_worker(_settings: object, *, job_ids=None) -> bool:
        received.append(job_ids)
        return True

    monkeypatch.setattr(cli, "_resolve_queued_research_job_id", resolve)

    def settings(_root: Path, *, backend: str) -> EngineSettings:
        selected_backends.append(backend)
        return EngineSettings(
            database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus",
            AGENT_RUNTIME_MODE=backend,
            OPENAI_API_KEY="test-key" if backend == "responses" else None,
            OPENAI_EXTRACTION_MODEL="test-model" if backend == "responses" else None,
            OPENAI_REASONING_MODEL="test-model" if backend == "responses" else None,
        )

    monkeypatch.setattr(cli, "local_settings", settings)
    monkeypatch.setattr(cli, "run_agent_worker_once", run_worker)

    result = asyncio.run(
        cli.run_research_once(
            EngineSettings(
                database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus",
                AGENT_RUNTIME_MODE="responses",
                OPENAI_API_KEY="test-key",
                OPENAI_EXTRACTION_MODEL="test-model",
                OPENAI_REASONING_MODEL="test-model",
            ),
            cli.AgentPaperRunResearchRequest(action="run-research", cycle_id=cycle_id),
            repo_root=tmp_path,
        )
    )

    assert received == [(job_id,)]
    assert selected_backends == ["responses"]
    assert result.model_dump(mode="json") == {
        "cycle_id": str(cycle_id),
        "job_id": str(job_id),
        "job_processed": True,
        "execution_locked": True,
    }


@pytest.mark.parametrize("schwab", [False, True])
def test_collect_window_forwards_database_scope_and_explicit_provider_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    schwab: bool,
) -> None:
    cycle_id = UUID(int=1)
    instrument_id = UUID(int=2)
    window_start = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
    scope = AgentPaperMarketScope(
        cycle_id=cycle_id,
        instrument_id=instrument_id,
        execution_source_policy_id=(
            PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID if schwab else PAPER_EXECUTION_SOURCE_POLICY_ID
        ),
        window_start=window_start,
        window_end=window_start.replace(minute=35),
    )
    expected = object()
    environment = {"NEXUS_SCHWAB_APP_KEY": "registered-key"}
    providers = []

    def schwab_provider(root, http, *, symbol_resolver, process_environment):
        assert root == tmp_path
        assert process_environment == environment
        provider = object()
        providers.append(provider)
        return provider

    class AsyncContext:
        def __init__(self, value: object) -> None:
            self.value = value

        async def __aenter__(self) -> object:
            return self.value

        async def __aexit__(self, *_args: object) -> None:
            return None

    class Engine:
        async def dispose(self) -> None:
            return None

    class Factory:
        def __call__(self) -> AsyncContext:
            return AsyncContext(object())

    async def resolve(*_args: object, **kwargs: object) -> AgentPaperMarketScope:
        assert kwargs["instrument_id"] == instrument_id
        return scope

    async def collect(
        _factory: object,
        *,
        cycle_id: UUID,
        instrument_id: UUID | None = None,
        window_start: datetime | None = None,
        **_kwargs: object,
    ) -> object:
        assert cycle_id == scope.cycle_id
        assert instrument_id == scope.instrument_id
        assert window_start == scope.window_start
        if schwab:
            assert _kwargs["provider"] is providers[0]
        return expected

    monkeypatch.setattr(cli, "create_async_engine", lambda *_args, **_kwargs: Engine())
    monkeypatch.setattr(cli, "async_sessionmaker", lambda *_args, **_kwargs: Factory())
    monkeypatch.setattr(cli, "S3ProviderEvidenceStore", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli.httpx, "AsyncClient", lambda *_args, **_kwargs: AsyncContext(object()))
    monkeypatch.setattr(cli, "FixedOriginHttpClient", lambda **_kwargs: object())
    monkeypatch.setattr(cli, "AlpacaHttpTransport", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli, "AlpacaMarketDataProvider", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli, "SchwabPriceHistoryProvider", schwab_provider)
    monkeypatch.setattr(cli, "resolve_agent_paper_market_scope", resolve)
    monkeypatch.setattr(cli, "collect_agent_paper_window", collect)

    settings = cli.AgentPaperMarketSettings(
        database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus",
        object_store_endpoint="http://127.0.0.1:9000",
        alpaca_key=SecretStr("key"),
        alpaca_secret=SecretStr("secret"),
    )
    result = asyncio.run(
        cli.collect_window_once(
            settings,
            AgentPaperCollectWindowRequest(
                action="collect-window",
                cycle_id=cycle_id,
                instrument_id=instrument_id,
            ),
            repo_root=tmp_path,
            **({"process_environment": environment} if schwab else {}),
        )
    )

    assert result is expected


def test_trigger_and_resume_requests_exclude_client_time_symbol_and_provider() -> None:
    account_id = UUID(int=1)
    cycle_id = UUID(int=2)

    trigger = AgentPaperTriggerRequest(
        action="trigger",
        account_id=account_id,
        idempotency_key="trigger-1",
    )
    resume = AgentPaperResumeRequest(
        action="resume",
        cycle_id=cycle_id,
        idempotency_key="resume-1",
    )

    assert trigger.account_id == account_id
    assert resume.cycle_id == cycle_id
    with pytest.raises(ValidationError):
        AgentPaperTriggerRequest.model_validate(
            {
                "action": "trigger",
                "account_id": account_id,
                "idempotency_key": "trigger-1",
                "decision_time": datetime(2026, 8, 26, 19, tzinfo=UTC),
            }
        )
    for command, identifier in (("trigger", "--account-id"), ("resume", "--cycle-id")):
        parsed = parser().parse_args(
            [command, identifier, str(account_id), "--idempotency-key", f"{command}-1"]
        )
        assert parsed.action == command
        with pytest.raises(SystemExit):
            parser().parse_args(
                [
                    command,
                    identifier,
                    str(account_id),
                    "--idempotency-key",
                    f"{command}-1",
                    "--symbol",
                    "AAPL",
                ]
            )


def test_trigger_orders_provider_facts_before_database_freeze_and_cycle(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    account_id = UUID(int=1)
    portfolio_id = UUID(int=2)
    universe_id = UUID(int=3)
    instrument_id = UUID(int=4)
    artifact_id = UUID(int=5)
    quote_id = UUID(int=6)
    cycle_id = UUID(int=7)
    initial = datetime(2026, 9, 14, 14, 0, tzinfo=UTC)
    frozen = datetime(2026, 9, 14, 14, 0, 10, tzinfo=UTC)
    events: list[str] = []

    class Session:
        calls = 0

        async def scalar(self, _statement):
            self.calls += 1
            events.append(f"clock-{self.calls}")
            return initial if self.calls == 1 else frozen

        async def get(self, model, _identifier):
            identifier = quote_id if model is cli.LatestQuote else artifact_id
            return SimpleNamespace(id=identifier)

    authority = CycleAuthority(
        account_id=account_id,
        portfolio_id=portfolio_id,
        decision_session=initial.astimezone(ZoneInfo("America/New_York")).date(),
        decision_time=initial,
        execution_session=initial.astimezone(ZoneInfo("America/New_York")).date(),
        calendar_authority_id=UUID(int=8),
        universe_snapshot_id=universe_id,
    )
    reservation = AgentPaperTriggerReservation(
        claim_id=UUID(int=9),
        decision_session=authority.decision_session,
        decision_slot_at=initial,
    )

    async def root(*_args, **_kwargs):
        assert _kwargs["expected_calendar_authority_id"] == authority.calendar_authority_id
        root_number = events.count("root")
        events.append("root")
        return authority.model_copy(
            update={"decision_time": initial if root_number == 0 else frozen}
        )

    async def select_calendar(*_args, **_kwargs):
        events.append("select-calendar")
        return SimpleNamespace(calendar_authority_id=authority.calendar_authority_id)

    async def load_service_calendar(*_args, **kwargs):
        assert kwargs["expected_calendar_authority_id"] == authority.calendar_authority_id
        current = kwargs["now"]
        events.append("service-calendar")
        return ServiceCalendarAuthority(
            calendar_authority_id=authority.calendar_authority_id,
            as_of=current,
            local_date=authority.decision_session,
            session=authority.decision_session,
            decision_window_start_at=initial - cli.timedelta(hours=3),
            market_open_at=initial - cli.timedelta(minutes=30),
            market_close_at=initial + cli.timedelta(hours=6),
            next_session=authority.decision_session + cli.timedelta(days=1),
            next_decision_window_start_at=initial + cli.timedelta(hours=21),
            next_market_close_at=initial + cli.timedelta(days=1, hours=6),
        )

    async def scope(*_args, **_kwargs):
        events.append("scope")
        return RiskQueryScope(
            instrument_id,
            universe_id,
            (object(), object(), object()),  # type: ignore[arg-type]
        )

    async def risk(**_kwargs):
        events.append("risk-action-delisting-halt")
        return tuple(
            RiskSourceProof(kind, False, "a" * 64, initial)
            for kind in ("corporate_action", "delisting", "suspension")
        )

    async def level_one(*_args, **_kwargs):
        assert _kwargs["process_environment"] == {"NEXUS_SCHWAB_APP_KEY": "registered-key"}
        events.append("level-one")
        return SchwabMarketStreamImportResult("b" * 64, 1, 1, artifact_id, {"AAPL": quote_id}, {})

    async def persist(*_args, **_kwargs):
        events.append("persist-risk")
        return ()

    async def start(*_args, **kwargs):
        assert kwargs["decision_slot_at"] == initial
        events.append("cycle-job")
        return AgentPaperCycleResult(
            cycle_id=cycle_id,
            status="researching",
            result_code="RESEARCH_JOBS_PREPARED",
            replayed=False,
        )

    monkeypatch.setattr(cli, "load_quasi_realtime_cycle_authority", root)
    monkeypatch.setattr(
        cli,
        "select_agent_paper_service_calendar_authority",
        select_calendar,
    )
    monkeypatch.setattr(
        cli,
        "load_agent_paper_service_calendar_authority",
        load_service_calendar,
    )
    monkeypatch.setattr(cli, "inspect_query_authority_scope", scope)
    monkeypatch.setattr(cli, "collect_source_proofs", risk)
    monkeypatch.setattr(cli, "capture_and_import_schwab_level_one_once", level_one)
    monkeypatch.setattr(cli, "persist_query_authorities", persist)
    monkeypatch.setattr(
        cli,
        "_validate_trigger_quote",
        lambda *_args, **_kwargs: events.append("validate-l1"),
    )
    monkeypatch.setattr(cli, "start_schwab_agent_paper_cycle", start)
    settings = cli.AgentPaperTriggerSettings(
        database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus",
        alpaca_key=SecretStr("key"),
        alpaca_secret=SecretStr("secret"),
    )

    result = asyncio.run(
        trigger_in_session(
            Session(),
            object(),
            settings,
            AgentPaperTriggerRequest(
                action="trigger", account_id=account_id, idempotency_key="trigger-1"
            ),
            reservation=reservation,
            repo_root=tmp_path,
            client=object(),
            process_environment={"NEXUS_SCHWAB_APP_KEY": "registered-key"},
        )
    )

    assert result.cycle_id == cycle_id
    assert events == [
        "clock-1",
        "select-calendar",
        "service-calendar",
        "root",
        "scope",
        "risk-action-delisting-halt",
        "level-one",
        "clock-2",
        "service-calendar",
        "validate-l1",
        "persist-risk",
        "root",
        "cycle-job",
    ]

    async def rejected_level_one(*_args, **_kwargs):
        raise cli.ProviderMarketStreamError("SCHWAB_MARKET_STREAM_EMPTY")

    monkeypatch.setattr(cli, "capture_and_import_schwab_level_one_once", rejected_level_one)

    async def forbidden_calendar_reselection(*_args, **_kwargs):
        raise AssertionError("service-bound calendar authority must not be reselected")

    monkeypatch.setattr(
        cli,
        "select_agent_paper_service_calendar_authority",
        forbidden_calendar_reselection,
    )
    failed_request = AgentPaperTriggerRequest(
        action="trigger", account_id=account_id, idempotency_key="trigger-failure"
    )
    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        asyncio.run(
            trigger_in_session(
                Session(),
                object(),
                settings,
                failed_request,
                reservation=reservation,
                repo_root=tmp_path,
                client=object(),
                expected_calendar_authority_id=authority.calendar_authority_id,
            )
        )
    digest = cli.sha256(failed_request.idempotency_key.encode()).hexdigest()[:16]
    diagnostic = (
        tmp_path
        / ".secrets"
        / "nexus-agent-paper"
        / f"{reservation.decision_slot_at.strftime('%Y%m%dT%H%MZ')}-{digest}"
        / "schwab-failure.json"
    )
    assert json.loads(diagnostic.read_text())["provider_error_code"] == (
        "SCHWAB_MARKET_STREAM_EMPTY"
    )

    for index, provider_error in enumerate(
        (
            SchwabOAuthError("SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED"),
            SchwabOAuthError("SCHWAB_OAUTH_REFRESH_HTTP_401"),
            SchwabStreamError("SCHWAB_STREAM_CREDENTIALS_INVALID"),
            SchwabStreamError("SCHWAB_STREAM_FRAME_INVALID"),
            RuntimeError("SCHWAB_MARKET_STREAM_OBJECT_READBACK_MISMATCH"),
            OSError("private filesystem detail"),
        ),
        start=1,
    ):

        async def rejected_before_archive(*_args, _error=provider_error, **_kwargs):
            raise _error

        monkeypatch.setattr(
            cli, "capture_and_import_schwab_level_one_once", rejected_before_archive
        )
        request = AgentPaperTriggerRequest(
            action="trigger",
            account_id=account_id,
            idempotency_key=f"trigger-provider-failure-{index}",
        )
        with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
            asyncio.run(
                trigger_in_session(
                    Session(),
                    object(),
                    settings,
                    request,
                    reservation=reservation,
                    repo_root=tmp_path,
                    client=object(),
                    expected_calendar_authority_id=authority.calendar_authority_id,
                )
            )
        attempt = cli.sha256(request.idempotency_key.encode()).hexdigest()[:16]
        saved = (
            tmp_path
            / ".secrets"
            / "nexus-agent-paper"
            / f"{reservation.decision_slot_at.strftime('%Y%m%dT%H%MZ')}-{attempt}"
            / "schwab-failure.json"
        )
        expected_code = (
            "SCHWAB_MARKET_STREAM_FAILURE_UNCLASSIFIED"
            if isinstance(provider_error, OSError)
            else str(provider_error)
        )
        assert json.loads(saved.read_text())["provider_error_code"] == expected_code


def test_trigger_rejects_after_bound_half_day_close_before_provider_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calendar_authority_id = UUID(int=8)
    after_close = datetime(2026, 11, 27, 18, 1, tzinfo=UTC)

    class Session:
        async def scalar(self, _statement):
            return after_close

    async def load_calendar(*_args, **_kwargs):
        return ServiceCalendarAuthority(
            calendar_authority_id=calendar_authority_id,
            as_of=after_close,
            local_date=after_close.date(),
            session=after_close.date(),
            decision_window_start_at=datetime(2026, 11, 27, 12, tzinfo=UTC),
            market_open_at=datetime(2026, 11, 27, 14, 30, tzinfo=UTC),
            market_close_at=datetime(2026, 11, 27, 18, tzinfo=UTC),
            next_session=datetime(2026, 11, 30, tzinfo=UTC).date(),
            next_decision_window_start_at=datetime(2026, 11, 30, 12, tzinfo=UTC),
            next_market_close_at=datetime(2026, 11, 30, 21, tzinfo=UTC),
        )

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("closed bound session must not call a provider or create a cycle")

    monkeypatch.setattr(cli, "load_agent_paper_service_calendar_authority", load_calendar)
    monkeypatch.setattr(cli, "load_quasi_realtime_cycle_authority", forbidden)
    monkeypatch.setattr(cli, "collect_source_proofs", forbidden)
    monkeypatch.setattr(cli, "capture_and_import_schwab_level_one_once", forbidden)

    with pytest.raises(ValueError, match="AGENT_PAPER_OUTSIDE_WINDOW"):
        asyncio.run(
            trigger_in_session(
                Session(),
                object(),
                cli.AgentPaperTriggerSettings(
                    database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus",
                    alpaca_key=SecretStr("key"),
                    alpaca_secret=SecretStr("secret"),
                ),
                AgentPaperTriggerRequest(
                    action="trigger",
                    account_id=UUID(int=1),
                    idempotency_key="half-day-close",
                ),
                reservation=AgentPaperTriggerReservation(
                    claim_id=UUID(int=9),
                    decision_session=after_close.date(),
                    decision_slot_at=after_close,
                ),
                repo_root=tmp_path,
                client=object(),
                expected_calendar_authority_id=calendar_authority_id,
            )
        )


def test_trigger_replay_returns_before_any_provider_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing = AgentPaperCycleResult(
        cycle_id=UUID(int=9),
        status="researching",
        result_code="RESEARCH_JOBS_PREPARED",
        replayed=True,
    )

    reservation = AgentPaperTriggerReservation(
        claim_id=UUID(int=8),
        decision_session=datetime(2026, 9, 14, tzinfo=UTC).date(),
        decision_slot_at=datetime(2026, 9, 14, 14, tzinfo=UTC),
        replay=existing,
    )

    class Transaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *_args):
            return None

    class Session:
        async def scalar(self, _statement):
            return datetime(2026, 9, 14, 14, tzinfo=UTC)

        def begin(self):
            return Transaction()

    class SessionContext:
        async def __aenter__(self):
            return Session()

        async def __aexit__(self, *_args):
            return None

    class Engine:
        async def dispose(self):
            return None

    class Factory:
        def __call__(self):
            return SessionContext()

    async def reserve(*_args, **_kwargs):
        return reservation

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("provider must not be called for a persisted cycle")

    monkeypatch.setattr(cli, "create_async_engine", lambda *_args, **_kwargs: Engine())
    monkeypatch.setattr(cli, "async_sessionmaker", lambda *_args, **_kwargs: Factory())
    monkeypatch.setattr(cli, "reserve_schwab_agent_paper_trigger", reserve, raising=False)
    monkeypatch.setattr(cli, "trigger_in_session", forbidden)
    monkeypatch.setattr(cli, "S3ProviderEvidenceStore", forbidden)
    monkeypatch.setattr(cli.httpx, "AsyncClient", forbidden)
    settings = cli.AgentPaperTriggerSettings(
        database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus",
        object_store_endpoint="http://127.0.0.1:9000",
        alpaca_key=SecretStr("key"),
        alpaca_secret=SecretStr("secret"),
    )

    replay = asyncio.run(
        cli.trigger_once(
            settings,
            AgentPaperTriggerRequest(
                action="trigger", account_id=UUID(int=1), idempotency_key="trigger-replay"
            ),
            repo_root=Path("/unused"),
        )
    )

    assert replay == existing


def test_trigger_quote_distinguishes_incomplete_from_stale() -> None:
    received = datetime(2026, 9, 14, 14, tzinfo=UTC)
    quote_id = UUID(int=10)
    artifact_id = UUID(int=11)
    artifact = SimpleNamespace(
        id=artifact_id,
        source="schwab",
        schema_version="schwab_level_one_stream_v1",
        created_at=received,
        received_at=received,
    )
    quote = SimpleNamespace(
        id=quote_id,
        data_artifact_id=artifact_id,
        source="schwab",
        feed="l1",
        delivery_mode="real_time",
        event_kind="quote",
        evidence_status="verified",
        bid=100,
        ask=101,
        created_at=received,
        observed_at=received,
        received_at=received,
    )
    imported = SchwabMarketStreamImportResult("c" * 64, 1, 1, artifact_id, {"AAPL": quote_id}, {})

    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        cli._validate_trigger_quote(
            SimpleNamespace(**{**vars(quote), "ask": None}),
            artifact,
            imported,
            decision_time=received,
        )
    with pytest.raises(ValueError, match="SCHWAB_L1_FRESHNESS_UNPROVEN"):
        cli._validate_trigger_quote(
            quote,
            artifact,
            imported,
            decision_time=received.replace(minute=2),
        )


def test_schwab_failure_preserves_only_safe_root_cause_diagnostic(tmp_path) -> None:
    diagnostic = tmp_path / "schwab-failure.json"

    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        cli._raise_schwab_l1_incomplete(
            cli.ProviderMarketStreamError("SCHWAB_MARKET_STREAM_EMPTY"),
            diagnostic_path=diagnostic,
        )

    assert json.loads(diagnostic.read_text()) == {
        "provider_error_code": "SCHWAB_MARKET_STREAM_EMPTY",
        "schema": "nexus_schwab_l1_failure_v1",
        "stage": "capture_or_import",
    }

    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        cli._raise_schwab_l1_incomplete(
            cli.ProviderMarketStreamError("private upstream detail"),
            diagnostic_path=tmp_path / "redacted.json",
        )
    assert json.loads((tmp_path / "redacted.json").read_text())["provider_error_code"] == (
        "SCHWAB_MARKET_STREAM_FAILURE_UNCLASSIFIED"
    )

    blocked_parent = tmp_path / "blocked"
    blocked_parent.write_text("not a directory")
    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        cli._raise_schwab_l1_incomplete(
            cli.ProviderMarketStreamError("SCHWAB_MARKET_STREAM_EMPTY"),
            diagnostic_path=blocked_parent / "schwab-failure.json",
        )
