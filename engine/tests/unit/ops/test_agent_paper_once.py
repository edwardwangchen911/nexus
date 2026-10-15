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
    AgentPaperPrepareCandidatesRequest,
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
from nexus.paper.candidate_batch import (
    CandidateBatchResult,
    CandidateInstrumentIdentity,
)
from nexus.persistence.models.paper import (
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
)
from nexus.providers.market.schwab_oauth import SchwabOAuthError
from nexus.providers.market.schwab_stream import SchwabStreamError
from nexus.risk.query_authority_producer import RiskQueryScope, RiskSourceProof
from nexus.settings import EngineSettings
from pydantic import SecretStr, ValidationError


def test_trigger_requires_explicit_batch_before_configuration_or_provider() -> None:
    with pytest.raises(ValueError, match="CANDIDATE_BATCH_REQUIRED"):
        AgentPaperTriggerRequest(
            action="trigger", account_id=UUID(int=1), idempotency_key="missing"
        )


def test_trigger_accepts_explicit_candidate_batch() -> None:
    request = AgentPaperTriggerRequest(
        action="trigger",
        account_id=UUID(int=1),
        candidate_batch_id=UUID(int=20),
        idempotency_key="explicit",
    )
    assert request.candidate_batch_id == UUID(int=20)


@pytest.mark.parametrize(
    ("command", "expected_mode"),
    [
        ("trigger-manual-recovery", "manual_recovery"),
        ("trigger-synthesis-recovery", "synthesis_recovery"),
    ],
)
def test_manual_recovery_command_marks_new_trigger_mode(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
    expected_mode: str,
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
    observed: list[str] = []

    async def trigger(_settings, request, **_kwargs):
        observed.append(request.trigger_mode)
        return AgentPaperCycleResult(
            cycle_id=UUID(int=7),
            status="researching",
            result_code="RESEARCH_JOBS_PREPARED",
            replayed=False,
        )

    monkeypatch.setattr(cli, "trigger_once", trigger)
    assert (
        main(
            [
                command,
                "--candidate-batch-id",
                str(UUID(int=20)),
                "--account-id",
                str(UUID(int=1)),
                "--idempotency-key",
                "manual-once",
            ]
        )
        == 0
    )
    assert observed == [expected_mode]
    assert json.loads(capsys.readouterr().out)["status"] == "researching"


def test_request_requires_explicit_utc_time() -> None:
    with pytest.raises(ValueError, match="explicit UTC time is required"):
        AgentPaperRunRequest(
            action="run",
            account_id=UUID(int=1),
            decision_time=datetime(2026, 8, 26, 21),
            idempotency_key="cycle-1",
        )


def test_prepare_candidates_parser_accepts_only_explicit_instrument_ids() -> None:
    parsed = parser().parse_args(
        [
            "prepare-candidates",
            "--account-id",
            str(UUID(int=1)),
            "--universe-snapshot-id",
            str(UUID(int=2)),
            "--instrument-id",
            str(UUID(int=4)),
            "--instrument-id",
            str(UUID(int=3)),
            "--idempotency-key",
            "scope-1",
        ]
    )

    request = AgentPaperPrepareCandidatesRequest.model_validate(vars(parsed))
    assert request.instrument_ids == (UUID(int=4), UUID(int=3))
    with pytest.raises(SystemExit):
        parser().parse_args(
            [
                "prepare-candidates",
                "--account-id",
                str(UUID(int=1)),
                "--universe-snapshot-id",
                str(UUID(int=2)),
                "--symbol",
                "AAPL",
                "--idempotency-key",
                "scope-1",
            ]
        )


def test_initial_target_commands_require_exact_projection_identity() -> None:
    account_id = UUID(int=1)
    projection_as_of = datetime(2026, 9, 18, 20, tzinfo=UTC)
    inspect = parser().parse_args(
        [
            "inspect-projection",
            "--account-id",
            str(account_id),
            "--as-of",
            projection_as_of.isoformat(),
        ]
    )
    assert cli.AgentPaperInspectProjectionRequest.model_validate(vars(inspect)).as_of == (
        projection_as_of
    )

    approve = parser().parse_args(
        [
            "approve-initial-target",
            "--account-id",
            str(account_id),
            "--projection-as-of",
            projection_as_of.isoformat(),
            "--projection-hash",
            "a" * 64,
            "--approved-by",
            "local-user",
            "--approved-at",
            projection_as_of.isoformat(),
            "--idempotency-key",
            "initial-baseline-20260918",
        ]
    )
    request = cli.AgentPaperApproveInitialTargetRequest.model_validate(vars(approve))
    assert request.expected_projection_hash == "a" * 64
    assert request.projection_as_of == request.approved_at == projection_as_of


def test_initial_target_commands_reject_naive_time_and_invalid_hash() -> None:
    with pytest.raises(ValidationError):
        cli.AgentPaperInspectProjectionRequest(
            action="inspect-projection",
            account_id=UUID(int=1),
            as_of=datetime(2026, 9, 18, 20),
        )
    with pytest.raises(ValidationError):
        cli.AgentPaperApproveInitialTargetRequest(
            action="approve-initial-target",
            account_id=UUID(int=1),
            projection_as_of=datetime(2026, 9, 18, 20, tzinfo=UTC),
            expected_projection_hash="not-a-hash",
            approved_by="local-user",
            approved_at=datetime(2026, 9, 18, 20, tzinfo=UTC),
            idempotency_key="initial-baseline",
        )


def test_prepare_candidates_resolves_display_identities_before_creation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    now = datetime(2026, 9, 18, 14, tzinfo=UTC)
    account_id, universe_id, instrument_id, batch_id = (
        UUID(int=1),
        UUID(int=2),
        UUID(int=3),
        UUID(int=4),
    )
    events: list[str] = []

    class Session:
        async def scalar(self, _statement):
            return now

        def begin(self):
            return Transaction()

    class Transaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *_args):
            return None

    class SessionContext:
        async def __aenter__(self):
            return Session()

        async def __aexit__(self, *_args):
            return None

    class Factory:
        def __call__(self):
            return SessionContext()

    class Engine:
        async def dispose(self):
            return None

    identity = CandidateInstrumentIdentity(
        instrument_id=instrument_id,
        symbol="AAPL",
        asset_type="common_stock",
        exchange="XNAS",
        currency="USD",
    )
    batch = CandidateBatchResult(
        batch_id=batch_id,
        account_id=account_id,
        universe_snapshot_id=universe_id,
        instrument_ids=(instrument_id,),
        source="explicit_research_scope",
        batch_hash="a" * 64,
        created_at=now,
    )

    async def preflight(*_args, **kwargs):
        events.append("preflight")
        assert kwargs == {
            "account_id": account_id,
            "universe_snapshot_id": universe_id,
            "instrument_ids": (instrument_id,),
            "idempotency_key": "scope-1",
        }
        return None

    async def resolve(*_args, **kwargs):
        events.append("resolve-identities")
        assert kwargs == {
            "universe_snapshot_id": universe_id,
            "instrument_ids": (instrument_id,),
            "created_at": now,
        }
        return (identity,)

    async def create(*_args, **kwargs):
        assert json.loads(capsys.readouterr().err) == {
            "candidate_identities": [identity.model_dump(mode="json")]
        }
        events.append("create-batch")
        assert kwargs["account_id"] == account_id
        assert kwargs["universe_snapshot_id"] == universe_id
        assert kwargs["instrument_ids"] == (instrument_id,)
        assert kwargs["created_at"] == now
        return batch

    monkeypatch.setattr(cli, "create_async_engine", lambda *_args, **_kwargs: Engine())
    monkeypatch.setattr(cli, "async_sessionmaker", lambda *_args, **_kwargs: Factory())
    monkeypatch.setattr(cli, "preflight_agent_paper_candidate_batch", preflight)
    monkeypatch.setattr(cli, "resolve_agent_paper_candidate_identities", resolve)
    monkeypatch.setattr(cli, "create_agent_paper_candidate_batch", create)

    result = asyncio.run(
        cli.prepare_candidates_once(
            EngineSettings(database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus"),
            AgentPaperPrepareCandidatesRequest(
                action="prepare-candidates",
                account_id=account_id,
                universe_snapshot_id=universe_id,
                instrument_ids=(instrument_id,),
                idempotency_key="scope-1",
            ),
        )
    )

    assert events == ["preflight", "resolve-identities", "create-batch"]
    assert result.batch == batch
    assert result.identities == (identity,)


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
        "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED",
        "AGENT_PAPER_PROVIDER_CAPACITY_INVALID",
        "ECONOMIC_PROTOCOL_DATABASE_MISMATCH",
        "ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN",
        "ECONOMIC_WINDOW_COLLECTION_TOO_EARLY",
        "ECONOMIC_WINDOW_COLLECTION_UNKNOWN",
        "ECONOMIC_WINDOW_COLLECTION_FAILED",
    ],
)
def test_trigger_command_exposes_safe_failure_codes(
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
            "--candidate-batch-id",
            str(UUID(int=20)),
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


@pytest.mark.parametrize("action", ["run-research", "resume"])
def test_formal_step_parser_requires_explicit_opt_in(action: str) -> None:
    from nexus.ops.agent_paper_once import AgentPaperResumeRequest, AgentPaperRunResearchRequest

    arguments = [action, "--cycle-id", str(UUID(int=1))]
    if action == "resume":
        arguments += ["--idempotency-key", "formal-step"]
    request_type = AgentPaperResumeRequest if action == "resume" else AgentPaperRunResearchRequest
    default = request_type.model_validate(vars(parser().parse_args(arguments)))
    explicit = request_type.model_validate(
        vars(parser().parse_args(arguments + ["--formal-stage-capacity"]))
    )
    assert default.formal_stage_capacity is False
    assert explicit.formal_stage_capacity is True
    for bad in (1, "true", None):
        with pytest.raises(ValueError):
            request_type.model_validate({**default.model_dump(), "formal_stage_capacity": bad})


def test_formal_trigger_parser_requires_exact_model_and_scheduled_mode() -> None:
    arguments = [
        "trigger",
        "--account-id",
        str(UUID(int=1)),
        "--candidate-batch-id",
        str(UUID(int=2)),
        "--idempotency-key",
        "formal-trigger",
    ]
    plain = AgentPaperTriggerRequest.model_validate(vars(parser().parse_args(arguments)))
    assert plain.formal_stage_capacity is False
    assert plain.formal_model_config_id is None
    explicit = AgentPaperTriggerRequest.model_validate(
        vars(
            parser().parse_args(
                arguments
                + ["--formal-stage-capacity", "--formal-model-config-id", str(UUID(int=3))]
            )
        )
    )
    assert explicit.formal_model_config_id == UUID(int=3)
    for change in (
        {"formal_model_config_id": None},
        {"formal_stage_capacity": False},
        {"formal_stage_capacity": 1},
        {"trigger_mode": "manual_recovery"},
        {"trigger_mode": "synthesis_recovery"},
    ):
        with pytest.raises(ValueError):
            AgentPaperTriggerRequest.model_validate(explicit.model_dump() | change)


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


def test_run_research_selects_next_unstarted_job_after_confirmed_output() -> None:
    first, second = UUID(int=10), UUID(int=11)
    events = (
        SimpleNamespace(
            to_status="researching", fact_ids={"research_run_ids": [str(first), str(second)]}
        ),
    )
    done = SimpleNamespace(
        id=UUID(int=20),
        job_type="agent.paper.research",
        status="succeeded",
        payload={"run_id": str(first)},
    )
    queued = SimpleNamespace(
        id=UUID(int=21),
        job_type="agent.paper.research",
        status="queued",
        payload={"run_id": str(second)},
        attempt_count=0,
        manual_retry_count=0,
        max_attempts=1,
        started_at=None,
        finished_at=None,
    )
    assert cli.resolve_queued_cycle_research_job(
        events=events, jobs=(queued, done), output_run_ids=(first,)
    ) == UUID(int=21)
    with pytest.raises(ValueError, match="AGENT_PAPER_RESULT_UNKNOWN"):
        cli.resolve_queued_cycle_research_job(events=events, jobs=(queued, done), output_run_ids=())


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

    async def run_worker(_settings: object, *, job_id) -> bool:
        received.append((job_id,))
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
    monkeypatch.setattr(cli, "run_cycle_job_once", run_worker)

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
            class Session:
                async def scalar(self, statement: object) -> bool:
                    assert "to_regclass('economic_lane_cycles')" in str(statement)
                    return False

            return AsyncContext(Session())

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
        candidate_batch_id=UUID(int=20),
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


@pytest.mark.parametrize(
    ("two_candidates", "max_symbols"),
    [(False, 12), (True, 12), (True, 2)],
)
@pytest.mark.parametrize("window_case", ["scheduled", "manual", "preopen", "close_during_collect"])
def test_trigger_orders_provider_facts_before_database_freeze_and_cycle(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    two_candidates: bool,
    max_symbols: int,
    window_case: str,
) -> None:
    account_id = UUID(int=1)
    portfolio_id = UUID(int=2)
    universe_id = UUID(int=3)
    instrument_id = UUID(int=4)
    bindings = {"AAPL": instrument_id, **({"MSFT": UUID(int=14)} if two_candidates else {})}
    inherited_position_id = UUID(int=24)
    execution_bindings = {**bindings, "GOOGL": inherited_position_id}
    artifact_id = UUID(int=5)
    quote_id = UUID(int=6)
    cycle_id = UUID(int=7)
    manual = window_case != "scheduled"
    initial = datetime(2026, 9, 14, 13 if manual else 14, 40 if manual else 0, tzinfo=UTC)
    if window_case == "preopen":
        initial = initial.replace(minute=29)
    frozen = initial + cli.timedelta(seconds=10)
    if window_case == "close_during_collect":
        frozen = initial.replace(hour=20, minute=0)
    request_scope = (
        {}
        if not manual
        else {
            "trigger_mode": "manual",
            "manual_request_id": UUID(int=30),
            "formal_stage_capacity": True,
            "formal_model_config_id": UUID(int=31),
        }
    )

    async def formal_profile(*_args, **kwargs):
        assert kwargs["model_config_id"] == UUID(int=31)
        return SimpleNamespace(config_hash="c" * 64)

    async def manual_identity(*_args, **kwargs):
        assert kwargs["request_id"] == UUID(int=30)
        assert kwargs["batch_id"] == UUID(int=20)

    monkeypatch.setattr(
        "nexus.agents.codex_formal_stage.require_formal_stage_model_profile", formal_profile
    )
    monkeypatch.setattr(
        "nexus.paper.manual_request.require_preparing_manual_request", manual_identity
    )
    events: list[str] = []

    class Session:
        calls = 0

        async def scalar(self, _statement):
            self.calls += 1
            events.append(f"clock-{self.calls}")
            if self.calls == 1:
                return initial
            if self.calls == 2:
                return None
            return frozen

        async def get(self, model, _identifier):
            identifier = quote_id if model is cli.LatestQuote else artifact_id
            return SimpleNamespace(id=identifier)

        async def execute(self, _statement):
            class Rows:
                def tuples(self):
                    return tuple(
                        (
                            SimpleNamespace(instrument_id=identity, symbol=symbol),
                            SimpleNamespace(
                                asset_type="common_stock", exchange="XNAS", currency="USD"
                            ),
                        )
                        for symbol, identity in execution_bindings.items()
                    )

            return Rows()

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
        candidate_batch_id=UUID(int=20),
        candidate_batch_hash="a" * 64,
        decision_session=authority.decision_session,
        decision_slot_at=initial,
        **request_scope,
        **({"formal_model_hash": "c" * 64} if manual else {}),
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
            market_open_at=initial.replace(hour=13, minute=30),
            market_close_at=initial.replace(hour=20, minute=0),
            next_session=authority.decision_session + cli.timedelta(days=1),
            next_decision_window_start_at=initial + cli.timedelta(hours=21),
            next_market_close_at=initial + cli.timedelta(days=1, hours=6),
        )

    async def scope(*_args, **_kwargs):
        events.append("scope")
        return RiskQueryScope(
            execution_bindings[_kwargs["symbol"]],
            universe_id,
            (object(), object(), object()),  # type: ignore[arg-type]
        )

    async def risk(**_kwargs):
        assert _kwargs["max_instruments"] == max_symbols
        assert {
            item.symbol: item.instrument_id for item in _kwargs["candidates"]
        } == execution_bindings
        events.append("risk-action-delisting-halt")
        return {
            identity: tuple(
                RiskSourceProof(kind, False, "a" * 64, initial)
                for kind in ("corporate_action", "delisting", "suspension")
            )
            for identity in execution_bindings.values()
        }

    async def level_one(*_args, **_kwargs):
        assert _kwargs["max_instruments"] == max_symbols
        assert _kwargs["bindings"] == execution_bindings
        assert _kwargs["process_environment"] == {"NEXUS_SCHWAB_APP_KEY": "registered-key"}
        events.append("level-one")
        return SchwabMarketStreamImportResult(
            "b" * 64,
            1,
            1,
            artifact_id,
            {symbol: quote_id for symbol in execution_bindings},
            {},
        )

    async def persist(*_args, **_kwargs):
        events.append("persist-risk")
        return ()

    async def start(*_args, **kwargs):
        assert kwargs["decision_slot_at"] == initial
        assert kwargs["requested_instrument_ids"] == tuple(bindings.values())
        assert kwargs["candidate_batch_id"] == UUID(int=20)
        assert kwargs["execution_scope_max_symbols"] == max_symbols
        events.append("cycle-job")
        return AgentPaperCycleResult(
            cycle_id=cycle_id,
            status="researching",
            result_code="RESEARCH_JOBS_PREPARED",
            replayed=False,
        )

    async def batch_scope(*_args, **kwargs):
        assert kwargs["batch_id"] == UUID(int=20)
        return (
            CandidateBatchResult(
                batch_id=UUID(int=20),
                account_id=account_id,
                universe_snapshot_id=universe_id,
                instrument_ids=tuple(bindings.values()),
                source="explicit_research_scope",
                batch_hash="a" * 64,
                created_at=initial,
            ),
            tuple(
                CandidateInstrumentIdentity(
                    instrument_id=identity,
                    symbol=symbol,
                    asset_type="common_stock",
                    exchange="XNAS",
                    currency="USD",
                )
                for symbol, identity in bindings.items()
            ),
        )

    monkeypatch.setattr(cli, "validate_agent_paper_candidate_batch", batch_scope)

    async def projection(*_args, **_kwargs):
        return SimpleNamespace(
            positions=(SimpleNamespace(instrument_id=inherited_position_id),), open_orders=()
        )

    monkeypatch.setattr("nexus.paper.projection.load_paper_projection", projection)
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
    monkeypatch.setattr(cli, "collect_source_proofs_batch", risk)
    monkeypatch.setattr(cli, "capture_and_import_schwab_level_one_batch", level_one)
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
        intraday_max_symbols=max_symbols,
    )

    request = AgentPaperTriggerRequest(
        candidate_batch_id=UUID(int=20),
        action="trigger",
        account_id=account_id,
        idempotency_key="trigger-1",
        **request_scope,
    )
    if window_case == "preopen" or (
        window_case == "close_during_collect" and max_symbols >= len(execution_bindings)
    ):
        with pytest.raises(ValueError, match="^AGENT_PAPER_OUTSIDE_WINDOW$"):
            asyncio.run(
                trigger_in_session(
                    Session(),
                    object(),
                    settings,
                    request,
                    reservation=reservation,
                    repo_root=tmp_path,
                    client=object(),
                    process_environment={"NEXUS_SCHWAB_APP_KEY": "registered-key"},
                )
            )
        assert "persist-risk" not in events and "cycle-job" not in events
        if window_case == "preopen":
            assert "risk-action-delisting-halt" not in events and "level-one" not in events
        else:
            assert "level-one" in events
        return
    if max_symbols < len(execution_bindings):
        with pytest.raises(ValueError, match="^AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED$"):
            asyncio.run(
                trigger_in_session(
                    Session(),
                    object(),
                    settings,
                    request,
                    reservation=reservation,
                    repo_root=tmp_path,
                    client=object(),
                    process_environment={"NEXUS_SCHWAB_APP_KEY": "registered-key"},
                )
            )
        assert events == [
            "clock-1",
            "select-calendar",
            "service-calendar",
            "root",
            "clock-2",
        ]
        return

    result = asyncio.run(
        trigger_in_session(
            Session(),
            object(),
            settings,
            request,
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
        "clock-2",
        *(["scope"] * len(execution_bindings)),
        "risk-action-delisting-halt",
        "level-one",
        "clock-3",
        "service-calendar",
        *(["validate-l1"] * len(execution_bindings)),
        *(["persist-risk"] * len(execution_bindings)),
        "root",
        "cycle-job",
    ]

    async def rejected_level_one(*_args, **_kwargs):
        raise cli.ProviderMarketStreamError("SCHWAB_MARKET_STREAM_EMPTY")

    monkeypatch.setattr(cli, "capture_and_import_schwab_level_one_batch", rejected_level_one)

    async def forbidden_calendar_reselection(*_args, **_kwargs):
        raise AssertionError("service-bound calendar authority must not be reselected")

    monkeypatch.setattr(
        cli,
        "select_agent_paper_service_calendar_authority",
        forbidden_calendar_reselection,
    )
    failed_request = AgentPaperTriggerRequest(
        candidate_batch_id=UUID(int=20),
        action="trigger",
        account_id=account_id,
        idempotency_key="trigger-failure",
        **request_scope,
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
            cli, "capture_and_import_schwab_level_one_batch", rejected_before_archive
        )
        request = AgentPaperTriggerRequest(
            candidate_batch_id=UUID(int=20),
            action="trigger",
            account_id=account_id,
            idempotency_key=f"trigger-provider-failure-{index}",
            **request_scope,
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

    async def batch(*_args, **_kwargs):
        return SimpleNamespace(batch_id=None, batch_hash=None, universe_snapshot_id=UUID(int=3)), ()

    monkeypatch.setattr(cli, "validate_agent_paper_candidate_batch", batch)

    monkeypatch.setattr(cli, "load_agent_paper_service_calendar_authority", load_calendar)
    monkeypatch.setattr(cli, "load_quasi_realtime_cycle_authority", forbidden)
    monkeypatch.setattr(cli, "collect_source_proofs_batch", forbidden)
    monkeypatch.setattr(cli, "capture_and_import_schwab_level_one_batch", forbidden)

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
                    candidate_batch_id=UUID(int=20),
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
                candidate_batch_id=UUID(int=20),
                action="trigger",
                account_id=UUID(int=1),
                idempotency_key="trigger-replay",
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
        instrument_id=UUID(int=4),
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
            symbol="AAPL",
            instrument_id=UUID(int=4),
        )
    with pytest.raises(ValueError, match="SCHWAB_L1_FRESHNESS_UNPROVEN"):
        cli._validate_trigger_quote(
            quote,
            artifact,
            imported,
            decision_time=received.replace(minute=2),
            symbol="AAPL",
            instrument_id=UUID(int=4),
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

    archive_diagnostic = tmp_path / "archive-invalid.json"
    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        cli._raise_schwab_l1_incomplete(
            cli.ProviderMarketStreamError(
                "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID",
                failure_stage="archive_pack",
                validation_reason="CAPTURE_TIME_BOUND",
                capture_summary={
                    "status": "bounded_complete",
                    "stop_reason": "FRAME_LIMIT",
                    "authenticated": True,
                    "subscription_confirmed": True,
                    "observation_count": 1,
                    "frame_count": 3,
                    "origin": "must-not-be-persisted",
                },
            ),
            diagnostic_path=archive_diagnostic,
        )
    assert json.loads(archive_diagnostic.read_text()) == {
        "capture_summary": {
            "authenticated": True,
            "frame_count": 3,
            "observation_count": 1,
            "status": "bounded_complete",
            "stop_reason": "FRAME_LIMIT",
            "subscription_confirmed": True,
        },
        "provider_error_code": "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID",
        "schema": "nexus_schwab_l1_failure_v1",
        "stage": "archive_pack",
        "validation_reason": "CAPTURE_TIME_BOUND",
    }

    import_diagnostic = tmp_path / "stream-import-invalid.json"
    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        cli._raise_schwab_l1_incomplete(
            cli.ProviderMarketStreamError(
                "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID",
                failure_stage="stream_import",
                capture_summary={
                    "status": "bounded_complete",
                    "stop_reason": "FRAME_LIMIT",
                    "authenticated": True,
                    "subscription_confirmed": True,
                    "observation_count": 1,
                    "frame_count": 3,
                },
            ),
            diagnostic_path=import_diagnostic,
        )
    assert json.loads(import_diagnostic.read_text()) == {
        "capture_summary": {
            "authenticated": True,
            "frame_count": 3,
            "observation_count": 1,
            "status": "bounded_complete",
            "stop_reason": "FRAME_LIMIT",
            "subscription_confirmed": True,
        },
        "provider_error_code": "PROVIDER_MARKET_STREAM_ARCHIVE_INVALID",
        "schema": "nexus_schwab_l1_failure_v1",
        "stage": "stream_import",
    }

    summarized = tmp_path / "summarized.json"
    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        cli._raise_schwab_l1_incomplete(
            cli.ProviderMarketStreamError(
                "PROVIDER_MARKET_STREAM_INCOMPLETE",
                capture_summary={
                    "status": "partial",
                    "stop_reason": "DISCONNECTED",
                    "authenticated": True,
                    "subscription_confirmed": True,
                    "observation_count": 0,
                    "frame_count": 2,
                    "origin": "must-not-be-persisted",
                },
            ),
            diagnostic_path=summarized,
        )
    assert json.loads(summarized.read_text()) == {
        "capture_summary": {
            "authenticated": True,
            "frame_count": 2,
            "observation_count": 0,
            "status": "partial",
            "stop_reason": "DISCONNECTED",
            "subscription_confirmed": True,
        },
        "provider_error_code": "PROVIDER_MARKET_STREAM_INCOMPLETE",
        "schema": "nexus_schwab_l1_failure_v1",
        "stage": "capture_or_import",
    }

    connect_diagnostic = tmp_path / "connect-failure.json"
    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        cli._raise_schwab_l1_incomplete(
            cli.ProviderMarketStreamError(
                "PROVIDER_MARKET_STREAM_INCOMPLETE",
                capture_summary={
                    "status": "failed",
                    "stop_reason": "CONNECT_PROXY_FAILED",
                    "authenticated": False,
                    "subscription_confirmed": False,
                    "observation_count": 0,
                    "frame_count": 0,
                    "origin": "must-not-be-persisted",
                },
            ),
            diagnostic_path=connect_diagnostic,
        )
    assert json.loads(connect_diagnostic.read_text())["capture_summary"] == {
        "authenticated": False,
        "frame_count": 0,
        "observation_count": 0,
        "status": "failed",
        "stop_reason": "CONNECT_PROXY_FAILED",
        "subscription_confirmed": False,
    }

    runtime_diagnostic = tmp_path / "connect-runtime-failure.json"
    with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
        cli._raise_schwab_l1_incomplete(
            cli.ProviderMarketStreamError(
                "PROVIDER_MARKET_STREAM_INCOMPLETE",
                capture_summary={
                    "status": "failed",
                    "stop_reason": "CONNECT_RUNTIME_FAILED",
                    "authenticated": False,
                    "subscription_confirmed": False,
                    "observation_count": 0,
                    "frame_count": 0,
                    "private_detail": "must-not-be-persisted",
                },
            ),
            diagnostic_path=runtime_diagnostic,
        )
    runtime_summary = json.loads(runtime_diagnostic.read_text())["capture_summary"]
    assert runtime_summary["stop_reason"] == "CONNECT_RUNTIME_FAILED"
    assert "must-not-be-persisted" not in runtime_diagnostic.read_text()

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


@pytest.mark.parametrize("source_code", ["CANDIDATE_BATCH_INVALID", "private-credential-canary"])
def test_trigger_reservation_failure_annotated_before_any_provider(monkeypatch, source_code):
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

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    class Engine:
        disposed = False

        async def dispose(self):
            self.disposed = True

    engine = Engine()

    async def reserve(*_args, **_kwargs):
        raise ValueError(source_code)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("provider work forbidden after reservation failure")

    monkeypatch.setattr(cli, "create_async_engine", lambda *_a, **_k: engine)
    monkeypatch.setattr(cli, "async_sessionmaker", lambda *_a, **_k: Session)
    monkeypatch.setattr(cli, "reserve_schwab_agent_paper_trigger", reserve)
    monkeypatch.setattr(cli, "S3ProviderEvidenceStore", forbidden)
    settings = cli.AgentPaperTriggerSettings(
        database_url="postgresql+asyncpg://u:p@127.0.0.1/nexus",
        object_store_endpoint="http://127.0.0.1:9000",
        alpaca_key=SecretStr("fixture"),
        alpaca_secret=SecretStr("fixture"),
    )
    with pytest.raises(ValueError) as caught:
        asyncio.run(
            cli.trigger_once(
                settings,
                AgentPaperTriggerRequest(
                    action="trigger",
                    account_id=UUID(int=1),
                    candidate_batch_id=UUID(int=2),
                    idempotency_key="diagnose-reservation",
                ),
                repo_root=Path("/unused"),
            )
        )
    assert caught.value._nexus_agent_paper_failure_phase == "trigger_reservation"
    assert caught.value._nexus_agent_paper_failure_code == (
        source_code if source_code == "CANDIDATE_BATCH_INVALID" else "MANUAL_RESULT_UNKNOWN"
    )
    assert engine.disposed


@pytest.mark.parametrize("cleanup_failure", [False, True])
@pytest.mark.parametrize("failure_boundary", ["trigger", "journal", "effects", "claim"])
def test_manual_failure_survives_unknown_record_write_and_cleanup_failure(
    monkeypatch, cleanup_failure, failure_boundary
):
    from nexus.ops import manual_paper_host as host
    from sqlalchemy.exc import SQLAlchemyError

    readbacks = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, *_args):
            readbacks.append(True)
            raise SQLAlchemyError("private-database-canary")

    class Engine:
        async def dispose(self):
            if cleanup_failure:
                raise RuntimeError("private-cleanup-canary")

    class Factory:
        kw = {"bind": Engine()}

        def begin(self):
            return Session()

    async def claim(*_args, **_kwargs):
        if failure_boundary == "claim":
            raise ValueError("MANUAL_NOT_FOUND")
        return None, True

    async def trigger(*_args, **_kwargs):
        error = ValueError("CANDIDATE_BATCH_INVALID")
        host.annotate_manual_trigger_failure(error, phase="trigger_reservation")
        raise error

    async def started(*_a, **_kw):
        if failure_boundary == "journal":
            raise SQLAlchemyError("private-journal-canary")
        return UUID(int=6)

    async def effects(*_a, **_kw):
        if failure_boundary == "effects":
            raise SQLAlchemyError("private-effects-canary")
        return None

    monkeypatch.setattr(host, "start_preparation_execution", started)
    monkeypatch.setattr(host, "begin_preparation_effects", effects)
    monkeypatch.setattr(host, "session_factory", lambda *_a: Factory())
    monkeypatch.setattr(host, "claim_configured_manual_request", claim)
    monkeypatch.setattr(cli, "trigger_once", trigger)
    settings = EngineSettings(
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=5),
    )
    trigger_settings = cli.AgentPaperTriggerSettings.model_validate(
        settings.model_dump()
        | {
            "database_url": "postgresql+asyncpg://u:p@127.0.0.1/nexus",
            "object_store_endpoint": "http://127.0.0.1:9000",
            "alpaca_key": "fixture",
            "alpaca_secret": "fixture",
        }
    )
    with pytest.raises(ValueError, match="^MANUAL_RESULT_UNKNOWN$") as caught:
        asyncio.run(
            host.dispatch_manual_request(
                settings,
                trigger_settings,
                request_id=UUID(int=3),
                repo_root=Path("/unused"),
                calendar_authority_id=UUID(int=4),
                process_environment={},
            )
        )
    assert caught.value._nexus_agent_paper_failure_phase == (
        "trigger_reservation"
        if failure_boundary == "trigger"
        else "preparation_journal"
        if failure_boundary == "claim"
        else "preparation_" + failure_boundary
    )
    assert caught.value._nexus_agent_paper_failure_code == (
        "CANDIDATE_BATCH_INVALID" if failure_boundary == "trigger" else "MANUAL_RESULT_UNKNOWN"
    )
    assert caught.value._nexus_agent_paper_failure_kind == (
        "validation" if failure_boundary in {"trigger", "claim"} else "database"
    )
    assert "private" not in str(caught.value)
    assert len(readbacks) == (0 if failure_boundary in {"claim", "journal"} else 1)
