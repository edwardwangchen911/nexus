"""Provider worker capability and queue isolation."""

from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.jobs.models import JobType, QueueName
from nexus.provider_worker import (
    _PROVIDER_POLICY_EXPRESSIONS,
    _PROVIDER_POLICY_FUNCTIONS,
    _PROVIDER_PRICE_BAR_GUARD_FUNCTION,
    _PROVIDER_PRICE_BAR_GUARD_TRIGGERS,
    _PROVIDER_PRIVILEGES,
    _PROVIDER_PUBLICATION_FUNCTION,
    _PROVIDER_PUBLICATION_TRIGGERS,
    _PROVIDER_TRIGGER_FUNCTIONS,
    _PROVIDER_TRIGGER_TOPOLOGY,
    _ROW_SCOPED_PROVIDER_TABLES,
    _assert_intraday_job_queue_drained,
    _assert_provider_migration_head,
    _assert_provider_transaction_isolation,
    _provider_http_client,
    _provider_policy_function_violation,
    _provider_policy_violation,
    _provider_privilege_violation,
    _provider_publication_function_violation,
    _provider_registry,
    _provider_trigger_topology_violation,
    run_provider_worker,
)
from nexus.providers.settings import ProviderSettings, ProviderWorkerSettings
from pydantic import ValidationError


def _settings(*, intraday: bool) -> ProviderSettings:
    return ProviderSettings(
        runtime_mode="production",
        intraday_monitor_mode="production" if intraday else "disabled",
        fosun_market_gateway_origin="https://fosun-gateway.internal:8443",
        fosun_market_gateway_key_id="synthetic-market-current",
        fosun_market_gateway_secret="synthetic-market-secret",
        fosun_market_gateway_client_cert=Path("/synthetic/market.crt"),
        fosun_market_gateway_client_key=Path("/synthetic/market.key"),
        fosun_market_gateway_client_ca=Path("/synthetic/ca.crt"),
        sec_contact="owner@example.com",
        fred_api_key="fred-canary",
    )


def test_provider_worker_http_client_does_not_trust_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def capture(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("nexus.provider_worker.httpx.AsyncClient", capture)

    _provider_http_client()

    assert captured == {
        "follow_redirects": False,
        "timeout": None,
        "trust_env": False,
    }


@pytest.mark.asyncio
async def test_intraday_queue_gate_fails_closed_on_actionable_v1_jobs() -> None:
    class _Session:
        async def scalar(self, _statement: object) -> int:
            return 1

    with pytest.raises(RuntimeError, match="INTRADAY_V1_QUEUE_DRAIN_REQUIRED"):
        await _assert_intraday_job_queue_drained(_Session())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_provider_worker_checks_legacy_intraday_queue_even_when_monitor_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gates = 0
    migration_checks = 0
    startup_order: list[str] = []

    class _Engine:
        async def dispose(self) -> None:
            return None

    class _SessionContext:
        async def __aenter__(self) -> object:
            return object()

        async def __aexit__(self, *_: object) -> None:
            return None

    class _Factory:
        def __call__(self) -> _SessionContext:
            return _SessionContext()

    async def database_role_ok(_session: object) -> None:
        startup_order.append("role")

    async def record_gate(_session: object) -> None:
        nonlocal gates
        gates += 1
        startup_order.append("gate")

    async def record_migration_check(_session: object) -> None:
        nonlocal migration_checks
        migration_checks += 1
        startup_order.append("head")

    monkeypatch.setattr(
        "nexus.provider_worker.create_async_engine",
        lambda *_args, **_kwargs: _Engine(),
    )
    monkeypatch.setattr(
        "nexus.provider_worker.async_sessionmaker",
        lambda *_args, **_kwargs: _Factory(),
    )
    monkeypatch.setattr(
        "nexus.provider_worker._assert_provider_database_role",
        database_role_ok,
    )
    monkeypatch.setattr(
        "nexus.provider_worker._assert_intraday_job_queue_drained",
        record_gate,
    )
    monkeypatch.setattr(
        "nexus.provider_worker._assert_provider_migration_head",
        record_migration_check,
    )

    await run_provider_worker(
        ProviderWorkerSettings.model_validate(
            {
                "PROVIDER_DATABASE_URL": "postgresql+asyncpg://provider.invalid/nexus",
                "PROVIDER_OBJECT_STORE_ENDPOINT": "https://provider-store.invalid",
                "PROVIDER_OBJECT_STORE_REGION": "us-east-1",
                "PROVIDER_OBJECT_STORE_BUCKET": "provider-evidence",
                "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": "provider-access-canary",
                "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": "provider-secret-canary",
                "PROVIDER_OBJECT_STORE_PREFIX": "provider/market",
            }
        ),
        _settings(intraday=False),
        run_forever=False,
    )

    assert gates == 1
    assert migration_checks == 1
    assert startup_order == ["head", "gate"]


@pytest.mark.asyncio
async def test_provider_worker_rechecks_head_before_intraday_scheduling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration_checks = 0
    scheduler_calls = 0
    admission_lock_keys: list[object] = []

    class _Engine:
        async def dispose(self) -> None:
            return None

    class _Session:
        async def __aenter__(self) -> "_Session":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        def begin(self) -> "_Session":
            return self

        async def execute(self, _statement: object, _params: object) -> None:
            admission_lock_keys.append(_params)

        async def scalar(self, _statement: object) -> str:
            return "read committed"

    class _Factory:
        def __call__(self) -> _Session:
            return _Session()

    async def database_role_ok(_session: object) -> None:
        return None

    async def head_changes_after_startup(_session: object) -> None:
        nonlocal migration_checks
        migration_checks += 1
        if migration_checks == 2:
            raise RuntimeError("PROVIDER_DATABASE_MIGRATION_HEAD_MISMATCH")

    async def queue_drained(_session: object) -> None:
        return None

    async def schedule_never_reached(*_args: object, **_kwargs: object) -> None:
        nonlocal scheduler_calls
        scheduler_calls += 1

    @asynccontextmanager
    async def fake_http_client():
        yield object()

    @asynccontextmanager
    async def fake_registry(**_kwargs: object):
        yield object()

    monkeypatch.setattr(
        "nexus.provider_worker.create_async_engine",
        lambda *_args, **_kwargs: _Engine(),
    )
    monkeypatch.setattr(
        "nexus.provider_worker.async_sessionmaker",
        lambda *_args, **_kwargs: _Factory(),
    )
    monkeypatch.setattr(
        "nexus.provider_worker._assert_provider_database_role",
        database_role_ok,
    )
    monkeypatch.setattr(
        "nexus.provider_worker._assert_provider_migration_head",
        head_changes_after_startup,
    )
    monkeypatch.setattr(
        "nexus.provider_worker._assert_intraday_job_queue_drained",
        queue_drained,
    )
    monkeypatch.setattr(
        "nexus.provider_worker.schedule_due_intraday_snapshot",
        schedule_never_reached,
    )
    monkeypatch.setattr(
        "nexus.provider_worker.S3ProviderEvidenceStore",
        lambda _runtime: object(),
    )
    monkeypatch.setattr("nexus.provider_worker._provider_http_client", fake_http_client)
    monkeypatch.setattr("nexus.provider_worker._provider_registry", fake_registry)

    with pytest.raises(
        RuntimeError,
        match="PROVIDER_DATABASE_MIGRATION_HEAD_MISMATCH",
    ):
        await run_provider_worker(
            ProviderWorkerSettings.model_validate(
                {
                    "PROVIDER_DATABASE_URL": "postgresql+asyncpg://provider.invalid/nexus",
                    "PROVIDER_OBJECT_STORE_ENDPOINT": "https://provider-store.invalid",
                    "PROVIDER_OBJECT_STORE_REGION": "us-east-1",
                    "PROVIDER_OBJECT_STORE_BUCKET": "provider-evidence",
                    "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": "provider-access-canary",
                    "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": "provider-secret-canary",
                    "PROVIDER_OBJECT_STORE_PREFIX": "provider/market",
                }
            ),
            _settings(intraday=True),
            run_forever=True,
        )

    assert migration_checks == 2
    assert scheduler_calls == 0
    assert admission_lock_keys == [{"key": "nexus:intraday-schema-admission:v1"}]


@pytest.mark.asyncio
async def test_provider_worker_runs_the_schema_guard_inside_the_claim_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class _Engine:
        async def dispose(self) -> None:
            return None

    class _Session:
        async def __aenter__(self) -> "_Session":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        def begin(self) -> "_Session":
            return self

        async def execute(self, _statement: object, _params: object) -> None:
            events.append("schema-lock")

        async def scalar(self, _statement: object) -> str:
            return "read committed"

    class _Factory:
        def __call__(self) -> _Session:
            return _Session()

    async def role_ok(_session: object) -> None:
        events.append("role")

    async def head_ok(_session: object) -> None:
        events.append("head")

    async def queue_ok(_session: object) -> None:
        events.append("queue")

    async def fake_worker_once(_factory: object, **kwargs: object) -> bool:
        events.append("claim-transaction")
        guard = kwargs.get("preclaim_guard")
        assert callable(guard)
        await guard(_Session())
        raise RuntimeError("CLAIM_GUARD_OBSERVED")

    @asynccontextmanager
    async def fake_http_client():
        yield object()

    @asynccontextmanager
    async def fake_registry(**_kwargs: object):
        yield object()

    monkeypatch.setattr(
        "nexus.provider_worker.create_async_engine",
        lambda *_args, **_kwargs: _Engine(),
    )
    monkeypatch.setattr(
        "nexus.provider_worker.async_sessionmaker",
        lambda *_args, **_kwargs: _Factory(),
    )
    monkeypatch.setattr("nexus.provider_worker._assert_provider_database_role", role_ok)
    monkeypatch.setattr("nexus.provider_worker._assert_provider_migration_head", head_ok)
    monkeypatch.setattr("nexus.provider_worker._assert_intraday_job_queue_drained", queue_ok)
    monkeypatch.setattr("nexus.provider_worker.run_job_worker_once", fake_worker_once)
    monkeypatch.setattr(
        "nexus.provider_worker.S3ProviderEvidenceStore",
        lambda _runtime: object(),
    )
    monkeypatch.setattr("nexus.provider_worker._provider_http_client", fake_http_client)
    monkeypatch.setattr("nexus.provider_worker._provider_registry", fake_registry)

    with pytest.raises(RuntimeError, match="CLAIM_GUARD_OBSERVED"):
        await run_provider_worker(
            ProviderWorkerSettings.model_validate(
                {
                    "PROVIDER_DATABASE_URL": "postgresql+asyncpg://provider.invalid/nexus",
                    "PROVIDER_OBJECT_STORE_ENDPOINT": "https://provider-store.invalid",
                    "PROVIDER_OBJECT_STORE_REGION": "us-east-1",
                    "PROVIDER_OBJECT_STORE_BUCKET": "provider-evidence",
                    "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": "provider-access-canary",
                    "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": "provider-secret-canary",
                    "PROVIDER_OBJECT_STORE_PREFIX": "provider/market",
                }
            ),
            _settings(intraday=False),
            run_forever=True,
        )

    assert events == [
        "head",
        "queue",
        "schema-lock",
        "head",
        "queue",
        "claim-transaction",
        "schema-lock",
        "head",
        "queue",
    ]


@pytest.mark.asyncio
async def test_provider_worker_requires_the_exact_database_migration_head() -> None:
    class _Session:
        def __init__(self, head: str | None) -> None:
            self.head = head

        async def scalar(self, _statement: object) -> str | None:
            return self.head

    # Derive the accepted head independently of the runtime constant, so a new
    # migration catches a stale worker contract without freezing this fixture.
    config = Config(str(Path(__file__).resolve().parents[3] / "alembic.ini"))
    scripts = ScriptDirectory.from_config(config)
    current = scripts.get_current_head()
    assert current is not None
    revision = scripts.get_revision(current)
    assert revision is not None
    await _assert_provider_migration_head(_Session(current))  # type: ignore[arg-type]
    for head in (None, "0032_ticker_alias_authority", revision.down_revision, "unexpected"):
        with pytest.raises(
            RuntimeError,
            match="PROVIDER_DATABASE_MIGRATION_HEAD_MISMATCH",
        ):
            await _assert_provider_migration_head(_Session(head))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_provider_worker_requires_read_committed_transactions() -> None:
    class _Session:
        def __init__(self, isolation: str) -> None:
            self.isolation = isolation

        async def scalar(self, _statement: object) -> str:
            return self.isolation

    await _assert_provider_transaction_isolation(  # type: ignore[arg-type]
        _Session("read committed")
    )
    with pytest.raises(
        RuntimeError,
        match="PROVIDER_DATABASE_TRANSACTION_ISOLATION_INVALID",
    ):
        await _assert_provider_transaction_isolation(  # type: ignore[arg-type]
            _Session("repeatable read")
        )


@pytest.mark.asyncio
async def test_provider_registry_contains_only_provider_capabilities_without_http() -> None:
    requests = 0
    gateway = _NoopGateway()

    def fail_if_requested(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(599)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fail_if_requested)) as client:
        async with _provider_registry(
            provider_settings=_settings(intraday=True),
            provider_http_client=client,
            store=SimpleNamespace(),
            fosun_gateway_factory=lambda _identity: gateway,
        ) as registry:
            assert registry.job_types == {
                JobType.COLLECT_MARKET.value,
                JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value,
                JobType.PUBLISH_FOSUN_DAILY.value,
                JobType.COLLECT_SEC_MACRO.value,
                JobType.COLLECT_INTRADAY_SNAPSHOT.value,
            }
            assert all(spec.queue is QueueName.PROVIDER for spec in registry.specs)
            assert gateway.close_calls == 0

    assert requests == 0
    assert gateway.close_calls == 1


@pytest.mark.asyncio
async def test_disabled_intraday_registry_safely_drains_v2_jobs_without_provider_read() -> None:
    requests = 0
    gateway = _NoopGateway()

    def fail_if_requested(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(599)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fail_if_requested)) as client:
        async with _provider_registry(
            provider_settings=_settings(intraday=False),
            provider_http_client=client,
            store=SimpleNamespace(),
            fosun_gateway_factory=lambda _identity: gateway,
        ) as registry:
            spec = registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value)
            assert spec is not None
            result = await spec.handler(SimpleNamespace())  # type: ignore[arg-type]

    assert (result.outcome, result.result_code) == (
        "skipped",
        "INTRADAY_MONITOR_DISABLED",
    )
    assert requests == 0
    assert gateway.close_calls == 1


def test_provider_contract_includes_fosun_daily_jobs_and_staging() -> None:
    assert _PROVIDER_PRIVILEGES["alembic_version"] == frozenset({"SELECT"})
    assert _PROVIDER_PRIVILEGES["fosun_daily_bar_staging"] == frozenset({"SELECT", "INSERT"})
    assert "fosun_daily_bar_staging" in _ROW_SCOPED_PROVIDER_TABLES
    assert _PROVIDER_POLICY_EXPRESSIONS["fosun_daily_bar_staging"] == (
        "nexus_provider_job_owned(fetch_job_id)",
        "nexus_provider_job_owned(fetch_job_id)",
    )
    for job_type in (
        JobType.COLLECT_FOSUN_DAILY_INSTRUMENT.value,
        JobType.PUBLISH_FOSUN_DAILY.value,
    ):
        assert job_type in _PROVIDER_POLICY_FUNCTIONS["nexus_provider_job_visible"].source
        assert job_type in _PROVIDER_POLICY_FUNCTIONS["nexus_provider_job_writable"].source
    assert _PROVIDER_TRIGGER_TOPOLOGY[
        (
            "fosun_daily_bar_staging",
            "trg_fosun_daily_bar_staging_immutable",
        )
    ] == ("nexus_prevent_fosun_daily_staging_change", 27)
    assert _PROVIDER_TRIGGER_TOPOLOGY[
        (
            "fosun_daily_bar_staging",
            "trg_fosun_daily_bar_staging_created_at_immutable",
        )
    ] == ("nexus_prevent_created_at_change", 19)
    assert _PROVIDER_TRIGGER_TOPOLOGY[("jobs", "trg_jobs_intraday_schema_admission")] == (
        "nexus_guard_intraday_job_schema_admission",
        23,
    )
    assert (
        _PROVIDER_TRIGGER_FUNCTIONS["nexus_guard_intraday_job_schema_admission"].source_sha256
        == "532d7cdb9bf48ddb50fe129dfda5845ea9ad62b63771d0611ecfc18e25fb916f"
    )


def test_provider_contract_attests_authority_input_mutation_triggers() -> None:
    assert _PROVIDER_TRIGGER_TOPOLOGY[
        (
            "integration_status",
            "trg_00_integration_status_fosun_authority_lock",
        )
    ] == ("nexus_lock_fosun_market_input_change", 27)
    assert _PROVIDER_TRIGGER_TOPOLOGY[
        ("provider_capabilities", "trg_provider_capabilities_scope_lock")
    ] == ("nexus_lock_provider_capability_scope", 7)
    assert _PROVIDER_TRIGGER_TOPOLOGY[
        ("provider_capabilities", "trg_provider_capabilities_append_only")
    ] == ("nexus_prevent_provider_capability_change", 27)
    assert len(_PROVIDER_TRIGGER_TOPOLOGY) == 68


@pytest.mark.asyncio
async def test_provider_registry_closes_owned_gateway_when_worker_body_fails() -> None:
    gateway = _NoopGateway()

    with pytest.raises(RuntimeError, match="sentinel"):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: None)
        ) as client:
            async with _provider_registry(
                provider_settings=_settings(intraday=False),
                provider_http_client=client,
                store=SimpleNamespace(),
                fosun_gateway_factory=lambda _identity: gateway,
            ):
                raise RuntimeError("sentinel")

    assert gateway.close_calls == 1


class _NoopGateway:
    def __init__(self) -> None:
        self.close_calls = 0

    async def read_quotes_raw(self, codes: tuple[str, ...]) -> object:
        del codes
        raise AssertionError("composition must not perform a provider read")

    async def read_bars_raw(self, **kwargs: object) -> object:
        del kwargs
        raise AssertionError("composition must not perform a provider read")

    async def aclose(self) -> None:
        self.close_calls += 1


@pytest.mark.asyncio
async def test_provider_worker_rejects_fixture_runtime_before_opening_resources() -> None:
    runtime = ProviderWorkerSettings.model_validate(
        {
            "PROVIDER_DATABASE_URL": "postgresql+asyncpg://fixture.invalid/fixture",
            "PROVIDER_OBJECT_STORE_ENDPOINT": "https://object-store.invalid",
            "PROVIDER_OBJECT_STORE_REGION": "us-east-1",
            "PROVIDER_OBJECT_STORE_BUCKET": "fixture",
            "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": "access-canary",
            "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": "secret-canary",
            "PROVIDER_OBJECT_STORE_PREFIX": "provider/market",
        }
    )

    with pytest.raises(RuntimeError, match="PROVIDER_RUNTIME_PRODUCTION_REQUIRED"):
        await run_provider_worker(
            runtime,
            ProviderSettings(runtime_mode="fixture"),
            run_forever=False,
        )


def test_provider_worker_storage_uses_a_dedicated_fixed_prefix_env_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_values = {
        "PROVIDER_DATABASE_URL": "postgresql+asyncpg://provider.invalid/nexus",
        "PROVIDER_OBJECT_STORE_ENDPOINT": "https://provider-store.invalid",
        "PROVIDER_OBJECT_STORE_REGION": "us-east-1",
        "PROVIDER_OBJECT_STORE_BUCKET": "provider-evidence",
        "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": "provider-access-canary",
        "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": "provider-secret-canary",
    }
    for name in (*provider_values, "PROVIDER_OBJECT_STORE_PREFIX"):
        monkeypatch.delenv(name, raising=False)
    for name, value in {
        "OBJECT_STORE_ENDPOINT": "https://app-store.invalid",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "app-evidence",
        "OBJECT_STORE_ACCESS_KEY_ID": "app-access-canary",
        "OBJECT_STORE_SECRET_ACCESS_KEY": "app-secret-canary",
        "PROVIDER_DATABASE_URL": provider_values["PROVIDER_DATABASE_URL"],
    }.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError):
        ProviderWorkerSettings()

    for name, value in provider_values.items():
        monkeypatch.setenv(name, value)
    runtime = ProviderWorkerSettings()
    assert runtime.object_store_endpoint == "https://provider-store.invalid"
    assert runtime.object_store_prefix == "provider/market"

    with pytest.raises(ValidationError):
        ProviderWorkerSettings.model_validate(
            {
                **provider_values,
                "PROVIDER_OBJECT_STORE_PREFIX": "provider/other",
            }
        )


def test_provider_database_role_rejects_superuser_broad_access_and_missing_rls() -> None:
    assert (
        _provider_privilege_violation(
            direct_login=False,
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_SESSION_ROLE_INVALID"
    )
    assert (
        _provider_privilege_violation(
            role_can_login=False,
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_SESSION_ROLE_INVALID"
    )
    assert (
        _provider_privilege_violation(
            database_create=True,
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_CREATE_PRIVILEGE_INVALID"
    )
    assert (
        _provider_privilege_violation(
            superuser=True,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_ROLE_SUPERUSER"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=True,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_ROLE_BYPASSRLS"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=True,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_ROLE_PRIVILEGED"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            role_memberships=True,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_ROLE_MEMBERSHIP"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=True,
            rows=[],
        )
        == "PROVIDER_DATABASE_SCHEMA_PRIVILEGES_INVALID"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
            sequence_rows=[{"can_usage": True}],
        )
        == "PROVIDER_DATABASE_SEQUENCE_PRIVILEGES_INVALID"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            untrusted_schema_privileges=True,
            rows=[],
        )
        == "PROVIDER_DATABASE_SCHEMA_PRIVILEGES_INVALID"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            can_set_session_replication_role=True,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_PARAMETER_PRIVILEGES_INVALID"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            transaction_isolation="repeatable read",
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_TRANSACTION_ISOLATION_INVALID"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            session_replication_role="replica",
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_PARAMETER_PRIVILEGES_INVALID"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            can_alter_system_session_replication_role=True,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_PARAMETER_PRIVILEGES_INVALID"
    )
    rows = [
        {
            "schema_name": "public",
            "table_name": table_name,
            "relation_kind": "r",
            "can_select": "SELECT" in privileges,
            "can_insert": "INSERT" in privileges,
            "can_update": "UPDATE" in privileges,
            "can_delete": "DELETE" in privileges,
            "can_truncate": False,
            "can_references": False,
            "can_trigger": False,
            "can_maintain": False,
            "direct_table_privileges": tuple(sorted(privileges)),
            "direct_table_grantors_trusted": True,
            "has_public_table_privilege": False,
            "can_column_select": "SELECT" in privileges,
            "can_column_insert": "INSERT" in privileges,
            "can_column_update": "UPDATE" in privileges,
            "can_column_references": False,
            "has_provider_column_privilege": False,
            "has_public_column_privilege": False,
            "row_security": table_name != "jobs",
            "force_row_security": table_name != "jobs",
        }
        for table_name, privileges in _PROVIDER_PRIVILEGES.items()
    ]

    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_ROW_SCOPE_REQUIRED"
    )

    for row in rows:
        row["row_security"] = True
        row["force_row_security"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        is None
    )

    rows[0]["has_public_table_privilege"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_PUBLIC_PRIVILEGES_INVALID"
    )
    rows[0]["has_public_table_privilege"] = False
    expected_direct = rows[0]["direct_table_privileges"]
    rows[0]["direct_table_privileges"] = ()
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_PRIVILEGES_INVALID"
    )
    rows[0]["direct_table_privileges"] = expected_direct
    rows[0]["has_provider_column_privilege"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_COLUMN_PRIVILEGES_INVALID"
    )
    rows[0]["has_provider_column_privilege"] = False

    writable_row = next(row for row in rows if row["table_name"] == "jobs")
    writable_row["relation_kind"] = "p"
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_RELATION_KIND_INVALID"
    )
    writable_row["relation_kind"] = "r"

    rows[0]["has_table_grant_option"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_GRANT_OPTIONS_INVALID"
    )
    rows[0]["has_table_grant_option"] = False
    rows[0]["has_column_grant_option"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_GRANT_OPTIONS_INVALID"
    )
    rows[0]["has_column_grant_option"] = False

    rows.append(
        {
            "schema_name": "research",
            "table_name": "agent_research_runs",
            "relation_kind": "v",
            "can_select": True,
            "can_insert": False,
            "can_update": False,
            "can_delete": False,
            "can_truncate": False,
            "can_references": False,
            "can_trigger": False,
            "can_maintain": False,
            "direct_table_privileges": ("SELECT",),
            "direct_table_grantors_trusted": True,
            "has_public_table_privilege": False,
            "can_column_select": True,
            "can_column_insert": False,
            "can_column_update": False,
            "can_column_references": False,
            "has_provider_column_privilege": False,
            "has_public_column_privilege": False,
            "row_security": False,
            "force_row_security": False,
        }
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_PRIVILEGES_TOO_BROAD"
    )

    rows[-1]["can_select"] = False
    rows[0]["can_trigger"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_PRIVILEGES_INVALID"
    )

    rows[0]["can_trigger"] = False
    dependencies = next(row for row in rows if row["table_name"] == "job_dependencies")
    dependencies["can_column_update"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_PRIVILEGES_INVALID"
    )

    dependencies["can_column_update"] = False


def _valid_policy_rows(provider_role: str = "provider_runtime") -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    escaped_role = provider_role.replace("'", "''")
    bypass = f"NOT pg_catalog.pg_has_role(CURRENT_USER, '{escaped_role}', 'MEMBER')"
    provider_dependencies = {
        "jobs": (
            "public.nexus_provider_job_visible",
            "public.nexus_provider_job_writable",
        ),
        "job_leases": ("public.nexus_provider_job_owned",),
        "job_attempts": ("public.nexus_provider_job_owned",),
        "job_dependencies": ("public.nexus_provider_job_owned",),
        "worker_heartbeats": ("public.nexus_provider_queue",),
        "fosun_daily_bar_staging": ("public.nexus_provider_job_owned",),
    }
    for table_name, (using, with_check) in _PROVIDER_POLICY_EXPRESSIONS.items():
        rows.extend(
            (
                {
                    "table_name": table_name,
                    "policy_name": f"nexus_provider_{table_name}_scope",
                    "permissive": "PERMISSIVE",
                    "roles": [provider_role],
                    "command": "ALL",
                    "qual": using,
                    "with_check": with_check,
                    "builtin_binding_valid": True,
                    "function_dependencies": provider_dependencies[table_name],
                },
                {
                    "table_name": table_name,
                    "policy_name": f"nexus_non_provider_{table_name}_bypass",
                    "permissive": "PERMISSIVE",
                    "roles": ["public"],
                    "command": "ALL",
                    "qual": bypass,
                    "with_check": bypass,
                    "builtin_binding_valid": True,
                    "function_dependencies": (),
                },
            )
        )
    return rows


def test_provider_database_role_rejects_extra_or_mutated_applicable_policy() -> None:
    provider_role = "provider_runtime"
    rows = _valid_policy_rows(provider_role)
    assert _provider_policy_violation(rows, provider_role=provider_role) is None

    rows.append(
        {
            "table_name": "jobs",
            "policy_name": "unexpected_public_bypass",
            "permissive": "PERMISSIVE",
            "roles": ["public"],
            "command": "ALL",
            "qual": "true",
            "with_check": "true",
            "builtin_binding_valid": True,
            "function_dependencies": (),
        }
    )
    assert (
        _provider_policy_violation(rows, provider_role=provider_role)
        == "PROVIDER_DATABASE_POLICY_SET_INVALID"
    )

    rows = _valid_policy_rows(provider_role)
    bypass = next(row for row in rows if row["policy_name"] == "nexus_non_provider_jobs_bypass")
    bypass["function_dependencies"] = ("public.pg_has_role",)
    assert (
        _provider_policy_violation(rows, provider_role=provider_role)
        == "PROVIDER_DATABASE_POLICY_SET_INVALID"
    )

    rows = _valid_policy_rows(provider_role)
    bypass = next(row for row in rows if row["policy_name"] == "nexus_non_provider_jobs_bypass")
    bypass["builtin_binding_valid"] = False
    assert (
        _provider_policy_violation(rows, provider_role=provider_role)
        == "PROVIDER_DATABASE_POLICY_SET_INVALID"
    )

    rows = _valid_policy_rows(provider_role)
    provider_scope = next(row for row in rows if row["policy_name"] == "nexus_provider_jobs_scope")
    provider_scope["qual"] = 'public."NEXUS_PROVIDER_JOB_VISIBLE"(queue, job_type)'
    assert (
        _provider_policy_violation(rows, provider_role=provider_role)
        == "PROVIDER_DATABASE_POLICY_SET_INVALID"
    )

    rows = _valid_policy_rows(provider_role)
    bypass = next(row for row in rows if row["policy_name"] == "nexus_non_provider_jobs_bypass")
    bypass["qual"] = str(bypass["qual"]).replace(provider_role, provider_role.upper())
    assert (
        _provider_policy_violation(rows, provider_role=provider_role)
        == "PROVIDER_DATABASE_POLICY_SET_INVALID"
    )

    rows = _valid_policy_rows(provider_role)
    bypass = next(row for row in rows if row["policy_name"] == "nexus_non_provider_jobs_bypass")
    bypass["qual"] = "true"
    assert (
        _provider_policy_violation(rows, provider_role=provider_role)
        == "PROVIDER_DATABASE_POLICY_SET_INVALID"
    )


def test_provider_database_role_rejects_mutated_policy_function() -> None:
    rows = [
        {
            "function_name": name,
            "identity_arguments": spec.identity_arguments,
            "source": spec.source,
            "volatility": spec.volatility,
            "parallel_safety": "s",
            "security_definer": False,
            "search_path_fixed": True,
            "trusted_owner": True,
            "provider_is_owner_member": False,
            "provider_execute": True,
            "public_execute": False,
            "provider_execute_grantable": False,
            "acl_only_owner_and_provider": True,
            "acl_grantor_only_owner": True,
            "language": "sql",
            "returns_boolean": True,
        }
        for name, spec in _PROVIDER_POLICY_FUNCTIONS.items()
    ]
    assert _provider_policy_function_violation(rows) is None

    rows[0]["source"] = "SELECT true"
    assert _provider_policy_function_violation(rows) == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"

    rows[0]["source"] = _PROVIDER_POLICY_FUNCTIONS[str(rows[0]["function_name"])].source
    rows[0]["source"] = str(rows[0]["source"]).replace("'provider'", "'PROVIDER'")
    assert _provider_policy_function_violation(rows) == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"

    rows[0]["source"] = _PROVIDER_POLICY_FUNCTIONS[str(rows[0]["function_name"])].source
    rows[0]["trusted_owner"] = False
    assert _provider_policy_function_violation(rows) == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"

    rows[0]["trusted_owner"] = True
    rows[0]["acl_only_owner_and_provider"] = False
    assert _provider_policy_function_violation(rows) == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"

    rows[0]["acl_only_owner_and_provider"] = True
    rows[0]["provider_execute_grantable"] = True
    assert _provider_policy_function_violation(rows) == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"

    rows[0]["provider_execute_grantable"] = False
    rows.append(
        {
            **rows[0],
            "identity_arguments": "p_queue character varying, p_job_type character varying",
        }
    )
    assert _provider_policy_function_violation(rows) == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"


def _valid_publication_function_rows() -> list[dict[str, object]]:
    return [
        {
            "schema_name": "public",
            "function_name": spec.name,
            "identity_arguments": spec.identity_arguments,
            "source": spec.source,
            "volatility": "v",
            "parallel_safety": "u",
            "security_definer": True,
            "leakproof": False,
            "search_path_exact": True,
            "trusted_owner": True,
            "provider_is_owner_member": False,
            "provider_execute": False,
            "public_execute": False,
            "acl_only_owner": True,
            "acl_grantor_only_owner": True,
            "language": "plpgsql",
            "returns_trigger": True,
        }
        for spec in (
            _PROVIDER_PUBLICATION_FUNCTION,
            _PROVIDER_PRICE_BAR_GUARD_FUNCTION,
        )
    ]


def _valid_publication_trigger_rows() -> list[dict[str, object]]:
    publication = [
        {
            "trigger_name": trigger_name,
            "schema_name": "public",
            "table_name": table_name,
            "function_name": _PROVIDER_PUBLICATION_FUNCTION.name,
            "identity_arguments": _PROVIDER_PUBLICATION_FUNCTION.identity_arguments,
            "trigger_type": 5,
            "enabled": "O",
            "internal": False,
            "unconditional": True,
            "non_constraint": True,
            "non_deferrable": True,
            "initially_immediate": True,
            "no_arguments": True,
            "not_partition_clone": True,
            "no_update_columns": True,
            "relation_kind": "r",
        }
        for table_name, trigger_name in sorted(_PROVIDER_PUBLICATION_TRIGGERS.items())
    ]
    guard = [
        {
            "trigger_name": trigger_name,
            "schema_name": "public",
            "table_name": table_name,
            "function_name": _PROVIDER_PRICE_BAR_GUARD_FUNCTION.name,
            "identity_arguments": _PROVIDER_PRICE_BAR_GUARD_FUNCTION.identity_arguments,
            "trigger_type": 7,
            "enabled": "O",
            "internal": False,
            "unconditional": True,
            "non_constraint": True,
            "non_deferrable": True,
            "initially_immediate": True,
            "no_arguments": True,
            "not_partition_clone": True,
            "no_update_columns": True,
            "relation_kind": "r",
        }
        for table_name, trigger_name in sorted(_PROVIDER_PRICE_BAR_GUARD_TRIGGERS.items())
    ]
    return [*publication, *guard]


def test_provider_database_role_accepts_exact_publication_definer_topology() -> None:
    assert (
        _provider_publication_function_violation(
            _valid_publication_function_rows(),
            _valid_publication_trigger_rows(),
        )
        is None
    )


@pytest.mark.parametrize(
    ("field", "mutated"),
    (
        ("source", "BEGIN RETURN NEW; END;"),
        (
            "source",
            _PROVIDER_PUBLICATION_FUNCTION.source.replace(
                "backtest-authority-global", "BACKTEST-AUTHORITY-GLOBAL"
            ),
        ),
        ("security_definer", False),
        ("schema_name", "shadow"),
        ("search_path_exact", False),
        ("trusted_owner", False),
        ("provider_is_owner_member", True),
        ("provider_execute", True),
        ("public_execute", True),
        ("acl_only_owner", False),
        ("acl_grantor_only_owner", False),
        ("returns_trigger", False),
        ("leakproof", True),
    ),
)
def test_provider_database_role_rejects_mutated_publication_definer(
    field: str,
    mutated: object,
) -> None:
    functions = _valid_publication_function_rows()
    functions[0][field] = mutated

    assert (
        _provider_publication_function_violation(
            functions,
            _valid_publication_trigger_rows(),
        )
        == "PROVIDER_DATABASE_PUBLICATION_FUNCTION_INVALID"
    )


def test_provider_database_role_rejects_mutated_price_bar_guard_definer() -> None:
    functions = _valid_publication_function_rows()
    guard = next(
        row for row in functions if row["function_name"] == _PROVIDER_PRICE_BAR_GUARD_FUNCTION.name
    )
    guard["source"] = str(guard["source"]).replace(
        "backtest-authority-global", "BACKTEST-AUTHORITY-GLOBAL"
    )

    assert (
        _provider_publication_function_violation(
            functions,
            _valid_publication_trigger_rows(),
        )
        == "PROVIDER_DATABASE_PUBLICATION_FUNCTION_INVALID"
    )


@pytest.mark.parametrize(
    ("field", "mutated"),
    (
        ("function_name", "nexus_mark_backtest_authority_publication"),
        ("schema_name", "shadow"),
        ("trigger_type", 7),
        ("enabled", "D"),
        ("internal", True),
        ("unconditional", False),
        ("non_constraint", False),
        ("non_deferrable", False),
        ("initially_immediate", False),
        ("no_arguments", False),
        ("not_partition_clone", False),
        ("no_update_columns", False),
        ("relation_kind", "p"),
    ),
)
def test_provider_database_role_rejects_mutated_publication_trigger(
    field: str,
    mutated: object,
) -> None:
    triggers = _valid_publication_trigger_rows()
    triggers[0][field] = mutated

    assert (
        _provider_publication_function_violation(
            _valid_publication_function_rows(),
            triggers,
        )
        == "PROVIDER_DATABASE_PUBLICATION_FUNCTION_INVALID"
    )


def test_provider_database_role_rejects_extra_publication_definer_or_trigger() -> None:
    functions = _valid_publication_function_rows()
    functions.append({**functions[0], "function_name": "unexpected_definer"})
    assert (
        _provider_publication_function_violation(
            functions,
            _valid_publication_trigger_rows(),
        )
        == "PROVIDER_DATABASE_PUBLICATION_FUNCTION_INVALID"
    )


def _valid_provider_trigger_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for (table_name, trigger_name), (
        function_name,
        trigger_type,
    ) in _PROVIDER_TRIGGER_TOPOLOGY.items():
        function = _PROVIDER_TRIGGER_FUNCTIONS[function_name]
        rows.append(
            {
                "schema_name": "public",
                "table_name": table_name,
                "trigger_name": trigger_name,
                "function_schema": "public",
                "function_name": function_name,
                "identity_arguments": "",
                "source_sha256": function.source_sha256,
                "trigger_type": trigger_type,
                "enabled": "O",
                "internal": False,
                "unconditional": True,
                "non_constraint": True,
                "non_deferrable": True,
                "initially_immediate": True,
                "no_arguments": True,
                "not_partition_clone": True,
                "no_update_columns": True,
                "relation_kind": "r",
                "volatility": "v",
                "parallel_safety": "u",
                "strict": False,
                "security_definer": function.security_definer,
                "leakproof": False,
                "search_path_exact": function.search_path_exact,
                "no_function_config": not function.search_path_exact,
                "language": "plpgsql",
                "returns_trigger": True,
                "trusted_owner": True,
                "provider_is_owner_member": False,
            }
        )
    return rows


def test_provider_database_role_accepts_exact_insert_trigger_topology() -> None:
    assert _provider_trigger_topology_violation(_valid_provider_trigger_rows()) is None


@pytest.mark.parametrize(
    ("field", "mutated"),
    (
        ("enabled", "D"),
        ("trigger_type", 5),
        ("source_sha256", "0" * 64),
        ("function_schema", "shadow"),
        ("identity_arguments", "unexpected text"),
        ("security_definer", True),
        ("trusted_owner", False),
        ("provider_is_owner_member", True),
        ("search_path_exact", False),
        ("no_function_config", True),
        ("unconditional", False),
        ("not_partition_clone", False),
        ("no_update_columns", False),
        ("relation_kind", "p"),
    ),
)
def test_provider_database_role_rejects_mutated_insert_trigger_topology(
    field: str,
    mutated: object,
) -> None:
    rows = _valid_provider_trigger_rows()
    candidate = next(
        row for row in rows if row["function_name"] == "nexus_validate_fosun_bar_transport"
    )
    candidate[field] = mutated

    assert (
        _provider_trigger_topology_violation(rows) == "PROVIDER_DATABASE_TRIGGER_TOPOLOGY_INVALID"
    )


def test_provider_database_role_rejects_missing_or_late_insert_trigger() -> None:
    rows = _valid_provider_trigger_rows()
    assert (
        _provider_trigger_topology_violation(rows[1:])
        == "PROVIDER_DATABASE_TRIGGER_TOPOLOGY_INVALID"
    )

    rows.append(
        {
            **rows[0],
            "trigger_name": "trg_zz_unreviewed_provider_write",
            "trigger_type": 7,
        }
    )
    assert (
        _provider_trigger_topology_violation(rows) == "PROVIDER_DATABASE_TRIGGER_TOPOLOGY_INVALID"
    )

    triggers = _valid_publication_trigger_rows()
    triggers.append({**triggers[0], "trigger_name": "unexpected_trigger"})
    assert (
        _provider_publication_function_violation(
            _valid_publication_function_rows(),
            triggers,
        )
        == "PROVIDER_DATABASE_PUBLICATION_FUNCTION_INVALID"
    )
