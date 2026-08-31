from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
import requests
from nexus_fosun_gateway.adapter.probe_sdk import (
    PhysicalHttpBudget,
    ProbeBudgetError,
    ProbeRequestPlan,
    ProbeSdkError,
    ProbeTransportBundle,
    SdkProbeExecutor,
    _close_probe_controls_bounded,
    _require_page_contract,
    build_probe_transport,
)
from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport
from nexus_fosun_gateway.domain import (
    AccountItem,
    GatewayError,
    GatewayItem,
    GatewayPage,
)
from nexus_fosun_gateway.probe_settings import ProbeProviderSettings
from pydantic import ValidationError
from test_probe_settings import _admitted_document, _settings_values


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.value

    def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.value += delay


def test_physical_budget_counts_auth_and_business_at_fixed_labels() -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    budget.bind_http_controls()

    with budget.operation("Accounts"):
        budget.try_acquire_at_call_edge()
        budget.before_dispatch("auth")
        budget.after_response("auth", 200)
        budget.before_dispatch("business")
        budget.after_response("business", 200)
    with budget.operation("quote"):
        budget.try_acquire_at_call_edge()
        budget.before_dispatch("business")
        budget.after_response("business", 200)

    snapshot = budget.snapshot()
    assert snapshot.count("Session") == 1
    assert snapshot.count("Accounts") == 1
    assert snapshot.count("quote") == 1
    assert snapshot.total == 3
    assert snapshot.auth_successes == 1
    assert clock.sleeps == [1.0, 1.0]


def test_budget_refuses_before_http_controls_or_without_active_label() -> None:
    budget = PhysicalHttpBudget(clock=lambda: 0.0, sleeper=lambda _delay: None)

    with pytest.raises(ProbeBudgetError):
        budget.try_acquire_at_call_edge()
    budget.bind_http_controls()
    with pytest.raises(ProbeBudgetError):
        budget.try_acquire_at_call_edge()
    with pytest.raises(ProbeBudgetError):
        budget.before_dispatch("business")


@pytest.mark.asyncio
async def test_probe_budget_refusal_crosses_sdk_adapter_without_retry() -> None:
    class RefusingLimiter:
        def __init__(self) -> None:
            self.calls = 0

        async def acquire(self) -> None:
            raise AssertionError("unused")

        async def try_acquire(self) -> float | None:
            raise AssertionError("unused")

        def try_acquire_at_call_edge(self) -> float | None:
            self.calls += 1
            raise ProbeBudgetError()

    limiter = RefusingLimiter()
    sdk_calls = 0

    def list_accounts() -> object:
        nonlocal sdk_calls
        sdk_calls += 1
        raise AssertionError("budget refusal must precede the SDK call")

    transport = SdkFosunTransport(
        SimpleNamespace(account=SimpleNamespace(list_accounts=list_accounts)),
        rate_limiter=limiter,
    )

    with pytest.raises(ProbeBudgetError):
        await transport.execute("fosun_sync_accounts", {})

    assert limiter.calls == 1
    assert sdk_calls == 0


@pytest.mark.asyncio
async def test_probe_controls_close_has_an_absolute_one_shot_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = Event()
    release = Event()

    class BlockingControls:
        def close(self) -> None:
            entered.set()
            assert release.wait(timeout=2)

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk._PROBE_CONTROLS_CLOSE_TIMEOUT_SECONDS",
        0.02,
    )

    with pytest.raises(ProbeSdkError) as exc_info:
        await _close_probe_controls_bounded(BlockingControls())  # type: ignore[arg-type]

    assert exc_info.value.code == "PROBE_FAILED"
    assert entered.is_set()
    release.set()


@pytest.mark.asyncio
async def test_http_proxy_budget_refusal_crosses_real_sdk_adapter_without_retry() -> None:
    budget = _new_budget()
    harness = InstrumentedTransport(budget)
    bundle = _probe_bundle(harness, RecordingControls(), budget)
    budget._counts["Session"] = 3

    with budget.operation("Accounts"), pytest.raises(ProbeBudgetError):
        await bundle.transport.execute("fosun_sync_accounts", {})

    assert harness.calls == [("fosun_sync_accounts", {})]
    assert harness.auth_session is not None
    assert harness.auth_session.post_count == 0
    assert harness.business_session.request_count == 0
    await bundle.aclose()


def test_auth_is_allowed_only_during_accounts_and_labels_cannot_overlap() -> None:
    budget = _bound_budget()

    with budget.operation("quote"):
        with pytest.raises(ProbeBudgetError):
            budget.before_dispatch("auth")
        with pytest.raises(ProbeBudgetError):
            with budget.operation("kline_minimal"):
                pass


@pytest.mark.parametrize(
    ("label", "surface", "attempts"),
    [
        ("quote", "business", 4),
        ("Accounts", "auth", 4),
    ],
)
def test_per_label_cap_refuses_fourth_delegate(
    label: str,
    surface: str,
    attempts: int,
) -> None:
    delegated = 0
    budget = _bound_budget()

    with budget.operation(label):
        for _ in range(attempts):
            try:
                budget.before_dispatch(surface)
            except ProbeBudgetError:
                break
            delegated += 1

    assert delegated == 3
    expected_label = "Session" if surface == "auth" else label
    assert budget.snapshot().count(expected_label) == 3


def test_broker_domain_cap_includes_session_and_refuses_seventh_delegate() -> None:
    budget = _bound_budget()
    delegated = 0
    sequence: tuple[tuple[str, str], ...] = (
        ("Accounts", "auth"),
        ("Accounts", "business"),
        ("CashSummary", "business"),
        ("Holdings", "business"),
        ("OrderList", "business"),
        ("CashFlows", "business"),
        ("CashFlows", "business"),
    )

    for label, surface in sequence:
        try:
            with budget.operation(label):
                budget.before_dispatch(surface)
        except ProbeBudgetError:
            break
        delegated += 1

    assert delegated == 6
    assert budget.snapshot().total == 6


def test_total_hard_cap_refuses_before_twenty_fifth_delegate() -> None:
    budget = _bound_budget()
    budget._total = 24

    with budget.operation("quote"), pytest.raises(ProbeBudgetError):
        budget.before_dispatch("business")

    assert budget.snapshot().total == 24


def test_market_rolling_minute_cap_keeps_open_boundary_and_prunes_closed_boundary() -> None:
    clock = FakeClock()
    clock.value = 60.0
    budget = _bound_budget(clock=clock)
    budget._market_timestamps.extend([0.0001] * 24)

    with budget.operation("quote"), pytest.raises(ProbeBudgetError):
        budget.before_dispatch("business")

    budget._market_timestamps.clear()
    budget._market_timestamps.extend([0.0] * 24)
    with budget.operation("quote"):
        budget.before_dispatch("business")

    assert budget.snapshot().count("quote") == 1


def test_total_rolling_minute_cap_refuses_twenty_fifth_start() -> None:
    budget = _bound_budget()
    budget._total_timestamps.extend([0.0] * 24)

    with budget.operation("quote"), pytest.raises(ProbeBudgetError):
        budget.before_dispatch("business")

    assert budget.snapshot().total == 0


def test_budget_protocol_async_methods_are_assertion_only() -> None:
    budget = _bound_budget()

    async def exercise() -> None:
        with budget.operation("quote"):
            assert await budget.try_acquire() is None
            await budget.acquire()

    asyncio.run(exercise())
    assert budget.snapshot().total == 0


@pytest.mark.asyncio
async def test_executor_runs_exact_sequence_and_returns_only_value_free_evidence() -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    transport = InstrumentedTransport(budget)
    controls = RecordingControls()
    executor = SdkProbeExecutor(
        _probe_bundle(transport, controls, budget),
        budget,
    )
    plan = ProbeRequestPlan(
        symbol="usAAPL",
        session_date=date(2026, 8, 21),
        cash_flow_date_from=date(2026, 8, 20),
        cash_flow_date_to=date(2026, 8, 21),
        limit=1,
    )

    evidence = await executor.run(plan)

    assert [call[0] for call in transport.calls] == [
        "fosun_sync_accounts",
        "fosun_sync_portfolio",
        "fosun_sync_portfolio",
        "fosun_list_orders",
        "fosun_list_cash_flows",
        "fosun_get_quotes",
        "fosun_get_bars",
    ]
    assert transport.calls[1][1] == {
        "account_id": "synthetic-account-canary",
        "read_kind": "cash",
    }
    assert transport.calls[-1][1] == {
        "code": "usAAPL",
        "ktype": "day",
        "start_time": "2026-08-21T00:00:00Z",
        "end_time": "2026-08-21T00:00:00Z",
        "limit": 1,
    }
    assert evidence.request_counts == tuple((label, 1) for label in (
        "Session",
        "Accounts",
        "CashSummary",
        "Holdings",
        "OrderList",
        "CashFlows",
        "quote",
        "kline_minimal",
    ))
    assert evidence.auth_successes == 1
    assert evidence.history_window is True
    assert evidence.pagination is False
    assert evidence.adjustment_semantics is False
    assert controls.close_calls == 1
    assert transport.close_calls == 1
    assert "synthetic-account-canary" not in repr(evidence)
    assert "synthetic-request-canary" not in repr(evidence)

    with pytest.raises(ProbeSdkError) as exc_info:
        await executor.run(plan)

    assert exc_info.value.code == "PROBE_FAILED"
    assert len(transport.calls) == 7
    assert controls.close_calls == 1
    assert transport.close_calls == 1


@pytest.mark.asyncio
async def test_executor_accepts_exactly_seven_calendar_days_of_cash_flows() -> None:
    budget = _new_budget()
    transport = InstrumentedTransport(budget)
    plan = replace(
        _plan(),
        cash_flow_date_from=date(2026, 8, 15),
        cash_flow_date_to=date(2026, 8, 21),
    )

    await SdkProbeExecutor(
        _probe_bundle(transport, RecordingControls(), budget),
        budget,
    ).run(plan)

    assert transport.calls[4] == (
        "fosun_list_cash_flows",
        {
            "account_id": "synthetic-account-canary",
            "cursor": None,
            "date_from": "2026-08-15",
            "date_to": "2026-08-21",
            "limit": 1,
        },
    )


@pytest.mark.asyncio
async def test_executor_stops_immediately_when_sdk_returns_without_http_dispatch() -> None:
    budget = _new_budget()
    transport = InstrumentedTransport(budget, cached_at=1)

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(transport, RecordingControls(), budget),
            budget,
        ).run(_plan())

    assert exc_info.value.code == "SDK_CONTRACT_MISMATCH"
    assert len(transport.calls) == 2


def test_probe_bundle_rejects_manual_unattested_construction() -> None:
    budget = _bound_budget()

    with pytest.raises(ProbeSdkError) as exc_info:
        ProbeTransportBundle(
            transport=InstrumentedTransport(budget),
            controls=RecordingControls(),
        )

    assert exc_info.value.code == "SDK_CONTRACT_MISMATCH"
    assert not hasattr(ProbeTransportBundle, "_from_factory")


@pytest.mark.asyncio
async def test_executor_rechecks_installed_tls_proxy_before_provider_call() -> None:
    budget = _new_budget()
    transport = InstrumentedTransport(budget)
    controls = RecordingControls()
    bundle = _probe_bundle(transport, controls, budget)
    transport.client._http_session = transport.business_session

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(bundle, budget).run(_plan())

    assert exc_info.value.code == "SDK_CONTRACT_MISMATCH"
    assert transport.calls == []
    assert controls.close_calls == 1
    assert transport.close_calls == 1


@pytest.mark.asyncio
async def test_executor_rejects_a_different_budget_before_any_provider_call() -> None:
    bound_budget = _new_budget()
    other_budget = _bound_budget()
    transport = InstrumentedTransport(bound_budget)
    controls = RecordingControls()
    bundle = _probe_bundle(transport, controls, bound_budget)

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(bundle, other_budget).run(_plan())

    assert exc_info.value.code == "SDK_CONTRACT_MISMATCH"
    assert transport.calls == []
    assert controls.close_calls == 1
    assert transport.close_calls == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"symbol": "AAPL"},
        {"symbol": "usAAP/l"},
        {"limit": 0},
        {"limit": 11},
        {
            "cash_flow_date_from": date(2026, 8, 22),
            "cash_flow_date_to": date(2026, 8, 21),
        },
        {
            "cash_flow_date_from": date(2026, 8, 1),
            "cash_flow_date_to": date(2026, 8, 21),
        },
        {
            "cash_flow_date_from": date(2026, 8, 1),
            "cash_flow_date_to": date(2026, 8, 8),
        },
        {"session_date": date(2026, 8, 31)},
    ],
)
@pytest.mark.asyncio
async def test_executor_rejects_invalid_plan_before_any_provider_call(
    changes: dict[str, object],
) -> None:
    budget = _new_budget()
    transport = InstrumentedTransport(budget)
    controls = RecordingControls()

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(transport, controls, budget),
            budget,
            utc_clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
        ).run(replace(_plan(), **changes))

    assert exc_info.value.code == "INVALID_PROBE_PLAN"
    assert transport.calls == []
    assert controls.close_calls == 1
    assert transport.close_calls == 1


@pytest.mark.asyncio
async def test_executor_stops_on_empty_accounts_and_closes_both_surfaces() -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    transport = InstrumentedTransport(budget, empty_accounts=True)
    controls = RecordingControls()
    executor = SdkProbeExecutor(
        _probe_bundle(transport, controls, budget),
        budget,
    )

    with pytest.raises(ProbeSdkError) as exc_info:
        await executor.run(_plan())

    assert exc_info.value.code == "PROBE_ACCOUNT_UNAVAILABLE"
    assert len(transport.calls) == 1
    assert controls.close_calls == 1
    assert transport.close_calls == 1


@pytest.mark.asyncio
async def test_executor_reports_session_when_auth_response_fails_before_accounts() -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    transport = InstrumentedTransport(budget, auth_statuses=(401,))

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(transport, RecordingControls(), budget), budget
        ).run(_plan())

    assert exc_info.value.code == "PROBE_FAILED"
    assert exc_info.value.operation == "Session"
    assert exc_info.value.category == "auth"
    assert exc_info.value.physical_request_count == 1
    assert len(transport.calls) == 1


@pytest.mark.parametrize("fail_at", range(7))
@pytest.mark.asyncio
async def test_executor_stops_after_first_failed_operation(fail_at: int) -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    transport = InstrumentedTransport(budget, fail_at=fail_at)
    controls = RecordingControls()

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(transport, controls, budget),
            budget,
        ).run(_plan())

    assert exc_info.value.code == "PROBE_FAILED"
    assert exc_info.value.operation == (
        "Accounts",
        "CashSummary",
        "Holdings",
        "OrderList",
        "CashFlows",
        "quote",
        "kline_minimal",
    )[fail_at]
    assert exc_info.value.category == "permission"
    assert exc_info.value.physical_request_count == fail_at + 2
    assert len(transport.calls) == fail_at + 1
    assert controls.close_calls == 1
    assert transport.close_calls == 1


def test_rate_limited_gateway_error_has_a_closed_rate_limit_category() -> None:
    from nexus_fosun_gateway.adapter.probe_sdk import _map_gateway_error

    mapped = _map_gateway_error(
        GatewayError(
            "PROVIDER_RATE_LIMITED",
            "sanitized",
            status_code=429,
            next_action="RETRY_LATER",
        )
    )

    assert mapped.category == "rate_limit"


@pytest.mark.asyncio
async def test_executor_reports_rate_limit_at_the_dispatched_operation() -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    transport = InstrumentedTransport(budget, gateway_error_at=1)

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(transport, RecordingControls(), budget), budget
        ).run(_plan())

    assert exc_info.value.operation == "CashSummary"
    assert exc_info.value.category == "rate_limit"
    assert exc_info.value.physical_request_count == 3
    assert len(transport.calls) == 2


@pytest.mark.parametrize("auth_statuses", [(), (200, 200)])
@pytest.mark.asyncio
async def test_executor_requires_exactly_one_successful_session(
    auth_statuses: tuple[int, ...],
) -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    transport = InstrumentedTransport(budget, auth_statuses=auth_statuses)

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(transport, RecordingControls(), budget),
            budget,
        ).run(_plan())

    assert exc_info.value.code == "SDK_CONTRACT_MISMATCH"
    assert len(transport.calls) == 1


@pytest.mark.asyncio
async def test_executor_rejects_wrong_bar_identity_without_exposing_it() -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    transport = InstrumentedTransport(budget, wrong_bar=True)

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(transport, RecordingControls(), budget),
            budget,
        ).run(_plan())

    assert exc_info.value.code == "PROBE_WINDOW_MISMATCH"
    assert "usSECRET" not in repr(exc_info.value)


@pytest.mark.asyncio
async def test_executor_classifies_malformed_bar_as_response_not_window() -> None:
    budget = _new_budget()
    transport = InstrumentedTransport(budget, malformed_bar=True)

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(transport, RecordingControls(), budget),
            budget,
        ).run(_plan())

    assert exc_info.value.code == "PROBE_RESPONSE_INVALID"


def test_executor_rejects_malformed_pagination_as_response_contract() -> None:
    page = GatewayPage[GatewayItem].model_construct(
        items=(),
        source_request_id="synthetic-request-canary",
        has_more=True,
        next_cursor=1,
        as_of=datetime(2026, 8, 22, tzinfo=UTC),
    )

    with pytest.raises(ProbeSdkError) as exc_info:
        _require_page_contract(page, AccountItem)

    assert exc_info.value.code == "PROBE_RESPONSE_INVALID"


@pytest.mark.asyncio
async def test_executor_cancellation_closes_both_surfaces_and_propagates() -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    transport = InstrumentedTransport(budget, cancel_at=2)
    controls = RecordingControls()

    with pytest.raises(asyncio.CancelledError):
        await SdkProbeExecutor(
            _probe_bundle(transport, controls, budget),
            budget,
        ).run(_plan())

    assert len(transport.calls) == 3
    assert controls.close_calls == 1
    assert transport.close_calls == 1


@pytest.mark.asyncio
async def test_close_failure_never_replaces_primary_fixed_failure() -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)
    transport = InstrumentedTransport(budget, empty_accounts=True)

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(transport, FailingControls(), budget),
            budget,
        ).run(_plan())

    assert exc_info.value.code == "PROBE_ACCOUNT_UNAVAILABLE"
    assert "synthetic-close-secret" not in repr(exc_info.value)


@pytest.mark.asyncio
async def test_close_failure_after_success_is_sanitized() -> None:
    clock = FakeClock()
    budget = PhysicalHttpBudget(clock=clock, sleeper=clock.sleep)

    with pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            _probe_bundle(InstrumentedTransport(budget), FailingControls(), budget),
            budget,
        ).run(_plan())

    assert exc_info.value.code == "PROBE_FAILED"
    assert "synthetic-close-secret" not in repr(exc_info.value)


def test_probe_factory_rejects_blocked_manifest_before_sdk_import(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imported = False

    def forbidden_import(name: str) -> object:
        nonlocal imported
        imported = True
        raise AssertionError(name)

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
        forbidden_import,
    )
    settings = ProbeProviderSettings.model_validate(_settings_values(tmp_path))

    with pytest.raises(ProbeSdkError) as exc_info:
        build_probe_transport({}, settings, PhysicalHttpBudget())

    assert exc_info.value.code == "COMPLETE_SDK_ADMISSION"
    assert imported is False


def test_probe_factory_lazily_constructs_counted_no_retry_sdk(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    class FakeResponse:
        headers: dict[str, str] = {}
        status_code = 200

        def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
            del chunk_size
            return (b'{"code":0}',)

        def close(self) -> None:
            return None

    class FakeSession:
        trust_env = True

        def __init__(self) -> None:
            self.closed = False
            self._adapters = {"https://": requests.adapters.HTTPAdapter(max_retries=2)}

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            return FakeResponse()

        def post(self, *_args: object, **_kwargs: object) -> object:
            return FakeResponse()

        def close(self) -> None:
            self.closed = True

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeSession

    class FakeSdkClient:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)
            self._http_session = FakeSession()

    auth_module = SimpleNamespace(requests=FakeRequests())

    def module_import(name: str) -> object:
        if name == "fsopenapi":
            return SimpleNamespace(SDKClient=FakeSdkClient)
        if name == "fsopenapi.auth":
            return auth_module
        raise AssertionError(name)

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
        module_import,
    )
    settings = ProbeProviderSettings.model_validate(_settings_values(tmp_path))
    budget = PhysicalHttpBudget()

    bundle = build_probe_transport(_admitted_document(), settings, budget)

    assert captured == {
        "base_url": "https://openapi.fosunxcz.com",
        "api_key": "synthetic-api-key-canary",
        "logging_enable": False,
        "log_body": False,
    }
    with budget.operation("Accounts"):
        assert budget.try_acquire_at_call_edge() is None
    asyncio.run(bundle.aclose())


@pytest.mark.asyncio
async def test_probe_factory_disables_adapter_level_retries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sdk_calls = 0

    class FakeSession:
        trust_env = True

        def __init__(self) -> None:
            self._adapters = {"https://": requests.adapters.HTTPAdapter(max_retries=2)}

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("the synthetic SDK method fails before HTTP")

        def post(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("the synthetic SDK method fails before HTTP")

        def close(self) -> None:
            return None

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeSession

    def list_accounts() -> object:
        nonlocal sdk_calls
        sdk_calls += 1
        raise RuntimeError("synthetic retryable provider failure")

    class FakeSdkClient:
        def __init__(self, **_kwargs: object) -> None:
            self._http_session = FakeSession()
            self.account = SimpleNamespace(list_accounts=list_accounts)

    auth_module = SimpleNamespace(requests=FakeRequests())

    def module_import(name: str) -> object:
        if name == "fsopenapi":
            return SimpleNamespace(SDKClient=FakeSdkClient)
        if name == "fsopenapi.auth":
            return auth_module
        raise AssertionError(name)

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
        module_import,
    )
    settings = ProbeProviderSettings.model_validate(_settings_values(tmp_path))
    budget = PhysicalHttpBudget()
    bundle = build_probe_transport(_admitted_document(), settings, budget)

    try:
        with budget.operation("Accounts"), pytest.raises(GatewayError):
            await bundle.transport.execute("fosun_sync_accounts", {})
    finally:
        await bundle.aclose()

    assert sdk_calls == 1


def test_probe_settings_require_exact_fosun_base_url_before_sdk_import(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    imported = False

    def forbidden_import(name: str) -> object:
        nonlocal imported
        imported = True
        raise AssertionError(name)

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
        forbidden_import,
    )
    values = _settings_values(tmp_path)
    values["FSOPENAPI_BASE_URL"] = "https://different.invalid"
    with pytest.raises(ValidationError):
        ProbeProviderSettings.model_validate(values)

    assert imported is False


def test_probe_factory_maps_missing_sdk_to_fixed_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def missing_import(_name: str) -> object:
        raise ModuleNotFoundError("synthetic-import-secret")

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
        missing_import,
    )

    with pytest.raises(ProbeSdkError) as exc_info:
        build_probe_transport(
            _admitted_document(),
            ProbeProviderSettings.model_validate(_settings_values(tmp_path)),
            PhysicalHttpBudget(),
        )

    assert exc_info.value.code == "SDK_NOT_AVAILABLE"
    assert "synthetic-import-secret" not in repr(exc_info.value)


def test_probe_factory_rejects_missing_sdk_contract_without_binding_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
        lambda _name: SimpleNamespace(),
    )
    budget = PhysicalHttpBudget()

    with pytest.raises(ProbeSdkError) as exc_info:
        build_probe_transport(
            _admitted_document(),
            ProbeProviderSettings.model_validate(_settings_values(tmp_path)),
            budget,
        )

    assert exc_info.value.code == "SDK_CONTRACT_MISMATCH"
    with pytest.raises(ProbeBudgetError):
        with budget.operation("Accounts"):
            budget.try_acquire_at_call_edge()


@pytest.mark.parametrize("failure_kind", ["error", "interrupt", "system_exit"])
def test_probe_factory_closes_both_sessions_when_bundle_attestation_fails(
    failure_kind: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sessions: list[FakeSession] = []

    class FakeResponse:
        status_code = 200

    class FakeSession:
        trust_env = True

        def __init__(self) -> None:
            self.closed = False
            self._adapters = {
                "https://": requests.adapters.HTTPAdapter(max_retries=2)
            }
            sessions.append(self)

        def mount(self, prefix: str, adapter: object) -> None:
            self._adapters[prefix] = adapter

        def get_adapter(self, prefix: str) -> object:
            return self._adapters[prefix]

        def request(self, *_args: object, **_kwargs: object) -> object:
            return FakeResponse()

        def post(self, *_args: object, **_kwargs: object) -> object:
            return FakeResponse()

        def close(self) -> None:
            self.closed = True

    class FakeRequests:
        adapters = requests.adapters
        Session = FakeSession

    class FakeSdkClient:
        def __init__(self, **_kwargs: object) -> None:
            self._http_session = FakeSession()

    auth_module = SimpleNamespace(requests=FakeRequests())

    def module_import(name: str) -> object:
        if name == "fsopenapi":
            return SimpleNamespace(SDKClient=FakeSdkClient)
        if name == "fsopenapi.auth":
            return auth_module
        raise AssertionError(name)

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
        module_import,
    )
    def fail_bundle(**_kwargs: object) -> object:
        if failure_kind == "interrupt":
            raise KeyboardInterrupt
        if failure_kind == "system_exit":
            raise SystemExit(9)
        raise RuntimeError("synthetic-attestation")

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.ProbeTransportBundle",
        fail_bundle,
    )

    if failure_kind == "error":
        with pytest.raises(ProbeSdkError) as exc_info:
            build_probe_transport(
                _admitted_document(),
                ProbeProviderSettings.model_validate(_settings_values(tmp_path)),
                PhysicalHttpBudget(),
            )
        assert exc_info.value.code == "SDK_CONTRACT_MISMATCH"
    else:
        expected = KeyboardInterrupt if failure_kind == "interrupt" else SystemExit
        with pytest.raises(expected):
            build_probe_transport(
                _admitted_document(),
                ProbeProviderSettings.model_validate(_settings_values(tmp_path)),
                PhysicalHttpBudget(),
            )
    assert len(sessions) == 2
    assert all(session.closed for session in sessions)


class RecordingControls:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class FailingControls:
    def close(self) -> None:
        raise RuntimeError("synthetic-close-secret")


class InstrumentedTransport:
    def __init__(
        self,
        budget: PhysicalHttpBudget,
        *,
        empty_accounts: bool = False,
        fail_at: int | None = None,
        cancel_at: int | None = None,
        auth_statuses: tuple[int, ...] = (200,),
        wrong_bar: bool = False,
        malformed_bar: bool = False,
        cached_at: int | None = None,
        gateway_error_at: int | None = None,
    ) -> None:
        del budget
        self._empty_accounts = empty_accounts
        self._fail_at = fail_at
        self._cancel_at = cancel_at
        self._auth_statuses = auth_statuses
        self._wrong_bar = wrong_bar
        self._malformed_bar = malformed_bar
        self._cached_at = cached_at
        self._gateway_error_at = gateway_error_at
        self._external_controls: RecordingControls | FailingControls | None = None
        self._bundle: ProbeTransportBundle | None = None
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.business_session = _HarnessSession()
        self.auth_session: _HarnessSession | None = None
        self.client = SimpleNamespace(
            _http_session=self.business_session,
            account=SimpleNamespace(list_accounts=self._list_accounts),
            portfolio=SimpleNamespace(
                get_assets_summary=self._get_assets_summary,
                get_holdings=self._get_holdings,
            ),
            trade=SimpleNamespace(
                list_orders=self._list_orders,
                get_cash_flows=self._get_cash_flows,
            ),
            market=SimpleNamespace(quote=self._quote, kline=self._kline),
        )
        self.auth_module = SimpleNamespace(requests=_HarnessRequests(self))

    @property
    def close_calls(self) -> int:
        bundle = self._bundle
        return 0 if bundle is None or bundle.transport.is_ready() else 1

    def attach_controls(self, controls: RecordingControls | FailingControls) -> None:
        self._external_controls = controls

    def attach_bundle(self, bundle: ProbeTransportBundle) -> None:
        self._bundle = bundle

    def new_auth_session(self) -> _HarnessSession:
        session = _HarnessSession(
            statuses=list(self._auth_statuses),
            on_close=self._close_external_controls,
        )
        self.auth_session = session
        return session

    def _close_external_controls(self) -> None:
        controls = self._external_controls
        if controls is not None:
            controls.close()

    def _record(self, operation: str, payload: dict[str, object]) -> None:
        self.calls.append((operation, payload))
        call_index = len(self.calls) - 1
        if call_index == self._cached_at:
            return
        if operation == "fosun_sync_accounts":
            for _ in self._auth_statuses:
                self.auth_module.requests.post("https://provider.invalid/session")
        self.client._http_session.request("POST", "https://provider.invalid/read")
        if call_index == self._cancel_at:
            raise asyncio.CancelledError
        if call_index == self._gateway_error_at:
            raise GatewayError(
                "PROVIDER_RATE_LIMITED",
                "synthetic",
                status_code=429,
                next_action="RETRY_LATER",
            )
        if call_index == self._fail_at:
            raise PermissionError("synthetic-provider-secret")

    def _list_accounts(self) -> object:
        self._record("fosun_sync_accounts", {})
        sub_accounts: list[dict[str, object]] = []
        if not self._empty_accounts:
            sub_accounts.append(
                {
                    "subAccountId": "synthetic-account-canary",
                    "status": 1,
                    "subAccountType": 0,
                }
            )
        return _raw(
            {
                "clientId": 100000,
                "clientStatus": 1,
                "clientType": 1,
                "subAccounts": sub_accounts,
            },
            "synthetic-request-canary",
        )

    def _get_assets_summary(self, *, sub_account_id: str) -> object:
        self._record(
            "fosun_sync_portfolio",
            {"account_id": sub_account_id, "read_kind": "cash"},
        )
        return _raw(
            {
                "breakdown": [
                    {
                        "currency": "USD",
                        "ledgerBalance": "1.00",
                        "cashPurchasingPower": "1.00",
                    }
                ],
                "summary": {},
            },
            "synthetic-request-canary",
        )

    def _get_holdings(
        self,
        *,
        sub_account_id: str,
        start: int | None,
        count: int,
    ) -> object:
        self._record(
            "fosun_sync_portfolio",
            {
                "account_id": sub_account_id,
                "read_kind": "holdings",
                "cursor": None if start is None else str(start),
                "limit": count,
            },
        )
        return _raw(
            {
                "count": 1,
                "list": [
                    {
                        "marketCode": "us",
                        "stockCode": "AAPL",
                        "currency": "USD",
                        "quantity": "1",
                        "quantityAvail": "1",
                        "qtyOnHold": "0",
                    }
                ],
                "start": 0,
                "total": 1,
            },
            "synthetic-request-canary",
        )

    def _list_orders(
        self,
        *,
        sub_account_id: str,
        start: int | None,
        count: int,
    ) -> object:
        self._record(
            "fosun_list_orders",
            {
                "account_id": sub_account_id,
                "cursor": None if start is None else str(start),
                "limit": count,
            },
        )
        return _raw(
            {
                "count": 1,
                "list": [
                    {
                        "orderId": "synthetic-order-canary",
                        "marketCode": "us",
                        "stockCode": "AAPL",
                        "direction": 1,
                        "quantity": "1",
                        "filledQuantity": "0",
                        "orderStatus": 40,
                        "submittedTimestamp": 1787284800,
                    }
                ],
                "start": 0,
                "total": 1,
            },
            "synthetic-request-canary",
        )

    def _get_cash_flows(
        self,
        *,
        sub_account_id: str,
        trade_date_from: str,
        trade_date_to: str,
    ) -> object:
        self._record(
            "fosun_list_cash_flows",
            {
                "account_id": sub_account_id,
                "date_from": trade_date_from,
                "date_to": trade_date_to,
                "cursor": None,
                "limit": 1,
            },
        )
        return _raw(
            {
                "count": 1,
                "list": [
                    {
                        "flowId": "synthetic-flow-canary",
                        "currency": "USD",
                        "amount": "1.00",
                        "createdAt": "2026-08-21T00:00:00Z",
                        "flowType": 1,
                    }
                ],
                "start": 0,
                "total": 1,
            },
            "synthetic-request-canary",
        )

    def _quote(self, *, codes: list[str]) -> object:
        self._record("fosun_get_quotes", {"codes": tuple(codes)})
        return _raw(
            {
                "usAAPL": {
                    "price": 100,
                    "power": 2,
                    "qtDate": 20260821,
                    "qtTime": 120000000,
                }
            },
            "synthetic-request-canary",
        )

    def _kline(
        self,
        *,
        code: str,
        ktype: str,
        start_time: int,
        end_time: int,
        num: int,
    ) -> object:
        self._record(
            "fosun_get_bars",
            {
                "code": code,
                "ktype": ktype,
                "start_time": _timestamp_string(start_time),
                "end_time": _timestamp_string(end_time),
                "limit": num,
            },
        )
        return _raw(
            {
                "data": [
                    {
                        "time": 20260820 if self._wrong_bar else 20260821,
                        "open": "not-a-number" if self._malformed_bar else 100,
                        "high": 100,
                        "low": 100,
                        "close": 100,
                        "vol": 1,
                    }
                ],
                "power": 2,
                "total": 1,
                "delay": True,
            },
            "synthetic-request-canary",
        )


class _HarnessResponse:
    headers: dict[str, str] = {}

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def iter_content(self, *, chunk_size: int) -> tuple[bytes, ...]:
        del chunk_size
        return (b'{"code":0}',)

    def close(self) -> None:
        return None


class _HarnessSession:
    trust_env = True

    def __init__(
        self,
        *,
        statuses: list[int] | None = None,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self._statuses = statuses or [200]
        self._on_close = on_close
        self._adapters = {"https://": requests.adapters.HTTPAdapter(max_retries=2)}
        self.close_count = 0
        self.request_count = 0
        self.post_count = 0

    def mount(self, prefix: str, adapter: object) -> None:
        self._adapters[prefix] = adapter

    def get_adapter(self, prefix: str) -> object:
        return self._adapters[prefix]

    def request(self, *_args: object, **_kwargs: object) -> _HarnessResponse:
        self.request_count += 1
        return _HarnessResponse(200)

    def post(self, *_args: object, **_kwargs: object) -> _HarnessResponse:
        self.post_count += 1
        if not self._statuses:
            raise AssertionError("unexpected auth request")
        return _HarnessResponse(self._statuses.pop(0))

    def close(self) -> None:
        self.close_count += 1
        if self._on_close is not None:
            self._on_close()


class _HarnessRequests:
    adapters = requests.adapters

    def __init__(self, harness: InstrumentedTransport) -> None:
        self._harness = harness

    def Session(self) -> _HarnessSession:  # noqa: N802 - mirrors requests API
        return self._harness.new_auth_session()


def _raw(data: object, request_id: str) -> dict[str, object]:
    return {"code": 0, "requestId": request_id, "data": data}


def _timestamp_string(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _plan() -> ProbeRequestPlan:
    return ProbeRequestPlan(
        symbol="usAAPL",
        session_date=date(2026, 8, 21),
        cash_flow_date_from=date(2026, 8, 20),
        cash_flow_date_to=date(2026, 8, 21),
        limit=1,
    )


def _bound_budget(
    *,
    clock: Callable[[], float] | None = None,
) -> PhysicalHttpBudget:
    budget = _new_budget(clock=clock)
    budget.bind_http_controls()
    return budget


def _new_budget(
    *,
    clock: Callable[[], float] | None = None,
) -> PhysicalHttpBudget:
    resolved_clock = clock or FakeClock()
    sleeper = resolved_clock.sleep if isinstance(resolved_clock, FakeClock) else lambda _delay: None
    return PhysicalHttpBudget(clock=resolved_clock, sleeper=sleeper)


def _probe_bundle(
    transport: InstrumentedTransport,
    controls: RecordingControls | FailingControls,
    budget: PhysicalHttpBudget,
) -> ProbeTransportBundle:
    transport.attach_controls(controls)

    def sdk_client(**_kwargs: object) -> object:
        return transport.client

    def module_import(name: str) -> object:
        if name == "fsopenapi":
            return SimpleNamespace(SDKClient=sdk_client)
        if name == "fsopenapi.auth":
            return transport.auth_module
        raise AssertionError(name)

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
            module_import,
        )
        bundle = build_probe_transport(
            _admitted_document(),
            ProbeProviderSettings.model_validate(
                _settings_values(Path("/private/tmp/nexus-probe-test"))
            ),
            budget,
        )
    transport.attach_bundle(bundle)
    return bundle
