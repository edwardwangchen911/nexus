"""One-shot Fosun probe controls; this module is never imported by the Gateway root."""

from __future__ import annotations

import asyncio
import importlib
import re
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from threading import Lock
from typing import Literal

from nexus_fosun_gateway.adapter.protocol import FosunTransport
from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport
from nexus_fosun_gateway.admission import admitted_sdk_subject
from nexus_fosun_gateway.domain import (
    AccountItem,
    BarItem,
    CashFlowItem,
    CashItem,
    GatewayError,
    GatewayItem,
    GatewayPage,
    OrderItem,
    PositionItem,
    QuoteItem,
)
from nexus_fosun_gateway.policy.operations import GatewayOperation
from nexus_fosun_gateway.probe_settings import ProbeProviderSettings
from nexus_fosun_gateway.security.dependency_logging import (
    enforce_sdk_dependency_log_boundary,
)
from nexus_fosun_gateway.security.provider_tls import (
    HttpSurface,
    ProbeTlsControls,
    enforce_probe_tls_controls,
)

ProbeLabel = Literal[
    "Session",
    "Accounts",
    "CashSummary",
    "Holdings",
    "OrderList",
    "CashFlows",
    "quote",
    "kline_minimal",
]
BusinessProbeLabel = Literal[
    "Accounts",
    "CashSummary",
    "Holdings",
    "OrderList",
    "CashFlows",
    "quote",
    "kline_minimal",
]
ProbeDomain = Literal["broker", "market"]

_LABELS: tuple[ProbeLabel, ...] = (
    "Session",
    "Accounts",
    "CashSummary",
    "Holdings",
    "OrderList",
    "CashFlows",
    "quote",
    "kline_minimal",
)
_BUSINESS_LABELS = frozenset(_LABELS[1:])
_BROKER_LABELS = frozenset(
    {"Accounts", "CashSummary", "Holdings", "OrderList", "CashFlows"}
)
_MARKET_LABELS = frozenset({"quote", "kline_minimal"})
_BROKER_COUNT_LABELS: tuple[ProbeLabel, ...] = (
    "Session",
    "Accounts",
    "CashSummary",
    "Holdings",
    "OrderList",
    "CashFlows",
)
_MARKET_COUNT_LABELS: tuple[ProbeLabel, ...] = ("quote", "kline_minimal")
_MAX_PER_LABEL = 3
_MAX_TOTAL = 24
_MAX_BROKER = 6
_MAX_MARKET = 24
_WINDOW_SECONDS = 60.0
_MIN_DISPATCH_INTERVAL_SECONDS = 1.0
_SYMBOL_PATTERN = re.compile(r"us[A-Z][A-Z0-9.]{0,14}")
_PROBE_BUNDLE_ATTESTATION = object()


def _utc_now() -> datetime:
    return datetime.now(UTC)


class ProbeBudgetError(RuntimeError):
    """Fixed, value-free refusal raised before a provider delegate is called."""

    def __init__(self) -> None:
        super().__init__("probe HTTP budget rejected request")


ProbeErrorCode = Literal[
    "COMPLETE_SDK_ADMISSION",
    "INVALID_PROBE_PLAN",
    "PROBE_SETTINGS_INVALID",
    "SDK_NOT_AVAILABLE",
    "SDK_CONTRACT_MISMATCH",
    "PROBE_ACCOUNT_UNAVAILABLE",
    "PROBE_BUDGET_EXCEEDED",
    "PROBE_RESPONSE_INVALID",
    "PROBE_WINDOW_MISMATCH",
    "PROBE_FAILED",
]


class ProbeSdkError(RuntimeError):
    """A value-free runner failure suitable for the fixed CLI error contract."""

    def __init__(self, code: ProbeErrorCode) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class ProbeBudgetSnapshot:
    counts: tuple[tuple[ProbeLabel, int], ...]
    total: int
    auth_successes: int

    def count(self, label: ProbeLabel) -> int:
        return dict(self.counts)[label]


@dataclass(frozen=True, slots=True)
class ProbeRequestPlan:
    """Already-validated non-secret inputs for the fixed SDK sequence."""

    symbol: str
    session_date: date
    cash_flow_date_from: date
    cash_flow_date_to: date
    limit: int


@dataclass(frozen=True, slots=True)
class ProbeExecutionEvidence:
    """Value-free facts; normalized provider values never cross this boundary."""

    request_counts: tuple[tuple[ProbeLabel, int], ...]
    auth_successes: int
    history_window: bool
    pagination: bool
    adjustment_semantics: bool
    response_contract: bool


class ProbeTransportBundle:
    """Factory-attested transport and controls bound to one physical budget."""

    __slots__ = ("_budget", "controls", "transport")

    def __init__(
        self,
        *,
        transport: FosunTransport,
        controls: ProbeTlsControls,
        budget: PhysicalHttpBudget | None = None,
        client: object | None = None,
        _attestation: object | None = None,
    ) -> None:
        if (
            _attestation is not _PROBE_BUNDLE_ATTESTATION
            or budget is None
            or client is None
            or type(transport) is not SdkFosunTransport
            or not controls.attests(client, budget)
            or getattr(transport, "_client", None) is not client
        ):
            raise ProbeSdkError("SDK_CONTRACT_MISMATCH")
        self.transport = transport
        self.controls = controls
        self._budget = budget

    def is_bound_to(self, budget: PhysicalHttpBudget) -> bool:
        client = getattr(self.transport, "_client", None)
        execute = getattr(self.transport, "execute", None)
        close = getattr(self.transport, "aclose", None)
        return (
            self._budget is budget
            and type(self.transport) is SdkFosunTransport
            and getattr(execute, "__func__", None) is SdkFosunTransport.execute
            and getattr(close, "__func__", None) is SdkFosunTransport.aclose
            and self.controls.attests(client, budget)
        )

    async def aclose(self) -> None:
        """Attempt both closes and expose only a fixed failure."""
        interrupted: BaseException | None = None
        failed = False
        try:
            await self.transport.aclose()
        except (asyncio.CancelledError, KeyboardInterrupt) as error:
            interrupted = error
        except Exception:
            failed = True
        try:
            self.controls.close()
        except (asyncio.CancelledError, KeyboardInterrupt) as error:
            if interrupted is None:
                interrupted = error
        except Exception:
            failed = True
        if interrupted is not None:
            raise interrupted
        if failed:
            raise ProbeSdkError("PROBE_FAILED")


class PhysicalHttpBudget:
    """Count and pace the two reviewed physical HTTP delegates."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._clock = clock
        self._sleeper = sleeper
        self._state_lock = Lock()
        self._dispatch_lock = Lock()
        self._bound = False
        self._active_label: BusinessProbeLabel | None = None
        self._active_domain: ProbeDomain | None = None
        self._counts: dict[ProbeLabel, int] = {label: 0 for label in _LABELS}
        self._total = 0
        self._auth_successes = 0
        self._last_dispatch_at: float | None = None
        self._total_timestamps: deque[float] = deque()
        self._broker_timestamps: deque[float] = deque()
        self._market_timestamps: deque[float] = deque()

    def bind_http_controls(self) -> None:
        with self._state_lock:
            if self._bound:
                raise ProbeBudgetError()
            self._bound = True

    @contextmanager
    def operation(self, label: str) -> Iterator[None]:
        if label not in _BUSINESS_LABELS:
            raise ProbeBudgetError()
        typed_label = label  # narrowed by the closed membership check
        domain: ProbeDomain = "broker" if label in _BROKER_LABELS else "market"
        with self._state_lock:
            if self._active_label is not None:
                raise ProbeBudgetError()
            self._active_label = typed_label  # type: ignore[assignment]
            self._active_domain = domain
        try:
            yield
        finally:
            with self._state_lock:
                if self._active_label == label:
                    self._active_label = None
                    self._active_domain = None

    async def acquire(self) -> None:
        self.try_acquire_at_call_edge()

    async def try_acquire(self) -> float | None:
        return self.try_acquire_at_call_edge()

    def try_acquire_at_call_edge(self) -> float | None:
        with self._state_lock:
            if not self._bound or self._active_label is None:
                raise ProbeBudgetError()
        return None

    def before_dispatch(self, surface: HttpSurface) -> None:
        with self._dispatch_lock:
            while True:
                with self._state_lock:
                    label, domain = self._request_subject(surface)
                    try:
                        now = self._clock()
                    except Exception:
                        raise ProbeBudgetError() from None
                    if self._last_dispatch_at is None:
                        delay = 0.0
                    else:
                        delay = max(
                            self._last_dispatch_at
                            + _MIN_DISPATCH_INTERVAL_SECONDS
                            - now,
                            0.0,
                        )
                    if delay == 0.0:
                        self._prune(now)
                        self._require_available(label, domain)
                        self._counts[label] += 1
                        self._total += 1
                        self._total_timestamps.append(now)
                        if domain == "broker":
                            self._broker_timestamps.append(now)
                        else:
                            self._market_timestamps.append(now)
                        self._last_dispatch_at = now
                        return
                try:
                    self._sleeper(delay)
                except Exception:
                    raise ProbeBudgetError() from None

    def after_response(self, surface: HttpSurface, status_code: int) -> None:
        if not isinstance(status_code, int) or isinstance(status_code, bool):
            raise ProbeBudgetError()
        with self._state_lock:
            label, _domain = self._request_subject(surface)
            if surface == "auth" and label == "Session" and 200 <= status_code < 300:
                self._auth_successes += 1

    def snapshot(self) -> ProbeBudgetSnapshot:
        with self._state_lock:
            return ProbeBudgetSnapshot(
                counts=tuple((label, self._counts[label]) for label in _LABELS),
                total=self._total,
                auth_successes=self._auth_successes,
            )

    def _request_subject(self, surface: HttpSurface) -> tuple[ProbeLabel, ProbeDomain]:
        if not self._bound or self._active_label is None or self._active_domain is None:
            raise ProbeBudgetError()
        if surface == "auth":
            if self._active_label != "Accounts":
                raise ProbeBudgetError()
            return "Session", "broker"
        if surface != "business":
            raise ProbeBudgetError()
        return self._active_label, self._active_domain

    def _prune(self, now: float) -> None:
        cutoff = now - _WINDOW_SECONDS
        for timestamps in (
            self._total_timestamps,
            self._broker_timestamps,
            self._market_timestamps,
        ):
            while timestamps and timestamps[0] <= cutoff:
                timestamps.popleft()

    def _require_available(self, label: ProbeLabel, domain: ProbeDomain) -> None:
        if self._counts[label] >= _MAX_PER_LABEL or self._total >= _MAX_TOTAL:
            raise ProbeBudgetError()
        if len(self._total_timestamps) >= _MAX_TOTAL:
            raise ProbeBudgetError()
        if domain == "broker" and (
            len(self._broker_timestamps) >= _MAX_BROKER
            or sum(self._counts[item] for item in _BROKER_COUNT_LABELS)
            >= _MAX_BROKER
        ):
            raise ProbeBudgetError()
        if domain == "market" and (
            len(self._market_timestamps) >= _MAX_MARKET
            or sum(self._counts[item] for item in _MARKET_COUNT_LABELS) >= _MAX_MARKET
        ):
            raise ProbeBudgetError()


def build_probe_transport(
    manifest_document: object,
    settings: ProbeProviderSettings,
    budget: PhysicalHttpBudget,
) -> ProbeTransportBundle:
    """Construct the sole admitted, counted, no-retry probe transport."""
    subject = admitted_sdk_subject(manifest_document)
    if subject is None:
        raise ProbeSdkError("COMPLETE_SDK_ADMISSION")
    if subject.provider_base_url != settings.fsopenapi_base_url:
        raise ProbeSdkError("PROBE_SETTINGS_INVALID")

    try:
        enforce_sdk_dependency_log_boundary()
        sdk_module = importlib.import_module("fsopenapi")
        auth_module = importlib.import_module("fsopenapi.auth")
    except ImportError:
        raise ProbeSdkError("SDK_NOT_AVAILABLE") from None
    except Exception:
        raise ProbeSdkError("SDK_NOT_AVAILABLE") from None

    client_type = getattr(sdk_module, "SDKClient", None)
    if not callable(client_type):
        raise ProbeSdkError("SDK_CONTRACT_MISMATCH")

    controls: ProbeTlsControls | None = None
    transport: SdkFosunTransport | None = None
    try:
        enforce_sdk_dependency_log_boundary()
        client = client_type(
            base_url=subject.provider_base_url,
            api_key=settings.fsopenapi_api_key.get_secret_value(),
            logging_enable=False,
            log_body=False,
        )
        controls = enforce_probe_tls_controls(
            client,
            auth_module,
            observer=budget,
        )
        budget.bind_http_controls()
        transport = SdkFosunTransport(client, rate_limiter=budget)
    except (asyncio.CancelledError, KeyboardInterrupt):
        if controls is not None:
            _close_controls_quietly(controls)
        raise
    except Exception:
        if transport is not None:
            _close_unstarted_transport(transport)
        if controls is not None:
            _close_controls_quietly(controls)
        raise ProbeSdkError("SDK_CONTRACT_MISMATCH") from None
    assert transport is not None
    assert controls is not None
    return ProbeTransportBundle(
        transport=transport,
        controls=controls,
        budget=budget,
        client=client,
        _attestation=_PROBE_BUNDLE_ATTESTATION,
    )


class SdkProbeExecutor:
    """Run the exact seven-operation probe once and discard all live values."""

    def __init__(
        self,
        bundle: ProbeTransportBundle,
        budget: PhysicalHttpBudget,
        *,
        utc_clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._bundle = bundle
        self._budget = budget
        self._utc_clock = utc_clock
        self._run_lock = Lock()
        self._started = False

    async def run(self, plan: ProbeRequestPlan) -> ProbeExecutionEvidence:
        with self._run_lock:
            if self._started:
                raise ProbeSdkError("PROBE_FAILED")
            self._started = True
        failure: BaseException | None = None
        evidence: ProbeExecutionEvidence | None = None
        try:
            evidence = await self._execute(plan)
        except (asyncio.CancelledError, KeyboardInterrupt) as error:
            failure = error
        except ProbeSdkError as error:
            failure = error
        except ProbeBudgetError:
            failure = ProbeSdkError("PROBE_BUDGET_EXCEEDED")
        except GatewayError as error:
            failure = _map_gateway_error(error)
        except Exception:
            failure = ProbeSdkError("PROBE_FAILED")

        try:
            await self._bundle.aclose()
        except (asyncio.CancelledError, KeyboardInterrupt) as error:
            if failure is None:
                failure = error
        except ProbeSdkError as error:
            if failure is None:
                failure = error
        except Exception:
            if failure is None:
                failure = ProbeSdkError("PROBE_FAILED")

        if failure is not None:
            raise failure
        if evidence is None:
            raise ProbeSdkError("PROBE_FAILED")
        return evidence

    async def _execute(self, plan: ProbeRequestPlan) -> ProbeExecutionEvidence:
        if not self._bundle.is_bound_to(self._budget):
            raise ProbeSdkError("SDK_CONTRACT_MISMATCH")
        self._require_plan(plan)
        accounts = await self._call("Accounts", "fosun_sync_accounts", {})
        _require_page_contract(accounts, AccountItem)
        snapshot = self._budget.snapshot()
        if not 1 <= snapshot.count("Session") <= _MAX_PER_LABEL:
            raise ProbeSdkError("SDK_CONTRACT_MISMATCH")
        if snapshot.auth_successes != 1:
            raise ProbeSdkError("SDK_CONTRACT_MISMATCH")
        if not accounts.items:
            raise ProbeSdkError("PROBE_ACCOUNT_UNAVAILABLE")
        first_account = accounts.items[0]
        if not isinstance(first_account, AccountItem):
            raise ProbeSdkError("PROBE_RESPONSE_INVALID")
        account_id = first_account.external_account_id

        cash = await self._call(
            "CashSummary",
            "fosun_sync_portfolio",
            {"account_id": account_id, "read_kind": "cash"},
        )
        _require_page_contract(cash, CashItem, require_items=True)

        holdings = await self._call(
            "Holdings",
            "fosun_sync_portfolio",
            {
                "account_id": account_id,
                "read_kind": "holdings",
                "cursor": None,
                "limit": plan.limit,
            },
        )
        _require_page_contract(holdings, PositionItem)

        orders = await self._call(
            "OrderList",
            "fosun_list_orders",
            {"account_id": account_id, "cursor": None, "limit": plan.limit},
        )
        _require_page_contract(orders, OrderItem)

        cash_flows = await self._call(
            "CashFlows",
            "fosun_list_cash_flows",
            {
                "account_id": account_id,
                "date_from": plan.cash_flow_date_from.isoformat(),
                "date_to": plan.cash_flow_date_to.isoformat(),
                "cursor": None,
                "limit": plan.limit,
            },
        )
        _require_page_contract(cash_flows, CashFlowItem)

        quotes = await self._call(
            "quote",
            "fosun_get_quotes",
            {"codes": (plan.symbol,)},
        )
        _require_page_contract(quotes, QuoteItem, require_items=True)
        if (
            len(quotes.items) != 1
            or not isinstance(quotes.items[0], QuoteItem)
            or quotes.items[0].instrument_code != plan.symbol
        ):
            raise ProbeSdkError("PROBE_RESPONSE_INVALID")

        session_time = f"{plan.session_date.isoformat()}T00:00:00Z"
        bars = await self._call(
            "kline_minimal",
            "fosun_get_bars",
            {
                "code": plan.symbol,
                "ktype": "day",
                "start_time": session_time,
                "end_time": session_time,
                "limit": 1,
            },
        )
        _require_page_contract(bars, BarItem, require_items=True)
        if len(bars.items) != 1 or not isinstance(bars.items[0], BarItem):
            raise ProbeSdkError("PROBE_WINDOW_MISMATCH")
        bar = bars.items[0]
        if (
            bar.instrument_code != plan.symbol
            or bar.interval != "day"
            or bar.timestamp.astimezone(UTC).date() != plan.session_date
        ):
            raise ProbeSdkError("PROBE_WINDOW_MISMATCH")

        final = self._budget.snapshot()
        if any(final.count(label) < 1 for label in _LABELS):
            raise ProbeSdkError("SDK_CONTRACT_MISMATCH")
        if final.auth_successes != 1:
            raise ProbeSdkError("SDK_CONTRACT_MISMATCH")
        return ProbeExecutionEvidence(
            request_counts=final.counts,
            auth_successes=final.auth_successes,
            history_window=True,
            pagination=True,
            adjustment_semantics=True,
            response_contract=True,
        )

    def _require_plan(self, plan: ProbeRequestPlan) -> None:
        try:
            now = self._utc_clock()
        except Exception:
            raise ProbeSdkError("INVALID_PROBE_PLAN") from None
        if now.tzinfo is None or now.utcoffset() is None:
            raise ProbeSdkError("INVALID_PROBE_PLAN")
        if (
            not isinstance(plan.symbol, str)
            or _SYMBOL_PATTERN.fullmatch(plan.symbol) is None
            or not isinstance(plan.limit, int)
            or isinstance(plan.limit, bool)
            or not 1 <= plan.limit <= 10
            or type(plan.session_date) is not date
            or type(plan.cash_flow_date_from) is not date
            or type(plan.cash_flow_date_to) is not date
            or plan.session_date >= now.astimezone(UTC).date()
            or plan.cash_flow_date_from > plan.cash_flow_date_to
            or (plan.cash_flow_date_to - plan.cash_flow_date_from).days > 7
        ):
            raise ProbeSdkError("INVALID_PROBE_PLAN")

    async def _call(
        self,
        label: BusinessProbeLabel,
        operation: GatewayOperation,
        payload: Mapping[str, object],
    ) -> GatewayPage[GatewayItem]:
        if not self._bundle.is_bound_to(self._budget):
            raise ProbeSdkError("SDK_CONTRACT_MISMATCH")
        try:
            with self._budget.operation(label):
                page = await self._bundle.transport.execute(operation, payload)
        except GatewayError as error:
            if label == "kline_minimal" and error.code == "PROVIDER_MALFORMED_RESPONSE":
                raise ProbeSdkError("PROBE_WINDOW_MISMATCH") from None
            raise
        if not isinstance(page, GatewayPage):
            raise ProbeSdkError("PROBE_RESPONSE_INVALID")
        return page


def _require_page_contract(
    page: GatewayPage[GatewayItem],
    item_type: type[GatewayItem],
    *,
    require_items: bool = False,
) -> None:
    if require_items and not page.items:
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    if any(not isinstance(item, item_type) for item in page.items):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    if not isinstance(page.has_more, bool):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    if page.next_cursor is not None and not isinstance(page.next_cursor, str):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    if page.has_more != (page.next_cursor is not None):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    if page.next_cursor is not None and not page.next_cursor.strip():
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")


def _map_gateway_error(error: GatewayError) -> ProbeSdkError:
    if error.code == "SDK_CONTRACT_MISMATCH":
        return ProbeSdkError("SDK_CONTRACT_MISMATCH")
    if error.code in {"VALIDATION_ERROR", "PROVIDER_CONTRACT_ERROR"}:
        return ProbeSdkError("PROBE_RESPONSE_INVALID")
    return ProbeSdkError("PROBE_FAILED")


def _close_controls_quietly(controls: ProbeTlsControls) -> None:
    try:
        controls.close()
    except Exception:
        pass


def _close_unstarted_transport(transport: FosunTransport) -> None:
    """Close a transport built before an event loop exists, without leaking failures."""
    try:
        asyncio.run(transport.aclose())
    except Exception:
        pass
