"""Fail-closed provider settings and composition contracts."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date
from uuid import UUID

import pytest
from nexus.providers.base import InstrumentId
from nexus.providers.composition import (
    ProviderBundle,
    build_alpaca_market_provider,
    build_alpaca_snapshot_provider,
    build_provider_bundle,
)
from nexus.providers.market.fixture import FixtureMarketDataProvider
from nexus.providers.market.snapshot_contracts import (
    IntradaySnapshotRequest,
    SnapshotInstrument,
)
from nexus.providers.settings import ProviderRuntimeMode, ProviderSettings
from pydantic import ValidationError

from tests.support.provider_fakes import (
    FixedInstrumentSymbolResolver,
    RecordingFixedOriginClient,
    canonical_bars_request,
)


def test_fixture_bundle_never_constructs_http_or_reads_secret_values() -> None:
    """A fixture regression that enters production composition could make real calls."""

    def forbidden_http_factory(*_: object) -> object:
        pytest.fail("HTTP factory called")

    def forbidden_production_builder(*_: object) -> ProviderBundle:
        pytest.fail("production builder called")

    settings = ProviderSettings(
        runtime_mode="fixture",
        alpaca_key_id="fixture-secret-canary",
        alpaca_secret_key="fixture-secret-canary",
        fred_api_key="fixture-secret-canary",
    )

    bundle = build_provider_bundle(
        settings,
        http_client_factory=forbidden_http_factory,
        production_builder=forbidden_production_builder,
    )

    assert bundle.mode is ProviderRuntimeMode.FIXTURE
    assert isinstance(bundle.market_provider(object()), FixtureMarketDataProvider)
    assert "fixture-secret-canary" not in repr(settings)
    assert "fixture-secret-canary" not in repr(bundle)


@pytest.mark.parametrize(
    ("missing_key", "error_code"),
    (
        ("alpaca_key_id", "ALPACA_API_KEY_ID_REQUIRED"),
        ("alpaca_secret_key", "ALPACA_API_SECRET_KEY_REQUIRED"),
        ("alpaca_feed", "ALPACA_DATA_FEED_REQUIRED"),
        ("alpaca_delivery_mode", "ALPACA_DELIVERY_MODE_REQUIRED"),
        ("sec_contact", "SEC_CONTACT_REQUIRED"),
        ("fred_api_key", "FRED_API_KEY_REQUIRED"),
    ),
)
def test_production_requires_every_explicit_authority(
    missing_key: str,
    error_code: str,
) -> None:
    """Omitting one production authority must fail before composition."""
    values: dict[str, object] = {
        "runtime_mode": "production",
        "alpaca_key_id": "key-id-canary",
        "alpaca_secret_key": "secret-canary",
        "alpaca_feed": "sip",
        "alpaca_delivery_mode": "delayed",
        "sec_contact": "owner@example.com",
        "fred_api_key": "fred-canary",
    }
    values.pop(missing_key)

    with pytest.raises(ValidationError, match=error_code) as captured:
        ProviderSettings(**values)

    rendered = str(captured.value)
    assert "secret-canary" not in rendered
    assert "fred-canary" not in rendered


def test_valid_production_settings_use_only_injected_builder() -> None:
    """A hidden fallback must not replace the explicit production bundle builder."""
    fixture_bundle = build_provider_bundle(
        ProviderSettings(runtime_mode="fixture"),
        http_client_factory=lambda *_: pytest.fail("HTTP factory called"),
    )
    calls = 0

    def production_builder(
        settings: ProviderSettings,
        http_client_factory: Callable[..., object],
    ) -> ProviderBundle:
        nonlocal calls
        calls += 1
        assert settings.runtime_mode is ProviderRuntimeMode.PRODUCTION
        assert callable(http_client_factory)
        return fixture_bundle.with_mode(ProviderRuntimeMode.PRODUCTION)

    result = build_provider_bundle(
        _production_settings(),
        http_client_factory=lambda *_: object(),
        production_builder=production_builder,
    )

    assert result.mode is ProviderRuntimeMode.PRODUCTION
    assert calls == 1


def test_settings_and_composition_logs_never_reveal_secret_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Validation and composition logging must not serialize Provider secrets."""
    caplog.set_level(logging.DEBUG)
    settings = _production_settings()

    def safe_failure(*_: object) -> ProviderBundle:
        raise RuntimeError("PRODUCTION_PROVIDER_BUNDLE_UNAVAILABLE")

    with pytest.raises(RuntimeError, match="PRODUCTION_PROVIDER_BUNDLE_UNAVAILABLE"):
        build_provider_bundle(
            settings,
            http_client_factory=lambda *_: object(),
            production_builder=safe_failure,
        )

    assert "secret-canary" not in caplog.text
    assert "fred-canary" not in caplog.text
    assert "secret-canary" not in repr(settings)


def test_default_production_bundle_constructs_only_three_exact_origins() -> None:
    """A wrong host or nested retry budget must be visible at composition time."""
    calls: list[dict[str, object]] = []

    def http_factory(**kwargs: object) -> RecordingFixedOriginClient:
        calls.append(kwargs)
        return RecordingFixedOriginClient(payload=b"{}")

    bundle = build_provider_bundle(
        _production_settings(),
        http_client_factory=http_factory,
    )

    assert bundle.mode is ProviderRuntimeMode.PRODUCTION
    assert [(call["provider_name"], call["origin"].host) for call in calls] == [
        ("alpaca", "data.alpaca.markets"),
        ("sec", "data.sec.gov"),
        ("fred", "api.stlouisfed.org"),
    ]
    assert [call["policy"].max_attempts for call in calls] == [3, 1, 3]


@pytest.mark.asyncio
async def test_alpaca_factory_uses_configured_feed_and_delivery_authority() -> None:
    """Hard-coded SIP/delayed provenance would misstate the purchased data authority."""
    recorder = RecordingFixedOriginClient(payload=b'{"bars": []}')
    provider = build_alpaca_market_provider(
        _production_settings(),
        client=recorder,
        symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
    )

    page = await provider.fetch_bars(canonical_bars_request())

    assert page.provenance.feed == "sip"
    assert page.provenance.delivery_mode == "delayed"
    assert recorder.calls[0].path_segments == ("v2", "stocks", "AAPL", "bars")


def test_intraday_snapshot_factory_is_disabled_by_default() -> None:
    recorder = RecordingFixedOriginClient(payload=b"{}")
    with pytest.raises(RuntimeError, match="INTRADAY_MONITOR_DISABLED"):
        build_alpaca_snapshot_provider(
            _production_settings(),
            client=recorder,
            symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
        )
    assert recorder.calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    (("intraday_poll_seconds", 14), ("intraday_max_symbols", 31)),
)
def test_intraday_settings_keep_fixed_operational_bounds(field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        ProviderSettings(**{field: value})


@pytest.mark.asyncio
async def test_intraday_snapshot_factory_binds_exact_authority() -> None:
    instrument_id = InstrumentId(UUID("019d0000-0000-7000-8000-0000000000a1"))
    recorder = RecordingFixedOriginClient(payload=b'{"AAPL":{}}')
    provider = build_alpaca_snapshot_provider(
        _production_settings(
            intraday_monitor_mode="production",
            alpaca_intraday_feed="iex",
            alpaca_intraday_delivery_mode="real_time",
        ),
        client=recorder,
        symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
    )

    batch = await provider.fetch_snapshots(
        IntradaySnapshotRequest(
            instruments=(SnapshotInstrument(instrument_id=instrument_id, symbol="AAPL"),),
            symbol_as_of=date(2026, 8, 28),
            feed="iex",
            delivery_mode="real_time",
        )
    )

    assert batch.provenance.feed == "iex"
    assert batch.snapshots[0].trade is None
    assert recorder.calls[0].path_segments == ("v2", "stocks", "snapshots")


def _production_settings(**overrides: object) -> ProviderSettings:
    values: dict[str, object] = {
        "runtime_mode": "production",
        "alpaca_key_id": "key-id-canary",
        "alpaca_secret_key": "secret-canary",
        "alpaca_feed": "sip",
        "alpaca_delivery_mode": "delayed",
        "sec_contact": "owner@example.com",
        "fred_api_key": "fred-canary",
    }
    values.update(overrides)
    return ProviderSettings(**values)
