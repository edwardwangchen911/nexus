# NEXUS Local Runtime and Real US Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a repeatable fixture local stack and production-ready, default-disabled HTTP transports for Alpaca, SEC, and FRED/ALFRED.

**Architecture:** A deterministic local-stack CLI owns exact Compose commands without exposing environment contents. A provider-neutral fixed-origin HTTP client enforces TLS, redirect, host, path, timeout, retry, response-size, and log boundaries; provider transports build only typed requests and feed the existing normalization adapters. Production composition fails closed when provider configuration is missing, while fixture composition never constructs an HTTP client.

**Tech Stack:** Python 3.12, Pydantic 2, httpx, FastAPI worker composition, PostgreSQL 17, MinIO, Docker Compose v2, pytest/respx/Hypothesis, Ruff, Mypy.

**Spec:** `docs/superpowers/specs/2026-08-28-nexus-phase-3-broker-simulation-design.md`

## Global Constraints

- No task may read or print real API keys, account data, complete environment variables, raw Provider bodies, or production certificates.
- Fixture mode must pass with the offline socket guard and must never construct a production HTTP transport.
- All production HTTP origins are exact credential-free HTTPS origins; redirects are disabled and arbitrary caller-supplied URLs are rejected.
- Alpaca delivery mode is explicit configuration/evidence; it is never inferred from the feed name.
- SEC uses only `data.sec.gov`, an explicit contact User-Agent, and a process-wide ceiling of 5 requests per second.
- FRED/ALFRED admits only the eight `FRED_SERIES` identifiers already defined in `nexus.providers.fred`.
- Missing production configuration fails startup; no production path falls back to fixture.
- Broker profile, Fosun SDK, `SIMULATION_WRITE`, and `LIVE_WRITE` are not started by the local app workflow.
- Every production change follows red-green TDD and ends in a focused commit.

---

## File Map

- `engine/src/nexus/ops/local_stack.py`: exact local Compose command builder, doctor, up/status/down orchestration, and redacted results.
- `engine/tests/unit/ops/test_local_stack.py`: pure command, validation, redaction, and failure tests.
- `engine/pyproject.toml`: `nexus-local-stack` console entry point.
- `engine/src/nexus/providers/http.py`: fixed-origin asynchronous byte transport and bounded retry policy.
- `engine/src/nexus/providers/settings.py`: provider configuration with `SecretStr` and production-only validation.
- `engine/src/nexus/providers/market/alpaca_http.py`: typed Alpaca bars transport.
- `engine/src/nexus/providers/sec_http.py`: SEC fixed-path byte transport.
- `engine/src/nexus/providers/fred_http.py`: FRED/ALFRED fixed-series byte transport.
- `engine/src/nexus/providers/composition.py`: fixture/production provider composition without fallback.
- `engine/src/nexus/jobs/provider_collection.py`: bounded ingestion job handlers.
- `engine/src/nexus/market/object_store.py`: S3-compatible immutable raw/temporary/promote implementation shared by production collection handlers.
- `engine/src/nexus/jobs/daily_pipeline.py`: schedule the existing collection job through the new handlers.
- `engine/src/nexus/worker.py`: register provider handlers from explicit settings.
- `engine/tests/unit/providers/test_http.py`: transport policy tests.
- `engine/tests/unit/providers/test_alpaca_http.py`: exact Alpaca request tests.
- `engine/tests/unit/providers/test_sec_http.py`: exact SEC request tests.
- `engine/tests/unit/providers/test_fred_http.py`: exact FRED/ALFRED request tests.
- `engine/tests/unit/providers/test_composition.py`: fixture isolation and production fail-closed tests.
- `engine/tests/support/provider_fakes.py`: typed `RecordedGet`, `RecordingFixedOriginClient`, fixed UTC clock, and canonical `BarsRequest` builders shared by provider tests.
- `engine/tests/unit/ops/conftest.py`: temporary repository root with copied Compose file and an ignored, non-tracked fixture env file.
- `engine/tests/integration/test_provider_collection_pg.py`: persisted raw evidence, retry, idempotency, and quality tests.
- `infra/env.example`: explicit non-secret provider settings and fixture defaults.
- `docs/runbooks/local-development.md`: one repeatable local workflow using the CLI.

---

### Task 1: Add the redacted local-stack command model

**Files:**
- Create: `engine/src/nexus/ops/local_stack.py`
- Create: `engine/tests/unit/ops/test_local_stack.py`
- Create: `engine/tests/unit/ops/conftest.py`
- Modify: `engine/pyproject.toml`

**Interfaces:**
- Produces: `LocalStackAction`, `LocalStackCommand`, `build_local_stack_commands(action, *, repo_root, env_file, project_name)`, `run_local_stack(action, *, repo_root, env_file, project_name, runner=subprocess.run)`, and `main(argv: Sequence[str] | None = None) -> int`.
- Consumes: `infra/compose.yaml`, `infra/.env.local`, Docker CLI, uv, Node, npm.

- [x] **Step 1: Write failing command-policy tests**

```python
def test_up_uses_exact_app_services_and_never_broker_profile(repo_root: Path) -> None:
    commands = build_local_stack_commands(
        LocalStackAction.UP,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
    )
    rendered = [command.argv for command in commands]
    assert all("broker" not in argv for argv in rendered)
    assert rendered[-1][-4:] == ("up", "-d", "api", "worker")


def test_missing_or_tracked_env_file_fails_before_subprocess(repo_root: Path) -> None:
    with pytest.raises(LocalStackConfigurationError, match="LOCAL_ENV_REQUIRED"):
        build_local_stack_commands(
            LocalStackAction.UP,
            repo_root=repo_root,
            env_file=repo_root / "infra/missing.env",
            project_name="nexus-phase3-local",
        )
```

- [x] **Step 2: Run the tests and verify the missing module failure**

Run: `uv run --project engine pytest engine/tests/unit/ops/test_local_stack.py -q`

Expected: FAIL during collection because `nexus.ops.local_stack` does not exist.

- [x] **Step 3: Implement the exact command builder and redacted runner**

```python
class LocalStackAction(StrEnum):
    DOCTOR = "doctor"
    UP = "up"
    STATUS = "status"
    DOWN = "down"


@dataclass(frozen=True)
class LocalStackCommand:
    argv: tuple[str, ...]
    timeout_seconds: int


def build_local_stack_commands(
    action: LocalStackAction,
    *,
    repo_root: Path,
    env_file: Path,
    project_name: str,
) -> tuple[LocalStackCommand, ...]:
    _validate_repo_root(repo_root)
    _validate_project_name(project_name)
    if action is not LocalStackAction.DOCTOR:
        _validate_local_env(repo_root, env_file)
    compose = (
        "docker", "compose", "-p", project_name,
        "--env-file", str(env_file), "-f", str(repo_root / "infra/compose.yaml"),
        "--profile", "app",
    )
    if action is LocalStackAction.UP:
        return (
            LocalStackCommand((*compose, "config", "--quiet"), 30),
            LocalStackCommand((*compose, "up", "-d", "postgres", "minio"), 120),
            LocalStackCommand((*compose, "run", "--rm", "object-store-init"), 120),
            LocalStackCommand((*compose, "run", "--rm", "--no-deps", "api", "alembic", "-c", "engine/alembic.ini", "upgrade", "head"), 180),
            LocalStackCommand((*compose, "up", "-d", "api", "worker"), 180),
        )
    if action is LocalStackAction.STATUS:
        return (LocalStackCommand((*compose, "ps", "-a"), 30),)
    if action is LocalStackAction.DOWN:
        return (LocalStackCommand((*compose, "stop", "api", "worker"), 60), LocalStackCommand((*compose, "down"), 120))
    return _doctor_commands()
```

`_validate_local_env` resolves both paths, requires a regular file strictly below `repo_root`, rejects symlinks, rejects any file returned by `git -C <repo_root> ls-files --error-unmatch -- <relative-path>`, and rejects a file not matched by `git check-ignore -q`. `run_local_stack` must call `subprocess.run(argv, cwd=repo_root, timeout=..., check=False, text=True, capture_output=True)` and return only action, ordinal, exit code, and a fixed safe result code. It must not return captured stdout/stderr for Compose commands. `main` uses `argparse` with positional `{doctor,up,status,down}` plus `--repo-root`, `--env-file` and `--project-name`; defaults are the discovered repository root, `infra/.env.local`, and `nexus-local`. `doctor` alone may omit the env file.

- [x] **Step 4: Add the console entry point and run focused tests**

Add to `engine/pyproject.toml`:

```toml
nexus-local-stack = "nexus.ops.local_stack:main"
```

Run: `uv run --project engine pytest engine/tests/unit/ops/test_local_stack.py -q`

Expected: all tests PASS, including invalid project names, tracked env files, command timeouts, non-zero exits, and output redaction.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/ops/local_stack.py engine/tests/unit/ops/test_local_stack.py engine/tests/unit/ops/conftest.py engine/pyproject.toml
git commit -m "feat(ops): add redacted local stack controller"
```

---

### Task 2: Implement the fixed-origin HTTP policy

**Files:**
- Create: `engine/src/nexus/providers/http.py`
- Create: `engine/tests/unit/providers/test_http.py`
- Create: `engine/tests/support/provider_fakes.py`

**Interfaces:**
- Produces: `AllowedOrigin`, `HttpRequestPolicy`, `FixedOriginHttpClient.get_bytes(path_segments, query, headers) -> bytes`.
- Consumes: injected `httpx.AsyncClient`, monotonic clock, sleeper.

The test support file defines these complete, non-production helpers before provider-specific tasks use them:

```python
@dataclass(frozen=True)
class RecordedGet:
    path_segments: tuple[str, ...]
    query: tuple[tuple[str, str], ...]
    header_names: tuple[str, ...]


class RecordingFixedOriginClient:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[RecordedGet] = []

    async def get_bytes(self, path_segments, query, headers) -> bytes:
        self.calls.append(RecordedGet(path_segments, query, tuple(name for name, _ in headers)))
        return self.payload
```

- [x] **Step 1: Write failing origin, redirect, deadline, response-size, and retry tests**

```python
@pytest.mark.asyncio
async def test_fixed_origin_rejects_url_shaped_path_before_transport() -> None:
    transport = httpx.MockTransport(lambda request: pytest.fail("network called"))
    client = FixedOriginHttpClient(
        origin=AllowedOrigin(scheme="https", host="data.example.test", port=443),
        policy=HttpRequestPolicy(deadline_seconds=3, max_attempts=2, max_response_bytes=64),
        client=httpx.AsyncClient(transport=transport, follow_redirects=False),
    )
    with pytest.raises(HttpPolicyError, match="PATH_SEGMENT_INVALID"):
        await client.get_bytes(("https://evil.test",), (), ())


@pytest.mark.asyncio
async def test_302_is_never_followed_or_retried() -> None:
    calls = 0
    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(302, headers={"location": "https://evil.test"})
    client = fixed_client(respond)
    with pytest.raises(HttpPolicyError, match="REDIRECT_REJECTED"):
        await client.get_bytes(("v1", "bars"), (), ())
    assert calls == 1
```

- [x] **Step 2: Run the focused tests and verify RED**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_http.py -q`

Expected: FAIL because `nexus.providers.http` is missing.

- [x] **Step 3: Implement the minimal fixed-origin client**

```python
@dataclass(frozen=True)
class AllowedOrigin:
    scheme: Literal["https"]
    host: str
    port: int = 443


@dataclass(frozen=True)
class HttpRequestPolicy:
    deadline_seconds: float = 30
    max_attempts: int = 3
    max_response_bytes: int = 8 * 1024 * 1024
    retry_statuses: frozenset[int] = frozenset({429, 500, 502, 503, 504})


class FixedOriginHttpClient:
    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes:
        path = _canonical_path(path_segments)
        url = httpx.URL(scheme="https", host=self._origin.host, port=self._origin.port, path=path)
        return await self._bounded_get(url, query, headers)
```

`_canonical_path` must reject empty, dot, slash, percent-encoded slash/backslash, scheme, query, fragment, control, and non-ASCII segments. `_bounded_get` must use one absolute monotonic deadline, `follow_redirects=False`, stream chunks while enforcing the byte cap, accept only 2xx, reject 3xx immediately, and retry only configured statuses/transport errors within the same deadline. It may honor an integer `Retry-After` only when `0 <= value <= 5`. Retry ownership is single-layer: Alpaca and FRED use this client's bounded retry, while SEC composition sets `max_attempts=1` because `SecCompanyFactsClient` already owns its three-attempt deadline and process-wide limiter.

- [x] **Step 4: Prove retry and log redaction behavior**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_http.py -q`

Expected: PASS; captured logs contain only provider name, attempt, status class, elapsed bucket, and result code, with no host, path, query, headers, or body.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/providers/http.py engine/tests/unit/providers/test_http.py engine/tests/support/provider_fakes.py
git commit -m "feat(providers): add fixed-origin HTTP policy"
```

---

### Task 3: Add fail-closed provider settings and composition

**Files:**
- Create: `engine/src/nexus/providers/settings.py`
- Create: `engine/src/nexus/providers/composition.py`
- Create: `engine/tests/unit/providers/test_composition.py`
- Modify: `engine/src/nexus/settings.py`

**Interfaces:**
- Produces: `ProviderRuntimeMode`, `ProviderSettings`, `ProviderBundle`, `build_provider_bundle(settings, http_client_factory)`.
- Consumes: existing fixture provider implementations and future HTTP transports from Tasks 4–5.

- [x] **Step 1: Write failing fixture-isolation and production-validation tests**

```python
def test_fixture_bundle_never_calls_http_factory() -> None:
    bundle = build_provider_bundle(
        ProviderSettings(runtime_mode="fixture"),
        http_client_factory=lambda *_: pytest.fail("HTTP factory called"),
    )
    assert bundle.mode == ProviderRuntimeMode.FIXTURE


def test_production_requires_explicit_delivery_mode_and_credentials() -> None:
    with pytest.raises(ValidationError, match="ALPACA_DELIVERY_MODE"):
        ProviderSettings(runtime_mode="production", alpaca_key_id="key", alpaca_secret_key="secret")
```

- [x] **Step 2: Run and observe RED**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_composition.py -q`

Expected: FAIL because settings/composition modules do not exist.

- [x] **Step 3: Implement strict settings and bundle protocols**

```python
class ProviderRuntimeMode(StrEnum):
    FIXTURE = "fixture"
    PRODUCTION = "production"


class ProviderSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    runtime_mode: ProviderRuntimeMode = Field("fixture", validation_alias="NEXUS_PROVIDER_RUNTIME_MODE")
    alpaca_key_id: SecretStr | None = Field(None, validation_alias="ALPACA_API_KEY_ID")
    alpaca_secret_key: SecretStr | None = Field(None, validation_alias="ALPACA_API_SECRET_KEY")
    alpaca_feed: Literal["iex", "sip"] | None = Field(None, validation_alias="ALPACA_DATA_FEED")
    alpaca_delivery_mode: Literal["real_time", "delayed"] | None = Field(None, validation_alias="ALPACA_DELIVERY_MODE")
    sec_contact: str | None = Field(None, validation_alias="SEC_CONTACT")
    fred_api_key: SecretStr | None = Field(None, validation_alias="FRED_API_KEY")

    @model_validator(mode="after")
    def validate_production(self) -> "ProviderSettings":
        if self.runtime_mode is ProviderRuntimeMode.PRODUCTION:
            required = (self.alpaca_key_id, self.alpaca_secret_key, self.alpaca_feed, self.alpaca_delivery_mode, self.sec_contact, self.fred_api_key)
            if any(value is None for value in required):
                raise ValueError("PRODUCTION_PROVIDER_CONFIGURATION_INCOMPLETE")
        return self
```

`ProviderBundle` exposes only typed provider protocols, a mode, and `market_provider(session: AsyncSession) -> MarketDataProvider`; its `repr` must not expose `SecretStr` values. Production market construction creates a session-bound `SqlInstrumentSymbolResolver`, while fixture construction ignores the session, does not resolve secret values, and does not create any HTTP object.

- [x] **Step 4: Run settings, composition, and existing settings tests**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_composition.py engine/tests/test_health.py -q`

Expected: PASS with no secret canary in repr, error, or captured logs.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/providers/settings.py engine/src/nexus/providers/composition.py engine/tests/unit/providers/test_composition.py engine/src/nexus/settings.py
git commit -m "feat(providers): add fail-closed provider composition"
```

---

### Task 4: Implement the Alpaca bars transport

**Files:**
- Create: `engine/src/nexus/providers/market/alpaca_http.py`
- Create: `engine/tests/unit/providers/test_alpaca_http.py`
- Modify: `engine/src/nexus/providers/market/alpaca.py`
- Modify: `engine/src/nexus/providers/composition.py`

**Interfaces:**
- Produces: `AlpacaHttpTransport.fetch_bars(request: BarsRequest, cursor: str | None) -> AlpacaRawResponse` and async `SqlInstrumentSymbolResolver.resolve(instrument_id, as_of) -> str`.
- Consumes: `FixedOriginHttpClient`, `ProviderSettings`, existing `AlpacaMarketDataProvider`.

- [x] **Step 1: Write failing exact request and delivery-authority tests**

```python
@pytest.mark.asyncio
async def test_alpaca_transport_builds_only_the_frozen_bars_path() -> None:
    recorder = RecordingFixedOriginClient(payload=b'{"bars": [], "next_page_token": null}')
    request = canonical_bars_request()
    transport = AlpacaHttpTransport(
        recorder,
        key_id=SecretStr("id"),
        secret=SecretStr("secret"),
        feed="sip",
        symbol_resolver=FixedInstrumentSymbolResolver("AAPL"),
    )
    response = await transport.fetch_bars(request, None)
    assert response.raw_payload == recorder.payload
    assert recorder.calls[0].path_segments == ("v2", "stocks", "AAPL", "bars")
    assert recorder.calls[0].query == (
        ("adjustment", "all"),
        ("end", "2026-08-28T00:00:00Z"),
        ("feed", "sip"),
        ("limit", "10000"),
        ("sort", "asc"),
        ("start", "2026-08-01T00:00:00Z"),
        ("timeframe", "1Day"),
    )
    assert recorder.calls[0].header_names == ("APCA-API-KEY-ID", "APCA-API-SECRET-KEY")


def test_provider_uses_configured_delivery_mode_not_feed_name() -> None:
    provider = AlpacaMarketDataProvider(
        fixture_alpaca_transport(), feed="sip", delivery_mode="delayed"
    )
    assert provider.delivery_mode == "delayed"
```

- [x] **Step 2: Run the focused tests and verify RED**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_alpaca_http.py engine/tests/unit/providers/test_market_providers.py -q`

Expected: FAIL because `AlpacaHttpTransport` and explicit delivery mode are absent.

- [x] **Step 3: Implement typed path/query construction and JSON bounds**

```python
@dataclass(frozen=True)
class AlpacaRawResponse:
    payload: Mapping[str, object]
    raw_payload: bytes
    request_id: str | None


class AlpacaHttpTransport:
    async def fetch_bars(self, request: BarsRequest, cursor: str | None) -> AlpacaRawResponse:
        symbol = _validated_us_symbol(
            await self._symbol_resolver.resolve(request.instrument_id, request.end.date())
        )
        query = _bars_query(request, cursor, self._feed)
        raw = await self._client.get_bytes(
            ("v2", "stocks", symbol, "bars"),
            query,
            (("APCA-API-KEY-ID", self._key_id.get_secret_value()), ("APCA-API-SECRET-KEY", self._secret.get_secret_value())),
        )
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise MarketProviderPayloadError("ALPACA_OBJECT_REQUIRED")
        return AlpacaRawResponse(
            payload=value,
            raw_payload=raw,
            request_id=_optional_request_id(value.get("request_id")),
        )
```

`_bars_query` emits the seven canonical fields shown above and adds one bounded `page_token` last when present; start/end are canonical second-precision UTC. `SqlInstrumentSymbolResolver` performs one awaited query for `InstrumentAlias(alias_type="ticker")` whose validity contains `request.end.date()`, verifies the joined instrument is USD on a supported US exchange, and rejects missing/overlapping aliases. `FixedInstrumentSymbolResolver` is defined in the test support module. Update `AlpacaMarketDataProvider.__init__` to require `feed: Literal["iex", "sip"]` and `delivery_mode: Literal["real_time", "delayed"]`, preserve `AlpacaRawResponse.raw_payload` byte-for-byte, and emit those configured values in provenance. Remove its adapter-level retry so the fixed-origin client is the only retry owner. For a real bar without an upstream availability timestamp, use the adapter's UTC `received_at` as conservative `available_at`; fixture-only `available_at` remains accepted for deterministic historical tests.

- [x] **Step 4: Run provider tests**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_alpaca_http.py engine/tests/unit/providers/test_market_providers.py engine/tests/unit/quality/test_market.py -q`

Expected: PASS for cursor bounds, symbol/path rejection, decimal-string normalization, response-size failure, explicit IEX/SIP labels, and real_time/delayed authority.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/providers/market/alpaca_http.py engine/src/nexus/providers/market/alpaca.py engine/src/nexus/providers/composition.py engine/tests/unit/providers/test_alpaca_http.py
git commit -m "feat(providers): wire bounded Alpaca bars transport"
```

---

### Task 5: Implement SEC and FRED/ALFRED transports

**Files:**
- Create: `engine/src/nexus/providers/sec_http.py`
- Create: `engine/src/nexus/providers/fred_http.py`
- Create: `engine/tests/unit/providers/test_sec_http.py`
- Create: `engine/tests/unit/providers/test_fred_http.py`
- Modify: `engine/src/nexus/providers/composition.py`

**Interfaces:**
- Produces: `SecHttpTransport.get(path, headers) -> bytes`; `FredHttpTransport.get(path) -> bytes`.
- Consumes: `FixedOriginHttpClient`, `SecCompanyFactsClient`, `FredSeriesClient`, `ProviderSettings`.

- [x] **Step 1: Write failing fixed-path tests**

```python
@pytest.mark.asyncio
async def test_sec_accepts_only_companyfacts_and_submissions_paths() -> None:
    client = RecordingFixedOriginClient(payload=b"{}")
    transport = SecHttpTransport(client)
    await transport.get(
        "/api/xbrl/companyfacts/CIK0000320193.json",
        {"User-Agent": "NEXUS owner@example.com", "Accept-Encoding": "gzip, deflate"},
    )
    with pytest.raises(SecProviderError, match="SEC_PATH_NOT_ALLOWED"):
        await transport.get("https://evil.test/a", {"User-Agent": "NEXUS owner@example.com"})


@pytest.mark.asyncio
async def test_fred_allows_only_frozen_series_and_exact_vintage_dates() -> None:
    transport = FredHttpTransport(RecordingFixedOriginClient(payload=b"{}"), SecretStr("fred-key"))
    await transport.get("/fred/series/observations?series_id=DFF")
    with pytest.raises(FredValidationError, match="UNAPPROVED_SERIES"):
        await transport.get("/fred/series/observations?series_id=SP500")
```

- [x] **Step 2: Run and verify RED**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_sec_http.py engine/tests/unit/providers/test_fred_http.py -q`

Expected: FAIL because the HTTP transport modules do not exist.

- [x] **Step 3: Implement exact parsers instead of generic URL forwarding**

```python
_SEC_COMPANYFACTS = re.compile(r"^/api/xbrl/companyfacts/CIK(?P<cik>[0-9]{10})\.json$")
_SEC_SUBMISSIONS = re.compile(r"^/submissions/CIK(?P<cik>[0-9]{10})\.json$")


class SecHttpTransport:
    async def get(self, path: str, headers: dict[str, str]) -> bytes:
        match = _SEC_COMPANYFACTS.fullmatch(path) or _SEC_SUBMISSIONS.fullmatch(path)
        if match is None or set(headers) != {"User-Agent", "Accept-Encoding"}:
            raise SecProviderError("SEC_PATH_NOT_ALLOWED")
        return await self._client.get_bytes(_sec_segments(path), (), tuple(sorted(headers.items())))
```

`FredHttpTransport` must parse with `urllib.parse.parse_qsl(..., strict_parsing=True)`, accept the internal `/fred/series/observations` and `/alfred/series/observations` seams, map both to the external fixed path segments `("fred", "series", "observations")`, require one approved `series_id`, require both ISO `realtime_start`/`realtime_end` together for the ALFRED seam, append `api_key` and `file_type=json` internally, and never log either value.

- [x] **Step 4: Run all provider normalization and transport tests**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_sec_http.py engine/tests/unit/providers/test_fred_http.py engine/tests/unit/providers/test_sec.py engine/tests/unit/providers/test_fred.py -q`

Expected: PASS, including contact header, CIK bounds, date order, series allowlist, 429 budget, response-size, and secret redaction.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/providers/sec_http.py engine/src/nexus/providers/fred_http.py engine/src/nexus/providers/composition.py engine/tests/unit/providers/test_sec_http.py engine/tests/unit/providers/test_fred_http.py
git commit -m "feat(providers): add bounded SEC and FRED transports"
```

---

### Task 6: Wire durable provider collection jobs

**Files:**
- Create: `engine/src/nexus/jobs/provider_collection.py`
- Create: `engine/src/nexus/market/object_store.py`
- Create: `engine/tests/unit/jobs/test_provider_collection.py`
- Create: `engine/tests/integration/test_provider_collection_pg.py`
- Modify: `engine/src/nexus/jobs/daily_pipeline.py`
- Modify: `engine/src/nexus/jobs/daily_payloads.py`
- Modify: `engine/src/nexus/worker.py`

**Interfaces:**
- Produces: `build_provider_collection_registry(bundle, market_object_store, raw_evidence_store, now) -> JobRegistry`; handlers for `collect.market`, `collect.sec_macro` using the existing `DailyStagePayload` schema.
- Consumes: existing job leases/idempotency/outbox, provider bundle, `ingest_price_page`, research persistence, `MarketObjectStore`, and new narrow `RawEvidenceStore.put_if_absent(key, payload) -> versioned_uri`.

`engine/tests/unit/jobs/test_provider_collection.py` defines `InMemoryRawEvidenceStore`, `InMemoryMarketObjectStore`, `fixture_provider_bundle`, `provider_bundle_with_source_conflict`, `run_market_collection`, `selected_price_authority_count`, `daily_stage_context`, `daily_stage_payload`, and the fixed UUID/time constants used below. `engine/tests/integration/test_provider_collection_pg.py` uses the existing PostgreSQL `session` fixtures and in-memory object stores; it does not call S3 or the internet.

- [x] **Step 1: Write failing handler admission tests**

```python
@pytest.mark.asyncio
async def test_fixture_collection_uses_only_fixture_bundle(session) -> None:
    object_store = InMemoryRawEvidenceStore()
    registry = build_provider_collection_registry(
        fixture_provider_bundle(), InMemoryMarketObjectStore(), object_store, FROZEN_NOW
    )
    spec = registry.resolve(JobType.COLLECT_SEC_MACRO.value)
    assert spec is not None
    result = await spec.handler(daily_stage_context(session, target_id=None))
    assert result.outcome == "succeeded"
    assert object_store.external_calls == 0


@pytest.mark.asyncio
async def test_stale_or_conflicting_market_data_cannot_publish_selected_authority(session) -> None:
    result = await run_market_collection(
        session,
        provider_bundle_with_source_conflict(),
        daily_stage_payload(target_id=CANONICAL_INSTRUMENT_ID),
    )
    assert result.result_code == "SOURCE_CONFLICT"
    assert await selected_price_authority_count(session) == 0
```

- [x] **Step 2: Run unit/integration tests and verify RED**

Run unit: `uv run --project engine pytest engine/tests/unit/jobs/test_provider_collection.py -q`

Run PostgreSQL: `uv run --project engine pytest engine/tests/integration/test_provider_collection_pg.py -q`

Expected: collection registry tests fail because the module is missing.

- [x] **Step 3: Implement bounded handlers**

```python
def build_provider_collection_registry(
    bundle: ProviderBundle,
    market_object_store: MarketObjectStore,
    raw_evidence_store: RawEvidenceStore,
    now: Callable[[], datetime],
) -> JobRegistry:
    return JobRegistry((
        JobHandlerSpec(
            job_type=JobType.COLLECT_MARKET,
            schema_version="provider_market_v1",
            payload_model=DailyStagePayload,
            queue=QueueName.GENERAL,
            timeout_seconds=45,
            retry_policy=RetryPolicy.AUTOMATIC_SAFE,
            max_attempts=3,
            idempotency_namespace="provider.market.v1",
            handler=_market_handler(bundle, market_object_store, now),
        ),
        JobHandlerSpec(
            job_type=JobType.COLLECT_SEC_MACRO,
            schema_version="provider_sec_macro_v1",
            payload_model=DailyStagePayload,
            queue=QueueName.GENERAL,
            timeout_seconds=45,
            retry_policy=RetryPolicy.AUTOMATIC_SAFE,
            max_attempts=3,
            idempotency_namespace="provider.sec-macro.v1",
            handler=_sec_macro_handler(bundle, raw_evidence_store, now),
        ),
    ))
```

The scheduler resolves one latest complete `UniverseSnapshot` whose `published_at <= planned_at`, stores its UUID and content hash in the strict `DailyPipelinePayload`, and skips both collection stages with `NO_FROZEN_UNIVERSE` when none exists. The daily DAG remains one global `collect.market` node and one global `collect.sec_macro` node. Each handler reads at most 100 USD US instruments from that exact snapshot and exactly the eight approved macro series; it does not perform unbounded watchlist discovery inside the Job. `S3ProviderEvidenceStore` in `nexus.market.object_store` implements both `MarketObjectStore` and `RawEvidenceStore`, uses content-addressed keys plus object version IDs, and never overwrites an existing digest. Handlers archive exact raw bytes before normalization metadata commits, bind object hash/URI/length/source/received_at, and use the existing source policy to publish or abstain. They return safe result codes and never include provider error text. `collect.market` calls `ingest_price_page` per page; `collect.sec_macro` persists `ResearchEvidenceRef`, normalized financial facts and macro observations through one transaction per provider object so a restart can conflict on immutable identities without duplicating facts.

- [x] **Step 4: Register composition and prove restart idempotency**

Update `worker.py` to construct `ProviderSettings`, call `build_provider_bundle`, and merge the registry only after settings validation. Extend `daily_pipeline.py` payload construction without changing schedule authority.

Run: `uv run --project engine pytest engine/tests/unit/jobs/test_provider_collection.py engine/tests/integration/test_provider_collection_pg.py engine/tests/integration/test_daily_pipeline.py engine/tests/integration/test_job_worker.py -q`

Expected: PASS; same job identity produces one raw object metadata record and one normalized authority set after worker restart.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/jobs/provider_collection.py engine/src/nexus/jobs/daily_pipeline.py engine/src/nexus/jobs/daily_payloads.py engine/src/nexus/market/object_store.py engine/src/nexus/worker.py engine/tests/unit/jobs/test_provider_collection.py engine/tests/integration/test_provider_collection_pg.py
git commit -m "feat(jobs): collect durable provider evidence"
```

---

### Task 7: Expose safe configuration and document the runnable workflow

**Files:**
- Modify: `infra/env.example`
- Modify: `infra/compose.yaml`
- Modify: `docs/runbooks/local-development.md`
- Modify: `README.md`
- Create: `engine/tests/unit/ops/test_provider_compose_policy.py`

**Interfaces:**
- Produces: documented fixture workflow and explicit provider environment mapping.
- Consumes: `nexus-local-stack`, app Compose profile, ProviderSettings.

- [ ] **Step 1: Write failing Compose-policy tests**

```python
def test_app_profile_defaults_to_fixture_and_has_no_provider_secrets(compose_yaml: str) -> None:
    worker = compose_service_block(compose_yaml, "worker")
    assert "NEXUS_PROVIDER_RUNTIME_MODE=${NEXUS_PROVIDER_RUNTIME_MODE:-fixture}" in worker
    assert "ALPACA_API_SECRET_KEY" not in worker
    assert "FRED_API_KEY" not in worker


def test_production_provider_profile_is_not_started_by_local_stack() -> None:
    commands = build_local_stack_commands(
        LocalStackAction.UP,
        repo_root=FIXTURE_REPO_ROOT,
        env_file=FIXTURE_ENV_FILE,
        project_name="nexus-phase3-local",
    )
    assert all("provider" not in command.argv for command in commands)
```

`compose_service_block` is a small indentation-aware helper in this test module that returns the exact text below one top-level service key through the next service key. The fixture copies `infra/compose.yaml` into the temporary repository created by `engine/tests/unit/ops/conftest.py`; no YAML library or Docker daemon is required for the unit assertion.

- [ ] **Step 2: Run and verify the missing configuration failure**

Run: `uv run --project engine pytest engine/tests/unit/ops/test_provider_compose_policy.py -q`

Expected: FAIL because the explicit provider runtime mode is absent.

- [ ] **Step 3: Add only non-secret defaults and production variable names**

Add to `infra/env.example`:

```dotenv
NEXUS_PROVIDER_RUNTIME_MODE=fixture
ALPACA_DATA_FEED=
ALPACA_DELIVERY_MODE=
SEC_CONTACT=
# ALPACA_API_KEY_ID, ALPACA_API_SECRET_KEY and FRED_API_KEY belong only in an approved secret domain.
```

The app worker receives only `NEXUS_PROVIDER_RUNTIME_MODE=fixture` by default. Create no Compose path that mounts production provider secrets in ordinary local development.

- [ ] **Step 4: Update runbooks and verify exact commands**

Document:

```bash
cp infra/env.example infra/.env.local
uv run --project engine nexus-local-stack doctor
uv run --project engine nexus-local-stack up --env-file infra/.env.local
npm run dev
uv run --project engine nexus-local-stack status --env-file infra/.env.local
uv run --project engine nexus-local-stack down --env-file infra/.env.local
```

Run: `uv run --project engine pytest engine/tests/unit/ops/test_provider_compose_policy.py engine/tests/unit/ops/test_local_stack.py -q`

Run: `docker compose --env-file infra/env.example -f infra/compose.yaml --profile app config --quiet`

Expected: PASS; broker profile remains statically valid but is not started.

- [ ] **Step 5: Commit**

```bash
git add infra/env.example infra/compose.yaml docs/runbooks/local-development.md README.md engine/tests/unit/ops/test_provider_compose_policy.py
git commit -m "docs(ops): publish fixture runtime workflow"
```

---

### Task 8: Run runtime and provider acceptance

**Files:**
- Create: `docs/superpowers/reviews/2026-08-28-nexus-runtime-real-data-review.md`
- Modify: `docs/superpowers/plans/2026-08-28-nexus-runtime-real-data.md`

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces: exact acceptance evidence, residual risks, and rollback record.

- [ ] **Step 1: Run focused offline and security suites**

```bash
NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" \
  uv run --project engine pytest \
  engine/tests/unit/providers \
  engine/tests/unit/jobs/test_provider_collection.py \
  engine/tests/unit/ops/test_local_stack.py \
  engine/tests/unit/ops/test_provider_compose_policy.py -q
```

Expected: all focused tests PASS with zero external socket attempts.

- [ ] **Step 2: Run PostgreSQL provider integration tests**

Run: `uv run --project engine pytest engine/tests/integration/test_provider_collection_pg.py engine/tests/integration/test_market_ingestion.py engine/tests/integration/test_agent_persistence_pg.py -q`

Expected: PASS with `nexus_test_%` residual database count 0.

- [ ] **Step 3: Run the exact local fixture smoke workflow**

Create ignored `infra/.env.phase3-acceptance` from `infra/env.example` with deterministic fixture values and loopback ports `55433`, `59000`, `59001`, and `58000`. Use Compose project `nexus-phase3-runtime-acceptance`. Execute `doctor`, `up`, a second object-store initialization replay, `status`, `curl --fail http://127.0.0.1:58000/health/live`, and authenticated `/api/v1/readiness` through the Web BFF using its existing test HMAC flow. In a `finally`/shell trap, execute `nexus-local-stack down --env-file infra/.env.phase3-acceptance --project-name nexus-phase3-runtime-acceptance`; remove only that ignored env file after `docker compose -p nexus-phase3-runtime-acceptance ps -aq` and `docker network ls --filter label=com.docker.compose.project=nexus-phase3-runtime-acceptance -q` are empty. Do not pass `--volumes` and do not touch project `nexus-phase01-task2`.

Expected: API and worker ready, single migration head, bucket existing on replay, no broker services, no external network.

- [ ] **Step 4: Run full static and regression gates**

```bash
uv run --project engine ruff check engine
uv run --project engine mypy engine/src
uv run --project engine pytest --import-mode=importlib \
  engine/tests/unit engine/tests/property engine/tests/evals engine/tests/contract engine/tests/test_health.py -q
npm run check:generated
npm test
npm run typecheck
npm run lint
npm run build
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 5: Record review evidence and commit**

The review must classify Critical/Important/Minor findings, list exact test counts and durations, prove fixture socket attempts 0, secret-pattern findings 0, broker services 0, orphan databases 0, and task Compose residue 0.

```bash
git add docs/superpowers/reviews/2026-08-28-nexus-runtime-real-data-review.md docs/superpowers/plans/2026-08-28-nexus-runtime-real-data.md
git commit -m "docs: record runtime and provider acceptance"
```

## Plan Self-Review

- **Design coverage:** Tasks 1 and 7 cover Workstream A; Tasks 2–6 cover Workstream B; Task 8 covers runtime, offline, security, database and Web regression evidence.
- **Type consistency:** Existing `BarsRequest.instrument_id`, `DailyStagePayload`, `JobRegistry.resolve`, handler timeout ceiling, source-policy authority and generated contracts are used by their real signatures; symbol lookup is an explicit async database boundary.
- **No hidden activation:** Production transports are implemented but default-disabled. The plan contains no provider probe, secret loading, paid call, Fosun operation, simulation write or deployment step.
- **Failure safety:** One retry owner per request chain, conservative `available_at`, raw-byte preservation, bounded universe, content-addressed archive and source-conflict abstention are explicit.
- **Execution readiness:** Every task names create/modify paths, first failing command, minimal interface, focused verification and commit boundary; no `TODO`, `TBD` or user-choice placeholder remains.
