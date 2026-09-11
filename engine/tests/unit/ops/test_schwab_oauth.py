import base64
import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import nexus.ops.provider_market_stream as provider_market_stream
import pytest
from nexus.providers.market.schwab_oauth import SchwabOAuthToken


def test_schwab_app_config_loads_from_unified_local_file_without_secret_repr(
    tmp_path: Path,
) -> None:
    infra = tmp_path / "infra"
    infra.mkdir()
    infra.joinpath(".env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=fake-app-key\n"
        "NEXUS_SCHWAB_APP_SECRET=fake-app-secret\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )

    loader = getattr(provider_market_stream, "load_schwab_app_config", None)
    assert callable(loader), "Schwab local app configuration loader is missing"
    config = loader(tmp_path, {})

    assert config.app_key == "fake-app-key"
    assert config.app_secret == "fake-app-secret"
    assert config.callback_url == "https://127.0.0.1"
    assert "fake-app-key" not in repr(config)
    assert "fake-app-secret" not in repr(config)


def test_schwab_authorization_url_and_callback_are_bound_to_registered_loopback(
    tmp_path: Path,
) -> None:
    infra = tmp_path / "infra"
    infra.mkdir()
    infra.joinpath(".env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=fake-app-key\n"
        "NEXUS_SCHWAB_APP_SECRET=fake-app-secret\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )
    config = provider_market_stream.load_schwab_app_config(tmp_path, {})
    build = getattr(provider_market_stream, "build_schwab_authorization_url", None)
    parse = getattr(provider_market_stream, "parse_schwab_callback", None)

    assert callable(build), "Schwab authorization URL builder is missing"
    assert callable(parse), "Schwab callback parser is missing"
    assert build(config) == (
        "https://api.schwabapi.com/v1/oauth/authorize?"
        "client_id=fake-app-key&redirect_uri=https%3A%2F%2F127.0.0.1"
    )
    assert (
        parse(
            config,
            "https://127.0.0.1/?code=temporary%40code&session=fake-session",
        )
        == "temporary@code"
    )


@pytest.mark.asyncio
async def test_schwab_authorization_code_is_exchanged_once_without_secret_output(
    tmp_path: Path,
) -> None:
    infra = tmp_path / "infra"
    infra.mkdir()
    infra.joinpath(".env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=fake-app-key\n"
        "NEXUS_SCHWAB_APP_SECRET=fake-app-secret\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )
    config = provider_market_stream.load_schwab_app_config(tmp_path, {})
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "expires_in": 1800,
                "token_type": "Bearer",
                "scope": "api",
                "refresh_token": "fake-refresh-token",
                "access_token": "fake-access-token",
            },
        )

    exchange = getattr(
        provider_market_stream, "exchange_schwab_authorization_code", None
    )
    assert callable(exchange), "Schwab authorization-code exchange is missing"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        token = await exchange(
            config,
            "temporary@code",
            client,
            now=datetime(2026, 9, 11, 12, 0, tzinfo=UTC),
        )

    assert len(requests) == 1
    request = requests[0]
    assert str(request.url) == "https://api.schwabapi.com/v1/oauth/token"
    assert request.headers["Authorization"] == "Basic " + base64.b64encode(
        b"fake-app-key:fake-app-secret"
    ).decode()
    assert parse_qs(request.content.decode()) == {
        "grant_type": ["authorization_code"],
        "code": ["temporary@code"],
        "redirect_uri": ["https://127.0.0.1"],
    }
    assert token.access_token == "fake-access-token"
    assert token.refresh_token == "fake-refresh-token"
    assert token.expires_at == datetime(2026, 9, 11, 12, 30, tzinfo=UTC)
    assert "fake-access-token" not in repr(token)
    assert "fake-refresh-token" not in repr(token)


@pytest.mark.asyncio
async def test_schwab_tokens_are_saved_and_loaded_only_from_private_secret_file(
    tmp_path: Path,
) -> None:
    infra = tmp_path / "infra"
    infra.mkdir()
    infra.joinpath(".env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=fake-app-key\n"
        "NEXUS_SCHWAB_APP_SECRET=fake-app-secret\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )
    config = provider_market_stream.load_schwab_app_config(tmp_path, {})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "expires_in": 1800,
                    "token_type": "Bearer",
                    "scope": "api",
                    "refresh_token": "fake-refresh-token",
                    "access_token": "fake-access-token",
                },
            )
        )
    ) as client:
        token = await provider_market_stream.exchange_schwab_authorization_code(
            config,
            "temporary@code",
            client,
            now=datetime(2026, 9, 11, 12, 0, tzinfo=UTC),
        )
    save = getattr(provider_market_stream, "save_schwab_token", None)
    load = getattr(provider_market_stream, "load_schwab_token", None)

    assert callable(save), "Schwab private token writer is missing"
    assert callable(load), "Schwab private token loader is missing"
    token_path = save(tmp_path, token)
    loaded = load(tmp_path)

    assert token_path == tmp_path / ".secrets" / "nexus-schwab-local" / "tokens.json"
    assert loaded == token
    assert stat.S_IMODE(token_path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600


def test_stale_token_temp_file_does_not_block_next_atomic_save(tmp_path: Path) -> None:
    first = SchwabOAuthToken(
        access_token="first-access-token",
        refresh_token="first-refresh-token",
        obtained_at=datetime(2026, 9, 11, 12, 0, tzinfo=UTC),
        expires_at=datetime(2026, 9, 11, 12, 30, tzinfo=UTC),
    )
    provider_market_stream.save_schwab_token(tmp_path, first)
    directory = tmp_path / ".secrets" / "nexus-schwab-local"
    directory.joinpath(f".tokens-{os.getpid()}.tmp").write_text("interrupted")
    second = SchwabOAuthToken(
        access_token="second-access-token",
        refresh_token="second-refresh-token",
        obtained_at=datetime(2026, 9, 11, 13, 0, tzinfo=UTC),
        expires_at=datetime(2026, 9, 11, 13, 30, tzinfo=UTC),
    )

    provider_market_stream.save_schwab_token(tmp_path, second)

    assert provider_market_stream.load_schwab_token(tmp_path) == second


@pytest.mark.asyncio
async def test_user_preference_yields_only_stream_connection_fields_once() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "accounts": [
                    {
                        "accountNumber": "fake-account-number",
                        "primaryAccount": True,
                        "type": "BROKERAGE",
                        "nickName": "fake-name",
                        "displayAcctId": "fake-display-id",
                        "autoPositionEffect": False,
                        "lotSelectionMethod": "FIFO",
                    }
                ],
                "streamerInfo": [
                    {
                        "streamerSocketUrl": "wss://streamer-api.schwab.com/ws",
                        "schwabClientCustomerId": "fake-customer",
                        "schwabClientCorrelId": "fake-correlation",
                        "schwabClientChannel": "fake-channel",
                        "schwabClientFunctionId": "fake-function",
                    }
                ],
                "offers": [
                    {"level2Permissions": True, "mktDataPermission": "NP"}
                ],
            },
        )

    fetch = getattr(provider_market_stream, "fetch_schwab_stream_preferences", None)
    assert callable(fetch), "Schwab user-preference fetch is missing"
    token = SchwabOAuthToken(
        access_token="fake-access-token",
        refresh_token="fake-refresh-token",
        obtained_at=datetime(2026, 9, 11, 12, 0, tzinfo=UTC),
        expires_at=datetime(2026, 9, 11, 12, 30, tzinfo=UTC),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        streamer_url, credentials = await fetch(
            token,
            client,
            now=datetime(2026, 9, 11, 12, 10, tzinfo=UTC),
        )

    assert len(requests) == 1
    assert str(requests[0].url) == (
        "https://api.schwabapi.com/trader/v1/userPreference"
    )
    assert requests[0].headers["Authorization"] == "Bearer fake-access-token"
    assert streamer_url == "wss://streamer-api.schwab.com/ws"
    assert credentials.access_token == "fake-access-token"
    assert credentials.customer_id == "fake-customer"
    assert credentials.correlation_id == "fake-correlation"
    assert credentials.channel == "fake-channel"
    assert credentials.function_id == "fake-function"
    assert "fake-account-number" not in repr((streamer_url, credentials))


def test_market_stream_cli_exposes_authorize_and_repo_bound_stream_without_secrets(
    tmp_path: Path,
) -> None:
    authorize = provider_market_stream.build_parser().parse_args(
        ["authorize", "--repo-root", str(tmp_path)]
    )
    stream = provider_market_stream.build_parser().parse_args(
        [
            "schwab",
            "--repo-root",
            str(tmp_path),
            "--symbol",
            "AAPL",
            "--output-dir",
            str(tmp_path / "archive"),
        ]
    )

    assert authorize.source == "authorize"
    assert authorize.repo_root == tmp_path
    assert stream.source == "schwab"
    assert stream.repo_root == tmp_path
    assert all(
        "token" not in key and "secret" not in key
        for parsed in (authorize, stream)
        for key in vars(parsed)
    )


@pytest.mark.asyncio
async def test_authorize_flow_opens_browser_reads_callback_and_returns_safe_summary(
    tmp_path: Path,
) -> None:
    infra = tmp_path / "infra"
    infra.mkdir()
    infra.joinpath(".env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=fake-app-key\n"
        "NEXUS_SCHWAB_APP_SECRET=fake-app-secret\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )
    opened: list[str] = []
    prompts: list[str] = []

    def read_callback(prompt: str) -> str:
        prompts.append(prompt)
        return "https://127.0.0.1/?code=temporary%40code&session=fake-session"

    authorize = getattr(provider_market_stream, "authorize_schwab", None)
    assert callable(authorize), "Schwab authorization orchestration is missing"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "expires_in": 1800,
                    "token_type": "Bearer",
                    "scope": "api",
                    "refresh_token": "fake-refresh-token",
                    "access_token": "fake-access-token",
                },
            )
        )
    ) as client:
        result = await authorize(
            tmp_path,
            client,
            opener=lambda url: opened.append(url) or True,
            callback_reader=read_callback,
            now=datetime(2026, 9, 11, 12, 0, tzinfo=UTC),
        )

    assert opened == [
        "https://api.schwabapi.com/v1/oauth/authorize?"
        "client_id=fake-app-key&redirect_uri=https%3A%2F%2F127.0.0.1"
    ]
    assert prompts == ["完成嘉信授权后，粘贴浏览器地址栏中的完整回调 URL："]
    assert result == {
        "status": "authorized",
        "token_path": str(
            tmp_path / ".secrets" / "nexus-schwab-local" / "tokens.json"
        ),
        "expires_at": "2026-09-11T12:30:00+00:00",
        "secrets_printed": False,
    }
    assert "fake-access-token" not in repr(result)
    assert "fake-refresh-token" not in repr(result)


@pytest.mark.asyncio
async def test_stream_command_resolves_private_token_and_user_preference_before_capture(
    tmp_path: Path,
) -> None:
    provider_market_stream.save_schwab_token(
        tmp_path,
        SchwabOAuthToken(
            access_token="fake-access-token",
            refresh_token="fake-refresh-token",
            obtained_at=datetime(2026, 9, 11, 12, 0, tzinfo=UTC),
            expires_at=datetime(2026, 9, 11, 12, 30, tzinfo=UTC),
        ),
    )
    capture_calls: list[dict[str, object]] = []

    async def capture(**kwargs: object) -> dict[str, object]:
        capture_calls.append(kwargs)
        return {"status": "bounded_complete"}

    stream = getattr(provider_market_stream, "run_schwab_stream", None)
    assert callable(stream), "Schwab token-to-stream orchestration is missing"
    arguments = provider_market_stream.build_parser().parse_args(
        [
            "schwab",
            "--repo-root",
            str(tmp_path),
            "--symbol",
            "AAPL",
            "--output-dir",
            str(tmp_path / "archive"),
            "--duration",
            "5",
            "--max-frames",
            "3",
        ]
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "accounts": [],
                    "streamerInfo": [
                        {
                            "streamerSocketUrl": "wss://streamer-api.schwab.com/ws",
                            "schwabClientCustomerId": "fake-customer",
                            "schwabClientCorrelId": "fake-correlation",
                            "schwabClientChannel": "fake-channel",
                            "schwabClientFunctionId": "fake-function",
                        }
                    ],
                    "offers": [],
                },
            )
        )
    ) as client:
        result = await stream(
            arguments,
            client,
            collector=capture,
            now=datetime(2026, 9, 11, 12, 10, tzinfo=UTC),
        )

    assert result == {"status": "bounded_complete"}
    assert len(capture_calls) == 1
    call = capture_calls[0]
    assert call["symbols"] == ("AAPL",)
    assert call["directory"] == tmp_path / "archive"
    assert call["streamer_url"] == "wss://streamer-api.schwab.com/ws"
    assert call["duration"] == 5.0
    assert call["max_frames"] == 3
    assert call["credentials"].access_token == "fake-access-token"


@pytest.mark.asyncio
async def test_cli_dispatches_authorize_without_command_line_secrets(tmp_path: Path) -> None:
    infra = tmp_path / "infra"
    infra.mkdir()
    infra.joinpath(".env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=fake-app-key\n"
        "NEXUS_SCHWAB_APP_SECRET=fake-app-secret\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )
    arguments = provider_market_stream.build_parser().parse_args(
        ["authorize", "--repo-root", str(tmp_path)]
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={
                    "expires_in": 1800,
                    "token_type": "Bearer",
                    "scope": "api",
                    "refresh_token": "fake-refresh-token",
                    "access_token": "fake-access-token",
                },
            )
        )
    ) as client:
        result = await provider_market_stream._run(
            arguments,
            client=client,
            opener=lambda _url: True,
            callback_reader=lambda _prompt: (
                "https://127.0.0.1/?code=temporary%40code&session=fake-session"
            ),
        )

    assert result["status"] == "authorized"
    assert "fake-access-token" not in repr(result)
