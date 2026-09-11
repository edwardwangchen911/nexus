import base64
import os
import stat
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import nexus.ops.provider_market_stream as provider_market_stream
import pytest
from nexus.providers.market.schwab_oauth import SchwabOAuthToken

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
EXPIRES = datetime(2026, 9, 11, 12, 30, tzinfo=UTC)
CALLBACK = "https://127.0.0.1/?code=temporary%40code&session=fake-session"


def _write_config(root: Path) -> None:
    infra = root / "infra"
    infra.mkdir()
    infra.joinpath(".env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=fake-app-key\n"
        "NEXUS_SCHWAB_APP_SECRET=fake-app-secret\n"
        "NEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )


def _token_payload() -> dict[str, object]:
    return {
        "expires_in": 1800,
        "token_type": "Bearer",
        "scope": "api",
        "refresh_token": "fake-refresh-token",
        "access_token": "fake-access-token",
    }


def _refresh_payload(*, rotate: bool = True) -> dict[str, object]:
    payload: dict[str, object] = {
        "expires_in": 1800,
        "token_type": "Bearer",
        "scope": "api",
        "access_token": "refreshed-access-token",
    }
    if rotate:
        payload["refresh_token"] = "rotated-refresh-token"
    return payload


def _token(*, prefix: str = "fake") -> SchwabOAuthToken:
    return SchwabOAuthToken(
        access_token=f"{prefix}-access-token",
        refresh_token=f"{prefix}-refresh-token",
        obtained_at=NOW,
        expires_at=EXPIRES,
    )


def _preference_payload(*, with_account: bool) -> dict[str, object]:
    accounts: list[dict[str, object]] = []
    if with_account:
        accounts.append(
            {
                "accountNumber": "fake-account-number",
                "primaryAccount": True,
                "type": "BROKERAGE",
                "nickName": "fake-name",
                "displayAcctId": "fake-display-id",
                "autoPositionEffect": False,
                "lotSelectionMethod": "FIFO",
            }
        )
    return {
        "accounts": accounts,
        "streamerInfo": [
            {
                "streamerSocketUrl": "wss://streamer-api.schwab.com/ws",
                "schwabClientCustomerId": "fake-customer",
                "schwabClientCorrelId": "fake-correlation",
                "schwabClientChannel": "fake-channel",
                "schwabClientFunctionId": "fake-function",
            }
        ],
        "offers": [{"level2Permissions": True, "mktDataPermission": "NP"}],
    }


def test_schwab_app_config_loads_from_unified_local_file_without_secret_repr(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)

    config = provider_market_stream.load_schwab_app_config(tmp_path, {})

    assert config.app_key == "fake-app-key"
    assert config.app_secret == "fake-app-secret"
    assert config.callback_url == "https://127.0.0.1"
    assert "fake-app-key" not in repr(config)
    assert "fake-app-secret" not in repr(config)


def test_schwab_authorization_url_and_callback_are_bound_to_registered_loopback(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)
    config = provider_market_stream.load_schwab_app_config(tmp_path, {})

    assert provider_market_stream.build_schwab_authorization_url(config) == (
        "https://api.schwabapi.com/v1/oauth/authorize?"
        "client_id=fake-app-key&redirect_uri=https%3A%2F%2F127.0.0.1"
    )
    assert provider_market_stream.parse_schwab_callback(config, CALLBACK) == (
        "temporary@code"
    )


def test_native_dialog_returns_callback_without_putting_it_in_process_arguments() -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout=f"{CALLBACK}\n", stderr="")

    reader = getattr(provider_market_stream, "read_schwab_callback_dialog", None)
    assert callable(reader), "native Schwab callback dialog is missing"

    assert reader("ignored", runner=run) == CALLBACK
    assert len(calls) == 1
    command, options = calls[0]
    assert command[0] == "/usr/bin/osascript"
    assert CALLBACK not in repr(command)
    assert options["capture_output"] is True
    assert options["text"] is True


@pytest.mark.asyncio
async def test_macos_authorize_command_uses_native_dialog_by_default(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)
    arguments = provider_market_stream.build_parser().parse_args(
        ["authorize", "--repo-root", str(tmp_path)]
    )
    dialog_calls = 0

    def run_dialog(
        command: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        nonlocal dialog_calls
        dialog_calls += 1
        return subprocess.CompletedProcess(command, 0, stdout=f"{CALLBACK}\n", stderr="")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=_token_payload())
        )
    ) as client:
        result = await provider_market_stream._run(
            arguments,
            client=client,
            opener=lambda _url: True,
            dialog_runner=run_dialog,
            now=NOW,
        )

    assert dialog_calls == 1
    assert result["status"] == "authorized"
    assert provider_market_stream.load_schwab_token(tmp_path).access_token == (
        "fake-access-token"
    )


@pytest.mark.asyncio
async def test_schwab_authorization_code_is_exchanged_once_without_secret_output(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)
    config = provider_market_stream.load_schwab_app_config(tmp_path, {})
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_token_payload())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        token = await provider_market_stream.exchange_schwab_authorization_code(
            config, "temporary@code", client, now=NOW
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
    assert token.expires_at == EXPIRES
    assert "fake-access-token" not in repr(token)
    assert "fake-refresh-token" not in repr(token)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rotate", "expected_refresh_token"),
    [(True, "rotated-refresh-token"), (False, "fake-refresh-token")],
)
async def test_schwab_refresh_is_one_shot_and_preserves_or_rotates_refresh_token(
    tmp_path: Path, rotate: bool, expected_refresh_token: str
) -> None:
    _write_config(tmp_path)
    config = provider_market_stream.load_schwab_app_config(tmp_path, {})
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_refresh_payload(rotate=rotate))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        refreshed = await provider_market_stream.refresh_schwab_access_token(
            config,
            _token(),
            client,
            now=datetime(2026, 9, 11, 12, 20, tzinfo=UTC),
        )

    assert len(requests) == 1
    request = requests[0]
    assert str(request.url) == "https://api.schwabapi.com/v1/oauth/token"
    assert request.headers["Authorization"] == "Basic " + base64.b64encode(
        b"fake-app-key:fake-app-secret"
    ).decode()
    assert parse_qs(request.content.decode()) == {
        "grant_type": ["refresh_token"],
        "refresh_token": ["fake-refresh-token"],
    }
    assert refreshed.access_token == "refreshed-access-token"
    assert refreshed.refresh_token == expected_refresh_token
    assert refreshed.expires_at == datetime(2026, 9, 11, 12, 50, tzinfo=UTC)
    assert "refreshed-access-token" not in repr(refreshed)
    assert expected_refresh_token not in repr(refreshed)


@pytest.mark.asyncio
async def test_token_rejection_reports_only_safe_http_status(tmp_path: Path) -> None:
    _write_config(tmp_path)
    config = provider_market_stream.load_schwab_app_config(tmp_path, {})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                401, content=b"provider body with fake-app-secret"
            )
        )
    ) as client:
        with pytest.raises(
            ValueError, match="^SCHWAB_OAUTH_TOKEN_HTTP_401$"
        ) as captured:
            await provider_market_stream.exchange_schwab_authorization_code(
                config, "temporary@code", client, now=NOW
            )

    assert "fake-app-secret" not in str(captured.value)


def test_schwab_tokens_are_saved_and_loaded_only_from_private_secret_file(
    tmp_path: Path,
) -> None:
    token = _token()

    token_path = provider_market_stream.save_schwab_token(tmp_path, token)

    assert token_path == tmp_path / ".secrets" / "nexus-schwab-local" / "tokens.json"
    assert provider_market_stream.load_schwab_token(tmp_path) == token
    assert stat.S_IMODE(token_path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600


def test_stale_token_temp_file_does_not_block_next_atomic_save(tmp_path: Path) -> None:
    provider_market_stream.save_schwab_token(tmp_path, _token(prefix="first"))
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
        return httpx.Response(200, json=_preference_payload(with_account=True))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        streamer_url, credentials = (
            await provider_market_stream.fetch_schwab_stream_preferences(
                _token(),
                client,
                now=datetime(2026, 9, 11, 12, 10, tzinfo=UTC),
            )
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


@pytest.mark.asyncio
async def test_user_preference_rejection_reports_only_safe_http_status() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                403, content=b"provider body with fake-access-token"
            )
        )
    ) as client:
        with pytest.raises(
            ValueError, match="^SCHWAB_USER_PREFERENCE_HTTP_403$"
        ) as captured:
            await provider_market_stream.fetch_schwab_stream_preferences(
                _token(),
                client,
                now=datetime(2026, 9, 11, 12, 10, tzinfo=UTC),
            )

    assert "fake-access-token" not in str(captured.value)


def test_market_stream_cli_exposes_authorize_and_repo_bound_stream_without_secrets(
    tmp_path: Path,
) -> None:
    authorize = provider_market_stream.build_parser().parse_args(
        ["authorize", "--repo-root", str(tmp_path)]
    )
    refresh = provider_market_stream.build_parser().parse_args(
        ["refresh", "--repo-root", str(tmp_path)]
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
    assert refresh.source == "refresh"
    assert refresh.repo_root == tmp_path
    assert stream.source == "schwab"
    assert stream.repo_root == tmp_path
    assert all(
        "token" not in key and "secret" not in key
        for parsed in (authorize, refresh, stream)
        for key in vars(parsed)
    )


@pytest.mark.asyncio
async def test_refresh_command_atomically_replaces_token_and_returns_safe_summary(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)
    original = _token()
    provider_market_stream.save_schwab_token(tmp_path, original)
    arguments = provider_market_stream.build_parser().parse_args(
        ["refresh", "--repo-root", str(tmp_path)]
    )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=_refresh_payload())
        )
    ) as client:
        result = await provider_market_stream._run(
            arguments,
            client=client,
            now=datetime(2026, 9, 11, 12, 20, tzinfo=UTC),
        )

    saved = provider_market_stream.load_schwab_token(tmp_path)
    assert saved.access_token == "refreshed-access-token"
    assert saved.refresh_token == "rotated-refresh-token"
    assert result == {
        "status": "refreshed",
        "token_path": str(
            tmp_path / ".secrets" / "nexus-schwab-local" / "tokens.json"
        ),
        "expires_at": "2026-09-11T12:50:00+00:00",
        "secrets_printed": False,
    }


@pytest.mark.asyncio
async def test_failed_refresh_keeps_previous_private_token(tmp_path: Path) -> None:
    _write_config(tmp_path)
    original = _token()
    provider_market_stream.save_schwab_token(tmp_path, original)
    arguments = provider_market_stream.build_parser().parse_args(
        ["refresh", "--repo-root", str(tmp_path)]
    )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                401, content=b"provider body with fake-refresh-token"
            )
        )
    ) as client:
        with pytest.raises(
            ValueError, match="^SCHWAB_OAUTH_REFRESH_HTTP_401$"
        ) as captured:
            await provider_market_stream._run(arguments, client=client, now=NOW)

    assert provider_market_stream.load_schwab_token(tmp_path) == original
    assert "fake-refresh-token" not in str(captured.value)


@pytest.mark.asyncio
async def test_authorize_flow_opens_browser_reads_callback_and_returns_safe_summary(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)
    arguments = provider_market_stream.build_parser().parse_args(
        ["authorize", "--repo-root", str(tmp_path)]
    )
    opened: list[str] = []
    prompts: list[str] = []

    def read_callback(prompt: str) -> str:
        prompts.append(prompt)
        return CALLBACK

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=_token_payload())
        )
    ) as client:
        result = await provider_market_stream._run(
            arguments,
            client=client,
            opener=lambda url: opened.append(url) or True,
            callback_reader=read_callback,
            now=NOW,
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


@pytest.mark.asyncio
async def test_stream_command_resolves_private_token_and_user_preference_before_capture(
    tmp_path: Path,
) -> None:
    provider_market_stream.save_schwab_token(tmp_path, _token())
    capture_calls: list[dict[str, object]] = []

    async def capture(**kwargs: object) -> dict[str, object]:
        capture_calls.append(kwargs)
        return {"status": "bounded_complete"}

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
                200, json=_preference_payload(with_account=False)
            )
        )
    ) as client:
        result = await provider_market_stream.run_schwab_stream(
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
async def test_stream_command_auto_refreshes_access_token_near_expiry(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)
    provider_market_stream.save_schwab_token(tmp_path, _token())
    requests: list[httpx.Request] = []
    captured_access_tokens: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if str(request.url) == "https://api.schwabapi.com/v1/oauth/token":
            return httpx.Response(200, json=_refresh_payload())
        return httpx.Response(200, json=_preference_payload(with_account=False))

    async def capture(**kwargs: object) -> dict[str, object]:
        credentials = kwargs["credentials"]
        captured_access_tokens.append(credentials.access_token)
        return {"status": "bounded_complete"}

    arguments = provider_market_stream.build_parser().parse_args(
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
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await provider_market_stream.run_schwab_stream(
            arguments,
            client,
            collector=capture,
            now=datetime(2026, 9, 11, 12, 27, tzinfo=UTC),
        )

    assert result == {"status": "bounded_complete"}
    assert [str(request.url) for request in requests] == [
        "https://api.schwabapi.com/v1/oauth/token",
        "https://api.schwabapi.com/trader/v1/userPreference",
    ]
    assert captured_access_tokens == ["refreshed-access-token"]
    assert provider_market_stream.load_schwab_token(tmp_path).refresh_token == (
        "rotated-refresh-token"
    )
