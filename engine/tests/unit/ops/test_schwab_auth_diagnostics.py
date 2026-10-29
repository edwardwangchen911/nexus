"""Classify OAuth failures without retaining provider prose or changing retry policy."""

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from nexus.ops.agent_paper_once import _raise_schwab_l1_incomplete
from nexus.providers.market import schwab_oauth as oauth

NOW = datetime(2026, 10, 29, 13, 30, tzinfo=UTC)
CONFIG = oauth.SchwabAppConfig("fake-key", "fake-secret", "https://127.0.0.1")
TOKEN = oauth.SchwabOAuthToken(
    "fake-access",
    "fake-refresh",
    NOW - timedelta(days=3),
    NOW - timedelta(days=3) + timedelta(minutes=30),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["refresh", "exchange"])
@pytest.mark.parametrize(
    "category", ["invalid_grant", "invalid_client", "invalid_request", "unknown-provider-text"]
)
async def test_token_error_retains_only_allowlisted_category(operation, category, tmp_path):
    calls = []

    def transport(request):
        calls.append(request)
        return httpx.Response(
            400,
            json={
                "error": category,
                "error_description": "fake-refresh fake-secret personal-response",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        with pytest.raises(oauth.SchwabOAuthError) as caught:
            if operation == "refresh":
                await oauth.refresh_schwab_access_token(CONFIG, TOKEN, client, now=NOW)
            else:
                await oauth.exchange_schwab_authorization_code(CONFIG, "fake-code", client, now=NOW)
    expected = category if category != "unknown-provider-text" else None
    assert caught.value.oauth_error_code == expected
    assert len(calls) == 1
    assert (
        str(caught.value)
        == f"SCHWAB_OAUTH_{'REFRESH' if operation == 'refresh' else 'TOKEN'}_HTTP_400"
    )
    assert "personal-response" not in repr(caught.value)
    path = tmp_path / "failure.json"
    with pytest.raises(ValueError, match="^SCHWAB_L1_INCOMPLETE$"):
        _raise_schwab_l1_incomplete(caught.value, diagnostic_path=path)
    saved = json.loads(path.read_text())
    assert saved.get("oauth_error_code") == expected
    assert "fake-refresh" not in path.read_text() and "fake-secret" not in path.read_text()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b'{"error":"invalid_grant","error":"invalid_client"}',
        b'{"error":["invalid_grant"]}',
        b"x" * 262145,
    ],
    ids=["non-json", "duplicate-error", "non-string-error", "oversized"],
)
async def test_invalid_error_body_keeps_http_failure_unknown(body):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(400, content=body))
    ) as client:
        with pytest.raises(oauth.SchwabOAuthError) as caught:
            await oauth.refresh_schwab_access_token(CONFIG, TOKEN, client, now=NOW)
    assert str(caught.value) == "SCHWAB_OAUTH_REFRESH_HTTP_400"
    assert caught.value.oauth_error_code is None


@pytest.mark.parametrize("current", [False, True])
def test_local_readiness_never_claims_remote_authentication(tmp_path, current):
    token = (
        TOKEN
        if not current
        else oauth.SchwabOAuthToken("fake-access", "fake-refresh", NOW, NOW + timedelta(minutes=30))
    )
    oauth.save_schwab_token(tmp_path, token)
    original = (tmp_path / ".secrets/nexus-schwab-local/tokens.json").read_bytes()
    result = oauth.inspect_schwab_token_readiness(tmp_path, now=NOW)
    assert result["local_access_token"] == ("current" if current else "refresh_required")
    assert result["remote_authentication"] == "not_checked"
    assert result["market_window_evidence"] == "not_evaluated"
    assert (tmp_path / ".secrets/nexus-schwab-local/tokens.json").read_bytes() == original
    assert "fake-access" not in json.dumps(result) and "fake-refresh" not in json.dumps(result)


def test_readiness_cli_separates_static_success_from_expired_local_auth(
    tmp_path, monkeypatch, capsys
):
    from nexus.ops import agent_paper_readiness as readiness

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setattr(readiness, "datetime", Clock)
    (tmp_path / "infra").mkdir()
    (tmp_path / "infra/compose.yaml").touch()
    oauth.save_schwab_token(tmp_path, TOKEN)
    token_file = tmp_path / ".secrets/nexus-schwab-local/tokens.json"
    before = token_file.read_bytes()
    monkeypatch.setattr(readiness, "_validate_local_env", lambda *args: None)
    monkeypatch.setattr(
        readiness,
        "load_local_environment",
        lambda *args: {
            "NEXUS_ENV": "test",
            "DATABASE_URL": "postgresql+asyncpg://fixture:fixture@127.0.0.1:55439/fixture",
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": "00000000-0000-0000-0000-000000000001",
            "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": "00000000-0000-0000-0000-000000000002",
        },
    )

    async def inspect(*args, **kwargs):
        return {
            "status": "ready_static",
            "window_evidence_status": "not_evaluated",
            "external_requests": 0,
        }

    monkeypatch.setattr(readiness, "_inspect", inspect)
    assert readiness.main(["--repo-root", str(tmp_path)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "ready_static"
    assert result["window_evidence_status"] == "not_evaluated"
    assert result["external_requests"] == 0
    assert result["authentication"] == {
        "local_access_token": "refresh_required",
        "reason_code": "SCHWAB_OAUTH_ACCESS_TOKEN_EXPIRED",
        "remote_authentication": "not_checked",
        "market_window_evidence": "not_evaluated",
    }
    assert token_file.read_bytes() == before
