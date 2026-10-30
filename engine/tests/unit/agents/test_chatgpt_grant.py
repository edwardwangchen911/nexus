"""Real signatures/files and HTTP boundary substitutes; never real credentials."""

import base64
import json
from datetime import UTC, datetime
from urllib.parse import parse_qs

import httpx
import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

NOW = 1800000000
SCOPE = "openid offline_access resource.invoke chatgpt.tokens.use.direct"


def b64(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


@pytest.fixture
def fixture(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = key.public_key().public_numbers()
    jwks = {
        "keys": [
            {
                "kty": "RSA",
                "alg": "RS256",
                "use": "sig",
                "kid": "fixture",
                "n": b64(public.n.to_bytes(256, "big")),
                "e": b64(b"\x01\x00\x01"),
            }
        ]
    }

    def token(kind="access", **overrides):
        claims = {
            "iss": "https://auth.openai.com",
            "sub": "fixture-subject",
            "aud": "https://api.openai.com/v1" if kind == "access" else "oaiapp_fixture",
            "iat": NOW - 7200,
            "exp": NOW - 3600,
        }
        if kind == "access":
            claims.update(client_id="oaiapp_fixture", scope=SCOPE, nbf=NOW - 7200)
        claims.update(overrides)
        body = b64(b'{"alg":"RS256","kid":"fixture"}') + "." + b64(json.dumps(claims).encode())
        return body + "." + b64(key.sign(body.encode(), padding.PKCS1v15(), hashes.SHA256()))

    data = {
        "issuer": "https://auth.openai.com",
        "subject": "fixture-subject",
        "client_id": "oaiapp_fixture",
        "ext_agent_host_id": "urn:uuid:fixture-host",
        "access_token": token(),
        "id_token": token("id"),
        "refresh_token": "old-secret",
        "token_type": "Bearer",
        "scopes": SCOPE.split(),
        "expires_in": 3600,
        "saved_at_unix": NOW - 7200,
        "earliest_refresh_at": NOW - 3700,
    }
    tmp_path.chmod(0o700)
    for name, value in [
        ("credentials.json", data),
        ("host.json", {"ext_agent_host_id": data["ext_agent_host_id"]}),
        ("registration-1.json", {k: data[k] for k in ("client_id", "ext_agent_host_id")}),
    ]:
        path = tmp_path / name
        path.write_text(json.dumps(value))
        path.chmod(0o600)
    return tmp_path, jwks, token, data


def store(fixture):
    from nexus.agents.chatgpt_grant import GrantStore

    return GrantStore(fixture[0])


def now():
    return datetime.fromtimestamp(NOW, UTC)


def test_existing_expired_identity_verified_without_refresh_or_secret_repr(fixture):
    p, jwks, _, data = fixture
    before = (p / "credentials.json").read_bytes()
    grant = store(fixture).read(jwks=jwks, now=now())
    assert grant.expired(now()) and grant.identity_verified
    assert data["access_token"] not in repr(grant) and "old-secret" not in repr(grant)
    assert (p / "credentials.json").read_bytes() == before
    with pytest.raises(ValueError, match="CHATGPT_ACCESS_EXPIRED"):
        grant.bearer(now())


@pytest.mark.parametrize(
    "mutation", ["subject", "client", "scope", "signature", "host", "permissions", "symlink"]
)
def test_invalid_binding_never_becomes_a_grant(fixture, mutation):
    p, jwks, token, data = fixture
    if mutation == "subject":
        data["access_token"] = token(sub="wrong")
    if mutation == "client":
        data["access_token"] = token(client_id="oaiapp_other")
    if mutation == "scope":
        data["access_token"] = token(scope="openid")
    if mutation == "signature":
        data["access_token"] = data["access_token"][:-8] + "AAAAAAAA"
    if mutation == "host":
        data["ext_agent_host_id"] = "different"
    file = p / "credentials.json"
    file.write_text(json.dumps(data))
    if mutation == "permissions":
        file.chmod(0o644)
    if mutation == "symlink":
        file.rename(p / "target")
        file.symlink_to(p / "target")
    with pytest.raises(ValueError, match="CHATGPT_"):
        store(fixture).read(jwks=jwks, now=now())


def handler(fixture, response=None, error=None):
    _, jwks, token, _ = fixture
    calls = []

    async def request(req):
        assert req.url.host == "auth.openai.com"
        assert "authorization" not in req.headers
        if req.method == "GET":
            assert str(req.url) == "https://auth.openai.com/.well-known/jwks.json"
            return httpx.Response(200, json=jwks)
        calls.append(req)
        assert str(req.url) == "https://auth.openai.com/api/accounts/oauth/token"
        assert parse_qs(req.content.decode()) == {
            "client_id": ["oaiapp_fixture"],
            "grant_type": ["refresh_token"],
            "refresh_token": ["old-secret"],
            "resource": ["https://api.openai.com/v1"],
        }
        if error:
            raise error
        if response is not None:
            return response
        return httpx.Response(
            200,
            json={
                "access_token": token(iat=NOW, exp=NOW + 3600, nbf=NOW),
                "refresh_token": "replacement-secret",
                "expires_in": 3600,
                "token_type": "Bearer",
                "scope": SCOPE,
                "earliest_refresh_at": NOW + 3500,
            },
        )

    return httpx.MockTransport(request), calls


@pytest.mark.asyncio
async def test_refresh_rotates_atomically_and_retains_original_registration_and_id(fixture):
    p, jwks, _, data = fixture
    transport, calls = handler(fixture)
    grant = await store(fixture).refresh_once(clock=now, transport=transport)
    assert not grant.expired(now()) and grant.bearer(now()).startswith("Bearer ")
    with pytest.raises(ValueError, match="CHATGPT_TIME_INVALID"):
        grant.bearer(datetime.fromtimestamp(NOW - 1, UTC))
    saved = json.loads((p / "credentials.json").read_text())
    assert saved["refresh_token"] == "replacement-secret"
    for name in ("id_token", "client_id", "ext_agent_host_id", "subject"):
        assert saved[name] == data[name]
    assert (p / "credentials.json").stat().st_mode & 0o777 == 0o600
    assert len(calls) == 1
    assert store(fixture).read(jwks=jwks, now=now()).identity_verified


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["timeout", "invalid_grant", "redirect", "malformed", "scope", "wrong_identity"]
)
async def test_failed_or_unknown_refresh_preserves_original_and_never_replays(fixture, failure):
    p, _, token, _ = fixture
    old = (p / "credentials.json").read_bytes()
    response = {
        "invalid_grant": httpx.Response(
            400, json={"error": "invalid_grant", "error_description": "SECRET"}
        ),
        "redirect": httpx.Response(307, headers={"location": "https://evil.invalid"}),
        "malformed": httpx.Response(200, content=b"SECRET"),
        "scope": httpx.Response(200, json={"scope": "openid"}),
        "wrong_identity": httpx.Response(
            200,
            json={
                "access_token": token(sub="other", iat=NOW, exp=NOW + 3600),
                "refresh_token": "replacement-secret",
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": SCOPE,
            },
        ),
    }.get(failure)
    transport, calls = handler(
        fixture,
        response=response,
        error=httpx.ReadTimeout("SECRET") if failure == "timeout" else None,
    )
    with pytest.raises(ValueError, match="CHATGPT_") as exc:
        await store(fixture).refresh_once(clock=now, transport=transport)
    assert "SECRET" not in str(exc.value) and "old-secret" not in repr(exc.value)
    assert (p / "credentials.json").read_bytes() == old
    with pytest.raises(ValueError, match="CHATGPT_REFRESH_ALREADY_ATTEMPTED"):
        await store(fixture).refresh_once(clock=now, transport=transport)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_refresh_uses_existing_cross_process_lock(fixture):
    import fcntl

    p, _, _, _ = fixture
    lock = p / "credentials.refresh-lock"
    lock.touch(mode=0o600)
    transport, calls = handler(fixture)
    with lock.open("r+") as file:
        fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="CHATGPT_REFRESH_BUSY"):
            await store(fixture).refresh_once(clock=now, transport=transport)
    assert calls == []


@pytest.mark.asyncio
async def test_cancellation_after_intent_keeps_old_file_and_blocks_reentry(fixture):
    import asyncio

    p, jwks, _, _ = fixture
    before = (p / "credentials.json").read_bytes()
    entered = asyncio.Event()

    async def request(req):
        if req.method == "GET":
            return httpx.Response(200, json=jwks)
        entered.set()
        await asyncio.Future()

    transport = httpx.MockTransport(request)
    task = asyncio.create_task(store(fixture).refresh_once(clock=now, transport=transport))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (p / "credentials.json").read_bytes() == before
    with pytest.raises(ValueError, match="CHATGPT_REFRESH_ALREADY_ATTEMPTED"):
        await store(fixture).refresh_once(clock=now, transport=transport)


@pytest.mark.asyncio
async def test_external_credential_change_during_refresh_is_not_overwritten(fixture):
    p, jwks, token, data = fixture
    changed = json.dumps({**data, "refresh_token": "different-new-secret"}).encode()

    async def request(req):
        if req.method == "GET":
            return httpx.Response(200, json=jwks)
        (p / "credentials.json").write_bytes(changed)
        return httpx.Response(
            200,
            json={
                "access_token": token(iat=NOW, exp=NOW + 3600, nbf=NOW),
                "refresh_token": "replacement-secret",
                "expires_in": 3600,
                "token_type": "Bearer",
                "scope": SCOPE,
            },
        )

    with pytest.raises(ValueError, match="CHATGPT_CREDENTIAL_CHANGED"):
        await store(fixture).refresh_once(clock=now, transport=httpx.MockTransport(request))
    assert (p / "credentials.json").read_bytes() == changed


@pytest.mark.asyncio
async def test_replace_failure_retains_old_bytes_and_consumes_refresh(fixture, monkeypatch):
    import os

    p, _, _, _ = fixture
    before = (p / "credentials.json").read_bytes()
    transport, calls = handler(fixture)

    def fail(*args, **kwargs):
        raise OSError("private diagnostic")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(ValueError, match="CHATGPT_STORAGE_WRITE_UNCONFIRMED"):
        await store(fixture).refresh_once(clock=now, transport=transport)
    assert (p / "credentials.json").read_bytes() == before
    assert not list(p.glob(".credentials-*"))
    with pytest.raises(ValueError, match="CHATGPT_REFRESH_ALREADY_ATTEMPTED"):
        await store(fixture).refresh_once(clock=now, transport=transport)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_transport_close_failure_does_not_leak_provider_exception(fixture):
    transport, _ = handler(fixture)

    class FailsClose(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            return await transport.handle_async_request(request)

        async def aclose(self):
            raise RuntimeError("PRIVATE-TOKEN")

    with pytest.raises(ValueError, match="CHATGPT_") as exc:
        await store(fixture).refresh_once(clock=now, transport=FailsClose())
    assert "PRIVATE-TOKEN" not in str(exc.value)


def test_normal_inspect_entry_reads_existing_grant_without_rotation(fixture, monkeypatch, capsys):
    import sys
    from types import SimpleNamespace

    from nexus.ops import research_comparison as cli

    original = httpx.AsyncClient
    transport, calls = handler(fixture)
    monkeypatch.setattr(
        httpx, "AsyncClient", lambda **kwargs: original(**{**kwargs, "transport": transport})
    )
    monkeypatch.setattr(cli, "datetime", SimpleNamespace(now=lambda zone: now()))
    monkeypatch.setattr(
        sys,
        "argv",
        ["nexus-engine-research-comparison", "inspect-grant", "--credentials-dir", str(fixture[0])],
    )
    before = (fixture[0] / "credentials.json").read_bytes()
    assert cli.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "status": "refresh_required",
        "signature_verified": True,
        "inspection_only": True,
        "dispatch_authority": False,
        "model_calls": 0,
        "credential_writes": 0,
    }
    assert calls == [] and (fixture[0] / "credentials.json").read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_error", ["internal prose PRIVATE", "invalid_grant"])
async def test_public_key_rejection_is_diagnostic_not_grant_revocation(fixture, provider_error):
    async def request(req):
        assert req.method == "GET"
        return httpx.Response(403, json={"error": provider_error})

    before = (fixture[0] / "credentials.json").read_bytes()
    with pytest.raises(ValueError, match="CHATGPT_HTTP_REJECTED") as exc:
        await store(fixture).inspect(clock=now, transport=httpx.MockTransport(request))
    assert exc.value.http_status == 403 and exc.value.phase == "jwks"
    assert "PRIVATE" not in str(exc.value)
    assert (fixture[0] / "credentials.json").read_bytes() == before
    assert not list(fixture[0].glob(".refresh-attempt-*"))


@pytest.mark.asyncio
async def test_refresh_rejection_persists_only_safe_phase_and_category(fixture):
    transport, _ = handler(
        fixture,
        response=httpx.Response(
            400, json={"error": "invalid_grant", "error_description": "PRIVATE-PROVIDER-DETAIL"}
        ),
    )
    with pytest.raises(ValueError, match="CHATGPT_REFRESH_INVALID_GRANT"):
        await store(fixture).refresh_once(clock=now, transport=transport)
    receipts = list(fixture[0].glob(".refresh-attempt-*.outcome.json"))
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_bytes())
    assert receipt == {
        "reason": "CHATGPT_REFRESH_INVALID_GRANT",
        "phase": "token",
        "http_status": 400,
        "credential_commit_confirmed": False,
    }
    assert "PRIVATE" not in receipts[0].read_text() and "old-secret" not in receipts[0].read_text()


@pytest.mark.asyncio
async def test_absolute_deadline_during_refresh_retains_unknown_and_old_credentials(
    fixture, monkeypatch
):
    import asyncio

    real_timeout = asyncio.timeout
    contexts = []
    entered = asyncio.Event()

    def controlled_timeout(delay):
        context = real_timeout(delay)
        contexts.append((delay, context))
        return context

    monkeypatch.setattr(asyncio, "timeout", controlled_timeout)
    p, jwks, _, _ = fixture
    before = (p / "credentials.json").read_bytes()

    async def request(req):
        if req.method == "GET":
            return httpx.Response(200, json=jwks)
        entered.set()
        await asyncio.Future()

    task = asyncio.create_task(
        store(fixture).refresh_once(clock=now, transport=httpx.MockTransport(request))
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert len(contexts) == 1 and contexts[0][0] == 10
        contexts[0][1].reschedule(asyncio.get_running_loop().time())
        with pytest.raises(ValueError, match="CHATGPT_REFRESH_RESULT_UNKNOWN") as exc:
            await asyncio.wait_for(task, timeout=5)
        assert exc.value.phase == "token"
        assert (p / "credentials.json").read_bytes() == before
        receipt = json.loads(next(p.glob(".refresh-attempt-*.outcome.json")).read_bytes())
        assert receipt["reason"] == "CHATGPT_REFRESH_RESULT_UNKNOWN"
        assert not receipt["credential_commit_confirmed"]
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def test_normal_explicit_refresh_entry_rotates_without_dispatch(fixture, monkeypatch, capsys):
    import sys
    from types import SimpleNamespace

    from nexus.ops import research_comparison as cli

    original = httpx.AsyncClient
    transport, calls = handler(fixture)
    monkeypatch.setattr(
        httpx, "AsyncClient", lambda **kwargs: original(**{**kwargs, "transport": transport})
    )
    monkeypatch.setattr(cli, "datetime", SimpleNamespace(now=lambda zone: now()))
    monkeypatch.setattr(
        sys,
        "argv",
        ["nexus-engine-research-comparison", "refresh-grant", "--credentials-dir", str(fixture[0])],
    )
    assert cli.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "status": "access_ready",
        "credential_writes": 1,
        "dispatch_authority": False,
        "model_calls": 0,
    }
    assert (
        json.loads((fixture[0] / "credentials.json").read_text())["refresh_token"]
        == "replacement-secret"
    )
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_cleanup_failure_preserves_original_safe_refresh_rejection(fixture):
    transport, _ = handler(fixture, response=httpx.Response(400, json={"error": "invalid_grant"}))

    class BrokenClose(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            return await transport.handle_async_request(request)

        async def aclose(self):
            raise RuntimeError("PRIVATE-CLEANUP-DETAIL")

    before = (fixture[0] / "credentials.json").read_bytes()
    with pytest.raises(ValueError, match="CHATGPT_REFRESH_INVALID_GRANT") as exc:
        await store(fixture).refresh_once(clock=now, transport=BrokenClose())
    assert exc.value.http_status == 400 and exc.value.phase == "token"
    assert (fixture[0] / "credentials.json").read_bytes() == before
    receipt = json.loads(next(fixture[0].glob(".refresh-attempt-*.outcome.json")).read_bytes())
    assert receipt["reason"] == "CHATGPT_REFRESH_INVALID_GRANT"
    assert "PRIVATE" not in json.dumps(receipt)
