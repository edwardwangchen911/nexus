"""Explicit one-experiment authorization is not a fabricated gift balance."""

import hashlib
import json
import sys
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from unit.ops.test_platform_credentials import ENDPOINT, KEY, NOW, PURPOSE, private_file

pytest_plugins = ["unit.ops.test_platform_credentials"]

EXPERIMENT = UUID("11111111-2222-4333-8444-555555555555")
INPUTS = ("1" * 64, "2" * 64, "3" * 64)


@pytest.fixture
def authorization(pc, tmp_path):
    private_file(tmp_path / pc.PLATFORM_KEY_PATH, (KEY + "\n").encode())
    original = b"Synthetic user authorization: one experiment, at most USD 6.42."
    artifact = private_file(tmp_path / ".secrets/authorization/user.txt", original)
    payload = {
        "version": 1,
        "kind": "explicit_platform_spend_authorization",
        "purpose": PURPOSE,
        "endpoint": ENDPOINT,
        "credential_fingerprint": pc.credential_fingerprint(KEY),
        "experiment_id": str(EXPERIMENT),
        "input_hashes": INPUTS,
        "max_total_usd": "6.42",
        "authorized_at": "2026-10-31T07:59:00Z",
        "window_start": "2026-10-31T07:55:00Z",
        "window_end": "2026-11-01T00:00:00Z",
        "authorization_file": artifact.name,
        "authorization_sha256": hashlib.sha256(original).hexdigest(),
    }
    path = private_file(artifact.with_name("authorization.json"), json.dumps(payload).encode())
    return tmp_path, path, payload


def check_authorization(pc, authorization, **overrides):
    root, path, _ = authorization
    proof = pc.load_platform_funding_proof(path)
    credential = pc.resolve_platform_credential(
        root,
        expected_fingerprint=pc.credential_fingerprint(KEY),
        organization_id=None,
        project_id=None,
        key_scope_only=True,
    )
    args = dict(
        purpose=PURPOSE,
        now=NOW,
        required_usd=Decimal("6.414912"),
        local_held_usd=Decimal("2"),
        local_spent_since_proof_usd=Decimal("1"),
        required_valid_until=NOW + timedelta(minutes=20),
        experiment_id=EXPERIMENT,
        input_hashes=INPUTS,
    )
    args.update(overrides)
    return pc.check_platform_eligibility(credential, proof, **args), credential


def test_explicit_authorization_does_not_require_or_invent_gift_balance(pc, authorization):
    result, credential = check_authorization(pc, authorization)
    assert result.available_gift_usd is None
    assert result.funding_proof_hash == hashlib.sha256(authorization[1].read_bytes()).hexdigest()
    assert credential.request_headers() == {"Authorization": "Bearer " + KEY}
    assert KEY not in repr(result) + repr(credential)


@pytest.mark.parametrize(
    "override,code",
    [
        ({"experiment_id": UUID(int=1)}, "PLATFORM_PROOF_BINDING_MISMATCH"),
        ({"input_hashes": ("0" * 64,) * 3}, "PLATFORM_PROOF_BINDING_MISMATCH"),
        ({"required_usd": Decimal("6.420001")}, "PLATFORM_SPEND_LIMIT_EXCEEDED"),
        ({"now": NOW + timedelta(days=1)}, "PLATFORM_WINDOW_INVALID"),
        ({"now": NOW + timedelta(hours=16)}, "PLATFORM_WINDOW_INVALID"),
        ({"local_held_usd": Decimal("NaN")}, "PLATFORM_OCCUPANCY_INVALID"),
    ],
)
def test_authorization_identity_limit_and_time_are_mandatory(pc, authorization, override, code):
    with pytest.raises(pc.PlatformCredentialError, match=code):
        check_authorization(pc, authorization, **override)


@pytest.mark.parametrize("cap", ["6.43", "0", "NaN", 6.42])
def test_authorization_cannot_expand_fixed_limit(pc, authorization, cap):
    _, path, payload = authorization
    payload["max_total_usd"] = cap
    private_file(path, json.dumps(payload).encode())
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_PROOF_INVALID"):
        check_authorization(pc, authorization)


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("kind", "unknown_authority", "PLATFORM_PROOF_INVALID"),
        ("kind", "manual_platform_console", "PLATFORM_PROOF_INVALID"),
        ("gift_balance_usd", "100", "PLATFORM_PROOF_INVALID"),
        ("endpoint", "https://wrong.invalid/v1/responses", "PLATFORM_PROOF_BINDING_MISMATCH"),
        ("credential_fingerprint", "0" * 64, "PLATFORM_PROOF_BINDING_MISMATCH"),
        ("authorization_sha256", "0" * 64, "PLATFORM_PROOF_INVALID"),
        ("input_hashes", INPUTS[::-1], "PLATFORM_PROOF_BINDING_MISMATCH"),
    ],
)
def test_authorization_kind_scope_and_order_never_fall_back(pc, authorization, field, value, code):
    _, path, payload = authorization
    payload[field] = value
    private_file(path, json.dumps(payload).encode())
    with pytest.raises(pc.PlatformCredentialError, match=code):
        check_authorization(pc, authorization)


def test_old_credential_mode_still_requires_explicit_scope(pc, authorization):
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_CREDENTIAL_BINDING_MISMATCH"):
        pc.resolve_platform_credential(
            authorization[0],
            expected_fingerprint=pc.credential_fingerprint(KEY),
            organization_id=None,
            project_id=None,
        )


def test_declared_window_cannot_exceed_fixed_cutoff_before_expiry(pc, authorization):
    _, path, payload = authorization
    payload["window_end"] = "2026-11-01T01:00:00Z"
    private_file(path, json.dumps(payload).encode())
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_PROOF_INVALID"):
        check_authorization(pc, authorization)


def test_renewed_authorization_has_separate_version_and_exclusive_cutoff(pc, authorization):
    from datetime import UTC, datetime

    _, path, payload = authorization
    payload.update(
        version=2, window_end="2026-11-02T00:00:00Z", purpose=pc.PLATFORM_RENEWED_PURPOSE
    )
    private_file(path, json.dumps(payload).encode())
    cutoff = datetime(2026, 11, 2, tzinfo=UTC)
    result, _ = check_authorization(
        pc,
        authorization,
        purpose=pc.PLATFORM_RENEWED_PURPOSE,
        now=cutoff - timedelta(minutes=20),
        required_valid_until=cutoff,
    )
    assert result.valid_until == cutoff
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_WINDOW_INVALID"):
        check_authorization(
            pc,
            authorization,
            purpose=pc.PLATFORM_RENEWED_PURPOSE,
            now=cutoff,
            required_valid_until=cutoff,
        )
    payload["window_end"] = "2026-11-02T00:00:01Z"
    private_file(path, json.dumps(payload).encode())
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_PROOF_INVALID"):
        check_authorization(pc, authorization)


def test_exact_fixed_cutoff_and_right_exclusive_check(pc, authorization):
    cutoff = NOW + timedelta(hours=16)
    result, _ = check_authorization(
        pc, authorization, now=cutoff - timedelta(minutes=20), required_valid_until=cutoff
    )
    assert result.valid_until == cutoff
    assert pc.load_platform_funding_proof(authorization[1]).valid_until == cutoff
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_WINDOW_INVALID"):
        check_authorization(
            pc, authorization, now=cutoff, required_valid_until=cutoff + timedelta(minutes=20)
        )


def test_source_replacement_and_ambient_scope_cannot_bypass_authorization(
    pc, authorization, monkeypatch
):
    _, path, _ = authorization
    check_authorization(pc, authorization)
    monkeypatch.setenv("OPENAI_PROJECT_ID", "proj-other")
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_CREDENTIAL_BINDING_MISMATCH"):
        check_authorization(pc, authorization)
    monkeypatch.delenv("OPENAI_PROJECT_ID")
    private_file(path.with_name("user.txt"), b"Changed authorization")
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_PROOF_INVALID"):
        check_authorization(pc, authorization)


def test_normal_read_only_check_names_authorization_without_claiming_balance(
    pc, authorization, monkeypatch, capsys
):
    monkeypatch.setattr(pc, "_now", lambda: NOW)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "nexus-platform-local",
            "check",
            "--proof-file",
            str(authorization[1]),
            "--required-usd",
            "6.414912",
            "--local-held-usd",
            "2",
            "--local-spent-since-proof-usd",
            "1",
            "--valid-until",
            (NOW + timedelta(minutes=20)).isoformat(),
            "--experiment-id",
            str(EXPERIMENT),
            *[part for value in INPUTS for part in ("--input-hash", value)],
        ],
    )
    assert pc.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["funding_kind"] == "explicit_platform_spend_authorization"
    assert result["balance_verified"] is False
    assert result["dispatch_authority"] is False
    assert result["model_requests"] == result["provider_requests"] == result["database_writes"] == 0
    assert KEY not in json.dumps(result)


def test_third_authorization_never_consumes_second_round_or_extends_deadline(pc, authorization):
    from datetime import UTC, datetime

    _, path, payload = authorization
    purpose = "platform-terra-v3-20261101"
    payload.update(version=3, purpose=purpose, window_end="2026-11-02T00:00:00Z")
    private_file(path, json.dumps(payload).encode())
    cutoff = datetime(2026, 11, 2, tzinfo=UTC)
    args = dict(purpose=purpose, now=cutoff - timedelta(minutes=20), required_valid_until=cutoff)
    result, _ = check_authorization(pc, authorization, **args)
    assert result.valid_until == cutoff
    for version in (1, 2):
        payload["version"] = version
        private_file(path, json.dumps(payload).encode())
        with pytest.raises(
            pc.PlatformCredentialError, match="PLATFORM_PROOF_(BINDING_MISMATCH|INVALID)"
        ):
            check_authorization(pc, authorization, **args)
    payload.update(version=3, window_end="2026-11-02T00:00:01Z")
    private_file(path, json.dumps(payload).encode())
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_PROOF_INVALID"):
        check_authorization(pc, authorization, **args)


def test_third_round_cannot_substitute_an_unbound_console_attestation(pc, admission):
    from unit.ops.test_platform_credentials import check

    _, path, payload = admission
    purpose = "platform-terra-v3-20261101"
    payload["purpose"] = purpose
    private_file(path, json.dumps(payload).encode())
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_PROOF_BINDING_MISMATCH"):
        check(pc, admission, purpose=purpose)
