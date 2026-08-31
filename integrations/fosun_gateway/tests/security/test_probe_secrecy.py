import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from nexus_fosun_gateway.activation import REQUIRED_PROBE_OPERATIONS
from nexus_fosun_gateway.adapter.probe_sdk import ProbeExecutionEvidence, ProbeSdkError
from nexus_fosun_gateway.probe import ProbePlan, _build_prepared_probe

_CANARIES = (
    "synthetic-account-secret-canary",
    "synthetic-request-secret-canary",
    "synthetic-provider-body-canary",
    "synthetic-private-key-canary",
    "https://provider.invalid/private?secret=canary",
)
_MANIFEST_PATH = (
    Path(__file__).parents[4]
    / "contracts"
    / "providers"
    / "fosun"
    / "manifest.json"
)


def test_artifact_error_logs_and_repr_never_contain_live_or_secret_canaries(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = _admitted_manifest()
    manifest["admission"]["evidence"]["provider_test_vector"] = _CANARIES[-1]
    manifest_bytes = _json_bytes(manifest)
    key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    plan = ProbePlan(
        symbol="usSECRETCANARY",
        session_date=date(2026, 8, 28),
        cash_flow_date_from=date(2026, 8, 24),
        cash_flow_date_to=date(2026, 8, 28),
        limit=1,
        capability_ttl_seconds=3600,
        output_basename="synthetic-private-key-canary.json",
    )
    evidence = ProbeExecutionEvidence(
        request_counts=tuple((name, 1) for name in REQUIRED_PROBE_OPERATIONS),
        auth_successes=1,
        history_window=True,
        pagination=False,
        adjustment_semantics=False,
        response_contract=True,
    )

    with caplog.at_level(logging.DEBUG):
        prepared = _build_prepared_probe(
            manifest_bytes,
            plan,
            evidence,
            started_at=datetime(2026, 8, 31, 8, 0, tzinfo=UTC),
            captured_at=datetime(2026, 8, 31, 8, 0, tzinfo=UTC)
            + timedelta(seconds=1),
            private_key_factory=lambda: key,
        )

    captured = capsys.readouterr()
    rendered = "\n".join(
        (
            prepared.artifact_bytes.decode(),
            repr(prepared),
            caplog.text,
            captured.out,
            captured.err,
        )
    )
    for canary in _CANARIES:
        assert canary not in rendered
    assert "usSECRETCANARY" not in rendered


def test_exception_from_key_generation_is_sanitized(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def failing_key_factory() -> Ed25519PrivateKey:
        raise RuntimeError("synthetic-private-key-canary")

    with caplog.at_level(logging.DEBUG), pytest.raises(ProbeSdkError) as exc_info:
        _build_prepared_probe(
            _json_bytes(_admitted_manifest()),
            ProbePlan(
                symbol="usAAPL",
                session_date=date(2026, 8, 28),
                cash_flow_date_from=date(2026, 8, 24),
                cash_flow_date_to=date(2026, 8, 28),
                limit=1,
                capability_ttl_seconds=3600,
                output_basename="probe.json",
            ),
            ProbeExecutionEvidence(
                request_counts=tuple(
                    (name, 1) for name in REQUIRED_PROBE_OPERATIONS
                ),
                auth_successes=1,
                history_window=True,
                pagination=False,
                adjustment_semantics=False,
                response_contract=True,
            ),
            started_at=datetime(2026, 8, 31, 8, 0, tzinfo=UTC),
            captured_at=datetime(2026, 8, 31, 8, 0, 1, tzinfo=UTC),
            private_key_factory=failing_key_factory,
        )

    captured = capsys.readouterr()
    rendered = "\n".join(
        (repr(exc_info.value), caplog.text, captured.out, captured.err)
    )
    assert exc_info.value.code == "PROBE_FAILED"
    assert "synthetic-private-key-canary" not in rendered
    assert json.dumps({"code": exc_info.value.code}) == '{"code": "PROBE_FAILED"}'


def _admitted_manifest() -> dict[str, object]:
    manifest = json.loads(_MANIFEST_PATH.read_bytes())
    manifest["sdk"]["license"] = "PROVIDER-AUTHORIZED-PRIVATE-USE"
    manifest["admission"]["status"] = "admitted"
    manifest["admission"]["provider_base_url"] = "https://openapi.fosunxcz.com"
    manifest["admission"]["evidence"] = {
        "written_permission": "synthetic/permission",
        "provider_test_vector": "synthetic/test-vector",
        "security_review": "synthetic/security-review",
        "tls_verification_remediation": "synthetic/tls-review",
    }
    return manifest


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
