import base64
import hashlib
import inspect
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from nexus_fosun_gateway.activation import (
    REQUIRED_PROBE_OPERATIONS,
    ProbePublicationReceipt,
    SanitizedProbeArtifact,
    evaluate_activation,
)
from nexus_fosun_gateway.adapter.probe_sdk import (
    ProbeExecutionEvidence,
    ProbeSdkError,
)
from nexus_fosun_gateway.probe import (
    ProbePlan,
    _build_prepared_probe,
    response_contract_sha256,
    run_sanitized_probe,
    validate_sanitized_probe_artifact,
)
from nexus_fosun_gateway.publication import publish_probe_artifact
from pydantic import ValidationError
from test_activation import _admitted_manifest, _json_bytes

STARTED_AT = datetime(2026, 8, 31, 8, 0, tzinfo=UTC)
CAPTURED_AT = STARTED_AT + timedelta(seconds=8)
TEST_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
TRUSTED_PUBLICATION_KEY_BYTES = base64.urlsafe_b64encode(
    TEST_KEY.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
).rstrip(b"=")


def test_probe_plan_accepts_only_the_closed_non_secret_shape() -> None:
    plan = _plan()

    plan.validate_for_start(STARTED_AT)
    request = plan.to_request_plan()

    assert request.symbol == "usAAPL"
    assert request.limit == 1
    assert plan.output_basename == "probe-20260831.json"
    assert "key" not in repr(plan).lower()
    assert "account" not in repr(plan).lower()


def test_probe_runner_requires_an_explicit_publisher_signing_key_factory() -> None:
    parameter = inspect.signature(run_sanitized_probe).parameters[
        "private_key_factory"
    ]

    assert parameter.default is inspect.Parameter.empty


@pytest.mark.parametrize(
    "changes",
    [
        {"symbol": "AAPL"},
        {"symbol": "usAaPL"},
        {"symbol": "usAAP/L"},
        {"limit": 0},
        {"limit": 11},
        {"capability_ttl_seconds": 59},
        {"capability_ttl_seconds": 86401},
        {"output_basename": "../probe.json"},
        {"output_basename": ".probe.json"},
        {"output_basename": "Probe.json"},
        {"output_basename": "probe.txt"},
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
    ],
)
def test_probe_plan_rejects_invalid_values(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ProbePlan.model_validate({**_plan().model_dump(), **changes})


@pytest.mark.parametrize("unknown", ["api_key", "account_id", "url", "method", "output_dir"])
def test_probe_plan_rejects_secret_or_generic_operator_inputs(unknown: str) -> None:
    values = _plan().model_dump()
    values[unknown] = "synthetic-secret-canary"

    with pytest.raises(ValidationError) as exc_info:
        ProbePlan.model_validate(values)

    assert "synthetic-secret-canary" not in str(exc_info.value)


def test_probe_plan_requires_a_completed_provider_session_identity() -> None:
    plan = _plan().model_copy(update={"session_date": STARTED_AT.date()})

    with pytest.raises(ProbeSdkError) as exc_info:
        plan.validate_for_start(STARTED_AT)

    assert exc_info.value.code == "INVALID_PROBE_PLAN"


def test_response_contract_digests_are_static_and_versioned() -> None:
    expected = {
        "Session": "6223658f8e007479f00fe3c20a46588664ec01c94d6c2a737c5390a42dac0171",
        "Accounts": "1b3660a7eff1d7ec43b0fcbfbdf65ae8dd30eae75b57d47e2c4598b67988a118",
        "CashSummary": "fad8e8db49292aa8e785cb24aaa8adb8fff2424266a2f8f8885db74aae907243",
        "Holdings": "69c108c2a59445a2e25e84f1cdfd96fb8e1f56e90dc926c42c4597fe755054eb",
        "OrderList": "525ff22a5d85e28bb6dacbb1b3403bc05bb914bc2211412e304694742a99e696",
        "CashFlows": "1c3ee0a334a16a32d58f3b0bd28abebf43c5501bb35607333d9f0de6f4b8a427",
        "quote": "c38d50d4bf4eb899c6d69a5bc916f7aeefe40bd66a04f3c2520345b37dc3b579",
        "kline_minimal": "332970ce3cb34937ca39e021d693a923402bfba1ed536cde3de64564bf8edf37",
    }

    assert {
        label: response_contract_sha256(label) for label in REQUIRED_PROBE_OPERATIONS
    } == expected

    generated_schema = SanitizedProbeArtifact.model_json_schema()
    generated_schema["synthetic"] = "metadata-canary"
    assert {
        label: response_contract_sha256(label) for label in REQUIRED_PROBE_OPERATIONS
    } == expected


def test_prepared_artifact_contains_only_static_contract_and_value_free_evidence() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    prepared = _build_prepared_probe(
        manifest_bytes,
        _plan(),
        _evidence(),
        started_at=STARTED_AT,
        captured_at=CAPTURED_AT,
        private_key_factory=lambda: TEST_KEY,
    )

    artifact = SanitizedProbeArtifact.model_validate_json(prepared.artifact_bytes)
    assert artifact.captured_at == CAPTURED_AT
    assert artifact.capability_expires_at == STARTED_AT + timedelta(hours=1)
    assert tuple(item.name for item in artifact.operations) == REQUIRED_PROBE_OPERATIONS
    assert tuple(item.request_count for item in artifact.operations) == (1,) * 8
    assert all(item.passed for item in artifact.operations)
    assert artifact.checks.all_passed() is True
    assert prepared.artifact_bytes == json.dumps(
        artifact.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert prepared.byte_length == len(prepared.artifact_bytes)
    assert prepared.artifact_sha256 not in prepared.artifact_bytes.decode()
    for canary in (
        "synthetic-account-canary",
        "synthetic-request-canary",
        "synthetic-price-canary",
        "usAAPL",
        "probe-20260831.json",
    ):
        assert canary not in prepared.artifact_bytes.decode()
        assert canary not in repr(prepared)


def test_static_contract_digest_does_not_depend_on_live_counts_or_values() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    first = _build_prepared_probe(
        manifest_bytes,
        _plan(),
        _evidence(),
        started_at=STARTED_AT,
        captured_at=CAPTURED_AT,
        private_key_factory=lambda: TEST_KEY,
    )
    retried = _build_prepared_probe(
        manifest_bytes,
        _plan(),
        replace(
            _evidence(),
            request_counts=tuple(
                (name, 2 if name == "quote" else count)
                for name, count in _evidence().request_counts
            ),
        ),
        started_at=STARTED_AT,
        captured_at=CAPTURED_AT,
        private_key_factory=lambda: TEST_KEY,
    )

    first_artifact = SanitizedProbeArtifact.model_validate_json(first.artifact_bytes)
    retried_artifact = SanitizedProbeArtifact.model_validate_json(retried.artifact_bytes)
    assert tuple(
        item.response_contract_sha256 for item in first_artifact.operations
    ) == tuple(item.response_contract_sha256 for item in retried_artifact.operations)
    assert first_artifact.operations[6].request_count == 1
    assert retried_artifact.operations[6].request_count == 2


def test_contract_only_validator_rejects_modified_digest_and_sensitive_extra() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    prepared = _prepared(manifest_bytes)
    document = json.loads(prepared.artifact_bytes)
    document["operations"][0]["response_contract_sha256"] = "0" * 64

    with pytest.raises(ProbeSdkError) as digest_error:
        validate_sanitized_probe_artifact(manifest_bytes, _json_bytes(document))
    assert digest_error.value.code == "PROBE_RESPONSE_INVALID"

    document = json.loads(prepared.artifact_bytes)
    document["account_id"] = "synthetic-account-canary"
    with pytest.raises(ProbeSdkError) as extra_error:
        validate_sanitized_probe_artifact(manifest_bytes, _json_bytes(document))
    assert extra_error.value.code == "PROBE_RESPONSE_INVALID"


def test_contract_only_validator_rejects_broker_domain_request_overage() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    prepared = _prepared(manifest_bytes)
    document = json.loads(prepared.artifact_bytes)
    document["operations"][0]["request_count"] = 2

    with pytest.raises(ProbeSdkError) as exc_info:
        validate_sanitized_probe_artifact(manifest_bytes, _json_bytes(document))

    assert exc_info.value.code == "PROBE_RESPONSE_INVALID"


def test_receipt_signs_only_the_exact_durable_artifact_and_activates_it(
    tmp_path,
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    prepared = _prepared(manifest_bytes)
    publication = _publish(prepared, tmp_path, "exact")

    receipt_bytes = prepared.sign_publication_receipt(publication)
    receipt = ProbePublicationReceipt.model_validate_json(receipt_bytes)
    result = evaluate_activation(
        manifest_bytes,
        prepared.artifact_bytes,
        receipt_bytes,
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CAPTURED_AT,
    )

    assert receipt.artifact_sha256 == prepared.artifact_sha256
    assert result.status == "activated_read_only"
    assert receipt_bytes == json.dumps(
        receipt.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert base64.urlsafe_b64decode(receipt.signature_b64u + "==")
    assert "private" not in repr(prepared).lower()

    TEST_KEY.public_key().verify(
        base64.urlsafe_b64decode(receipt.signature_b64u + "=="),
        b"NEXUS-FOSUN-PROBE-PUBLICATION-V1\0"
        + bytes.fromhex(prepared.artifact_sha256),
    )
    assert prepared._private_key is None


def test_receipt_refuses_wrong_publication_and_is_one_shot(tmp_path) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    prepared = _prepared(manifest_bytes)
    other_bytes = b'{"schema_version":"other"}'
    output_dir = tmp_path / "other"
    output_dir.mkdir(mode=0o700)
    wrong_publication = publish_probe_artifact(
        output_dir,
        "other.json",
        other_bytes,
        expected_sha256=hashlib.sha256(other_bytes).hexdigest(),
        expected_byte_length=len(other_bytes),
    )

    with pytest.raises(ProbeSdkError) as digest_error:
        prepared.sign_publication_receipt(wrong_publication)
    assert digest_error.value.code == "PROBE_OUTPUT_INVALID"

    publication = _publish(prepared, tmp_path, "correct")
    with pytest.raises(ProbeSdkError) as repeat_error:
        prepared.sign_publication_receipt(publication)
    assert repeat_error.value.code == "PROBE_OUTPUT_INVALID"
    assert prepared._private_key is None


def test_prepared_artifact_can_discard_unused_signing_key(tmp_path) -> None:
    prepared = _prepared(_json_bytes(_admitted_manifest()))
    publication = _publish(prepared, tmp_path, "unused")

    prepared.dispose_signing_key()
    prepared.dispose_signing_key()

    with pytest.raises(ProbeSdkError) as exc_info:
        prepared.sign_publication_receipt(publication)
    assert exc_info.value.code == "PROBE_OUTPUT_INVALID"
    assert prepared._private_key is None


def test_direct_builder_and_self_signed_receipt_cannot_bypass_trusted_publisher(
    tmp_path,
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    attacker_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    prepared = _build_prepared_probe(
        manifest_bytes,
        _plan(),
        _evidence(),
        started_at=STARTED_AT,
        captured_at=CAPTURED_AT,
        private_key_factory=lambda: attacker_key,
    )
    publication = _publish(prepared, tmp_path, "attacker")

    result = evaluate_activation(
        manifest_bytes,
        prepared.artifact_bytes,
        prepared.sign_publication_receipt(publication),
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CAPTURED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_PUBLISHER_BINDING"


def test_probe_finishing_at_capability_expiry_creates_no_artifact() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())

    with pytest.raises(ProbeSdkError) as exc_info:
        _build_prepared_probe(
            manifest_bytes,
            _plan(),
            _evidence(),
            started_at=STARTED_AT,
            captured_at=STARTED_AT + timedelta(hours=1),
            private_key_factory=lambda: TEST_KEY,
        )

    assert exc_info.value.code == "PROBE_CAPABILITY_EXPIRED"


@pytest.mark.parametrize(
    "mutation",
    [
        "auth",
        "auth_bool",
        "history",
        "history_int",
        "labels",
        "count",
        "broker_count",
    ],
)
def test_invalid_execution_evidence_never_creates_an_artifact(
    mutation: str,
) -> None:
    evidence = _evidence()
    if mutation == "auth":
        evidence = replace(evidence, auth_successes=0)
    elif mutation == "auth_bool":
        evidence = replace(evidence, auth_successes=True)
    elif mutation == "history":
        evidence = replace(evidence, history_window=False)
    elif mutation == "history_int":
        evidence = replace(evidence, history_window=1)
    elif mutation == "labels":
        evidence = replace(evidence, request_counts=(("Session", 1),))
    elif mutation == "count":
        evidence = replace(
            evidence,
            request_counts=tuple(
                (name, 4 if name == "quote" else count)
                for name, count in evidence.request_counts
            ),
        )
    else:
        evidence = replace(
            evidence,
            request_counts=tuple(
                (name, 2 if name == "Session" else count)
                for name, count in evidence.request_counts
            ),
        )

    with pytest.raises(ProbeSdkError) as exc_info:
        _build_prepared_probe(
            _json_bytes(_admitted_manifest()),
            _plan(),
            evidence,
            started_at=STARTED_AT,
            captured_at=CAPTURED_AT,
            private_key_factory=lambda: TEST_KEY,
        )

    assert exc_info.value.code == "PROBE_RESPONSE_INVALID"


def test_blocked_manifest_is_rejected_before_key_generation() -> None:
    generated = False

    def forbidden_key_factory() -> Ed25519PrivateKey:
        nonlocal generated
        generated = True
        raise AssertionError

    with pytest.raises(ProbeSdkError) as exc_info:
        _build_prepared_probe(
            b"{}",
            _plan(),
            _evidence(),
            started_at=STARTED_AT,
            captured_at=CAPTURED_AT,
            private_key_factory=forbidden_key_factory,
        )

    assert exc_info.value.code == "COMPLETE_SDK_ADMISSION"
    assert generated is False


def test_signing_key_configuration_error_remains_actionable() -> None:
    def invalid_key_configuration() -> Ed25519PrivateKey:
        raise ProbeSdkError("PROBE_SETTINGS_INVALID")

    with pytest.raises(ProbeSdkError) as exc_info:
        _build_prepared_probe(
            _json_bytes(_admitted_manifest()),
            _plan(),
            _evidence(),
            started_at=STARTED_AT,
            captured_at=CAPTURED_AT,
            private_key_factory=invalid_key_configuration,
        )

    assert exc_info.value.code == "PROBE_SETTINGS_INVALID"


def _plan() -> ProbePlan:
    return ProbePlan(
        symbol="usAAPL",
        session_date=date(2026, 8, 28),
        cash_flow_date_from=date(2026, 8, 24),
        cash_flow_date_to=date(2026, 8, 28),
        limit=1,
        capability_ttl_seconds=3600,
        output_basename="probe-20260831.json",
    )


def _evidence() -> ProbeExecutionEvidence:
    return ProbeExecutionEvidence(
        request_counts=tuple((name, 1) for name in REQUIRED_PROBE_OPERATIONS),
        auth_successes=1,
        history_window=True,
        pagination=True,
        adjustment_semantics=True,
        response_contract=True,
    )


def _prepared(manifest_bytes: bytes):
    return _build_prepared_probe(
        manifest_bytes,
        _plan(),
        _evidence(),
        started_at=STARTED_AT,
        captured_at=CAPTURED_AT,
        private_key_factory=lambda: TEST_KEY,
    )


def _publish(prepared, tmp_path, label: str):
    output_dir = tmp_path / label
    output_dir.mkdir(mode=0o700)
    return publish_probe_artifact(
        output_dir,
        f"{label}.json",
        prepared.artifact_bytes,
        expected_sha256=prepared.artifact_sha256,
        expected_byte_length=prepared.byte_length,
    )
