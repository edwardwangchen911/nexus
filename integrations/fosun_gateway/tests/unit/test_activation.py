import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from nexus_fosun_gateway.activation import (
    REQUIRED_PROBE_OPERATIONS,
    FosunActivationResult,
    evaluate_activation,
    evaluate_activation_paths,
    main,
)

MANIFEST_PATH = Path(__file__).parents[4] / "contracts" / "providers" / "fosun" / "manifest.json"
CHECKED_AT = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)
PUBLICATION_DOMAIN = b"NEXUS-FOSUN-PROBE-PUBLICATION-V1\0"
TEST_PUBLICATION_KEY = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
TEST_PUBLICATION_PUBLIC_KEY_B64U = base64.urlsafe_b64encode(
    TEST_PUBLICATION_KEY.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
).rstrip(b"=").decode()


def test_committed_manifest_can_only_report_fixture_with_next_action() -> None:
    result = evaluate_activation(MANIFEST_PATH.read_bytes(), checked_at=CHECKED_AT)

    assert result == FosunActivationResult(
        status="fixture_with_next_action",
        contract_hash=result.contract_hash,
        checked_at=CHECKED_AT,
        capability_expires_at=None,
        next_action="COMPLETE_SDK_ADMISSION",
    )
    assert len(result.contract_hash) == 64


def test_complete_probe_cannot_override_committed_blocked_manifest() -> None:
    manifest_bytes = MANIFEST_PATH.read_bytes()
    probe_bytes = _probe_bytes(manifest_bytes)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "COMPLETE_SDK_ADMISSION"


@pytest.mark.parametrize(
    "missing_evidence",
    [
        "written_permission",
        "provider_test_vector",
        "security_review",
        "tls_verification_remediation",
    ],
)
def test_activation_requires_every_admission_evidence_reference(
    missing_evidence: str,
) -> None:
    manifest = _admitted_manifest()
    manifest["admission"]["evidence"][missing_evidence] = None
    manifest_bytes = _json_bytes(manifest)
    probe_bytes = _probe_bytes(manifest_bytes)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "COMPLETE_SDK_ADMISSION"


def test_unresolved_sdk_license_cannot_be_labeled_admitted() -> None:
    manifest = _admitted_manifest()
    manifest["sdk"]["license"] = "UNRESOLVED"
    manifest_bytes = _json_bytes(manifest)
    probe_bytes = _probe_bytes(manifest_bytes)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "COMPLETE_SDK_ADMISSION"


def test_admission_requires_an_exact_https_provider_base_url() -> None:
    manifest = _admitted_manifest()
    manifest["admission"]["provider_base_url"] = None
    manifest_bytes = _json_bytes(manifest)
    probe_bytes = _probe_bytes(manifest_bytes)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "COMPLETE_SDK_ADMISSION"


def test_expired_probe_cannot_activate_read_only() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["captured_at"] = (CHECKED_AT - timedelta(hours=1)).isoformat()
    probe["capability_expires_at"] = (CHECKED_AT - timedelta(seconds=1)).isoformat()
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.capability_expires_at == CHECKED_AT - timedelta(seconds=1)
    assert result.next_action == "REFRESH_GUARDED_READ_ONLY_PROBE"


@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate"])
def test_probe_operation_set_must_be_exact_and_closed(mutation: str) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    if mutation == "missing":
        probe["operations"].pop()
    elif mutation == "extra":
        probe["operations"].append(_operation("News"))
    else:
        probe["operations"].append(_operation(REQUIRED_PROBE_OPERATIONS[0]))

    probe_bytes = _json_bytes(probe)
    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "FIX_PROBE_OPERATION_SET"


@pytest.mark.parametrize(
    "write_operation",
    ["PlaceOrder", "CancelOrder", "SimulationTrade", "WebhookSubscription"],
)
def test_any_write_simulation_subscription_or_webhook_probe_is_rejected(
    write_operation: str,
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["operations"].append(_operation(write_operation))

    probe_bytes = _json_bytes(probe)
    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REMOVE_FORBIDDEN_PROBE_OPERATION"


def test_probe_is_bound_to_exact_manifest_and_sdk_subject() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["sdk_commit"] = "0" * 40

    probe_bytes = _json_bytes(probe)
    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_SUBJECT"


def test_request_cap_and_all_probe_checks_must_pass() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    over_cap = _probe_document(manifest_bytes)
    over_cap["operations"][0]["request_count"] = 4
    failed_check = _probe_document(manifest_bytes)
    failed_check["checks"]["pagination"] = False
    over_cap_bytes = _json_bytes(over_cap)
    failed_check_bytes = _json_bytes(failed_check)

    cap_result = evaluate_activation(
        manifest_bytes,
        over_cap_bytes,
        _receipt_bytes(over_cap_bytes),
        checked_at=CHECKED_AT,
    )
    check_result = evaluate_activation(
        manifest_bytes,
        failed_check_bytes,
        _receipt_bytes(failed_check_bytes),
        checked_at=CHECKED_AT,
    )

    assert cap_result.next_action == "REDUCE_PROBE_REQUEST_COUNT"
    assert check_result.next_action == "REVIEW_PROBE_EVIDENCE"


def test_future_dated_probe_cannot_activate() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["captured_at"] = (CHECKED_AT + timedelta(minutes=1)).isoformat()
    probe["capability_expires_at"] = (CHECKED_AT + timedelta(hours=1)).isoformat()
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_EVIDENCE"


def test_complete_sanitized_probe_is_the_only_activation_counterexample() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)
    receipt_bytes = _receipt_bytes(probe_bytes)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        receipt_bytes,
        checked_at=CHECKED_AT,
    )

    assert result.status == "activated_read_only"
    assert result.next_action is None
    assert result.capability_expires_at == CHECKED_AT + timedelta(hours=1)
    assert (
        result.contract_hash
        == evaluate_activation(
            manifest_bytes,
            probe_bytes,
            receipt_bytes,
            checked_at=CHECKED_AT + timedelta(minutes=1),
        ).contract_hash
    )


def test_complete_probe_without_publication_receipt_cannot_activate() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)

    result = evaluate_activation(manifest_bytes, probe_bytes, checked_at=CHECKED_AT)

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "PROVIDE_PROBE_PUBLICATION_RECEIPT"


@pytest.mark.parametrize(
    "receipt_mutation",
    ["malformed", "wrong_digest", "wrong_signature", "extra_field"],
)
def test_publication_receipt_is_strict_and_bound_to_exact_artifact(
    receipt_mutation: str,
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)
    receipt = json.loads(_receipt_bytes(probe_bytes))
    if receipt_mutation == "malformed":
        receipt_bytes = b"not-json"
    else:
        if receipt_mutation == "wrong_digest":
            receipt["artifact_sha256"] = "0" * 64
        elif receipt_mutation == "wrong_signature":
            receipt["signature_b64u"] = "A" * 86
        else:
            receipt["unexpected"] = True
        receipt_bytes = _json_bytes(receipt)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        receipt_bytes,
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_PUBLICATION_RECEIPT"


def test_publication_receipt_cannot_replay_across_probe_bytes() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)
    other_probe_bytes = probe_bytes + b"\n"

    result = evaluate_activation(
        manifest_bytes,
        other_probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_PUBLICATION_RECEIPT"


@pytest.mark.parametrize(
    ("schema_version", "public_key"),
    [
        ("nexus.fosun.sanitized-probe.v1", TEST_PUBLICATION_PUBLIC_KEY_B64U),
        ("nexus.fosun.sanitized-probe.v2", "A" * 42),
        ("nexus.fosun.sanitized-probe.v2", "A" * 42 + "B"),
    ],
)
def test_probe_v2_requires_exact_raw_ed25519_public_key(
    schema_version: str,
    public_key: str,
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["schema_version"] = schema_version
    probe["publication_public_key_b64u"] = public_key
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"


def test_contract_hash_binds_exact_publication_receipt_bytes() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)
    receipt_bytes = _receipt_bytes(probe_bytes)

    canonical = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        receipt_bytes,
        checked_at=CHECKED_AT,
    )
    with_trailing_newline = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        receipt_bytes + b"\n",
        checked_at=CHECKED_AT,
    )

    assert canonical.status == with_trailing_newline.status == "activated_read_only"
    assert canonical.contract_hash != with_trailing_newline.contract_hash


def test_activation_paths_rejects_oversized_publication_receipt(tmp_path: Path) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)
    manifest_path = tmp_path / "manifest.json"
    probe_path = tmp_path / "probe.json"
    receipt_path = tmp_path / "receipt.json"
    manifest_path.write_bytes(manifest_bytes)
    probe_path.write_bytes(probe_bytes)
    receipt_path.write_bytes(b"x" * (16 * 1024 + 1))

    result = evaluate_activation_paths(
        manifest_path,
        probe_path,
        receipt_path,
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_PUBLICATION_RECEIPT"


def test_cli_activates_only_with_matching_publication_receipt(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    captured_at = datetime.now(UTC)
    probe = _probe_document(manifest_bytes)
    probe["captured_at"] = captured_at.isoformat()
    probe["capability_expires_at"] = (captured_at + timedelta(hours=1)).isoformat()
    probe_bytes = _json_bytes(probe)
    manifest_path = tmp_path / "manifest.json"
    probe_path = tmp_path / "probe.json"
    receipt_path = tmp_path / "receipt.json"
    manifest_path.write_bytes(manifest_bytes)
    probe_path.write_bytes(probe_bytes)
    receipt_path.write_bytes(_receipt_bytes(probe_bytes))

    exit_code = main(
        [
            "--manifest",
            str(manifest_path),
            "--probe-artifact",
            str(probe_path),
            "--publication-receipt",
            str(receipt_path),
            "--require-activated",
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["status"] == "activated_read_only"


@pytest.mark.parametrize("sensitive_field", ["api_key", "account_id", "raw_response", "holdings"])
def test_probe_contract_has_no_place_for_sensitive_or_account_data(
    sensitive_field: str,
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe[sensitive_field] = "must-not-be-accepted"
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"


def test_contract_hash_binds_exact_probe_artifact_bytes() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)

    canonical = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )
    trailing_probe_bytes = probe_bytes + b"\n"
    with_trailing_newline = evaluate_activation(
        manifest_bytes,
        trailing_probe_bytes,
        _receipt_bytes(trailing_probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert canonical.status == with_trailing_newline.status == "activated_read_only"
    assert canonical.contract_hash != with_trailing_newline.contract_hash


def test_cli_reports_current_block_without_requiring_probe(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["--manifest", str(MANIFEST_PATH)])

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["status"] == "fixture_with_next_action"
    assert output["next_action"] == "COMPLETE_SDK_ADMISSION"
    assert set(output) == {
        "status",
        "contract_hash",
        "checked_at",
        "capability_expires_at",
        "next_action",
    }


def _admitted_manifest() -> dict[str, object]:
    manifest = json.loads(MANIFEST_PATH.read_text())
    manifest["sdk"]["license"] = "WRITTEN-PERMISSION"
    manifest["admission"]["status"] = "admitted"
    manifest["admission"]["provider_base_url"] = "https://provider.invalid"
    manifest["admission"]["evidence"] = {
        "written_permission": "evidence/written-permission.sha256",
        "provider_test_vector": "evidence/provider-test-vector.sha256",
        "security_review": "evidence/security-review.sha256",
        "tls_verification_remediation": "evidence/tls-remediation.sha256",
    }
    return manifest


def _probe_bytes(manifest_bytes: bytes) -> bytes:
    return _json_bytes(_probe_document(manifest_bytes))


def _probe_document(manifest_bytes: bytes) -> dict[str, object]:
    manifest = json.loads(manifest_bytes)
    return {
        "schema_version": "nexus.fosun.sanitized-probe.v2",
        "provider": "fosun",
        "captured_at": CHECKED_AT.isoformat(),
        "capability_expires_at": (CHECKED_AT + timedelta(hours=1)).isoformat(),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "sdk_tag": manifest["sdk"]["tag"],
        "sdk_commit": manifest["sdk"]["commit"],
        "publication_public_key_b64u": _public_key_b64u(),
        "operations": [_operation(name) for name in REQUIRED_PROBE_OPERATIONS],
        "checks": {
            "permission_read_only": True,
            "history_window": True,
            "adjustment_semantics": True,
            "pagination": True,
            "rate_limit": True,
            "response_contract": True,
        },
    }


def _operation(name: str) -> dict[str, object]:
    return {
        "name": name,
        "request_count": 1,
        "passed": True,
        "response_contract_sha256": hashlib.sha256(name.encode()).hexdigest(),
    }


def _json_bytes(document: dict[str, object]) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def _receipt_bytes(probe_bytes: bytes) -> bytes:
    artifact_sha256 = hashlib.sha256(probe_bytes).hexdigest()
    signature = TEST_PUBLICATION_KEY.sign(
        PUBLICATION_DOMAIN + bytes.fromhex(artifact_sha256)
    )
    return _json_bytes(
        {
            "schema_version": "nexus.fosun.probe-publication.v1",
            "artifact_sha256": artifact_sha256,
            "signature_b64u": _b64u(signature),
        }
    )


def _public_key_b64u() -> str:
    public_key = TEST_PUBLICATION_KEY.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return _b64u(public_key)


def _b64u(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()
