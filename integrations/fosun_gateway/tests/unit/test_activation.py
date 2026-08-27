import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from nexus_fosun_gateway.activation import (
    REQUIRED_PROBE_OPERATIONS,
    FosunActivationResult,
    evaluate_activation,
    main,
)

MANIFEST_PATH = Path(__file__).parents[4] / "contracts" / "providers" / "fosun" / "manifest.json"
CHECKED_AT = datetime(2026, 8, 27, 8, 0, tzinfo=UTC)


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

    result = evaluate_activation(
        manifest_bytes,
        _probe_bytes(manifest_bytes),
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

    result = evaluate_activation(manifest_bytes, probe_bytes, checked_at=CHECKED_AT)

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "COMPLETE_SDK_ADMISSION"


def test_unresolved_sdk_license_cannot_be_labeled_admitted() -> None:
    manifest = _admitted_manifest()
    manifest["sdk"]["license"] = "UNRESOLVED"
    manifest_bytes = _json_bytes(manifest)

    result = evaluate_activation(
        manifest_bytes,
        _probe_bytes(manifest_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "COMPLETE_SDK_ADMISSION"


def test_admission_requires_an_exact_https_provider_base_url() -> None:
    manifest = _admitted_manifest()
    manifest["admission"]["provider_base_url"] = None
    manifest_bytes = _json_bytes(manifest)

    result = evaluate_activation(
        manifest_bytes,
        _probe_bytes(manifest_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "COMPLETE_SDK_ADMISSION"


def test_expired_probe_cannot_activate_read_only() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["captured_at"] = (CHECKED_AT - timedelta(hours=1)).isoformat()
    probe["capability_expires_at"] = (CHECKED_AT - timedelta(seconds=1)).isoformat()

    result = evaluate_activation(manifest_bytes, _json_bytes(probe), checked_at=CHECKED_AT)

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

    result = evaluate_activation(manifest_bytes, _json_bytes(probe), checked_at=CHECKED_AT)

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

    result = evaluate_activation(manifest_bytes, _json_bytes(probe), checked_at=CHECKED_AT)

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REMOVE_FORBIDDEN_PROBE_OPERATION"


def test_probe_is_bound_to_exact_manifest_and_sdk_subject() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["sdk_commit"] = "0" * 40

    result = evaluate_activation(manifest_bytes, _json_bytes(probe), checked_at=CHECKED_AT)

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_SUBJECT"


def test_request_cap_and_all_probe_checks_must_pass() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    over_cap = _probe_document(manifest_bytes)
    over_cap["operations"][0]["request_count"] = 4
    failed_check = _probe_document(manifest_bytes)
    failed_check["checks"]["pagination"] = False

    cap_result = evaluate_activation(
        manifest_bytes,
        _json_bytes(over_cap),
        checked_at=CHECKED_AT,
    )
    check_result = evaluate_activation(
        manifest_bytes,
        _json_bytes(failed_check),
        checked_at=CHECKED_AT,
    )

    assert cap_result.next_action == "REDUCE_PROBE_REQUEST_COUNT"
    assert check_result.next_action == "REVIEW_PROBE_EVIDENCE"


def test_future_dated_probe_cannot_activate() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["captured_at"] = (CHECKED_AT + timedelta(minutes=1)).isoformat()
    probe["capability_expires_at"] = (CHECKED_AT + timedelta(hours=1)).isoformat()

    result = evaluate_activation(manifest_bytes, _json_bytes(probe), checked_at=CHECKED_AT)

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_EVIDENCE"


def test_complete_sanitized_probe_is_the_only_activation_counterexample() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)

    result = evaluate_activation(manifest_bytes, probe_bytes, checked_at=CHECKED_AT)

    assert result.status == "activated_read_only"
    assert result.next_action is None
    assert result.capability_expires_at == CHECKED_AT + timedelta(hours=1)
    assert (
        result.contract_hash
        == evaluate_activation(
            manifest_bytes,
            probe_bytes,
            checked_at=CHECKED_AT + timedelta(minutes=1),
        ).contract_hash
    )


@pytest.mark.parametrize("sensitive_field", ["api_key", "account_id", "raw_response", "holdings"])
def test_probe_contract_has_no_place_for_sensitive_or_account_data(
    sensitive_field: str,
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe[sensitive_field] = "must-not-be-accepted"

    result = evaluate_activation(manifest_bytes, _json_bytes(probe), checked_at=CHECKED_AT)

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"


def test_contract_hash_binds_exact_probe_artifact_bytes() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)

    canonical = evaluate_activation(manifest_bytes, probe_bytes, checked_at=CHECKED_AT)
    with_trailing_newline = evaluate_activation(
        manifest_bytes,
        probe_bytes + b"\n",
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
        "schema_version": "nexus.fosun.sanitized-probe.v1",
        "provider": "fosun",
        "captured_at": CHECKED_AT.isoformat(),
        "capability_expires_at": (CHECKED_AT + timedelta(hours=1)).isoformat(),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "sdk_tag": manifest["sdk"]["tag"],
        "sdk_commit": manifest["sdk"]["commit"],
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
