import base64
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import nexus_fosun_gateway.activation as activation_module
import nexus_fosun_gateway.probe_contract as probe_contract
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from nexus_fosun_gateway.activation import (
    REQUIRED_PROBE_OPERATIONS,
    FosunActivationResult,
    evaluate_activation_paths,
    main,
)
from nexus_fosun_gateway.activation import (
    evaluate_activation as _evaluate_activation,
)
from nexus_fosun_gateway.probe_contract import probe_response_contract_sha256

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
TRUSTED_PUBLICATION_KEY_BYTES = TEST_PUBLICATION_PUBLIC_KEY_B64U.encode("ascii")


def test_scoped_activation_admits_only_exact_v3_v2_scope_pair() -> None:
    evaluator = getattr(activation_module, "evaluate_scoped_activation")
    manifest_bytes = _json_bytes(_admitted_manifest())
    result, authorization = evaluator(
        manifest_bytes,
        _scoped_probe_bytes(manifest_bytes, scope="core"),
        _scoped_receipt_bytes(_scoped_probe_bytes(manifest_bytes, scope="core"), scope="core"),
        expected_scope="core",
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CHECKED_AT,
    )
    assert result.status == "activated_read_only"
    assert authorization is not None
    assert authorization.scope == "core"
    with pytest.raises((TypeError, AttributeError)):
        authorization.scope = "extended"  # type: ignore[misc]


def test_scoped_activation_rejects_cross_scope_pair() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _scoped_probe_bytes(manifest_bytes, scope="core")
    result, authorization = activation_module.evaluate_scoped_activation(
        manifest_bytes, probe_bytes, _scoped_receipt_bytes(probe_bytes, scope="core"),
        expected_scope="extended", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CHECKED_AT,
    )
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"
    assert authorization is None


def test_scoped_activation_admits_exact_extended_authorization() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    extended = _scoped_probe_bytes(manifest_bytes, scope="extended", run_id="readonly-extended-run-0001")
    result, authorization = activation_module.evaluate_scoped_activation(manifest_bytes, extended, _scoped_receipt_bytes(extended, scope="extended", run_id="readonly-extended-run-0001"), expected_scope="extended", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
    core = _scoped_probe_bytes(manifest_bytes, scope="core")
    core_result, _ = activation_module.evaluate_scoped_activation(manifest_bytes, core, _scoped_receipt_bytes(core, scope="core"), expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
    assert result.status == "activated_read_only" and authorization is not None
    assert authorization.scope == "extended"
    assert authorization.scope_contract_version == "nexus.fosun.read-only-scope.v1.extended"
    assert authorization.run_id == "readonly-extended-run-0001"
    assert authorization.capability_expires_at == CHECKED_AT + timedelta(hours=1)
    assert authorization.authorized_operation_set == frozenset({"fosun_list_orders", "fosun_list_cash_flows"})
    assert result.contract_hash != core_result.contract_hash


@pytest.mark.parametrize("check", ["permission_read_only", "history_window", "adjustment_semantics", "pagination", "rate_limit", "response_contract"])
def test_scoped_activation_rejects_each_failed_check(check: str) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    document = json.loads(_scoped_probe_bytes(manifest_bytes, scope="core"))
    document["checks"][check] = False
    probe_bytes = _json_bytes(document)
    result, authorization = activation_module.evaluate_scoped_activation(manifest_bytes, probe_bytes, _scoped_receipt_bytes(probe_bytes, scope="core"), expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"
    assert authorization is None


@pytest.mark.parametrize("field,value", [("captured_at", "2026-08-27T08:00:00"), ("capability_expires_at", "2026-08-27T09:00:00")])
def test_scoped_activation_rejects_naive_timestamps(field: str, value: str) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    document = json.loads(_scoped_probe_bytes(manifest_bytes, scope="core"))
    document[field] = value
    probe_bytes = _json_bytes(document)
    result, authorization = activation_module.evaluate_scoped_activation(manifest_bytes, probe_bytes, _scoped_receipt_bytes(probe_bytes, scope="core"), expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"
    assert authorization is None


def test_scoped_activation_rejects_per_operation_and_broker_budgets() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    for index, counts in ((4, [1, 1, 1, 1, 4, 1]), (None, [1, 2, 2, 2, 1, 1])):
        document = json.loads(_scoped_probe_bytes(manifest_bytes, scope="core"))
        for operation, count in zip(document["operations"], counts, strict=True): operation["request_count"] = count
        probe_bytes = _json_bytes(document)
        result, authorization = activation_module.evaluate_scoped_activation(manifest_bytes, probe_bytes, _scoped_receipt_bytes(probe_bytes, scope="core"), expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
        assert result.next_action == "REVIEW_PROBE_ARTIFACT" and authorization is None


def test_scoped_activation_total_budget_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    # The closed core row has six operations, so 6 * 3 < production total cap 24.
    assert 6 * activation_module.MAX_REQUESTS_PER_OPERATION < activation_module.MAX_PROBE_REQUESTS
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _scoped_probe_bytes(manifest_bytes, scope="core")
    monkeypatch.setattr(activation_module, "MAX_PROBE_REQUESTS", 5)
    result, authorization = activation_module.evaluate_scoped_activation(manifest_bytes, probe_bytes, _scoped_receipt_bytes(probe_bytes, scope="core"), expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"
    assert authorization is None


def test_scoped_activation_rejects_legacy_v2_v1_pair() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    legacy = _probe_bytes(manifest_bytes)
    result, authorization = activation_module.evaluate_scoped_activation(
        manifest_bytes, legacy, _receipt_bytes(legacy), expected_scope="core",
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT,
    )
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"
    assert authorization is None


def test_scoped_run_id_signature_and_hash_are_independently_bound() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    original = _scoped_probe_bytes(manifest_bytes, scope="core")
    changed_document = json.loads(original)
    changed_document["run_id"] = "readonly-core-run-0002"
    changed = _json_bytes(changed_document)
    stale_result, stale_authorization = activation_module.evaluate_scoped_activation(manifest_bytes, changed, _scoped_receipt_bytes(changed, scope="core", run_id="readonly-core-run-0002", signature_run_id="readonly-core-run-0001"), expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
    assert stale_result.next_action == "REVIEW_PROBE_ARTIFACT"
    assert stale_authorization is None
    result, authorization = activation_module.evaluate_scoped_activation(manifest_bytes, changed, _scoped_receipt_bytes(changed, scope="core", run_id="readonly-core-run-0002"), expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
    original_result, _ = activation_module.evaluate_scoped_activation(manifest_bytes, original, _scoped_receipt_bytes(original, scope="core"), expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
    assert result.status == "activated_read_only" and authorization is not None
    assert authorization.run_id == "readonly-core-run-0002"
    assert authorization.scope == "core"
    assert authorization.scope_contract_version == "nexus.fosun.read-only-scope.v1.core"
    assert authorization.capability_expires_at == CHECKED_AT + timedelta(hours=1)
    assert authorization.authorized_operation_set == frozenset({"fosun_sync_accounts", "fosun_sync_portfolio:cash", "fosun_sync_portfolio:holdings", "fosun_get_quotes", "fosun_get_bars"})
    assert result.contract_hash != original_result.contract_hash


@pytest.mark.parametrize("mutation", ["reordered", "duplicate", "extra", "passed", "digest", "ttl"])
def test_scoped_activation_fail_closed_for_canonical_mutations(mutation: str) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    document = json.loads(_scoped_probe_bytes(manifest_bytes, scope="core"))
    if mutation == "reordered": document["physical_operation_set"].reverse()
    elif mutation == "duplicate": document["authorized_operation_set"].append(document["authorized_operation_set"][0])
    elif mutation == "extra": document["prerequisite_operation_set"].append("quote")
    elif mutation == "passed": document["operations"][0]["passed"] = False
    elif mutation == "digest": document["operations"][0]["response_contract_sha256"] = "0" * 64
    else: document["capability_expires_at"] = "2026-08-28T09:00:01Z"
    probe_bytes = _json_bytes(document)
    result, authorization = activation_module.evaluate_scoped_activation(manifest_bytes, probe_bytes, _scoped_receipt_bytes(probe_bytes, scope="core"), expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT)
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"
    assert authorization is None


@pytest.mark.parametrize("field,value", [
    ("run_id", "readonly-core-run-0002"),
    ("scope_contract_version", "nexus.fosun.read-only-scope.v1.extended"),
    ("physical_operation_set", ["Accounts", "Session"]),
    ("authorized_operation_set", ["fosun_get_bars"]),
    ("capability_expires_at", "2026-08-27T08:00:00Z"),
])
def test_scoped_activation_rejects_bound_artifact_mutations(field: str, value: object) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    document = json.loads(_scoped_probe_bytes(manifest_bytes, scope="core"))
    document[field] = value
    probe_bytes = _json_bytes(document)
    result, authorization = activation_module.evaluate_scoped_activation(
        manifest_bytes, probe_bytes, _scoped_receipt_bytes(probe_bytes, scope="core"),
        expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES, checked_at=CHECKED_AT,
    )
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"
    assert authorization is None


def evaluate_activation(
    manifest_bytes: bytes,
    probe_bytes: bytes | None = None,
    publication_receipt_bytes: bytes | None = None,
    trusted_publication_public_key_bytes: bytes | None = TRUSTED_PUBLICATION_KEY_BYTES,
    *,
    checked_at: datetime | None = None,
) -> FosunActivationResult:
    """Exercise normal tests with the independently configured test trust anchor."""
    return _evaluate_activation(
        manifest_bytes,
        probe_bytes,
        publication_receipt_bytes,
        trusted_publication_public_key_bytes=(
            trusted_publication_public_key_bytes
        ),
        checked_at=checked_at,
    )


def test_admitted_manifest_requires_independent_publisher_trust_anchor() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)

    missing = _evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )
    malformed = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        trusted_publication_public_key_bytes=(
            b"not-a-canonical-ed25519-public-key"
        ),
        checked_at=CHECKED_AT,
    )

    assert missing.next_action == "PROVIDE_TRUSTED_PROBE_PUBLISHER"
    assert malformed.next_action == "REVIEW_TRUSTED_PROBE_PUBLISHER"


def test_artifact_self_signed_by_untrusted_key_cannot_activate() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    attacker_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    probe = _probe_document(manifest_bytes)
    probe["publication_public_key_b64u"] = _b64u(
        attacker_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes, private_key=attacker_key),
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_PUBLISHER_BINDING"


def test_committed_probe_ready_manifest_cannot_activate_continuous_runtime() -> None:
    result = evaluate_activation(MANIFEST_PATH.read_bytes(), checked_at=CHECKED_AT)

    assert result == FosunActivationResult(
        status="fixture_with_next_action",
        contract_hash=result.contract_hash,
        checked_at=CHECKED_AT,
        capability_expires_at=None,
        next_action="COMPLETE_SDK_ADMISSION",
    )
    assert len(result.contract_hash) == 64


def test_complete_probe_can_verify_the_committed_one_shot_subject() -> None:
    manifest_bytes = MANIFEST_PATH.read_bytes()
    probe_bytes = _probe_bytes(manifest_bytes, one_shot=True)

    verifier = getattr(activation_module, "verify_probe_publication", None)
    assert callable(verifier)
    contract_hash = verifier(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CHECKED_AT,
    )

    assert isinstance(contract_hash, str)
    assert len(contract_hash) == 64


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
    probe["captured_at"] = _utc_text(CHECKED_AT - timedelta(hours=1))
    probe["capability_expires_at"] = _utc_text(CHECKED_AT - timedelta(seconds=1))
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


def test_probe_at_exact_capability_expiry_cannot_activate_read_only() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["captured_at"] = _utc_text(CHECKED_AT - timedelta(hours=1))
    probe["capability_expires_at"] = _utc_text(CHECKED_AT)
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.capability_expires_at == CHECKED_AT
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


def test_probe_operation_sequence_must_match_the_fixed_runner_plan() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["operations"][0], probe["operations"][1] = (
        probe["operations"][1],
        probe["operations"][0],
    )
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "FIX_PROBE_OPERATION_SEQUENCE"


def test_probe_response_contract_digests_are_code_owned() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["operations"][0]["response_contract_sha256"] = "0" * 64
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_RESPONSE_CONTRACT"


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


def test_signed_probe_cannot_exceed_the_broker_domain_request_cap() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["operations"][0]["request_count"] = 2
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REDUCE_PROBE_REQUEST_COUNT"


def test_future_dated_probe_cannot_activate() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    probe["captured_at"] = _utc_text(CHECKED_AT + timedelta(minutes=1))
    probe["capability_expires_at"] = _utc_text(CHECKED_AT + timedelta(hours=1))
    probe_bytes = _json_bytes(probe)

    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_EVIDENCE"


def test_activation_clock_and_probe_timestamps_must_be_utc() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe = _probe_document(manifest_bytes)
    non_utc = timezone(timedelta(hours=8))

    with pytest.raises(ValueError):
        evaluate_activation(
            manifest_bytes,
            _json_bytes(probe),
            _receipt_bytes(_json_bytes(probe)),
            checked_at=CHECKED_AT.astimezone(non_utc),
        )

    probe["captured_at"] = CHECKED_AT.astimezone(non_utc).isoformat()
    probe["capability_expires_at"] = (
        CHECKED_AT + timedelta(hours=1)
    ).astimezone(non_utc).isoformat()
    probe_bytes = _json_bytes(probe)
    result = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        _receipt_bytes(probe_bytes),
        checked_at=CHECKED_AT,
    )

    assert result.next_action == "REVIEW_PROBE_ARTIFACT"


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
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"


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

    assert canonical.status == "activated_read_only"
    assert with_trailing_newline.status == "fixture_with_next_action"
    assert with_trailing_newline.next_action == "REVIEW_PROBE_PUBLICATION_RECEIPT"
    assert canonical.contract_hash != with_trailing_newline.contract_hash


def test_contract_hash_binds_independent_publisher_trust_anchor() -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)
    receipt_bytes = _receipt_bytes(probe_bytes)
    alternate_key = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    alternate_public_key = _b64u(
        alternate_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    ).encode("ascii")

    trusted = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        receipt_bytes,
        checked_at=CHECKED_AT,
    )
    alternate = evaluate_activation(
        manifest_bytes,
        probe_bytes,
        receipt_bytes,
        trusted_publication_public_key_bytes=alternate_public_key,
        checked_at=CHECKED_AT,
    )

    assert trusted.status == "activated_read_only"
    assert alternate.next_action == "REVIEW_PROBE_PUBLISHER_BINDING"
    assert trusted.contract_hash != alternate.contract_hash


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
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_PUBLICATION_RECEIPT"


@pytest.mark.parametrize("linked_input", ["manifest", "probe", "receipt"])
def test_activation_paths_never_follow_capability_evidence_symlinks(
    tmp_path: Path,
    linked_input: str,
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)
    originals = {
        "manifest": tmp_path / "manifest-original.json",
        "probe": tmp_path / "probe-original.json",
        "receipt": tmp_path / "receipt-original.json",
    }
    originals["manifest"].write_bytes(manifest_bytes)
    originals["probe"].write_bytes(probe_bytes)
    originals["receipt"].write_bytes(_receipt_bytes(probe_bytes))
    supplied = dict(originals)
    supplied[linked_input] = tmp_path / f"{linked_input}-link.json"
    supplied[linked_input].symlink_to(originals[linked_input])

    result = evaluate_activation_paths(
        supplied["manifest"],
        supplied["probe"],
        supplied["receipt"],
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"


def test_activation_paths_rejects_group_or_world_writable_evidence(
    tmp_path: Path,
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)
    manifest_path = tmp_path / "manifest.json"
    probe_path = tmp_path / "probe.json"
    receipt_path = tmp_path / "receipt.json"
    manifest_path.write_bytes(manifest_bytes)
    probe_path.write_bytes(probe_bytes)
    receipt_path.write_bytes(_receipt_bytes(probe_bytes))
    receipt_path.chmod(0o666)

    result = evaluate_activation_paths(
        manifest_path,
        probe_path,
        receipt_path,
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CHECKED_AT,
    )

    assert result.status == "fixture_with_next_action"
    assert result.next_action == "REVIEW_PROBE_PUBLICATION_RECEIPT"


@pytest.mark.parametrize("attacked_input", ["manifest", "probe", "receipt"])
@pytest.mark.parametrize(
    ("attack_kind", "writable_parent_mode"),
    [
        ("intermediate_symlink", None),
        ("hard_link", None),
        ("writable_parent", 0o770),
        ("writable_parent", 0o777),
    ],
)
def test_activation_paths_rejects_path_capability_attacks_for_every_input(
    tmp_path: Path,
    attacked_input: str,
    attack_kind: str,
    writable_parent_mode: int | None,
) -> None:
    secure_dir = tmp_path / "secure"
    secure_dir.mkdir(mode=0o700)
    secure_dir.chmod(0o700)
    manifest_bytes = _json_bytes(_admitted_manifest())
    probe_bytes = _probe_bytes(manifest_bytes)
    contents = {
        "manifest": manifest_bytes,
        "probe": probe_bytes,
        "receipt": _receipt_bytes(probe_bytes),
    }
    supplied: dict[str, Path] = {}
    for name, content in contents.items():
        path = secure_dir / f"{name}.json"
        path.write_bytes(content)
        path.chmod(0o600)
        supplied[name] = path

    if attack_kind == "intermediate_symlink":
        actual_parent = secure_dir / "actual-parent"
        actual_parent.mkdir(mode=0o700)
        attacked_path = actual_parent / f"{attacked_input}.json"
        attacked_path.write_bytes(contents[attacked_input])
        attacked_path.chmod(0o600)
        linked_parent = secure_dir / "linked-parent"
        linked_parent.symlink_to(actual_parent, target_is_directory=True)
        supplied[attacked_input] = linked_parent / attacked_path.name
    elif attack_kind == "hard_link":
        attacked_path = secure_dir / f"{attacked_input}-hard-link.json"
        os.link(supplied[attacked_input], attacked_path)
        supplied[attacked_input] = attacked_path
    else:
        assert writable_parent_mode is not None
        writable_parent = secure_dir / "writable-parent"
        writable_parent.mkdir(mode=0o700)
        attacked_path = writable_parent / f"{attacked_input}.json"
        attacked_path.write_bytes(contents[attacked_input])
        attacked_path.chmod(0o600)
        writable_parent.chmod(writable_parent_mode)
        supplied[attacked_input] = attacked_path

    result = evaluate_activation_paths(
        supplied["manifest"],
        supplied["probe"],
        supplied["receipt"],
        trusted_publication_public_key_bytes=TRUSTED_PUBLICATION_KEY_BYTES,
        checked_at=CHECKED_AT,
    )

    expected_action = {
        "manifest": "REVIEW_PROVIDER_MANIFEST",
        "probe": "REVIEW_PROBE_ARTIFACT",
        "receipt": "REVIEW_PROBE_PUBLICATION_RECEIPT",
    }
    assert result.status == "fixture_with_next_action"
    assert result.next_action == expected_action[attacked_input]


def test_bounded_evidence_reader_rejects_metadata_change_during_read(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_bytes(b'{"synthetic":true}')
    evidence_path.chmod(0o600)
    real_fstat = os.fstat
    fstat_calls = 0

    monkeypatch.setattr(
        activation_module,
        "_open_evidence_file",
        lambda _path: os.open(evidence_path, os.O_RDONLY),
    )

    def drifting_fstat(file_descriptor: int):
        nonlocal fstat_calls
        metadata = real_fstat(file_descriptor)
        fstat_calls += 1
        if fstat_calls != 2:
            return metadata
        fields = {
            name: getattr(metadata, name)
            for name in (
                "st_dev",
                "st_ino",
                "st_uid",
                "st_mode",
                "st_nlink",
                "st_size",
                "st_mtime_ns",
                "st_ctime_ns",
            )
        }
        fields["st_mtime_ns"] += 1
        return SimpleNamespace(**fields)

    monkeypatch.setattr(activation_module.os, "fstat", drifting_fstat)

    content = activation_module._read_bounded_or_empty(
        evidence_path,
        max_bytes=1024,
    )

    assert content == b""
    assert fstat_calls == 2


def test_relative_evidence_reader_rejects_untrusted_starting_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    unsafe_cwd = tmp_path / "unsafe-cwd"
    unsafe_cwd.mkdir(mode=0o700)
    evidence_path = unsafe_cwd / "evidence.json"
    evidence_path.write_bytes(b'{"synthetic":true}')
    evidence_path.chmod(0o600)
    unsafe_cwd.chmod(0o777)
    monkeypatch.chdir(unsafe_cwd)

    assert (
        activation_module._read_bounded_or_empty(
            Path("evidence.json"),
            max_bytes=1024,
        )
        == b""
    )


def test_cli_activates_only_with_matching_publication_receipt(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest_bytes = _json_bytes(_admitted_manifest())
    captured_at = datetime.now(UTC)
    probe = _probe_document(manifest_bytes)
    probe["captured_at"] = _utc_text(captured_at)
    probe["capability_expires_at"] = _utc_text(captured_at + timedelta(hours=1))
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
            "--trusted-publisher-public-key-b64u",
            TEST_PUBLICATION_PUBLIC_KEY_B64U,
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

    assert canonical.status == "activated_read_only"
    assert with_trailing_newline.status == "fixture_with_next_action"
    assert with_trailing_newline.next_action == "REVIEW_PROBE_ARTIFACT"
    assert canonical.contract_hash != with_trailing_newline.contract_hash


def test_activation_cli_keeps_continuous_runtime_closed_for_probe_ready_manifest(
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
    manifest["sdk"]["license"] = "PROVIDER-AUTHORIZED-PRIVATE-USE"
    manifest["admission"]["status"] = "admitted"
    manifest["admission"]["provider_base_url"] = "https://openapi.fosunxcz.com"
    manifest["admission"]["evidence"] = {
        "written_permission": "evidence/written-permission.sha256",
        "provider_test_vector": "evidence/provider-test-vector.sha256",
        "security_review": "evidence/security-review.sha256",
        "tls_verification_remediation": "evidence/tls-remediation.sha256",
    }
    return manifest


def _probe_bytes(manifest_bytes: bytes, *, one_shot: bool = False) -> bytes:
    return _json_bytes(_probe_document(manifest_bytes, one_shot=one_shot))


def _probe_document(
    manifest_bytes: bytes,
    *,
    one_shot: bool = False,
) -> dict[str, object]:
    manifest = json.loads(manifest_bytes)
    return {
        "schema_version": "nexus.fosun.sanitized-probe.v2",
        "provider": "fosun",
        "captured_at": _utc_text(CHECKED_AT),
        "capability_expires_at": _utc_text(CHECKED_AT + timedelta(hours=1)),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "sdk_tag": manifest["sdk"]["tag"],
        "sdk_commit": manifest["sdk"]["commit"],
        "publication_public_key_b64u": _public_key_b64u(),
        "operations": [_operation(name) for name in REQUIRED_PROBE_OPERATIONS],
        "checks": {
            "permission_read_only": not one_shot,
            "history_window": True,
            "adjustment_semantics": not one_shot,
            "pagination": not one_shot,
            "rate_limit": not one_shot,
            "response_contract": True,
        },
    }


def _operation(name: str) -> dict[str, object]:
    response_contract = probe_response_contract_sha256(name) or "0" * 64
    return {
        "name": name,
        "request_count": 1,
        "passed": True,
        "response_contract_sha256": response_contract,
    }


def _json_bytes(document: dict[str, object]) -> bytes:
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def _utc_text(value: datetime) -> str:
    assert value.utcoffset() == timedelta(0)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _receipt_bytes(
    probe_bytes: bytes,
    *,
    private_key: Ed25519PrivateKey = TEST_PUBLICATION_KEY,
) -> bytes:
    artifact_sha256 = hashlib.sha256(probe_bytes).hexdigest()
    signature = private_key.sign(
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


def _scoped_probe_bytes(manifest_bytes: bytes, *, scope: str, run_id: str = "readonly-core-run-0001") -> bytes:
    descriptor = probe_contract.scope_descriptor(scope)  # type: ignore[arg-type]
    manifest = json.loads(manifest_bytes)
    document = {
        "schema_version": "nexus.fosun.sanitized-probe.v3",
        "run_id": run_id,
        "scope": scope,
        "scope_contract_version": descriptor.scope_contract_version,
        "provider": "fosun",
        "captured_at": _utc_text(CHECKED_AT),
        "capability_expires_at": _utc_text(CHECKED_AT + timedelta(hours=1)),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "sdk_tag": manifest["sdk"]["tag"],
        "sdk_commit": manifest["sdk"]["commit"],
        "publication_public_key_b64u": _public_key_b64u(),
        "physical_operation_set": list(descriptor.physical_operation_set),
        "prerequisite_operation_set": list(descriptor.prerequisite_operation_set),
        "authorized_operation_set": list(descriptor.authorized_operation_set),
        "operations": [_operation(name) for name in descriptor.physical_operation_set],
        "checks": {"permission_read_only": True, "history_window": True, "adjustment_semantics": True, "pagination": True, "rate_limit": True, "response_contract": True},
    }
    return _json_bytes(document)


def _scoped_receipt_bytes(probe_bytes: bytes, *, scope: str, run_id: str = "readonly-core-run-0001", signature_run_id: str | None = None) -> bytes:
    descriptor = probe_contract.scope_descriptor(scope)  # type: ignore[arg-type]
    digest = hashlib.sha256(probe_bytes).hexdigest()
    signature = TEST_PUBLICATION_KEY.sign(
        b"NEXUS-FOSUN-PROBE-PUBLICATION-V2\0" + bytes.fromhex(digest) + b"\0" + (signature_run_id or run_id).encode() + b"\0" + scope.encode() + b"\0" + descriptor.scope_contract_version.encode()
    )
    return _json_bytes({"schema_version": "nexus.fosun.probe-publication.v2", "artifact_sha256": digest, "run_id": run_id, "scope": scope, "scope_contract_version": descriptor.scope_contract_version, "signature_b64u": _b64u(signature)})
