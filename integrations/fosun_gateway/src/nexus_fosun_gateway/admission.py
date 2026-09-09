"""Machine-readable admission gate for the frozen Fosun SDK and contracts."""

import json
from collections.abc import Mapping
from dataclasses import dataclass

from nexus_fosun_gateway.settings import FOSUN_REST_BASE_URL, GatewaySettings

_SDK_TAG = "v1.2.0"
_SDK_COMMIT = "891d95fb4967efaa682387c1b35966ceea38fb21"
_SDK_PRIVATE_USE_BASIS = "PROVIDER-AUTHORIZED-PRIVATE-USE"
_READ_ONLY_OPENAPI: dict[str, tuple[str, str]] = {
    "market": (
        "https://openapi-docs.fosunxcz.com/specs/public/market/market/openapi.yaml",
        "f35cfaa3484d8507723729b68a2b8d60fa85d4d28d06991b4a434995cbfac22a",
    ),
    "trade": (
        "https://openapi-docs.fosunxcz.com/specs/public/trade/openapi.yaml",
        "87e4e6601b8da122a504239a1922d8742ecd4072e2d10c30c0d6a97f896fb808",
    ),
}
_PUSH_OPENAPI = (
    "https://openapi-docs.fosunxcz.com/specs/public/market/push/openapi.yaml",
    "15f5416f268c9c47cdcef6a2ff7eaaa89ab4b61f1c9927074969b1e3fef32c05",
)
_REQUIRED_EVIDENCE = (
    "written_permission",
    "provider_test_vector",
    "security_review",
    "tls_verification_remediation",
)


@dataclass(frozen=True, slots=True)
class AdmittedSdkSubject:
    provider_base_url: str
    sdk_tag: str
    sdk_commit: str
    evidence_references: tuple[str, ...]


def sdk_admission_is_valid(settings: GatewaySettings) -> bool:
    """Require the runtime switch and a complete record for the exact reviewed subject."""
    if not settings.sdk_admitted:
        return False
    try:
        document = json.loads(settings.provider_manifest.read_text())
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return (
        sdk_admission_document_is_valid(document)
        and _provider_base_url(document) == settings.fsopenapi_base_url
    )


def sdk_admission_document_is_valid(document: object) -> bool:
    """Validate admission evidence without reading settings, credentials, or provider state."""
    return _sdk_subject_document_is_valid(document, status="admitted")


def sdk_probe_ready_document_is_valid(document: object) -> bool:
    """Validate the exact subject allowed only for one explicit, bounded probe."""
    return _sdk_subject_document_is_valid(
        document, status="probe_ready"
    ) or _sdk_subject_document_is_valid(document, status="admitted")


def _sdk_subject_document_is_valid(document: object, *, status: str) -> bool:
    if not isinstance(document, Mapping) or document.get("provider") != "fosun":
        return False
    sdk = document.get("sdk")
    if not isinstance(sdk, Mapping):
        return False
    if sdk.get("tag") != _SDK_TAG or sdk.get("commit") != _SDK_COMMIT:
        return False
    if sdk.get("license") != _SDK_PRIVATE_USE_BASIS:
        return False
    openapi = document.get("openapi")
    if not isinstance(openapi, Mapping):
        return False
    for name, (url, digest) in _READ_ONLY_OPENAPI.items():
        contract = openapi.get(name)
        if not isinstance(contract, Mapping):
            return False
        if contract.get("url") != url or contract.get("sha256") != digest:
            return False
    admission = document.get("admission")
    if not isinstance(admission, Mapping) or admission.get("status") != status:
        return False
    if _provider_base_url(document) is None:
        return False
    if admission.get("sdk_commit") != _SDK_COMMIT:
        return False
    admission_hashes = admission.get("openapi_sha256")
    if not isinstance(admission_hashes, Mapping) or any(
        admission_hashes.get(name) != digest
        for name, (_, digest) in _READ_ONLY_OPENAPI.items()
    ):
        return False
    evidence = admission.get("evidence")
    if not isinstance(evidence, Mapping):
        return False
    return all(_evidence_reference(evidence.get(name)) for name in _REQUIRED_EVIDENCE)


def admitted_sdk_subject(document: object) -> AdmittedSdkSubject | None:
    """Return the exact safe subject only after the complete document is admitted."""
    return _validated_sdk_subject(document, status="admitted")


def probe_ready_sdk_subject(document: object) -> AdmittedSdkSubject | None:
    """Return the exact subject permitted only to the one-shot probe runner."""
    return _validated_sdk_subject(
        document, status="probe_ready"
    ) or _validated_sdk_subject(document, status="admitted")


def push_probe_ready_sdk_subject(document: object) -> AdmittedSdkSubject | None:
    """Return the frozen subject only when the push contract is bound too."""
    subject = probe_ready_sdk_subject(document)
    if subject is None or not isinstance(document, Mapping):
        return None
    openapi = document.get("openapi")
    admission = document.get("admission")
    if not isinstance(openapi, Mapping) or not isinstance(admission, Mapping):
        return None
    contract = openapi.get("push")
    hashes = admission.get("openapi_sha256")
    url, digest = _PUSH_OPENAPI
    if (
        not isinstance(contract, Mapping)
        or contract.get("url") != url
        or contract.get("sha256") != digest
        or not isinstance(hashes, Mapping)
        or hashes.get("push") != digest
    ):
        return None
    return subject


def _validated_sdk_subject(
    document: object,
    *,
    status: str,
) -> AdmittedSdkSubject | None:
    if (
        not _sdk_subject_document_is_valid(document, status=status)
        or not isinstance(document, Mapping)
    ):
        return None
    sdk = document.get("sdk")
    admission = document.get("admission")
    if not isinstance(sdk, Mapping) or not isinstance(admission, Mapping):
        return None
    evidence = admission.get("evidence")
    provider_base_url = _provider_base_url(document)
    if not isinstance(evidence, Mapping) or provider_base_url is None:
        return None
    references: list[str] = []
    for name in _REQUIRED_EVIDENCE:
        value = evidence.get(name)
        if not isinstance(value, str):
            return None
        references.append(value)
    return AdmittedSdkSubject(
        provider_base_url=provider_base_url,
        sdk_tag=_SDK_TAG,
        sdk_commit=_SDK_COMMIT,
        evidence_references=tuple(references),
    )


def _evidence_reference(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip()) and len(value) <= 512


def _provider_base_url(document: Mapping[object, object]) -> str | None:
    admission = document.get("admission")
    if not isinstance(admission, Mapping):
        return None
    value = admission.get("provider_base_url")
    return FOSUN_REST_BASE_URL if value == FOSUN_REST_BASE_URL else None
