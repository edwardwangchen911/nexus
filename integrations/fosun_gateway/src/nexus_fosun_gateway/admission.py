"""Machine-readable admission gate for the frozen Fosun SDK and contracts."""

import json
from collections.abc import Mapping

from nexus_fosun_gateway.settings import GatewaySettings

_SDK_TAG = "v1.2.0"
_SDK_COMMIT = "891d95fb4967efaa682387c1b35966ceea38fb21"
_OPENAPI: dict[str, tuple[str, str]] = {
    "market": (
        "https://openapi-docs.fosunxcz.com/specs/public/market/market/openapi.yaml",
        "f35cfaa3484d8507723729b68a2b8d60fa85d4d28d06991b4a434995cbfac22a",
    ),
    "trade": (
        "https://openapi-docs.fosunxcz.com/specs/public/trade/openapi.yaml",
        "379561548be045c8e531f31e88ee76c8cbdb7bda5b59e12a9b9d5c9040abd204",
    ),
    "push": (
        "https://openapi-docs.fosunxcz.com/specs/public/market/push/openapi.yaml",
        "15f5416f268c9c47cdcef6a2ff7eaaa89ab4b61f1c9927074969b1e3fef32c05",
    ),
}
_REQUIRED_EVIDENCE = (
    "written_permission",
    "provider_test_vector",
    "security_review",
    "tls_verification_remediation",
)


def sdk_admission_is_valid(settings: GatewaySettings) -> bool:
    """Require the runtime switch and a complete record for the exact reviewed subject."""
    if not settings.sdk_admitted:
        return False
    try:
        document = json.loads(settings.provider_manifest.read_text())
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if not isinstance(document, Mapping) or document.get("provider") != "fosun":
        return False
    sdk = document.get("sdk")
    if not isinstance(sdk, Mapping):
        return False
    if sdk.get("tag") != _SDK_TAG or sdk.get("commit") != _SDK_COMMIT:
        return False
    openapi = document.get("openapi")
    if not isinstance(openapi, Mapping):
        return False
    for name, (url, digest) in _OPENAPI.items():
        contract = openapi.get(name)
        if not isinstance(contract, Mapping):
            return False
        if contract.get("url") != url or contract.get("sha256") != digest:
            return False
    admission = document.get("admission")
    if not isinstance(admission, Mapping) or admission.get("status") != "admitted":
        return False
    if admission.get("sdk_commit") != _SDK_COMMIT:
        return False
    admission_hashes = admission.get("openapi_sha256")
    if not isinstance(admission_hashes, Mapping) or any(
        admission_hashes.get(name) != digest for name, (_, digest) in _OPENAPI.items()
    ):
        return False
    evidence = admission.get("evidence")
    if not isinstance(evidence, Mapping):
        return False
    return all(_evidence_reference(evidence.get(name)) for name in _REQUIRED_EVIDENCE)


def _evidence_reference(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip()) and len(value) <= 512
