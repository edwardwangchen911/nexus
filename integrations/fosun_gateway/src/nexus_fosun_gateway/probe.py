"""Value-free v2 evidence generation for the one-shot Fosun read-only probe."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from threading import Lock
from types import MappingProxyType
from typing import Annotated, cast

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    model_validator,
)

from nexus_fosun_gateway.activation import (
    MAX_PROBE_REQUESTS,
    MAX_REQUESTS_PER_OPERATION,
    PUBLICATION_SIGNING_DOMAIN,
    REQUIRED_PROBE_OPERATIONS,
    ProbeChecks,
    ProbeOperationEvidence,
    ProbePublicationReceipt,
    SanitizedProbeArtifact,
)
from nexus_fosun_gateway.adapter.probe_sdk import (
    ProbeExecutionEvidence,
    ProbeLabel,
    ProbeRequestPlan,
    ProbeSdkError,
    SdkProbeExecutor,
)
from nexus_fosun_gateway.admission import admitted_sdk_subject
from nexus_fosun_gateway.publication import DurableProbePublication

_Symbol = Annotated[str, StringConstraints(pattern=r"^us[A-Z][A-Z0-9.]{0,14}$")]
_OutputBasename = Annotated[
    str,
    StringConstraints(pattern=r"^[a-z0-9][a-z0-9._-]{0,62}\.json$"),
]
_CONTRACT_SCHEMA_VERSION = "nexus.fosun.probe-response-contract.v1"
_REQUIRED_LABELS = cast("tuple[ProbeLabel, ...]", REQUIRED_PROBE_OPERATIONS)

_PAGE_FIELDS: tuple[tuple[str, str], ...] = (
    ("items", "tuple[expected-item]"),
    ("source_request_id", "nonempty-string-discarded-after-validation"),
    ("has_more", "boolean"),
    ("next_cursor", "null-or-nonempty-string"),
    ("as_of", "aware-utc-datetime"),
    ("quality", "PASS-or-PARTIAL"),
)
_ITEM_FIELDS: Mapping[str, tuple[tuple[str, str], ...]] = MappingProxyType({
    "Session": (
        ("http_status", "integer-2xx"),
        ("response_body", "bounded-and-discarded"),
    ),
    "Accounts": (
        ("kind", "literal-account"),
        ("external_account_id", "nonempty-string-in-memory-only"),
        ("status", "string"),
        ("base_currency", "null-or-string"),
    ),
    "CashSummary": (
        ("kind", "literal-cash"),
        ("external_account_id", "nonempty-string-in-memory-only"),
        ("currency", "string"),
        ("cash_balance", "finite-decimal"),
        ("available_cash", "finite-decimal"),
        ("provider_as_of", "null-or-aware-utc-datetime"),
    ),
    "Holdings": (
        ("kind", "literal-position"),
        ("external_account_id", "nonempty-string-in-memory-only"),
        ("instrument_code", "string"),
        ("currency", "string"),
        ("quantity", "finite-decimal"),
        ("available_quantity", "finite-decimal"),
        ("frozen_quantity", "finite-decimal"),
        ("average_cost", "null-or-finite-decimal"),
        ("diluted_cost", "null-or-finite-decimal"),
        ("current_price", "null-or-finite-decimal"),
        ("provider_as_of", "null-or-aware-utc-datetime"),
    ),
    "OrderList": (
        ("kind", "literal-order"),
        ("external_order_id", "nonempty-string"),
        ("instrument_code", "string"),
        ("side", "buy-or-sell"),
        ("quantity", "finite-decimal"),
        ("filled_quantity", "finite-decimal"),
        ("price", "null-or-finite-decimal"),
        ("status", "string"),
        ("submitted_at", "aware-utc-datetime"),
        ("updated_at", "null-or-aware-utc-datetime"),
    ),
    "CashFlows": (
        ("kind", "literal-cash-flow"),
        ("external_cash_flow_id", "nonempty-string"),
        ("currency", "string"),
        ("amount", "finite-decimal"),
        ("occurred_at", "aware-utc-datetime"),
        ("flow_type", "string"),
    ),
    "quote": (
        ("kind", "literal-quote"),
        ("instrument_code", "requested-symbol"),
        ("price", "null-or-finite-decimal"),
        ("as_of", "aware-utc-datetime"),
    ),
    "kline_minimal": (
        ("kind", "literal-bar"),
        ("instrument_code", "requested-symbol"),
        ("interval", "literal-day"),
        ("timestamp", "requested-provider-day-utc"),
        ("open", "positive-finite-decimal"),
        ("high", "positive-finite-decimal"),
        ("low", "positive-finite-decimal"),
        ("close", "positive-finite-decimal"),
        ("volume", "nonnegative-finite-decimal"),
    ),
})


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _contract_material(label: str) -> dict[str, object]:
    item_fields = _ITEM_FIELDS.get(label)
    if item_fields is None or label not in _REQUIRED_LABELS:
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    return {
        "contract_schema_version": _CONTRACT_SCHEMA_VERSION,
        "item_fields": item_fields,
        "logical_label": label,
        "page_fields": (
            (("authentication", "bounded-physical-http-result"),)
            if label == "Session"
            else _PAGE_FIELDS
        ),
    }


_RESPONSE_CONTRACT_SHA256: Mapping[str, str] = MappingProxyType({
    label: hashlib.sha256(_canonical_json(_contract_material(label))).hexdigest()
    for label in _REQUIRED_LABELS
})


def response_contract_sha256(label: str) -> str:
    """Return a code-owned digest that never sees runtime values or model metadata."""
    digest = _RESPONSE_CONTRACT_SHA256.get(label)
    if digest is None:
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    return digest


class ProbePlan(BaseModel):
    """Strict non-secret operator inputs for one closed read-only probe."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        hide_input_in_errors=True,
    )

    symbol: _Symbol
    session_date: date
    cash_flow_date_from: date
    cash_flow_date_to: date
    limit: int = Field(ge=1, le=10)
    capability_ttl_seconds: int = Field(ge=60, le=86_400)
    output_basename: _OutputBasename

    @model_validator(mode="after")
    def _validate_closed_ranges(self) -> ProbePlan:
        if (
            self.cash_flow_date_from > self.cash_flow_date_to
            or (self.cash_flow_date_to - self.cash_flow_date_from).days > 7
            or ".." in self.output_basename
        ):
            raise ValueError("probe plan range is invalid")
        return self

    def validate_for_start(self, started_at: datetime) -> None:
        _require_aware(started_at)
        if self.session_date >= started_at.astimezone(UTC).date():
            raise ProbeSdkError("INVALID_PROBE_PLAN")

    def to_request_plan(self) -> ProbeRequestPlan:
        return ProbeRequestPlan(
            symbol=self.symbol,
            session_date=self.session_date,
            cash_flow_date_from=self.cash_flow_date_from,
            cash_flow_date_to=self.cash_flow_date_to,
            limit=self.limit,
        )


class PreparedProbeArtifact:
    """Canonical artifact plus one in-memory, one-shot publication signing key."""

    __slots__ = (
        "_private_key",
        "_sign_lock",
        "_signing_started",
        "artifact_bytes",
        "artifact_sha256",
        "byte_length",
        "capability_expires_at",
    )
    _private_key: Ed25519PrivateKey | None

    def __init__(
        self,
        *,
        artifact_bytes: bytes,
        capability_expires_at: datetime,
        private_key: Ed25519PrivateKey,
    ) -> None:
        self.artifact_bytes = artifact_bytes
        self.artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
        self.byte_length = len(artifact_bytes)
        self.capability_expires_at = capability_expires_at
        self._private_key = private_key
        self._sign_lock = Lock()
        self._signing_started = False

    def __repr__(self) -> str:
        return (
            "PreparedProbeArtifact("
            f"artifact_sha256={self.artifact_sha256!r}, "
            f"byte_length={self.byte_length!r}, "
            f"capability_expires_at={self.capability_expires_at!r})"
        )

    def sign_publication_receipt(
        self,
        publication: DurableProbePublication,
    ) -> bytes:
        with self._sign_lock:
            if self._signing_started:
                raise ProbeSdkError("PROBE_OUTPUT_INVALID")
            self._signing_started = True
            private_key = self._private_key
            self._private_key = None
            if private_key is None:
                raise ProbeSdkError("PROBE_OUTPUT_INVALID")
            if type(publication) is not DurableProbePublication or not publication.attests(
                self.artifact_sha256,
                self.byte_length,
            ):
                raise ProbeSdkError("PROBE_OUTPUT_INVALID")
            try:
                signature = private_key.sign(
                    PUBLICATION_SIGNING_DOMAIN
                    + bytes.fromhex(self.artifact_sha256)
                )
                receipt = ProbePublicationReceipt(
                    schema_version="nexus.fosun.probe-publication.v1",
                    artifact_sha256=self.artifact_sha256,
                    signature_b64u=_base64url(signature),
                )
                receipt_bytes = _canonical_json(receipt.model_dump(mode="json"))
                ProbePublicationReceipt.model_validate_json(receipt_bytes)
            except Exception:
                raise ProbeSdkError("PROBE_OUTPUT_INVALID") from None
        return receipt_bytes

    def dispose_signing_key(self) -> None:
        """Irreversibly drop this process's reference to unused signing material."""
        with self._sign_lock:
            self._signing_started = True
            self._private_key = None


async def run_sanitized_probe(
    manifest_bytes: bytes,
    plan: ProbePlan,
    executor: SdkProbeExecutor,
    *,
    started_at: datetime,
    private_key_factory: Callable[[], Ed25519PrivateKey],
    utc_clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> PreparedProbeArtifact:
    """Execute first; only a successful exact SDK executor may create signing material."""
    if type(executor) is not SdkProbeExecutor:
        raise ProbeSdkError("SDK_CONTRACT_MISMATCH")
    plan.validate_for_start(started_at)
    evidence = await executor.run(plan.to_request_plan())
    try:
        captured_at = utc_clock()
    except Exception:
        raise ProbeSdkError("PROBE_FAILED") from None
    return _build_prepared_probe(
        manifest_bytes,
        plan,
        evidence,
        started_at=started_at,
        captured_at=captured_at,
        private_key_factory=private_key_factory,
    )


def _build_prepared_probe(
    manifest_bytes: bytes,
    plan: ProbePlan,
    evidence: ProbeExecutionEvidence,
    *,
    started_at: datetime,
    captured_at: datetime,
    private_key_factory: Callable[[], Ed25519PrivateKey],
) -> PreparedProbeArtifact:
    plan.validate_for_start(started_at)
    _require_aware(captured_at)
    if captured_at < started_at:
        raise ProbeSdkError("PROBE_FAILED")
    capability_expires_at = started_at + timedelta(
        seconds=plan.capability_ttl_seconds
    )
    if captured_at >= capability_expires_at:
        raise ProbeSdkError("PROBE_CAPABILITY_EXPIRED")

    manifest_document = _load_manifest(manifest_bytes)
    subject = admitted_sdk_subject(manifest_document)
    if subject is None:
        raise ProbeSdkError("COMPLETE_SDK_ADMISSION")
    _validate_execution_evidence(evidence)

    try:
        private_key = private_key_factory()
        if not isinstance(private_key, Ed25519PrivateKey):
            raise TypeError
        public_key = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    except ProbeSdkError as error:
        if error.code == "PROBE_SETTINGS_INVALID":
            raise
        raise ProbeSdkError("PROBE_FAILED") from None
    except Exception:
        raise ProbeSdkError("PROBE_FAILED") from None

    counts = dict(evidence.request_counts)
    artifact = SanitizedProbeArtifact(
        schema_version="nexus.fosun.sanitized-probe.v2",
        provider="fosun",
        captured_at=captured_at,
        capability_expires_at=capability_expires_at,
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        sdk_tag=subject.sdk_tag,
        sdk_commit=subject.sdk_commit,
        publication_public_key_b64u=_base64url(public_key),
        operations=tuple(
            ProbeOperationEvidence(
                name=label,
                request_count=counts[label],
                passed=True,
                response_contract_sha256=response_contract_sha256(label),
            )
            for label in _REQUIRED_LABELS
        ),
        checks=ProbeChecks(
            permission_read_only=True,
            history_window=evidence.history_window,
            adjustment_semantics=evidence.adjustment_semantics,
            pagination=evidence.pagination,
            rate_limit=True,
            response_contract=evidence.response_contract,
        ),
    )
    artifact_bytes = _canonical_json(artifact.model_dump(mode="json"))
    validate_sanitized_probe_artifact(manifest_bytes, artifact_bytes)
    return PreparedProbeArtifact(
        artifact_bytes=artifact_bytes,
        capability_expires_at=capability_expires_at,
        private_key=private_key,
    )


def validate_sanitized_probe_artifact(
    manifest_bytes: bytes,
    artifact_bytes: bytes,
) -> SanitizedProbeArtifact:
    """Validate v2 bytes and static contracts without granting activation."""
    manifest_document = _load_manifest(manifest_bytes)
    subject = admitted_sdk_subject(manifest_document)
    if subject is None:
        raise ProbeSdkError("COMPLETE_SDK_ADMISSION")
    try:
        artifact = SanitizedProbeArtifact.model_validate_json(artifact_bytes)
        canonical = _canonical_json(artifact.model_dump(mode="json"))
    except (ValidationError, ValueError):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID") from None
    if not hmac.compare_digest(canonical, artifact_bytes):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    if (
        artifact.manifest_sha256 != hashlib.sha256(manifest_bytes).hexdigest()
        or artifact.sdk_tag != subject.sdk_tag
        or artifact.sdk_commit != subject.sdk_commit
        or tuple(item.name for item in artifact.operations)
        != REQUIRED_PROBE_OPERATIONS
        or any(
            item.request_count > MAX_REQUESTS_PER_OPERATION or not item.passed
            for item in artifact.operations
        )
        or sum(item.request_count for item in artifact.operations)
        > MAX_PROBE_REQUESTS
        or not artifact.checks.all_passed()
        or any(
            item.response_contract_sha256
            != response_contract_sha256(item.name)
            for item in artifact.operations
        )
    ):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    return artifact


def _validate_execution_evidence(evidence: ProbeExecutionEvidence) -> None:
    if not isinstance(evidence, ProbeExecutionEvidence):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    counts = evidence.request_counts
    if (
        tuple(name for name, _count in counts) != REQUIRED_PROBE_OPERATIONS
        or any(
            not isinstance(count, int)
            or isinstance(count, bool)
            or not 1 <= count <= MAX_REQUESTS_PER_OPERATION
            for _name, count in counts
        )
        or sum(count for _name, count in counts) > MAX_PROBE_REQUESTS
        or not isinstance(evidence.auth_successes, int)
        or isinstance(evidence.auth_successes, bool)
        or evidence.auth_successes != 1
        or not all(
            type(value) is bool and value
            for value in (
                evidence.history_window,
                evidence.pagination,
                evidence.adjustment_semantics,
                evidence.response_contract,
            )
        )
    ):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")


def _load_manifest(manifest_bytes: bytes) -> object:
    try:
        document = json.loads(manifest_bytes)
    except (UnicodeError, json.JSONDecodeError):
        raise ProbeSdkError("COMPLETE_SDK_ADMISSION") from None
    return document


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _require_aware(value: datetime) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ProbeSdkError("INVALID_PROBE_PLAN")
