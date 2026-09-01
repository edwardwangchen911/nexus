"""Value-free v2 evidence generation for the one-shot Fosun read-only probe."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from threading import Lock
from typing import Annotated, Literal

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
    ProbeChecks,
    ProbeOperationEvidence,
    ProbePublicationReceipt,
    SanitizedProbeArtifact,
    ScopedProbeArtifact,
    ScopedProbePublicationReceipt,
)
from nexus_fosun_gateway.adapter.probe_sdk import (
    CoreProbeRequestPlan,
    ProbeExecutionEvidence,
    ProbeRequestPlan,
    ProbeSdkError,
    SdkProbeExecutor,
)
from nexus_fosun_gateway.admission import probe_ready_sdk_subject
from nexus_fosun_gateway.probe_contract import (
    BROKER_PROBE_OPERATIONS,
    MAX_BROKER_REQUESTS,
    REQUIRED_PROBE_OPERATIONS,
    probe_response_contract_sha256,
    scope_descriptor,
)
from nexus_fosun_gateway.publication import DurableProbePublication

_Symbol = Annotated[str, StringConstraints(pattern=r"^us[A-Z][A-Z0-9.]{0,14}$")]
_OutputBasename = Annotated[
    str,
    StringConstraints(pattern=r"^[a-z0-9][a-z0-9._-]{0,62}\.json$"),
]
_RunId = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9-]{15,63}$")]
_PUBLICATION_RECEIPT_BASENAME = "publication-receipt.json"
_SCOPED_PUBLICATION_SIGNING_DOMAIN = b"NEXUS-FOSUN-PROBE-PUBLICATION-V2\0"


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def response_contract_sha256(label: str) -> str:
    """Return a code-owned digest that never sees runtime values or model metadata."""
    digest = probe_response_contract_sha256(label)
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
            or (self.cash_flow_date_to - self.cash_flow_date_from).days >= 7
            or ".." in self.output_basename
            or self.output_basename == _PUBLICATION_RECEIPT_BASENAME
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


class CoreProbePlan(BaseModel):
    """Strict non-secret inputs for one explicit core probe."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        hide_input_in_errors=True,
    )

    run_id: _RunId
    scope: Literal["core"]
    symbol: _Symbol
    session_date: date
    limit: int = Field(ge=1, le=10)
    capability_ttl_seconds: int = Field(ge=60, le=86_400)
    output_basename: _OutputBasename

    @model_validator(mode="after")
    def _validate_output_name(self) -> CoreProbePlan:
        if (
            ".." in self.output_basename
            or self.output_basename == _PUBLICATION_RECEIPT_BASENAME
        ):
            raise ValueError("probe plan output name is invalid")
        return self

    def validate_for_start(self, started_at: datetime) -> None:
        _require_aware(started_at)
        if self.session_date >= started_at.astimezone(UTC).date():
            raise ProbeSdkError("INVALID_PROBE_PLAN")

    def to_request_plan(self) -> CoreProbeRequestPlan:
        return CoreProbeRequestPlan(
            symbol=self.symbol,
            session_date=self.session_date,
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
        "run_id",
        "scope",
        "scope_contract_version",
    )
    _private_key: Ed25519PrivateKey | None

    def __init__(
        self,
        *,
        artifact_bytes: bytes,
        capability_expires_at: datetime,
        private_key: Ed25519PrivateKey,
        run_id: str | None = None,
        scope: Literal["core"] | None = None,
        scope_contract_version: str | None = None,
    ) -> None:
        self.artifact_bytes = artifact_bytes
        self.artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
        self.byte_length = len(artifact_bytes)
        self.capability_expires_at = capability_expires_at
        self.run_id = run_id
        self.scope = scope
        self.scope_contract_version = scope_contract_version
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
                receipt: ScopedProbePublicationReceipt | ProbePublicationReceipt
                if (
                    self.scope == "core"
                    and self.run_id is not None
                    and self.scope_contract_version is not None
                ):
                    signature = private_key.sign(
                        _SCOPED_PUBLICATION_SIGNING_DOMAIN
                        + bytes.fromhex(self.artifact_sha256)
                        + b"\0"
                        + self.run_id.encode()
                        + b"\0"
                        + self.scope.encode()
                        + b"\0"
                        + self.scope_contract_version.encode()
                    )
                    receipt = ScopedProbePublicationReceipt(
                        schema_version="nexus.fosun.probe-publication.v2",
                        artifact_sha256=self.artifact_sha256,
                        run_id=self.run_id,
                        scope=self.scope,
                        scope_contract_version=self.scope_contract_version,
                        signature_b64u=_base64url(signature),
                    )
                elif (
                    self.scope is None
                    and self.run_id is None
                    and self.scope_contract_version is None
                ):
                    signature = private_key.sign(
                        PUBLICATION_SIGNING_DOMAIN
                        + bytes.fromhex(self.artifact_sha256)
                    )
                    receipt = ProbePublicationReceipt(
                        schema_version="nexus.fosun.probe-publication.v1",
                        artifact_sha256=self.artifact_sha256,
                        signature_b64u=_base64url(signature),
                    )
                else:
                    raise ProbeSdkError("PROBE_OUTPUT_INVALID")
                receipt_bytes = _canonical_json(receipt.model_dump(mode="json"))
                type(receipt).model_validate_json(receipt_bytes)
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
    plan: ProbePlan | CoreProbePlan,
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
    try:
        return _build_prepared_probe(
            manifest_bytes,
            plan,
            evidence,
            started_at=started_at,
            captured_at=captured_at,
            private_key_factory=private_key_factory,
        )
    except ProbeSdkError as error:
        # Settings validation occurs before the executor. Once the fixed sequence has
        # returned, any signer failure must conservatively consume the probe window.
        if error.code == "PROBE_SETTINGS_INVALID":
            raise ProbeSdkError("PROBE_FAILED") from None
        raise


def _build_prepared_probe(
    manifest_bytes: bytes,
    plan: ProbePlan | CoreProbePlan,
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
    subject = probe_ready_sdk_subject(manifest_document)
    if subject is None:
        raise ProbeSdkError("COMPLETE_SDK_ADMISSION")
    if isinstance(plan, CoreProbePlan):
        descriptor = scope_descriptor("core")
        _validate_core_execution_evidence(evidence)
    else:
        descriptor = None
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

    counts: dict[str, int] = dict(evidence.request_counts)
    if isinstance(plan, CoreProbePlan) and descriptor is not None:
        artifact: SanitizedProbeArtifact | ScopedProbeArtifact = ScopedProbeArtifact(
            schema_version="nexus.fosun.sanitized-probe.v3",
            run_id=plan.run_id,
            scope="core",
            scope_contract_version=descriptor.scope_contract_version,
            provider="fosun",
            captured_at=captured_at,
            capability_expires_at=capability_expires_at,
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            sdk_tag=subject.sdk_tag,
            sdk_commit=subject.sdk_commit,
            publication_public_key_b64u=_base64url(public_key),
            physical_operation_set=descriptor.physical_operation_set,
            prerequisite_operation_set=descriptor.prerequisite_operation_set,
            authorized_operation_set=descriptor.authorized_operation_set,
            operations=tuple(
                ProbeOperationEvidence(
                    name=label,
                    request_count=counts[label],
                    passed=True,
                    response_contract_sha256=response_contract_sha256(label),
                )
                for label in descriptor.physical_operation_set
            ),
            checks=ProbeChecks(
                permission_read_only=True,
                history_window=True,
                adjustment_semantics=True,
                pagination=True,
                rate_limit=True,
                response_contract=True,
            ),
        )
    else:
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
                for label in REQUIRED_PROBE_OPERATIONS
            ),
            checks=ProbeChecks(
                permission_read_only=False,
                history_window=evidence.history_window,
                adjustment_semantics=evidence.adjustment_semantics,
                pagination=evidence.pagination,
                rate_limit=False,
                response_contract=evidence.response_contract,
            ),
        )
    artifact_bytes = _canonical_json(artifact.model_dump(mode="json"))
    if isinstance(plan, CoreProbePlan):
        validate_scoped_probe_artifact(
            manifest_bytes,
            artifact_bytes,
            expected_scope="core",
        )
    else:
        validate_sanitized_probe_artifact(manifest_bytes, artifact_bytes)
    return PreparedProbeArtifact(
        artifact_bytes=artifact_bytes,
        capability_expires_at=capability_expires_at,
        private_key=private_key,
        run_id=plan.run_id if isinstance(plan, CoreProbePlan) else None,
        scope="core" if isinstance(plan, CoreProbePlan) else None,
        scope_contract_version=(
            descriptor.scope_contract_version
            if isinstance(plan, CoreProbePlan) and descriptor is not None
            else None
        ),
    )


def validate_scoped_probe_artifact(
    manifest_bytes: bytes,
    artifact_bytes: bytes,
    *,
    expected_scope: Literal["core"],
) -> ScopedProbeArtifact:
    """Validate exact canonical core v3 bytes without granting runtime activation."""
    manifest_document = _load_manifest(manifest_bytes)
    subject = probe_ready_sdk_subject(manifest_document)
    if subject is None:
        raise ProbeSdkError("COMPLETE_SDK_ADMISSION")
    try:
        artifact = ScopedProbeArtifact.model_validate_json(artifact_bytes)
        canonical = _canonical_json(artifact.model_dump(mode="json"))
        _decode_base64url_exact(artifact.publication_public_key_b64u, expected_bytes=32)
        descriptor = scope_descriptor(expected_scope)
    except (ValidationError, UnicodeError, ValueError):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID") from None
    if (
        not hmac.compare_digest(canonical, artifact_bytes)
        or artifact.scope != expected_scope
        or artifact.scope_contract_version != descriptor.scope_contract_version
        or artifact.physical_operation_set != descriptor.physical_operation_set
        or artifact.prerequisite_operation_set != descriptor.prerequisite_operation_set
        or artifact.authorized_operation_set != descriptor.authorized_operation_set
        or tuple(item.name for item in artifact.operations)
        != descriptor.physical_operation_set
        or artifact.manifest_sha256 != hashlib.sha256(manifest_bytes).hexdigest()
        or artifact.sdk_tag != subject.sdk_tag
        or artifact.sdk_commit != subject.sdk_commit
        or any(item.request_count != 1 or not item.passed for item in artifact.operations)
        or not artifact.checks.all_passed()
        or any(
            item.response_contract_sha256 != response_contract_sha256(item.name)
            for item in artifact.operations
        )
    ):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    return artifact


def validate_sanitized_probe_artifact(
    manifest_bytes: bytes,
    artifact_bytes: bytes,
) -> SanitizedProbeArtifact:
    """Validate v2 bytes and static contracts without granting activation."""
    manifest_document = _load_manifest(manifest_bytes)
    subject = probe_ready_sdk_subject(manifest_document)
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
        or sum(
            item.request_count
            for item in artifact.operations
            if item.name in BROKER_PROBE_OPERATIONS
        )
        > MAX_BROKER_REQUESTS
        or not artifact.checks.one_shot_observations_are_exact()
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
        or sum(
            count
            for name, count in counts
            if name in BROKER_PROBE_OPERATIONS
        )
        > MAX_BROKER_REQUESTS
        or not isinstance(evidence.auth_successes, int)
        or isinstance(evidence.auth_successes, bool)
        or evidence.auth_successes != 1
        or evidence.history_window is not True
        or evidence.pagination is not False
        or evidence.adjustment_semantics is not False
        or evidence.response_contract is not True
    ):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")


def _validate_core_execution_evidence(evidence: ProbeExecutionEvidence) -> None:
    descriptor = scope_descriptor("core")
    if not isinstance(evidence, ProbeExecutionEvidence):
        raise ProbeSdkError("PROBE_RESPONSE_INVALID")
    counts = evidence.request_counts
    if (
        tuple(name for name, _count in counts) != descriptor.physical_operation_set
        or any(count != 1 for _name, count in counts)
        or not isinstance(evidence.auth_successes, int)
        or isinstance(evidence.auth_successes, bool)
        or evidence.auth_successes != 1
        or evidence.history_window is not True
        or evidence.pagination is not True
        or evidence.adjustment_semantics is not True
        or evidence.response_contract is not True
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


def _decode_base64url_exact(value: str, *, expected_bytes: int) -> bytes:
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (TypeError, ValueError):
        raise ValueError("invalid base64url") from None
    if len(decoded) != expected_bytes or not hmac.compare_digest(_base64url(decoded), value):
        raise ValueError("invalid base64url")
    return decoded


def _require_aware(value: datetime) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ProbeSdkError("INVALID_PROBE_PLAN")
