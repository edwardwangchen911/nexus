"""Local Platform key and explicit funding authority; never contact a provider or DB.

Manual console evidence is an operator attestation, not a remote balance query.
Admission callers must freeze both hashes, reread the same private evidence at
claim/send, and supply current shared-ledger occupancy and spending since the
snapshot. A successful check grants no dispatch right and no model permission.
"""

import argparse
import getpass
import hashlib
import json
import os
import re
import stat
import warnings
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, DecimalException, Inexact, localcontext
from pathlib import Path
from typing import Literal
from uuid import UUID

from dotenv import dotenv_values
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    ValidationInfo,
    field_validator,
)

from nexus.ops import openai_local

PLATFORM_KEY_PATH = Path(".secrets/nexus-platform-local/api-key")
PLATFORM_ENDPOINT = "https://api.openai.com/v1/responses"
PLATFORM_PURPOSE = "platform-gifted-terra-v1-20261031"
PLATFORM_RENEWED_PURPOSE = "platform-terra-v2-20261101"
PLATFORM_THIRD_PURPOSE = "platform-terra-v3-20261101"
PLATFORM_QUALIFICATION_PURPOSE = "platform-qualification-v1"
EXPLICIT_SPEND_DEADLINE = datetime(2026, 11, 1, tzinfo=UTC)
RENEWED_EXPLICIT_SPEND_DEADLINE = datetime(2026, 11, 2, tzinfo=UTC)
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_ERRORS = frozenset(
    {
        "PLATFORM_CREDENTIAL_INVALID",
        "PLATFORM_CREDENTIAL_MISSING",
        "PLATFORM_CREDENTIAL_BINDING_MISMATCH",
        "PLATFORM_DESTINATION_MISMATCH",
        "PLATFORM_PROOF_INVALID",
        "PLATFORM_PROOF_UNSAFE",
        "PLATFORM_PROOF_BINDING_MISMATCH",
        "PLATFORM_FUNDING_UNQUALIFIED",
        "PLATFORM_WINDOW_INVALID",
        "PLATFORM_BALANCE_INSUFFICIENT",
        "PLATFORM_SPEND_LIMIT_EXCEEDED",
        "PLATFORM_OCCUPANCY_INVALID",
        "PLATFORM_KEY_ALREADY_EXISTS",
        "PLATFORM_KEY_SAVE_FAILED",
        "PLATFORM_KEY_ENTRY_CANCELLED",
        "PLATFORM_KEY_REQUIRES_LOCAL_TERMINAL",
        "PLATFORM_REPOSITORY_ROOT_REQUIRED",
        "PLATFORM_CHECK_FAILED",
    }
)


class PlatformCredentialError(ValueError):
    def __init__(self, code: str):
        super().__init__(code if code in _ERRORS else "PLATFORM_CHECK_FAILED")


def _reject(code: str) -> None:
    raise PlatformCredentialError(code)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def credential_fingerprint(key: str) -> str:
    if not isinstance(key, str) or not key or len(key) > 1024 or any(c.isspace() for c in key):
        _reject("PLATFORM_CREDENTIAL_INVALID")
    return _sha256(b"nexus-platform-key-v1\0" + key.encode("utf-8"))


@dataclass(frozen=True)
class PlatformCredential:
    api_key: SecretStr = field(repr=False)
    fingerprint: str
    organization_id: str | None = field(repr=False)
    project_id: str | None = field(repr=False)
    endpoint: str = PLATFORM_ENDPOINT
    credential_kind: str = "platform_project_key"
    key_scope_only: bool = False

    @property
    def binding_hash(self) -> str:
        return _sha256(
            json.dumps(
                {
                    "credential_kind": self.credential_kind,
                    "fingerprint": self.fingerprint,
                    "organization_id": self.organization_id,
                    "project_id": self.project_id,
                    "endpoint": self.endpoint,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )

    def request_headers(self) -> dict[str, str]:
        """Only the final HTTP sender consumes these; never log this dictionary."""
        _validate_credential(self)
        if self.key_scope_only:
            return {"Authorization": "Bearer " + self.api_key.get_secret_value()}
        assert self.organization_id is not None and self.project_id is not None
        return {
            "Authorization": "Bearer " + self.api_key.get_secret_value(),
            "OpenAI-Organization": self.organization_id,
            "OpenAI-Project": self.project_id,
        }


def _validate_credential(credential: PlatformCredential) -> None:
    scope_valid = (
        credential.organization_id is None and credential.project_id is None
        if credential.key_scope_only
        else isinstance(credential.organization_id, str)
        and isinstance(credential.project_id, str)
        and bool(_IDENTITY.fullmatch(credential.organization_id))
        and bool(_IDENTITY.fullmatch(credential.project_id))
    )
    if (
        credential.credential_kind != "platform_project_key"
        or credential.endpoint != PLATFORM_ENDPOINT
        or not scope_valid
        or credential.fingerprint != credential_fingerprint(credential.api_key.get_secret_value())
    ):
        _reject("PLATFORM_CREDENTIAL_BINDING_MISMATCH")


def _resolved_key(root: Path) -> SecretStr:
    try:
        settings = openai_local.local_settings(
            root, backend="responses", key_path=PLATFORM_KEY_PATH
        )
    except Exception:
        raise PlatformCredentialError("PLATFORM_CREDENTIAL_INVALID") from None
    if settings.openai_api_key is None:
        _reject("PLATFORM_CREDENTIAL_MISSING")
    assert settings.openai_api_key is not None
    credential_fingerprint(settings.openai_api_key.get_secret_value())
    return settings.openai_api_key


def resolve_platform_credential(
    root: Path,
    *,
    expected_fingerprint: str,
    organization_id: str | None,
    project_id: str | None,
    endpoint: str = PLATFORM_ENDPOINT,
    key_scope_only: bool = False,
) -> PlatformCredential:
    """Resolve the actual key, including normal .env/process overrides, then bind it."""
    if endpoint != PLATFORM_ENDPOINT:
        _reject("PLATFORM_DESTINATION_MISMATCH")
    key = _resolved_key(root)
    actual = credential_fingerprint(key.get_secret_value())
    if actual != expected_fingerprint:
        _reject("PLATFORM_CREDENTIAL_BINDING_MISMATCH")
    environment = {
        name: value for name, value in dotenv_values(root / "infra/.env.local").items() if value
    }
    environment.update({name: value for name, value in os.environ.items() if value})
    if key_scope_only and any(
        environment.get(name)
        for name in ("OPENAI_ORG_ID", "OPENAI_ORGANIZATION", "OPENAI_PROJECT_ID", "OPENAI_PROJECT")
    ):
        _reject("PLATFORM_CREDENTIAL_BINDING_MISMATCH")
    expected = {
        "OPENAI_BASE_URL": "https://api.openai.com/v1",
        "OPENAI_API_BASE": "https://api.openai.com/v1",
        "OPENAI_ORG_ID": organization_id,
        "OPENAI_ORGANIZATION": organization_id,
        "OPENAI_PROJECT_ID": project_id,
        "OPENAI_PROJECT": project_id,
    }
    if any(environment.get(name, value) != value for name, value in expected.items()):
        _reject("PLATFORM_DESTINATION_MISMATCH")
    result = PlatformCredential(
        key, actual, organization_id, project_id, endpoint, key_scope_only=key_scope_only
    )
    _validate_credential(result)
    return result


class _FundingRecord(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True, allow_inf_nan=False)
    version: int = Field(ge=1, le=1)
    kind: Literal["manual_platform_console"]
    purpose: str
    endpoint: str
    credential_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    organization_id: str = Field(repr=False)
    project_id: str = Field(repr=False)
    key_ownership_confirmed: bool
    gift_claim_active: bool
    gift_balance_usd: Decimal = Field(ge=0, repr=False)
    gift_expires_at: AwareDatetime = Field(repr=False)
    purchased_balance_usd: Decimal = Field(ge=0, repr=False)
    automatic_recharge_enabled: bool = Field(repr=False)
    other_paid_fallback_enabled: bool = Field(repr=False)
    organization_exclusive_window_confirmed: bool
    other_usage_bounded: bool
    other_usage_upper_bound_usd: Decimal = Field(ge=0, repr=False)
    inflight_usage_bounded: bool
    unknown_inflight_present: bool
    unreflected_usage_upper_bound_usd: Decimal = Field(ge=0, repr=False)
    verified_at: AwareDatetime
    window_start: AwareDatetime
    window_end: AwareDatetime
    console_evidence_file: str = Field(repr=False)
    console_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ExplicitPlatformSpendAuthorization(BaseModel):
    """Exact local record of an already verified user instruction, never a balance."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True, allow_inf_nan=False)
    version: int = Field(ge=1, le=3)
    kind: Literal["explicit_platform_spend_authorization"]
    purpose: str
    endpoint: str
    credential_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    organization_id: None = None
    project_id: None = None
    experiment_id: UUID
    input_hashes: tuple[str, str, str]
    max_total_usd: Decimal = Field(gt=0, le=Decimal("6.42"))
    authorized_at: AwareDatetime
    window_start: AwareDatetime
    window_end: AwareDatetime
    authorization_file: str = Field(repr=False)
    authorization_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("window_end")
    @classmethod
    def enforce_fixed_deadline(cls, value: datetime, info: ValidationInfo) -> datetime:
        deadline = (
            RENEWED_EXPLICIT_SPEND_DEADLINE
            if info.data.get("version") in (2, 3)
            else EXPLICIT_SPEND_DEADLINE
        )
        if value > deadline:
            raise ValueError("PLATFORM_AUTHORIZATION_DEADLINE_EXCEEDED")
        return value


class PlatformQualificationBudget(BaseModel):
    """Frozen operating bound under one retained task policy; not consent or credit."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True, allow_inf_nan=False)
    version: Literal[1]
    kind: Literal["platform_qualification_budget"]
    purpose: Literal["platform-qualification-v1"]
    endpoint: str
    credential_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    organization_id: None = None
    project_id: None = None
    experiment_id: UUID
    input_hashes: tuple[str, str, str]
    configuration_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    units_version: str
    max_total_usd: Decimal = Field(gt=0, le=Decimal("6.42"))
    registered_at: AwareDatetime
    window_start: AwareDatetime
    window_end: AwareDatetime
    task_policy_file: str = Field(repr=False)
    task_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    continuation_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class PlatformFundingProof:
    record: _FundingRecord | ExplicitPlatformSpendAuthorization | PlatformQualificationBudget = (
        field(repr=False)
    )
    proof_hash: str

    @property
    def valid_until(self) -> datetime:
        if isinstance(
            self.record, ExplicitPlatformSpendAuthorization | PlatformQualificationBudget
        ):
            return self.record.window_end
        return min(self.record.window_end, self.record.gift_expires_at)


def _private_bytes(path: Path, limit: int) -> bytes:
    try:
        if path.resolve() != path.absolute():
            _reject("PLATFORM_PROOF_UNSAFE")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
                or info.st_nlink != 1
                or info.st_size > limit
            ):
                _reject("PLATFORM_PROOF_UNSAFE")
            content = stream.read(limit + 1)
        if len(content) > limit:
            _reject("PLATFORM_PROOF_UNSAFE")
        return content
    except PlatformCredentialError:
        raise
    except Exception:
        raise PlatformCredentialError("PLATFORM_PROOF_UNSAFE") from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _reject("PLATFORM_PROOF_INVALID")
        result[key] = value
    return result


def load_platform_funding_proof(path: Path) -> PlatformFundingProof:
    """Read a private gift attestation or an explicit, experiment-bound authorization."""
    raw = _private_bytes(path, 65536)
    try:
        payload = json.loads(raw, object_pairs_hook=_unique_object)
        if not isinstance(payload, dict):
            _reject("PLATFORM_PROOF_INVALID")
        if payload.get("kind") in (
            "explicit_platform_spend_authorization",
            "platform_qualification_budget",
        ):
            if not isinstance(payload.get("max_total_usd"), str):
                _reject("PLATFORM_PROOF_INVALID")
            authorization = (
                PlatformQualificationBudget.model_validate_json(raw)
                if payload["kind"] == "platform_qualification_budget"
                else ExplicitPlatformSpendAuthorization.model_validate_json(raw)
            )
            if not all(_HASH.fullmatch(value) for value in authorization.input_hashes):
                _reject("PLATFORM_PROOF_INVALID")
            name = (
                authorization.task_policy_file
                if isinstance(authorization, PlatformQualificationBudget)
                else authorization.authorization_file
            )
            if not name or Path(name).name != name or name in {".", ".."}:
                _reject("PLATFORM_PROOF_INVALID")
            original = _private_bytes(path.parent / name, 16 * 1024 * 1024)
            original_hash = (
                authorization.task_policy_sha256
                if isinstance(authorization, PlatformQualificationBudget)
                else authorization.authorization_sha256
            )
            if not original.strip() or _sha256(original) != original_hash:
                _reject("PLATFORM_PROOF_INVALID")
            return PlatformFundingProof(authorization, _sha256(raw))
        if any(
            not isinstance(payload.get(name), str)
            for name in (
                "gift_balance_usd",
                "purchased_balance_usd",
                "other_usage_upper_bound_usd",
                "unreflected_usage_upper_bound_usd",
            )
        ):
            _reject("PLATFORM_PROOF_INVALID")
        record = _FundingRecord.model_validate_json(raw)
        name = record.console_evidence_file
        if not name or Path(name).name != name or name in {".", ".."}:
            _reject("PLATFORM_PROOF_INVALID")
        original = _private_bytes(path.parent / name, 16 * 1024 * 1024)
        if not original.strip() or _sha256(original) != record.console_evidence_sha256:
            _reject("PLATFORM_PROOF_INVALID")
        return PlatformFundingProof(record, _sha256(raw))
    except PlatformCredentialError:
        raise
    except Exception:
        raise PlatformCredentialError("PLATFORM_PROOF_INVALID") from None


@dataclass(frozen=True)
class PlatformEligibility:
    funding_proof_hash: str
    credential_binding_hash: str
    purpose: str
    endpoint: str
    valid_until: datetime
    available_gift_usd: Decimal | None = field(repr=False)


def _money(value: Decimal) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value >= 0


def check_platform_eligibility(
    credential: PlatformCredential,
    proof: PlatformFundingProof,
    *,
    purpose: str,
    now: datetime,
    required_usd: Decimal,
    local_held_usd: Decimal,
    local_spent_since_proof_usd: Decimal,
    required_valid_until: datetime,
    experiment_id: UUID | None = None,
    input_hashes: tuple[str, ...] | None = None,
) -> PlatformEligibility:
    """Pure qualification. Occupancy must come from the shared ledger, never this file.

    Held includes unknown/in-flight reservations. Spent is the confirmed local
    spend since verified_at: settlement never replenishes this manual snapshot.
    Use the returned credential binding with the exact same key and HTTP headers.
    """
    _validate_credential(credential)
    record = proof.record
    if (
        purpose
        not in (
            PLATFORM_PURPOSE,
            PLATFORM_RENEWED_PURPOSE,
            PLATFORM_THIRD_PURPOSE,
            PLATFORM_QUALIFICATION_PURPOSE,
        )
        or record.purpose != purpose
        or record.endpoint != credential.endpoint
        or record.credential_fingerprint != credential.fingerprint
        or record.organization_id != credential.organization_id
        or record.project_id != credential.project_id
        or not _HASH.fullmatch(proof.proof_hash)
    ):
        _reject("PLATFORM_PROOF_BINDING_MISMATCH")
    if purpose == PLATFORM_THIRD_PURPOSE and not isinstance(
        record, ExplicitPlatformSpendAuthorization
    ):
        _reject("PLATFORM_PROOF_BINDING_MISMATCH")
    if (purpose == PLATFORM_QUALIFICATION_PURPOSE) != isinstance(
        record, PlatformQualificationBudget
    ):
        _reject("PLATFORM_PROOF_BINDING_MISMATCH")
    if isinstance(record, ExplicitPlatformSpendAuthorization | PlatformQualificationBudget):
        if (
            isinstance(record, ExplicitPlatformSpendAuthorization)
            and record.version
            != {PLATFORM_PURPOSE: 1, PLATFORM_RENEWED_PURPOSE: 2, PLATFORM_THIRD_PURPOSE: 3}.get(
                purpose
            )
            or not credential.key_scope_only
            or (record.experiment_id != experiment_id or record.input_hashes != input_hashes)
        ):
            _reject("PLATFORM_PROOF_BINDING_MISMATCH")
        if (
            not isinstance(now, datetime)
            or now.utcoffset() is None
            or not isinstance(required_valid_until, datetime)
            or required_valid_until.utcoffset() is None
            or not record.window_start
            <= (
                record.registered_at
                if isinstance(record, PlatformQualificationBudget)
                else record.authorized_at
            )
            <= now
            < required_valid_until
            <= record.window_end
        ):
            _reject("PLATFORM_WINDOW_INVALID")
        if not all(
            _money(value) for value in (required_usd, local_held_usd, local_spent_since_proof_usd)
        ):
            _reject("PLATFORM_OCCUPANCY_INVALID")
        if required_usd > record.max_total_usd:
            _reject("PLATFORM_SPEND_LIMIT_EXCEEDED")
        return PlatformEligibility(
            proof.proof_hash,
            credential.binding_hash,
            purpose,
            credential.endpoint,
            required_valid_until,
            None,
        )
    if credential.key_scope_only:
        _reject("PLATFORM_PROOF_BINDING_MISMATCH")
    if (
        not record.key_ownership_confirmed
        or not record.gift_claim_active
        or record.purchased_balance_usd != 0
        or record.automatic_recharge_enabled
        or record.other_paid_fallback_enabled
        or not record.organization_exclusive_window_confirmed
        or not record.other_usage_bounded
        or not record.inflight_usage_bounded
        or (record.unknown_inflight_present and record.unreflected_usage_upper_bound_usd <= 0)
    ):
        _reject("PLATFORM_FUNDING_UNQUALIFIED")
    if (
        not isinstance(now, datetime)
        or now.utcoffset() is None
        or not isinstance(required_valid_until, datetime)
        or required_valid_until.utcoffset() is None
        or not (
            record.window_start
            <= record.verified_at
            <= now
            < required_valid_until
            <= record.window_end
            <= record.gift_expires_at
        )
    ):
        _reject("PLATFORM_WINDOW_INVALID")
    if (
        not _money(required_usd)
        or not _money(local_held_usd)
        or not _money(local_spent_since_proof_usd)
    ):
        _reject("PLATFORM_OCCUPANCY_INVALID")
    try:
        with localcontext() as context:
            context.traps[Inexact] = True
            available = (
                record.gift_balance_usd
                - record.other_usage_upper_bound_usd
                - record.unreflected_usage_upper_bound_usd
                - local_held_usd
                - local_spent_since_proof_usd
            )
    except DecimalException:
        raise PlatformCredentialError("PLATFORM_OCCUPANCY_INVALID") from None
    if available < required_usd:
        _reject("PLATFORM_BALANCE_INSUFFICIENT")
    return PlatformEligibility(
        proof.proof_hash,
        credential.binding_hash,
        purpose,
        credential.endpoint,
        record.window_end,
        available,
    )


def _now() -> datetime:
    return datetime.now(UTC)


def _save_platform_key(root: Path) -> None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            openai_local._save_key(root, key_path=PLATFORM_KEY_PATH)
    except FileExistsError:
        raise PlatformCredentialError("PLATFORM_KEY_ALREADY_EXISTS") from None
    except KeyboardInterrupt:
        raise PlatformCredentialError("PLATFORM_KEY_ENTRY_CANCELLED") from None
    except Exception as error:
        code = (
            "PLATFORM_KEY_REQUIRES_LOCAL_TERMINAL"
            if type(error) is ValueError and str(error) == "OPENAI_KEY_REQUIRES_LOCAL_TERMINAL"
            else "PLATFORM_KEY_SAVE_FAILED"
        )
        raise PlatformCredentialError(code) from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser(
        "save-key", help="Hidden local input only; never verify a model or overwrite a key"
    )
    sub.add_parser(
        "fingerprint", help="Identify the actually resolved credential; no qualification"
    )
    check = sub.add_parser(
        "check", help="Check private manual evidence; does not dispatch or write DB"
    )
    check.add_argument("--proof-file", type=Path, required=True)
    check.add_argument("--required-usd", required=True)
    check.add_argument("--local-held-usd", required=True)
    check.add_argument("--local-spent-since-proof-usd", required=True)
    check.add_argument("--valid-until", required=True)
    check.add_argument("--experiment-id", type=UUID)
    check.add_argument("--input-hash", action="append")
    args = parser.parse_args()
    try:
        root = Path.cwd()
        if not (root / "engine/pyproject.toml").is_file():
            _reject("PLATFORM_REPOSITORY_ROOT_REQUIRED")
        result: dict[str, object] = {
            "dispatch_authority": False,
            "model_requests": 0,
            "database_writes": 0,
            "provider_requests": 0,
            "execution_locked": True,
        }
        if args.action == "save-key":
            _save_platform_key(root)
            result["status"] = "PLATFORM_KEY_SAVED"
        elif args.action == "fingerprint":
            result.update(
                status="PLATFORM_KEY_RESOLVED",
                credential_fingerprint=credential_fingerprint(
                    _resolved_key(root).get_secret_value()
                ),
                endpoint=PLATFORM_ENDPOINT,
                credential_kind="platform_project_key",
            )
        else:
            proof = load_platform_funding_proof(args.proof_file)
            record = proof.record
            credential = resolve_platform_credential(
                root,
                expected_fingerprint=record.credential_fingerprint,
                organization_id=record.organization_id,
                project_id=record.project_id,
                endpoint=record.endpoint,
                key_scope_only=isinstance(
                    record, ExplicitPlatformSpendAuthorization | PlatformQualificationBudget
                ),
            )
            eligibility = check_platform_eligibility(
                credential,
                proof,
                purpose=proof.record.purpose,
                now=_now(),
                required_usd=Decimal(args.required_usd),
                local_held_usd=Decimal(args.local_held_usd),
                local_spent_since_proof_usd=Decimal(args.local_spent_since_proof_usd),
                required_valid_until=datetime.fromisoformat(args.valid_until),
                experiment_id=args.experiment_id,
                input_hashes=tuple(args.input_hash) if args.input_hash is not None else None,
            )
            result.update(
                status="PLATFORM_ELIGIBLE_AT_CHECK",
                funding_proof_hash=eligibility.funding_proof_hash,
                credential_binding_hash=eligibility.credential_binding_hash,
            )
            if isinstance(record, ExplicitPlatformSpendAuthorization | PlatformQualificationBudget):
                result.update(funding_kind=record.kind, balance_verified=False)
        print(json.dumps(result))
        return 0
    except (Exception, KeyboardInterrupt) as error:
        code = str(error) if isinstance(error, PlatformCredentialError) else "PLATFORM_CHECK_FAILED"
        print(json.dumps({"status": "blocked", "code": code, "execution_locked": True}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
