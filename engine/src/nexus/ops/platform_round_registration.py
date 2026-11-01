"""Freeze one stable qualification identity; registration never grants a send."""

import argparse
import base64
import fcntl
import hashlib
import json
import os
import stat
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.agents.comparison_manifest import (
    PLATFORM_QUALIFICATION_PURPOSE,
    parse_comparison_manifest,
)
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.comparison_registration import register_comparison
from nexus.agents.platform_manifest import QualificationPlatformManifest
from nexus.agents.platform_profile import platform_model_fields, platform_profile_hash
from nexus.agents.platform_schema import CURRENT_PLATFORM_UNITS_VERSION
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
from nexus.ops.platform_credentials import (
    PlatformCredentialError,
    PlatformQualificationBudget,
    _resolved_key,
    credential_fingerprint,
    resolve_platform_credential,
)
from nexus.ops.platform_credentials import (
    _private_bytes as _read_private_bytes,
)

_PUBLICATION_LOCK = ".round-publication.lock"


@contextmanager
def _publication_lock(directory: Path, *, shared: bool = False) -> Iterator[None]:
    # Only file publication/readback is serialized, never DB or provider work.
    flags = os.O_RDONLY if shared else os.O_RDWR | os.O_CREAT
    descriptor = os.open(directory / _PUBLICATION_LOCK, flags | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or info.st_nlink != 1
        ):
            raise PlatformCredentialError("PLATFORM_PROOF_UNSAFE")
        deadline = time.monotonic() + 5
        while True:
            try:
                fcntl.flock(
                    descriptor, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB
                )
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ValueError("PLATFORM_ROUND_PUBLICATION_BUSY") from None
                time.sleep(0.01)
        yield
    finally:
        os.close(descriptor)


def _private_bytes(path: Path, limit: int) -> bytes:
    lock = path.parent / _PUBLICATION_LOCK
    if lock.exists() or lock.is_symlink():
        with _publication_lock(path.parent, shared=True):
            try:
                return _read_private_bytes(path, limit)
            except PlatformCredentialError:
                pass
        try:
            with _publication_lock(path.parent):
                _recover_publication(path, limit)
                return _read_private_bytes(path, limit)
        except OSError:
            raise PlatformCredentialError("PLATFORM_PROOF_UNSAFE") from None
    return _read_private_bytes(path, limit)


def _wire(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _publication_prefix(path: Path, raw: bytes) -> str:
    return f".round-{path.name}-{hashlib.sha256(raw).hexdigest()}-"


def _recover_publication(path: Path, limit: int) -> None:
    """Under the exclusive lock, remove only our exact crashed publication alias."""
    if path.resolve() != path.absolute():
        raise PlatformCredentialError("PLATFORM_PROOF_UNSAFE")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if info.st_nlink == 1:
            return  # The ordinary private reader still performs every check.
        directory = path.parent.stat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or info.st_nlink != 2
            or info.st_size > limit
            or directory.st_uid != os.getuid()
            or directory.st_mode & 0o077
        ):
            raise PlatformCredentialError("PLATFORM_PROOF_UNSAFE")
        raw = stream.read(limit + 1)
        if len(raw) != info.st_size:
            raise PlatformCredentialError("PLATFORM_PROOF_UNSAFE")
        aliases = []
        for candidate in path.parent.glob(_publication_prefix(path, raw) + "*"):
            candidate_info = candidate.lstat()
            if stat.S_ISREG(candidate_info.st_mode) and (
                candidate_info.st_dev,
                candidate_info.st_ino,
            ) == (info.st_dev, info.st_ino):
                aliases.append(candidate)
        if len(aliases) != 1:
            raise PlatformCredentialError("PLATFORM_PROOF_UNSAFE")
        aliases[0].unlink()
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        if os.fstat(stream.fileno()).st_nlink != 1:
            raise PlatformCredentialError("PLATFORM_PROOF_UNSAFE")


def _publish(path: Path, raw: bytes) -> bool:
    """Exclusive durable publication, without overwriting a concurrent winner."""
    descriptor, name = tempfile.mkstemp(prefix=_publication_prefix(path, raw), dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        with _publication_lock(path.parent):
            try:
                os.link(name, path, follow_symlinks=False)
            except FileExistsError:
                _recover_publication(path, 32 * 1024 * 1024)
                return False
            finally:
                os.unlink(name)
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        return True
    except BaseException:
        if os.path.exists(name):
            os.unlink(name)
        raise


def _export(path: Path, raw: bytes) -> None:
    if not _publish(path, raw) and _private_bytes(path, 32 * 1024 * 1024) != raw:
        raise ValueError("PLATFORM_ROUND_FILE_CONFLICT")


def _durable_directory(path: Path, root: Path) -> None:
    """Persist each directory name before any attempt can reach the database."""
    parent = root
    for part in path.relative_to(root).parts:
        child = parent / part
        child.mkdir(mode=0o700, exist_ok=True)
        descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            # Repeat on existing directories too: a previous process may have
            # created the name and exited before syncing its parent.
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        parent = child


def _freeze(args: argparse.Namespace, now: datetime, request: dict[str, str]) -> dict[str, Any]:
    source = _private_bytes(args.source.absolute(), 8 * 1024 * 1024)
    template_bytes = _private_bytes(args.template.absolute(), 65536)
    if hashlib.sha256(template_bytes).hexdigest() != request["template_sha256"]:
        raise ValueError("PLATFORM_ROUND_TEMPLATE_CHANGED")
    template = parse_comparison_manifest(json.loads(template_bytes))
    prepared = prepare_archive(
        source,
        expected_source_hash=args.source_sha256,
        experiment_id=args.experiment_id,
        purpose=PLATFORM_QUALIFICATION_PURPOSE,
        platform_price_version=CURRENT_PLATFORM_PRICE_VERSION,
        platform_units_version=CURRENT_PLATFORM_UNITS_VERSION,
    )
    prepared["rebuilt_at"] = now.isoformat()
    fields = platform_model_fields(CURRENT_PLATFORM_PRICE_VERSION)
    if (
        not cast(datetime, fields["pricing_available_at"])
        <= now
        < now + timedelta(seconds=990)
        < cast(datetime, fields["pricing_expires_at"])
    ):
        raise ValueError("PLATFORM_PRICE_WINDOW_INVALID")
    credential = resolve_platform_credential(
        args.repo_root,
        expected_fingerprint=credential_fingerprint(
            _resolved_key(args.repo_root).get_secret_value()
        ),
        organization_id=None,
        project_id=None,
        key_scope_only=True,
    )
    data = template.model_dump(mode="json")
    data.update(
        {
            k: prepared[k]
            for k in (
                "experiment_id",
                "source_manifest_hash",
                "identity_equivalence_hash",
                "outbound_safety_hash",
                "input_hashes",
                "instrument_ids",
                "allocations",
            )
        }
    )
    data.update(
        platform_contract=PLATFORM_QUALIFICATION_PURPOSE,
        implementation_sha=args.implementation_sha,
        api_config_hash=platform_profile_hash(CURRENT_PLATFORM_PRICE_VERSION),
        funding_proof_hash="0" * 64,
        credential_binding_hash=credential.binding_hash,
    )
    draft = QualificationPlatformManifest.model_validate(data)
    budget = PlatformQualificationBudget.model_validate_json(
        _wire(
            dict(
                version=1,
                kind="platform_qualification_budget",
                purpose=PLATFORM_QUALIFICATION_PURPOSE,
                endpoint=credential.endpoint,
                credential_fingerprint=credential.fingerprint,
                experiment_id=str(args.experiment_id),
                input_hashes=prepared["input_hashes"],
                configuration_hash=draft.api_config_hash,
                units_version=CURRENT_PLATFORM_UNITS_VERSION,
                max_total_usd=str(draft.total_reservation_usd),
                registered_at=now.isoformat(),
                window_start=now.isoformat(),
                window_end=cast(datetime, fields["pricing_expires_at"]).isoformat(),
                task_policy_file="task-policy.json",
                task_policy_sha256=request["task_policy_sha256"],
                continuation_evidence_sha256=request["continuation_evidence_sha256"],
            )
        )
    )
    budget_data = budget.model_dump(mode="json")
    data["funding_proof_hash"] = hashlib.sha256(_wire(budget_data)).hexdigest()
    manifest = QualificationPlatformManifest.model_validate(data)
    return dict(
        request=request,
        manifest=manifest.model_dump(mode="json"),
        prepared=prepared,
        budget=budget_data,
        policy=base64.b64encode(
            _private_bytes(args.task_policy.absolute(), 16 * 1024 * 1024)
        ).decode(),
    )


async def register_round(
    factory: async_sessionmaker[AsyncSession], args: argparse.Namespace
) -> dict[str, Any]:
    """A persisted attempt is read-only on reentry, including an unconfirmed commit."""
    from nexus.agents.research_maintenance import require_research_open

    require_research_open()
    policy = _private_bytes(args.task_policy.absolute(), 16 * 1024 * 1024)
    if hashlib.sha256(policy).hexdigest() != args.task_policy_sha256:
        raise ValueError("PLATFORM_PROOF_BINDING_MISMATCH")
    request = dict(
        experiment_id=str(args.experiment_id),
        source_sha256=args.source_sha256,
        template_sha256=hashlib.sha256(_private_bytes(args.template.absolute(), 65536)).hexdigest(),
        implementation_sha=args.implementation_sha,
        task_policy_sha256=args.task_policy_sha256,
        continuation_evidence_sha256=hashlib.sha256(
            _private_bytes(args.continuation_evidence.absolute(), 16 * 1024 * 1024)
        ).hexdigest(),
    )
    output = args.output.absolute()
    if output.resolve() != output:
        raise ValueError("PLATFORM_PROOF_UNSAFE")
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    # --output only chooses an export. An identity cannot gain a fresh attempt
    # by changing that directory after an unknown transaction result.
    authority = (
        args.repo_root.resolve()
        / ".secrets/nexus-platform-local/qualification-rounds"
        / str(args.experiment_id)
    )
    if authority.resolve() != authority:
        raise ValueError("PLATFORM_PROOF_UNSAFE")
    _durable_directory(authority, args.repo_root.resolve())
    bundle_path = authority / "round.json"
    attempt_path = authority / "registration-attempt.json"
    if not bundle_path.exists():
        if attempt_path.exists() or attempt_path.is_symlink():
            raise ValueError("PLATFORM_ROUND_LOCAL_BUNDLE_REQUIRED")
        # No manifest exists before the immutable local bundle is durable.
        async with factory() as session:
            if await session.scalar(
                text("SELECT 1 FROM research_comparison_manifest WHERE id=:id"),
                {"id": args.experiment_id},
            ):
                raise ValueError("PLATFORM_ROUND_LOCAL_BUNDLE_REQUIRED")
            now = await session.scalar(text("SELECT clock_timestamp()"))
        bundle = _freeze(args, now, request)
        _publish(bundle_path, _wire(bundle))
    bundle = json.loads(_private_bytes(bundle_path, 32 * 1024 * 1024))
    if bundle["request"] != request:
        raise ValueError("COMPARISON_REGISTRATION_CONFLICT")
    manifest = parse_comparison_manifest(bundle["manifest"])
    budget = PlatformQualificationBudget.model_validate_json(_wire(bundle["budget"]))
    policy_bytes = base64.b64decode(bundle["policy"], validate=True)
    if (
        manifest.purpose != PLATFORM_QUALIFICATION_PURPOSE
        or manifest.experiment_id != args.experiment_id
        or manifest.source_manifest_hash != args.source_sha256
        or manifest.implementation_sha != args.implementation_sha
        or not isinstance(manifest, QualificationPlatformManifest)
        or manifest.funding_proof_hash != hashlib.sha256(_wire(bundle["budget"])).hexdigest()
        or budget.experiment_id != manifest.experiment_id
        or budget.input_hashes != manifest.input_hashes
        or budget.configuration_hash != manifest.api_config_hash
        or budget.units_version != manifest.allocations[0].units_version
        or budget.max_total_usd != manifest.total_reservation_usd
        or budget.task_policy_file != "task-policy.json"
        or budget.task_policy_sha256 != request["task_policy_sha256"]
        or budget.continuation_evidence_sha256 != request["continuation_evidence_sha256"]
        or hashlib.sha256(policy_bytes).hexdigest() != budget.task_policy_sha256
    ):
        raise ValueError("PLATFORM_ROUND_BUNDLE_CONFLICT")
    _export(output / "round.json", _wire(bundle))
    for name, value in (
        ("manifest.json", bundle["manifest"]),
        ("prepared.json", bundle["prepared"]),
        ("funding.json", bundle["budget"]),
    ):
        _export(output / name, _wire(value))
    _export(output / "task-policy.json", base64.b64decode(bundle["policy"], validate=True))
    attempt_wire = _wire(
        dict(experiment_id=str(manifest.experiment_id), manifest_hash=manifest.digest)
    )
    attempt = _publish(attempt_path, attempt_wire)
    if _private_bytes(attempt_path, 65536) != attempt_wire:
        raise ValueError("PLATFORM_ROUND_BUNDLE_CONFLICT")
    if attempt:
        async with factory() as session:
            transaction = await session.begin()
            try:
                await register_comparison(session, manifest, allow_new_round=True)
            except BaseException:
                await transaction.rollback()
                raise
            try:
                await transaction.commit()
            except Exception:
                # Never retry the INSERT or infer rollback from a single absence.
                pass
    return await inspect_round(factory, manifest.experiment_id, expected=manifest)


async def inspect_round(
    factory: async_sessionmaker[AsyncSession],
    experiment_id: UUID,
    *,
    expected: QualificationPlatformManifest | None = None,
) -> dict[str, Any]:
    """Read immutable facts without requiring an unexpired policy, Key or local bundle."""
    async with factory() as session:
        row = (
            (
                await session.execute(
                    text("""SELECT m.manifest_hash,m.payload,s.stopped
          FROM research_comparison_manifest m LEFT JOIN research_stage_scope s ON s.id=m.id
          WHERE m.id=:id"""),
                    {"id": experiment_id},
                )
            )
            .mappings()
            .one_or_none()
        )
    async with factory() as session:
        positions = (
            (
                await session.execute(
                    text("""SELECT c.ordinal,b.state
          FROM research_comparison_chain c
          JOIN agent_daily_budget_reservations b ON b.run_id=c.run_id
          WHERE c.experiment_id=:id ORDER BY c.ordinal"""),
                    {"id": experiment_id},
                )
            )
            .mappings()
            .all()
        )
    manifest = parse_comparison_manifest(row["payload"]) if row is not None else expected
    if row is not None and (
        manifest is None
        or manifest.experiment_id != experiment_id
        or row["manifest_hash"] != manifest.digest
        or expected is not None
        and manifest.model_dump(mode="json") != expected.model_dump(mode="json")
    ):
        raise ValueError("COMPARISON_REGISTRATION_CONFLICT")
    return dict(
        status="registered" if row is not None else "registration_unconfirmed",
        experiment_id=str(experiment_id),
        manifest_hash=None if manifest is None else manifest.digest,
        inspection_only=True,
        replay_allowed=False,
        stopped=None if row is None else row["stopped"],
        budget_status=(
            "not_observed"
            if manifest is None
            else "native_positions_present"
            if positions
            else "bound_frozen_positions_unreserved"
        ),
        position_count=len(positions),
        reserved_positions=sum(item["state"] == "reserved" for item in positions),
        dispatch_authority=False,
        model_calls=0,
    )
