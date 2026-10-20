"""One independent synthesis observation over immutable real research, never execution."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

from nexus.agents.codex_input_view import project_portfolio_payload
from nexus.agents.codex_sdk import (
    CodexInvocationError,
    InvocationPhase,
    codex_request_hash,
    codex_request_wire,
    invoke_codex,
)
from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.sdk_stream_progress import SdkStreamObserver
from nexus.ops.historical_nvda_observation import check_current_configuration
from nexus.ops.historical_pool_diagnostic import verify_results
from nexus.ops.research_budget_diagnostic import _private_directory
from nexus.paper.historical_assessment_source import read_archive

PROFILE = "independent-synthesis-observation-v1"
LIMITS = dict(
    sdk_total_seconds=90,
    sdk_work_seconds=83,
    cleanup_seconds=7,
    provider_calls=1,
    input_tokens=64000,
    output_tokens=8192,
    max_wire_bytes=64000,
    execution_allowed=False,
)
SAFE_CODES = frozenset(
    {
        "OBSERVATION_ALREADY_EXISTS",
        "OBSERVATION_ALREADY_ATTEMPTED",
        "OBSERVATION_SOURCE_CHANGED",
        "OBSERVATION_IDENTITY_INVALID",
        "OBSERVATION_NOT_COMPLETE",
        "OBSERVATION_BUDGET_INVALID",
        "OBSERVATION_SCHEMA_INVALID",
        "OBSERVATION_BINDING_INVALID",
        "OBSERVATION_NONEXECUTING_REQUIRED",
        "OBSERVATION_DEADLINE",
        "OBSERVATION_CANCELLED",
        "OBSERVATION_UNKNOWN",
        "HISTORICAL_CASE_V11_REQUIRED",
        "HISTORICAL_CASE_BUDGET_UNAVAILABLE",
        "DIAGNOSTIC_DIRECTORY_UNSAFE",
        "OBSERVATION_FILE_UNSAFE",
    }
)


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _code_hash() -> str:
    # Bind the complete installed local implementation, including SDK and all validators.
    source = Path(__file__).resolve().parents[1]
    return _hash(
        [
            (str(p.relative_to(source)), hashlib.sha256(p.read_bytes()).hexdigest())
            for p in sorted(source.rglob("*.py"))
        ]
    )


def _write(path: Path, value: Any) -> None:
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    parent_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def _read(path: Path) -> Any:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("OBSERVATION_FILE_UNSAFE")
        return json.load(stream)


def _directory(root: Path, source: dict[str, Any]) -> Path:
    # Source identity (not a mutable digest) makes changing bytes insufficient to retry.
    source_id = UUID(source["case"]["completion"]["id"])
    return root / ".secrets/nexus-synthesis-observations" / f"{source_id}-v1"


async def prepare(root: Path, source_path: Path) -> Path:
    root = root.resolve()
    _private_directory(root / ".secrets")
    if source_path.is_symlink():
        raise ValueError("OBSERVATION_FILE_UNSAFE")
    source_path = source_path.resolve()
    expected_parent = root / ".secrets/nexus-historical-three-stock"
    if source_path.parent.parent != expected_parent or source_path.name != "case.sqlite3":
        raise ValueError("OBSERVATION_IDENTITY_INVALID")
    _private_directory(expected_parent)
    _private_directory(source_path.parent)
    source = read_archive(source_path)
    directory = _directory(root, source)
    if directory.exists():
        raise ValueError("OBSERVATION_ALREADY_EXISTS")
    await check_current_configuration(root, source["case"])
    directory.parent.mkdir(mode=0o700, exist_ok=True)
    _private_directory(directory.parent)
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        raise ValueError("OBSERVATION_ALREADY_EXISTS") from None
    manifest = dict(
        profile=PROFILE,
        id=str(uuid4()),
        root=str(root),
        source=str(source_path),
        source_archive_hash=source["archive_hash"],
        limits=LIMITS,
        code_hash=_code_hash(),
        prepared_at=datetime.now(UTC).isoformat(),
    )
    _write(directory / "manifest.json", manifest)
    for parent in (directory.parent, directory.parent.parent):
        parent_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    return directory


def _load(directory: Path, *, executing: bool = False) -> tuple[dict[str, Any], dict[str, Any]]:
    _private_directory(directory)
    _private_directory(directory.parent)
    manifest = _read(directory / "manifest.json")
    root = Path(manifest["root"])
    _private_directory(root / ".secrets")
    source_path = Path(manifest["source"])
    if source_path.is_symlink() or source_path.parent.is_symlink():
        raise ValueError("OBSERVATION_FILE_UNSAFE")
    source = read_archive(source_path)
    UUID(manifest["id"])
    if (
        manifest["profile"] != PROFILE
        or manifest["limits"] != LIMITS
        or directory != _directory(root, source)
        or root != root.resolve()
        or source_path.parent.parent != root / ".secrets/nexus-historical-three-stock"
    ):
        raise ValueError("OBSERVATION_IDENTITY_INVALID")
    if source["archive_hash"] != manifest["source_archive_hash"]:
        raise ValueError("OBSERVATION_SOURCE_CHANGED")
    if executing and manifest["code_hash"] != _code_hash():
        raise ValueError("OBSERVATION_SOURCE_CHANGED")
    return manifest, source


def _validate_source_time(source: dict[str, Any], now: datetime) -> None:
    from nexus.domain.time import require_utc

    times = [
        datetime.fromisoformat(source["case"]["evidence_as_of"]),
        datetime.fromisoformat(source["observed_started_at"]),
        datetime.fromisoformat(source["observed_finished_at"]),
        now,
    ]
    for value in times:
        require_utc(value)
    if times != sorted(times):
        raise ValueError("OBSERVATION_SOURCE_CHANGED")


def _valid_usage(value: dict[str, Any]) -> bool:
    return (
        all(
            type(value.get(key)) is int
            for key in ("input_tokens", "output_tokens", "provider_calls", "tool_calls")
        )
        and 0 <= value["input_tokens"] <= 64000
        and 0 <= value["output_tokens"] <= 8192
        and value["provider_calls"] == 1
        and value["tool_calls"] == 0
    )


def _usage(usage: UsageSnapshot | None) -> dict[str, int] | None:
    if usage is None:
        return None
    return dict(
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        provider_calls=usage.provider_calls,
        tool_calls=usage.tool_calls,
    )


async def run(directory: Path) -> dict[str, Any]:
    if (directory / "claim.json").exists():
        raise ValueError("OBSERVATION_ALREADY_ATTEMPTED")
    manifest, source = _load(directory, executing=True)
    await check_current_configuration(Path(manifest["root"]), source["case"])
    # No model request can occur until this exclusive, durable claim exists.
    started = datetime.now(UTC)
    _validate_source_time(source, started)
    try:
        _write(
            directory / "claim.json",
            dict(manifest_hash=_hash(manifest), started_at=started.isoformat()),
        )
    except FileExistsError:
        raise ValueError("OBSERVATION_ALREADY_ATTEMPTED") from None
    clock = asyncio.get_running_loop().time
    claim_at = clock()
    usage = None
    raw = None
    phases: dict[InvocationPhase, int] = {}
    observer = SdkStreamObserver()
    stage = "projection"
    result: dict[str, Any] = dict(
        profile=PROFILE,
        id=manifest["id"],
        status="unknown",
        code="OBSERVATION_UNKNOWN",
        usage_complete=False,
        execution_allowed=False,
        started_at=started.isoformat(),
    )
    try:
        frozen = PortfolioSynthesisInputV1.model_validate(
            {
                **source["outputs"][3]["frozen_input"],
                "admitted_at": started,
                "expires_at": started + timedelta(seconds=90),
            }
        )
        payload = dict(
            instructions=source["case"]["synthesis_prompt"]["content"],
            input_hash=canonical_portfolio_synthesis_hash(frozen),
            frozen_input=frozen.model_dump(mode="json"),
        )
        projected = project_portfolio_payload(payload)
        request_hash = codex_request_hash(projected, PortfolioSynthesisIntentV1)
        result.update(
            request_hash=request_hash, request_bytes=len(codex_request_wire(projected).encode())
        )
        _write(
            directory / "request.json",
            dict(
                payload=projected,
                request_hash=request_hash,
                frozen_input=payload["frozen_input"],
                input_hash=payload["input_hash"],
            ),
        )
        stage = "sdk"
        # Projection time consumes the same deadline; cleanup is inside the 90-second cap.
        async with asyncio.timeout_at(claim_at + 90):
            raw, usage = await invoke_codex(
                projected,
                output_model=PortfolioSynthesisIntentV1,
                max_wire_bytes=64000,
                timeout_seconds=83,
                deadline_at=claim_at + 83,
                phase_elapsed_ms=phases,
                stream_observer=observer,
            )
        stage = "usage"
        if not _valid_usage(cast(dict[str, Any], _usage(usage))):
            raise ValueError("OBSERVATION_BUDGET_INVALID")
        stage = "schema"
        try:
            intent = PortfolioSynthesisIntentV1.model_validate_json(raw)
        except ValueError:
            raise ValueError("OBSERVATION_SCHEMA_INVALID") from None
        stage = "binding"
        try:
            validate_portfolio_synthesis_intent(intent, frozen)
        except ValueError:
            raise ValueError("OBSERVATION_BINDING_INVALID") from None
        if any(c.action != "defer" for c in intent.choices):
            raise ValueError("OBSERVATION_NONEXECUTING_REQUIRED")
        if datetime.now(UTC) >= frozen.expires_at or clock() >= claim_at + 90:
            raise ValueError("OBSERVATION_DEADLINE")
        result.update(
            status="observed",
            code="OBSERVATION_COMPLETE",
            output=dict(
                intent=intent.model_dump(mode="json"),
                input_hash=payload["input_hash"],
                frozen_input=frozen.model_dump(mode="json"),
                usage=_usage(usage),
            ),
        )
    except (Exception, asyncio.CancelledError) as error:
        if isinstance(error, CodexInvocationError):
            usage = error.usage
            result.update(
                code=error.reason,
                phase=error.phase,
                timeout_source=error.timeout_source,
                provider_call_started=error.provider_call_started,
            )
        elif isinstance(error, TimeoutError):
            result.update(code="OBSERVATION_DEADLINE", timeout_source="observation_total")
        elif isinstance(error, asyncio.CancelledError):
            result["code"] = "OBSERVATION_CANCELLED"
        elif (
            len(error.args) == 1 and isinstance(error.args[0], str) and error.args[0] in SAFE_CODES
        ):
            result["code"] = error.args[0]
    result.update(
        stage=stage,
        usage_complete=usage is not None,
        usage=_usage(usage),
        phase_elapsed_ms=phases,
        stream_progress=observer.snapshot().model_dump(mode="json"),
        stream_timestamps=observer.diagnostic_timestamps(),
        elapsed_ms=round((clock() - claim_at) * 1000),
        finished_at=datetime.now(UTC).isoformat(),
        manifest_hash=_hash(manifest),
    )
    if raw is not None:
        # Private invalid response is retained for diagnosis, never printed by this CLI.
        _write(directory / "response.json", dict(raw=raw))
    _write(directory / "result.json", result)
    return {k: v for k, v in result.items() if k != "output"}


def read_observation(directory: Path) -> dict[str, Any]:
    manifest, source = _load(directory)
    claim = _read(directory / "claim.json")
    result = _read(directory / "result.json")
    if result["status"] != "observed":
        raise ValueError("OBSERVATION_NOT_COMPLETE")
    if (
        claim["manifest_hash"] != _hash(manifest)
        or result["manifest_hash"] != _hash(manifest)
        or result["id"] != manifest["id"]
        or result["started_at"] != claim["started_at"]
        or not result["usage_complete"]
        or result["usage"] != result["output"]["usage"]
    ):
        raise ValueError("OBSERVATION_IDENTITY_INVALID")
    started = datetime.fromisoformat(result["started_at"])
    finished = datetime.fromisoformat(result["finished_at"])
    if not datetime.fromisoformat(source["observed_finished_at"]) <= started <= finished:
        raise ValueError("OBSERVATION_IDENTITY_INVALID")
    measured = result["usage"]
    frozen = result["output"]["frozen_input"]
    if (
        finished > started + timedelta(seconds=90)
        or frozen["admitted_at"] != started.isoformat().replace("+00:00", "Z")
        or datetime.fromisoformat(frozen["expires_at"]) != started + timedelta(seconds=90)
        or not _valid_usage(measured)
        or type(result["elapsed_ms"]) is not int
        or not 0 <= result["elapsed_ms"] <= 90000
    ):
        raise ValueError("OBSERVATION_BINDING_INVALID")
    derived = json.loads(json.dumps(source))
    metadata = dict(
        profile=PROFILE,
        id=manifest["id"],
        limits=LIMITS,
        source_archive_hash=source["archive_hash"],
        source_completion_id=source["case"]["completion"]["id"],
        source_research_observed_started_at=source["observed_started_at"],
        source_research_observed_finished_at=source["observed_finished_at"],
        synthesis_started_at=result["started_at"],
        synthesis_finished_at=result["finished_at"],
        manifest_hash=_hash(manifest),
        result_hash=_hash(result),
    )
    derived["case"]["completion"]["id"] = manifest["id"]
    derived["synthesis_observation"] = metadata
    derived["case"]["synthesis_observation"] = metadata
    derived["outputs"][3] = result["output"]
    derived["observed_finished_at"] = result["finished_at"]
    derived["archive_hash"] = _hash(dict(manifest=manifest, result=result))
    verify_results(derived["case"], derived["outputs"])
    if any(c["action"] != "defer" for c in derived["outputs"][3]["intent"]["choices"]):
        raise ValueError("OBSERVATION_NONEXECUTING_REQUIRED")
    return cast(dict[str, Any], derived)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preparation = commands.add_parser("prepare")
    preparation.add_argument("--repo-root", required=True, type=Path)
    preparation.add_argument("--source", required=True, type=Path)
    for name in ("run", "read"):
        commands.add_parser(name).add_argument("--observation-dir", required=True, type=Path)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            directory = asyncio.run(prepare(args.repo_root, args.source))
            print(json.dumps(dict(status="prepared", observation_dir=str(directory))))
        elif args.command == "run":
            result = asyncio.run(run(args.observation_dir))
            print(json.dumps(result))
            return 0 if result["status"] == "observed" else 1
        else:
            archive = read_observation(args.observation_dir)
            print(
                json.dumps(
                    dict(
                        status="observed",
                        archive_hash=archive["archive_hash"],
                        research_reused=3,
                        new_synthesis_calls=1,
                        execution_allowed=False,
                    )
                )
            )
    except Exception as error:
        code = error.args[0] if len(error.args) == 1 and isinstance(error.args[0], str) else None
        print(json.dumps(dict(status="refused", code=code if code in SAFE_CODES else "UNKNOWN")))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
