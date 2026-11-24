"""Explicit lifecycle evidence only; never accepts unknown business effects or starts a host."""

import asyncio
import hashlib
import importlib.util
import re
import sys
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from types import ModuleType
from uuid import UUID

from dotenv import dotenv_values
from sqlalchemy import text

from nexus.ops import agent_paper_recovery as recovery
from nexus.ops.agent_paper_loop import acquire_agent_paper_lock
from nexus.ops.price_renewal import require_equivalent_price_renewal
from nexus.paper.manual_entry import manual_host_scope
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings

_PREFIX = "AGENT_PAPER_EXIT_RECONCILIATION_"
_CODES = frozenset(
    _PREFIX + suffix
    for suffix in (
        "UNAVAILABLE",
        "SCOPE_INVALID",
        "BUSINESS_BLOCKED",
        "SCHEMA_MISMATCH",
        "PROCESS_UNCONFIRMED",
        "LOCK_BUSY",
        "PRICE_RENEWAL_INVALID",
    )
)


def _load_service(root: Path) -> ModuleType:
    source = root / "scripts/macos/agent-paper-loop-service.py"
    spec = importlib.util.spec_from_file_location("nexus_managed_exit_service", source)
    if spec is None or spec.loader is None:
        raise ValueError(_PREFIX + "UNAVAILABLE")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def published_monitor_replacement(
    settings: EngineSettings, repo_root: Path, env_file: Path
) -> tuple[dict[str, object], str] | None:
    state_path = repo_root / ".secrets/nexus-agent-paper/recovery.json"
    if not state_path.is_file():
        return None
    service = _load_service(repo_root)
    manager = service._recovery_module()
    helper = service._reconciliation_module()
    state = manager.read(repo_root)
    if state is None or state.get("exit_reconciliation") is None:
        return None
    origin = helper.monitor_replacement_origin(repo_root, state, settings.release_revision)
    if origin is None:
        return None
    project = "nexus-fosun-live"
    status, exit_code = service._launchctl_inspection("launchctl", repo_root, env_file, project)
    if not helper.qualified_exit(
        service,
        manager,
        repo_root,
        env_file,
        project,
        state,
        status,
        exit_code,
        allow_retirement_pending=True,
    ):
        raise ValueError(_PREFIX + "SCOPE_INVALID")
    return origin


def reconcile_managed_exit(
    settings: EngineSettings,
    *,
    repo_root: Path,
    env_file: Path,
    project: str,
    check_only: bool,
    target_model_config_id: UUID | None = None,
    record_original_nvda_proof: str | None = None,
) -> dict[str, object]:
    """Hold both locks continuously through committed canonical inspection and proof publish."""
    from nexus.monitoring.original_occupancy import BLOCKED as ORIGINAL_OCCUPANCY_BLOCKED
    from nexus.ops.monitor_occupancy_local import DispositionResultUnknown

    allowed = _CODES | recovery._KNOWN_CODES | {ORIGINAL_OCCUPANCY_BLOCKED}
    failure = _PREFIX + "UNAVAILABLE"
    recorded: dict[str, object] | None = None

    def blocked(codes: list[str]) -> dict[str, object]:
        return {
            "status": "blocked",
            "blocking_codes": codes,
            "execution_locked": True,
            "external_requests": 0,
            "check_only": check_only,
            "current_eligible": False,
            **({"original_occupancy": recorded} if recorded is not None else {}),
        }

    try:
        if (
            type(check_only) is not bool
            or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", project) is None
        ):
            raise ValueError(_PREFIX + "SCOPE_INVALID")
        service = _load_service(repo_root)
        helper = service._reconciliation_module()
        helper_codes: object = getattr(helper, "SAFE_CODES", frozenset())
        if isinstance(helper_codes, set | frozenset) and all(
            isinstance(code, str) and re.fullmatch(r"AGENT_PAPER_EXIT_RECONCILIATION_[A-Z_]+", code)
            for code in helper_codes
        ):
            allowed |= helper_codes
        with service._service_control_lock(), acquire_agent_paper_lock():
            manager = service._recovery_module()
            old_attempt = None
            origin_digest = None
            exited_at = None
            if (
                project == "nexus-fosun-live"
                and target_model_config_id is None
                and record_original_nvda_proof is None
                and settings.agent_runtime_mode is AgentRuntimeMode.RESPONSES
            ):
                state = manager.read(repo_root)
                if state is not None and state.get("attempt") is None:
                    active = state.get("active_attempt")
                    if isinstance(active, dict) and active.get("runtime_mode") == "monitor_only":
                        old_attempt = active
            failure = _PREFIX + "PROCESS_UNCONFIRMED"
            if old_attempt is None:
                recovery.require_recovery_process_quiescence()
            failure = _PREFIX + "UNAVAILABLE"
            capture = helper.capture_exit(service, manager, repo_root, env_file, project)
            if old_attempt is not None:
                if (
                    capture["producer"].get("active_attempt") != old_attempt
                    or capture["producer"].get("schema") != CURRENT_MIGRATION_HEAD
                    or capture.get("reconciliation_revision") != settings.release_revision
                ):
                    raise ValueError(_PREFIX + "SCOPE_INVALID")
                exited_at = datetime.fromtimestamp(
                    helper.monitor_replacement_exited_at(repo_root, capture), tz=UTC
                )
                origin_digest = asyncio.run(
                    recovery.inspect_monitor_replacement(settings, old_attempt, exited_at=exited_at)
                ).digest
                failure = _PREFIX + "PROCESS_UNCONFIRMED"
                recovery.require_monitor_replacement_process_quiescence()
            if record_original_nvda_proof is not None:
                from nexus.ops.manual_paper import _require_maintenance_code
                from nexus.ops.monitor_occupancy_local import record_for_managed_exit
                from nexus.paper.manual_preparation_close import (
                    require_preparation_inspector_source,
                )

                revision = settings.release_revision
                if (
                    check_only
                    or target_model_config_id is not None
                    or project != "nexus-fosun-live"
                    or "pending_attempt" in capture["producer"]
                    or capture["producer"]["schema"] != CURRENT_MIGRATION_HEAD
                    or capture["reconciliation_revision"] != revision
                ):
                    raise ValueError(_PREFIX + "SCOPE_INVALID")

                def revalidate() -> None:
                    _require_maintenance_code(repo_root, revision)
                    require_preparation_inspector_source(repo_root, revision)
                    recovery.require_recovery_process_quiescence()
                    if (
                        helper.capture_exit(service, manager, repo_root, env_file, project)
                        != capture
                    ):
                        raise ValueError(_PREFIX + "SCOPE_INVALID")

                revalidate()
                recorded = record_for_managed_exit(
                    settings, expected_hash=record_original_nvda_proof, revalidate=revalidate
                )
            target = None
            source = None
            if target_model_config_id is not None:
                if (
                    "pending_attempt" in capture["producer"]
                    or capture["producer"]["schema"] != CURRENT_MIGRATION_HEAD
                ):
                    raise ValueError(_PREFIX + "SCHEMA_MISMATCH")
                source = helper.price_source_environment(repo_root, env_file)
                settings, target = asyncio.run(
                    _price_target(settings, source, env_file, target_model_config_id, capture)
                )
            readiness = asyncio.run(recovery.inspect_recovery_readiness(settings))
            if old_attempt is not None and (
                readiness.get("status") != "monitor_only"
                or readiness.get("paid_dispatch_ready") is not False
            ):
                raise ValueError(_PREFIX + "BUSINESS_BLOCKED")
            if readiness.get("status") != "ready" and not (
                readiness.get("status") == "monitor_only"
                and readiness.get("paid_dispatch_ready") is False
            ):
                codes = readiness.get("blocking_codes")
                return blocked(
                    [
                        code
                        if isinstance(code, str) and code in allowed
                        else _PREFIX + "BUSINESS_BLOCKED"
                        for code in codes
                    ]
                    if isinstance(codes, list) and codes
                    else [_PREFIX + "BUSINESS_BLOCKED"]
                )
            if readiness.get("migration_head") != CURRENT_MIGRATION_HEAD:
                raise ValueError(_PREFIX + "SCHEMA_MISMATCH")
            if (
                readiness.get("scope_hash") != manual_host_scope(settings)
                or readiness.get("execution_locked") is not True
                or readiness.get("external_requests") != 0
            ):
                raise ValueError(_PREFIX + "SCOPE_INVALID")
            failure = _PREFIX + "PROCESS_UNCONFIRMED"
            if old_attempt is None:
                recovery.require_recovery_process_quiescence()
            else:
                recovery.require_monitor_replacement_process_quiescence()
                asyncio.run(
                    recovery.inspect_monitor_replacement(
                        settings, old_attempt, expected=origin_digest, exited_at=exited_at
                    )
                )
            failure = _PREFIX + "UNAVAILABLE"
            published = helper.publish_exit(
                service,
                manager,
                repo_root,
                env_file,
                project,
                capture=capture,
                readiness=readiness,
                check_only=check_only,
                **({"monitor_only_origin": origin_digest} if origin_digest is not None else {}),
                **(
                    {"price_target": target, "source_environment": source}
                    if target is not None
                    else {}
                ),
            )
            statuses = {"eligible_at_check"} if check_only else {"reconciled", "already_reconciled"}
            if not isinstance(published, dict) or published.get("status") not in statuses:
                raise ValueError(_PREFIX + "UNAVAILABLE")
            result: dict[str, object] = {
                "status": published["status"],
                "blocking_codes": [],
                "execution_locked": True,
                "external_requests": 0,
                "check_only": check_only,
                "current_eligible": True,
                **({"original_occupancy": recorded} if recorded is not None else {}),
            }
            if "proof_id" in published:
                value = published["proof_id"]
                if not isinstance(value, str) or str(UUID(value)) != value:
                    raise ValueError(_PREFIX + "UNAVAILABLE")
                result["proof_id"] = value
            if "proof_hash" in published:
                value = published["proof_hash"]
                if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                    raise ValueError(_PREFIX + "UNAVAILABLE")
                result["proof_hash"] = value
            return result
    except DispositionResultUnknown:
        return {
            **blocked([ORIGINAL_OCCUPANCY_BLOCKED]),
            "status": "result_unknown",
            "read_original_disposition": True,
        }
    except Exception as error:
        code = str(error)
        if code in {"AGENT_PAPER_LOOP_ALREADY_RUNNING", "AGENT_PAPER_SERVICE_CONTROL_BUSY"}:
            code = _PREFIX + "LOCK_BUSY"
        return blocked([code if code in allowed else failure])


async def _price_target(
    settings: EngineSettings, source: bytes, env_file: Path, target_id: UUID, capture: dict
) -> tuple[EngineSettings, dict[str, str]]:
    if (
        not 0 < len(source) <= 1024 * 1024
        or hashlib.sha256(source).hexdigest() != capture["producer"]["config_hash"]
    ):
        raise ValueError(_PREFIX + "SCOPE_INVALID")
    content = source.decode("utf-8")
    if "${" in content:
        raise ValueError(_PREFIX + "SCOPE_INVALID")
    key = "NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID"
    pattern = re.compile(r"(?m)^(NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID=)([^\r\n]*)(\r?)$")
    declarations = re.findall(
        r"(?m)^\s*(?:export\s+)?(?:NEXUS_)?AGENT_PAPER_FORMAL_MODEL_CONFIG_ID\s*=", content
    )
    if len(pattern.findall(content)) != 1 or len(declarations) != 1:
        raise ValueError(_PREFIX + "SCOPE_INVALID")
    parsed = dotenv_values(stream=StringIO(content), interpolate=False)
    old_id = UUID(str(parsed.get(key)))
    target_raw = pattern.sub(lambda m: m[1] + str(target_id) + m[3], content).encode("utf-8")
    # Every other byte, including credentials and authority settings, stays exact.
    if env_file.read_bytes() not in (source, target_raw):
        raise ValueError(_PREFIX + "SCOPE_INVALID")
    if settings.agent_paper_formal_model_config_id not in {old_id, target_id}:
        raise ValueError(_PREFIX + "SCOPE_INVALID")
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            old = await session.get(AgentModelConfigRecord, old_id)
            new = await session.get(AgentModelConfigRecord, target_id)
            if old is None or new is None:
                raise ValueError(_PREFIX + "PRICE_RENEWAL_INVALID")
            identity = require_equivalent_price_renewal(old, new)
    finally:
        await factory.kw["bind"].dispose()
    target_values = settings.model_dump()
    target_values["agent_paper_formal_model_config_id"] = target_id
    target_settings = type(settings).model_validate(target_values)
    if any(
        getattr(target_settings, name) != value
        for name, value in settings.model_dump().items()
        if name != "agent_paper_formal_model_config_id"
    ):
        raise ValueError(_PREFIX + "SCOPE_INVALID")
    return target_settings, {
        "schema": CURRENT_MIGRATION_HEAD,
        "revision": capture["reconciliation_revision"],
        "policy": "same_schema_price_renewal_v1",
        "config_hash": hashlib.sha256(target_raw).hexdigest(),
        "scope_hash": manual_host_scope(target_settings),
        **identity,
    }
