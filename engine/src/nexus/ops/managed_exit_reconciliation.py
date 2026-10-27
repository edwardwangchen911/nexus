"""Explicit lifecycle evidence only; never accepts unknown business effects or starts a host."""

import asyncio
import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType
from uuid import UUID

from nexus.ops import agent_paper_recovery as recovery
from nexus.ops.agent_paper_loop import acquire_agent_paper_lock
from nexus.paper.manual_entry import manual_host_scope
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.settings import EngineSettings

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


def reconcile_managed_exit(
    settings: EngineSettings, *, repo_root: Path, env_file: Path, project: str, check_only: bool
) -> dict[str, object]:
    """Hold both locks continuously through committed canonical inspection and proof publish."""
    allowed = _CODES | recovery._KNOWN_CODES
    failure = _PREFIX + "UNAVAILABLE"

    def blocked(codes: list[str]) -> dict[str, object]:
        return {
            "status": "blocked",
            "blocking_codes": codes,
            "execution_locked": True,
            "external_requests": 0,
            "check_only": check_only,
            "current_eligible": False,
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
            failure = _PREFIX + "PROCESS_UNCONFIRMED"
            recovery.require_recovery_process_quiescence()
            failure = _PREFIX + "UNAVAILABLE"
            manager = service._recovery_module()
            capture = helper.capture_exit(service, manager, repo_root, env_file, project)
            readiness = asyncio.run(recovery.inspect_recovery_readiness(settings))
            if readiness.get("status") != "ready":
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
            recovery.require_recovery_process_quiescence()
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
    except Exception as error:
        code = str(error)
        if code in {"AGENT_PAPER_LOOP_ALREADY_RUNNING", "AGENT_PAPER_SERVICE_CONTROL_BUSY"}:
            code = _PREFIX + "LOCK_BUSY"
        return blocked([code if code in allowed else failure])
