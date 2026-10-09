"""Apply optional formal protection from protected local configuration, without work."""

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url

from nexus.ops.formal_budget_inventory import configuration
from nexus.paper.formal_budget_protection import _validate_binding, apply_protection, latest_binding
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


async def bind_runtime_environment(
    settings: EngineSettings,
    environment: dict[str, str],
) -> tuple[dict[str, str], str | None]:
    """Read applied authority; an immediate producer stop never needs an apply."""
    requested = environment.get("NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED", "false").lower()
    if requested not in {"true", "false"}:
        raise ValueError("FORMAL_BUDGET_INPUT_INVALID")
    if requested == "false":
        return environment, None
    if settings.database_url is None or make_url(environment.get("DATABASE_URL", "")) != make_url(
        settings.database_url
    ):
        raise ValueError("FORMAL_BUDGET_DATABASE_MISMATCH")
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            actual = await session.scalar(text("SELECT current_database()"))
            if actual != make_url(settings.database_url).database:
                raise ValueError("FORMAL_BUDGET_DATABASE_MISMATCH")
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("FORMAL_BUDGET_CLOCK_INVALID")
            binding = await latest_binding(session, business_date=now.date())
            if binding is None:
                raise ValueError("FORMAL_BUDGET_APPLY_REQUIRED")
            await _validate_binding(session, binding)
            bound = dict(environment)
            p = binding.payload
            bound["NEXUS_AGENT_PAPER_ACCOUNT_ID"] = str(p["account_id"])
            bound["NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID"] = str(p["batch_id"])
            bound["AGENT_DAILY_BUDGET_USD"] = str(binding.daily_limit_usd)
            from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION

            if p["pricing_version"] == FORMAL_STAGE_CAPACITY_CONFIG_VERSION:
                bound["NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID"] = str(p["model_id"])
            else:
                bound.pop("NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID", None)
            if not p["enabled"]:
                bound["NEXUS_AGENT_PAPER_CYCLE_ENABLED"] = "false"
            return bound, str(p["calendar_id"])
    finally:
        await factory.kw["bind"].dispose()


async def inspect_protection(root: Path) -> dict[str, object]:
    """Read only today's binding; this is not research or execution readiness."""
    settings, _account, _batch, enabled, fingerprint = configuration(root.resolve())
    receipt: dict[str, object] = {
        "execution_locked": True,
        "economic_activation_permitted": False,
        "external_requests": 0,
        "budget_binding_ready": None,
    }
    if not settings.formal_budget_protection_enabled or not enabled:
        return {
            **receipt,
            "status": "disabled"
            if not settings.formal_budget_protection_enabled
            else "producer_stopped",
        }
    if settings.database_url is None:
        raise ValueError("FORMAL_BUDGET_INPUT_INVALID")
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            if (
                await session.scalar(text("SELECT current_database()"))
                != make_url(settings.database_url).database
            ):
                raise ValueError("FORMAL_BUDGET_DATABASE_MISMATCH")
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime) or now.utcoffset() != timedelta(0):
                raise ValueError("FORMAL_BUDGET_CLOCK_INVALID")
            receipt["budget_day_utc"] = now.date().isoformat()
            binding = await latest_binding(session, business_date=now.date())
            if binding is None:
                result = {**receipt, "status": "apply_required", "budget_binding_ready": False}
            else:
                await _validate_binding(session, binding)
                pending = binding.payload["configuration_hash"] != fingerprint
                result = {
                    **receipt,
                    "status": "configuration_pending_apply" if pending else "applied",
                    "budget_binding_ready": True,
                    "sequence": binding.sequence,
                    "binding_hash": binding.content_hash,
                }
        after = configuration(root.resolve())
        if (
            after[-1] != fingerprint
            or after[0].database_url != settings.database_url
            or after[0].formal_budget_protection_enabled
            != settings.formal_budget_protection_enabled
        ):
            raise ValueError("FORMAL_CONFIGURATION_CHANGED_DURING_READ")
        return result
    finally:
        await factory.kw["bind"].dispose()


async def apply(root: Path, *, idempotency_key: str) -> dict[str, object]:
    root = root.resolve()
    settings, account, batch, enabled, fingerprint = configuration(root)
    if not settings.formal_budget_protection_enabled:
        raise ValueError("FORMAL_BUDGET_MODE_DISABLED")
    if settings.database_url is None:
        raise ValueError("FORMAL_BUDGET_INPUT_INVALID")
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            actual = await session.scalar(text("SELECT current_database()"))
            if actual != make_url(settings.database_url).database:
                raise ValueError("FORMAL_BUDGET_DATABASE_MISMATCH")
            binding = await apply_protection(
                session,
                account_id=account,
                batch_id=batch,
                enabled=enabled,
                daily_limit_usd=settings.agent_daily_budget_usd,
                configuration_hash=fingerprint,
                formal_model_config_id=settings.agent_paper_formal_model_config_id,
                idempotency_key=idempotency_key,
            )
            after = configuration(root)
            if after[-1] != fingerprint or after[0].database_url != settings.database_url:
                raise ValueError("FORMAL_CONFIGURATION_CHANGED_DURING_APPLY")
            receipt = {
                "status": "applied",
                "binding_hash": binding.content_hash,
                "budget_day_utc": binding.business_date.isoformat(),
                "sequence": binding.sequence,
                "execution_locked": True,
                "economic_activation_permitted": False,
            }
        return receipt
    finally:
        await factory.kw["bind"].dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--idempotency-key")
    parser.add_argument("--status", action="store_true", help="Read binding state without applying")
    args = parser.parse_args()
    if args.status:
        if args.idempotency_key:
            parser.error("--status does not accept --idempotency-key")
        try:
            receipt = asyncio.run(inspect_protection(args.repo_root))
        except Exception:
            print(json.dumps({"status": "unavailable", "code": "FORMAL_BUDGET_STATUS_UNAVAILABLE"}))
            return 1
        print(json.dumps(receipt, sort_keys=True))
        return 0 if receipt["budget_binding_ready"] is True else 1
    if not args.idempotency_key:
        parser.error("--idempotency-key is required when applying")
    try:
        receipt = asyncio.run(apply(args.repo_root, idempotency_key=args.idempotency_key))
    except Exception:
        # Never print config, account IDs, provider data, or raw DB exceptions.
        print(json.dumps({"status": "unavailable", "code": "FORMAL_BUDGET_APPLY_UNAVAILABLE"}))
        return 1
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
