"""Inspect formal task demand in a repeatable read-only snapshot; never claim or reserve."""

import argparse
import asyncio
import json
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource
from sqlalchemy import text
from sqlalchemy.engine import make_url

from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.fosun_local import _localize_compose_endpoints
from nexus.ops.local_stack import _validate_local_env
from nexus.paper.formal_budget_inventory import FormalBudgetInventory, inspect_inventory, reference
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


class _ProtectedFileSettings(EngineSettings):
    """A protected apply cannot inherit database or policy from its shell."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings,)


def configuration(root: Path) -> tuple[EngineSettings, UUID, UUID, bool, str]:
    _validate_local_env(root, root / "infra/.env.local")
    environment = {
        name: value
        for name, value in dotenv_values(root / "infra/.env.local", interpolate=False).items()
        if isinstance(value, str) and value
    }
    _localize_compose_endpoints(environment)
    model_id = None
    model_value = environment.get("NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID")
    if model_value is not None:
        try:
            model_id = UUID(model_value)
        except ValueError:
            raise ValueError("FORMAL_BUDGET_INPUT_INVALID") from None
    settings = _ProtectedFileSettings.model_validate(
        {
            **environment,
            "AGENT_RUNTIME_MODE": "codex_sdk",
            "NEXUS_PROVIDER_JOBS_ENABLED": "false",
            "OPENAI_API_KEY": None,
        }
    )
    _validate_local_database(settings.database_url)
    if settings.database_url is None:
        raise ValueError("FORMAL_BUDGET_INPUT_INVALID")
    account = UUID(environment.get("NEXUS_AGENT_PAPER_ACCOUNT_ID", ""))
    batch = UUID(environment.get("NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID", ""))
    enabled = environment.get("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "false").lower()
    if enabled not in {"true", "false"}:
        raise ValueError("FORMAL_BUDGET_INPUT_INVALID")
    target = make_url(settings.database_url)
    fingerprint = reference(
        account,
        batch,
        enabled,
        settings.agent_daily_budget_usd,
        target.host,
        target.port,
        target.database,
    )
    # Preserve the legacy fingerprint when no explicit formal model is bound.
    # A selected model is immutable authority; changing it requires a new apply.
    if model_id is not None:
        fingerprint = reference(fingerprint, "formal_model_config_id", model_id)
    return settings, account, batch, enabled == "true", fingerprint


async def inspect(root: Path) -> FormalBudgetInventory:
    root = root.resolve()
    if not (root / "infra/compose.yaml").is_file():
        raise ValueError("FORMAL_BUDGET_INPUT_INVALID")
    settings, account, batch, enabled, fingerprint = configuration(root)
    if settings.database_url is None:
        raise ValueError("FORMAL_BUDGET_INPUT_INVALID")
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            actual = await session.scalar(text("SELECT current_database()"))
            if actual != make_url(settings.database_url).database:
                raise ValueError("FORMAL_BUDGET_DATABASE_MISMATCH")
            report = await inspect_inventory(
                session,
                account_id=account,
                batch_id=batch,
                enabled=enabled,
                configuration_hash=fingerprint,
            )
        if configuration(root)[-1] != fingerprint:
            return report.model_copy(
                update={
                    "observed_uncovered_formal_usd": None,
                    "blocking_codes": tuple(
                        sorted(
                            set(report.blocking_codes)
                            | {"FORMAL_CONFIGURATION_CHANGED_DURING_READ"}
                        )
                    ),
                }
            )
        return report
    finally:
        await factory.kw["bind"].dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    try:
        report = asyncio.run(inspect(args.repo_root))
    except (ValueError, OSError):
        print(
            json.dumps({"status": "unavailable", "code": "FORMAL_BUDGET_INPUT_OR_TARGET_INVALID"})
        )
        return 1
    except Exception:
        print(json.dumps({"status": "unavailable", "code": "FORMAL_BUDGET_DATABASE_UNAVAILABLE"}))
        return 1
    print(report.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
