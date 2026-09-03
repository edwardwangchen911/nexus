"""Small Mac-local OpenAI setup; no scheduler, broker access, or order capability."""

import argparse
import asyncio
import getpass
import hashlib
import json
import os
import stat
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode
from uuid import UUID

import httpx
from dotenv import dotenv_values
from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.engine import make_url

from nexus.agents.committee import load_prompt_assets
from nexus.ops.fosun_local import _localize_compose_endpoints
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings

# Official model page checked 2026-09-02; do not extend pricing validity on each startup.
# https://developers.openai.com/api/docs/models/gpt-5.4-mini
MODEL_ID = "gpt-5.4-mini-2026-03-17"
_CONFIG: dict[str, str | int] = {
    "model_id": MODEL_ID,
    "version": "openai-standard-2026-09-02",
    "max_input_tokens": 64000,
    "max_output_tokens": 8192,
    "max_tool_calls": 4,
    "input_usd_per_million": "0.75",
    "output_usd_per_million": "4.50",
    "pricing_available_at": "2026-09-02T00:00:00+00:00",
    "pricing_expires_at": "2026-10-02T00:00:00+00:00",
}
_KEY_PATH = Path(".secrets/nexus-openai-local/api-key")


def local_settings(
    root: Path, *, load_model_key: bool = True, backend: str = "responses"
) -> EngineSettings:
    """Load only the local app configuration and OpenAI key, never broker credentials."""
    if backend not in {"responses", "codex_sdk"}:
        raise ValueError("OPENAI_BACKEND_INVALID")
    if backend == "codex_sdk":
        load_model_key = False
    environment = {
        key: value for key, value in dotenv_values(root / "infra/.env.local").items() if value
    }
    environment.update({key: value for key, value in os.environ.items() if value})
    _localize_compose_endpoints(environment)
    if not load_model_key:
        environment.pop("OPENAI_API_KEY", None)
    key = environment.get("OPENAI_API_KEY", "").strip()
    key_path = root / _KEY_PATH
    if load_model_key and not key and (key_path.exists() or key_path.is_symlink()):
        descriptor = os.open(key_path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor) as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise ValueError("OPENAI_KEY_FILE_UNSAFE")
            key = stream.read(1025).strip()
    if key and (len(key) > 1024 or any(character.isspace() for character in key)):
        raise ValueError("OPENAI_KEY_INVALID")
    environment.update(
        {
            "AGENT_RUNTIME_MODE": "codex_sdk"
            if backend == "codex_sdk"
            else ("responses" if key else "fixture"),
            "OPENAI_EXTRACTION_MODEL": MODEL_ID,
            "OPENAI_REASONING_MODEL": MODEL_ID,
            "NEXUS_PROVIDER_JOBS_ENABLED": "false",
        }
    )
    if key:
        environment["OPENAI_API_KEY"] = key
    # BaseSettings also reads the process environment; an explicit None prevents
    # it from restoring a key that this offline-only command intentionally ignores.
    return EngineSettings.model_validate(
        {**environment, **({"OPENAI_API_KEY": None} if not load_model_key else {})}
    )


def status(settings: EngineSettings) -> dict[str, object]:
    if settings.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK:
        from nexus.agents.codex_sdk import sdk_status

        return asyncio.run(sdk_status())
    key_present = settings.openai_api_key is not None and bool(
        settings.openai_api_key.get_secret_value()
    )
    return {
        "provider": "openai",
        "model": MODEL_ID,
        "ready_for_model_requests": key_present,
        "blocker": None if key_present else "OPENAI_API_KEY_MISSING",
        "daily_budget_usd": str(settings.agent_daily_budget_usd),
        "execution_locked": True,
        "automatic_scheduling": False,
    }


async def prepare_database(
    settings: EngineSettings, *, staged: bool = False, strategy_rnd: bool = False
) -> None:
    """Register existing prompts and one priced model idempotently, not fake research."""
    if settings.database_url is None:
        raise ValueError("OPENAI_LOCAL_DATABASE_REQUIRED")
    host = make_url(settings.database_url).host
    if host is None or host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("OPENAI_LOCAL_DATABASE_REQUIRED")
    config = dict(_CONFIG)
    if staged and settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK:
        raise ValueError("CODEX_STAGED_BACKEND_REQUIRED")
    if strategy_rnd and settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK:
        raise ValueError("CODEX_STRATEGY_RND_SETUP_ONLY")
    if staged and strategy_rnd:
        raise ValueError("CODEX_STRATEGY_RND_STAGED_CONFLICT")
    if settings.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK:
        from nexus.agents.codex_sdk import (
            CONFIG_VERSION,
            MAX_INPUT_TOKENS,
            MAX_OUTPUT_TOKENS,
            STAGED_CONFIG_VERSION,
        )
        from nexus.agents.codex_sdk import MODEL_ID as SDK_MODEL

        version = STAGED_CONFIG_VERSION if staged else CONFIG_VERSION
        if strategy_rnd:
            from nexus.evolution.codex import CODEX_RND_CONFIG_VERSION

            version = CODEX_RND_CONFIG_VERSION

        config.update(
            {
                "model_id": SDK_MODEL,
                "version": version,
                "max_input_tokens": MAX_INPUT_TOKENS,
                "max_output_tokens": MAX_OUTPUT_TOKENS,
                "max_tool_calls": 0,
                "input_usd_per_million": "0",
                "output_usd_per_million": "0",
                # These fields bound configuration validity, not subscription pricing.
                "pricing_expires_at": "2027-09-02T00:00:00+00:00",
                "billing_mode": "chatgpt_subscription",
                "max_sdk_turns": 3 if staged else 1,
            }
        )
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    prompts: list[tuple[str, str, str, str]] = [
        (prompt.role, "local-openai-v1", prompt.content, prompt.content_hash)
        for prompt in load_prompt_assets().values()
    ]
    if strategy_rnd:
        from nexus.evolution.agent import load_strategy_rnd_prompt

        prompt = load_strategy_rnd_prompt()
        prompts.append((prompt.role, prompt.version, prompt.content, prompt.content_hash))
    async with session_factory(settings)() as session, session.begin():
        for role, version, content, content_hash in prompts:
            existing = await session.scalar(
                select(AgentPromptVersionRecord).where(
                    AgentPromptVersionRecord.role == role,
                    AgentPromptVersionRecord.version == version,
                )
            )
            if existing is None:
                session.add(
                    AgentPromptVersionRecord(
                        role=role,
                        version=version,
                        content=content,
                        content_hash=content_hash,
                    )
                )
            elif existing.content_hash != content_hash or existing.content != content:
                raise ValueError("OPENAI_PROMPT_VERSION_CONFLICT")
        existing_model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.model_id == config["model_id"],
                AgentModelConfigRecord.version == config["version"],
            )
        )
        if existing_model is None:
            session.add(
                AgentModelConfigRecord(
                    model_id=str(config["model_id"]),
                    version=str(config["version"]),
                    config_hash=config_hash,
                    max_input_tokens=int(config["max_input_tokens"]),
                    max_output_tokens=int(config["max_output_tokens"]),
                    max_tool_calls=int(config["max_tool_calls"]),
                    input_usd_per_million=Decimal(str(config["input_usd_per_million"])),
                    output_usd_per_million=Decimal(str(config["output_usd_per_million"])),
                    pricing_available_at=datetime.fromisoformat(
                        str(config["pricing_available_at"])
                    ),
                    pricing_expires_at=datetime.fromisoformat(str(config["pricing_expires_at"])),
                )
            )
        elif existing_model.config_hash != config_hash:
            raise ValueError("OPENAI_MODEL_VERSION_CONFLICT")


async def verify_connection(
    settings: EngineSettings, *, http_client: httpx.AsyncClient | None = None
) -> dict[str, object]:
    """One synthetic request, no tools/account data, no retry; no research result is saved."""
    if settings.openai_api_key is None:
        raise ValueError("OPENAI_API_KEY_MISSING")
    async with AsyncOpenAI(
        api_key=settings.openai_api_key.get_secret_value(),
        base_url="https://api.openai.com/v1",
        max_retries=0,
        timeout=30,
        http_client=http_client,
    ) as client:
        response = await client.responses.create(
            model=MODEL_ID,
            store=False,
            max_output_tokens=128,
            input=(
                "Reply exactly NEXUS_OPENAI_OK. "
                "This is a connectivity check, not financial research."
            ),
        )
    if (
        response.status != "completed"
        or response.output_text.strip() != "NEXUS_OPENAI_OK"
        or response.usage is None
    ):
        raise ValueError("OPENAI_CONNECTIVITY_RESPONSE_INVALID")
    return {
        "status": "OPENAI_CONNECTED",
        "model": MODEL_ID,
        "physical_requests": 1,
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
        "research_completed": False,
        "execution_locked": True,
    }


async def prepare_local_research(
    settings: EngineSettings, instrument_id: UUID, *, as_of: datetime
) -> dict[str, object]:
    """Freeze already-ingested research inputs; do not enqueue work or contact providers."""
    from nexus.agents.snapshots import prepare_research_snapshot

    if settings.database_url is None or make_url(settings.database_url).host not in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        raise ValueError("OPENAI_LOCAL_DATABASE_REQUIRED")
    async with session_factory(settings)() as session, session.begin():
        snapshot = await prepare_research_snapshot(session, instrument_id, as_of=as_of)
        return {
            "status": "RESEARCH_INPUTS_PREPARED" if snapshot else "RESEARCH_EVIDENCE_UNAVAILABLE",
            "data_snapshot_id": str(snapshot.id) if snapshot else None,
            "research_path": f"/research/{instrument_id}?"
            + urlencode(
                {
                    "as_of": as_of.isoformat().replace("+00:00", "Z"),
                }
            ),
            "model_requests": 0,
            "provider_requests": 0,
            "research_completed": False,
            "execution_locked": True,
        }


def _save_key(root: Path) -> None:
    if not sys.stdin.isatty():
        raise ValueError("OPENAI_KEY_REQUIRES_LOCAL_TERMINAL")
    key = getpass.getpass("OpenAI API Key（本机隐藏输入，不会发送到聊天）: ").strip()
    if not key or len(key) > 1024 or any(character.isspace() for character in key):
        raise ValueError("OPENAI_KEY_INVALID")
    path = root / _KEY_PATH
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    if path.parent.resolve() != path.parent.absolute():
        raise ValueError("OPENAI_KEY_FILE_UNSAFE")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(key + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=(
            "setup",
            "status",
            "verify",
            "prepare-research",
            "refresh-research",
            "refresh-disclosure",
            "refresh-fund",
            "run-once",
        ),
    )
    parser.add_argument("--instrument-id", type=UUID)
    parser.add_argument("--accession")
    parser.add_argument(
        "--form", choices=("6-K", "8-K"), help="Company disclosure form (default: 6-K)"
    )
    parser.add_argument("--backend", choices=("codex_sdk", "responses"), default="codex_sdk")
    parser.add_argument(
        "--staged", action="store_true", help="Setup the three-stage SDK committee version"
    )
    parser.add_argument(
        "--strategy-rnd",
        action="store_true",
        help="Setup the one-turn Codex Strategy R&D prompt and model version",
    )
    parser.add_argument("--as-of", type=datetime.fromisoformat)
    parser.add_argument(
        "--skip-key", action="store_true", help="Prepare local DB only; no key prompt or network"
    )
    args = parser.parse_args()
    root = Path.cwd().resolve()
    try:
        if args.form is not None and args.action != "refresh-disclosure":
            raise ValueError("SEC_LOCAL_DISCLOSURE_FORM_INVALID")
        if args.staged and (args.action != "setup" or args.backend != "codex_sdk"):
            raise ValueError("CODEX_STAGED_SETUP_ONLY")
        if args.strategy_rnd and (args.action != "setup" or args.backend != "codex_sdk"):
            raise ValueError("CODEX_STRATEGY_RND_SETUP_ONLY")
        if args.strategy_rnd and args.staged:
            raise ValueError("CODEX_STRATEGY_RND_STAGED_CONFLICT")
        if not (root / "engine/pyproject.toml").is_file():
            raise ValueError("OPENAI_REPOSITORY_ROOT_REQUIRED")
        settings = local_settings(
            root,
            load_model_key=args.action
            not in {"prepare-research", "refresh-research", "refresh-disclosure", "refresh-fund"},
            backend=args.backend,
        )
        if args.action == "setup":
            asyncio.run(
                prepare_database(
                    settings,
                    staged=args.staged,
                    strategy_rnd=args.strategy_rnd,
                )
            )
            if (
                args.backend == "responses"
                and not args.skip_key
                and settings.openai_api_key is None
            ):
                _save_key(root)
                settings = local_settings(root)
            result = {**status(settings), "database_configuration": "registered"}
            if args.backend == "responses" and not args.skip_key:
                result.update(asyncio.run(verify_connection(settings)))
        elif args.action == "verify":
            if args.backend == "codex_sdk":
                raise ValueError("CODEX_USE_RUN_ONCE_FOR_RESEARCH")
            result = asyncio.run(verify_connection(settings))
        elif args.action == "prepare-research":
            if args.instrument_id is None:
                raise ValueError("OPENAI_RESEARCH_INSTRUMENT_REQUIRED")
            result = asyncio.run(
                prepare_local_research(
                    settings, args.instrument_id, as_of=args.as_of or datetime.now(UTC)
                )
            )
        elif args.action in {"refresh-research", "refresh-disclosure", "refresh-fund"}:
            if (args.action == "refresh-fund") != (args.accession is not None):
                raise ValueError("SEC_LOCAL_FUND_ACCESSION_REQUIRED")
            if args.instrument_id is None:
                raise ValueError("OPENAI_RESEARCH_INSTRUMENT_REQUIRED")
            if args.as_of is not None:
                raise ValueError("SEC_LOCAL_REFRESH_USES_CURRENT_TIME")
            from nexus.ops.sec_local import refresh_sec_research

            contact = os.environ.get("SEC_CONTACT") or dotenv_values(root / "infra/.env.local").get(
                "SEC_CONTACT"
            )
            result = asyncio.run(
                refresh_sec_research(
                    settings,
                    args.instrument_id,
                    contact=contact,
                    disclosure_only=args.action == "refresh-disclosure",
                    disclosure_form=args.form or "6-K",
                    fund_accession=args.accession,
                )
            )
        elif args.action == "run-once":
            if args.backend == "responses" and settings.openai_api_key is None:
                raise ValueError("OPENAI_API_KEY_MISSING")
            if args.backend == "responses" and datetime.now(UTC) >= datetime.fromisoformat(
                str(_CONFIG["pricing_expires_at"])
            ):
                raise ValueError("OPENAI_PRICING_EXPIRED")
            if args.backend == "codex_sdk" and not status(settings)["ready_for_model_requests"]:
                raise ValueError("CODEX_CHATGPT_LOGIN_REQUIRED")
            from nexus.worker import run_agent_worker_once

            result = {
                "job_processed": asyncio.run(run_agent_worker_once(settings)),
                "execution_locked": True,
            }
        else:
            result = status(settings)
        print(json.dumps(result))
        return 0
    except Exception as error:
        # Provider/DB errors can include credentials or request contents; never print them.
        code = (
            str(error)
            if isinstance(error, ValueError)
            and str(error).startswith(("OPENAI_", "SEC_", "CODEX_"))
            else type(error).__name__
        )
        print(json.dumps({"status": "blocked", "code": code, "execution_locked": True}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
