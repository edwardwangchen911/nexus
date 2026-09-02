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

import httpx
from dotenv import dotenv_values
from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.engine import make_url

from nexus.agents.committee import load_prompt_assets
from nexus.ops.fosun_local import _localize_compose_endpoints
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

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


def local_settings(root: Path) -> EngineSettings:
    """Load only the local app configuration and OpenAI key, never broker credentials."""
    environment = {
        key: value for key, value in dotenv_values(root / "infra/.env.local").items() if value
    }
    environment.update({key: value for key, value in os.environ.items() if value})
    _localize_compose_endpoints(environment)
    key = environment.get("OPENAI_API_KEY", "").strip()
    key_path = root / _KEY_PATH
    if not key and (key_path.exists() or key_path.is_symlink()):
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
            "AGENT_RUNTIME_MODE": "responses" if key else "fixture",
            "OPENAI_EXTRACTION_MODEL": MODEL_ID,
            "OPENAI_REASONING_MODEL": MODEL_ID,
            "NEXUS_PROVIDER_JOBS_ENABLED": "false",
        }
    )
    if key:
        environment["OPENAI_API_KEY"] = key
    return EngineSettings.model_validate(environment)


def status(settings: EngineSettings) -> dict[str, object]:
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


async def prepare_database(settings: EngineSettings) -> None:
    """Register existing prompts and one priced model idempotently, not fake research."""
    if settings.database_url is None:
        raise ValueError("OPENAI_LOCAL_DATABASE_REQUIRED")
    host = make_url(settings.database_url).host
    if host is None or host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("OPENAI_LOCAL_DATABASE_REQUIRED")
    config_hash = hashlib.sha256(json.dumps(_CONFIG, sort_keys=True).encode()).hexdigest()
    async with session_factory(settings)() as session, session.begin():
        for prompt in load_prompt_assets().values():
            existing = await session.scalar(
                select(AgentPromptVersionRecord).where(
                    AgentPromptVersionRecord.role == prompt.role,
                    AgentPromptVersionRecord.version == "local-openai-v1",
                )
            )
            if existing is None:
                session.add(
                    AgentPromptVersionRecord(
                        role=prompt.role,
                        version="local-openai-v1",
                        content=prompt.content,
                        content_hash=prompt.content_hash,
                    )
                )
            elif existing.content_hash != prompt.content_hash or existing.content != prompt.content:
                raise ValueError("OPENAI_PROMPT_VERSION_CONFLICT")
        existing_model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.model_id == MODEL_ID,
                AgentModelConfigRecord.version == _CONFIG["version"],
            )
        )
        if existing_model is None:
            session.add(
                AgentModelConfigRecord(
                    model_id=MODEL_ID,
                    version=str(_CONFIG["version"]),
                    config_hash=config_hash,
                    max_input_tokens=int(_CONFIG["max_input_tokens"]),
                    max_output_tokens=int(_CONFIG["max_output_tokens"]),
                    max_tool_calls=int(_CONFIG["max_tool_calls"]),
                    input_usd_per_million=Decimal(str(_CONFIG["input_usd_per_million"])),
                    output_usd_per_million=Decimal(str(_CONFIG["output_usd_per_million"])),
                    pricing_available_at=datetime.fromisoformat(
                        str(_CONFIG["pricing_available_at"])
                    ),
                    pricing_expires_at=datetime.fromisoformat(str(_CONFIG["pricing_expires_at"])),
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
    parser.add_argument("action", choices=("setup", "status", "verify", "run-once"))
    parser.add_argument(
        "--skip-key", action="store_true", help="Prepare local DB only; no key prompt or network"
    )
    args = parser.parse_args()
    root = Path.cwd().resolve()
    try:
        if not (root / "engine/pyproject.toml").is_file():
            raise ValueError("OPENAI_REPOSITORY_ROOT_REQUIRED")
        settings = local_settings(root)
        if args.action == "setup":
            asyncio.run(prepare_database(settings))
            if not args.skip_key and settings.openai_api_key is None:
                _save_key(root)
                settings = local_settings(root)
            result = {**status(settings), "database_configuration": "registered"}
            if not args.skip_key:
                result.update(asyncio.run(verify_connection(settings)))
        elif args.action == "verify":
            result = asyncio.run(verify_connection(settings))
        elif args.action == "run-once":
            if settings.openai_api_key is None:
                raise ValueError("OPENAI_API_KEY_MISSING")
            if datetime.now(UTC) >= datetime.fromisoformat(str(_CONFIG["pricing_expires_at"])):
                raise ValueError("OPENAI_PRICING_EXPIRED")
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
            if isinstance(error, ValueError) and str(error).startswith("OPENAI_")
            else type(error).__name__
        )
        print(json.dumps({"status": "blocked", "code": code, "execution_locked": True}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
