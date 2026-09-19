"""Small Mac-local OpenAI setup; no scheduler, broker access, or order capability."""

import argparse
import asyncio
import getpass
import hashlib
import json
import os
import stat
import sys
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlencode
from uuid import UUID

import httpx
from dotenv import dotenv_values
from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.engine import make_url

from nexus.agents import codex_token_budget
from nexus.agents.codex_versions import (
    ACTIONABLE_CONFIG_VERSION,
    INLINE_CITATION_CONFIG_VERSION,
    PORTFOLIO_DECISION_CONFIG_VERSION,
    PORTFOLIO_DECISION_MAX_INPUT_TOKENS,
    PORTFOLIO_DECISION_MAX_OUTPUT_TOKENS,
    PORTFOLIO_DECISION_MAX_TOOL_CALLS,
    PORTFOLIO_DECISION_MAX_WIRE_BYTES,
    STRUCTURED_96K_CONFIG_VERSION,
    STRUCTURED_96K_WIRE64K_CONFIG_VERSION,
)
from nexus.agents.committee import load_portfolio_synthesis_prompt, load_prompt_assets
from nexus.ops.fosun_local import _localize_compose_endpoints
from nexus.ops.research_admit_local import admit_local_research
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
_TOKENIZER_TIMEOUT_SECONDS = 30


async def prepare_tokenizer(*, allow_network: bool = True) -> dict[str, object]:
    """Explicitly prepare one fixed public vocabulary, without loading app settings."""
    result: dict[str, object] = {
        "status": "prepared",
        "tokenizer": "o200k_base",
        "downloaded": False,
        "execution_locked": True,
    }
    if codex_token_budget.local_vocabulary() is not None:
        return result
    if not allow_network:
        raise ValueError("CODEX_TOKENIZER_NETWORK_DISABLED")
    try:
        async with asyncio.timeout(_TOKENIZER_TIMEOUT_SECONDS):
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(20, connect=5), follow_redirects=False, trust_env=False
            ) as client:
                async with client.stream("GET", codex_token_budget.VOCABULARY_URL) as response:
                    if response.status_code != 200:
                        raise ValueError("CODEX_TOKENIZER_DOWNLOAD_FAILED")
                    data = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        if len(data) + len(chunk) > codex_token_budget.MAX_VOCABULARY_BYTES:
                            raise ValueError("CODEX_TOKENIZER_TOO_LARGE")
                        data.extend(chunk)
        if not codex_token_budget.valid_vocabulary(bytes(data)):
            raise ValueError("CODEX_TOKENIZER_HASH_INVALID")
    except (httpx.HTTPError, TimeoutError) as error:
        raise ValueError("CODEX_TOKENIZER_DOWNLOAD_FAILED") from error
    path = codex_token_budget.tokenizer_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(dir=path.parent, prefix=".o200k-", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            with suppress(FileNotFoundError):
                temporary.unlink()
    return {**result, "downloaded": True}


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
    settings: EngineSettings,
    *,
    staged: bool = False,
    strategy_rnd: bool = False,
    structured_synthesis: bool = False,
    aggregate_input_tokens: int = 64000,
    max_wire_bytes: int = 55808,
    inline_citations: bool = False,
    actionable_paper: bool = False,
) -> None:
    """Register existing prompts and one priced model idempotently, not fake research."""
    if settings.database_url is None:
        raise ValueError("OPENAI_LOCAL_DATABASE_REQUIRED")
    host = make_url(settings.database_url).host
    if host is None or host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("OPENAI_LOCAL_DATABASE_REQUIRED")
    config = dict(_CONFIG)
    if (
        type(actionable_paper) is not bool
        or actionable_paper
        and (
            not structured_synthesis
            or aggregate_input_tokens != 96000
            or max_wire_bytes != 64000
            or not inline_citations
            or settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
        )
    ):
        raise ValueError("CODEX_ACTIONABLE_SETUP_INVALID")
    if (
        type(inline_citations) is not bool
        or inline_citations
        and (
            not structured_synthesis
            or aggregate_input_tokens != 96000
            or max_wire_bytes != 64000
            or settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
        )
    ):
        raise ValueError("CODEX_CITATION_SETUP_INVALID")
    if (
        type(aggregate_input_tokens) is not int
        or aggregate_input_tokens not in {64000, 96000}
        or (aggregate_input_tokens != 64000 and not structured_synthesis)
    ):
        raise ValueError("CODEX_AGGREGATE_SETUP_INVALID")
    if (
        type(max_wire_bytes) is not int
        or max_wire_bytes not in {55808, 64000}
        or (max_wire_bytes != 55808 and aggregate_input_tokens != 96000)
    ):
        raise ValueError("CODEX_WIRE_SETUP_INVALID")
    if structured_synthesis and (staged or strategy_rnd):
        raise ValueError("CODEX_STRUCTURED_SETUP_CONFLICT")
    if (
        staged or structured_synthesis
    ) and settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK:
        raise ValueError("CODEX_STAGED_BACKEND_REQUIRED")
    if strategy_rnd and settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK:
        raise ValueError("CODEX_STRATEGY_RND_SETUP_ONLY")
    if staged and strategy_rnd:
        raise ValueError("CODEX_STRATEGY_RND_STAGED_CONFLICT")
    if settings.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK:
        from nexus.agents.codex_sdk import (
            CONFIG_VERSION,
            MAX_OUTPUT_TOKENS,
            STAGED_CONFIG_VERSION,
            STRUCTURED_CONFIG_VERSION,
        )
        from nexus.agents.codex_sdk import MODEL_ID as SDK_MODEL

        version = STAGED_CONFIG_VERSION if staged else CONFIG_VERSION
        if structured_synthesis:
            version = (
                INLINE_CITATION_CONFIG_VERSION
                if inline_citations
                else STRUCTURED_96K_WIRE64K_CONFIG_VERSION
                if max_wire_bytes == 64000
                else STRUCTURED_96K_CONFIG_VERSION
                if aggregate_input_tokens == 96000
                else STRUCTURED_CONFIG_VERSION
            )
        if actionable_paper:
            version = ACTIONABLE_CONFIG_VERSION
        if strategy_rnd:
            from nexus.evolution.codex import CODEX_RND_CONFIG_VERSION

            version = CODEX_RND_CONFIG_VERSION

        config.update(
            {
                "model_id": SDK_MODEL,
                "version": version,
                "max_input_tokens": aggregate_input_tokens,
                "max_output_tokens": MAX_OUTPUT_TOKENS,
                "max_tool_calls": 0,
                "input_usd_per_million": "0",
                "output_usd_per_million": "0",
                # These fields bound configuration validity, not subscription pricing.
                "pricing_expires_at": "2027-09-02T00:00:00+00:00",
                "billing_mode": "chatgpt_subscription",
                "max_sdk_turns": 3 if staged or structured_synthesis else 1,
            }
        )
    if max_wire_bytes != 55808:
        config["max_wire_bytes"] = max_wire_bytes
    if inline_citations:
        config["inline_citations"] = True
    configs = [config]
    if settings.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK:
        configs.append(
            {
                "model_id": SDK_MODEL,
                "version": PORTFOLIO_DECISION_CONFIG_VERSION,
                "max_input_tokens": PORTFOLIO_DECISION_MAX_INPUT_TOKENS,
                "max_output_tokens": PORTFOLIO_DECISION_MAX_OUTPUT_TOKENS,
                "max_tool_calls": PORTFOLIO_DECISION_MAX_TOOL_CALLS,
                "input_usd_per_million": "0",
                "output_usd_per_million": "0",
                "pricing_available_at": "2026-09-02T00:00:00+00:00",
                "pricing_expires_at": "2027-09-02T00:00:00+00:00",
                "billing_mode": "chatgpt_subscription",
                "max_sdk_turns": 2,
                "max_wire_bytes": PORTFOLIO_DECISION_MAX_WIRE_BYTES,
            }
        )
    prompts: list[tuple[str, str, str, str]] = [
        (prompt.role, "local-openai-v1", prompt.content, prompt.content_hash)
        for prompt in load_prompt_assets().values()
    ]
    portfolio_decision_prompt_path = (
        Path(__file__).resolve().parents[1] / "agents/prompts/portfolio-decision-v1.md"
    )
    portfolio_decision_prompt = portfolio_decision_prompt_path.read_text()
    prompts.append(
        (
            "portfolio_decision",
            "portfolio-decision-v1",
            portfolio_decision_prompt,
            hashlib.sha256(portfolio_decision_prompt.encode()).hexdigest(),
        )
    )
    if structured_synthesis:
        synthesis_prompt = load_portfolio_synthesis_prompt()
        prompts.append(
            (
                synthesis_prompt.role,
                synthesis_prompt.version,
                synthesis_prompt.content,
                synthesis_prompt.content_hash,
            )
        )
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
        for configured in configs:
            config_hash = hashlib.sha256(
                json.dumps(configured, sort_keys=True).encode()
            ).hexdigest()
            existing_model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.model_id == configured["model_id"],
                    AgentModelConfigRecord.version == configured["version"],
                )
            )
            if existing_model is None:
                session.add(
                    AgentModelConfigRecord(
                        model_id=str(configured["model_id"]),
                        version=str(configured["version"]),
                        config_hash=config_hash,
                        max_input_tokens=int(configured["max_input_tokens"]),
                        max_output_tokens=int(configured["max_output_tokens"]),
                        max_tool_calls=int(configured["max_tool_calls"]),
                        input_usd_per_million=Decimal(
                            str(configured["input_usd_per_million"])
                        ),
                        output_usd_per_million=Decimal(
                            str(configured["output_usd_per_million"])
                        ),
                        pricing_available_at=datetime.fromisoformat(
                            str(configured["pricing_available_at"])
                        ),
                        pricing_expires_at=datetime.fromisoformat(
                            str(configured["pricing_expires_at"])
                        ),
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
    settings: EngineSettings,
    instrument_id: UUID,
    *,
    as_of: datetime,
    news_evidence_id: UUID | None = None,
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
        snapshot = await prepare_research_snapshot(
            session,
            instrument_id,
            as_of=as_of,
            news_evidence_id=news_evidence_id,
        )
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
            "prepare-tokenizer",
            "status",
            "verify",
            "prepare-research",
            "refresh-research",
            "refresh-disclosure",
            "refresh-fund",
            "project-sec-facts",
            "import-sec-exhibit",
            "admit-research",
            "run-once",
            "run-portfolio-once",
        ),
    )
    parser.add_argument("--research-request", type=Path)
    parser.add_argument("--instrument-id", type=UUID)
    parser.add_argument("--news-evidence-id", type=UUID)
    parser.add_argument("--filing-id", type=UUID)
    parser.add_argument("--identity-snapshot-id", type=UUID)
    parser.add_argument("--quarter-metrics", action="store_true")
    parser.add_argument("--aggregate-input-tokens", type=int, choices=(64000, 96000), default=64000)
    parser.add_argument("--max-wire-bytes", type=int, choices=(55808, 64000), default=55808)
    parser.add_argument("--inline-citations", action="store_true")
    parser.add_argument(
        "--actionable-paper",
        action="store_true",
        help="Setup the bounded v4 Agent paper research version",
    )
    for part in ("submissions", "parent", "exhibit"):
        parser.add_argument(f"--sec-{part}", type=Path)
        parser.add_argument(f"--sec-{part}-receipt", type=Path)
    parser.add_argument("--accession")
    parser.add_argument(
        "--form", choices=("6-K", "8-K"), help="Company disclosure form (default: 6-K)"
    )
    parser.add_argument("--backend", choices=("codex_sdk", "responses"), default="codex_sdk")
    parser.add_argument(
        "--staged", action="store_true", help="Setup the three-stage SDK committee version"
    )
    parser.add_argument(
        "--structured-synthesis",
        action="store_true",
        help="Setup v3 structured synthesis (three bounded SDK stages)",
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
        if args.actionable_paper and (
            args.action != "setup"
            or args.backend != "codex_sdk"
            or not args.structured_synthesis
            or args.aggregate_input_tokens != 96000
            or args.max_wire_bytes != 64000
            or not args.inline_citations
        ):
            raise ValueError("CODEX_ACTIONABLE_SETUP_INVALID")
        if args.news_evidence_id is not None and args.action != "prepare-research":
            raise ValueError("OPENAI_NEWS_ARGUMENTS_INVALID")
        if args.inline_citations and (
            args.action != "setup"
            or not args.structured_synthesis
            or args.backend != "codex_sdk"
            or args.aggregate_input_tokens != 96000
            or args.max_wire_bytes != 64000
        ):
            raise ValueError("CODEX_CITATION_SETUP_INVALID")
        if args.max_wire_bytes != 55808 and (
            args.action != "setup"
            or not args.structured_synthesis
            or args.backend != "codex_sdk"
            or args.aggregate_input_tokens != 96000
        ):
            raise ValueError("CODEX_WIRE_SETUP_INVALID")
        if args.aggregate_input_tokens != 64000 and (
            args.action != "setup" or not args.structured_synthesis or args.backend != "codex_sdk"
        ):
            raise ValueError("CODEX_AGGREGATE_SETUP_INVALID")
        if (args.action == "admit-research") != (args.research_request is not None):
            raise ValueError("CODEX_ADMISSION_ARGUMENTS_INVALID")
        if args.action == "admit-research" and any(
            v is not None for v in (args.instrument_id, args.as_of, args.accession)
        ):
            raise ValueError("CODEX_ADMISSION_ARGUMENTS_INVALID")
        exhibit_archives = [
            (getattr(args, f"sec_{part}"), getattr(args, f"sec_{part}_receipt"))
            for part in ("submissions", "parent", "exhibit")
        ]
        exhibit_arguments = [
            args.identity_snapshot_id,
            *(p for pair in exhibit_archives for p in pair),
        ]
        if args.action == "import-sec-exhibit":
            if not all(p is not None for p in exhibit_arguments) or any(
                p is not None
                for p in (args.as_of, args.instrument_id, args.filing_id, args.accession)
            ):
                raise ValueError("SEC_EXHIBIT_ARGUMENTS_INVALID")
        elif args.quarter_metrics or any(p is not None for p in exhibit_arguments):
            raise ValueError("SEC_EXHIBIT_ARGUMENTS_INVALID")
        if args.filing_id is not None and args.action != "project-sec-facts":
            raise ValueError("SEC_FACT_PROJECTION_ARGUMENTS_INVALID")
        if args.action == "project-sec-facts" and (
            args.filing_id is None
            or args.as_of is not None
            or args.instrument_id is not None
            or args.accession is not None
        ):
            raise ValueError("SEC_FACT_PROJECTION_ARGUMENTS_INVALID")
        if args.form is not None and args.action != "refresh-disclosure":
            raise ValueError("SEC_LOCAL_DISCLOSURE_FORM_INVALID")
        if args.structured_synthesis and (args.action != "setup" or args.backend != "codex_sdk"):
            raise ValueError("CODEX_STRUCTURED_SETUP_ONLY")
        if args.structured_synthesis and (args.staged or args.strategy_rnd):
            raise ValueError("CODEX_STRUCTURED_SETUP_CONFLICT")
        if args.staged and (args.action != "setup" or args.backend != "codex_sdk"):
            raise ValueError("CODEX_STAGED_SETUP_ONLY")
        if args.strategy_rnd and (args.action != "setup" or args.backend != "codex_sdk"):
            raise ValueError("CODEX_STRATEGY_RND_SETUP_ONLY")
        if args.strategy_rnd and args.staged:
            raise ValueError("CODEX_STRATEGY_RND_STAGED_CONFLICT")
        if args.action == "prepare-tokenizer":
            print(json.dumps(asyncio.run(prepare_tokenizer(allow_network=not args.skip_key))))
            return 0
        if not (root / "engine/pyproject.toml").is_file():
            raise ValueError("OPENAI_REPOSITORY_ROOT_REQUIRED")
        settings = local_settings(
            root,
            load_model_key=args.action
            not in {
                "prepare-research",
                "refresh-research",
                "refresh-disclosure",
                "refresh-fund",
                "project-sec-facts",
                "import-sec-exhibit",
                "admit-research",
            },
            backend=args.backend,
        )
        if args.action == "setup":
            asyncio.run(
                prepare_database(
                    settings,
                    staged=args.staged,
                    strategy_rnd=args.strategy_rnd,
                    **({"actionable_paper": True} if args.actionable_paper else {}),
                    **({"inline_citations": True} if args.inline_citations else {}),
                    **({"structured_synthesis": True} if args.structured_synthesis else {}),
                    **(
                        {"aggregate_input_tokens": args.aggregate_input_tokens}
                        if args.aggregate_input_tokens != 64000
                        else {}
                    ),
                    **(
                        {"max_wire_bytes": args.max_wire_bytes}
                        if args.max_wire_bytes != 55808
                        else {}
                    ),
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
                    settings,
                    args.instrument_id,
                    as_of=args.as_of or datetime.now(UTC),
                    news_evidence_id=args.news_evidence_id,
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
        elif args.action == "project-sec-facts":
            from nexus.ops.sec_local import project_saved_sec_facts

            result = asyncio.run(project_saved_sec_facts(settings, args.filing_id))
        elif args.action == "import-sec-exhibit":
            from nexus.ops.sec_exhibit_local import import_sec_exhibit

            result = asyncio.run(
                import_sec_exhibit(
                    settings,
                    identity_snapshot_id=args.identity_snapshot_id,
                    archives=exhibit_archives,
                    include_quarter_metrics=args.quarter_metrics,
                )
            )
        elif args.action == "admit-research":
            result = asyncio.run(admit_local_research(settings, args.research_request))
        elif args.action in {"run-once", "run-portfolio-once"}:
            if args.backend == "responses" and settings.openai_api_key is None:
                raise ValueError("OPENAI_API_KEY_MISSING")
            if args.backend == "responses" and datetime.now(UTC) >= datetime.fromisoformat(
                str(_CONFIG["pricing_expires_at"])
            ):
                raise ValueError("OPENAI_PRICING_EXPIRED")
            if args.backend == "codex_sdk" and not status(settings)["ready_for_model_requests"]:
                raise ValueError("CODEX_CHATGPT_LOGIN_REQUIRED")
            from nexus.worker import run_agent_worker_once, run_portfolio_decision_worker_once

            result = {
                "job_processed": asyncio.run(
                    run_portfolio_decision_worker_once(settings)
                    if args.action == "run-portfolio-once"
                    else run_agent_worker_once(settings)
                ),
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
