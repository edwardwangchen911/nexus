"""Runtime settings with safe development fixtures and redacted representations."""

from decimal import Decimal
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class AgentRuntimeMode(StrEnum):
    FIXTURE = "fixture"
    RESPONSES = "responses"
    CODEX_SDK = "codex_sdk"


class _SafeSettings(BaseSettings):
    """Prevent logging a settings object from exposing operational configuration."""

    model_config = SettingsConfigDict(
        env_file=None,
        env_ignore_empty=True,
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    def __repr_args__(self) -> "list[tuple[str | None, object]]":
        return []


class EngineSettings(_SafeSettings):
    @property
    def object_store_prefix(self) -> Literal["provider/market"]:
        """Read the same frozen market namespace used by the provider collector."""
        return "provider/market"

    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        validation_alias="NEXUS_ENV",
    )
    database_url: str | None = Field(default=None, validation_alias="DATABASE_URL")
    release_revision: str | None = Field(
        default=None,
        validation_alias="NEXUS_RELEASE_REVISION",
        pattern=r"^[0-9a-f]{40}$",
    )
    object_store_endpoint: str | None = Field(
        default=None,
        validation_alias="OBJECT_STORE_ENDPOINT",
    )
    object_store_region: str | None = Field(default=None, validation_alias="OBJECT_STORE_REGION")
    object_store_bucket: str | None = Field(default=None, validation_alias="OBJECT_STORE_BUCKET")
    object_store_access_key_id: SecretStr | None = Field(
        default=None,
        validation_alias="OBJECT_STORE_ACCESS_KEY_ID",
    )
    object_store_secret_access_key: SecretStr | None = Field(
        default=None,
        validation_alias="OBJECT_STORE_SECRET_ACCESS_KEY",
    )
    engine_hmac_current_key_id: str | None = Field(
        default="current",
        validation_alias="NEXUS_ENGINE_HMAC_CURRENT_KEY_ID",
    )
    engine_hmac_current_secret: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "NEXUS_ENGINE_HMAC_CURRENT_SECRET", "NEXUS_ENGINE_HMAC_SECRET"
        ),
    )
    engine_hmac_previous_key_id: str | None = Field(
        default=None,
        validation_alias="NEXUS_ENGINE_HMAC_PREVIOUS_KEY_ID",
    )
    engine_hmac_previous_secret: SecretStr | None = Field(
        default=None,
        validation_alias="NEXUS_ENGINE_HMAC_PREVIOUS_SECRET",
    )
    agent_runtime_mode: AgentRuntimeMode | None = Field(
        default=None,
        validation_alias="AGENT_RUNTIME_MODE",
    )
    portfolio_decision_runtime_mode: AgentRuntimeMode | None = Field(
        default=None,
        validation_alias="PORTFOLIO_DECISION_RUNTIME_MODE",
    )
    openai_api_key: SecretStr | None = Field(default=None, validation_alias="OPENAI_API_KEY")
    openai_extraction_model: str | None = Field(
        default=None,
        validation_alias=AliasChoices("OPENAI_EXTRACTION_MODEL", "AGENT_EXTRACTION_MODEL_ID"),
    )
    openai_reasoning_model: str | None = Field(
        default=None,
        validation_alias=AliasChoices("OPENAI_REASONING_MODEL", "AGENT_REASONING_MODEL_ID"),
    )
    agent_daily_budget_usd: Decimal = Field(
        default=Decimal("3.00"), validation_alias="AGENT_DAILY_BUDGET_USD", gt=0
    )
    agent_paper_cycle_enabled: bool = Field(
        default=False,
        validation_alias="NEXUS_AGENT_PAPER_CYCLE_ENABLED",
    )
    formal_budget_protection_enabled: bool = Field(
        default=False, validation_alias="NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED"
    )
    agent_paper_formal_model_config_id: UUID | None = Field(
        default=None, validation_alias="NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID"
    )
    candidate_stage_capacity_reads: bool = Field(
        default=False, validation_alias="NEXUS_CANDIDATE_STAGE_CAPACITY_READS"
    )
    provider_jobs_enabled: bool = Field(
        default=False,
        validation_alias="NEXUS_PROVIDER_JOBS_ENABLED",
    )
    market_provider: Literal["fosun", "alpaca"] = Field(
        default="fosun",
        validation_alias="NEXUS_MARKET_PROVIDER",
    )

    @model_validator(mode="after")
    def apply_environment_policy(self) -> Self:
        if self.candidate_stage_capacity_reads:
            from sqlalchemy.engine import make_url

            try:
                url = make_url(self.database_url or "")
            except Exception:
                raise ValueError("CODEX_CANDIDATE_READ_CONFIGURATION_REJECTED") from None
            if (
                self.environment is not Environment.TEST
                or self.provider_jobs_enabled
                or self.agent_paper_cycle_enabled
                or url.drivername != "postgresql+asyncpg"
                or url.host not in {"127.0.0.1", "localhost", "::1"}
                or not (url.database or "").startswith(("nexus_test_", "nexus_diag_"))
                or bool(set(url.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
            ):
                raise ValueError("CODEX_CANDIDATE_READ_CONFIGURATION_REJECTED")
        if self.environment is not Environment.PRODUCTION and self.agent_runtime_mode is None:
            self.agent_runtime_mode = AgentRuntimeMode.FIXTURE
        self._validate_agent_runtime()
        if self.environment is Environment.PRODUCTION:
            self._validate_hmac_key_pairs()
            missing = [name for name, value in self._required_values().items() if value is None]
            if missing:
                raise ValueError(f"production settings require: {', '.join(missing)}")
            return self

        fixtures: dict[str, str | SecretStr] = {
            "database_url": "postgresql+asyncpg://nexus:nexus@postgres:5432/nexus",
            "object_store_endpoint": "http://minio:9000",
            "object_store_region": "us-east-1",
            "object_store_bucket": "nexus-fixtures",
            "object_store_access_key_id": SecretStr("fixture-object-store-access-key"),
            "object_store_secret_access_key": SecretStr("fixture-object-store-secret"),
            "engine_hmac_current_key_id": "fixture-current",
            "engine_hmac_current_secret": SecretStr("fixture-engine-hmac-secret"),
        }
        for name, value in fixtures.items():
            if getattr(self, name) is None:
                setattr(self, name, value)
        return self

    def _validate_agent_runtime(self) -> None:
        if self.agent_runtime_mode in {None, AgentRuntimeMode.FIXTURE}:
            return
        if self.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK:
            if self.environment is Environment.PRODUCTION or self.provider_jobs_enabled:
                raise ValueError("Codex SDK is only available for local research without providers")
            return
        if (
            _nonempty_secret(self.openai_api_key) is None
            or _nonempty(self.openai_extraction_model) is None
            or _nonempty(self.openai_reasoning_model) is None
        ):
            raise ValueError(
                "Agent non-fixture mode requires OPENAI_API_KEY, OPENAI_EXTRACTION_MODEL, "
                "and OPENAI_REASONING_MODEL"
            )

    def _validate_hmac_key_pairs(self) -> None:
        current_key_id = _nonempty(self.engine_hmac_current_key_id)
        current_secret = _nonempty_secret(self.engine_hmac_current_secret)
        if current_key_id is None or current_secret is None:
            raise ValueError(
                "NEXUS_ENGINE_HMAC_CURRENT_KEY_ID and NEXUS_ENGINE_HMAC_CURRENT_SECRET are required"
            )
        previous_key_id = _nonempty(self.engine_hmac_previous_key_id)
        previous_secret = _nonempty_secret(self.engine_hmac_previous_secret)
        has_previous_key_id = previous_key_id is not None
        has_previous_secret = previous_secret is not None
        if has_previous_key_id != has_previous_secret:
            raise ValueError(
                "NEXUS_ENGINE_HMAC_PREVIOUS_KEY_ID and "
                "NEXUS_ENGINE_HMAC_PREVIOUS_SECRET must be configured together"
            )
        if has_previous_key_id and previous_key_id == current_key_id:
            raise ValueError("NEXUS_ENGINE_HMAC_PREVIOUS_KEY_ID must differ from current key ID")

    def _required_values(self) -> dict[str, object | None]:
        return {
            "DATABASE_URL": self.database_url,
            "OBJECT_STORE_ENDPOINT": self.object_store_endpoint,
            "OBJECT_STORE_REGION": self.object_store_region,
            "OBJECT_STORE_BUCKET": self.object_store_bucket,
            "OBJECT_STORE_ACCESS_KEY_ID": self.object_store_access_key_id,
            "OBJECT_STORE_SECRET_ACCESS_KEY": self.object_store_secret_access_key,
            "NEXUS_ENGINE_HMAC_CURRENT_KEY_ID": self.engine_hmac_current_key_id,
            "NEXUS_ENGINE_HMAC_CURRENT_SECRET": self.engine_hmac_current_secret,
        }

    @classmethod
    def for_test(cls) -> Self:
        return cls(environment=Environment.TEST)


def _nonempty(value: str | None) -> str | None:
    return value.strip() if value is not None and value.strip() else None


def _nonempty_secret(value: SecretStr | None) -> str | None:
    return _nonempty(value.get_secret_value()) if value is not None else None
