"""One approved comparison, not a general experiment scheduler or execution permit."""

from typing import Annotated, ClassVar, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nexus.jobs.models import job_input_hash

Hash = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
ACTOR = "research-comparison:direct-responses-terra-v11"
PURPOSE = "direct-responses-terra-v11-20261030"
SDK_HASH = "cdf5e7d3b0cfd2a3e6fda267f74bc9b3d135f28e33f965431f17175b9f2a8720"
Route = Literal["sdk", "responses"]


class ComparisonManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    experiment_id: UUID
    sdk_config_hash: Hash
    api_config_hash: Hash
    implementation_sha: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    source_manifest_hash: Hash
    identity_equivalence_hash: Hash
    outbound_safety_hash: Hash
    scoring_hash: Hash
    blinding_hash: Hash
    input_hashes: tuple[Hash, Hash, Hash]  # AAPL, AVGO, NVDA; shared by both routes.
    instrument_ids: tuple[UUID, UUID, UUID]
    stage_limit: ClassVar[int] = 36
    positions: ClassVar[tuple[tuple[int, str, Route], ...]] = (
        (1, "AAPL", "sdk"),
        (1, "AAPL", "responses"),
        (1, "AVGO", "sdk"),
        (1, "AVGO", "responses"),
        (1, "NVDA", "sdk"),
        (1, "NVDA", "responses"),
        (2, "NVDA", "responses"),
        (2, "NVDA", "sdk"),
        (2, "AVGO", "responses"),
        (2, "AVGO", "sdk"),
        (2, "AAPL", "responses"),
        (2, "AAPL", "sdk"),
    )

    @model_validator(mode="after")
    def check_configs(self) -> "ComparisonManifest":
        if (
            self.sdk_config_hash != SDK_HASH
            or self.api_config_hash == SDK_HASH
            or len(set(self.instrument_ids)) != 3
        ):
            raise ValueError("COMPARISON_CONFIG_INVALID")
        return self

    def input_hash_for(self, ordinal: int) -> str:
        if type(ordinal) is not int or not 1 <= ordinal <= len(self.positions):
            raise ValueError("COMPARISON_POSITION_INVALID")
        symbol = self.positions[ordinal - 1][1]
        return self.input_hashes[("AAPL", "AVGO", "NVDA").index(symbol)]

    @property
    def digest(self) -> str:
        return job_input_hash(
            {
                **self.model_dump(mode="json"),
                "purpose": PURPOSE,
                "positions": self.positions,
                "stage_limit": self.stage_limit,
            }
        )


def require_business_run(actor_id: str) -> None:
    if actor_id == ACTOR:
        raise ValueError("COMPARISON_BUSINESS_CONSUMPTION_FORBIDDEN")
