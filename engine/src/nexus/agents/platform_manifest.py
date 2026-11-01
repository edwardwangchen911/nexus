"""One frozen gifted-credit qualification: three HTTP chains, at most nine sends."""

from decimal import Decimal
from typing import ClassVar, Literal

from pydantic import model_validator

from nexus.agents.comparison_manifest import (
    PLATFORM_ACTOR,
    PLATFORM_PURPOSE,
    PLATFORM_RENEWED_PURPOSE,
    PLATFORM_THIRD_PURPOSE,
    ComparisonManifest,
    Hash,
    Route,
)
from nexus.agents.platform_profile import platform_profile_hash
from nexus.agents.platform_usage import (
    CURRENT_PLATFORM_PRICE_VERSION,
    PLATFORM_PRICE_VERSION,
    platform_reservation_usd,
)
from nexus.agents.platform_wire import PlatformAllocation, PlatformProfile


class PlatformManifest(ComparisonManifest):
    platform_contract: Literal[
        "platform-gifted-terra-v1-20261031",
        "platform-terra-v2-20261101",
        "platform-terra-v3-20261101",
    ] = PLATFORM_PURPOSE
    allocations: tuple[PlatformAllocation, PlatformAllocation, PlatformAllocation]
    funding_proof_hash: Hash
    credential_binding_hash: Hash
    purpose: ClassVar[str] = PLATFORM_PURPOSE
    actor: ClassVar[str] = PLATFORM_ACTOR
    stage_limit: ClassVar[int] = 9
    positions: ClassVar[tuple[tuple[int, str, Route], ...]] = (
        (1, "AAPL", "responses"),
        (1, "AVGO", "responses"),
        (1, "NVDA", "responses"),
    )

    @model_validator(mode="after")
    def check_platform_contract(self) -> "PlatformManifest":
        version = self.price_version
        expected = (
            PLATFORM_PRICE_VERSION
            if self.purpose == PLATFORM_PURPOSE
            else CURRENT_PLATFORM_PRICE_VERSION
        )
        if (
            self.platform_contract != self.purpose
            or version != expected
            or self.api_config_hash != platform_profile_hash(version)
        ):
            raise ValueError("PLATFORM_PROFILE_INVALID")
        for index, a in enumerate(self.allocations):
            if (
                a.input_hash != self.input_hashes[index]
                or a.profile != PlatformProfile(price_version=version)
                or a.stage_order != ("research", "critique", "synthesis")
                or a.output_limit != 8192
                or a.framing_units != 8192
                or a.units_version != "platform-full-http-utf8-plus8192-v1"
                or len(a.stage_input_upper_bounds) != 3
                or any(
                    type(v) is not int or not 8192 <= v <= 272000
                    for v in a.stage_input_upper_bounds
                )
                or len(a.stage_schema_envelope_hashes) != 3
                or any(
                    len(v) != 64 or any(c not in "0123456789abcdef" for c in v)
                    for v in a.stage_schema_envelope_hashes
                )
            ):
                raise ValueError("PLATFORM_ALLOCATION_INVALID")
        return self

    @property
    def price_version(self) -> str:
        return self.allocations[0].profile.price_version

    def reservation_for(self, ordinal: int) -> Decimal:
        self.input_hash_for(ordinal)
        a = self.allocations[ordinal - 1]
        return platform_reservation_usd(a.stage_input_upper_bounds, a.output_limit)

    @property
    def total_reservation_usd(self) -> Decimal:
        return sum((self.reservation_for(i) for i in (1, 2, 3)), Decimal(0))


class RenewedPlatformManifest(PlatformManifest):
    """The one separately authorized new round; the original purpose stays consumed."""

    platform_contract: Literal[
        "platform-gifted-terra-v1-20261031",
        "platform-terra-v2-20261101",
        "platform-terra-v3-20261101",
    ] = PLATFORM_RENEWED_PURPOSE
    purpose: ClassVar[str] = PLATFORM_RENEWED_PURPOSE


class ThirdPlatformManifest(PlatformManifest):
    """One separately authorized third round, using the unchanged v2 price window."""

    platform_contract: Literal[
        "platform-gifted-terra-v1-20261031",
        "platform-terra-v2-20261101",
        "platform-terra-v3-20261101",
    ] = PLATFORM_THIRD_PURPOSE
    purpose: ClassVar[str] = PLATFORM_THIRD_PURPOSE
