"""Typed real-account advisory policy without mutable threshold inputs."""

from decimal import Decimal
from typing import Literal

from pydantic import ConfigDict

from nexus.diagnostics.models import _DiagnosticModel


class RealAdvisoryPolicy(_DiagnosticModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal["real_advisory_policy_v1"] = "real_advisory_policy_v1"
    single_name_warning: Decimal = Decimal("0.15")
    single_name_critical: Decimal = Decimal("0.30")
    top3_warning: Decimal = Decimal("0.60")
    sector_warning: Decimal = Decimal("0.30")
    sector_critical: Decimal = Decimal("0.50")
    currency_warning: Decimal = Decimal("0.80")
    correlation_cluster_warning: Decimal = Decimal("0.50")
    cash_minimum: Decimal = Decimal("0.10")
    drawdown_warning: Decimal = Decimal("0.08")
    drawdown_critical: Decimal = Decimal("0.15")
    days_to_exit_warning: Decimal = Decimal("2")
    days_to_exit_critical: Decimal = Decimal("5")

    @classmethod
    def v1(cls) -> "RealAdvisoryPolicy":
        return cls()
