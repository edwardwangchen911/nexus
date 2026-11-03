"""Classify only known frozen portfolio prompt versions."""

import re

from nexus.agents.committee import (
    CLAIM_REVIEW_SCOPE_PROMPT_VERSION,
    DISCLOSURE_SCOPE_PROMPT_VERSIONS,
    INFERENCE_REVIEW_SCOPE_PROMPT_VERSION,
    MONITORING_SCOPE_PROMPT_VERSIONS,
    SECURITY_RESEARCH_SCOPE_PROMPT_VERSION,
)
from nexus.agents.research_only import PROMPT_PREFIX
from nexus.api.schemas.research_monitoring import MonitoringApplicability


def read_applicability(manifest: object) -> MonitoringApplicability:
    portfolio = manifest.get("portfolio") if isinstance(manifest, dict) else None
    version = portfolio.get("version") if isinstance(portfolio, dict) else None
    if isinstance(version, str):
        if version in MONITORING_SCOPE_PROMPT_VERSIONS:
            return MonitoringApplicability(
                price="applicable",
                disclosure="applicable"
                if version in DISCLOSURE_SCOPE_PROMPT_VERSIONS
                else "not_applicable",
            )
        if version in {
            "fixture-v1",
            "local-openai-v1",
            "local-portfolio-synthesis-v2",
            "local-portfolio-synthesis-v3",
            "local-portfolio-synthesis-v4",
            SECURITY_RESEARCH_SCOPE_PROMPT_VERSION,
            CLAIM_REVIEW_SCOPE_PROMPT_VERSION,
            INFERENCE_REVIEW_SCOPE_PROMPT_VERSION,
        } or re.fullmatch(re.escape(PROMPT_PREFIX) + r"[0-9a-f]{64}", version):
            return MonitoringApplicability(price="not_applicable", disclosure="not_applicable")
    return MonitoringApplicability(price="unknown", disclosure="unknown")
