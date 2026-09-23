"""Freeze a prospective portfolio challenger input without changing the live run."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.portfolio_synthesis_persistence import _frozen_run
from nexus.domain.time import require_utc
from nexus.paper.agent_challenger import (
    AgentChallengerError,
    PortfolioChallengerPublicationV1,
)
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
)


@dataclass(frozen=True)
class PortfolioChallengerCase:
    source_run_id: UUID
    source_input_hash: str
    challenger_input_hash: str
    challenger_prompt_hash: str
    challenger_prompt_version: str
    frozen_input: PortfolioSynthesisInputV1
    instructions: str


@dataclass(frozen=True)
class PortfolioChallengerDecisionDifference:
    selection_changed: bool
    reasoning_changed: bool
    changed_instrument_ids: tuple[UUID, ...]
    economic_result: Literal["not_evaluated"] = "not_evaluated"


def compare_portfolio_challenger_decisions(
    *,
    incumbent_input: PortfolioSynthesisInputV1,
    incumbent_intent: PortfolioSynthesisIntentV1,
    challenger_input: PortfolioSynthesisInputV1,
    challenger_intent: PortfolioSynthesisIntentV1,
) -> PortfolioChallengerDecisionDifference:
    """Compare admitted decision semantics; this does not infer investment value."""
    validate_portfolio_synthesis_intent(incumbent_intent, incumbent_input)
    validate_portfolio_synthesis_intent(challenger_intent, challenger_input)
    if incumbent_input.model_dump(
        mode="json", exclude={"prompt_version_id", "prompt_hash"}
    ) != challenger_input.model_dump(mode="json", exclude={"prompt_version_id", "prompt_hash"}):
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_SOURCE_MISMATCH")
    incumbent = {choice.instrument_id: choice for choice in incumbent_intent.choices}
    challenger = {choice.instrument_id: choice for choice in challenger_intent.choices}
    changed = tuple(
        instrument_id
        for instrument_id in incumbent_input.candidate_instrument_ids
        if incumbent[instrument_id].model_dump(
            mode="json", exclude={"thesis", "counter_thesis", "invalidation"}
        )
        != challenger[instrument_id].model_dump(
            mode="json", exclude={"thesis", "counter_thesis", "invalidation"}
        )
    )
    reasoning_changed = (
        incumbent_intent.cash_rationale != challenger_intent.cash_rationale
        or any(
            (
                incumbent[instrument_id].thesis,
                incumbent[instrument_id].counter_thesis,
                incumbent[instrument_id].invalidation,
            )
            != (
                challenger[instrument_id].thesis,
                challenger[instrument_id].counter_thesis,
                challenger[instrument_id].invalidation,
            )
            for instrument_id in incumbent_input.candidate_instrument_ids
        )
    )
    return PortfolioChallengerDecisionDifference(
        selection_changed=bool(changed),
        reasoning_changed=reasoning_changed,
        changed_instrument_ids=changed,
    )


def freeze_prospective_portfolio_challenger_case(
    *,
    run: AgentPortfolioSynthesisRunRecord,
    incumbent_prompt: AgentPromptVersionRecord,
    challenger_prompt: AgentPromptVersionRecord,
    publication: PortfolioChallengerPublicationV1,
    published_at: datetime,
    now: datetime,
) -> PortfolioChallengerCase:
    """Require publication before a new multi-stock run; never replay an old case as OOS."""
    require_utc(published_at)
    require_utc(now)
    try:
        frozen = _frozen_run(run)
    except ValueError as error:
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_SOURCE_INVALID") from error
    if (
        len(frozen.candidate_instrument_ids) < 2
        or run.status not in {"prepared", "running", "completed"}
        or not published_at < frozen.admitted_at <= now < frozen.expires_at
        or challenger_prompt.created_at is None
        or not challenger_prompt.created_at < frozen.admitted_at
        or incumbent_prompt.id != publication.incumbent_prompt_version_id
        or incumbent_prompt.id != frozen.prompt_version_id
        or incumbent_prompt.role != "portfolio_decision"
        or incumbent_prompt.release_channel != "active"
        or incumbent_prompt.content_hash != frozen.prompt_hash
        or sha256(incumbent_prompt.content.encode()).hexdigest() != frozen.prompt_hash
        or challenger_prompt.id != publication.challenger_prompt_version_id
        or challenger_prompt.role != "portfolio_decision"
        or challenger_prompt.release_channel != "challenger"
        or challenger_prompt.content_hash != publication.challenger_prompt_hash
        or sha256(challenger_prompt.content.encode()).hexdigest()
        != publication.challenger_prompt_hash
        or publication.challenger_model_config_id != frozen.model_config_id
    ):
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_CASE_NOT_PROSPECTIVE")
    document = frozen.model_dump(mode="json")
    # prompt_version is the synthesis contract version; the record's distinct
    # challenger release version is retained separately in the comparison Job.
    document["prompt_version_id"] = str(challenger_prompt.id)
    document["prompt_hash"] = challenger_prompt.content_hash
    challenger_input = PortfolioSynthesisInputV1.model_validate(document)
    return PortfolioChallengerCase(
        source_run_id=run.id,
        source_input_hash=run.input_hash,
        challenger_input_hash=canonical_portfolio_synthesis_hash(challenger_input),
        challenger_prompt_hash=challenger_prompt.content_hash,
        challenger_prompt_version=challenger_prompt.version,
        frozen_input=challenger_input,
        instructions=challenger_prompt.content,
    )
