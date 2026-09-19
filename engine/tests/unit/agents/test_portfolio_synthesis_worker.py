"""One-turn synthesis prompt and transport boundaries."""

from pathlib import Path


def test_prompt_bounds_investment_choices_without_execution_authority():
    path = (
        Path(__file__).parents[3] / "src/nexus/agents/prompts/portfolio-management-synthesis-v1.md"
    )
    assert path.exists(), "the exact registered synthesis prompt must exist"
    prompt = " ".join(path.read_text().lower().split())
    for boundary in (
        "weights",
        "amounts",
        "quantities",
        "orders",
        "risk overrides",
        "unbound instruments",
        "strict priority",
        "abstain",
    ):
        assert boundary in prompt


def test_synthesis_worker_exposes_explicit_once_entrypoint():
    from nexus.worker import run_portfolio_synthesis_worker_once

    assert callable(run_portfolio_synthesis_worker_once)
