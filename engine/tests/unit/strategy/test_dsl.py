"""Restricted strategy DSL compiler behavior."""

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from nexus.strategy.dsl import (
    FeatureRegistry,
    StrategyDslError,
    baseline_feature_registry,
    baseline_v1_document,
    compile_strategy,
)


@pytest.fixture
def feature_registry() -> FeatureRegistry:
    return baseline_feature_registry()


@pytest.mark.parametrize("field", ["python", "sql", "shell", "url", "import", "path"])
def test_strategy_dsl_rejects_executable_fields(
    feature_registry: FeatureRegistry, field: str
) -> None:
    with pytest.raises(StrategyDslError, match="FORBIDDEN_FIELD"):
        compile_strategy({"version": "1", field: "import os"}, feature_registry)


def test_strategy_dsl_builds_typed_nodes_and_a_stable_hash(
    feature_registry: FeatureRegistry,
) -> None:
    document = baseline_v1_document()

    first = compile_strategy(document, feature_registry)
    second = compile_strategy(dict(reversed(tuple(document.items()))), feature_registry)

    assert first.strategy_version == "baseline_v1"
    assert first.root.op == "all"
    assert first.root.args[0].feature == "adv20_usd"
    assert first.ranking.feature == "momentum_126_skip5"
    assert first.portfolio.max_positions == 6
    assert first.holding_period.maximum_sessions == 10_000
    assert first.expression_hash == second.expression_hash
    assert len(first.expression_hash) == 64


def test_strategy_hash_binds_the_referenced_feature_semantics() -> None:
    document = baseline_v1_document()
    canonical = baseline_feature_registry()
    changed = FeatureRegistry(
        tuple(
            definition.model_copy(
                update={"version": "raw_close_v2"} if definition.name == "raw_close" else {}
            )
            for definition in canonical.definitions()
        )
    )

    assert (
        compile_strategy(document, canonical).expression_hash
        != compile_strategy(document, changed).expression_hash
    )


def test_strategy_dsl_rejects_excessive_depth(feature_registry: FeatureRegistry) -> None:
    root: dict[str, object] = {
        "op": "gte",
        "feature": "raw_close",
        "value": "5",
    }
    for _ in range(8):
        root = {"op": "not", "arg": root}

    with pytest.raises(StrategyDslError, match="AST_DEPTH_EXCEEDED"):
        compile_strategy({**baseline_v1_document(), "version": "1", "root": root}, feature_registry)


def test_strategy_dsl_rejects_excessive_operations(feature_registry: FeatureRegistry) -> None:
    predicate = {"op": "gte", "feature": "raw_close", "value": "5"}
    with pytest.raises(StrategyDslError, match="OPERATION_LIMIT_EXCEEDED"):
        compile_strategy(
            {
                **baseline_v1_document(),
                "version": "1",
                "root": {"op": "all", "args": [predicate] * 65},
            },
            feature_registry,
        )


def test_strategy_dsl_rejects_unknown_features(feature_registry: FeatureRegistry) -> None:
    with pytest.raises(StrategyDslError, match="UNKNOWN_FEATURE"):
        compile_strategy(
            {
                **baseline_v1_document(),
                "version": "1",
                "root": {"op": "gte", "feature": "future_alpha", "value": "0"},
            },
            feature_registry,
        )


def test_strategy_dsl_rejects_out_of_range_parameters(
    feature_registry: FeatureRegistry,
) -> None:
    with pytest.raises(StrategyDslError, match="PARAMETER_OUT_OF_RANGE"):
        compile_strategy(
            {
                **baseline_v1_document(),
                "version": "1",
                "root": {"op": "gte", "feature": "raw_close", "value": "1000001"},
            },
            feature_registry,
        )


def test_strategy_dsl_rejects_lookahead(feature_registry: FeatureRegistry) -> None:
    with pytest.raises(StrategyDslError, match="LOOKAHEAD_FORBIDDEN"):
        compile_strategy(
            {
                **baseline_v1_document(),
                "version": "1",
                "root": {"op": "gte", "feature": "raw_close", "value": "5", "offset": 1},
            },
            feature_registry,
        )


def test_strategy_dsl_schema_accepts_only_the_data_expression_contract() -> None:
    root = Path(__file__).resolve().parents[4]
    schema = json.loads((root / "contracts/strategy/strategy-dsl.schema.json").read_text())
    validator = Draft202012Validator(schema)
    document = baseline_v1_document()

    assert list(validator.iter_errors(document)) == []
    assert list(validator.iter_errors({"version": "1", "python": "import os"}))


def test_strategy_dsl_compiler_and_schema_reject_the_same_invalid_version(
    feature_registry: FeatureRegistry,
) -> None:
    document = {**baseline_v1_document(), "version": "contains space"}
    root = Path(__file__).resolve().parents[4]
    schema = json.loads((root / "contracts/strategy/strategy-dsl.schema.json").read_text())

    assert list(Draft202012Validator(schema).iter_errors(document))
    with pytest.raises(StrategyDslError, match="VERSION_INVALID"):
        compile_strategy(document, feature_registry)


@pytest.mark.parametrize("invalid", ["1e3", "01", "-01", "+1"])
def test_strategy_dsl_runs_the_json_schema_before_ast_compilation(
    feature_registry: FeatureRegistry, invalid: str
) -> None:
    document = baseline_v1_document()
    document["root"]["args"][1]["value"] = invalid

    with pytest.raises(StrategyDslError, match="SCHEMA_INVALID"):
        compile_strategy(document, feature_registry)


def test_strategy_parameter_must_bind_the_exact_policy_node(
    feature_registry: FeatureRegistry,
) -> None:
    document = baseline_v1_document()
    document["parameters"][0]["value"] = "60000000"

    with pytest.raises(StrategyDslError, match="PARAMETER_BINDING_MISMATCH"):
        compile_strategy(document, feature_registry)


@pytest.mark.parametrize(
    "policy_ref",
    ("$.parameters[0].value", "$.parameters[1].minimum"),
)
def test_strategy_parameter_cannot_bind_parameter_metadata(
    feature_registry: FeatureRegistry, policy_ref: str
) -> None:
    document = baseline_v1_document()
    document["parameters"][0]["policy_ref"] = policy_ref

    with pytest.raises(StrategyDslError, match="PARAMETER_BINDING_INVALID"):
        compile_strategy(document, feature_registry)


def test_strategy_dsl_rejects_pathologically_deep_json_without_recursion_escape(
    feature_registry: FeatureRegistry,
) -> None:
    node: dict[str, object] = {"op": "gte", "feature": "raw_close", "value": "5"}
    for _ in range(1_100):
        node = {"op": "not", "arg": node}

    with pytest.raises(StrategyDslError, match="SCHEMA_INVALID|AST_DEPTH_EXCEEDED"):
        compile_strategy({**baseline_v1_document(), "root": node}, feature_registry)


def test_rebalance_frequency_cannot_contradict_decision_session(
    feature_registry: FeatureRegistry,
) -> None:
    document = baseline_v1_document()
    document["rebalance"]["frequency"] = "monthly"

    with pytest.raises(StrategyDslError, match="POLICY_INVALID"):
        compile_strategy(document, feature_registry)


@pytest.mark.parametrize(
    "override",
    (
        {"parameters": []},
        {
            "parameters": [
                {"name": f"parameter_{index}", "value": "1", "minimum": "0", "maximum": "2"}
                for index in range(33)
            ]
        },
        {"ranking": {**baseline_v1_document()["ranking"], "select_count": 7}},
    ),
)
def test_strategy_dsl_rejects_incomplete_or_internally_inconsistent_policy(
    feature_registry: FeatureRegistry,
    override: dict[str, object],
) -> None:
    with pytest.raises(StrategyDslError, match="POLICY_INVALID"):
        compile_strategy({**baseline_v1_document(), **override}, feature_registry)
