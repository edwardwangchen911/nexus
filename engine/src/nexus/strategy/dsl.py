"""Compiler for the bounded, data-only NEXUS strategy DSL."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from importlib.resources import files
from types import MappingProxyType
from typing import Literal, cast

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

_FORBIDDEN_FIELD_TOKENS = ("python", "sql", "shell", "url", "import", "path")
_COMPARISON_OPERATORS = frozenset({"gt", "gte", "lt", "lte", "eq"})
_BOOLEAN_OPERATORS = frozenset({"all", "any", "not"})
_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
_MAX_AST_DEPTH = 8
_MAX_OPERATIONS = 64
_BASELINE_PARAMETER_BINDINGS = {
    "min_adv20_usd": "$.root.args[0].value",
    "min_close": "$.root.args[1].value",
    "target_weight": "$.target_weight.weight",
}


class StrategyDslError(ValueError):
    """A strategy document cannot be compiled into the restricted expression tree."""


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class FeatureDefinition(_FrozenModel):
    """One versioned numeric feature with explicit point-in-time semantics."""

    name: str = Field(max_length=100, pattern=r"^[a-z][a-z0-9_]*$")
    version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    adjustment: Literal["raw", "all", "derived_from_raw", "derived_from_all"]
    availability: Literal["exchange_close_plus_30m"]
    minimum: Decimal
    maximum: Decimal
    compatibility_class: str | None = Field(
        default=None, max_length=100, pattern=r"^[a-z][a-z0-9_]*$"
    )
    inputs: tuple[str, ...] = ()
    unit: str | None = Field(default=None, max_length=100, pattern=r"^[a-z][a-z0-9_]*$")
    minimum_history_sessions: int | None = Field(default=None, ge=1, le=10_000)
    availability_delay: Literal["exchange_close_plus_30m"] | None = None
    missing_policy: Literal["reject_instrument"] | None = None
    implementation_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _ordered_range(self) -> FeatureDefinition:
        if self.minimum > self.maximum:
            raise ValueError("feature minimum must not exceed maximum")
        if len(self.inputs) != len(set(self.inputs)):
            raise ValueError("feature inputs must be unique")
        return self


class FeatureRegistry:
    """Frozen allowlist used by the compiler; features cannot be resolved dynamically."""

    def __init__(self, definitions: Sequence[FeatureDefinition]) -> None:
        by_name = {definition.name: definition for definition in definitions}
        if len(by_name) != len(definitions):
            raise ValueError("feature names must be unique")
        self._definitions = MappingProxyType(by_name)

    def get(self, name: str) -> FeatureDefinition | None:
        return self._definitions.get(name)

    def definitions(self) -> tuple[FeatureDefinition, ...]:
        return tuple(self._definitions[name] for name in sorted(self._definitions))

    def identity(self, names: set[str]) -> dict[str, object]:
        return {
            name: self._definitions[name].model_dump(
                mode="json", exclude_none=True, exclude_defaults=True
            )
            for name in sorted(names)
        }


class ExpressionNode(_FrozenModel):
    """Typed internal node; it contains values, never executable source text."""

    op: Literal["all", "any", "not", "gt", "gte", "lt", "lte", "eq"]
    args: tuple[ExpressionNode, ...] = ()
    arg: ExpressionNode | None = None
    feature: str | None = None
    value: Decimal | None = None
    right_feature: str | None = None
    offset: int = 0


class UniversePolicy(_FrozenModel):
    key: Literal["us_liquid"]
    snapshot_selection: Literal["latest_published_effective"]
    eligibility_status: Literal["PASS"]
    asset_types: tuple[Literal["stock", "etf"], ...] = Field(min_length=1, max_length=2)
    etf_sector_exposure_required: bool

    @model_validator(mode="after")
    def _unique_asset_types(self) -> UniversePolicy:
        if len(self.asset_types) != len(set(self.asset_types)):
            raise ValueError("asset types must be unique")
        return self


class RankingPolicy(_FrozenModel):
    feature: str = Field(max_length=100, pattern=r"^[a-z][a-z0-9_]*$")
    direction: Literal["asc", "desc"]
    select_count: int = Field(ge=1, le=50)
    exit_buffer_rank: int = Field(ge=1, le=500)
    exit_after_consecutive_evaluations: int = Field(ge=1, le=52)

    @model_validator(mode="after")
    def _buffer_contains_selection(self) -> RankingPolicy:
        if self.exit_buffer_rank < self.select_count:
            raise ValueError("exit buffer must contain the selected ranks")
        return self


class RebalancePolicy(_FrozenModel):
    frequency: Literal["daily", "weekly", "monthly"]
    decision_session: Literal["last_exchange_session_of_iso_week", "last_exchange_session_of_month"]
    fill_timing: Literal["next_exchange_session_open"]

    @model_validator(mode="after")
    def _frequency_matches_decision_session(self) -> RebalancePolicy:
        expected = {
            "weekly": "last_exchange_session_of_iso_week",
            "monthly": "last_exchange_session_of_month",
        }
        if self.frequency == "daily" or expected[self.frequency] != self.decision_session:
            raise ValueError("rebalance frequency contradicts its decision session")
        return self


class HoldingPeriodPolicy(_FrozenModel):
    minimum_sessions: int = Field(ge=0, le=10_000)
    maximum_sessions: int | None = Field(default=None, ge=1, le=10_000)
    exit_evaluation: Literal["scheduled_and_event_driven"]

    @model_validator(mode="after")
    def _ordered(self) -> HoldingPeriodPolicy:
        if self.maximum_sessions is not None and self.minimum_sessions > self.maximum_sessions:
            raise ValueError("holding-period bounds must be ordered")
        return self


class PortfolioPolicy(_FrozenModel):
    max_positions: int = Field(ge=1, le=50)
    max_etfs: int = Field(ge=0, le=50)
    max_stocks_per_sector: int = Field(ge=1, le=50)
    sector_taxonomy: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    max_sector_weight: Decimal
    min_cash_weight: Decimal

    @model_validator(mode="after")
    def _valid_weights(self) -> PortfolioPolicy:
        if not Decimal(0) <= self.max_sector_weight <= Decimal(1):
            raise ValueError("max sector weight must be between zero and one")
        if not Decimal(0) <= self.min_cash_weight <= Decimal(1):
            raise ValueError("minimum cash weight must be between zero and one")
        return self


class ExitPolicy(_FrozenModel):
    trend_rule: ExpressionNode
    ranking_buffer_enabled: bool
    hard_risk_exit: Literal["forward_only"]


class ParameterDefinition(_FrozenModel):
    name: str = Field(max_length=100, pattern=r"^[a-z][a-z0-9_]*$")
    policy_ref: str = Field(
        max_length=255,
        pattern=r"^\$((\.[a-z][a-z0-9_]*)|(\[[0-9]+\]))+$",
    )
    value: Decimal
    minimum: Decimal
    maximum: Decimal

    @model_validator(mode="after")
    def _valid_range(self) -> ParameterDefinition:
        if self.minimum > self.maximum or not self.minimum <= self.value <= self.maximum:
            raise ValueError("parameter value must be within its declared range")
        return self


class TargetWeightPolicy(_FrozenModel):
    method: Literal["fixed_equal_weight"]
    weight: Decimal

    @model_validator(mode="after")
    def _valid_weight(self) -> TargetWeightPolicy:
        if not Decimal(0) < self.weight <= Decimal(1):
            raise ValueError("target weight must be positive and no greater than one")
        return self


class CompiledStrategy(_FrozenModel):
    """Canonical result whose hash covers the complete declarative policy."""

    strategy_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    expression_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    universe: UniversePolicy
    root: ExpressionNode
    ranking: RankingPolicy
    rebalance: RebalancePolicy
    holding_period: HoldingPeriodPolicy
    portfolio: PortfolioPolicy
    exits: ExitPolicy
    parameters: tuple[ParameterDefinition, ...]
    target_weight: TargetWeightPolicy

    @model_validator(mode="after")
    def _unique_parameters(self) -> CompiledStrategy:
        names = [parameter.name for parameter in self.parameters]
        if not 1 <= len(names) <= 32:
            raise ValueError("strategy parameters must contain between one and 32 definitions")
        if len(names) != len(set(names)):
            raise ValueError("parameter names must be unique")
        paths = [parameter.policy_ref for parameter in self.parameters]
        if len(paths) != len(set(paths)):
            raise ValueError("parameter policy paths must be unique")
        if self.ranking.select_count > self.portfolio.max_positions:
            raise ValueError("ranking selection cannot exceed maximum positions")
        if self.portfolio.max_etfs > self.portfolio.max_positions:
            raise ValueError("ETF cap cannot exceed maximum positions")
        return self


def baseline_feature_registry() -> FeatureRegistry:
    """Feature allowlist needed by the historical baseline compiler."""
    return FeatureRegistry(
        (
            FeatureDefinition(
                name="raw_close",
                version="raw_close_v1",
                adjustment="raw",
                availability="exchange_close_plus_30m",
                minimum=Decimal("0"),
                maximum=Decimal("1000000"),
            ),
            FeatureDefinition(
                name="adjusted_close",
                version="adjusted_close_v1",
                adjustment="all",
                availability="exchange_close_plus_30m",
                minimum=Decimal("0"),
                maximum=Decimal("1000000"),
            ),
            FeatureDefinition(
                name="adv20_usd",
                version="adv20_usd_v1",
                adjustment="derived_from_raw",
                availability="exchange_close_plus_30m",
                minimum=Decimal("0"),
                maximum=Decimal("1000000000000"),
            ),
            FeatureDefinition(
                name="sma200",
                version="sma200_all_v1",
                adjustment="derived_from_all",
                availability="exchange_close_plus_30m",
                minimum=Decimal("0"),
                maximum=Decimal("1000000"),
            ),
            FeatureDefinition(
                name="return_126",
                version="return_126_all_v1",
                adjustment="derived_from_all",
                availability="exchange_close_plus_30m",
                minimum=Decimal("-1"),
                maximum=Decimal("1000"),
            ),
            FeatureDefinition(
                name="momentum_126_skip5",
                version="momentum_126_skip5_all_v1",
                adjustment="derived_from_all",
                availability="exchange_close_plus_30m",
                minimum=Decimal("-1"),
                maximum=Decimal("1000"),
            ),
        )
    )


def evolution_feature_registry_v1() -> FeatureRegistry:
    """Feature authority with explicit availability metadata for Phase 2 strategies."""
    metadata: dict[str, dict[str, object]] = {
        "raw_close": {
            "compatibility_class": "price_level",
            "inputs": ("raw_close",),
            "unit": "usd_per_share",
            "minimum_history_sessions": 1,
        },
        "adjusted_close": {
            "compatibility_class": "price_level",
            "inputs": ("adjusted_close",),
            "unit": "usd_per_share",
            "minimum_history_sessions": 1,
        },
        "adv20_usd": {
            "compatibility_class": "liquidity_usd",
            "inputs": ("raw_close", "raw_volume"),
            "unit": "usd",
            "minimum_history_sessions": 20,
        },
        "sma200": {
            "compatibility_class": "trend_level",
            "inputs": ("adjusted_close",),
            "unit": "usd_per_share",
            "minimum_history_sessions": 200,
        },
        "return_126": {
            "compatibility_class": "momentum_rank",
            "inputs": ("adjusted_close",),
            "unit": "ratio",
            "minimum_history_sessions": 127,
        },
        "momentum_126_skip5": {
            "compatibility_class": "momentum_rank",
            "inputs": ("adjusted_close",),
            "unit": "ratio",
            "minimum_history_sessions": 127,
        },
    }
    definitions: list[FeatureDefinition] = []
    for definition in baseline_feature_registry().definitions():
        details = metadata[definition.name]
        implementation_hash = hashlib.sha256(
            f"nexus.feature.{definition.version}.phase2_v1".encode()
        ).hexdigest()
        definitions.append(
            definition.model_copy(
                update={
                    **details,
                    "availability_delay": "exchange_close_plus_30m",
                    "missing_policy": "reject_instrument",
                    "implementation_hash": implementation_hash,
                }
            )
        )
    return FeatureRegistry(tuple(definitions))


def baseline_v1_document() -> dict[str, object]:
    """Return the complete, immutable published policy for the historical baseline."""
    return {
        "version": "baseline_v1",
        "universe": {
            "key": "us_liquid",
            "snapshot_selection": "latest_published_effective",
            "eligibility_status": "PASS",
            "asset_types": ["stock", "etf"],
            "etf_sector_exposure_required": True,
        },
        "root": {
            "op": "all",
            "args": [
                {"op": "gte", "feature": "adv20_usd", "value": "50000000"},
                {"op": "gte", "feature": "raw_close", "value": "5"},
                {"op": "gt", "feature": "adjusted_close", "right_feature": "sma200"},
                {"op": "gt", "feature": "return_126", "value": "0"},
            ],
        },
        "ranking": {
            "feature": "momentum_126_skip5",
            "direction": "desc",
            "select_count": 6,
            "exit_buffer_rank": 20,
            "exit_after_consecutive_evaluations": 2,
        },
        "rebalance": {
            "frequency": "weekly",
            "decision_session": "last_exchange_session_of_iso_week",
            "fill_timing": "next_exchange_session_open",
        },
        "holding_period": {
            "minimum_sessions": 0,
            "maximum_sessions": 10000,
            "exit_evaluation": "scheduled_and_event_driven",
        },
        "portfolio": {
            "max_positions": 6,
            "max_etfs": 2,
            "max_stocks_per_sector": 2,
            "sector_taxonomy": "NEXUS_SECTOR_V1",
            "max_sector_weight": "0.30",
            "min_cash_weight": "0.25",
        },
        "exits": {
            "trend_rule": {
                "op": "gt",
                "feature": "adjusted_close",
                "right_feature": "sma200",
            },
            "ranking_buffer_enabled": True,
            "hard_risk_exit": "forward_only",
        },
        "parameters": [
            {
                "name": "min_adv20_usd",
                "policy_ref": "$.root.args[0].value",
                "value": "50000000",
                "minimum": "10000000",
                "maximum": "200000000",
            },
            {
                "name": "min_close",
                "policy_ref": "$.root.args[1].value",
                "value": "5",
                "minimum": "1",
                "maximum": "100",
            },
            {
                "name": "target_weight",
                "policy_ref": "$.target_weight.weight",
                "value": "0.125",
                "minimum": "0.05",
                "maximum": "0.20",
            },
        ],
        "target_weight": {"method": "fixed_equal_weight", "weight": "0.125"},
    }


def compile_strategy(document: Mapping[str, object], registry: FeatureRegistry) -> CompiledStrategy:
    """Validate and canonicalize a complete data-only strategy without dynamic execution."""
    _validate_json_schema(document)
    _reject_forbidden_fields(document)
    required = {
        "version",
        "universe",
        "root",
        "ranking",
        "rebalance",
        "holding_period",
        "portfolio",
        "exits",
        "parameters",
        "target_weight",
    }
    if set(document) != required:
        raise StrategyDslError("DOCUMENT_FIELDS_INVALID")
    version = document.get("version")
    if not isinstance(version, str) or _VERSION_PATTERN.fullmatch(version) is None:
        raise StrategyDslError("VERSION_INVALID")
    counter = [0]
    root = _compile_node(
        _mapping(document.get("root")), registry=registry, depth=1, counter=counter
    )
    exits_raw = _mapping(document.get("exits"))
    if set(exits_raw) != {"trend_rule", "ranking_buffer_enabled", "hard_risk_exit"}:
        raise StrategyDslError("EXIT_FIELDS_INVALID")
    trend_rule = _compile_node(
        _mapping(exits_raw.get("trend_rule")), registry=registry, depth=1, counter=counter
    )
    ranking_raw = _mapping(document.get("ranking"))
    ranking_feature = ranking_raw.get("feature")
    if not isinstance(ranking_feature, str) or registry.get(ranking_feature) is None:
        raise StrategyDslError("UNKNOWN_FEATURE")
    try:
        universe = UniversePolicy.model_validate(document.get("universe"))
        ranking = RankingPolicy.model_validate(ranking_raw)
        rebalance = RebalancePolicy.model_validate(document.get("rebalance"))
        holding_period = HoldingPeriodPolicy.model_validate(document.get("holding_period"))
        portfolio = PortfolioPolicy.model_validate(document.get("portfolio"))
        exits = ExitPolicy.model_validate({**exits_raw, "trend_rule": trend_rule})
        target_weight = TargetWeightPolicy.model_validate(document.get("target_weight"))
        parameters_raw = document.get("parameters")
        if not isinstance(parameters_raw, Sequence) or isinstance(parameters_raw, str | bytes):
            raise StrategyDslError("PARAMETERS_INVALID")
        parameters = tuple(ParameterDefinition.model_validate(item) for item in parameters_raw)
    except ValidationError as error:
        raise StrategyDslError("POLICY_INVALID") from error
    if portfolio.max_positions * target_weight.weight > Decimal(1) - portfolio.min_cash_weight:
        raise StrategyDslError("PORTFOLIO_WEIGHT_LIMIT_EXCEEDED")
    if {parameter.name: parameter.policy_ref for parameter in parameters} != (
        _BASELINE_PARAMETER_BINDINGS
    ):
        raise StrategyDslError("PARAMETER_BINDING_INVALID")
    for parameter in parameters:
        if _decimal(_resolve_policy_path(document, parameter.policy_ref)) != parameter.value:
            raise StrategyDslError("PARAMETER_BINDING_MISMATCH")
    try:
        provisional = CompiledStrategy(
            strategy_version=version,
            expression_hash="0" * 64,
            universe=universe,
            root=root,
            ranking=ranking,
            rebalance=rebalance,
            holding_period=holding_period,
            portfolio=portfolio,
            exits=exits,
            parameters=parameters,
            target_weight=target_weight,
        )
    except ValidationError as error:
        raise StrategyDslError("POLICY_INVALID") from error
    referenced_features = _referenced_feature_names(provisional)
    identity = {
        "document": _canonical_document(provisional),
        "features": registry.identity(referenced_features),
    }
    return provisional.model_copy(update={"expression_hash": _hash(identity)})


def verify_compiled_strategy(strategy: CompiledStrategy, registry: FeatureRegistry) -> None:
    """Recompile a possibly caller-constructed object and reject forged policy identities."""
    rebuilt = compile_strategy(_canonical_document(strategy), registry)
    if rebuilt != strategy:
        raise StrategyDslError("COMPILED_STRATEGY_INTEGRITY_FAILED")


def _canonical_document(strategy: CompiledStrategy) -> dict[str, object]:
    payload = strategy.model_dump(
        mode="json",
        exclude={"expression_hash"},
        exclude_none=True,
        exclude_defaults=True,
    )
    payload["version"] = payload.pop("strategy_version")
    return payload


def _referenced_feature_names(strategy: CompiledStrategy) -> set[str]:
    names = {strategy.ranking.feature}

    def visit(node: ExpressionNode) -> None:
        if node.feature is not None:
            names.add(node.feature)
        if node.right_feature is not None:
            names.add(node.right_feature)
        for child in node.args:
            visit(child)
        if node.arg is not None:
            visit(node.arg)

    visit(strategy.root)
    visit(strategy.exits.trend_rule)
    return names


def _reject_forbidden_fields(value: object) -> None:
    pending = [value]
    visited = 0
    while pending:
        current = pending.pop()
        visited += 1
        if visited > 10_000:
            raise StrategyDslError("SCHEMA_INVALID")
        if isinstance(current, Mapping):
            for key, nested in current.items():
                normalized = "".join(
                    character for character in str(key).lower() if character.isalnum()
                )
                if any(token in normalized for token in _FORBIDDEN_FIELD_TOKENS):
                    raise StrategyDslError("FORBIDDEN_FIELD")
                pending.append(nested)
        elif isinstance(current, Sequence) and not isinstance(current, str | bytes | bytearray):
            pending.extend(current)
        elif isinstance(current, float):
            raise StrategyDslError("BINARY_FLOAT_FORBIDDEN")


@lru_cache(maxsize=1)
def _schema_validator() -> Draft202012Validator:
    try:
        payload = files("nexus.strategy").joinpath("strategy-dsl.schema.json").read_text()
    except FileNotFoundError:
        try:
            payload = (
                files("nexus.strategy")
                .joinpath("../../../../contracts/strategy/strategy-dsl.schema.json")
                .read_text()
            )
        except FileNotFoundError as error:
            raise StrategyDslError("STRATEGY_SCHEMA_UNAVAILABLE") from error
    schema = json.loads(payload)
    return Draft202012Validator(schema)


def _validate_json_schema(document: Mapping[str, object]) -> None:
    try:
        errors = sorted(_schema_validator().iter_errors(document), key=lambda item: list(item.path))
    except RecursionError as recursion_error:
        raise StrategyDslError("SCHEMA_INVALID") from recursion_error
    if not errors:
        return
    _reject_forbidden_fields(document)
    first = errors[0]
    path = tuple(first.absolute_path)
    descendants = [first]
    for descendant in descendants:
        descendants.extend(descendant.context)
    if path == ("version",):
        raise StrategyDslError("VERSION_INVALID")
    if any(tuple(error.absolute_path)[-1:] == ("offset",) for error in descendants):
        raise StrategyDslError("LOOKAHEAD_FORBIDDEN")
    if any(
        tuple(error.absolute_path)[-1:] == ("args",) and error.validator == "maxItems"
        for error in descendants
    ):
        raise StrategyDslError("OPERATION_LIMIT_EXCEEDED")
    if path and path[0] in {"parameters", "ranking", "rebalance", "holding_period"}:
        raise StrategyDslError("POLICY_INVALID")
    raise StrategyDslError("SCHEMA_INVALID")


_POLICY_PATH_TOKEN = re.compile(r"\.([a-z][a-z0-9_]*)|\[([0-9]+)\]")


def _resolve_policy_path(document: Mapping[str, object], path: str) -> object:
    current: object = document
    position = 1
    for match in _POLICY_PATH_TOKEN.finditer(path, position):
        if match.start() != position:
            raise StrategyDslError("PARAMETER_BINDING_INVALID")
        key, index = match.groups()
        if key is not None:
            if not isinstance(current, Mapping) or key not in current:
                raise StrategyDslError("PARAMETER_BINDING_INVALID")
            current = current[key]
        else:
            if (
                not isinstance(current, Sequence)
                or isinstance(current, str | bytes | bytearray)
                or int(index) >= len(current)
            ):
                raise StrategyDslError("PARAMETER_BINDING_INVALID")
            current = current[int(index)]
        position = match.end()
    if position != len(path):
        raise StrategyDslError("PARAMETER_BINDING_INVALID")
    return current


def _compile_node(
    raw: Mapping[str, object], *, registry: FeatureRegistry, depth: int, counter: list[int]
) -> ExpressionNode:
    if depth > _MAX_AST_DEPTH:
        raise StrategyDslError("AST_DEPTH_EXCEEDED")
    counter[0] += 1
    if counter[0] > _MAX_OPERATIONS:
        raise StrategyDslError("OPERATION_LIMIT_EXCEEDED")
    op = raw.get("op")
    if not isinstance(op, str) or op not in _COMPARISON_OPERATORS | _BOOLEAN_OPERATORS:
        raise StrategyDslError("OPERATOR_INVALID")
    if op in {"all", "any"}:
        if set(raw) != {"op", "args"}:
            raise StrategyDslError("NODE_FIELDS_INVALID")
        args = raw.get("args")
        if not isinstance(args, Sequence) or isinstance(args, str | bytes) or not args:
            raise StrategyDslError("BOOLEAN_ARGUMENTS_INVALID")
        return ExpressionNode(
            op=cast(Literal["all", "any"], op),
            args=tuple(
                _compile_node(_mapping(item), registry=registry, depth=depth + 1, counter=counter)
                for item in args
            ),
        )
    if op == "not":
        if set(raw) != {"op", "arg"}:
            raise StrategyDslError("NODE_FIELDS_INVALID")
        return ExpressionNode(
            op="not",
            arg=_compile_node(
                _mapping(raw.get("arg")), registry=registry, depth=depth + 1, counter=counter
            ),
        )
    allowed = {"op", "feature", "value", "right_feature", "offset"}
    if not set(raw) <= allowed or not {"op", "feature"} <= set(raw):
        raise StrategyDslError("NODE_FIELDS_INVALID")
    feature = raw.get("feature")
    if not isinstance(feature, str) or registry.get(feature) is None:
        raise StrategyDslError("UNKNOWN_FEATURE")
    offset = raw.get("offset", 0)
    if not isinstance(offset, int) or isinstance(offset, bool):
        raise StrategyDslError("OFFSET_INVALID")
    if offset > 0:
        raise StrategyDslError("LOOKAHEAD_FORBIDDEN")
    if offset < -10_000:
        raise StrategyDslError("PARAMETER_OUT_OF_RANGE")
    has_value = "value" in raw
    has_right_feature = "right_feature" in raw
    if has_value == has_right_feature:
        raise StrategyDslError("COMPARISON_OPERAND_INVALID")
    value: Decimal | None = None
    right_feature: str | None = None
    if has_value:
        value = _decimal(raw.get("value"))
        definition = registry.get(feature)
        assert definition is not None
        if not definition.minimum <= value <= definition.maximum:
            raise StrategyDslError("PARAMETER_OUT_OF_RANGE")
    else:
        candidate = raw.get("right_feature")
        if not isinstance(candidate, str) or registry.get(candidate) is None:
            raise StrategyDslError("UNKNOWN_FEATURE")
        right_feature = candidate
    return ExpressionNode(
        op=cast(Literal["gt", "gte", "lt", "lte", "eq"], op),
        feature=feature,
        value=value,
        right_feature=right_feature,
        offset=offset,
    )


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise StrategyDslError("NODE_INVALID")
    return value


def _decimal(value: object) -> Decimal:
    if not isinstance(value, str | int) or isinstance(value, bool):
        raise StrategyDslError("DECIMAL_STRING_REQUIRED")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise StrategyDslError("DECIMAL_INVALID") from error
    if not parsed.is_finite():
        raise StrategyDslError("DECIMAL_INVALID")
    return parsed


def _hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()
