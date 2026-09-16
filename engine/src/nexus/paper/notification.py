"""Closed, event-time facts used to render internal-paper notifications."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Annotated, Literal, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.paper.models import exact_decimal
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.paper import (
    PaperOrderRecord,
    StrategyDecisionAgentOutputRecord,
    StrategyDecisionRecord,
)
from nexus.risk.models import PaperPortfolioProjection, RiskEvaluation
from nexus.strategy.intents import ResearchCondition, ResearchIntent

NotificationStatus = Literal["risk_rejected", "filled", "cancelled"]
NotificationPriceKind = Literal["reference", "worst_case", "average_fill"]
NotificationBalanceKind = Literal["projected_after", "actual_after", "unchanged"]
NotificationSymbol = Annotated[
    str,
    StringConstraints(pattern=r"^[A-Z0-9][A-Z0-9.\-]{0,31}$"),
]

_CANONICAL_DECIMAL = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]{1,12})?$")
_SAFE_RULE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,99}$")


class PaperNotificationError(ValueError):
    """Value-free notification contract error safe for outbox recording."""


def _exact_notification_decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, str):
        if _CANONICAL_DECIMAL.fullmatch(value) is None:
            raise ValueError(f"{field} must be a canonical decimal string")
        try:
            value = Decimal(value)
        except InvalidOperation as error:
            raise ValueError(f"{field} must be a canonical decimal string") from error
    return exact_decimal(value, field=field)


def _require_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must use UTC")
    return value


def _decimal_text(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in {"-0", ""} else rendered


class _FrozenNotificationModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class PaperNotificationDecisionContext(_FrozenNotificationModel):
    """User-visible decision facts linked to one order instrument."""

    symbol: NotificationSymbol
    agent_cycle_id: UUID | None
    agent_reason: str | None = Field(default=None, min_length=1, max_length=500)
    invalidation: str | None = Field(default=None, min_length=1, max_length=500)


class PaperNotificationFill(_FrozenNotificationModel):
    """Minimal immutable fill facts needed by a terminal notification."""

    quantity: Decimal
    notional: Decimal
    fee: Decimal

    @field_validator("quantity", "notional", "fee", mode="before")
    @classmethod
    def _exact_values(cls, value: object, info: ValidationInfo) -> Decimal:
        return _exact_notification_decimal(value, field=info.field_name or "fill value")

    @model_validator(mode="after")
    def _valid_values(self) -> PaperNotificationFill:
        if self.quantity <= 0 or self.quantity != self.quantity.to_integral_value():
            raise ValueError("fill quantity must be positive whole shares")
        if self.notional <= 0 or self.fee < 0:
            raise ValueError("fill notional and fee must be valid")
        return self


class PaperNotificationPrice(_FrozenNotificationModel):
    kind: NotificationPriceKind
    value: Decimal
    currency: Literal["USD"]

    @field_validator("value", mode="before")
    @classmethod
    def _exact_value(cls, value: object) -> Decimal:
        result = _exact_notification_decimal(value, field="price value")
        if result <= 0:
            raise ValueError("price value must be positive")
        return result

    @field_serializer("value", when_used="json")
    def _serialize_value(self, value: Decimal) -> str:
        return _decimal_text(value)


class PaperNotificationCash(_FrozenNotificationModel):
    kind: NotificationBalanceKind
    available: Decimal

    @field_validator("available", mode="before")
    @classmethod
    def _exact_available(cls, value: object) -> Decimal:
        result = _exact_notification_decimal(value, field="available cash")
        if result < 0:
            raise ValueError("available cash must not be negative")
        return result

    @field_serializer("available", when_used="json")
    def _serialize_available(self, value: Decimal) -> str:
        return _decimal_text(value)


class PaperNotificationPosition(_FrozenNotificationModel):
    kind: NotificationBalanceKind
    quantity: Decimal
    portfolio_ratio: Decimal | None

    @field_validator("quantity", "portfolio_ratio", mode="before")
    @classmethod
    def _exact_values(cls, value: object, info: ValidationInfo) -> Decimal | None:
        if value is None:
            return None
        return _exact_notification_decimal(value, field=info.field_name or "position value")

    @model_validator(mode="after")
    def _valid_values(self) -> PaperNotificationPosition:
        if self.quantity < 0 or self.quantity != self.quantity.to_integral_value():
            raise ValueError("position quantity must be non-negative whole shares")
        if self.portfolio_ratio is not None and not (
            Decimal(0) <= self.portfolio_ratio <= Decimal(1)
        ):
            raise ValueError("portfolio ratio must be between zero and one")
        return self

    @field_serializer("quantity", "portfolio_ratio", when_used="json")
    def _serialize_values(self, value: Decimal | None) -> str | None:
        return None if value is None else _decimal_text(value)


class PaperOrderNotificationSnapshot(_FrozenNotificationModel):
    """Frozen business facts sufficient to render one order outcome."""

    schema_version: Literal["paper_notification_v1"]
    mode: Literal["internal_paper"]
    order_id: UUID
    agent_cycle_id: UUID | None
    occurred_at: datetime
    symbol: NotificationSymbol
    side: Literal["buy", "sell"]
    status: NotificationStatus
    requested_quantity: Decimal
    approved_quantity: Decimal
    filled_quantity: Decimal
    remaining_quantity: Decimal
    price: PaperNotificationPrice
    notional: Decimal
    fee: Decimal | None
    cash: PaperNotificationCash
    position: PaperNotificationPosition
    agent_reason: str | None = Field(default=None, min_length=1, max_length=500)
    invalidation: str | None = Field(default=None, min_length=1, max_length=500)
    risk_reason: str | None = Field(default=None, min_length=1, max_length=500)

    @field_validator(
        "requested_quantity",
        "approved_quantity",
        "filled_quantity",
        "remaining_quantity",
        "notional",
        "fee",
        mode="before",
    )
    @classmethod
    def _exact_values(cls, value: object, info: ValidationInfo) -> Decimal | None:
        if value is None:
            return None
        return _exact_notification_decimal(value, field=info.field_name or "notification value")

    @field_validator("occurred_at")
    @classmethod
    def _occurred_at_is_utc(cls, value: datetime) -> datetime:
        return _require_utc(value, field="occurred_at")

    @field_serializer(
        "requested_quantity",
        "approved_quantity",
        "filled_quantity",
        "remaining_quantity",
        "notional",
        "fee",
        when_used="json",
    )
    def _serialize_values(self, value: Decimal | None) -> str | None:
        return None if value is None else _decimal_text(value)

    @model_validator(mode="after")
    def _valid_outcome(self) -> PaperOrderNotificationSnapshot:
        quantities = (
            self.requested_quantity,
            self.approved_quantity,
            self.filled_quantity,
            self.remaining_quantity,
        )
        if any(value < 0 or value != value.to_integral_value() for value in quantities):
            raise ValueError("notification quantities must be non-negative whole shares")
        if self.requested_quantity <= 0:
            raise ValueError("requested quantity must be positive")
        if self.approved_quantity > self.requested_quantity:
            raise ValueError("approved quantity must not exceed requested quantity")
        if self.filled_quantity + self.remaining_quantity != self.approved_quantity:
            raise ValueError("filled and remaining quantity must equal approved quantity")
        if self.notional < 0 or self.fee is not None and self.fee < 0:
            raise ValueError("notional and fee must not be negative")

        if self.status == "filled":
            if (
                self.price.kind != "average_fill"
                or self.cash.kind != "actual_after"
                or self.position.kind != "actual_after"
                or self.remaining_quantity != 0
                or self.filled_quantity <= 0
                or self.fee is None
                or self.notional <= 0
            ):
                raise ValueError("filled notification requires actual terminal values")
        elif self.status == "risk_rejected":
            if (
                self.price.kind not in {"reference", "worst_case"}
                or self.cash.kind != "unchanged"
                or self.position.kind != "unchanged"
                or any(
                    value != 0
                    for value in (
                        self.approved_quantity,
                        self.filled_quantity,
                        self.remaining_quantity,
                    )
                )
                or self.fee is not None
                or self.risk_reason is None
                or self.notional <= 0
            ):
                raise ValueError("risk rejection requires unchanged account values")
        else:
            if self.cash.kind != "actual_after" or self.position.kind != "actual_after":
                raise ValueError("cancelled notification requires actual account values")
            if self.filled_quantity > 0 and (
                self.price.kind != "average_fill" or self.fee is None or self.notional <= 0
            ):
                raise ValueError("partially filled cancellation requires fill values")
            if self.filled_quantity == 0 and (
                self.price.kind not in {"reference", "worst_case"}
                or self.fee is not None
                or self.notional != 0
            ):
                raise ValueError("unfilled cancellation requires reference values")
        return self


def _bounded_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized[:500] or None


def _render_invalidation(condition: ResearchCondition | None) -> str | None:
    if condition is None or not isinstance(condition.threshold, bool):
        return None
    fixed = {
        ("committee.evidence_valid", "is_false", True, "boolean"): "研究证据不再有效。",
        ("committee.support_ready", "is_false", True, "boolean"): "研究支持条件不再成立。",
        ("committee.risk_triggered", "is_true", True, "boolean"): "研究风险条件已触发。",
        (
            "portfolio.top1_warning_exceeded",
            "is_true",
            True,
            "boolean",
        ): "单一标的仓位已超过预警阈值。",
        (
            "portfolio.top1_critical_exceeded",
            "is_true",
            True,
            "boolean",
        ): "单一标的仓位已超过严重阈值。",
    }
    return fixed.get((condition.signal, condition.operator, condition.threshold, condition.unit))


def decision_context_from_research_intent(
    intent: ResearchIntent,
    *,
    agent_cycle_id: UUID,
) -> PaperNotificationDecisionContext:
    """Extract bounded copy from an already validated frozen research intent."""
    assessment = intent.synthesis_assessment
    reason = (
        assessment.claims[0].text
        if assessment is not None and assessment.claims
        else intent.theses[0].text
        if intent.theses
        else None
    )
    condition = intent.invalidation_conditions[0] if intent.invalidation_conditions else None
    return PaperNotificationDecisionContext(
        symbol=intent.display_symbol,
        agent_cycle_id=agent_cycle_id,
        agent_reason=_bounded_text(reason),
        invalidation=_render_invalidation(condition),
    )


async def build_order_decision_context(
    session: AsyncSession,
    *,
    strategy_decision_id: UUID,
    instrument_id: UUID,
    decision_session: date,
) -> PaperNotificationDecisionContext:
    """Load the exact symbol and optional Agent explanation bound to a decision."""
    decision = await session.get(
        StrategyDecisionRecord,
        strategy_decision_id,
        populate_existing=True,
    )
    if decision is None or decision.decision_session != decision_session:
        raise ValueError("paper notification decision context is invalid")

    if decision.decision_origin == "agent_research":
        outputs = tuple(
            await session.scalars(
                select(AgentResearchOutputRecord)
                .join(
                    StrategyDecisionAgentOutputRecord,
                    StrategyDecisionAgentOutputRecord.agent_research_output_id
                    == AgentResearchOutputRecord.id,
                )
                .join(
                    AgentResearchRunRecord,
                    AgentResearchRunRecord.id == AgentResearchOutputRecord.run_id,
                )
                .where(
                    StrategyDecisionAgentOutputRecord.strategy_decision_id == strategy_decision_id,
                    AgentResearchRunRecord.instrument_id == instrument_id,
                )
            )
        )
        if len(outputs) != 1 or outputs[0].intent is None:
            raise ValueError("paper notification Agent context is invalid")
        intent = ResearchIntent.model_validate(outputs[0].intent)
        if intent.instrument_id != instrument_id:
            raise ValueError("paper notification Agent context is invalid")
        if decision.agent_paper_cycle_id is None:
            raise ValueError("paper notification Agent cycle is missing")
        return decision_context_from_research_intent(
            intent,
            agent_cycle_id=decision.agent_paper_cycle_id,
        )

    if decision.decision_origin != "registered_strategy":
        raise ValueError("paper notification decision origin is invalid")
    aliases = tuple(
        await session.scalars(
            select(InstrumentAlias)
            .where(
                InstrumentAlias.instrument_id == instrument_id,
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= decision_session,
                (
                    InstrumentAlias.valid_to.is_(None)
                    | (InstrumentAlias.valid_to >= decision_session)
                ),
                InstrumentAlias.created_at <= decision.decision_time,
            )
            .order_by(InstrumentAlias.valid_from.desc(), InstrumentAlias.id.desc())
        )
    )
    if len(aliases) != 1:
        raise ValueError("paper notification ticker context is invalid")
    return PaperNotificationDecisionContext(
        symbol=aliases[0].symbol,
        agent_cycle_id=None,
        agent_reason=None,
        invalidation=None,
    )


def _percentage(value: Decimal) -> str:
    return _decimal_text(value * Decimal(100))


def _risk_rejection_reason(evaluation: RiskEvaluation) -> str:
    failed = next((rule for rule in evaluation.rules if not rule.passed), None)
    if failed is None:
        return "风控规则未通过。"
    if failed.rule_code == "DRAWDOWN_LOCKED":
        return "账户处于回撤锁定状态。"
    if failed.rule_code == "INSUFFICIENT_POSITION":
        return "可卖持仓不足。"
    if failed.rule_code == "SINGLE_NAME_LIMIT" and failed.limit is not None:
        return f"成交后单一标的仓位将超过 {_percentage(failed.limit)}% 上限。"
    if failed.rule_code == "SECTOR_LIMIT" and failed.limit is not None:
        return f"成交后行业暴露将超过 {_percentage(failed.limit)}% 上限。"
    if failed.rule_code == "CASH_FLOOR" and failed.limit is not None:
        return f"成交后可用现金比例将低于 {_percentage(failed.limit)}% 下限。"
    if failed.rule_code == "BELOW_MINIMUM_ORDER" and failed.limit is not None:
        return f"订单金额低于最小金额 ${_decimal_text(failed.limit)}。"
    if _SAFE_RULE_CODE.fullmatch(failed.rule_code) is not None:
        return f"风控规则未通过（{failed.rule_code}）。"
    return "风控规则未通过。"


def build_risk_rejected_snapshot(
    *,
    order: PaperOrderRecord,
    evaluation: RiskEvaluation,
    portfolio: PaperPortfolioProjection,
    context: PaperNotificationDecisionContext,
    occurred_at: datetime,
) -> PaperOrderNotificationSnapshot:
    """Freeze a rejected order and the unchanged account state in one snapshot."""
    if (
        evaluation.outcome != "rejected"
        or order.risk_outcome != "rejected"
        or order.instrument_id != evaluation.instrument_id
        or order.account_id != portfolio.account_id
    ):
        raise ValueError("risk rejection notification inputs do not match")
    position = next(
        (item for item in portfolio.positions if item.instrument_id == evaluation.instrument_id),
        None,
    )
    return PaperOrderNotificationSnapshot(
        schema_version="paper_notification_v1",
        mode="internal_paper",
        order_id=order.id,
        agent_cycle_id=context.agent_cycle_id,
        occurred_at=occurred_at,
        symbol=context.symbol,
        side=cast(Literal["buy", "sell"], order.side),
        status="risk_rejected",
        requested_quantity=evaluation.desired_quantity,
        approved_quantity=Decimal(0),
        filled_quantity=Decimal(0),
        remaining_quantity=Decimal(0),
        price=PaperNotificationPrice(
            kind="reference",
            value=evaluation.reference_close,
            currency=portfolio.currency,
        ),
        notional=evaluation.desired_quantity * evaluation.reference_close,
        fee=None,
        cash=PaperNotificationCash(
            kind="unchanged",
            available=portfolio.available_cash,
        ),
        position=PaperNotificationPosition(
            kind="unchanged",
            quantity=Decimal(0) if position is None else position.quantity,
            portfolio_ratio=evaluation.projected_name_ratio,
        ),
        agent_reason=context.agent_reason,
        invalidation=context.invalidation,
        risk_reason=_risk_rejection_reason(evaluation),
    )


_CANCELLATION_REASONS = {
    "paper_user_cancelled": "用户取消了订单。",
    "paper_eod_remainder": "交易时段结束，未成交部分已取消。",
    "missing_fill_data": "执行窗口缺少合格行情，订单已取消。",
    "paper_reservation_insufficient": "剩余预留资金或持仓不足以继续成交。",
    "paper_execution_price_authority_unavailable": "执行价格数据不可用，订单已取消。",
}


def build_terminal_execution_snapshot(
    *,
    order: PaperOrderRecord,
    requested_quantity: Decimal,
    approved_quantity: Decimal,
    fills: tuple[PaperNotificationFill, ...],
    available_cash: Decimal,
    position_quantity: Decimal,
    context: PaperNotificationDecisionContext,
    status: Literal["filled", "cancelled"],
    cancellation_reason_code: str | None,
    occurred_at: datetime,
) -> PaperOrderNotificationSnapshot:
    """Freeze aggregate fills and post-transaction account values for one terminal order."""
    filled_quantity = sum((fill.quantity for fill in fills), start=Decimal(0))
    notional = sum((fill.notional for fill in fills), start=Decimal(0))
    fee = sum((fill.fee for fill in fills), start=Decimal(0)) if fills else None
    if filled_quantity > 0:
        price_kind: NotificationPriceKind = "average_fill"
        price_value = (notional / filled_quantity).quantize(
            Decimal("0.000000000001"),
            rounding=ROUND_HALF_EVEN,
        )
    else:
        price_kind = "reference"
        price_value = order.reference_close
    reason = None
    if status == "cancelled":
        reason = _CANCELLATION_REASONS.get(
            cancellation_reason_code or "",
            "订单已取消。",
        )

    return PaperOrderNotificationSnapshot(
        schema_version="paper_notification_v1",
        mode="internal_paper",
        order_id=order.id,
        agent_cycle_id=context.agent_cycle_id,
        occurred_at=occurred_at,
        symbol=context.symbol,
        side=cast(Literal["buy", "sell"], order.side),
        status=status,
        requested_quantity=requested_quantity,
        approved_quantity=approved_quantity,
        filled_quantity=filled_quantity,
        remaining_quantity=approved_quantity - filled_quantity,
        price=PaperNotificationPrice(
            kind=price_kind,
            value=price_value,
            currency="USD",
        ),
        notional=notional,
        fee=fee,
        cash=PaperNotificationCash(kind="actual_after", available=available_cash),
        position=PaperNotificationPosition(
            kind="actual_after",
            quantity=position_quantity,
            portfolio_ratio=None,
        ),
        agent_reason=context.agent_reason,
        invalidation=context.invalidation,
        risk_reason=reason,
    )
def notification_snapshot_payload(snapshot: PaperOrderNotificationSnapshot) -> dict[str, object]:
    """Serialize a validated snapshot for the existing outbox payload."""
    return {"notification_snapshot": snapshot.model_dump(mode="json")}


def parse_notification_snapshot(payload: Mapping[str, object]) -> PaperOrderNotificationSnapshot:
    """Parse a closed snapshot without leaking rejected values into the error."""
    raw = payload.get("notification_snapshot")
    if not isinstance(raw, Mapping):
        raise PaperNotificationError("PAPER_NOTIFICATION_INVALID")
    try:
        return PaperOrderNotificationSnapshot.model_validate(dict(raw))
    except (TypeError, ValueError, ValidationError):
        raise PaperNotificationError("PAPER_NOTIFICATION_INVALID") from None
