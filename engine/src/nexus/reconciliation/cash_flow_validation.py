"""Fail-closed validation for cash-flow persistence evidence."""

from nexus.reconciliation.cash_flows import CashFlowSyncResult
from nexus.reconciliation.persistence_errors import BrokerSyncPersistenceError


def validate_cash_flow_result(result: CashFlowSyncResult) -> None:
    if result.pagination_complete != (result.watermark_date is not None):
        raise BrokerSyncPersistenceError("cash-flow watermark evidence is inconsistent")
    observation_ids = {observation.source_request_id for observation in result.source_observations}
    complete_evidence = (
        bool(result.source_observations)
        and all(observation.source_request_id for observation in result.source_observations)
        and all(observation.quality == "PASS" for observation in result.source_observations)
        and all(fact.source_request_id in observation_ids for fact in result.facts)
    )
    if result.quality == "PASS" and (
        not result.pagination_complete
        or not complete_evidence
        or any(fact.identity_quality != "STABLE" for fact in result.facts)
    ):
        raise BrokerSyncPersistenceError("cash-flow PASS evidence is incomplete")
    if result.watermark_date is not None and not complete_evidence:
        raise BrokerSyncPersistenceError("cash-flow watermark evidence is incomplete")
    identities: set[str] = set()
    for fact in result.facts:
        if (
            fact.provider_cash_flow_id in identities
            or not fact.provider_cash_flow_id
            or len(fact.currency) != 3
            or not fact.currency.isascii()
            or not fact.currency.isalpha()
            or fact.currency != fact.currency.upper()
            or not fact.amount.is_finite()
            or fact.occurred_at.tzinfo is None
            or fact.occurred_at.utcoffset() is None
            or fact.response_as_of.tzinfo is None
            or fact.response_as_of.utcoffset() is None
            or not fact.source_request_id
            or (
                fact.identity_quality == "STABLE"
                and fact.identity_version != "provider_cash_flow_id_v1"
            )
            or (
                fact.identity_quality == "FALLBACK_PARTIAL"
                and fact.identity_version != "broker_cash_flow_fallback_v1"
            )
            or (
                result.watermark_date is not None
                and fact.provider_business_date > result.watermark_date
            )
        ):
            raise BrokerSyncPersistenceError("cash-flow facts are invalid")
        identities.add(fact.provider_cash_flow_id)
