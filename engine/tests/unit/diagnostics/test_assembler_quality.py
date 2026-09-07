from decimal import Decimal
from uuid import UUID

from nexus.diagnostics.assembler import _market_quality, _risk_bucket_weights
from nexus.diagnostics.models import InstrumentIdentity


def test_current_fosun_raw_series_remains_partial() -> None:
    assert (
        _market_quality(
            policy_version="source_policy_v2",
            benchmark_available=True,
            selected_count=2,
            position_count=2,
        )
        == "PARTIAL"
    )


def test_historical_adjusted_series_can_pass() -> None:
    assert (
        _market_quality(
            policy_version="source_policy_v1",
            benchmark_available=True,
            selected_count=2,
            position_count=2,
        )
        == "PASS"
    )


def test_repository_risk_bucket_becomes_full_weight_diagnostic_bucket() -> None:
    instrument_id = UUID(int=1)
    identity = InstrumentIdentity.standard(instrument_id).model_copy(
        update={"risk_bucket": "EQUITY"}
    )
    unresolved = InstrumentIdentity.unresolved(UUID(int=2))

    identities = {instrument_id: identity, unresolved.instrument_id: unresolved}
    assert _risk_bucket_weights(identities) == {
        instrument_id: {"EQUITY": Decimal(1)}
    }
