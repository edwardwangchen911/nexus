from decimal import Decimal

from hypothesis import given, strategies as st

from nexus.diagnostics.metrics import security_hhi


@given(st.lists(st.integers(min_value=1, max_value=10_000), min_size=1, max_size=20))
def test_security_hhi_is_bounded_for_positive_security_values(values: list[int]) -> None:
    """Replacing normalized HHI with raw values must fail this concentration invariant."""
    hhi = security_hhi(tuple(Decimal(value) for value in values))

    assert hhi is not None
    assert Decimal(1) / Decimal(len(values)) <= hhi <= Decimal(1)
