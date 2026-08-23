"""Capability and payload boundaries of the read-only gateway image."""

from read_only_helpers import gateway_client as gateway_client
from read_only_surface_cases import (
    test_authority_overrides_are_rejected as test_authority_overrides_are_rejected,
)
from read_only_surface_cases import (
    test_gateway_registers_exactly_seven_authenticated_read_only_operations as test_gateway_registers_exactly_seven_authenticated_read_only_operations,  # noqa: E501
)
from read_only_surface_cases import (
    test_portfolio_authority_override_is_rejected_independently as test_portfolio_authority_override_is_rejected_independently,  # noqa: E501
)
from read_only_surface_cases import (
    test_portfolio_cash_and_first_holdings_page_accept_only_their_valid_shapes as test_portfolio_cash_and_first_holdings_page_accept_only_their_valid_shapes,  # noqa: E501
)
from read_only_surface_cases import (
    test_portfolio_read_kind_has_strict_disjoint_payloads as test_portfolio_read_kind_has_strict_disjoint_payloads,  # noqa: E501
)
from read_only_surface_cases import (
    test_read_only_image_has_no_write_or_webhook_route as test_read_only_image_has_no_write_or_webhook_route,  # noqa: E501
)
from read_only_surface_cases import (
    test_typed_parameters_reject_invalid_temporal_values_and_cursors as test_typed_parameters_reject_invalid_temporal_values_and_cursors,  # noqa: E501
)
