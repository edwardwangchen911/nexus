"""Authentication, transport bounds, replay, and request-ID behavior."""

from read_only_helpers import gateway_client as gateway_client
from read_only_transport_cases import (
    test_bad_signature_does_not_consume_nonce as test_bad_signature_does_not_consume_nonce,
)
from read_only_transport_cases import (
    test_current_and_previous_gateway_key_domains_authenticate as test_current_and_previous_gateway_key_domains_authenticate,  # noqa: E501
)
from read_only_transport_cases import (
    test_future_dated_signature_is_rejected_without_consuming_nonce as test_future_dated_signature_is_rejected_without_consuming_nonce,  # noqa: E501
)
from read_only_transport_cases import (
    test_gateway_hmac_v2_binds_actor_and_request_id as test_gateway_hmac_v2_binds_actor_and_request_id,  # noqa: E501
)
from read_only_transport_cases import (
    test_invalid_secret_configuration_cannot_initialize_hmac_authentication as test_invalid_secret_configuration_cannot_initialize_hmac_authentication,  # noqa: E501
)
from read_only_transport_cases import (
    test_read_only_routes_reject_non_json_before_authentication as test_read_only_routes_reject_non_json_before_authentication,  # noqa: E501
)
from read_only_transport_cases import (
    test_read_only_routes_stream_reject_oversized_body_before_json_parsing as test_read_only_routes_stream_reject_oversized_body_before_json_parsing,  # noqa: E501
)
from read_only_transport_cases import (
    test_replay_and_expired_signatures_fail_closed as test_replay_and_expired_signatures_fail_closed,  # noqa: E501
)
from read_only_transport_cases import (
    test_replay_cache_capacity_rejects_churn_without_evicting_live_nonce as test_replay_cache_capacity_rejects_churn_without_evicting_live_nonce,  # noqa: E501
)
from read_only_transport_cases import (
    test_replay_capacity_is_reserved_independently_per_hmac_key as test_replay_capacity_is_reserved_independently_per_hmac_key,  # noqa: E501
)
from read_only_transport_cases import (
    test_restart_drain_rejects_requests_until_old_signatures_are_stale as test_restart_drain_rejects_requests_until_old_signatures_are_stale,  # noqa: E501
)
from read_only_transport_cases import (
    test_unauthorized_operation_does_not_consume_principal_replay_quota as test_unauthorized_operation_does_not_consume_principal_replay_quota,  # noqa: E501
)
from read_only_transport_cases import (
    test_unverified_request_id_header_is_not_reflected as test_unverified_request_id_header_is_not_reflected,  # noqa: E501
)
from read_only_transport_cases import (
    test_valid_uuid7_is_not_reflected_on_absent_route as test_valid_uuid7_is_not_reflected_on_absent_route,  # noqa: E501
)
from read_only_transport_cases import (
    test_valid_uuid7_is_not_reflected_when_hmac_authentication_fails as test_valid_uuid7_is_not_reflected_when_hmac_authentication_fails,  # noqa: E501
)
from read_only_transport_cases import (
    test_verified_request_id_is_reflected_on_post_auth_validation_error as test_verified_request_id_is_reflected_on_post_auth_validation_error,  # noqa: E501
)
