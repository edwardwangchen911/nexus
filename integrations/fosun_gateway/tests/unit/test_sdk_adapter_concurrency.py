"""SDK adapter cancellation, retry, and rate-limit serialization."""

from sdk_adapter_concurrency_cases import (
    test_bars_sdk_failure_is_terminal_after_one_actual_call as test_bars_sdk_failure_is_terminal_after_one_actual_call,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_blocking_log_runs_before_the_rate_call_edge as test_blocking_log_runs_before_the_rate_call_edge,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_caller_cancellation_poisons_transport_without_overlapping_sdk_calls as test_caller_cancellation_poisons_transport_without_overlapping_sdk_calls,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_capability_expiry_while_cross_lane_waiting_never_invokes_sdk_method as test_capability_expiry_while_cross_lane_waiting_never_invokes_sdk_method,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_deadline_snapshot as test_deadline_snapshot,
)
from sdk_adapter_concurrency_cases import (
    test_dual_token_bucket_applies_thirty_per_minute_budget as test_dual_token_bucket_applies_thirty_per_minute_budget,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_dual_token_bucket_applies_two_qps_burst_limit as test_dual_token_bucket_applies_two_qps_burst_limit,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_late_sdk_exception_after_timeout_is_consumed_without_logging as test_late_sdk_exception_after_timeout_is_consumed_without_logging,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_local_deadline_worker_category as test_local_deadline_worker_category,
)
from sdk_adapter_concurrency_cases import (
    test_market_backlog_cannot_queue_ahead_of_broker_work as test_market_backlog_cannot_queue_ahead_of_broker_work,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_market_rate_budget_cannot_delay_broker_budget as test_market_rate_budget_cannot_delay_broker_budget,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_market_rate_wait_releases_shared_sdk_slot_for_broker as test_market_rate_wait_releases_shared_sdk_slot_for_broker,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_poisoned_transport_close_is_idempotent_and_never_waits_for_sdk_thread as test_poisoned_transport_close_is_idempotent_and_never_waits_for_sdk_thread,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_provider_timeout_never_retries_while_blocking_call_is_active as test_provider_timeout_never_retries_while_blocking_call_is_active,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_rate_and_lane_waits_are_inside_the_request_deadline as test_rate_and_lane_waits_are_inside_the_request_deadline,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_rate_token_is_consumed_after_sdk_slot_wait_at_actual_call_time as test_rate_token_is_consumed_after_sdk_slot_wait_at_actual_call_time,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_rate_token_is_consumed_inside_deferred_sdk_thread as test_rate_token_is_consumed_inside_deferred_sdk_thread,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_retry_after_is_honored_with_bounded_retries_and_safe_logging as test_retry_after_is_honored_with_bounded_retries_and_safe_logging,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_retry_deadline_is_bounded as test_retry_deadline_is_bounded,
)
from sdk_adapter_concurrency_cases import (
    test_sdk_adapter_serializes_calls_with_concurrency_one as test_sdk_adapter_serializes_calls_with_concurrency_one,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_timed_out_sdk_call_poisons_cross_lane_requests_until_restart as test_timed_out_sdk_call_poisons_cross_lane_requests_until_restart,  # noqa: E501
)
from sdk_adapter_concurrency_cases import (
    test_worker_timeout_mapping as test_worker_timeout_mapping,
)
