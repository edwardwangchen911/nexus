"""Core migration cases, defined in one non-collected helper module."""

from helpers.migration_core_cases import (
    test_0003_backfills_admission_state_after_the_released_0002_schema as test_0003_backfills_admission_state_after_the_released_0002_schema,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0003_fail_closed_backfills_disabled_admission_epoch as test_0003_fail_closed_backfills_disabled_admission_epoch,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0003_legacy_read_only_with_old_admission_fails_closed_across_round_trip as test_0003_legacy_read_only_with_old_admission_fails_closed_across_round_trip,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0005_upgrades_and_round_trips_the_committed_0004_schema as test_0005_upgrades_and_round_trips_the_committed_0004_schema,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0006_preserves_existing_admission_and_allows_same_contract_across_scopes as test_0006_preserves_existing_admission_and_allows_same_contract_across_scopes,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0006_publishes_and_exactly_removes_the_quote_domain_policy as test_0006_publishes_and_exactly_removes_the_quote_domain_policy,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_initial_migration_creates_an_exact_reversible_schema as test_initial_migration_creates_an_exact_reversible_schema,  # noqa: E501
)
