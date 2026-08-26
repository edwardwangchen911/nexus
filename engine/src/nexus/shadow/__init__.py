"""Pure Proposal Shadow domain contracts."""

from nexus.shadow.canonical import (
    checkpoint_metrics_content_hash,
    final_authority_content_hash,
    proposal_bundle_content_hash,
    shadow_session_content_hash,
    simulation_state_content_hash,
    target_weights_content_hash,
)
from nexus.shadow.metrics import calculate_checkpoint_metrics
from nexus.shadow.models import (
    CHECKPOINT_ORDINALS,
    EVALUATION_SESSION_COUNT,
    GRACE_SESSION_COUNT,
    INITIAL_NAV,
    SHADOW_VARIANTS,
    ShadowFinalGate,
    ShadowMetrics,
    ShadowMetricValue,
    ShadowNavPoint,
    ShadowVariant,
    VariantMetrics,
    WorstSession,
)
from nexus.shadow.query import ShadowGroupView, load_shadow_view
from nexus.shadow.service import ShadowAdvanceResult, advance_shadow_session
from nexus.shadow.state_machine import checkpoint_kind, decide_terminal_action

__all__ = [
    "CHECKPOINT_ORDINALS",
    "EVALUATION_SESSION_COUNT",
    "GRACE_SESSION_COUNT",
    "INITIAL_NAV",
    "SHADOW_VARIANTS",
    "ShadowFinalGate",
    "ShadowAdvanceResult",
    "ShadowGroupView",
    "ShadowMetricValue",
    "ShadowMetrics",
    "ShadowNavPoint",
    "ShadowVariant",
    "VariantMetrics",
    "WorstSession",
    "calculate_checkpoint_metrics",
    "advance_shadow_session",
    "checkpoint_kind",
    "checkpoint_metrics_content_hash",
    "decide_terminal_action",
    "final_authority_content_hash",
    "load_shadow_view",
    "proposal_bundle_content_hash",
    "shadow_session_content_hash",
    "simulation_state_content_hash",
    "target_weights_content_hash",
]
