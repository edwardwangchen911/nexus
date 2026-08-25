"""Authenticated read-only strategy registry for the deterministic baseline."""

from datetime import UTC, datetime
from typing import Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.strategies import (
    BacktestEvidenceSummaryResponse,
    StrategyListEnvelope,
    StrategySummaryResponse,
)
from nexus.persistence.models.backtest import (
    BacktestAuthorityBundleRecord,
    BacktestAuthorityCollectionRecord,
    BacktestMetricRecord,
    BacktestRunRecord,
    ExchangeCalendarAuthorityRecord,
    StrategyDefinitionRecord,
)
from nexus.persistence.models.data import PriceBarSetAuthority, PriceSeriesManifest
from nexus.strategy.dsl import (
    baseline_feature_registry,
    baseline_v1_document,
    compile_strategy,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_COMPARISON_KEYS: tuple[Literal["spy", "equal_weight", "champion"], ...] = (
    "spy",
    "equal_weight",
    "champion",
)


@router.get(
    "/internal/v1/strategies",
    response_model=StrategyListEnvelope,
    operation_id="listStrategies",
    responses={401: {"model": NexusErrorEnvelope}},
)
async def list_strategies(
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> StrategyListEnvelope:
    """Expose the only Phase 0-1 champion without implying live execution authority."""
    now = datetime.now(UTC)
    expected_strategy = compile_strategy(baseline_v1_document(), baseline_feature_registry())
    definition = await session.scalar(
        select(StrategyDefinitionRecord)
        .where(StrategyDefinitionRecord.strategy_version == "baseline_v1")
        .limit(2)
    )
    if (
        definition is None
        or definition.schema_version != "2"
        or definition.expression_hash != expected_strategy.expression_hash
        or definition.document != baseline_v1_document()
        or definition.lifecycle != "champion"
        or definition.published_at > now
    ):
        return StrategyListEnvelope(
            data=(), meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now)
        )
    latest = await session.scalar(
        select(BacktestRunRecord)
        .where(BacktestRunRecord.strategy_definition_id == definition.id)
        .order_by(BacktestRunRecord.completed_at.desc(), BacktestRunRecord.id.desc())
        .limit(1)
    )
    latest_summary = None
    if latest is not None:
        stored_keys = set(
            await session.scalars(
                select(BacktestMetricRecord.comparison_key)
                .where(
                    BacktestMetricRecord.backtest_run_id == latest.id,
                    BacktestMetricRecord.comparison_key.in_(_COMPARISON_KEYS),
                )
                .distinct()
            )
        )
        ordered_keys = tuple(key for key in _COMPARISON_KEYS if key in stored_keys)
        provenance: tuple[str, ...] = ()
        data_mode: Literal["fixture", "delayed", "real_time", "mixed"] | None = None
        bundle = await session.get(BacktestAuthorityBundleRecord, latest.authority_bundle_id)
        if bundle is not None:
            try:
                selected_ids = {
                    UUID(str(report["selected_price_bar_set_authority_id"]))
                    for report in bundle.source_selection_reports
                }
            except (KeyError, TypeError, ValueError):
                selected_ids = set()
            if selected_ids:
                provenance_rows = tuple(
                    await session.execute(
                        select(PriceBarSetAuthority, PriceSeriesManifest)
                        .join(
                            PriceSeriesManifest,
                            PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                        )
                        .where(PriceBarSetAuthority.id.in_(selected_ids))
                    )
                )
                if {row[0].id for row in provenance_rows} == selected_ids:
                    expected_collections = {
                        bundle.universe_collection_id: "universe",
                        bundle.sector_collection_id: "sector",
                        bundle.corporate_action_collection_id: "corporate_action",
                    }
                    collection_rows = tuple(
                        await session.execute(
                            select(BacktestAuthorityCollectionRecord).where(
                                BacktestAuthorityCollectionRecord.id.in_(expected_collections)
                            )
                        )
                    )
                    collections = {row[0].id: row[0] for row in collection_rows}
                    calendar = await session.get(
                        ExchangeCalendarAuthorityRecord, bundle.calendar_authority_id
                    )
                    if (
                        set(collections) == set(expected_collections)
                        and all(
                            collections[record_id].kind == kind
                            for record_id, kind in expected_collections.items()
                        )
                        and calendar is not None
                        and calendar.source
                    ):
                        price_sources = {
                            f"price:{manifest.source}:{manifest.feed}:{manifest.delivery_mode}"
                            for _, manifest in provenance_rows
                        }
                        collection_sources = {
                            f"{expected_collections[record_id]}:{record.source}"
                            for record_id, record in collections.items()
                        }
                        provenance = tuple(
                            sorted(
                                {
                                    *price_sources,
                                    *collection_sources,
                                    f"calendar:{calendar.source}",
                                }
                            )
                        )
                        fixture_domains: set[str] = set()
                        non_fixture_domains: set[str] = set()
                        if any(
                            manifest.source == "fixture" or manifest.delivery_mode == "fixture"
                            for _, manifest in provenance_rows
                        ):
                            fixture_domains.add("price")
                        if any(
                            manifest.source != "fixture" and manifest.delivery_mode != "fixture"
                            for _, manifest in provenance_rows
                        ):
                            non_fixture_domains.add("price")
                        for record_id, record in collections.items():
                            target = (
                                fixture_domains
                                if record.source == "fixture"
                                else non_fixture_domains
                            )
                            target.add(expected_collections[record_id])
                        modes = {manifest.delivery_mode for _, manifest in provenance_rows}
                        if fixture_domains:
                            data_mode = "mixed" if non_fixture_domains else "fixture"
                        elif len(modes) == 1 and next(iter(modes)) in {"delayed", "real_time"}:
                            data_mode = cast(Literal["delayed", "real_time"], next(iter(modes)))
                        else:
                            data_mode = "mixed"
        if (
            latest.strategy_data_complete
            and latest.benchmark_comparison_complete
            and ordered_keys == _COMPARISON_KEYS
            and provenance
            and data_mode is not None
        ):
            latest_summary = BacktestEvidenceSummaryResponse(
                result_hash=latest.result_hash,
                data_hash=latest.data_hash,
                code_hash=latest.code_hash,
                start_session=latest.start_session,
                end_session=latest.end_session,
                oos_window_count=latest.oos_window_count,
                comparison_keys=ordered_keys,
                benchmark_comparison_complete=True,
                data_mode=data_mode,
                data_sources=provenance,
                completed_at=latest.completed_at,
            )
    return StrategyListEnvelope(
        data=(
            StrategySummaryResponse(
                strategy_version="baseline_v1",
                champion=True,
                evolution_status="locked",
                rebalance_frequency="weekly",
                historical_agent_overlay=False,
                live_execution_enabled=False,
                latest_backtest=latest_summary,
            ),
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )
