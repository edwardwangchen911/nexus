import csv
import io
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import nexus.backtest.universe as universe
import nexus.market.universe as market_universe
import pytest
from nexus.market.universe import (
    EQUITY_SECTOR_CODES,
    SectorExposureRecord,
    etf_new_position_eligibility,
    stable_instrument_id,
)

STOCKS = {
    "AAPL", "ABBV", "ABT", "AMD", "AMGN", "AMT", "AMZN", "APD", "AVGO", "BAC",
    "CAT", "COST", "CRM", "CSCO", "CVX", "GE", "GOOGL", "GS", "HD", "IBM", "JNJ",
    "JPM", "KO", "LLY", "MA", "MCD", "META", "MSFT", "NEE", "NFLX", "NVDA", "ORCL",
    "PG", "QCOM", "RTX", "SHW", "TSLA", "TXN", "UNH", "V", "WMT", "XOM",
}
ETFS = {"SPY", "QQQ", "IWM", "DIA", "VTI", "TLT", "GLD"}


def _reviewed_sector_csv(*rows: tuple[str, str, str, str, str, str]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        ("instrument_id", "sector_code", "weight", "as_of", "source_url", "content_hash")
    )
    writer.writerows(rows)
    return output.getvalue().encode()


def _seed_rows() -> list[dict[str, str]]:
    seed_path = Path(universe.__file__).resolve().parent / "data/us_liquid_v1.csv"
    with seed_path.open(newline="", encoding="utf-8") as seed_file:
        return list(csv.DictReader(seed_file))


def test_reviewed_sector_csv_parses_exact_traceable_rows() -> None:
    instrument = stable_instrument_id("SPY")
    records = market_universe.parse_reviewed_sector_csv(
        _reviewed_sector_csv(
            (
                str(instrument),
                "information_technology",
                "0.600000",
                "2026-09-09",
                "https://issuer.example/fund/sector.csv",
                "a" * 64,
            ),
            (
                str(instrument),
                "financials",
                "0.400000",
                "2026-09-09",
                "https://issuer.example/fund/sector.csv",
                "a" * 64,
            ),
        )
    )

    assert [(row.sector_code, row.weight) for row in records] == [
        ("financials", Decimal("0.400000")),
        ("information_technology", Decimal("0.600000")),
    ]
    assert all(row.reviewed and not row.synthetic for row in records)


def test_reviewed_sector_csv_rejects_untraceable_or_ambiguous_rows() -> None:
    instrument = stable_instrument_id("SPY")
    duplicated = _reviewed_sector_csv(
        (
            str(instrument),
            "information_technology",
            "1",
            "2026-09-09",
            "file:///tmp/sector.csv",
            "b" * 64,
        ),
        (
            str(instrument),
            "information_technology",
            "1",
            "2026-09-09",
            "file:///tmp/sector.csv",
            "b" * 64,
        ),
    )

    with pytest.raises(ValueError, match="SECTOR_EXPOSURE_CSV_INVALID"):
        market_universe.parse_reviewed_sector_csv(duplicated)


def test_us_liquid_v1_has_exact_versioned_symbol_sets_and_stable_ids() -> None:
    rows = _seed_rows()
    stocks = {row["symbol"] for row in rows if row["asset_type"] == "common_stock"}
    etfs = {row["symbol"] for row in rows if row["asset_type"].endswith("_etf")}

    assert stocks == STOCKS
    assert etfs == ETFS
    assert len(rows) == 49
    assert {row["universe_version"] for row in rows} == {"us_liquid_v1"}
    assert all(row["instrument_id"] == str(stable_instrument_id(row["symbol"])) for row in rows)


def test_seed_uses_only_the_bound_sector_codes_and_explicit_non_equity_buckets() -> None:
    rows = _seed_rows()
    equity_sectors = {
        row["sector_code"]
        for row in rows
        if row["asset_type"] == "common_stock"
    }
    non_equity = {
        row["symbol"]: row["risk_bucket"]
        for row in rows
        if row["symbol"] in {"TLT", "GLD"}
    }

    assert equity_sectors == set(EQUITY_SECTOR_CODES)
    assert non_equity == {"TLT": "US_TREASURY", "GLD": "GOLD"}


def test_equity_etf_requires_recent_reviewed_exposure_outside_fixture_mode() -> None:
    decision_date = date(2026, 8, 23)
    exposure = SectorExposureRecord(
        instrument_id=stable_instrument_id("SPY"),
        sector_code="information_technology",
        weight="1.000000",
        as_of=decision_date - timedelta(days=30),
        source_url="https://issuer.invalid/spy.csv",
        content_hash="a" * 64,
        synthetic=False,
        reviewed=True,
    )

    assert etf_new_position_eligibility(
        instrument_id=stable_instrument_id("SPY"),
        asset_type="equity_etf",
        mode="production",
        exposures=(exposure,),
        decision_date=decision_date,
    ).eligible is True
    assert etf_new_position_eligibility(
        instrument_id=stable_instrument_id("SPY"),
        asset_type="equity_etf",
        mode="production",
        exposures=(),
        decision_date=decision_date,
    ).reasons == ("ETF_EXPOSURE_REVIEW_REQUIRED",)


def test_synthetic_exposure_is_fixture_only_and_non_equity_etfs_use_risk_buckets() -> None:
    decision_date = date(2026, 8, 23)
    synthetic = SectorExposureRecord(
        instrument_id=stable_instrument_id("SPY"),
        sector_code="information_technology",
        weight="1.000000",
        as_of=decision_date,
        source_url="fixture://us_liquid_v1",
        content_hash="b" * 64,
        synthetic=True,
        reviewed=False,
    )

    assert etf_new_position_eligibility(
        instrument_id=stable_instrument_id("SPY"),
        asset_type="equity_etf",
        mode="fixture",
        exposures=(synthetic,),
        decision_date=decision_date,
    ).eligible is True
    assert etf_new_position_eligibility(
        instrument_id=stable_instrument_id("SPY"),
        asset_type="equity_etf",
        mode="production",
        exposures=(synthetic,),
        decision_date=decision_date,
    ).eligible is False
    assert etf_new_position_eligibility(
        instrument_id=stable_instrument_id("TLT"),
        asset_type="non_equity_etf",
        mode="production",
        exposures=(),
        decision_date=decision_date,
    ).eligible is True


def test_equity_etf_rejects_exposures_for_another_instrument() -> None:
    decision_date = date(2026, 8, 23)
    wrong_instrument = SectorExposureRecord(
        instrument_id=stable_instrument_id("QQQ"),
        sector_code="information_technology",
        weight="1.000000",
        as_of=decision_date,
        source_url="https://issuer.invalid/spy.csv",
        content_hash="c" * 64,
        synthetic=False,
        reviewed=True,
    )

    result = etf_new_position_eligibility(
        instrument_id=stable_instrument_id("SPY"),
        asset_type="equity_etf",
        mode="production",
        exposures=(wrong_instrument,),
        decision_date=decision_date,
    )

    assert result.eligible is False
    assert result.reasons == ("ETF_EXPOSURE_INSTRUMENT_MISMATCH",)


def test_equity_etf_rejects_uncontrolled_sector_and_mixed_snapshots() -> None:
    decision_date = date(2026, 8, 23)
    target = stable_instrument_id("SPY")

    def exposure(
        sector: str,
        weight: str,
        *,
        as_of: date = decision_date,
        content_hash: str = "d" * 64,
    ) -> SectorExposureRecord:
        return SectorExposureRecord(
            instrument_id=target,
            sector_code=sector,
            weight=weight,
            as_of=as_of,
            source_url="https://issuer.invalid/spy.csv",
            content_hash=content_hash,
            synthetic=False,
            reviewed=True,
        )

    uncontrolled = etf_new_position_eligibility(
        instrument_id=target,
        asset_type="equity_etf",
        mode="production",
        exposures=(exposure("crypto", "1.000000"),),
        decision_date=decision_date,
    )
    mixed = etf_new_position_eligibility(
        instrument_id=target,
        asset_type="equity_etf",
        mode="production",
        exposures=(
            exposure("information_technology", "0.500000"),
            exposure(
                "financials",
                "0.500000",
                as_of=decision_date - timedelta(days=1),
                content_hash="e" * 64,
            ),
        ),
        decision_date=decision_date,
    )

    assert uncontrolled.reasons == ("ETF_EXPOSURE_SECTOR_INVALID",)
    assert mixed.reasons == ("ETF_EXPOSURE_SNAPSHOT_MISMATCH",)


def test_equity_etf_rejects_duplicate_sector_rows() -> None:
    decision_date = date(2026, 8, 23)
    target = stable_instrument_id("SPY")
    rows = tuple(
        SectorExposureRecord(
            instrument_id=target,
            sector_code="information_technology",
            weight="0.500000",
            as_of=decision_date,
            source_url="https://issuer.invalid/spy.csv",
            content_hash="f" * 64,
            synthetic=False,
            reviewed=True,
        )
        for _ in range(2)
    )

    result = etf_new_position_eligibility(
        instrument_id=target,
        asset_type="equity_etf",
        mode="production",
        exposures=rows,
        decision_date=decision_date,
    )

    assert result.reasons == ("ETF_EXPOSURE_SECTOR_DUPLICATE",)


def test_equity_etf_rejects_each_out_of_range_weight_before_aggregation() -> None:
    decision_date = date(2026, 8, 23)
    target = stable_instrument_id("SPY")
    rows = tuple(
        SectorExposureRecord(
            instrument_id=target,
            sector_code=sector,
            weight=weight,
            as_of=decision_date,
            source_url="https://issuer.invalid/spy.csv",
            content_hash="0" * 64,
            synthetic=False,
            reviewed=True,
        )
        for sector, weight in (
            ("information_technology", "-1.000000"),
            ("financials", "2.000000"),
        )
    )

    result = etf_new_position_eligibility(
        instrument_id=target,
        asset_type="equity_etf",
        mode="production",
        exposures=rows,
        decision_date=decision_date,
    )

    assert result.reasons == ("ETF_EXPOSURE_WEIGHT_OUT_OF_RANGE",)
