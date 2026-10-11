import importlib
from datetime import UTC, datetime

import pytest
from helpers.iso_mic import BATS, CSV, HEADER, NOW, PUBLICATION


def parse(publication=PUBLICATION, csv=CSV, now=NOW, previous=None):
    module = importlib.import_module("nexus.market.iso_mic")
    return module.parse_bzx_mic(
        publication_html=publication, mic_csv=csv, now=now, previous=previous
    )


def test_exact_active_equities_segment_and_parent():
    result = parse()
    assert result.mic == "BATS" and result.operating_mic == "XCBO"
    assert result.publication_on.isoformat() == "2026-09-14"
    assert result.implementation_on.isoformat() == "2026-09-28"
    assert result.next_publication_on.isoformat() == "2026-10-12"


@pytest.mark.parametrize(
    "changes",
    [
        {"csv": CSV + BATS},
        {"csv": CSV.replace(b"BATS,XCBO,SGMT", b"BATS,XCBO,OPRT")},
        {"csv": CSV.replace(b"BATS,XCBO,SGMT", b"BATS,BATO,SGMT")},
        {"csv": CSV.replace(b"CBOE BZX U.S. EQUITIES", b"CBOE BZX U.S. OPTIONS")},
        {"csv": CSV.replace(b"ACTIVE", b"EXPIRED")},
        {"csv": HEADER + BATS},
        {"csv": CSV.replace(b"US,CHICAGO", b"GB,LONDON")},
        {"csv": CSV.replace(b"20180625", b"20261012")},
        {"publication": PUBLICATION.replace(b"14 September 2026", b"14 November 2026")},
        {"now": datetime(2026, 10, 12, 14, tzinfo=UTC)},
        {"publication": PUBLICATION.replace(b"ISO10383_MIC.csv", b"other.csv")},
    ],
)
def test_wrong_market_stale_publication_or_future_row_rejected(changes):
    with pytest.raises(ValueError, match="MIC_BZX_INVALID"):
        parse(**changes)


def test_new_publication_with_unchanged_old_row_can_precede_implementation():
    publication = (
        PUBLICATION.replace(b"14 September 2026", b"12 October 2026")
        .replace(b"28 September 2026", b"26 October 2026")
        .replace(b"<td>12 October 2026</td></tr>", b"<td>09 November 2026</td></tr>")
    )
    result = parse(
        publication=publication,
        now=datetime(2026, 10, 13, 14, tzinfo=UTC),
        previous=(PUBLICATION, CSV),
    )
    assert result.implementation_on.isoformat() == "2026-10-26"
    assert result.applicability_basis == "unchanged_from_previous_effective_dataset"


def test_old_row_dates_alone_do_not_prove_applicability_before_implementation():
    publication = (
        PUBLICATION.replace(b"14 September 2026", b"12 October 2026")
        .replace(b"28 September 2026", b"26 October 2026")
        .replace(b"<td>12 October 2026</td></tr>", b"<td>09 November 2026</td></tr>")
    )
    with pytest.raises(ValueError, match="MIC_BZX_INVALID"):
        parse(publication=publication, now=datetime(2026, 10, 13, 14, tzinfo=UTC))


def test_changed_row_waits_for_announced_implementation():
    publication = (
        PUBLICATION.replace(b"14 September 2026", b"12 October 2026")
        .replace(b"28 September 2026", b"26 October 2026")
        .replace(b"<td>12 October 2026</td></tr>", b"<td>09 November 2026</td></tr>")
    )
    with pytest.raises(ValueError, match="MIC_BZX_INVALID"):
        parse(
            publication=publication,
            csv=CSV.replace(b"20180625", b"20261012"),
            now=datetime(2026, 10, 13, 14, tzinfo=UTC),
        )
