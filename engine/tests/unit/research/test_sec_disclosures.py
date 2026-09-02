"""Text evidence is bounded, inert, and tied to one SEC accession."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from nexus.research.sec_disclosures import MAX_EXCERPT, SecDisclosure, disclosure_excerpt


def test_text_excludes_nested_hidden_markup_without_losing_visible_disclosure():
    text, truncated = disclosure_excerpt(b"""<html><head><title>secret</title></head><body>
        <div hidden><div>secret</div>still secret</div><input hidden>
        <script>secret</script><style>secret</style><ix:header>secret</ix:header>
        <div style='visibility:hidden'>secret</div><span aria-hidden='true'>secret</span>
        <p>Company <b>has not</b> decided &amp; will disclose further developments.</p>
        </body></html>""")
    assert text == "Company has not decided & will disclose further developments."
    assert truncated is False


def test_excerpt_cap_is_explicit_and_short_or_oversized_documents_fail():
    text, truncated = disclosure_excerpt(b"<p>" + b"evidence " * 2000 + b"</p>")
    assert len(text) == MAX_EXCERPT and truncated is True
    for raw in (b"<script>no visible evidence</script>", b"a" * (8 * 1024 * 1024 + 1)):
        with pytest.raises(ValueError):
            disclosure_excerpt(raw)


@pytest.mark.parametrize(
    "change",
    [
        {"source_url": "https://other.invalid/document.html"},
        {"accession_number": "0000000001-26-000002"},
        {"filed_at": datetime(2026, 8, 22)},
        {"accepted_at": datetime(2026, 8, 21)},
    ],
)
def test_document_rejects_wrong_source_accession_and_time(change):
    payload = dict(
        instrument_id=uuid4(),
        form="6-K",
        accession_number="0000000001-26-000001",
        source_url="https://www.sec.gov/Archives/edgar/data/1/000000000126000001/doc.htm",
        filed_at=datetime(2026, 8, 21, tzinfo=UTC),
        accepted_at=datetime(2026, 8, 21, 20, tzinfo=UTC),
        raw_content_hash="a" * 64,
        excerpt="Company confirms no decision has been made.",
        truncated=False,
    )
    payload.update(change)
    with pytest.raises(ValueError):
        SecDisclosure.model_validate(payload)
