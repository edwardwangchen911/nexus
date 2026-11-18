"""Content closure resolves only the exact designated graphic context."""

import pytest
from nexus.monitoring.filing_adopted_content import verify_dependency_closure


def test_one_header_image_and_body_links_are_closed():
    result = verify_dependency_closure(
        {
            "q3fy27cfocommentary.htm": b'<img src="nvdalogoa19.jpg"><p>Full CFO commentary.</p>',
            "main.htm": b'<p>Complete filing.</p><a href="q3fy27cfocommentary.htm">Exhibit</a>',
        },
        graphic="nvdalogoa19.jpg",
        source=b"",
        accession="0001045810-26-000088",
        issuer_cik="0001045810",
    )
    assert result["graphic_references"] == 1
    assert result["all_aggregate_members_reviewed"] is False


@pytest.mark.parametrize(
    "html",
    [
        b'<img src="other.jpg">',
        b'<img src="nvdalogoa19.jpg" srcset="other.jpg 2x">',
        b'<img src="nvdalogoa19.jpg" src="other.jpg">',
        b"<svg></svg>",
        b"<script></script>",
        b'<iframe src="other.htm"></iframe>',
        b'<p style="background:url(other.jpg)">Text</p>',
        b'<style>@import "other.css"</style>',
        b'<style>p{background:image-set("x.jpg")}</style>',
        b'<link rel="stylesheet" href="other.css">',
        b'<object data="x.pdf"></object>',
        b'<p onclick="x()">Text</p>',
        b'<base href="https://other.test/">',
    ],
)
def test_unresolved_or_ambiguous_dependency_is_refused(html):
    with pytest.raises(ValueError):
        verify_dependency_closure(
            {"q3fy27cfocommentary.htm": html},
            graphic="nvdalogoa19.jpg",
            source=b"",
            accession="0001045810-26-000088",
            issuer_cik="0001045810",
        )


def test_same_image_in_another_context_is_not_adopted():
    with pytest.raises(ValueError):
        verify_dependency_closure(
            {"another.htm": b'<img src="nvdalogoa19.jpg">'},
            graphic="nvdalogoa19.jpg",
            source=b"",
            accession="0001045810-26-000088",
            issuer_cik="0001045810",
        )
