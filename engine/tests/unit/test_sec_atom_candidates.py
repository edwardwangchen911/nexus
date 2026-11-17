from datetime import UTC, datetime

import pytest


def raw(cik="0001045810", form="8-K", accession="0001193125-26-123456", href=None):
    href = href or (
        "https://www.sec.gov/Archives/edgar/data/1045810/"
        f"{accession.replace('-', '')}/{accession}-index.htm"
    )
    return f"""
<feed xmlns="http://www.w3.org/2005/Atom">
<company-info>
<cik>{cik}</cik>
</company-info>
<updated>2026-11-17T13:00:00-05:00</updated>
<link rel="next" href="https://www.sec.gov/cgi-bin/browse-edgar?start=100"/>
<entry>
<content>
<accession-number>{accession}</accession-number>
<filing-type>{form}</filing-type>
<filing-date>2026-11-17</filing-date>
<filing-href>{href}</filing-href>
</content>
<updated>2026-11-17T12:59:00-05:00</updated>
</entry>
</feed>""".encode()


def test_agent_accession_is_not_the_issuer_and_source_update_is_not_acceptance():
    from nexus.research.sec_atom_candidates import parse_atom

    result = parse_atom(
        raw(), issuer_cik="0001045810", received_at=datetime(2026, 11, 17, 18, 1, tzinfo=UTC)
    )
    assert result.has_next_page and result.negative_conclusion_authorized is False
    assert result.entries[0].accession_number.startswith("0001193125")
    assert result.entries[0].source_updated_at.hour == 17
    assert "accepted_at" not in result.entries[0].model_dump()


@pytest.mark.parametrize(
    "body",
    [
        raw(cik="0000000001"),
        raw(href="https://evil.invalid/doc"),
        raw(
            href="https://www.sec.gov/Archives/edgar/data/1/000119312526123456/0001193125-26-123456-index.htm"
        ),
        b"<!DOCTYPE feed><feed/>",
        (
            '<!DOCTYPE feed [<!ENTITY c "0001045810">]>'
            + raw().decode().replace("<cik>0001045810</cik>", "<cik>&c;</cik>")
        ).encode("utf-16"),
        (
            '<!DOCTYPE feed [<!ENTITY c "0001045810">]>'
            + raw().decode().replace("<cik>0001045810</cik>", "<cik>&c;</cik>")
        ).encode("utf-16-le"),
    ],
)
def test_unbound_or_unsafe_atom_cannot_supply_candidates(body):
    from nexus.research.sec_atom_candidates import parse_atom

    with pytest.raises(ValueError):
        parse_atom(
            body, issuer_cik="0001045810", received_at=datetime(2026, 11, 17, 18, 1, tzinfo=UTC)
        )


def test_out_of_scope_form_and_empty_feed_never_grant_negative():
    from nexus.research.sec_atom_candidates import parse_atom

    result = parse_atom(
        raw(form="8-K12B"),
        issuer_cik="0001045810",
        received_at=datetime(2026, 11, 17, 18, 1, tzinfo=UTC),
    )
    assert result.entries == () and not result.negative_conclusion_authorized
