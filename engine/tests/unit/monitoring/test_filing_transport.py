import pytest
from nexus.monitoring.filing_transport import prove_document


def bundle(body=b"<p>Quarterly revenue and supporting reconciliation.</p>"):
    member = (
        b"<DOCUMENT>\n<TYPE>EX-99.1\n<SEQUENCE>2\n<FILENAME>ex99.htm\n<DESCRIPTION>Results\n<TEXT>\n"
        + body
        + b"\n</TEXT>\n</DOCUMENT>\n"
    )
    source = (
        b"ACCESSION NUMBER: 0000123456-26-000002\n"
        b"CENTRAL INDEX KEY: 0000123456\n"
        b"CONFORMED SUBMISSION TYPE: 8-K\n</SEC-HEADER>\n" + member
    )
    return source, member, body


def test_exact_member_envelope_and_empty_same_origin_delivery_script():
    source, member, body = bundle(b"<html><body>Quarterly reconciliation complete.</body></html>")
    received = member.replace(
        b"</body>", b'<script type="text/javascript"  src="/opaque/path"></script></body>'
    )
    assert (
        prove_document(
            source,
            received,
            accession="0000123456-26-000002",
            issuer_cik="0000123456",
            form="8-K",
            filename="ex99.htm",
            kind="EX-99.1",
            declared_size=len(member),
        )
        == body
    )


@pytest.mark.parametrize(
    "damage",
    [
        "foreign",
        "inline",
        "double",
        "wrong_file",
        "wrong_type",
        "wrong_accession",
        "changed",
        "size",
    ],
)
def test_proof_rejects_every_unproven_byte_or_identity_change(damage):
    source, member, _ = bundle(b"<html><body>Quarterly reconciliation complete.</body></html>")
    script = b'<script type="text/javascript"  src="/opaque/path"></script>'
    if damage == "foreign":
        script = script.replace(b"/opaque/path", b"//otherhost/path")
    if damage == "inline":
        script = script.replace(b"></script>", b">code()</script>")
    if damage == "double":
        script += script
    received = member.replace(b"</body>", script + b"</body>")
    if damage == "wrong_file":
        source = source.replace(b"ex99.htm", b"other.htm")
    if damage == "wrong_type":
        source = source.replace(b"<TYPE>EX-99.1", b"<TYPE>EX-99.2")
    if damage == "wrong_accession":
        source = source.replace(b"26-000002", b"26-000003")
    if damage == "changed":
        received = received.replace(b"reconciliation", b"reconciliation changed")
    with pytest.raises(ValueError):
        prove_document(
            source,
            received,
            accession="0000123456-26-000002",
            issuer_cik="0000123456",
            form="8-K",
            filename="ex99.htm",
            kind="EX-99.1",
            declared_size=len(member) + (damage == "size"),
        )


def test_primary_xbrl_wrapper_preserves_filing_final_newline_for_declared_size():
    body = b"<html><body>Exact complete primary report.</body></html>\n"
    source, _, _ = bundle(b"<XBRL>\n" + body + b"</XBRL>")
    source = source.replace(b"<TYPE>EX-99.1", b"<TYPE>8-K")
    assert prove_document(
        source,
        body,
        accession="0000123456-26-000002",
        issuer_cik="0000123456",
        form="8-K",
        filename="ex99.htm",
        kind="8-K",
        declared_size=len(body),
    ) == body.rstrip(b"\n")
