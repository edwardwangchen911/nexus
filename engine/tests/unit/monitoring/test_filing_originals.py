"""Non-text original custody never implies research qualification."""

from hashlib import sha256

import pytest
from unit.monitoring.test_filing_content import ACCESSION, AT, BASE, receipt, source


def package():
    entry, inventory, bodies = source()
    graphic = b"\xff\xd8financial-chart-not-exempt-by-logo-name\xff\xd9"
    inventory = inventory.replace(
        b"</table>",
        (
            f'<tr><td>8</td><td>Image</td><td><a href="{BASE}logo.jpg">logo.jpg</a></td>'
            f"<td>GRAPHIC</td><td>{len(graphic)}</td></tr></table>"
        ).encode(),
    )
    aggregate = (
        f"ACCESSION NUMBER: {ACCESSION}\nCENTRAL INDEX KEY: 0000123456\n"
        "CONFORMED SUBMISSION TYPE: 8-K\n</SEC-HEADER>\n"
    ).encode()
    for name, kind in [("main.htm", "8-K"), ("ex99.htm", "EX-99.1"), ("ex992.htm", "EX-99.2")]:
        aggregate += (
            f"<DOCUMENT>\n<TYPE>{kind}\n<FILENAME>{name}\n<TEXT>\n".encode()
            + bodies[name]
            + b"\n</TEXT>\n</DOCUMENT>\n"
        )
    raw = {
        ACCESSION + "-index.html": inventory,
        ACCESSION + ".txt": aggregate,
        **bodies,
        "logo.jpg": graphic,
    }
    pairs = {name: (body, receipt(body, BASE + name)) for name, body in raw.items()}
    return entry, pairs


def test_original_custody_retains_sequence_and_unresolved_nontext_qualification():
    from nexus.monitoring.filing_originals import verify_originals

    entry, pairs = package()
    result = verify_originals(entry=entry, issuer_cik="0000123456", originals=pairs, now=AT)
    assert [r["sequence"] for r in result] == [1, 2, 3, 8]
    assert result[-1]["content_qualification"] == "uninterpreted"
    assert result[-1]["submission_relation"] == "unverified"
    assert result[-1]["raw_hash"] == sha256(pairs["logo.jpg"][0]).hexdigest()
    assert all(r["submission_relation"] == "verified" for r in result[:-1])


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "size",
        "receipt",
        "wrong_path",
        "wrong_text",
        "unknown_type",
        "html_graphic",
        "extra",
        "sequence",
    ],
)
def test_damaged_originals_never_form_a_verified_unqualified_capture(damage):
    from nexus.monitoring.filing_originals import verify_originals

    entry, pairs = package()
    if damage == "missing":
        del pairs["logo.jpg"]
    elif damage == "size":
        pairs["logo.jpg"] = (pairs["logo.jpg"][0] + b"x", pairs["logo.jpg"][1])
    elif damage == "receipt":
        pairs["logo.jpg"] = (pairs["logo.jpg"][0], pairs["main.htm"][1])
    elif damage == "wrong_path":
        pairs["logo.jpg"] = (
            pairs["logo.jpg"][0],
            receipt(pairs["logo.jpg"][0], BASE + "other.jpg"),
        )
    elif damage == "wrong_text":
        pairs["ex99.htm"] = (
            b"<p>Changed financial facts.</p>",
            receipt(b"<p>Changed financial facts.</p>", BASE + "ex99.htm"),
        )
    elif damage == "extra":
        pairs["extra.jpg"] = pairs["logo.jpg"]
    else:
        name = ACCESSION + "-index.html"
        raw = pairs[name][0]
        if damage == "unknown_type":
            raw = raw.replace(b"GRAPHIC", b"EX-99.4")
        elif damage == "sequence":
            raw = raw.replace(b"<td>8</td>", b"<td>2</td>")
        elif damage == "html_graphic":
            body = b"<html>not a graphic</html>"
            raw = raw.replace(
                str(len(pairs["logo.jpg"][0])).encode() + b"</td>",
                str(len(body)).encode() + b"</td>",
            )
            pairs["logo.jpg"] = (body, receipt(body, BASE + "logo.jpg"))
        pairs[name] = (raw, receipt(raw, BASE + name))
    with pytest.raises(ValueError):
        verify_originals(entry=entry, issuer_cik="0000123456", originals=pairs, now=AT)
