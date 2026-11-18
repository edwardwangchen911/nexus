"""Exact binary relation is separate from image content qualification."""

import binascii
from hashlib import sha256

import pytest
from nexus.monitoring.filing_transport import prove_graphic

ACCESSION = "0000123456-26-000002"


def package(raw=b"\xff\xd8" + bytes(range(90)) + b"\xff\xd9"):
    lines = [binascii.b2a_uu(raw[i : i + 45]).rstrip(b"\n") for i in range(0, len(raw), 45)]
    encoded = b"\n".join(lines) + b"\n\nend"
    member = (
        b"<DOCUMENT>\n<TYPE>GRAPHIC\n<SEQUENCE>8\n<FILENAME>image.jpg\n<TEXT>\n"
        b"begin 644 image.jpg\n" + encoded + b"\n</TEXT>\n</DOCUMENT>\n"
    )
    source = (
        f"ACCESSION NUMBER: {ACCESSION}\nCENTRAL INDEX KEY: 0000123456\n"
        "CONFORMED SUBMISSION TYPE: 8-K\n</SEC-HEADER>\n"
    ).encode() + member
    return source, raw


def prove(source, raw, *, size=None):
    return prove_graphic(
        source,
        raw,
        accession=ACCESSION,
        issuer_cik="0000123456",
        form="8-K",
        filename="image.jpg",
        sequence=8,
        declared_size=len(raw) if size is None else size,
    )


def test_exact_graphic_accepts_sec_padding_and_trimmed_zero_space():
    raw = b"\xff\xd8" + b"\x00" * 87 + b"\xff\xd9"
    source, raw = package(raw)
    source = source.replace(b" \n", b"\n", 1)
    # Only unused padding differs; every declared decoded byte remains identical.
    source = source.replace(b"!V0  \n", b"!V0$!\n")
    proof = prove(source, raw)
    assert proof["raw_hash"] == sha256(raw).hexdigest()
    assert proof["aggregate_hash"] == sha256(source).hexdigest()
    assert proof["decoder_profile"] == "sec_uu_http_exact_v1"
    assert proof["restored_zero_lines"] == [1]


@pytest.mark.parametrize("sequence", [b"8", b"08"])
def test_another_aggregate_member_cannot_reuse_target_sequence(sequence):
    source, raw = package()
    other = (
        b"<DOCUMENT>\n<TYPE>GRAPHIC\n<SEQUENCE>"
        + sequence
        + b"\n<FILENAME>other.jpg\n<TEXT>\nunknown\n</TEXT>\n</DOCUMENT>\n"
    )
    with pytest.raises(ValueError, match="SEC_MONITOR_DOCUMENT_SIZE_INVALID"):
        prove(source + other, raw)


@pytest.mark.parametrize(
    "damage", ["two_missing", "extra_group", "bare_cr", "duplicate_sequence", "data_bit"]
)
def test_recovery_never_allows_other_shortening_or_ignored_data(damage):
    source, raw = package(b"\xff\xd8" + b"\x00" * 87 + b"\xff\xd9")
    if damage == "two_missing":
        source = source.replace(b"  \n", b"\n", 1)
    elif damage == "extra_group":
        source = source.replace(b"\n\nend", b"    \n\nend")
    elif damage == "bare_cr":
        source = source.replace(b"begin 644 image.jpg\n", b"begin 644 image.jpg\r")
    elif damage == "duplicate_sequence":
        source = source.replace(b"<SEQUENCE>8\n", b"<SEQUENCE>8\n<SEQUENCE>8\n")
    else:
        # A missing nonzero final character cannot match the independent object.
        lines = source.split(b"\n")
        index = next(i for i, line in enumerate(lines) if line.startswith(b"M")) + 1
        assert lines[index][-1:] != b" "
        lines[index] = lines[index][:-1]
        source = b"\n".join(lines)
    with pytest.raises(ValueError, match="SEC_MONITOR_DOCUMENT_SIZE_INVALID"):
        prove(source, raw)


@pytest.mark.parametrize(
    "damage",
    [
        "byte",
        "accession",
        "issuer",
        "type",
        "filename",
        "sequence",
        "begin_name",
        "end",
        "extra",
        "alphabet",
        "oversize_line",
        "short_line",
        "duplicate",
        "size",
    ],
)
def test_graphic_rejects_unproven_identity_or_bytes(damage):
    source, raw = package()
    changes = {
        "accession": (ACCESSION.encode(), b"0000123456-26-000003"),
        "issuer": (b"KEY: 0000123456", b"KEY: 0000123457"),
        "type": (b"<TYPE>GRAPHIC", b"<TYPE>EX-99.1"),
        "filename": (b"<FILENAME>image.jpg", b"<FILENAME>other.jpg"),
        "sequence": (b"<SEQUENCE>8", b"<SEQUENCE>7"),
        "begin_name": (b"begin 644 image.jpg", b"begin 644 other.jpg"),
        "end": (b"\nend\n", b"\nfinish\n"),
        "extra": (b"\nend\n", b"\nend\nextra\n"),
        "alphabet": (b"begin 644 image.jpg\n", b"begin 644 image.jpg\n\xff\n"),
        "oversize_line": (b"\nM", b"\nN"),
        "short_line": (b"\nM", b"\nL"),
    }
    if damage in changes:
        source = source.replace(*changes[damage])
    elif damage == "byte":
        raw = raw[:-1] + b"x"
    elif damage == "duplicate":
        source += source.split(b"</SEC-HEADER>\n")[1]
    with pytest.raises(ValueError, match="SEC_MONITOR_DOCUMENT_SIZE_INVALID"):
        prove(source, raw, size=len(raw) + (damage == "size"))
