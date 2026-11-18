"""v2 exact SEC submission-to-HTTP proof; originals are never rewritten."""

import binascii
import re
from hashlib import sha256
from urllib.parse import urlsplit

_ERROR = "SEC_MONITOR_DOCUMENT_SIZE_INVALID"
_SCRIPT = re.compile(
    rb'<script type="text/javascript" +src="(/[A-Za-z0-9/_-]+)"></script>(?=</body>)'
)


def prove_document(
    source: bytes,
    received: bytes,
    *,
    accession: str,
    issuer_cik: str,
    form: str,
    filename: str,
    kind: str,
    declared_size: int,
) -> bytes:
    if not source or not received or max(len(source), len(received)) > 8 * 1024 * 1024:
        raise ValueError(_ERROR)
    if form not in {"8-K", "8-K/A"} or not re.fullmatch(r"[0-9]{10}", issuer_cik):
        raise ValueError(_ERROR)
    parts = source.split(b"</SEC-HEADER>")
    if len(parts) != 2:
        raise ValueError(_ERROR)
    header = parts[0]
    if (
        re.findall(rb"(?m)^ACCESSION NUMBER:\s*([^\s]+)", header) != [accession.encode()]
        or set(re.findall(rb"(?m)^\s*CENTRAL INDEX KEY:\s*([^\s]+)", header))
        != {issuer_cik.encode()}
        or re.findall(rb"(?m)^CONFORMED SUBMISSION TYPE:\s*([^\s]+)", header) != [form.encode()]
    ):
        raise ValueError(_ERROR)
    members = re.findall(rb"(?ms)^<DOCUMENT>\r?\n.*?^</DOCUMENT>(?:\r?\n|$)", parts[1])
    if len(members) != len(re.findall(rb"(?m)^<DOCUMENT>", parts[1])) or len(members) != len(
        re.findall(rb"(?m)^</DOCUMENT>", parts[1])
    ):
        raise ValueError(_ERROR)
    matches = []
    for member in members:
        head = member.split(b"<TEXT>", 1)[0]
        names = re.findall(rb"(?m)^<FILENAME>([^\r\n]+)", head)
        if filename.encode() in names:
            if names != [filename.encode()] or re.findall(rb"(?m)^<TYPE>([^\r\n]+)", head) != [
                kind.encode()
            ]:
                raise ValueError(_ERROR)
            matches.append(member)
    if len(matches) != 1:
        raise ValueError(_ERROR)
    member = matches[0]
    texts = re.findall(rb"(?ms)^<TEXT>\r?\n(.*?)\r?\n</TEXT>\r?\n</DOCUMENT>", member)
    if (
        len(texts) != 1
        or len(re.findall(rb"(?m)^<TEXT>", member)) != 1
        or len(re.findall(rb"(?m)^</TEXT>", member)) != 1
    ):
        raise ValueError(_ERROR)
    content: bytes = texts[0].strip(b"\r\n")
    wrapper = re.fullmatch(rb"<XBRL>\r?\n(.*?)</XBRL>", content, re.S)
    if wrapper is not None:
        content = wrapper[1].strip(b"\r\n")
    variants = [member, member.strip(b"\r\n"), texts[0], texts[0].strip(b"\r\n"), content]
    if wrapper is not None:
        variants.append(wrapper[1])
    expected = [v for v in variants if len(v) == declared_size]
    delivered = [received, received.strip(b"\r\n")]
    scripts = list(_SCRIPT.finditer(received))
    if len(scripts) == 1:
        src = scripts[0][1].decode("ascii")
        url = urlsplit(src)
        if not src.startswith("//") and not url.netloc and not url.scheme:
            plain = received[: scripts[0].start()] + received[scripts[0].end() :]
            delivered.extend([plain, plain.strip(b"\r\n")])
    if not any(a == b for a in expected for b in delivered):
        raise ValueError(_ERROR)
    return content


def prove_graphic(
    source: bytes,
    received: bytes,
    *,
    accession: str,
    issuer_cik: str,
    form: str,
    filename: str,
    sequence: int,
    declared_size: int,
) -> dict[str, object]:
    """Prove decoded aggregate bytes, without granting image content qualification."""
    if (
        not 0 < declared_size <= 8 * 1024 * 1024
        or len(received) != declared_size
        or not re.fullmatch(r"[a-zA-Z0-9_-]+\.(?:jpg|jpeg|png|gif|webp)", filename)
        or not 0 < sequence <= 999
        or len(source) > 8 * 1024 * 1024
    ):
        raise ValueError(_ERROR)
    all_members = re.findall(rb"(?ms)^<DOCUMENT>\r?\n.*?^</DOCUMENT>(?:\r?\n|$)", source)
    sequence_members = [
        m
        for m in all_members
        if any(
            n.strip().lstrip(b"0") == str(sequence).encode()
            for n in re.findall(rb"(?m)^<SEQUENCE>([^\r\n]+)", m.split(b"<TEXT>", 1)[0])
        )
    ]
    if len(sequence_members) != 1:
        raise ValueError(_ERROR)
    members = [
        m
        for m in all_members
        if filename.encode() in re.findall(rb"(?m)^<FILENAME>([^\r\n]+)", m.split(b"<TEXT>", 1)[0])
    ]
    if len(members) != 1:
        raise ValueError(_ERROR)
    member = members[0]
    if re.findall(rb"(?m)^<SEQUENCE>([^\r\n]+)", member.split(b"<TEXT>", 1)[0]) != [
        str(sequence).encode()
    ]:
        raise ValueError(_ERROR)
    bodies = re.findall(rb"(?ms)^<TEXT>\r?\n(.*?)\r?\n</TEXT>\r?\n</DOCUMENT>", member)
    if len(bodies) != 1:
        raise ValueError(_ERROR)
    encoded = bodies[0]
    prove_document(
        source,
        encoded,
        accession=accession,
        issuer_cik=issuer_cik,
        form=form,
        filename=filename,
        kind="GRAPHIC",
        declared_size=len(encoded),
    )
    if b"\r" in encoded.replace(b"\r\n", b""):
        raise ValueError(_ERROR)
    lines = encoded.replace(b"\r\n", b"\n").split(b"\n")
    if (
        len(lines) < 4
        or lines[0] != b"begin 644 " + filename.encode()
        or lines[-1] != b"end"
        or lines[-2] not in {b"", b" ", b"`"}
    ):
        raise ValueError(_ERROR)
    decoded = bytearray()
    restored_zero_lines = []
    unused_padding = []
    data_lines = lines[1:-2]
    if not data_lines or len(data_lines) != (declared_size + 44) // 45:
        raise ValueError(_ERROR)
    for i, line in enumerate(data_lines):
        n = min(45, declared_size - len(decoded))
        # SEC drops a trailing zero-encoded space on some full lines. Only that
        # one omitted character is supported; missing meaningful bytes still fail
        # the complete independent HTTP comparison below.
        width = 1 + 4 * ((n + 2) // 3)
        if (
            not line
            or line[0] != n + 32
            or any(not 32 <= c <= 96 for c in line)
            or len(line) not in ({width, width - 1} if n == 45 else {width})
            or (i < len(data_lines) - 1 and n != 45)
        ):
            raise ValueError(_ERROR)
        try:
            # Decode complete six-bit groups, then use the declared byte count.
            # EDGAR's unused final padding can encode 0x01 (CPython #100308).
            expanded = binascii.a2b_uu(b"M" + line[1:])
            value = expanded[:n]
        except binascii.Error as error:
            raise ValueError(_ERROR) from error
        decoded.extend(value)
        if len(line) == width - 1:
            restored_zero_lines.append(i + 1)
        padding = expanded[n : 3 * ((n + 2) // 3)]
        if any(padding):
            unused_padding.append({"line": i + 1, "hex": padding.hex()})
    if bytes(decoded) != received:
        raise ValueError(_ERROR)
    return {
        "decoder_profile": "sec_uu_http_exact_v1",
        "aggregate_hash": sha256(source).hexdigest(),
        "member_hash": sha256(member).hexdigest(),
        "raw_hash": sha256(received).hexdigest(),
        "restored_zero_lines": restored_zero_lines,
        "nonzero_unused_padding": unused_padding,
        "submission_relation": "verified",
    }
