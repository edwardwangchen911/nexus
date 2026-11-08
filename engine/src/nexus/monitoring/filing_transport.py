"""v2 exact SEC submission-to-HTTP proof; originals are never rewritten."""

import re
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
