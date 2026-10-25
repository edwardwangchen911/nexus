"""A frozen SEC event needs its exact document inventory and every declared text body."""

import json
from datetime import UTC, datetime
from hashlib import sha256

import pytest
from nexus.research.sec_disclosure_index import SecIndexEntry

AT = datetime(2026, 10, 9, 14, tzinfo=UTC)
ACCESSION = "0000123456-26-000002"
BASE = "https://www.sec.gov/Archives/edgar/data/123456/000012345626000002/"


def source(form="8-K"):
    entry = SecIndexEntry(
        accession_number=ACCESSION,
        form=form,
        accepted_at=AT,
        filed_at=AT.replace(hour=0),
        primary_document="main.htm",
    )
    raw = {
        "main.htm": b"<p>Item 2.02. Quarterly operating results are furnished.</p>",
        "ex99.htm": b"<p>Unaudited results: revenue USD 100 million for the quarter.</p>",
        "ex992.htm": (
            b"<p>Additional reconciliation: GAAP and non GAAP use different definitions.</p>"
        ),
    }
    rows = "".join(
        f'<tr><td>{n}</td><td>Report</td><td><a href="{BASE}{name}">{name}</a></td>'
        f"<td>{kind}</td><td>{len(raw[name])}</td></tr>"
        for n, (name, kind) in enumerate(
            (("main.htm", form), ("ex99.htm", "EX-99.1"), ("ex992.htm", "EX-99.2")), 1
        )
    )
    inventory = (
        '<table class="tableFile" summary="Document Format Files">'
        "<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>"
        + rows
        + "</table>"
    ).encode()
    return entry, inventory, raw


def receipt(raw, url, at=AT):
    return json.dumps(
        dict(
            source_url=url,
            received_at=at.isoformat(),
            sha256=sha256(raw).hexdigest(),
            byte_length=len(raw),
            http_status=200,
        )
    ).encode()


@pytest.mark.parametrize("form", ["8-K", "8-K/A"])
def test_exact_filing_retains_both_exhibits_and_never_infers_accounting_revision(form):
    from nexus.monitoring.filing_content import verify_filing_documents

    entry, inventory, raw = source(form)
    package = verify_filing_documents(
        entry=entry,
        issuer_cik="0000123456",
        inventory=(inventory, receipt(inventory, BASE + ACCESSION + "-index.html")),
        documents={name: (body, receipt(body, BASE + name)) for name, body in raw.items()},
        now=AT,
    )
    assert [row.filename for row in package] == ["main.htm", "ex99.htm", "ex992.htm"]
    assert package[2].document_type == "EX-99.2"
    assert "different definitions" in package[2].text
    assert all(not row.truncated for row in package)


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "extra",
        "wrong_hash",
        "future",
        "wrong_path",
        "wrong_form",
        "duplicate",
        "truncated",
    ],
)
def test_filing_inventory_cannot_be_satisfied_by_wrong_or_partial_bodies(damage):
    from nexus.monitoring.filing_content import verify_filing_documents

    entry, inventory, raw = source()
    documents = {name: (body, receipt(body, BASE + name)) for name, body in raw.items()}
    if damage == "missing":
        del documents["ex992.htm"]
    elif damage == "extra":
        documents["extra.htm"] = documents["ex99.htm"]
    elif damage == "wrong_hash":
        documents["ex99.htm"] = (b"wrong original", documents["ex99.htm"][1])
    elif damage == "future":
        from datetime import timedelta

        documents["ex99.htm"] = (
            raw["ex99.htm"],
            receipt(raw["ex99.htm"], BASE + "ex99.htm", AT + timedelta(seconds=1)),
        )
    elif damage == "wrong_path":
        inventory = inventory.replace(b"/data/123456/", b"/data/654321/")
    elif damage == "wrong_form":
        inventory = inventory.replace(b">8-K<", b">8-K/A<")
    elif damage == "duplicate":
        inventory = inventory.replace(
            b"</table>",
            inventory[inventory.index(b"<tr><td>") : inventory.index(b"</table>")] + b"</table>",
        )
    elif damage == "truncated":
        body = b"<p>" + b"x" * 33000 + b"</p>"
        inventory = inventory.replace(
            str(len(raw["ex99.htm"])).encode() + b"</td>", str(len(body)).encode() + b"</td>"
        )
        documents["ex99.htm"] = (body, receipt(body, BASE + "ex99.htm"))
    with pytest.raises(ValueError, match="SEC_MONITOR_"):
        verify_filing_documents(
            entry=entry,
            issuer_cik="0000123456",
            inventory=(inventory, receipt(inventory, BASE + ACCESSION + "-index.html")),
            documents=documents,
            now=AT,
        )


def test_unclosed_last_inventory_row_never_silently_omits_declared_exhibit():
    from nexus.monitoring.filing_content import document_inventory

    entry, inventory, _ = source()
    inventory = inventory.rsplit(b"</tr>", 1)[0] + b"</table>"
    with pytest.raises(ValueError, match="SEC_MONITOR_INVENTORY_INVALID"):
        document_inventory(inventory, entry=entry, issuer_cik="0000123456")


def test_inventory_received_before_filing_is_not_a_complete_document_set():
    from datetime import timedelta

    from nexus.monitoring.filing_content import verify_filing_documents

    entry, inventory, raw = source()
    with pytest.raises(ValueError, match="SEC_MONITOR_DOCUMENT_TIME_INVALID"):
        verify_filing_documents(
            entry=entry,
            issuer_cik="0000123456",
            inventory=(
                inventory,
                receipt(inventory, BASE + ACCESSION + "-index.html", AT - timedelta(seconds=1)),
            ),
            documents={name: (body, receipt(body, BASE + name)) for name, body in raw.items()},
            now=AT,
        )


def test_normal_cli_preserves_fixed_content_gap_without_raw_error(monkeypatch, tmp_path, capsys):
    from nexus.ops import research_monitor as command

    monkeypatch.setattr(command, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(command, "load_agent_paper_service_environment", lambda *a, **k: {})

    async def blocked(environment, args):
        raise ValueError("SEC_MONITOR_ATTACHMENT_UNSUPPORTED")

    monkeypatch.setattr(command, "run_configured", blocked)
    assert (
        command.main(
            [
                "--repo-root",
                str(tmp_path),
                "import-content",
                "--index-snapshot-id",
                "01000000-0000-7000-8000-000000000001",
                "--accession",
                ACCESSION,
                "--archive-dir",
                str(tmp_path),
            ]
        )
        == 1
    )
    assert json.loads(capsys.readouterr().out)["error_code"] == "SEC_MONITOR_ATTACHMENT_UNSUPPORTED"


def test_nontext_declared_exhibit_is_explicitly_unsupported_and_never_omitted():
    from nexus.monitoring.filing_content import document_inventory

    entry, inventory, _ = source()
    inventory = inventory.replace(b"ex992.htm", b"ex992.pdf")
    with pytest.raises(ValueError, match="SEC_MONITOR_ATTACHMENT_UNSUPPORTED"):
        document_inventory(inventory, entry=entry, issuer_cik="0000123456")
