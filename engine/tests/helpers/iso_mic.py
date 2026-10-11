"""Handwritten synthetic fixtures; no captured source or account data."""

from datetime import UTC, datetime

NOW = datetime(2026, 10, 11, 14, tzinfo=UTC)

PUBLICATION = b"""<html><table><thead><tr><th>Excel format</th><th>PDF Format</th>
<th>Comma Separated Values</th><th>Extended Markup Language and Schema</th>
<th>Annexes</th><th>Publication date</th><th>Modification implementation date</th>
<th>Next publication date</th></tr></thead><tbody><tr>
<td>Excel</td><td>PDF</td><td>
<a href="/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv">CSV</a></td>
<td>XML</td><td>Annexes</td><td>14 September 2026</td><td>28 September 2026</td>
<td>12 October 2026</td></tr></tbody></table></html>"""

HEADER = (
    b"MIC,OPERATING MIC,OPRT/SGMT,MARKET NAME-INSTITUTION DESCRIPTION,LEGAL ENTITY NAME,LEI,"
    b"MARKET CATEGORY CODE,ACRONYM,ISO COUNTRY CODE (ISO 3166),CITY,WEBSITE,STATUS,"
    b"CREATION DATE,LAST UPDATE DATE,LAST VALIDATION DATE,EXPIRY DATE,COMMENTS\n"
)

BATS = (
    b"BATS,XCBO,SGMT,CBOE BZX U.S. EQUITIES EXCHANGE,,,,BATS,US,CHICAGO,,ACTIVE,"
    b"20081124,20180625,,,\n"
)

PARENT = b"XCBO,XCBO,OPRT,CBOE GLOBAL MARKETS,,,,,US,CHICAGO,,ACTIVE,19730426,20200901,,,\n"

CSV = HEADER + BATS + PARENT
