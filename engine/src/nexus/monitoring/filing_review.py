"""Bounded observations for review preparation, never content authority."""

import re
from collections import Counter
from html.parser import HTMLParser

ERROR = "SEC_MONITOR_REVIEW_MATERIAL_INVALID"
MAX_BYTES = 8 * 1024 * 1024


class _Dependencies(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.references: list[dict[str, object]] = []
        self.css: list[str] = []
        self.position = 0
        self.style = False
        self.script = False
        self.inline_script_characters = 0
        self.events = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.position += 1
        references = {
            k: v
            for k, v in attrs
            if k in {"src", "srcset", "data", "poster", "background", "href", "xlink:href", "rel"}
        }
        if references or tag in {
            "script",
            "object",
            "embed",
            "iframe",
            "svg",
            "canvas",
            "video",
            "audio",
            "picture",
            "source",
            "base",
        }:
            self.references.append(
                {
                    "tag": tag,
                    "position": self.position,
                    "line": self.getpos()[0],
                    "column": self.getpos()[1],
                    "attributes": references,
                    "attribute_occurrences": [[k, v] for k, v in attrs if k in references],
                    "duplicate_attributes": sorted(
                        k for k, n in Counter(k for k, _ in attrs).items() if n > 1
                    ),
                }
            )
            if len(self.references) > 4096:
                raise ValueError(ERROR)
        self.events += sum(k.startswith("on") for k, _ in attrs)
        self.css.extend(v for k, v in attrs if k == "style" and v is not None)
        if tag == "style":
            self.style = True
        if tag == "script":
            self.script = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "style":
            self.style = False
        if tag == "script":
            self.script = False

    def handle_data(self, data: str) -> None:
        if self.style:
            self.css.append(data)
        if self.script:
            self.inline_script_characters += len(data)


def observe_html_dependencies(raw: bytes) -> dict[str, object]:
    if not 0 < len(raw) <= MAX_BYTES:
        raise ValueError(ERROR)
    parser = _Dependencies()
    try:
        parser.feed(raw.decode("utf-8-sig"))
        parser.close()
    except (UnicodeError, ValueError) as error:
        raise ValueError(ERROR) from error
    css = "\n".join(parser.css)
    return {
        "status": "observed_not_qualified",
        "research_qualified": False,
        "references": parser.references,
        "event_attribute_count": parser.events,
        "inline_script_characters": parser.inline_script_characters,
        "css_observations": {
            "url_functions": len(re.findall(r"url\s*\(", css, re.I)),
            "imports": len(re.findall(r"@import", css, re.I)),
            "image_sets": len(re.findall(r"image-set\s*\(", css, re.I)),
            "backslashes": css.count("\\"),
        },
        "limitations": (
            "Source observations only; not browser execution, dependency closure, "
            "or content qualification."
        ),
    }
