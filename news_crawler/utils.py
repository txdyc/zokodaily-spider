from __future__ import annotations

import re
from datetime import date
from typing import Iterable
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
from dateutil import parser as date_parser

# Some sites reference images on legacy/alternate hosts that are flaky or
# unreachable from certain networks (e.g. graphic.com.gh's old graphiconline.com
# domain serves the same /images/ paths but its TLS endpoint intermittently
# refuses connections). Rewrite those to the canonical, reliable host.
LEGACY_IMAGE_HOST_REWRITES = {
    "graphiconline.com": "www.graphic.com.gh",
    "www.graphiconline.com": "www.graphic.com.gh",
}


def clean_text(value: str) -> str:
    value = re.sub(r"Audio By Carbonatix.*", "", value or "", flags=re.I)
    value = re.sub(r"\s+", " ", value or "")
    return value.strip()


def normalize_url(base_url: str, candidate: str) -> str:
    return urljoin(base_url, (candidate or "").strip())


def sanitize_image_url(base_url: str, candidate: str) -> str:
    """Resolve, validate and normalize an image URL for downloading.

    Returns an absolute http(s) URL, rewriting known-flaky legacy hosts, or an
    empty string for non-downloadable values (data: URIs, empty, etc.).
    """
    url = normalize_url(base_url, candidate)
    if not url.lower().startswith(("http://", "https://")):
        return ""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    rewrite = LEGACY_IMAGE_HOST_REWRITES.get(host)
    if rewrite:
        url = parsed._replace(netloc=rewrite).geturl()
    return url


def parse_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date_parser.parse(value, fuzzy=True, dayfirst=False).date()
    except Exception:
        return None


def first_text(soup: BeautifulSoup | Tag, selectors: Iterable[str]) -> str:
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                return text
    return ""


def first_attr(soup: BeautifulSoup | Tag, selectors: Iterable[tuple[str, str]]) -> str:
    for selector, attr in selectors:
        node = soup.select_one(selector)
        if node and node.get(attr):
            return clean_text(node.get(attr, ""))
    return ""


def unique_strings(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered
