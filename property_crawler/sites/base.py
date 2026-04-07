from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import PropertyRecord, PropertySection


class BasePropertySpider(ABC):
    site_name: str = ""
    allowed_domains: tuple[str, ...] = ()
    sections: tuple[PropertySection, ...] = ()

    def listing_page_urls(self, section: PropertySection, max_pages: int) -> list[str]:
        urls = [section.url]
        for page in range(2, max(max_pages, 1) + 1):
            separator = "&" if "?" in section.url else "?"
            urls.append(f"{section.url}{separator}page={page}")
        return urls

    @abstractmethod
    def extract_listing_urls(self, html: str, section: PropertySection) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def parse_property(self, html: str, seed) -> PropertyRecord | None:
        raise NotImplementedError
