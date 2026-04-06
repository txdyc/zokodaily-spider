from __future__ import annotations

from urllib.parse import urljoin, urlparse

from ..models import NewsSection
from .base import BaseNewsSpider

NON_ARTICLE_SLUGS = {"news", "business", "opinion", "research", "category", "tag", "author", "page"}
BLOCKED_SLUGS = {
    "adom-fm-live",
    "adom-tv-audio",
    "adom-tv-live",
    "advertise",
    "contact-us",
    "privacy-policy",
    "terms-of-use",
}
STOP_TEXTS = (
    "The Multimedia Group",
    "Advertise With Us",
    "Contact Us",
    "Terms of Use",
    "Privacy Policy",
)


class MyJoySpider(BaseNewsSpider):
    site_name = "myjoyonline"
    allowed_domains = ("www.myjoyonline.com",)
    sections = (
        NewsSection(name="News", url="https://www.myjoyonline.com/category/news/", category=0),
        NewsSection(name="Business", url="https://www.myjoyonline.com/category/business/", category=2),
        NewsSection(name="Opinion", url="https://www.myjoyonline.com/category/opinion/", category=0),
        NewsSection(name="Research", url="https://www.myjoyonline.com/category/research/", category=0),
    )
    stop_texts = STOP_TEXTS
    title_suffix_pattern = r"\s*-\s*MyJoyOnline\s*$"
    summary_selectors = (".subtitle", ".entry-subtitle", ".post-excerpt", "article h2", "main p")
    date_selectors = ("time", ".entry-date", ".post-date")

    def listing_page_urls(self, section: NewsSection, max_pages: int) -> list[str]:
        return [section.url] + [urljoin(section.url.rstrip("/") + "/", f"page/{page}/") for page in range(2, max_pages + 1)]

    def is_article_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc != "www.myjoyonline.com":
            return False
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) != 1:
            return False
        slug = parts[0].lower()
        return slug not in NON_ARTICLE_SLUGS and slug not in BLOCKED_SLUGS and slug.count("-") >= 2
