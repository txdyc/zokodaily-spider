from __future__ import annotations

from urllib.parse import urlparse

from ..models import NewsSection
from .base import BaseNewsSpider

GRAPHIC_SECTION_PAGES = {
    "/news.html",
    "/news/",
    "/news/politics.html",
    "/news/politics/",
    "/business/business-news.html",
    "/business/business-news/",
    "/business.html",
    "/business/",
    "/politics.html",
    "/politics/",
}


class GraphicSpider(BaseNewsSpider):
    site_name = "graphic"
    allowed_domains = ("www.graphic.com.gh",)
    sections = (
        NewsSection(name="General News", url="https://www.graphic.com.gh/news.html", category=0),
        NewsSection(name="Politics", url="https://www.graphic.com.gh/news/politics.html", category=1),
        NewsSection(name="Business", url="https://www.graphic.com.gh/business/business-news.html", category=2),
    )
    stop_texts = ("Our newsletter gives you access", "From Around The Web", "Trending Ghana News", "About GCGL")
    title_suffix_pattern = r"\s*-\s*Graphic Online\s*$"
    summary_selectors = ("article p", "main p", ".item-page p")
    date_selectors = ("time", ".article-info", ".item-page .text-muted")
    image_attr_selectors = (
        ("meta[name='twitter:image']", "content"),
        ("figure.article-full-image img", "src"),
        ("figure.item-image img", "src"),
        (".item-page figure img", "src"),
        (".item-page img.caption", "src"),
        ("meta[property='og:image']", "content"),
        ("article img.caption", "src"),
        ("article img", "src"),
        ("img", "src"),
    )
    image_desc_attr_selectors = (
        ("figure.article-full-image img", "alt"),
        ("figure.item-image img", "alt"),
        (".item-page figure img", "alt"),
        (".item-page img.caption", "alt"),
        ("article img.caption", "alt"),
        ("article img", "alt"),
        ("img", "alt"),
    )
    paragraph_selectors = (
        ".item-page p",
        "article p",
        "main p",
        ".com-content-article__body p",
        ".articleBody p",
        "p",
    )

    def is_article_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc != "www.graphic.com.gh":
            return False
        path = parsed.path.rstrip("/") or "/"
        if path in GRAPHIC_SECTION_PAGES:
            return False
        if not path.endswith(".html"):
            return False
        parts = [part for part in path.split("/") if part]
        return len(parts) >= 3
