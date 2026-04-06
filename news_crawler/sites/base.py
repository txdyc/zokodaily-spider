from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Tag

from ..models import ArticleImageRecord, ArticleRecord, ArticleSeed, NewsSection
from ..utils import clean_text, first_attr, first_text, normalize_url, parse_date, unique_strings

logger = logging.getLogger("news-crawler")


class BaseNewsSpider(ABC):
    site_name: str = ""
    allowed_domains: tuple[str, ...] = ()
    sections: tuple[NewsSection, ...] = ()
    blocked_paths: tuple[str, ...] = ()
    stop_texts: tuple[str, ...] = ()
    title_suffix_pattern: str | None = None
    min_content_length: int = 200
    title_selectors: tuple[str, ...] = ("article h1", "main h1", "h1")
    summary_selectors: tuple[str, ...] = ("meta[name='description']",)
    date_selectors: tuple[str, ...] = ("time",)
    date_attr_selectors: tuple[tuple[str, str], ...] = (
        ("meta[property='article:published_time']", "content"),
        ("meta[property='og:updated_time']", "content"),
    )
    image_attr_selectors: tuple[tuple[str, str], ...] = (
        ("meta[property='og:image']", "content"),
        ("article img", "src"),
        ("img", "src"),
    )
    image_desc_attr_selectors: tuple[tuple[str, str], ...] = (("article img", "alt"), ("img", "alt"))
    author_selectors: tuple[str, ...] = ("meta[name='author']", "[itemprop='author']", ".author", ".createdby")
    paragraph_selectors: tuple[str, ...] = (
        "article p",
        "main p",
        ".item-page p",
        ".entry-content p",
        ".article-text p",
        "p",
    )

    def listing_page_urls(self, section: NewsSection, max_pages: int) -> list[str]:
        return [section.url]

    @abstractmethod
    def is_article_url(self, url: str) -> bool:
        raise NotImplementedError

    def extract_listing_urls(self, html: str, section: NewsSection) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls = [normalize_url(section.url, link.get("href", "")) for link in soup.select("a[href]")]
        return [url for url in unique_strings(urls) if self.is_article_url(url)]

    def parse_article(self, html: str, seed: ArticleSeed) -> tuple[dict[str, str], str]:
        soup = BeautifulSoup(html, "html.parser")
        raw_text = clean_text(soup.get_text("\n", strip=True))
        title = first_attr(
            soup,
            (
                ("meta[property='og:title']", "content"),
                ("meta[name='twitter:title']", "content"),
            ),
        ) or first_text(soup, self.title_selectors)
        if self.title_suffix_pattern and title:
            title = re.sub(self.title_suffix_pattern, "", title).strip()

        summary = first_attr(
            soup,
            (
                ("meta[name='description']", "content"),
                ("meta[property='og:description']", "content"),
            ),
        ) or first_text(soup, self.summary_selectors)
        parsed_date = parse_date(first_attr(soup, self.date_attr_selectors))
        if not parsed_date:
            parsed_date = parse_date(first_text(soup, self.date_selectors))
        content = self.extract_content(soup)
        img = first_attr(soup, self.image_attr_selectors)
        img = normalize_url(seed.url, img) if img else ""
        img_desc = first_attr(soup, self.image_desc_attr_selectors)
        creator = self.extract_author(soup)
        return {
            "title": title,
            "summary": summary,
            "news_date": parsed_date.isoformat() if parsed_date else "",
            "content": content,
            "img": img,
            "img_desc": img_desc,
            "creator": creator,
        }, raw_text

    def extract_author(self, soup: BeautifulSoup) -> str:
        author = first_attr(soup, (("meta[name='author']", "content"),))
        if author:
            return author
        return first_text(soup, self.author_selectors)

    def extract_content(self, soup: BeautifulSoup) -> str:
        seen: set[str] = set()
        paragraphs: list[str] = []
        for selector in self.paragraph_selectors:
            for node in soup.select(selector):
                if not isinstance(node, Tag):
                    continue
                text = clean_text(node.get_text(" ", strip=True))
                if not text:
                    continue
                if self.stop_texts and any(stop in text for stop in self.stop_texts):
                    return "\n".join(paragraphs)
                if len(text) < 40 or text in seen:
                    continue
                seen.add(text)
                paragraphs.append(text)
            if paragraphs:
                break
        return "\n".join(paragraphs)

    def build_record(
        self,
        seed: ArticleSeed,
        parsed: dict[str, str],
        translation: dict[str, str],
        image_path: str,
    ) -> ArticleRecord | None:
        title = clean_text(parsed.get("title", ""))
        content = parsed.get("content", "").strip()
        news_date = parse_date(parsed.get("news_date", ""))
        if not title or len(content) < self.min_content_length or not news_date:
            logger.info("Skipping low-quality article for %s: %s", self.site_name, seed.url)
            return None

        chinese_content = translation.get("chinese_content", "").strip()
        bilingual_content = translation.get("bilingual_content", "").strip()
        if not bilingual_content and content and chinese_content:
            english_paragraphs = [clean_text(paragraph) for paragraph in content.splitlines() if clean_text(paragraph)]
            chinese_paragraphs = [clean_text(paragraph) for paragraph in chinese_content.splitlines() if clean_text(paragraph)]
            if len(english_paragraphs) == len(chinese_paragraphs):
                bilingual_content = "\n\n".join(
                    f"{english}\n{chinese}" for english, chinese in zip(english_paragraphs, chinese_paragraphs)
                )
            else:
                bilingual_content = f"{content}\n\n{chinese_content}".strip()

        images: list[ArticleImageRecord] = []
        if parsed.get("img") or image_path:
            images.append(
                ArticleImageRecord(
                    source_url=parsed.get("img", "").strip(),
                    local_path=image_path.strip(),
                    description=clean_text(parsed.get("img_desc", "")),
                    is_cover=True,
                    sort_order=0,
                )
            )

        return ArticleRecord(
            site=self.site_name,
            title=title,
            chinese_title=clean_text(translation.get("chinese_title", "")),
            summary=clean_text(parsed.get("summary", "")),
            chinese_summary=clean_text(translation.get("chinese_summary", "")),
            news_date=news_date,
            content=content,
            chinese_content=chinese_content,
            bilingual_content=bilingual_content,
            url=seed.url,
            category=seed.category,
            creator=clean_text(parsed.get("creator", "")),
            images=images,
        )

    def allows(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.netloc not in self.allowed_domains:
            return False
        return not any(parsed.path.lower() == blocked for blocked in self.blocked_paths)
