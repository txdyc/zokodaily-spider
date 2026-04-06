from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class NewsSection:
    name: str
    url: str
    category: int


@dataclass(frozen=True)
class ArticleSeed:
    site: str
    section_name: str
    category: int
    url: str


@dataclass
class ArticleImageRecord:
    source_url: str
    local_path: str
    description: str
    is_cover: bool = True
    sort_order: int = 0

    def to_db_payload(self, news_id: int, creator: str = "") -> dict[str, object]:
        return {
            "news_id": news_id,
            "source_url": self.source_url,
            "local_path": self.local_path,
            "img_desc": self.description,
            "is_cover": 1 if self.is_cover else 0,
            "sort_order": self.sort_order,
            "creator": creator,
            "updater": "crawler",
            "deleted": 0,
            "tenant_id": 0,
        }


@dataclass
class ArticleRecord:
    site: str
    title: str
    chinese_title: str
    summary: str
    chinese_summary: str
    news_date: date
    content: str
    chinese_content: str
    bilingual_content: str
    url: str
    category: int
    creator: str
    images: list[ArticleImageRecord]

    def to_news_payload(self) -> dict[str, object]:
        return {
            "site": self.site,
            "title": self.title,
            "chinese_title": self.chinese_title,
            "summary": self.summary,
            "chinese_summary": self.chinese_summary,
            "news_date": self.news_date,
            "content": self.content,
            "chinese_content": self.chinese_content,
            "bilingual_content": self.bilingual_content,
            "url": self.url,
            "category": self.category,
            "creator": self.creator,
            "updater": "crawler",
            "deleted": 0,
            "tenant_id": 0,
        }
