from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Iterable

from .config import Settings
from .db import DatabaseClient
from .http import HttpClient
from .images import ImageDownloader
from .models import ArticleSeed
from .sites import SITE_REGISTRY
from .translator import LLMTranslator
from .utils import clean_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("news-crawler")
DEFAULT_SITE_NAMES = ("myjoyonline", "graphic")


class NewsCrawlerRunner:
    def __init__(self, settings: Settings, site_names: Iterable[str] | None = None):
        requested = [name.strip().lower() for name in (site_names or DEFAULT_SITE_NAMES) if name.strip()]
        unique_requested: list[str] = []
        for name in requested:
            normalized = "myjoyonline" if name == "myjoy" else name
            if normalized not in unique_requested:
                unique_requested.append(normalized)
        unknown = [name for name in unique_requested if name not in SITE_REGISTRY]
        if unknown:
            raise ValueError(f"Unsupported sites: {', '.join(unknown)}")

        self.settings = settings
        self.spiders = [SITE_REGISTRY[name]() for name in unique_requested]
        self.db = DatabaseClient(settings)
        self.translator = LLMTranslator(settings)
        self.images = ImageDownloader(settings)

    async def run(self) -> int:
        total_inserted = 0
        async with HttpClient(self.settings) as http:
            for spider in self.spiders:
                inserted = await self._run_spider(http, spider)
                total_inserted += inserted
        logger.info("Finished run. Inserted %s articles in total.", total_inserted)
        return total_inserted

    async def _run_spider(self, http: HttpClient, spider) -> int:
        logger.info("Collecting article URLs for %s", spider.site_name)
        seeds = await self._collect_seeds(http, spider)
        if self.settings.max_articles > 0:
            seeds = seeds[: self.settings.max_articles]
        existing = self.db.existing_urls(spider.site_name, [seed.url for seed in seeds])
        pending = [seed for seed in seeds if seed.url not in existing]
        if not pending:
            logger.info("No new articles found for %s.", spider.site_name)
            return 0

        semaphore = asyncio.Semaphore(self.settings.concurrency)

        async def worker(seed: ArticleSeed):
            async with semaphore:
                try:
                    html = await http.get_text(seed.url)
                    parsed, raw_text = spider.parse_article(html, seed)
                    if (not parsed.get("title") or len(parsed.get("content", "")) < spider.min_content_length) and self.translator.enabled:
                        fallback = await self.translator.extract_article(raw_text)
                        parsed["title"] = parsed.get("title") or clean_text(fallback.get("title", ""))
                        parsed["summary"] = parsed.get("summary") or clean_text(fallback.get("summary", ""))
                        parsed["news_date"] = parsed.get("news_date") or clean_text(fallback.get("news_date", ""))
                        parsed["content"] = parsed.get("content") or clean_text(fallback.get("content", ""))
                    translation = await self.translator.translate(
                        parsed.get("title", ""),
                        parsed.get("summary", ""),
                        parsed.get("content", ""),
                    )
                    image_path = ""
                    if parsed.get("img"):
                        image_path = await self.images.download(http, spider.site_name, parsed["img"])
                    return spider.build_record(seed, parsed, translation, image_path)
                except Exception as exc:
                    logger.error("Parse failed for %s: %s", seed.url, exc)
                    return None

        records = [record for record in await asyncio.gather(*(worker(seed) for seed in pending)) if record]
        inserted = self.db.insert_articles(records)
        logger.info("%s inserted %s/%s new articles.", spider.site_name, inserted, len(records))
        return inserted

    async def _collect_seeds(self, http: HttpClient, spider) -> list[ArticleSeed]:
        found: list[ArticleSeed] = []
        seen: set[str] = set()
        for section in spider.sections:
            for page_url in spider.listing_page_urls(section, self.settings.max_pages):
                try:
                    html = await http.get_text(page_url)
                except Exception as exc:
                    logger.error("Failed to load listing page %s: %s", page_url, exc)
                    continue
                for article_url in spider.extract_listing_urls(html, section):
                    if article_url not in seen:
                        seen.add(article_url)
                        found.append(
                            ArticleSeed(
                                site=spider.site_name,
                                section_name=section.name,
                                category=section.category,
                                url=article_url,
                            )
                        )
                logger.info("%s listing %s -> %s urls", spider.site_name, page_url, len(found))
        return found


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl Ghana news sites into zokodaily_news")
    parser.add_argument("--sites", default="myjoyonline,graphic", help="Comma-separated site keys. Supported: myjoyonline, graphic")
    parser.add_argument("--list-sites", action="store_true", help="List supported site keys")
    return parser.parse_args()


def run_crawler(site_names: Iterable[str] | None = None) -> int:
    return asyncio.run(NewsCrawlerRunner(Settings(), site_names=site_names).run())


def main() -> int:
    args = parse_args()
    if args.list_sites:
        print("\n".join(sorted(DEFAULT_SITE_NAMES)))
        return 0
    site_names = [name.strip() for name in args.sites.split(",") if name.strip()]
    return run_crawler(site_names=site_names)


if __name__ == "__main__":
    raise SystemExit(main())
