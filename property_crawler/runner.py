from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Iterable

from news_crawler.config import Settings
from news_crawler.http import HttpClient

from .db import PropertyDatabaseClient
from .images import PropertyImageDownloader
from .models import PropertySeed
from .sites import SITE_REGISTRY

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("property-crawler")
DEFAULT_SITE_NAMES = ("jiji",)


class PropertyCrawlerRunner:
    def __init__(self, settings: Settings, site_names: Iterable[str] | None = None):
        requested = [name.strip().lower() for name in (site_names or DEFAULT_SITE_NAMES) if name.strip()]
        unique_requested: list[str] = []
        for name in requested:
            if name not in unique_requested:
                unique_requested.append(name)
        unknown = [name for name in unique_requested if name not in SITE_REGISTRY]
        if unknown:
            raise ValueError(f"Unsupported sites: {', '.join(unknown)}")

        self.settings = settings
        self.spiders = [SITE_REGISTRY[name]() for name in unique_requested]
        self.db = PropertyDatabaseClient(settings)
        self.images = PropertyImageDownloader(settings)

    async def run(self) -> int:
        total_inserted = 0
        async with HttpClient(self.settings) as http:
            for spider in self.spiders:
                inserted = await self._run_spider(http, spider)
                total_inserted += inserted
        logger.info("Finished run. Inserted %s properties in total.", total_inserted)
        return total_inserted

    async def _run_spider(self, http: HttpClient, spider) -> int:
        logger.info("Collecting property URLs for %s", spider.site_name)
        seeds = await self._collect_seeds(http, spider)
        if self.settings.max_articles > 0:
            seeds = seeds[: self.settings.max_articles]
        existing = self.db.existing_urls(spider.site_name, [seed.url for seed in seeds])
        pending = [seed for seed in seeds if seed.url not in existing]
        if not pending:
            logger.info("No new properties found for %s.", spider.site_name)
            return 0

        semaphore = asyncio.Semaphore(self.settings.concurrency)

        async def worker(seed: PropertySeed):
            async with semaphore:
                try:
                    html = await http.get_text(seed.url)
                    record = spider.parse_property(html, seed)
                    if not record:
                        return None
                    downloaded_images = []
                    for image in record.images:
                        local_path = await self.images.download(http, spider.site_name, image.source_url)
                        image.local_path = local_path
                        downloaded_images.append(image)
                    record.images = downloaded_images
                    return record
                except Exception as exc:
                    logger.error("Parse failed for %s: %s", seed.url, exc)
                    return None

        inserted = 0
        parsed = 0
        buffered_records: list = []
        batch_size = max(self.settings.concurrency * 2, 20)
        tasks = [asyncio.create_task(worker(seed)) for seed in pending]
        for task in asyncio.as_completed(tasks):
            record = await task
            if not record:
                continue
            parsed += 1
            buffered_records.append(record)
            if len(buffered_records) >= batch_size:
                inserted += self.db.insert_properties(buffered_records)
                logger.info(
                    "%s progress: parsed %s/%s properties, inserted %s so far.",
                    spider.site_name,
                    parsed,
                    len(pending),
                    inserted,
                )
                buffered_records.clear()

        if buffered_records:
            inserted += self.db.insert_properties(buffered_records)
        logger.info("%s inserted %s/%s new properties.", spider.site_name, inserted, parsed)
        return inserted

    async def _collect_seeds(self, http: HttpClient, spider) -> list[PropertySeed]:
        found: list[PropertySeed] = []
        seen: set[str] = set()
        for section in spider.sections:
            for page_url in spider.listing_page_urls(section, self.settings.max_pages):
                try:
                    html = await http.get_text(page_url)
                except Exception as exc:
                    logger.error("Failed to load listing page %s: %s", page_url, exc)
                    break
                page_count = 0
                for property_url in spider.extract_listing_urls(html, section):
                    if property_url not in seen:
                        seen.add(property_url)
                        found.append(
                            PropertySeed(
                                site=spider.site_name,
                                section_key=section.key,
                                section_name=section.name,
                                url=property_url,
                            )
                        )
                        page_count += 1
                logger.info("%s listing %s -> %s new urls", spider.site_name, page_url, page_count)
                if page_count == 0:
                    logger.info("%s listing %s produced no new urls; stop paging this section.", spider.site_name, page_url)
                    break
        return found


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl Ghana property sites into zokodaily_property")
    parser.add_argument("--sites", default="jiji", help="Comma-separated site keys. Supported: jiji")
    parser.add_argument("--list-sites", action="store_true", help="List supported site keys")
    return parser.parse_args()


def run_crawler(site_names: Iterable[str] | None = None) -> int:
    return asyncio.run(PropertyCrawlerRunner(Settings(), site_names=site_names).run())


def main() -> int:
    args = parse_args()
    if args.list_sites:
        print("\n".join(sorted(DEFAULT_SITE_NAMES)))
        return 0
    site_names = [name.strip() for name in args.sites.split(",") if name.strip()]
    return run_crawler(site_names=site_names)


if __name__ == "__main__":
    raise SystemExit(main())
