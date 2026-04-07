import asyncio
import logging

from property_crawler import Settings
from property_crawler.runner import PropertyCrawlerRunner

logger = logging.getLogger("jiji-property-crawler")


def run_daily() -> int:
    return asyncio.run(PropertyCrawlerRunner(Settings(), site_names=["jiji"]).run())


if __name__ == "__main__":
    inserted = run_daily()
    logger.info("Run completed with %s inserted properties.", inserted)
