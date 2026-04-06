import asyncio
import logging

from news_crawler import Settings
from news_crawler.runner import NewsCrawlerRunner

logger = logging.getLogger("myjoy-crawler")


def run_daily() -> int:
    return asyncio.run(NewsCrawlerRunner(Settings(), site_names=["myjoyonline"]).run())


if __name__ == "__main__":
    inserted = run_daily()
    logger.info("Run completed with %s inserted articles.", inserted)
