import asyncio
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus, urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
from sqlalchemy import Column, MetaData, String, Table, Text, create_engine, select
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError

KEYWORDS = [
    "restaurants",
    "supermarkets",
    "hotels",
    "coffee shops",
    "shopping malls",
]
SCROLL_RESULTS_JS = """
(() => {
  const feed = document.querySelector('div[role="feed"]');
  if (feed) {
    feed.scrollTop = feed.scrollHeight;
  } else {
    window.scrollTo(0, document.body.scrollHeight);
  }
})();
"""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger = logging.getLogger("google-maps-crawler")


@dataclass
class Settings:
    db_name: str = os.getenv("DB_NAME", "zokodaily")
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "3306"))
    db_user: str = os.getenv("DB_USER", "root")
    db_password: str = os.getenv("DB_PASSWORD", "Napster@1009")
    db_charset: str = os.getenv("DB_CHARSET", "utf8mb4")
    db_use_unicode: bool = os.getenv("DB_USE_UNICODE", "true").lower() == "true"
    timeout_ms: int = int(os.getenv("GMAPS_TIMEOUT_MS", "60000"))
    concurrency: int = int(os.getenv("GMAPS_CONCURRENCY", "3"))
    scroll_limit: int = int(os.getenv("GMAPS_SCROLL_LIMIT", "12"))
    max_results_per_keyword: int = int(os.getenv("GMAPS_MAX_RESULTS_PER_KEYWORD", "40"))
    delay_seconds: float = float(os.getenv("GMAPS_DELAY_SECONDS", "4"))
    image_limit_per_place: int = int(os.getenv("GMAPS_IMAGE_LIMIT_PER_PLACE", "6"))
    image_dir: str = os.getenv("GMAPS_IMAGE_DIR", os.path.join("downloads", "google_maps_places"))

    @property
    def db_url(self) -> str:
        explicit_url = os.getenv("DATABASE_URL")
        if explicit_url:
            return explicit_url
        return URL.create(
            "mysql+pymysql",
            username=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            query={"charset": self.db_charset},
        ).render_as_string(hide_password=False)


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def strip_label(value: str, labels: list[str]) -> str:
    value = clean_text(value)
    for label in labels:
        value = re.sub(rf"^{re.escape(label)}\s*[:：]\s*", "", value, flags=re.I)
    return clean_text(value.lstrip(""))


def first_text(soup: BeautifulSoup, selectors: list[str]) -> str:
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                return text
    return ""


def first_attr(soup: BeautifulSoup, selectors: list[tuple[str, str]]) -> str:
    for selector, attr in selectors:
        node = soup.select_one(selector)
        if node and node.get(attr):
            return clean_text(node.get(attr, ""))
    return ""


def extract_rating(soup: BeautifulSoup) -> str:
    for selector in ['span[aria-label*="stars"]', 'span[aria-label*="Star"]', 'span[aria-label*="星级"]']:
        node = soup.select_one(selector)
        if node and node.get("aria-label"):
            match = re.search(r"(\d+(?:\.\d+)?)", node.get("aria-label", ""))
            if match:
                return match.group(1)
    header = first_text(soup, ["div.lMbq3e", "div.TIHn2"])
    match = re.search(r"\b(\d+(?:\.\d+)?)\b", header)
    if match:
        return match.group(1)
    return ""


def extract_review_count(text: str) -> str:
    patterns = [
        r"(\d[\d,]*)\s+reviews",
        r"(\d[\d,]*)\s+review",
        r"(\d[\d,]*)\s+条评价",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            return match.group(1).replace(",", "")
    return ""


def extract_opening_text(text: str) -> str:
    patterns = [
        r"(Open[^.]*?(?:Closes[^.]*|$))",
        r"(Closed[^.]*?(?:Opens[^.]*|$))",
        r"(Opens[^.]*$)",
        r"(正在营业[^。]*?(?:结束营业时间[^。]*|$))",
        r"(已打烊[^。]*?(?:开始营业时间[^。]*|$))",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            return clean_text(match.group(1))
    return ""


def extract_lat_lng(url: str) -> tuple[str, str]:
    match = re.search(r"!3d(-?\d+(?:\.\d+)?)!4d(-?\d+(?:\.\d+)?)", url)
    if match:
        return match.group(1), match.group(2)
    match = re.search(r"@(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)", url)
    if match:
        return match.group(1), match.group(2)
    return "", ""


def ensure_https(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return f"https:{url}"
    return url


def image_extension(image_url: str, content_type: str) -> str:
    if content_type:
        content_type = content_type.split(";")[0].strip().lower()
        if content_type == "image/jpeg":
            return ".jpg"
        if content_type == "image/png":
            return ".png"
        if content_type == "image/webp":
            return ".webp"
    parsed = urlparse(image_url)
    _, ext = os.path.splitext(parsed.path)
    return ext if ext.lower() in {".jpg", ".jpeg", ".png", ".webp"} else ".jpg"


def is_place_image(url: str) -> bool:
    url = ensure_https(url)
    if not url:
        return False
    blocked = ["branding/mapslogo", "/maps/vt/", "gstatic.com/mapfiles", "googleusercontent.com/gpms-cs-s"]
    if any(token in url for token in blocked):
        return False
    allowed_hosts = [
        "googleusercontent.com",
        "streetviewpixels-pa.googleapis.com",
        "geo0.ggpht.com",
        "lh5.googleusercontent.com",
        "lh3.googleusercontent.com",
    ]
    return any(host in url for host in allowed_hosts)


def likely_small_image(url: str) -> bool:
    match = re.search(r"[=/-]w(\d+)-h(\d+)", url)
    if match:
        return int(match.group(1)) < 120 or int(match.group(2)) < 120
    return False


def extract_image_urls(soup: BeautifulSoup, limit: int) -> list[str]:
    images: list[str] = []
    seen: set[str] = set()
    for img in soup.select("img"):
        src = ensure_https(img.get("src") or img.get("data-src") or "")
        if not is_place_image(src):
            continue
        if likely_small_image(src):
            continue
        if src not in seen:
            seen.add(src)
            images.append(src)
        if len(images) >= limit:
            break
    return images


class DatabaseClient:
    def __init__(self, settings: Settings):
        self._ensure_database_exists(settings)
        self.engine = create_engine(
            settings.db_url,
            future=True,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"use_unicode": settings.db_use_unicode, "charset": settings.db_charset},
        )
        metadata = MetaData()
        self.table = Table(
            "google_maps_places",
            metadata,
            Column("record_hash", String(64), primary_key=True),
            Column("place_hash", String(64), nullable=False),
            Column("search_keyword", String(128), nullable=False),
            Column("search_rank", String(16)),
            Column("name", Text),
            Column("category", String(255)),
            Column("address", Text),
            Column("phone", String(128)),
            Column("website", Text),
            Column("rating", String(16)),
            Column("review_count", String(32)),
            Column("opening_text", Text),
            Column("plus_code", String(64)),
            Column("latitude", String(32)),
            Column("longitude", String(32)),
            Column("detail_url", Text, nullable=False),
            Column("source_search_url", Text),
            Column("raw_text", Text),
            Column("cover_image_path", Text),
            Column("image_paths_json", Text),
            Column("image_count", String(16)),
            Column("crawled_at", String(32)),
        )
        self.images_table = Table(
            "google_maps_place_images",
            metadata,
            Column("image_hash", String(64), primary_key=True),
            Column("record_hash", String(64), nullable=False),
            Column("place_hash", String(64), nullable=False),
            Column("detail_url", Text, nullable=False),
            Column("image_url", Text, nullable=False),
            Column("local_path", Text, nullable=False),
            Column("image_order", String(16)),
            Column("downloaded_at", String(32)),
        )
        metadata.create_all(self.engine)
        self._ensure_place_columns()

    def _ensure_database_exists(self, settings: Settings) -> None:
        server_url = URL.create(
            "mysql+pymysql",
            username=settings.db_user,
            password=settings.db_password,
            host=settings.db_host,
            port=settings.db_port,
            query={"charset": settings.db_charset},
        ).render_as_string(hide_password=False)
        server_engine = create_engine(
            server_url,
            future=True,
            pool_pre_ping=True,
            connect_args={"use_unicode": settings.db_use_unicode, "charset": settings.db_charset},
        )
        safe_db_name = settings.db_name.replace("`", "``")
        with server_engine.begin() as conn:
            conn.exec_driver_sql(
                f"CREATE DATABASE IF NOT EXISTS `{safe_db_name}` "
                f"CHARACTER SET {settings.db_charset} COLLATE {settings.db_charset}_unicode_ci"
            )
        server_engine.dispose()

    @staticmethod
    def record_hash(keyword: str, detail_url: str) -> str:
        return hashlib.sha256(f"{keyword}|{detail_url}".encode("utf-8")).hexdigest()

    @staticmethod
    def place_hash(detail_url: str) -> str:
        return hashlib.sha256(detail_url.encode("utf-8")).hexdigest()

    @staticmethod
    def image_hash(record_hash: str, image_url: str) -> str:
        return hashlib.sha256(f"{record_hash}|{image_url}".encode("utf-8")).hexdigest()

    def _ensure_place_columns(self) -> None:
        with self.engine.begin() as conn:
            existing = {
                row[0]
                for row in conn.exec_driver_sql(
                    """
                    SELECT COLUMN_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'google_maps_places'
                    """
                )
            }
            if "cover_image_path" not in existing:
                conn.exec_driver_sql("ALTER TABLE google_maps_places ADD COLUMN cover_image_path TEXT")
            if "image_paths_json" not in existing:
                conn.exec_driver_sql("ALTER TABLE google_maps_places ADD COLUMN image_paths_json TEXT")
            if "image_count" not in existing:
                conn.exec_driver_sql("ALTER TABLE google_maps_places ADD COLUMN image_count VARCHAR(16)")

    def existing_urls(self, keyword: str, detail_urls: list[str]) -> set[str]:
        if not detail_urls:
            return set()
        hashes = [self.record_hash(keyword, url) for url in detail_urls]
        with self.engine.begin() as conn:
            query = select(self.table.c.detail_url).where(self.table.c.record_hash.in_(hashes))
            return {row[0] for row in conn.execute(query)}

    def insert_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        try:
            with self.engine.begin() as conn:
                conn.execute(self.table.insert(), rows)
            return len(rows)
        except IntegrityError:
            inserted = 0
            with self.engine.begin() as conn:
                for row in rows:
                    try:
                        conn.execute(self.table.insert(), row)
                        inserted += 1
                    except IntegrityError:
                        logger.info("Skip duplicate: %s", row["detail_url"])
            return inserted

    def insert_image_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        try:
            with self.engine.begin() as conn:
                conn.execute(self.images_table.insert(), rows)
            return len(rows)
        except IntegrityError:
            inserted = 0
            with self.engine.begin() as conn:
                for row in rows:
                    try:
                        conn.execute(self.images_table.insert(), row)
                        inserted += 1
                    except IntegrityError:
                        logger.info("Skip duplicate image: %s", row["image_url"])
            return inserted


class GoogleMapsPlacesCrawler:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = DatabaseClient(settings)
        self.browser = BrowserConfig(headless=True, verbose=False)
        os.makedirs(self.settings.image_dir, exist_ok=True)

    @staticmethod
    def search_url(keyword: str) -> str:
        return f"https://www.google.com/maps/search/{quote_plus(f'{keyword} in Ghana')}?hl=en&gl=gh"

    async def fetch(self, crawler: AsyncWebCrawler, url: str, config: CrawlerRunConfig):
        for attempt in range(1, 4):
            try:
                result = await crawler.arun(url=url, config=config)
                if getattr(result, "success", False):
                    return result
                raise RuntimeError(getattr(result, "error_message", "crawl failed"))
            except Exception as exc:
                logger.error("Fetch failed (%s/3) %s -> %s", attempt, url, exc)
                await asyncio.sleep(attempt)
        return None

    def parse_search_links(self, html: str) -> list[str]:
        soup = BeautifulSoup(html or "", "html.parser")
        ordered: list[str] = []
        seen: set[str] = set()
        for anchor in soup.select('a[href*="/maps/place/"]'):
            href = anchor.get("href", "")
            full_url = urljoin("https://www.google.com", href)
            if full_url and full_url not in seen:
                seen.add(full_url)
                ordered.append(full_url)
        return ordered

    async def collect_place_links(self, crawler: AsyncWebCrawler, keyword: str) -> list[str]:
        search_url = self.search_url(keyword)
        session_id = f"gmaps-{hashlib.md5(keyword.encode('utf-8')).hexdigest()[:10]}"
        base_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            page_timeout=self.settings.timeout_ms,
            session_id=session_id,
            wait_until="domcontentloaded",
            wait_for='js:() => document.querySelectorAll(\'a[href*="/maps/place/"]\').length > 0',
            delay_before_return_html=self.settings.delay_seconds,
        )
        result = await self.fetch(crawler, search_url, base_config)
        if not result:
            return []
        collected = self.parse_search_links(result.html or "")
        stagnant_rounds = 0
        for _ in range(self.settings.scroll_limit):
            if len(collected) >= self.settings.max_results_per_keyword:
                break
            scroll_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=self.settings.timeout_ms,
                session_id=session_id,
                js_only=True,
                js_code=SCROLL_RESULTS_JS,
                wait_for='js:() => document.querySelectorAll(\'a[href*="/maps/place/"]\').length > 0',
                delay_before_return_html=3,
            )
            scrolled = await self.fetch(crawler, search_url, scroll_config)
            if not scrolled:
                break
            links = self.parse_search_links(scrolled.html or "")
            prev_count = len(collected)
            for link in links:
                if link not in collected:
                    collected.append(link)
            if len(collected) == prev_count:
                stagnant_rounds += 1
                if stagnant_rounds >= 2:
                    break
            else:
                stagnant_rounds = 0
        logger.info("%s -> %s place links", keyword, len(collected))
        return collected[: self.settings.max_results_per_keyword]

    def parse_place_detail(self, html: str, detail_url: str, keyword: str, rank: int) -> dict[str, str] | None:
        soup = BeautifulSoup(html or "", "html.parser")
        title = first_text(soup, ["h1.DUwDvf", "h1"])
        category = first_text(soup, ['button[jsaction*="category"]'])
        address = strip_label(
            first_attr(soup, [("button[data-item-id='address']", "aria-label")]) or first_text(soup, ["button[data-item-id='address']"]),
            ["Address", "地址"],
        )
        phone = strip_label(
            first_attr(soup, [("button[data-item-id^='phone']", "aria-label")]) or first_text(soup, ["button[data-item-id^='phone']"]),
            ["Phone", "电话"],
        )
        website = first_attr(soup, [("a[data-item-id='authority']", "href"), ("a[data-item-id='authority']", "data-href")])
        plus_code = first_text(soup, ["button[data-item-id='oloc']"])
        rating = extract_rating(soup)
        raw_text = clean_text(soup.get_text(" ", strip=True))
        review_count = extract_review_count(raw_text)
        opening_text = extract_opening_text(raw_text)
        latitude, longitude = extract_lat_lng(detail_url)
        image_urls = extract_image_urls(soup, self.settings.image_limit_per_place)
        if not title:
            logger.info("Skip invalid place page: %s", detail_url)
            return None
        return {
            "record_hash": self.db.record_hash(keyword, detail_url),
            "place_hash": self.db.place_hash(detail_url),
            "search_keyword": keyword,
            "search_rank": str(rank),
            "name": title,
            "category": category,
            "address": address,
            "phone": phone,
            "website": website,
            "rating": rating,
            "review_count": review_count,
            "opening_text": opening_text,
            "plus_code": plus_code,
            "latitude": latitude,
            "longitude": longitude,
            "detail_url": detail_url,
            "source_search_url": self.search_url(keyword),
            "raw_text": raw_text[:6000],
            "image_urls": image_urls,
            "crawled_at": datetime.now(timezone.utc).isoformat(),
        }

    async def download_image(self, session: aiohttp.ClientSession, image_url: str, keyword: str, place_hash: str, image_index: int) -> str:
        safe_keyword = re.sub(r"[^a-zA-Z0-9_-]+", "_", keyword.strip().lower())
        target_dir = os.path.join(self.settings.image_dir, safe_keyword, place_hash)
        os.makedirs(target_dir, exist_ok=True)
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.google.com/"}
        async with session.get(image_url, headers=headers) as resp:
            resp.raise_for_status()
            content = await resp.read()
            ext = image_extension(image_url, resp.headers.get("Content-Type", ""))
        filename = f"{image_index:02d}{ext}"
        full_path = os.path.join(target_dir, filename)
        with open(full_path, "wb") as file_obj:
            file_obj.write(content)
        return full_path.replace("\\", "/")

    async def download_place_images(self, row: dict[str, str]) -> tuple[list[str], list[dict[str, str]]]:
        image_urls = row.pop("image_urls", [])
        if not image_urls:
            row["cover_image_path"] = ""
            row["image_paths_json"] = "[]"
            row["image_count"] = "0"
            return [], []
        timeout = aiohttp.ClientTimeout(total=60)
        downloaded_paths: list[str] = []
        image_rows: list[dict[str, str]] = []
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for index, image_url in enumerate(image_urls, start=1):
                try:
                    local_path = await self.download_image(session, image_url, row["search_keyword"], row["place_hash"], index)
                    downloaded_paths.append(local_path)
                    image_rows.append(
                        {
                            "image_hash": self.db.image_hash(row["record_hash"], image_url),
                            "record_hash": row["record_hash"],
                            "place_hash": row["place_hash"],
                            "detail_url": row["detail_url"],
                            "image_url": image_url,
                            "local_path": local_path,
                            "image_order": str(index),
                            "downloaded_at": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                except Exception as exc:
                    logger.error("Image download failed %s -> %s", image_url, exc)
        row["cover_image_path"] = downloaded_paths[0] if downloaded_paths else ""
        row["image_paths_json"] = json.dumps(downloaded_paths, ensure_ascii=False)
        row["image_count"] = str(len(downloaded_paths))
        return downloaded_paths, image_rows

    async def fetch_place_detail(self, crawler: AsyncWebCrawler, keyword: str, detail_url: str, rank: int) -> dict[str, str] | None:
        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            page_timeout=self.settings.timeout_ms,
            wait_until="domcontentloaded",
            delay_before_return_html=3,
        )
        result = await self.fetch(crawler, detail_url, config)
        if not result:
            return None
        row = self.parse_place_detail(result.html or "", detail_url, keyword, rank)
        if not row:
            return None
        _, image_rows = await self.download_place_images(row)
        row["_image_rows"] = image_rows
        return row

    async def crawl_keyword(self, crawler: AsyncWebCrawler, keyword: str) -> int:
        detail_urls = await self.collect_place_links(crawler, keyword)
        if not detail_urls:
            return 0
        existing = self.db.existing_urls(keyword, detail_urls)
        targets = [(rank, url) for rank, url in enumerate(detail_urls, start=1) if url not in existing]
        if not targets:
            logger.info("%s -> no new places", keyword)
            return 0
        semaphore = asyncio.Semaphore(self.settings.concurrency)

        async def worker(rank: int, url: str):
            async with semaphore:
                return await self.fetch_place_detail(crawler, keyword, url, rank)

        rows = [row for row in await asyncio.gather(*(worker(rank, url) for rank, url in targets)) if row]
        image_rows: list[dict[str, str]] = []
        for row in rows:
            image_rows.extend(row.pop("_image_rows", []))
        inserted = self.db.insert_rows(rows)
        self.db.insert_image_rows(image_rows)
        logger.info("%s -> inserted %s/%s", keyword, inserted, len(rows))
        return inserted

    async def run(self) -> int:
        total_inserted = 0
        async with AsyncWebCrawler(config=self.browser, thread_safe=True) as crawler:
            for keyword in KEYWORDS:
                total_inserted += await self.crawl_keyword(crawler, keyword)
        return total_inserted


def run_google_maps_daily() -> int:
    return asyncio.run(GoogleMapsPlacesCrawler(Settings()).run())


if __name__ == "__main__":
    inserted = run_google_maps_daily()
    logger.info("Google Maps crawl finished, inserted %s rows.", inserted)
