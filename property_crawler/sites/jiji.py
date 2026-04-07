from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
from urllib.parse import urlparse, urlunparse

from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from news_crawler.utils import clean_text, normalize_url, unique_strings

from ..models import PropertyImageRecord, PropertyRecord, PropertySection, PropertySeed
from .base import BasePropertySpider

logger = logging.getLogger("property-crawler")


class JijiPropertySpider(BasePropertySpider):
    site_name = "jiji"
    allowed_domains = ("jiji.com.gh",)
    sections = (
        PropertySection("new-builds", "新建物业", "https://jiji.com.gh/new-builds"),
        PropertySection("houses-apartments-for-rent", "出租物业", "https://jiji.com.gh/houses-apartments-for-rent"),
        PropertySection("houses-apartments-for-sale", "出售物业", "https://jiji.com.gh/houses-apartments-for-sale"),
        PropertySection("temporary-and-vacation-rentals", "短租物业", "https://jiji.com.gh/temporary-and-vacation-rentals"),
        PropertySection("land-and-plots-for-rent", "出租地块", "https://jiji.com.gh/land-and-plots-for-rent"),
        PropertySection("land-and-plots-for-sale", "出售地块", "https://jiji.com.gh/land-and-plots-for-sale"),
        PropertySection("event-centers-and-venues", "场所出租", "https://jiji.com.gh/event-centers-and-venues"),
        PropertySection("commercial-property-for-rent", "商业出租", "https://jiji.com.gh/commercial-property-for-rent"),
        PropertySection("commercial-properties", "商业出售", "https://jiji.com.gh/commercial-properties"),
    )

    def extract_listing_urls(self, html: str, section: PropertySection) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls: list[str] = []
        for node in soup.select(".qa-advert-list-item[href], .b-list-advert-base[href], a[href*='.html']"):
            href = node.get("href", "")
            url = self._canonicalize_property_url(normalize_url(section.url, href))
            if self.is_property_url(url):
                urls.append(url)
        return unique_strings(urls)

    def is_property_url(self, url: str) -> bool:
        parsed = urlparse(url)
        return parsed.netloc in self.allowed_domains and parsed.path.endswith(".html")

    def parse_property(self, html: str, seed: PropertySeed) -> PropertyRecord | None:
        soup = BeautifulSoup(html, "html.parser")
        advert_payload = self._extract_advert_payload(soup)
        if not advert_payload:
            logger.warning("Could not extract advert payload for %s", seed.url)
            return None

        advert = advert_payload.get("advert") or {}
        seller = advert_payload.get("seller") or {}
        attrs = advert.get("attrs") or []
        attr_map = self._build_attr_map(attrs)
        price_obj = advert.get("price_obj") or {}
        images = self._build_images(advert, advert.get("title") or "")
        return PropertyRecord(
            site=self.site_name,
            source_id=self._as_int(advert.get("id")),
            guid=clean_text(str(advert.get("guid") or "")),
            section_key=seed.section_key,
            section_name=seed.section_name,
            category_name=clean_text(str(advert.get("category_name") or "")),
            category_slug=clean_text(str(advert.get("category_slug") or "")),
            title=clean_text(str(advert.get("title") or "")),
            price_amount=self._as_float((advert.get("price") or {}).get("value")),
            currency=self._detect_currency(price_obj),
            price_text=clean_text(str(price_obj.get("view") or (advert.get("price") or {}).get("title") or "")),
            price_type=clean_text(str(price_obj.get("type") or (advert.get("price") or {}).get("type") or "")),
            price_period=clean_text(str(price_obj.get("period") or "")),
            region_name=clean_text(str(advert.get("region_name") or "")),
            region_slug=clean_text(str(advert.get("region_slug") or "")),
            region_text=clean_text(str(advert.get("region_text") or "")),
            description=clean_text(str(advert.get("description") or self._extract_description(soup))),
            url=self._canonicalize_property_url(normalize_url(seed.url, str(advert.get("url") or seed.url))),
            estate_name=attr_map.get("Estate Name", ""),
            property_type=attr_map.get("Property Type", ""),
            property_size=attr_map.get("Property Size", ""),
            property_size_unit=self._find_attr_unit(attrs, "Property Size"),
            bedrooms=attr_map.get("Number of Bedrooms", ""),
            bathrooms=attr_map.get("Number of Bathrooms", ""),
            furnishing=attr_map.get("Furnishing", ""),
            parking_spot=attr_map.get("Parking Spot", ""),
            status_of_construction=attr_map.get("Status of Construction", ""),
            seller_id=self._as_int(seller.get("id")),
            seller_guid=clean_text(str(seller.get("guid") or "")),
            seller_name=clean_text(str(seller.get("name") or "")),
            seller_page_url=normalize_url(seed.url, str(seller.get("page_url") or "")),
            seller_phone=clean_text(str(seller.get("phone") or "")),
            seller_response_time=clean_text(str((seller.get("user_response_time") or {}).get("message") or "")),
            seller_last_seen=clean_text(str(seller.get("last_seen") or "")),
            view_count=self._as_int(advert.get("count_views")) or 0,
            fav_count=self._as_int(advert.get("fav_count")) or 0,
            count_images=self._as_int(advert.get("count_images")) or len(images),
            is_promoted=bool((advert.get("paid_info") or {}).get("text")),
            is_negotiable="negotiable" in clean_text(str((advert.get("price") or {}).get("type") or "")).lower(),
            is_active=bool(advert.get("is_active")),
            is_closed=bool(advert.get("is_closed")),
            posted_at_raw=clean_text(str(advert.get("date") or "")),
            date_created=self._parse_datetime(advert.get("date_created")),
            date_moderated=self._parse_datetime(advert.get("date_moderated")),
            attrs_json=attrs,
            labels_json=advert.get("labels") or [],
            seller_labels_json=seller.get("labels") or [],
            breadcrumbs_json=advert_payload.get("breadcrumbs_data") or [],
            safety_tips_json=advert.get("safety_tips") or [],
            raw_payload_json=advert_payload,
            images=images,
        )

    def _extract_description(self, soup: BeautifulSoup) -> str:
        node = soup.select_one('.qa-advert-description, .qa-description-text, .b-advert__description-text')
        return clean_text(node.get_text("\n", strip=True)) if node else ""

    def _extract_advert_payload(self, soup: BeautifulSoup) -> dict[str, Any] | None:
        script = soup.find('script', attrs={'type': 'application/json'})
        if not script or not script.string:
            return None
        try:
            payload = json.loads(script.string)
        except json.JSONDecodeError:
            return None
        resolved = self._resolve_nuxt_payload(payload)
        if not isinstance(resolved, dict):
            return None
        data = resolved.get("data") or {}
        if not isinstance(data, dict):
            return None
        for key, value in data.items():
            if key.startswith("advert-item-") and isinstance(value, dict):
                advert_payload = value.get("advert")
                if isinstance(advert_payload, dict):
                    return advert_payload
        return None

    def _resolve_nuxt_payload(self, payload: list[Any]) -> Any:
        cache: dict[int, Any] = {}

        def resolve_ref(index: int) -> Any:
            if index in cache:
                return cache[index]
            value = payload[index]
            if isinstance(value, list):
                if value and isinstance(value[0], str) and value[0] in {"ShallowReactive", "Reactive", "ref"}:
                    resolved = resolve_ref(value[1]) if len(value) > 1 and isinstance(value[1], int) else None
                else:
                    resolved = [resolve_item(item) for item in value]
            elif isinstance(value, dict):
                resolved = {key: resolve_item(item) for key, item in value.items()}
            else:
                resolved = value
            cache[index] = resolved
            return resolved

        def resolve_item(item: Any) -> Any:
            if isinstance(item, bool) or item is None:
                return item
            if isinstance(item, int):
                if 0 <= item < len(payload):
                    return resolve_ref(item)
                return item
            if isinstance(item, list):
                return [resolve_item(x) for x in item]
            if isinstance(item, dict):
                return {key: resolve_item(value) for key, value in item.items()}
            return item

        return resolve_ref(1)

    def _build_images(self, advert: dict[str, Any], alt_text: str) -> list[PropertyImageRecord]:
        image_items = advert.get("images") or []
        records: list[PropertyImageRecord] = []
        for index, image in enumerate(image_items):
            if not isinstance(image, dict):
                continue
            url = clean_text(str(image.get("url") or ""))
            if not url:
                continue
            records.append(
                PropertyImageRecord(
                    image_id=self._as_int(image.get("id")),
                    source_url=url,
                    local_path="",
                    alt_text=alt_text,
                    is_main=bool(image.get("is_main")),
                    sort_order=index,
                    width=self._as_int(image.get("width")),
                    height=self._as_int(image.get("height")),
                )
            )
        return records

    @staticmethod
    def _build_attr_map(attrs: list[dict[str, Any]]) -> dict[str, str]:
        result: dict[str, str] = {}
        for attr in attrs:
            name = clean_text(str(attr.get("name") or ""))
            if not name:
                continue
            value = attr.get("value")
            if isinstance(value, list):
                rendered = ", ".join(clean_text(str(item)) for item in value if clean_text(str(item)))
            else:
                rendered = clean_text(str(value or ""))
            result[name] = rendered
        return result

    @staticmethod
    def _find_attr_unit(attrs: list[dict[str, Any]], target_name: str) -> str:
        for attr in attrs:
            if clean_text(str(attr.get("name") or "")) == target_name:
                return clean_text(str(attr.get("unit") or ""))
        return ""

    @staticmethod
    def _detect_currency(price_obj: dict[str, Any]) -> str:
        view = clean_text(str(price_obj.get("view") or ""))
        if "GH₵" in view:
            return "GHS"
        if "$" in view:
            return "USD"
        return ""

    @staticmethod
    def _canonicalize_property_url(url: str) -> str:
        parsed = urlparse(url)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if not value:
            return None
        try:
            return date_parser.parse(str(value), fuzzy=True)
        except Exception:
            return None

    @staticmethod
    def _as_int(value: Any) -> int | None:
        try:
            if value is None or value == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_float(value: Any) -> float | None:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
