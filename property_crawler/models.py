from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True)
class PropertySection:
    key: str
    name: str
    url: str


@dataclass(frozen=True)
class PropertySeed:
    site: str
    section_key: str
    section_name: str
    url: str


@dataclass
class PropertyImageRecord:
    image_id: int | None
    source_url: str
    local_path: str
    alt_text: str
    is_main: bool = False
    sort_order: int = 0
    width: int | None = None
    height: int | None = None

    def to_db_payload(self, property_id: int, creator: str = "") -> dict[str, object]:
        return {
            "property_id": property_id,
            "image_id": self.image_id,
            "source_url": self.source_url,
            "local_path": self.local_path,
            "alt_text": self.alt_text,
            "is_main": 1 if self.is_main else 0,
            "sort_order": self.sort_order,
            "width": self.width,
            "height": self.height,
            "creator": creator,
            "updater": "crawler",
            "deleted": 0,
            "tenant_id": 0,
        }


@dataclass
class PropertyRecord:
    site: str
    source_id: int | None
    guid: str
    section_key: str
    section_name: str
    category_name: str
    category_slug: str
    title: str
    price_amount: float | None
    currency: str
    price_text: str
    price_type: str
    price_period: str
    region_name: str
    region_slug: str
    region_text: str
    description: str
    url: str
    estate_name: str
    property_type: str
    property_size: str
    property_size_unit: str
    bedrooms: str
    bathrooms: str
    furnishing: str
    parking_spot: str
    status_of_construction: str
    seller_id: int | None
    seller_guid: str
    seller_name: str
    seller_page_url: str
    seller_phone: str
    seller_response_time: str
    seller_last_seen: str
    view_count: int
    fav_count: int
    count_images: int
    is_promoted: bool
    is_negotiable: bool
    is_active: bool
    is_closed: bool
    posted_at_raw: str
    date_created: datetime | None
    date_moderated: datetime | None
    attrs_json: list[dict[str, Any]]
    labels_json: list[dict[str, Any]]
    seller_labels_json: list[dict[str, Any]]
    breadcrumbs_json: list[dict[str, Any]]
    safety_tips_json: list[str]
    raw_payload_json: dict[str, Any]
    images: list[PropertyImageRecord]

    def to_db_payload(self) -> dict[str, object]:
        return {
            "site": self.site,
            "source_id": self.source_id,
            "guid": self.guid,
            "section_key": self.section_key,
            "section_name": self.section_name,
            "category_name": self.category_name,
            "category_slug": self.category_slug,
            "title": self.title,
            "price_amount": self.price_amount,
            "currency": self.currency,
            "price_text": self.price_text,
            "price_type": self.price_type,
            "price_period": self.price_period,
            "region_name": self.region_name,
            "region_slug": self.region_slug,
            "region_text": self.region_text,
            "description": self.description,
            "url": self.url,
            "estate_name": self.estate_name,
            "property_type": self.property_type,
            "property_size": self.property_size,
            "property_size_unit": self.property_size_unit,
            "bedrooms": self.bedrooms,
            "bathrooms": self.bathrooms,
            "furnishing": self.furnishing,
            "parking_spot": self.parking_spot,
            "status_of_construction": self.status_of_construction,
            "seller_id": self.seller_id,
            "seller_guid": self.seller_guid,
            "seller_name": self.seller_name,
            "seller_page_url": self.seller_page_url,
            "seller_phone": self.seller_phone,
            "seller_response_time": self.seller_response_time,
            "seller_last_seen": self.seller_last_seen,
            "view_count": self.view_count,
            "fav_count": self.fav_count,
            "count_images": self.count_images,
            "is_promoted": 1 if self.is_promoted else 0,
            "is_negotiable": 1 if self.is_negotiable else 0,
            "is_active": 1 if self.is_active else 0,
            "is_closed": 1 if self.is_closed else 0,
            "posted_at_raw": self.posted_at_raw,
            "date_created": self.date_created,
            "date_moderated": self.date_moderated,
            "attrs_json": _json_dumps(self.attrs_json),
            "labels_json": _json_dumps(self.labels_json),
            "seller_labels_json": _json_dumps(self.seller_labels_json),
            "breadcrumbs_json": _json_dumps(self.breadcrumbs_json),
            "safety_tips_json": _json_dumps(self.safety_tips_json),
            "raw_payload_json": _json_dumps(self.raw_payload_json),
            "creator": self.seller_name,
            "updater": "crawler",
            "deleted": 0,
            "tenant_id": 0,
        }
