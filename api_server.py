from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path
from typing import Any

from flask import Flask, abort, jsonify, request, send_from_directory, url_for
from flask_cors import CORS
from sqlalchemy import create_engine, text

from news_crawler.config import Settings

NEWS_CATEGORY_LABELS = {
    0: "general",
    1: "political",
    2: "economy",
    3: "entertaining",
    4: "headline",
}
NEWS_CATEGORY_LOOKUP = {str(key): key for key in NEWS_CATEGORY_LABELS}
NEWS_CATEGORY_LOOKUP.update({value: key for key, value in NEWS_CATEGORY_LABELS.items()})


class ApiRepository:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.engine = create_engine(
            settings.db_url,
            future=True,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"use_unicode": settings.db_use_unicode, "charset": settings.db_charset},
        )

    def get_news_list(
        self,
        *,
        page: int,
        page_size: int,
        q: str,
        news_date: str,
        date_from: str,
        date_to: str,
        category: int | None,
    ) -> tuple[list[dict[str, Any]], int]:
        filters = [
            "deleted = b'0'",
        ]
        params: dict[str, Any] = {
            "limit": page_size,
            "offset": (page - 1) * page_size,
        }

        if q:
            filters.append(
                "("
                "title LIKE :q OR chinese_title LIKE :q OR summary LIKE :q OR chinese_summary LIKE :q OR "
                "content LIKE :q OR chinese_content LIKE :q OR bilingual_content LIKE :q"
                ")"
            )
            params["q"] = f"%{q.strip()}%"
        if news_date:
            filters.append("news_date = :news_date")
            params["news_date"] = news_date
        if date_from:
            filters.append("news_date >= :date_from")
            params["date_from"] = date_from
        if date_to:
            filters.append("news_date <= :date_to")
            params["date_to"] = date_to
        if category is not None:
            filters.append("category = :category")
            params["category"] = category

        where_clause = " AND ".join(filters)
        count_sql = text(f"SELECT COUNT(*) FROM zokodaily_news WHERE {where_clause}")
        list_sql = text(
            f"""
            SELECT
              n.id,
              n.site,
              n.title,
              n.chinese_title,
              n.news_date,
              n.category,
              (
                SELECT i.local_path
                FROM zokodaily_news_image i
                WHERE i.news_id = n.id AND i.deleted = b'0'
                ORDER BY i.is_cover DESC, i.sort_order ASC, i.id ASC
                LIMIT 1
              ) AS thumbnail_path
            FROM zokodaily_news n
            WHERE {where_clause}
            ORDER BY n.news_date DESC, n.id DESC
            LIMIT :limit OFFSET :offset
            """
        )
        with self.engine.begin() as conn:
            total = int(conn.execute(count_sql, params).scalar_one())
            rows = [dict(row._mapping) for row in conn.execute(list_sql, params)]
        return rows, total

    def get_news_detail(self, news_id: int) -> dict[str, Any] | None:
        news_sql = text(
            """
            SELECT
              id,
              site,
              title,
              chinese_title,
              summary,
              chinese_summary,
              news_date,
              content,
              chinese_content,
              bilingual_content,
              url,
              category,
              creator,
              create_time,
              update_time
            FROM zokodaily_news
            WHERE id = :news_id AND deleted = b'0'
            """
        )
        images_sql = text(
            """
            SELECT
              id,
              source_url,
              local_path,
              img_desc,
              is_cover,
              sort_order
            FROM zokodaily_news_image
            WHERE news_id = :news_id AND deleted = b'0'
            ORDER BY is_cover DESC, sort_order ASC, id ASC
            """
        )
        with self.engine.begin() as conn:
            row = conn.execute(news_sql, {"news_id": news_id}).mappings().first()
            if not row:
                return None
            images = [dict(image) for image in conn.execute(images_sql, {"news_id": news_id}).mappings()]
        payload = dict(row)
        payload["images"] = images
        return payload

    def get_place_categories(self) -> list[dict[str, Any]]:
        sql = text(
            """
            SELECT
              search_keyword,
              COUNT(DISTINCT place_hash) AS place_count,
              MIN(category) AS sample_category
            FROM google_maps_places
            GROUP BY search_keyword
            ORDER BY search_keyword ASC
            """
        )
        with self.engine.begin() as conn:
            return [dict(row._mapping) for row in conn.execute(sql)]

    def get_places(
        self,
        *,
        page: int,
        page_size: int,
        q: str,
        search_keyword: str,
        category: str,
    ) -> tuple[list[dict[str, Any]], int]:
        filters = ["rn = 1"]
        params: dict[str, Any] = {
            "limit": page_size,
            "offset": (page - 1) * page_size,
        }
        if q:
            filters.append("(name LIKE :q OR category LIKE :q OR address LIKE :q OR raw_text LIKE :q)")
            params["q"] = f"%{q.strip()}%"
        if search_keyword:
            filters.append("search_keyword = :search_keyword")
            params["search_keyword"] = search_keyword
        if category:
            filters.append("category LIKE :category")
            params["category"] = f"%{category.strip()}%"

        where_clause = " AND ".join(filters)
        cte = """
            WITH ranked_places AS (
              SELECT
                p.*,
                ROW_NUMBER() OVER (
                  PARTITION BY p.place_hash
                  ORDER BY
                    CASE
                      WHEN p.search_rank REGEXP '^[0-9]+$' THEN CAST(p.search_rank AS UNSIGNED)
                      ELSE 999999
                    END ASC,
                    p.crawled_at DESC,
                    p.record_hash DESC
                ) AS rn
              FROM google_maps_places p
            )
        """
        count_sql = text(
            cte
            + f"""
            SELECT COUNT(*)
            FROM ranked_places
            WHERE {where_clause}
            """
        )
        list_sql = text(
            cte
            + f"""
            SELECT
              place_hash,
              record_hash,
              name,
              category,
              search_keyword,
              opening_text,
              cover_image_path,
              rating,
              review_count
            FROM ranked_places
            WHERE {where_clause}
            ORDER BY name ASC
            LIMIT :limit OFFSET :offset
            """
        )
        with self.engine.begin() as conn:
            total = int(conn.execute(count_sql, params).scalar_one())
            rows = [dict(row._mapping) for row in conn.execute(list_sql, params)]
        return rows, total

    def get_place_detail(self, place_hash: str) -> dict[str, Any] | None:
        detail_sql = text(
            """
            WITH ranked_places AS (
              SELECT
                p.*,
                ROW_NUMBER() OVER (
                  PARTITION BY p.place_hash
                  ORDER BY
                    CASE
                      WHEN p.search_rank REGEXP '^[0-9]+$' THEN CAST(p.search_rank AS UNSIGNED)
                      ELSE 999999
                    END ASC,
                    p.crawled_at DESC,
                    p.record_hash DESC
                ) AS rn
              FROM google_maps_places p
            )
            SELECT *
            FROM ranked_places
            WHERE place_hash = :place_hash AND rn = 1
            """
        )
        images_sql = text(
            """
            SELECT
              image_hash,
              record_hash,
              place_hash,
              detail_url,
              image_url,
              local_path,
              image_order,
              downloaded_at
            FROM google_maps_place_images
            WHERE place_hash = :place_hash
            ORDER BY
              CASE
                WHEN image_order REGEXP '^[0-9]+$' THEN CAST(image_order AS UNSIGNED)
                ELSE 999999
              END ASC,
              image_hash ASC
            """
        )
        with self.engine.begin() as conn:
            row = conn.execute(detail_sql, {"place_hash": place_hash}).mappings().first()
            if not row:
                return None
            images = [dict(image) for image in conn.execute(images_sql, {"place_hash": place_hash}).mappings()]
        payload = dict(row)
        payload["images"] = images
        return payload


def parse_int(value: str | None, default: int, *, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def parse_news_category(value: str | None) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    normalized = str(value).strip().lower()
    return NEWS_CATEGORY_LOOKUP.get(normalized)


def parse_closing_time(opening_text: str) -> str:
    if not opening_text:
        return ""
    patterns = [
        r"Closes?\s+([0-9:\sAPMapm\.]+)",
        r"Opens?\s+until\s+([0-9:\sAPMapm\.]+)",
        r"结束营业时间[:：]?\s*([0-9:\s]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, opening_text, flags=re.I)
        if match:
            return match.group(1).strip()
    return ""


def bit_to_bool(value: Any) -> bool:
    if isinstance(value, (bytes, bytearray)):
        return any(value)
    return bool(value)


def json_loads_safe(value: str | None) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        return []


def create_app() -> Flask:
    settings = Settings()
    repository = ApiRepository(settings)
    app = Flask(__name__)
    app.config["JSON_AS_ASCII"] = False
    CORS(app)

    project_root = Path(__file__).resolve().parent
    downloads_root = (project_root / "downloads").resolve()

    def media_url(local_path: str | None) -> str | None:
        if not local_path:
            return None
        normalized = local_path.replace("\\", "/").lstrip("/")
        prefix = "downloads/"
        relative = normalized[len(prefix) :] if normalized.startswith(prefix) else normalized
        return url_for("serve_media", filename=relative, _external=False)

    def serialize_news_summary(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row["id"],
            "site": row["site"],
            "title": row["title"],
            "chinese_title": row["chinese_title"] or "",
            "news_date": row["news_date"].isoformat() if row.get("news_date") else None,
            "category": row["category"],
            "category_label": NEWS_CATEGORY_LABELS.get(row["category"], "unknown"),
            "thumbnail_path": row.get("thumbnail_path") or "",
            "thumbnail_url": media_url(row.get("thumbnail_path")),
        }

    def serialize_news_detail(row: dict[str, Any]) -> dict[str, Any]:
        images = [
            {
                "id": image["id"],
                "source_url": image["source_url"] or "",
                "local_path": image["local_path"] or "",
                "image_url": media_url(image["local_path"]),
                "description": image["img_desc"] or "",
                "is_cover": bit_to_bool(image["is_cover"]),
                "sort_order": image["sort_order"],
            }
            for image in row.get("images", [])
        ]
        cover_image = images[0] if images else None
        return {
            "id": row["id"],
            "site": row["site"],
            "source_url": row["url"],
            "news_date": row["news_date"].isoformat() if row.get("news_date") else None,
            "category": row["category"],
            "category_label": NEWS_CATEGORY_LABELS.get(row["category"], "unknown"),
            "creator": row.get("creator") or "",
            "title": row["title"],
            "chinese_title": row.get("chinese_title") or "",
            "summary": row.get("summary") or "",
            "chinese_summary": row.get("chinese_summary") or "",
            "content": row.get("content") or "",
            "chinese_content": row.get("chinese_content") or "",
            "bilingual_content": row.get("bilingual_content") or "",
            "cover_image": cover_image,
            "images": images,
            "create_time": row.get("create_time").isoformat() if row.get("create_time") else None,
            "update_time": row.get("update_time").isoformat() if row.get("update_time") else None,
        }

    def serialize_place_category(row: dict[str, Any]) -> dict[str, Any]:
        keyword = row["search_keyword"]
        return {
            "search_keyword": keyword,
            "display_name": keyword.replace("_", " ").title(),
            "place_count": int(row["place_count"] or 0),
            "sample_category": row.get("sample_category") or "",
        }

    def serialize_place_summary(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "place_hash": row["place_hash"],
            "record_hash": row["record_hash"],
            "name": row["name"],
            "category": row.get("category") or "",
            "search_keyword": row.get("search_keyword") or "",
            "opening_text": row.get("opening_text") or "",
            "closing_time": parse_closing_time(row.get("opening_text") or ""),
            "cover_image_path": row.get("cover_image_path") or "",
            "cover_image_url": media_url(row.get("cover_image_path")),
            "rating": row.get("rating") or "",
            "review_count": row.get("review_count") or "",
        }

    def serialize_place_detail(row: dict[str, Any]) -> dict[str, Any]:
        images = []
        for image in row.get("images", []):
            images.append(
                {
                    "image_hash": image["image_hash"],
                    "record_hash": image["record_hash"],
                    "place_hash": image["place_hash"],
                    "detail_url": image["detail_url"],
                    "source_url": image["image_url"],
                    "local_path": image["local_path"],
                    "image_url": media_url(image["local_path"]),
                    "image_order": image["image_order"],
                    "downloaded_at": image["downloaded_at"],
                }
            )

        image_paths = json_loads_safe(row.get("image_paths_json"))
        return {
            "place_hash": row["place_hash"],
            "record_hash": row["record_hash"],
            "search_keyword": row.get("search_keyword") or "",
            "search_rank": row.get("search_rank") or "",
            "name": row.get("name") or "",
            "category": row.get("category") or "",
            "address": row.get("address") or "",
            "phone": row.get("phone") or "",
            "website": row.get("website") or "",
            "rating": row.get("rating") or "",
            "review_count": row.get("review_count") or "",
            "opening_text": row.get("opening_text") or "",
            "closing_time": parse_closing_time(row.get("opening_text") or ""),
            "plus_code": row.get("plus_code") or "",
            "latitude": row.get("latitude") or "",
            "longitude": row.get("longitude") or "",
            "detail_url": row.get("detail_url") or "",
            "source_search_url": row.get("source_search_url") or "",
            "raw_text": row.get("raw_text") or "",
            "cover_image_path": row.get("cover_image_path") or "",
            "cover_image_url": media_url(row.get("cover_image_path")),
            "image_paths": image_paths,
            "image_urls": [media_url(path) for path in image_paths if path],
            "image_count": row.get("image_count") or "0",
            "crawled_at": row.get("crawled_at") or "",
            "images": images,
        }

    @app.get("/api/health")
    def health() -> Any:
        return jsonify({"status": "ok"})

    @app.get("/media/<path:filename>")
    def serve_media(filename: str) -> Any:
        return send_from_directory(downloads_root, filename)

    @app.get("/api/news")
    @app.get("/api/news/search")
    def news_list() -> Any:
        page = parse_int(request.args.get("page"), 1, minimum=1, maximum=100000)
        page_size = parse_int(request.args.get("page_size"), 10, minimum=1, maximum=50)
        category = parse_news_category(request.args.get("category"))
        rows, total = repository.get_news_list(
            page=page,
            page_size=page_size,
            q=(request.args.get("q") or "").strip(),
            news_date=(request.args.get("date") or "").strip(),
            date_from=(request.args.get("date_from") or "").strip(),
            date_to=(request.args.get("date_to") or "").strip(),
            category=category,
        )
        return jsonify(
            {
                "items": [serialize_news_summary(row) for row in rows],
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": math.ceil(total / page_size) if total else 0,
                },
            }
        )

    @app.get("/api/news/<int:news_id>")
    def news_detail(news_id: int) -> Any:
        row = repository.get_news_detail(news_id)
        if not row:
            abort(404, description="News not found")
        return jsonify(serialize_news_detail(row))

    @app.get("/api/place-categories")
    def place_categories() -> Any:
        rows = repository.get_place_categories()
        return jsonify({"items": [serialize_place_category(row) for row in rows]})

    @app.get("/api/places")
    def places_list() -> Any:
        page = parse_int(request.args.get("page"), 1, minimum=1, maximum=100000)
        page_size = parse_int(request.args.get("page_size"), 10, minimum=1, maximum=50)
        rows, total = repository.get_places(
            page=page,
            page_size=page_size,
            q=(request.args.get("q") or "").strip(),
            search_keyword=(request.args.get("search_keyword") or "").strip(),
            category=(request.args.get("category") or "").strip(),
        )
        return jsonify(
            {
                "items": [serialize_place_summary(row) for row in rows],
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": math.ceil(total / page_size) if total else 0,
                },
            }
        )

    @app.get("/api/places/<string:place_hash>")
    def place_detail(place_hash: str) -> Any:
        row = repository.get_place_detail(place_hash)
        if not row:
            abort(404, description="Place not found")
        return jsonify(serialize_place_detail(row))

    @app.errorhandler(404)
    def not_found(error: Exception) -> Any:
        return jsonify({"error": "not_found", "message": str(error)}), 404

    @app.errorhandler(400)
    def bad_request(error: Exception) -> Any:
        return jsonify({"error": "bad_request", "message": str(error)}), 400

    return app


app = create_app()


if __name__ == "__main__":
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host=host, port=port, debug=debug)
