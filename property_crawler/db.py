from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from sqlalchemy import MetaData, Table, create_engine, select, text, tuple_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import URL

from news_crawler.config import Settings

from .models import PropertyRecord

logger = logging.getLogger("property-crawler")


class PropertyDatabaseClient:
    table_name = "zokodaily_property"
    image_table_name = "zokodaily_property_image"

    def __init__(self, settings: Settings):
        self.settings = settings
        self._ensure_database_exists()
        self.engine = create_engine(
            settings.db_url,
            future=True,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"use_unicode": settings.db_use_unicode, "charset": settings.db_charset},
        )
        self._ensure_tables()
        metadata = MetaData()
        self.table = Table(self.table_name, metadata, autoload_with=self.engine)
        self.image_table = Table(self.image_table_name, metadata, autoload_with=self.engine)

    def _ensure_database_exists(self) -> None:
        server_url = URL.create(
            "mysql+pymysql",
            username=self.settings.db_user,
            password=self.settings.db_password,
            host=self.settings.db_host,
            port=self.settings.db_port,
            query={"charset": self.settings.db_charset},
        ).render_as_string(hide_password=False)
        server_engine = create_engine(
            server_url,
            future=True,
            pool_pre_ping=True,
            connect_args={"use_unicode": self.settings.db_use_unicode, "charset": self.settings.db_charset},
        )
        safe_db_name = self.settings.db_name.replace("`", "``")
        with server_engine.begin() as conn:
            conn.exec_driver_sql(
                f"CREATE DATABASE IF NOT EXISTS `{safe_db_name}` "
                f"CHARACTER SET {self.settings.db_charset} COLLATE {self.settings.db_charset}_unicode_ci"
            )
        server_engine.dispose()

    def _ensure_tables(self) -> None:
        create_property_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
          `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'ID',
          `site` varchar(100) NOT NULL COMMENT 'source website',
          `source_id` bigint DEFAULT NULL COMMENT 'source platform listing id',
          `guid` varchar(64) NOT NULL DEFAULT '' COMMENT 'source guid',
          `section_key` varchar(100) NOT NULL COMMENT 'section key',
          `section_name` varchar(100) NOT NULL COMMENT 'section name',
          `category_name` varchar(100) DEFAULT '' COMMENT 'category name',
          `category_slug` varchar(100) DEFAULT '' COMMENT 'category slug',
          `title` varchar(255) NOT NULL COMMENT 'listing title',
          `price_amount` decimal(18,2) DEFAULT NULL COMMENT 'listing price',
          `currency` varchar(16) DEFAULT '' COMMENT 'currency',
          `price_text` varchar(128) DEFAULT '' COMMENT 'formatted price',
          `price_type` varchar(64) DEFAULT '' COMMENT 'price type',
          `price_period` varchar(64) DEFAULT '' COMMENT 'price period',
          `region_name` varchar(128) DEFAULT '' COMMENT 'region name',
          `region_slug` varchar(128) DEFAULT '' COMMENT 'region slug',
          `region_text` varchar(255) DEFAULT '' COMMENT 'full region text',
          `description` longtext COMMENT 'listing description',
          `url` varchar(500) NOT NULL COMMENT 'listing url',
          `estate_name` varchar(128) DEFAULT '' COMMENT 'estate name',
          `property_type` varchar(128) DEFAULT '' COMMENT 'property type',
          `property_size` varchar(64) DEFAULT '' COMMENT 'property size value',
          `property_size_unit` varchar(32) DEFAULT '' COMMENT 'property size unit',
          `bedrooms` varchar(32) DEFAULT '' COMMENT 'bedrooms',
          `bathrooms` varchar(32) DEFAULT '' COMMENT 'bathrooms',
          `furnishing` varchar(64) DEFAULT '' COMMENT 'furnishing',
          `parking_spot` varchar(128) DEFAULT '' COMMENT 'parking spot',
          `status_of_construction` varchar(128) DEFAULT '' COMMENT 'construction status',
          `seller_id` bigint DEFAULT NULL COMMENT 'seller id',
          `seller_guid` varchar(64) DEFAULT '' COMMENT 'seller guid',
          `seller_name` varchar(255) DEFAULT '' COMMENT 'seller name',
          `seller_page_url` varchar(500) DEFAULT '' COMMENT 'seller page url',
          `seller_phone` varchar(64) DEFAULT '' COMMENT 'seller phone',
          `seller_response_time` varchar(255) DEFAULT '' COMMENT 'seller response time',
          `seller_last_seen` varchar(128) DEFAULT '' COMMENT 'seller last seen',
          `view_count` int NOT NULL DEFAULT '0' COMMENT 'views',
          `fav_count` int NOT NULL DEFAULT '0' COMMENT 'favorites',
          `count_images` int NOT NULL DEFAULT '0' COMMENT 'image count',
          `is_promoted` bit(1) NOT NULL DEFAULT b'0' COMMENT 'is promoted',
          `is_negotiable` bit(1) NOT NULL DEFAULT b'0' COMMENT 'is negotiable',
          `is_active` bit(1) NOT NULL DEFAULT b'1' COMMENT 'is active',
          `is_closed` bit(1) NOT NULL DEFAULT b'0' COMMENT 'is closed',
          `posted_at_raw` varchar(128) DEFAULT '' COMMENT 'relative posted time',
          `date_created` datetime DEFAULT NULL COMMENT 'created at on source',
          `date_moderated` datetime DEFAULT NULL COMMENT 'moderated at on source',
          `attrs_json` longtext COMMENT 'listing attributes',
          `labels_json` longtext COMMENT 'listing labels',
          `seller_labels_json` longtext COMMENT 'seller labels',
          `breadcrumbs_json` longtext COMMENT 'breadcrumbs',
          `safety_tips_json` longtext COMMENT 'safety tips',
          `raw_payload_json` longtext COMMENT 'raw source payload',
          `creator` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'creator',
          `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
          `updater` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'updater',
          `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
          `deleted` bit(1) NOT NULL DEFAULT b'0' COMMENT 'is deleted',
          `tenant_id` bigint NOT NULL DEFAULT '0' COMMENT 'tenant id',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ghana property listings';
        """
        create_image_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.image_table_name}` (
          `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'ID',
          `property_id` bigint NOT NULL COMMENT 'related property id',
          `image_id` bigint DEFAULT NULL COMMENT 'source image id',
          `source_url` varchar(500) DEFAULT NULL COMMENT 'source image url',
          `local_path` varchar(500) DEFAULT NULL COMMENT 'downloaded local path',
          `alt_text` varchar(255) DEFAULT '' COMMENT 'image alt text',
          `is_main` bit(1) NOT NULL DEFAULT b'0' COMMENT 'is main image',
          `sort_order` int NOT NULL DEFAULT '0' COMMENT 'image order',
          `width` int DEFAULT NULL COMMENT 'image width',
          `height` int DEFAULT NULL COMMENT 'image height',
          `creator` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'creator',
          `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
          `updater` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'updater',
          `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
          `deleted` bit(1) NOT NULL DEFAULT b'0' COMMENT 'is deleted',
          `tenant_id` bigint NOT NULL DEFAULT '0' COMMENT 'tenant id',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ghana property images';
        """
        with self.engine.begin() as conn:
            conn.execute(text(create_property_sql))
            conn.execute(text(create_image_sql))
            indexes = {}
            for row in conn.execute(text(f"SHOW INDEX FROM `{self.table_name}`")):
                indexes.setdefault(row[2], []).append(row[4])
            image_indexes = {}
            for row in conn.execute(text(f"SHOW INDEX FROM `{self.image_table_name}`")):
                image_indexes.setdefault(row[2], []).append(row[4])
            if "uniq_site_guid" not in indexes:
                conn.execute(text(f"ALTER TABLE `{self.table_name}` ADD UNIQUE KEY `uniq_site_guid` (`site`,`guid`)"))
            if "uniq_site_url" not in indexes:
                conn.execute(text(f"ALTER TABLE `{self.table_name}` ADD UNIQUE KEY `uniq_site_url` (`site`,`url`)"))
            if "idx_section_key" not in indexes:
                conn.execute(text(f"ALTER TABLE `{self.table_name}` ADD INDEX `idx_section_key` (`section_key`)"))
            if "idx_region_slug" not in indexes:
                conn.execute(text(f"ALTER TABLE `{self.table_name}` ADD INDEX `idx_region_slug` (`region_slug`)"))
            if "idx_seller_id" not in indexes:
                conn.execute(text(f"ALTER TABLE `{self.table_name}` ADD INDEX `idx_seller_id` (`seller_id`)"))
            if "idx_property_id" not in image_indexes:
                conn.execute(text(f"ALTER TABLE `{self.image_table_name}` ADD INDEX `idx_property_id` (`property_id`)"))
            if "uniq_property_source_url" not in image_indexes:
                conn.execute(text(f"ALTER TABLE `{self.image_table_name}` ADD UNIQUE KEY `uniq_property_source_url` (`property_id`,`source_url`)"))

    def existing_urls(self, site: str, urls: Sequence[str]) -> set[str]:
        if not urls:
            return set()
        with self.engine.begin() as conn:
            query = select(self.table.c.url).where(self.table.c.site == site, self.table.c.url.in_(list(urls)))
            return {row[0] for row in conn.execute(query)}

    def insert_properties(self, records: list[PropertyRecord]) -> int:
        if not records:
            return 0
        property_rows = [record.to_db_payload() for record in records]
        stmt = mysql_insert(self.table).prefix_with("IGNORE")
        with self.engine.begin() as conn:
            result = conn.execute(stmt, property_rows)
            inserted = max(int(result.rowcount or 0), 0)
            image_rows = self._build_image_rows(conn, records)
            if image_rows:
                conn.execute(mysql_insert(self.image_table).prefix_with("IGNORE"), image_rows)
            return inserted

    def _build_image_rows(self, conn, records: list[PropertyRecord]) -> list[dict[str, Any]]:
        site_url_pairs = [(record.site, record.url) for record in records]
        query = select(self.table.c.id, self.table.c.site, self.table.c.url).where(tuple_(self.table.c.site, self.table.c.url).in_(site_url_pairs))
        id_map = {(row.site, row.url): row.id for row in conn.execute(query)}
        image_rows: list[dict[str, Any]] = []
        for record in records:
            property_id = id_map.get((record.site, record.url))
            if not property_id:
                continue
            for image in record.images:
                if not image.source_url and not image.local_path:
                    continue
                image_rows.append(image.to_db_payload(property_id=property_id, creator=record.seller_name))
        return image_rows
