from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from sqlalchemy import MetaData, Table, create_engine, select, text, tuple_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import URL

from .config import Settings
from .models import ArticleRecord

logger = logging.getLogger("news-crawler")


class DatabaseClient:
    table_name = "zokodaily_news"
    image_table_name = "zokodaily_news_image"

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
        create_news_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.table_name}` (
          `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'ID',
          `site` varchar(100) NOT NULL COMMENT 'source website',
          `title` varchar(255) NOT NULL COMMENT 'news title',
          `chinese_title` varchar(255) DEFAULT NULL COMMENT 'chinese title',
          `summary` text COMMENT 'news summary',
          `chinese_summary` text COMMENT 'chinese summary',
          `news_date` date NOT NULL COMMENT 'news date',
          `content` longtext NOT NULL COMMENT 'news content',
          `chinese_content` longtext COMMENT 'chinese content',
          `bilingual_content` longtext DEFAULT NULL,
          `url` varchar(255) NOT NULL COMMENT 'news url',
          `category` int NOT NULL DEFAULT '0' COMMENT 'category, 0:general,1:political, 2:economy,3:entertaining,4:headline',
          `creator` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'creator',
          `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
          `updater` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'updater',
          `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
          `deleted` bit(1) NOT NULL DEFAULT b'0' COMMENT 'is deleted',
          `tenant_id` bigint NOT NULL DEFAULT '0' COMMENT 'tenant id',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ghana news';
        """
        create_image_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.image_table_name}` (
          `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'ID',
          `news_id` bigint NOT NULL COMMENT 'related news id',
          `source_url` varchar(500) DEFAULT NULL COMMENT 'source image url',
          `local_path` varchar(500) DEFAULT NULL COMMENT 'downloaded local path',
          `img_desc` varchar(255) DEFAULT NULL COMMENT 'image description',
          `is_cover` bit(1) NOT NULL DEFAULT b'1' COMMENT 'is cover image',
          `sort_order` int NOT NULL DEFAULT '0' COMMENT 'image order',
          `creator` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'creator',
          `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
          `updater` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'updater',
          `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
          `deleted` bit(1) NOT NULL DEFAULT b'0' COMMENT 'is deleted',
          `tenant_id` bigint NOT NULL DEFAULT '0' COMMENT 'tenant id',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='ghana news images';
        """
        required_columns = {
            "site": "ALTER TABLE `zokodaily_news` ADD COLUMN `site` varchar(100) NOT NULL DEFAULT '' AFTER `id`",
            "creator": (
                "ALTER TABLE `zokodaily_news` "
                "ADD COLUMN `creator` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' AFTER `category`"
            ),
            "create_time": "ALTER TABLE `zokodaily_news` ADD COLUMN `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER `creator`",
            "updater": (
                "ALTER TABLE `zokodaily_news` "
                "ADD COLUMN `updater` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' AFTER `create_time`"
            ),
            "update_time": (
                "ALTER TABLE `zokodaily_news` "
                "ADD COLUMN `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER `updater`"
            ),
            "deleted": "ALTER TABLE `zokodaily_news` ADD COLUMN `deleted` bit(1) NOT NULL DEFAULT b'0' AFTER `update_time`",
            "tenant_id": "ALTER TABLE `zokodaily_news` ADD COLUMN `tenant_id` bigint NOT NULL DEFAULT '0' AFTER `deleted`",
            "bilingual_content": "ALTER TABLE `zokodaily_news` ADD COLUMN `bilingual_content` longtext DEFAULT NULL AFTER `chinese_content`",
        }
        required_image_columns = {
            "news_id": f"ALTER TABLE `{self.image_table_name}` ADD COLUMN `news_id` bigint NOT NULL AFTER `id`",
            "source_url": f"ALTER TABLE `{self.image_table_name}` ADD COLUMN `source_url` varchar(500) DEFAULT NULL AFTER `news_id`",
            "local_path": f"ALTER TABLE `{self.image_table_name}` ADD COLUMN `local_path` varchar(500) DEFAULT NULL AFTER `source_url`",
            "img_desc": f"ALTER TABLE `{self.image_table_name}` ADD COLUMN `img_desc` varchar(255) DEFAULT NULL AFTER `local_path`",
            "is_cover": f"ALTER TABLE `{self.image_table_name}` ADD COLUMN `is_cover` bit(1) NOT NULL DEFAULT b'1' AFTER `img_desc`",
            "sort_order": f"ALTER TABLE `{self.image_table_name}` ADD COLUMN `sort_order` int NOT NULL DEFAULT '0' AFTER `is_cover`",
            "creator": (
                f"ALTER TABLE `{self.image_table_name}` "
                "ADD COLUMN `creator` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' AFTER `sort_order`"
            ),
            "create_time": f"ALTER TABLE `{self.image_table_name}` ADD COLUMN `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER `creator`",
            "updater": (
                f"ALTER TABLE `{self.image_table_name}` "
                "ADD COLUMN `updater` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '' AFTER `create_time`"
            ),
            "update_time": (
                f"ALTER TABLE `{self.image_table_name}` "
                "ADD COLUMN `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER `updater`"
            ),
            "deleted": f"ALTER TABLE `{self.image_table_name}` ADD COLUMN `deleted` bit(1) NOT NULL DEFAULT b'0' AFTER `update_time`",
            "tenant_id": f"ALTER TABLE `{self.image_table_name}` ADD COLUMN `tenant_id` bigint NOT NULL DEFAULT '0' AFTER `deleted`",
        }

        with self.engine.begin() as conn:
            conn.execute(text(create_news_sql))
            conn.execute(text(create_image_sql))
            columns = {row[0] for row in conn.execute(text(f"SHOW COLUMNS FROM `{self.table_name}`"))}
            for column, alter_sql in required_columns.items():
                if column not in columns:
                    conn.execute(text(alter_sql))
            image_columns = {row[0] for row in conn.execute(text(f"SHOW COLUMNS FROM `{self.image_table_name}`"))}
            for column, alter_sql in required_image_columns.items():
                if column not in image_columns:
                    conn.execute(text(alter_sql))

            indexes = {}
            for row in conn.execute(text(f"SHOW INDEX FROM `{self.table_name}`")):
                indexes.setdefault(row[2], []).append(row[4])
            image_indexes = {}
            for row in conn.execute(text(f"SHOW INDEX FROM `{self.image_table_name}`")):
                image_indexes.setdefault(row[2], []).append(row[4])

            if "uni" in indexes and indexes["uni"] == ["title", "news_date"]:
                conn.execute(text(f"ALTER TABLE `{self.table_name}` DROP INDEX `uni`"))
                indexes.pop("uni", None)

            if "uniq_site_title_date" not in indexes:
                try:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{self.table_name}` "
                            "ADD UNIQUE KEY `uniq_site_title_date` (`site`,`title`,`news_date`)"
                        )
                    )
                except Exception as exc:
                    logger.warning("Could not add uniq_site_title_date: %s", exc)

            if "uniq_site_url" not in indexes:
                try:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{self.table_name}` "
                            "ADD UNIQUE KEY `uniq_site_url` (`site`,`url`)"
                        )
                    )
                except Exception as exc:
                    logger.warning("Could not add uniq_site_url: %s", exc)

            if "idx_news_id" not in image_indexes:
                conn.execute(text(f"ALTER TABLE `{self.image_table_name}` ADD INDEX `idx_news_id` (`news_id`)"))
            if "uniq_news_source_url" not in image_indexes:
                try:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{self.image_table_name}` "
                            "ADD UNIQUE KEY `uniq_news_source_url` (`news_id`,`source_url`)"
                        )
                    )
                except Exception as exc:
                    logger.warning("Could not add uniq_news_source_url: %s", exc)

            self._migrate_legacy_image_columns(conn, columns)

    def _migrate_legacy_image_columns(self, conn, news_columns: set[str]) -> None:
        has_img = "img" in news_columns
        has_img_desc = "img_desc" in news_columns
        if has_img:
            conn.execute(
                text(
                    f"""
                    INSERT IGNORE INTO `{self.image_table_name}`
                    (`news_id`, `source_url`, `local_path`, `img_desc`, `is_cover`, `sort_order`, `creator`, `updater`, `deleted`, `tenant_id`)
                    SELECT
                      `id`,
                      CASE WHEN `img` LIKE 'http://%' OR `img` LIKE 'https://%' THEN `img` ELSE '' END,
                      CASE WHEN `img` LIKE 'http://%' OR `img` LIKE 'https://%' THEN '' ELSE `img` END,
                      {('`img_desc`' if has_img_desc else "''")},
                      b'1',
                      0,
                      COALESCE(`creator`, ''),
                      COALESCE(`updater`, ''),
                      COALESCE(`deleted`, b'0'),
                      COALESCE(`tenant_id`, 0)
                    FROM `{self.table_name}`
                    WHERE `img` IS NOT NULL AND `img` <> ''
                    """
                )
            )
            conn.execute(text(f"ALTER TABLE `{self.table_name}` DROP COLUMN `img`"))
        if has_img_desc:
            conn.execute(text(f"ALTER TABLE `{self.table_name}` DROP COLUMN `img_desc`"))

    def existing_urls(self, site: str, urls: Sequence[str]) -> set[str]:
        if not urls:
            return set()
        with self.engine.begin() as conn:
            query = select(self.table.c.url).where(self.table.c.site == site, self.table.c.url.in_(list(urls)))
            return {row[0] for row in conn.execute(query)}

    def insert_articles(self, records: list[ArticleRecord]) -> int:
        if not records:
            return 0
        news_rows = [record.to_news_payload() for record in records]
        stmt = mysql_insert(self.table).prefix_with("IGNORE")
        with self.engine.begin() as conn:
            result = conn.execute(stmt, news_rows)
            inserted = max(int(result.rowcount or 0), 0)
            image_rows = self._build_image_rows(conn, records)
            if image_rows:
                conn.execute(mysql_insert(self.image_table).prefix_with("IGNORE"), image_rows)
            return inserted

    def _build_image_rows(self, conn, records: list[ArticleRecord]) -> list[dict[str, Any]]:
        site_url_pairs = [(record.site, record.url) for record in records]
        query = (
            select(self.table.c.id, self.table.c.site, self.table.c.url)
            .where(tuple_(self.table.c.site, self.table.c.url).in_(site_url_pairs))
        )
        id_map = {(row.site, row.url): row.id for row in conn.execute(query)}
        image_rows: list[dict[str, Any]] = []
        for record in records:
            news_id = id_map.get((record.site, record.url))
            if not news_id:
                continue
            for image in record.images:
                if not image.source_url and not image.local_path:
                    continue
                image_rows.append(image.to_db_payload(news_id=news_id, creator=record.creator))
        return image_rows
