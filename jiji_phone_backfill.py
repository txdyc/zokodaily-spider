from __future__ import annotations

import argparse
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path

from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright
from sqlalchemy import create_engine, text

from news_crawler.config import Settings


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
PHONE_REGEX = re.compile(r"(?:\+?233|0)[\s-]*[235](?:[\s-]*\d){8}")


@dataclass
class PropertyRow:
    record_id: int
    url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Jiji seller_phone values in zokodaily_property")
    parser.add_argument("--limit", type=int, default=50, help="Maximum number of missing rows to process")
    parser.add_argument("--headful", action="store_true", help="Run browser in headed mode")
    parser.add_argument("--delay-ms", type=int, default=1500, help="Delay between listings in milliseconds")
    parser.add_argument(
        "--storage-state",
        default=os.getenv("JIJI_STORAGE_STATE", "downloads/jiji_storage_state.json"),
        help="Playwright storage state file to reuse/save login session",
    )
    parser.add_argument("--email", default=os.getenv("JIJI_EMAIL", ""), help="Jiji login email or phone")
    parser.add_argument("--password", default=os.getenv("JIJI_PASSWORD", ""), help="Jiji login password")
    parser.add_argument("--site", default="jiji", help="Site key to backfill")
    return parser.parse_args()


class JijiPhoneBackfiller:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.settings = Settings()
        self.engine = create_engine(self.settings.db_url, future=True)
        self.storage_state = Path(args.storage_state)
        self.storage_state.parent.mkdir(parents=True, exist_ok=True)

    def fetch_missing_rows(self) -> list[PropertyRow]:
        query = text(
            """
            SELECT id, url
            FROM zokodaily_property
            WHERE site = :site
              AND COALESCE(NULLIF(TRIM(seller_phone), ''), '') = ''
            ORDER BY id ASC
            LIMIT :limit
            """
        )
        with self.engine.begin() as conn:
            rows = conn.execute(query, {"site": self.args.site, "limit": self.args.limit}).fetchall()
        return [PropertyRow(record_id=row.id, url=row.url) for row in rows]

    def update_phone(self, record_id: int, phone: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE zokodaily_property
                    SET seller_phone = :phone, update_time = CURRENT_TIMESTAMP
                    WHERE id = :record_id
                    """
                ),
                {"phone": phone, "record_id": record_id},
            )

    def run(self) -> int:
        rows = self.fetch_missing_rows()
        if not rows:
            print("No missing seller_phone rows found.")
            return 0

        updated = 0
        with sync_playwright() as playwright:
            launch_options = {"headless": not self.args.headful}
            browser = playwright.chromium.launch(**launch_options)
            context = self._create_context(browser)
            page = context.new_page()
            self.ensure_logged_in(page)

            for index, row in enumerate(rows, start=1):
                print(f"[{index}/{len(rows)}] Processing {row.record_id} {row.url}")
                phone = self.extract_phone(page, row.url)
                if not phone:
                    print(f"  no phone extracted for {row.record_id}")
                    continue
                self.update_phone(row.record_id, phone)
                updated += 1
                print(f"  updated seller_phone={phone}")
                context.storage_state(path=str(self.storage_state))
                if self.args.delay_ms > 0:
                    time.sleep(self.args.delay_ms / 1000)

            context.close()
            browser.close()
        print(f"Updated {updated} rows.")
        return updated

    def _create_context(self, browser) -> BrowserContext:
        context_kwargs = {
            "user_agent": DEFAULT_USER_AGENT,
            "locale": "en-US",
            "timezone_id": "Africa/Accra",
            "viewport": {"width": 1280, "height": 900},
        }
        if self.storage_state.exists():
            context_kwargs["storage_state"] = str(self.storage_state)
        return browser.new_context(**context_kwargs)

    def ensure_logged_in(self, page: Page) -> None:
        page.goto("https://jiji.com.gh/", wait_until="networkidle", timeout=120000)
        if self._looks_logged_in(page):
            print("Using existing Jiji session.")
            return

        if not self.args.email or not self.args.password:
            raise RuntimeError(
                "Jiji login is required to see contacts. Set JIJI_EMAIL and JIJI_PASSWORD, "
                "or provide a valid storage state file."
            )

        print("Logging into Jiji...")
        page.goto("https://jiji.com.gh/?auth=Login", wait_until="networkidle", timeout=120000)
        self._open_email_login(page)
        login_field = page.locator(".qa-login-field").last
        password_field = page.locator(".qa-password-field").last
        login_field.fill(self.args.email)
        password_field.fill(self.args.password)
        submit = page.locator(".qa-login-submit").last
        submit.click()
        page.wait_for_timeout(5000)
        if not self._looks_logged_in(page):
            if page.get_by_text("Sign in to see contacts", exact=False).count():
                raise RuntimeError("Jiji login failed; sign-in prompt is still shown.")
            raise RuntimeError("Jiji login did not complete. Check credentials or solve any extra verification.")
        self.storage_state.parent.mkdir(parents=True, exist_ok=True)
        page.context.storage_state(path=str(self.storage_state))
        print("Jiji login successful.")

    def _open_email_login(self, page: Page) -> None:
        if page.locator(".qa-login-field").count() and page.locator(".qa-password-field").count():
            return
        email_or_phone_button = page.get_by_role("button", name="E-mail or phone")
        if email_or_phone_button.count():
            email_or_phone_button.click(timeout=30000)
            page.wait_for_timeout(1000)
        if not page.locator(".qa-login-field").count():
            raise RuntimeError("Could not open Jiji email/password login form.")

    def _looks_logged_in(self, page: Page) -> bool:
        markers = [
            page.locator(".qa-user-avatar"),
            page.get_by_text("My profile", exact=False),
            page.get_by_text("Logout", exact=False),
        ]
        return any(marker.count() for marker in markers)

    def extract_phone(self, page: Page, url: str) -> str | None:
        page.goto(url, wait_until="networkidle", timeout=120000)
        show_contact = page.locator(".js-show-contact").first
        if show_contact.count() == 0:
            return self._extract_phone_from_text(page)

        show_contact.scroll_into_view_if_needed(timeout=10000)
        show_contact.click(timeout=30000, force=True)
        page.wait_for_timeout(2500)

        if page.get_by_text("Sign in to see contacts", exact=False).count():
            raise RuntimeError("Current Jiji session cannot view contacts.")

        phone = self._extract_phone_from_links(page) or self._extract_phone_from_text(page)
        return phone

    def _extract_phone_from_links(self, page: Page) -> str | None:
        links = page.locator("a[href^='tel:']")
        for index in range(links.count()):
            href = links.nth(index).get_attribute("href") or ""
            phone = normalize_phone(href.replace("tel:", ""))
            if phone:
                return phone
        return None

    def _extract_phone_from_text(self, page: Page) -> str | None:
        text_content = page.locator("body").inner_text(timeout=30000)
        match = PHONE_REGEX.search(text_content)
        if not match:
            return None
        return normalize_phone(match.group(0))


def normalize_phone(value: str) -> str | None:
    digits = re.sub(r"\D+", "", value or "")
    if not digits:
        return None
    if digits.startswith("233") and len(digits) == 12:
        return f"+{digits}"
    if digits.startswith("0") and len(digits) == 10:
        return digits
    if len(digits) == 9 and digits[:2] in {"20", "23", "24", "25", "26", "27", "28", "50", "53", "54", "55", "57", "59"}:
        return f"0{digits}"
    return f"+{digits}" if value.strip().startswith("+") else digits


def main() -> int:
    args = parse_args()
    backfiller = JijiPhoneBackfiller(args)
    try:
        return backfiller.run()
    except PlaywrightTimeoutError as exc:
        print(f"Playwright timeout: {exc}")
        return 1
    except Exception as exc:
        print(f"Backfill failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
