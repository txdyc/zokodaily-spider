from __future__ import annotations

import asyncio
import logging

import aiohttp

from .config import Settings

logger = logging.getLogger("news-crawler")


class HttpClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "HttpClient":
        timeout = aiohttp.ClientTimeout(total=self.settings.timeout_seconds)
        headers = {"User-Agent": self.settings.user_agent}
        self._session = aiohttp.ClientSession(timeout=timeout, headers=headers)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()

    async def get_text(self, url: str) -> str:
        response = await self.get(url)
        try:
            return await response.text()
        finally:
            response.release()

    async def get(self, url: str) -> aiohttp.ClientResponse:
        if not self._session:
            raise RuntimeError("HttpClient session is not initialized.")
        for attempt in range(1, 4):
            try:
                response = await self._session.get(url, allow_redirects=True)
                response.raise_for_status()
                return response
            except Exception as exc:
                logger.error("GET failed (%s/3) for %s: %s", attempt, url, exc)
                if attempt == 3:
                    raise
                await asyncio.sleep(attempt)
