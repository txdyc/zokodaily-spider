from __future__ import annotations

import hashlib
import imghdr
import logging
import mimetypes
from pathlib import Path
from urllib.parse import urlparse

from .config import Settings
from .http import HttpClient

logger = logging.getLogger("news-crawler")


class ImageDownloader:
    def __init__(self, settings: Settings):
        self.root = Path(settings.image_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    async def download(self, http: HttpClient, site: str, image_url: str) -> str:
        if not image_url:
            return ""
        file_hash = hashlib.sha256(image_url.encode("utf-8")).hexdigest()
        target_dir = self.root / site
        target_dir.mkdir(parents=True, exist_ok=True)
        existing = next(iter(target_dir.glob(f"{file_hash}.*")), None)
        if existing:
            return existing.as_posix()

        try:
            response = await http.get(image_url)
            try:
                body = await response.read()
            finally:
                response.release()
        except Exception as exc:
            logger.error("Failed downloading image %s: %s", image_url, exc)
            return ""

        extension = self._detect_extension(image_url, response.headers.get("Content-Type", ""), body)
        target_path = target_dir / f"{file_hash}{extension}"
        try:
            target_path.write_bytes(body)
            return target_path.as_posix()
        except Exception as exc:
            logger.error("Failed writing image %s: %s", target_path, exc)
            return ""

    @staticmethod
    def _detect_extension(image_url: str, content_type: str, body: bytes) -> str:
        guessed_type, _ = mimetypes.guess_type(image_url)
        content_type = (content_type or guessed_type or "").split(";")[0].strip().lower()
        if content_type:
            ext = mimetypes.guess_extension(content_type)
            if ext:
                return ".jpg" if ext == ".jpe" else ext
        kind = imghdr.what(None, h=body)
        if kind:
            return ".jpg" if kind == "jpeg" else f".{kind}"
        suffix = Path(urlparse(image_url).path).suffix.lower()
        return suffix if suffix and len(suffix) <= 5 else ".jpg"
