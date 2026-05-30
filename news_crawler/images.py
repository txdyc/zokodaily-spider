from __future__ import annotations

import hashlib
import logging
import mimetypes
from pathlib import Path
from urllib.parse import urlparse

from .config import Settings
from .http import HttpClient
from .utils import sanitize_image_url

logger = logging.getLogger("news-crawler")

# Browser-like Accept header for image requests. Many news image hosts (e.g.
# Cloudflare-fronted graphic.com.gh) serve HTML happily but gate static assets
# behind hotlink/bot checks that key on Accept/Referer, which is why downloads
# can fail from a server even when the article HTML downloads fine.
IMAGE_ACCEPT = "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"


class ImageDownloader:
    def __init__(self, settings: Settings):
        self.root = Path(settings.image_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    async def download(self, http: HttpClient, site: str, image_url: str, referer: str = "") -> str:
        image_url = sanitize_image_url(image_url, image_url)
        if not image_url:
            return ""
        file_hash = hashlib.sha256(image_url.encode("utf-8")).hexdigest()
        target_dir = self.root / site
        target_dir.mkdir(parents=True, exist_ok=True)
        existing = next(iter(target_dir.glob(f"{file_hash}.*")), None)
        if existing:
            return existing.as_posix()

        try:
            response = await http.get(image_url, headers=self._request_headers(image_url, referer))
            try:
                body = await response.read()
            finally:
                response.release()
        except Exception as exc:
            logger.error("Failed downloading image %s: %s", image_url, exc)
            return ""

        if not body:
            logger.error("Failed downloading image %s: empty response body", image_url)
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
    def _request_headers(image_url: str, referer: str) -> dict[str, str]:
        parsed = urlparse(image_url)
        origin = f"{parsed.scheme}://{parsed.netloc}/"
        return {
            "Accept": IMAGE_ACCEPT,
            # Prefer the article URL as Referer (same-site) to satisfy hotlink
            # protection; fall back to the image's own origin.
            "Referer": referer or origin,
        }

    @classmethod
    def _detect_extension(cls, image_url: str, content_type: str, body: bytes) -> str:
        guessed_type, _ = mimetypes.guess_type(image_url)
        content_type = (content_type or guessed_type or "").split(";")[0].strip().lower()
        if content_type and content_type.startswith("image/"):
            ext = mimetypes.guess_extension(content_type)
            if ext:
                return ".jpg" if ext == ".jpe" else ext
        kind = cls._sniff_image_kind(body)
        if kind:
            return ".jpg" if kind == "jpeg" else f".{kind}"
        suffix = Path(urlparse(image_url).path).suffix.lower()
        return suffix if suffix and len(suffix) <= 5 else ".jpg"

    @staticmethod
    def _sniff_image_kind(body: bytes) -> str:
        """Detect common image types from magic bytes.

        Replaces the stdlib ``imghdr`` module, which is deprecated since Python
        3.11 and removed in Python 3.13 (the deployment target is Ubuntu, where
        the interpreter version is not pinned).
        """
        if len(body) < 12:
            return ""
        if body[:3] == b"\xff\xd8\xff":
            return "jpeg"
        if body[:8] == b"\x89PNG\r\n\x1a\n":
            return "png"
        if body[:6] in (b"GIF87a", b"GIF89a"):
            return "gif"
        if body[:4] == b"RIFF" and body[8:12] == b"WEBP":
            return "webp"
        if body[:2] == b"BM":
            return "bmp"
        if body[:4] in (b"II*\x00", b"MM\x00*"):
            return "tiff"
        return ""
