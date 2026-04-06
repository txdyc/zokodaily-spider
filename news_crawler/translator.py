from __future__ import annotations

import asyncio
import json
import logging
import re

import aiohttp

from .config import Settings
from .utils import clean_text

logger = logging.getLogger("news-crawler")


class LLMTranslator:
    def __init__(self, settings: Settings):
        self.settings = settings

    @property
    def enabled(self) -> bool:
        return bool(self.settings.llm_api_key)

    async def _json_completion(self, system: str, payload: dict[str, object]) -> dict[str, object]:
        if not self.enabled:
            return {}
        try:
            headers = {"Authorization": f"Bearer {self.settings.llm_api_key}", "Content-Type": "application/json"}
            body = {
                "model": self.settings.llm_model,
                "temperature": 0.1,
                "response_format": {"type": "json_object"},
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
            }
            timeout = aiohttp.ClientTimeout(total=90)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.settings.llm_endpoint, headers=headers, json=body) as resp:
                    resp.raise_for_status()
                    content = (await resp.json())["choices"][0]["message"]["content"]
                    return json.loads(re.sub(r"^```json|```$", "", content.strip(), flags=re.M))
        except Exception as exc:
            logger.error("LLM request failed: %s", exc)
            return {}

    async def extract_article(self, raw_text: str) -> dict[str, str]:
        system = "Extract news article fields from text. Return JSON with title, summary, news_date, content. Leave unknown fields empty."
        result = await self._json_completion(system, {"text": raw_text[:14000]})
        return {key: clean_text(str(value)) for key, value in result.items() if isinstance(value, str)}

    async def translate(self, title: str, summary: str, content: str) -> dict[str, str]:
        if not self.enabled:
            return {
                "chinese_title": "",
                "chinese_summary": "",
                "chinese_content": "",
                "bilingual_content": "",
            }

        short_task = self.translate_title_summary(title, summary)
        content_task = self.translate_bilingual_content(content)
        short_result, content_result = await asyncio.gather(short_task, content_task)
        return {
            "chinese_title": short_result.get("chinese_title", ""),
            "chinese_summary": short_result.get("chinese_summary", ""),
            "chinese_content": content_result.get("chinese_content", ""),
            "bilingual_content": content_result.get("bilingual_content", ""),
        }

    async def translate_title_summary(self, title: str, summary: str) -> dict[str, str]:
        system = (
            "You are a professional news translator. Translate the given title and summary into natural, faithful "
            "Simplified Chinese. Keep names, numbers, titles, institutions, and quoted meaning accurate. "
            "Do not add explanations. Return JSON with chinese_title and chinese_summary."
        )
        result = await self._json_completion(
            system,
            {
                "title": title.strip(),
                "summary": summary.strip(),
            },
        )
        return {
            "chinese_title": clean_text(str(result.get("chinese_title", ""))),
            "chinese_summary": clean_text(str(result.get("chinese_summary", ""))),
        }

    async def translate_bilingual_content(self, content: str) -> dict[str, str]:
        paragraphs = [clean_text(paragraph) for paragraph in content.splitlines() if clean_text(paragraph)]
        if not paragraphs:
            return {"chinese_content": "", "bilingual_content": ""}

        chunks = self._chunk_paragraphs(paragraphs, max_chars=5500)
        bilingual_blocks: list[str] = []
        chinese_blocks: list[str] = []

        for chunk in chunks:
            translations = await self._translate_paragraph_chunk(chunk)
            if not translations:
                translations = await self._translate_paragraphs_one_by_one(chunk)
            if not translations:
                continue
            for source, target in zip(chunk, translations):
                source_text = clean_text(source)
                target_text = clean_text(target)
                if not source_text or not target_text:
                    continue
                bilingual_blocks.append(f"{source_text}\n{target_text}")
                chinese_blocks.append(target_text)

        return {
            "chinese_content": "\n\n".join(chinese_blocks).strip(),
            "bilingual_content": "\n\n".join(bilingual_blocks).strip(),
        }

    async def _translate_paragraph_chunk(self, paragraphs: list[str]) -> list[str]:
        system = (
            "You are a professional news translator. Translate each paragraph into Simplified Chinese. "
            "Preserve the original paragraph order. Do not merge or split paragraphs. "
            "Do not summarize or omit information. Return JSON object with one field: "
            "translations, which must be an array of translated paragraphs with the same length as the input."
        )
        result = await self._json_completion(system, {"paragraphs": paragraphs})
        translations = result.get("translations", [])
        if not isinstance(translations, list):
            return []

        cleaned = [clean_text(str(item)) for item in translations]
        if len(cleaned) != len(paragraphs):
            logger.warning("Translation paragraph count mismatch: expected %s, got %s", len(paragraphs), len(cleaned))
            return []
        return cleaned

    async def _translate_paragraphs_one_by_one(self, paragraphs: list[str]) -> list[str]:
        translations: list[str] = []
        for paragraph in paragraphs:
            translated = await self._translate_paragraph_chunk([paragraph])
            if not translated or not translated[0]:
                logger.warning("Single paragraph translation fallback failed.")
                return []
            translations.append(translated[0])
        return translations

    @staticmethod
    def _chunk_paragraphs(paragraphs: list[str], max_chars: int) -> list[list[str]]:
        chunks: list[list[str]] = []
        current_chunk: list[str] = []
        current_size = 0

        for paragraph in paragraphs:
            paragraph_size = len(paragraph)
            if current_chunk and current_size + paragraph_size > max_chars:
                chunks.append(current_chunk)
                current_chunk = []
                current_size = 0
            current_chunk.append(paragraph)
            current_size += paragraph_size

        if current_chunk:
            chunks.append(current_chunk)
        return chunks
