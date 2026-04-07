import os
from dataclasses import dataclass

from sqlalchemy.engine import URL


@dataclass
class Settings:
    db_name: str = os.getenv("DB_NAME", "zokodaily")
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "3306"))
    db_user: str = os.getenv("DB_USER", "root")
    db_password: str = os.getenv("DB_PASSWORD", "Napster@1009")
    db_charset: str = os.getenv("DB_CHARSET", "utf8mb4")
    db_use_unicode: bool = os.getenv("DB_USE_UNICODE", "true").lower() == "true"
    llm_api_key: str = os.getenv("CLOSEAI_API_KEY", os.getenv("LLM_API_KEY", "sk-iLVlw7F2p9RjUEqz6rZRM2nJisziatf1WVBh2Q6oA7YZItK1"))
    llm_model: str = os.getenv("LLM_MODEL", "gpt-4o-mini")
    llm_base_url: str = os.getenv("CLOSEAI_BASE_URL", os.getenv("LLM_BASE_URL", "https://api.openai-proxy.org/v1"))
    max_pages: int = int(os.getenv("MAX_PAGES", "3"))
    max_articles: int = int(os.getenv("MAX_ARTICLES", "0"))
    timeout_seconds: int = int(os.getenv("CRAWL_TIMEOUT_SECONDS", "45"))
    concurrency: int = int(os.getenv("CRAWL_CONCURRENCY", "4"))
    image_dir: str = os.getenv("NEWS_IMAGE_DIR", "downloads/news_images")
    property_image_dir: str = os.getenv("PROPERTY_IMAGE_DIR", "downloads/property_images")
    user_agent: str = os.getenv(
        "CRAWL_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    )

    @property
    def db_url(self) -> str:
        explicit_url = os.getenv("DATABASE_URL")
        if explicit_url:
            return explicit_url
        return URL.create(
            "mysql+pymysql",
            username=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            query={"charset": self.db_charset},
        ).render_as_string(hide_password=False)

    @property
    def llm_endpoint(self) -> str:
        return self.llm_base_url.rstrip("/") + "/chat/completions"
