from .jiji import JijiPropertySpider

SITE_REGISTRY = {
    "jiji": JijiPropertySpider,
}

__all__ = ["JijiPropertySpider", "SITE_REGISTRY"]
