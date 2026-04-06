from .graphic import GraphicSpider
from .myjoy import MyJoySpider

SITE_REGISTRY = {
    "graphic": GraphicSpider,
    "myjoy": MyJoySpider,
    "myjoyonline": MyJoySpider,
}

__all__ = ["GraphicSpider", "MyJoySpider", "SITE_REGISTRY"]
