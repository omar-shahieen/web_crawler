from typing import List, Optional, Tuple

from services.crawl_core.crawler import threaded_crawl
from services.crawl_core.indexer import run_indexer


SEED_URLS: List[str] = [
    "https://www.wikipedia.org/",
    "https://www.britannica.com/",
    "https://www.bbc.com/news",
    "https://www.nytimes.com/",
    "https://www.theguardian.com/",
    "https://www.reuters.com/",
    "https://github.com/trending",
    "https://news.ycombinator.com/",
    "https://stackoverflow.com/",
    "https://www.khanacademy.org/",
    "https://ocw.mit.edu/",
    "https://www.reddit.com/",
    "https://dev.to/",
    "https://www.cnn.com/",
    "https://www.aljazeera.com/",
]


def crawl_web(
    seed_urls: Optional[List[str]] = None,
    max_pages: int = 100,
    max_workers: int = 10,
    delay_range: Tuple[float, float] = (1.5, 3.0),
):
    return threaded_crawl(
        seed_urls=seed_urls or SEED_URLS,
        max_pages=max_pages,
        max_workers=max_workers,
        delay_range=delay_range,
    )


def index_content() -> None:
    run_indexer()


def crawl_and_index(
    seed_urls: Optional[List[str]] = None,
    max_pages: int = 100,
    max_workers: int = 10,
    delay_range: Tuple[float, float] = (1.5, 3.0),
) -> None:
    crawl_web(
        seed_urls=seed_urls,
        max_pages=max_pages,
        max_workers=max_workers,
        delay_range=delay_range,
    )
    index_content()