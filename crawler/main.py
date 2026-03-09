"""
main.py — Entry point. Only concerns: seed list + CLI args.
"""
from typing import List

from crawler import threaded_crawl



SEED_URLS: List[str] = [
    "https://www.wikipedia.org/",
    "https://curlie.org/",
    "https://news.google.com/",
    "https://www.reuters.com/",
    "https://www.reddit.com/",
    "https://news.ycombinator.com/",
    "https://www.bbc.com/news",
    "https://www.npr.org/",
    "https://www.nytimes.com/",
    "https://www.github.com/trending",
    "https://stackoverflow.com",
]

if __name__ == "__main__":
    threaded_crawl(
        seed_urls=SEED_URLS,
        max_pages=100,
        max_workers=10,
        )