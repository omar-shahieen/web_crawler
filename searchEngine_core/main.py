"""
main.py — Entry point. Only concerns: seed list + CLI args.
"""
from typing import List

# Initialize logging before importing other modules so handlers are configured
from log import setup_logging
setup_logging()

from crawler import threaded_crawl
from indexer import run_indexer


SEED_URLS: List[str] = [
    # Knowledge / Reference
    "https://www.wikipedia.org/",          # encyclopedic, huge number of links
    "https://www.britannica.com/",        # authoritative articles

    # News
    "https://www.bbc.com/news",           # international news
    "https://www.nytimes.com/",           # US news
    "https://www.theguardian.com/",       # UK news
    "https://www.reuters.com/",           # financial + global news

    # Tech / Programming
    "https://github.com/trending",        # trending repositories
    "https://news.ycombinator.com/",      # hacker news
    "https://stackoverflow.com/",         # programming Q&A

    # Education / Science
    "https://www.khanacademy.org/",       # educational content
    "https://ocw.mit.edu/",               # open courseware

    # Communities / Forums
    "https://www.reddit.com/",            # general discussions
    "https://dev.to/",                    # developer community

    # Misc High-Link Hubs
    "https://www.cnn.com/",               # news + high outbound links
    "https://www.aljazeera.com/",         # global news perspective
]
if __name__ == "__main__":
    # threaded_crawl(
    #     seed_urls=SEED_URLS,
    #     max_pages=100,
    #     max_workers=10,
    #     delay_range=(1.5,3)
    #     )
    
    run_indexer()