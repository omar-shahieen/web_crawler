from crawler import threaded_crawel
from indexer import run_indexer
from typing import List

seed_urls: List[str] = [
    "https://www.wikipedia.org/",
    "https://curlie.org/",
    "https://news.google.com/",
    "https://www.reuters.com/",
    "https://www.reddit.com/",
    "https://news.ycombinator.com/",
    "https://www.bbc.com/news",
    "https://www.npr.org/",
    "https://www.nytimes.com/",
    "https://www.github.com/trending"
] 
threaded_crawel(seed_urls,max_pages=100 , max_workers=20,delay_range=(0.1,0.2))

run_indexer()