import requests
from bs4 import BeautifulSoup
import csv
from collections import defaultdict, deque
from urllib.parse import urlparse , urljoin
from requests.exceptions import RequestException
from urllib.robotparser import RobotFileParser
from concurrent.futures import ThreadPoolExecutor ,as_completed
import threading
import queue # thread safe queue
import time
import random
import logging
import hashlib
import os
from typing import Optional, Set, Dict, Deque, Iterable, List, Tuple

# logging config
logging.basicConfig(
    filename="crawler.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


USER_AGENTS = [
    # Windows - Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",

    # Windows - Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",

    # Windows - Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",

    # macOS - Chrome
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",

    # macOS - Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",

    # macOS - Firefox
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5; rv:121.0) Gecko/20100101 Firefox/121.0",

    # Linux - Chrome
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",

    # Linux - Firefox
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",

    # Android - Chrome
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36",

    # Android - Firefox
    "Mozilla/5.0 (Android 13; Mobile; rv:121.0) Gecko/121.0 Firefox/121.0",

    # iPhone - Safari
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1",

    # iPad - Safari
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1",

    # iPhone - Chrome
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/537.36 CriOS/120.0.0.0 Mobile/15E148 Safari/537.36",
]

# for future
# PROXIES= [
#     "http://proxy1.example.com:8080",
#     "http://proxy2.example.com:8000",
# ]


# filter for paths and queries
EXCLUDED_PATHS = ["/login", "/admin", "/signup", "/cart", "/checkout"]
EXCLUDED_QUERY_PARAMS = [ "q=", "search=", "filter=", "sort=","sessionid","utm_","ref","page" ]



# filter for extensions
IGNORE_EXTENSIONS = (
'.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.ico', 
'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.tar', 
'.mp4', '.mp3', '.css', '.js')



# Fetch html web page by given link

def fetch(url: str, max_tries: int = 3) -> str:
    headers = {
        'User-Agent' : random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive"
    }
    
    # proxies to distribute traffic across IPs
    # proxies = {"http": random.choice(PROXIES), "https": random.choice(PROXIES)}
    
    for attempt in range(max_tries):
        try : 
            response = requests.get(
                url,
                timeout=5 ,
                headers=headers,
                # proxies=proxies
                )

            if response.status_code == 200 :
                
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' in content_type:
                    return response.text
                
            break
        except RequestException as e :
            logging.warning(f"[retry {attempt + 1}] error crul {url} : {e} ")
            time.sleep(1.5 + attempt * 1.0) # increasing backoff
    return ""


#  extract links from the fetched web page 
def extract_links(html: str, base_url: str) -> Set[str]:
    
    parsed_page = BeautifulSoup(html, 'html.parser')
    
    parsed_base = urlparse(base_url)
    base_domain =  parsed_base.netloc
    sub_base_domain = base_domain.split(".")[-2:]
    
    links = set() 
    
    for link in parsed_page.find_all('a',href=True) : 
        href = link.get('href')
        
        absolute = urljoin(base_url , href)
        
        parsed_absolute = urlparse(absolute)

        # skip if base domain not match
        if parsed_absolute.netloc != base_domain :
            continue

         
        # skip if path in excluded paths (e.g admin)   
        if any(path in parsed_absolute.path for path in EXCLUDED_PATHS):
            continue
        
        
        # skip if query in excluded query parameter (e.g q , filter)   
        if any(q in parsed_absolute.query for q in EXCLUDED_QUERY_PARAMS):
            continue
            
        
        if absolute.startswith("http"):
            links.add(absolute)
    return links
    



# ignore urls with videos , images , pdfs

def skip_url(url: str) -> bool:
    return url.lower().endswith(IGNORE_EXTENSIONS)


# Cache lives outside the function — persists across all calls
robots_cache: Dict[str, Optional[RobotFileParser]] = {}
robots_cache_lock = threading.Lock()

# rescpect robotfile
def is_allowed(url: str, user_agent: str = '*', fallback: bool = True) -> bool:
    parsed_url = urlparse(url)
    host = parsed_url.netloc
    robot_url = f"{parsed_url.scheme}://{host}/robots.txt"

    # Check cache first (with lock — multiple threads call this)
    with robots_cache_lock:
        if host not in robots_cache:
            rp = RobotFileParser()
            rp.set_url(robot_url)
            try:
                rp.read()                         # HTTP request — only ONCE per host
                logging.info(f"[robots.txt] Fetched and cached for: {host}")
            except Exception:
                logging.warning(f"[robots.txt] Not accessible for: {host}, using fallback={fallback}")
                rp = None

            robots_cache[host] = rp               # cache even if None (failed fetch)

        rp = robots_cache[host]

    if rp is None:
        return fallback                            # fetch failed → use fallback

    return rp.can_fetch(user_agent, url)
    
    
# crawl urls from seed

def crawel(seed_url: str, maxPage: int = 5) -> None:
    # setup queue for links and visited set to avoid duplicates
    url_queue: Deque[str] = deque([seed_url])
    visited: Set[str] = set()
    
    while url_queue and len(visited) < maxPage: 
        # fetch the page
        url= url_queue.popleft()
        
        # skip the link if visited or with ignored extention

        if url in visited or skip_url(url) or not is_allowed(url):
            continue
        
        # fetch the page
        logging.info(f"Crawling: {url}")
        page = fetch(url)
        if not page :
            continue
        
        # extract links 
        links = extract_links(page , url)
        
        # extend url_queue with unvisited links 
        url_queue.extend(links - visited)
        
        # mark url as visited
        visited.add(url)
        
    # Export visited URLs to CSV
    with open("crawled_urls.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["URL"])
        for url in visited:
            writer.writerow([url])
                
                
                
                
                
                
def threaded_crawel(seed_urls: Iterable[str], max_pages: int = 50, max_workers: int = 5, delay_range: Tuple[float, float] = (1.5, 3.5)) -> None:

    mapping_table: Dict[str, queue.Queue] = {}
    mapping_table_lock = threading.Lock()
    visited: Set[str] = set()
    visited_lock = threading.Lock()
    pages_crawled: int = 0
    pages_crawled_lock = threading.Lock()

    active_workers: int = 0                    # ← track how many workers are alive
    active_workers_lock = threading.Lock()
    all_done = threading.Event()          # ← signals when all workers finished

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        def ensure_worker(host: str, fifo_queue: queue.Queue) -> None:
            nonlocal active_workers
            if host not in futures:
                with active_workers_lock:
                    active_workers += 1   # increment BEFORE submitting
                futures[host] = executor.submit(worker, host, fifo_queue)
                logging.info(f"[Executor] New worker spawned for: {host}")

        def queue_router(url: str) -> Optional[queue.Queue]:
            host = urlparse(url).netloc
            if not host:
                return None
            with mapping_table_lock:
                if host not in mapping_table:
                    if len(mapping_table) >= max_workers:
                        target_host = list(mapping_table.keys())[hash(host) % max_workers]
                        return mapping_table[target_host]
                    mapping_table[host] = queue.Queue()
                    ensure_worker(host, mapping_table[host])
                    logging.info(f"[Queue Router] New queue → host: {host}")
                return mapping_table[host]

        def worker(host: str, fifo_queue: queue.Queue) -> None:
            nonlocal pages_crawled, active_workers
            try:
                while True:
                    try:
                        url = fifo_queue.get(timeout=5)
                    except queue.Empty:
                        break

                    with visited_lock:
                        if url in visited or skip_url(url) or not is_allowed(url):
                            fifo_queue.task_done()
                            continue
                        visited.add(url)

                    with pages_crawled_lock:
                        if pages_crawled >= max_pages:
                            fifo_queue.task_done()
                            break
                        pages_crawled += 1

                    logging.info(f"[{host}] Crawling ({pages_crawled}/{max_pages}): {url}")

                    html = fetch(url)
                    time.sleep(random.uniform(*delay_range))

                    if not html:
                        fifo_queue.task_done()
                        continue

                    save_page(html, url)

                    for link in extract_links(html, url):
                        with visited_lock:
                            already_seen = link in visited
                        if not already_seen:
                            q = queue_router(link)
                            if q:
                                q.put(link)

                    fifo_queue.task_done()

            finally:
                # ── Worker exiting — decrement counter ───────────────────────
                with active_workers_lock:
                    active_workers -= 1
                    logging.info(f"[Worker] {host} done. Active workers: {active_workers}")
                    if active_workers == 0:
                        all_done.set()    #  last worker signals completion

        # Seed
        for seed in seed_urls:
            q = queue_router(seed)
            if q:
                q.put(seed)

        # ── Block here until every worker (including late spawns) is done ────
        all_done.wait()
        logging.info(f"Crawl complete. Total pages crawled: {pages_crawled}")

    with open("crawled_urls.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["URL"])
        for url in visited:
            writer.writerow([url])

# save pages to disk
def save_page(html: str, url: str, folder: str = "pages") -> None:
    os.makedirs(folder, exist_ok=True)
    file_id = hashlib.md5(url.encode()).hexdigest()
    filepath = os.path.join(folder, f"{file_id}.html")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)

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
threaded_crawel(seed_urls,max_pages=100 , max_workers=10,delay_range=(0.1,0.2))