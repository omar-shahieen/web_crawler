# Webcrawler

Brief lightweight web crawler for single-host and multi-host threaded crawling.

**Requirements:**

- Python 3.8 or newer
- pip
- External Python packages: `requests`, `beautifulsoup4`

Install dependencies (recommended inside a virtual environment):

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# then
pip install requests beautifulsoup4
```

Alternatively install from pip directly:

```bash
pip install requests beautifulsoup4
```

**How to run**

Run the crawler script from the project root:

```bash
python crawler.py
```

This will start the default threaded crawl (see bottom of `crawler.py`).

Outputs produced:

- `crawled_urls.csv` — CSV file with one column `URL` containing visited URLs.
- `pages/` — directory containing fetched HTML pages (MD5 of URL as filename).
- `crawler.log` — logging output for crawl events and warnings.

**Brief explanation**

- `fetch(url)`: downloads a URL and returns the HTML (uses rotating user-agents).
- `extract_links(html, base_url)`: parses anchor tags and returns same-host absolute links, applying basic filters.
- `save_page(html, url)`: writes the HTML to `pages/<md5>.html`.
- `threaded_crawel(seed_urls, ...)`: multi-host threaded crawler that routes URLs to per-host worker queues, respects robots.txt (cached), avoids duplicates, and writes `crawled_urls.csv` at the end.

**Notes & tips**

- The crawler respects `robots.txt` by default; if robots fetching fails it falls back to allowing access.
- Adjust `max_pages`, `max_workers`, and `delay_range` parameters near the bottom of `crawler.py` for different crawl sizes and politeness.
- Use a short `delay_range` for quick local testing; increase delays for real-world crawls to avoid overloading servers.

If you want, I can also add a `requirements.txt` file or update the script to accept CLI args for seeds and options.
