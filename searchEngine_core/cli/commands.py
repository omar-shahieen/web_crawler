import argparse

from infrastructure.logging_utils import setup_logging
from presentation.api_app import create_app
from services.crawl_service import SEED_URLS, crawl_and_index, crawl_web, index_content


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search engine operations")
    subparsers = parser.add_subparsers(dest="command")

    crawl_parser = subparsers.add_parser("crawl", help="Run the crawler only")
    crawl_parser.add_argument("--max-pages", type=int, default=100)
    crawl_parser.add_argument("--max-workers", type=int, default=10)
    crawl_parser.add_argument("--delay-min", type=float, default=1.5)
    crawl_parser.add_argument("--delay-max", type=float, default=3.0)

    index_parser = subparsers.add_parser("index", help="Run indexing only")
    index_parser.add_argument("--full", action="store_true", help="Reserved for future full reindex flows")

    crawl_index_parser = subparsers.add_parser("crawl-index", help="Run crawler then indexer")
    crawl_index_parser.add_argument("--max-pages", type=int, default=100)
    crawl_index_parser.add_argument("--max-workers", type=int, default=10)
    crawl_index_parser.add_argument("--delay-min", type=float, default=1.5)
    crawl_index_parser.add_argument("--delay-max", type=float, default=3.0)

    serve_parser = subparsers.add_parser("serve", help="Run the Flask API")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=3001)
    serve_parser.add_argument("--debug", action="store_true")

    return parser


def main() -> None:
    setup_logging()
    parser = _build_parser()
    args = parser.parse_args()
    command = args.command or "index"

    if command == "crawl":
        crawl_web(
            max_pages=args.max_pages,
            max_workers=args.max_workers,
            delay_range=(args.delay_min, args.delay_max),
        )
        return

    if command == "crawl-index":
        crawl_and_index(
            max_pages=args.max_pages,
            max_workers=args.max_workers,
            delay_range=(args.delay_min, args.delay_max),
        )
        return

    if command == "serve":
        create_app().run(host=args.host, port=args.port, debug=args.debug)
        return

    index_content()