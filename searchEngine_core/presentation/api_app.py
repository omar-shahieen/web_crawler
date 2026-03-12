from __future__ import annotations

from typing import Callable, List, Tuple

from flask import Flask, jsonify, request

from domain.query_language import count_boolean_operators, extract_query_terms
from services.search_service import extract_quoted_phrase, make_snippet, parse_query_with_operators, phrase_search, search_query, search_with_operators


DEFAULT_TOP_K = 10
MAX_TOP_K = 50


def _resolve_search_strategy(query_text: str) -> Callable[[str, int], List[Tuple[dict, float]]]:
    if parse_query_with_operators(query_text):
        return search_with_operators

    quoted_phrase = extract_quoted_phrase(query_text)
    if quoted_phrase:
        return lambda _query_text, top_k: phrase_search(quoted_phrase, top_k=top_k)

    return search_query


def _serialize_results(results: List[Tuple[dict, float]], query_text: str) -> List[dict]:
    query_terms = extract_query_terms(query_text)
    serialized: List[dict] = []
    for page, score in results:
        content = page.get("content", "")
        serialized.append(
            {
                "id": str(page.get("_id", "")),
                "title": page.get("title", "No Title"),
                "url": page.get("url", ""),
                "description": make_snippet(content, query_terms),
                "score": round(float(score), 6),
            }
        )
    return serialized


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/api/health")
    def health() -> tuple[dict, int]:
        return {"status": "ok"}, 200

    @app.get("/api/search")
    def search() -> tuple:
        query_text = request.args.get("q", "", type=str).strip()
        if not query_text:
            return jsonify({"error": "Missing required query parameter 'q'."}), 400

        if count_boolean_operators(query_text) > 2:
            return jsonify({"error": "Maximum number of boolean operators per search is 2."}), 400

        top_k = request.args.get("top", DEFAULT_TOP_K, type=int)
        top_k = max(1, min(top_k, MAX_TOP_K))

        search_fn = _resolve_search_strategy(query_text)
        results = search_fn(query_text, top_k=top_k)
        return jsonify(_serialize_results(results, query_text)), 200

    @app.errorhandler(Exception)
    def handle_unexpected_error(error: Exception):
        return jsonify({"error": str(error)}), 500

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3001, debug=False)