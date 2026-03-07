import argparse
import re
from typing import Dict, List, Optional, Set, Tuple

from bson import ObjectId
from nltk.stem import PorterStemmer

from db import Pages, Indeverted_index
from indexer import preprocess
from ranker import rank_documents


porter_stemmer = PorterStemmer()


def make_snippet(text: str, query_terms: List[str], max_len: int = 180) -> str:
    if not text:
        return ""

    lower = text.lower()
    hit_pos = -1
    for term in query_terms:
        pos = lower.find(term.lower())
        if pos != -1 and (hit_pos == -1 or pos < hit_pos):
            hit_pos = pos

    if hit_pos == -1:
        snippet = text[:max_len]
    else:
        start = max(0, hit_pos - 50)
        snippet = text[start : start + max_len]

    snippet = re.sub(r"\s+", " ", snippet).strip()
    return snippet + ("..." if len(text) > len(snippet) else "")


def get_term_postings(term: str) -> List[dict]:
    row = Indeverted_index.find_one({"term": term}, {"postings": 1})
    if not row:
        return []
    return row.get("postings", [])


def build_weighted_query_terms(query: str) -> Dict[str, float]:
    """
    Build weighted query terms.
    Exact normalized query terms get full weight,
    while stem-related variants get a lower weight.
    """
    weighted_terms: Dict[str, float] = {}
    normalized_terms = preprocess(query)

    for term in normalized_terms:
        weighted_terms[term] = max(weighted_terms.get(term, 0.0), 1.0)

    # Expand using terms that share the same stem.
    raw_tokens = re.findall(r"[a-zA-Z]+", query.lower())
    for raw in raw_tokens:
        stem = porter_stemmer.stem(raw)
        related_rows = Indeverted_index.find(
            {"term": {"$regex": f"^{re.escape(stem)}"}},
            {"term": 1},
        ).limit(25)

        for row in related_rows:
            term = row.get("term")
            if not term:
                continue

            # Same-stem terms get a lower score than exact query terms.
            if term not in weighted_terms and porter_stemmer.stem(term) == stem:
                weighted_terms[term] = 0.6

    return weighted_terms


def search_query(query: str, top_k: int = 10) -> List[Tuple[dict, float]]:
    weighted_terms = build_weighted_query_terms(query)
    if not weighted_terms:
        return []

    candidate_docs: Set[ObjectId] = set()
    for term in weighted_terms:
        for posting in get_term_postings(term):
            candidate_docs.add(posting.get("doc_id"))

    if not candidate_docs:
        return []

    scores = rank_documents(weighted_terms, candidate_docs)
    ranked_ids = sorted(scores.keys(), key=lambda d: scores[d], reverse=True)[:top_k]

    pages = {
        p["_id"]: p
        for p in Pages.find(
            {"_id": {"$in": ranked_ids}},
            {"title": 1, "url": 1, "content": 1},
        )
    }

    results: List[Tuple[dict, float]] = []
    for doc_id in ranked_ids:
        page = pages.get(doc_id)
        if page:
            results.append((page, scores[doc_id]))
    return results


def normalize_text_for_phrase_match(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def phrase_search(phrase: str, top_k: int = 10) -> List[Tuple[dict, float]]:
    terms = preprocess(phrase)
    normalized_phrase = normalize_text_for_phrase_match(phrase)
    if not normalized_phrase:
        return []

    if not terms:
        # Fallback: if phrase tokens vanish after preprocessing (e.g., stop words),
        # scan pages directly for exact phrase order.
        phrase_hits = list(
            Pages.find(
                {
                    "$or": [
                        {"content": {"$regex": re.escape(phrase), "$options": "i"}},
                        {"title": {"$regex": re.escape(phrase), "$options": "i"}},
                    ]
                },
                {"title": 1, "url": 1, "content": 1},
            ).limit(top_k)
        )
        return [(page, 1.0) for page in phrase_hits]

    postings_by_term: Dict[str, Dict[ObjectId, Set[int]]] = {}
    for term in terms:
        posting_map: Dict[ObjectId, Set[int]] = {}
        for p in get_term_postings(term):
            posting_map[p["doc_id"]] = set(p.get("pos", []))
        postings_by_term[term] = posting_map

    doc_sets = [set(postings_by_term[t].keys()) for t in terms if postings_by_term[t]]
    if not doc_sets:
        return []

    candidate_docs = set.intersection(*doc_sets)
    matched_docs: Set[ObjectId] = set()

    for doc_id in candidate_docs:
        first_positions = postings_by_term[terms[0]][doc_id]
        other_positions = [postings_by_term[t][doc_id] for t in terms[1:]]

        for start_pos in first_positions:
            if all((start_pos + idx + 1) in other_positions[idx] for idx in range(len(other_positions))):
                matched_docs.add(doc_id)
                break

    if not matched_docs:
        return []

    # Keep phrase results a strict subset of normal search results,
    # then enforce exact word order at text level.
    pages_map = {
        p["_id"]: p
        for p in Pages.find(
            {"_id": {"$in": list(matched_docs)}},
            {"title": 1, "url": 1, "content": 1},
        )
    }

    exact_phrase_docs: Set[ObjectId] = set()
    for doc_id, page in pages_map.items():
        full_text = f"{page.get('title', '')} {page.get('content', '')}"
        if normalized_phrase in normalize_text_for_phrase_match(full_text):
            exact_phrase_docs.add(doc_id)

    if not exact_phrase_docs:
        return []

    # Enforce phrase results as a subset of normal query results.
    # We take a larger normal candidate window and then filter phrase-matching docs from it.
    normal_ranked = search_query(phrase, top_k=max(top_k * 20, 200))
    results: List[Tuple[dict, float]] = []
    for page, score in normal_ranked:
        doc_id = page.get("_id")
        if doc_id in exact_phrase_docs:
            results.append((page, score))
            if len(results) >= top_k:
                break

    # Fallback for very rare cases where phrase hits exist outside the sampled normal window.
    if len(results) < top_k:
        missing_docs = exact_phrase_docs - {r[0].get("_id") for r in results}
        if missing_docs:
            weighted_terms = {term: 1.0 for term in terms}
            extra_scores = rank_documents(weighted_terms, missing_docs)
            extra_ids = sorted(missing_docs, key=lambda d: extra_scores.get(d, 0.0), reverse=True)
            for doc_id in extra_ids:
                page = pages_map.get(doc_id)
                if not page:
                    continue
                results.append((page, extra_scores.get(doc_id, 0.0)))
                if len(results) >= top_k:
                    break

    return results


def extract_quoted_phrase(query: str) -> Optional[str]:
    match = re.search(r'"([^"]+)"', query)
    if not match:
        return None
    return match.group(1).strip()


def print_results(results: List[Tuple[dict, float]], query_text: str) -> None:
    query_terms = preprocess(query_text)

    if not results:
        print("No results found.")
        return

    for idx, (page, score) in enumerate(results, start=1):
        title = page.get("title", "No Title")
        url = page.get("url", "")
        content = page.get("content", "")
        snippet = make_snippet(content, query_terms)

        print(f"{idx}. {title}")
        print(f"   URL: {url}")
        print(f"   Score: {score:.6f}")
        print(f"   Snippet: {snippet}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple search over MongoDB index")
    parser.add_argument("query", help="Search query text")
    parser.add_argument("--phrase", action="store_true", help="Enable exact phrase search")
    parser.add_argument("--top", type=int, default=10, help="Number of results to show")
    args = parser.parse_args()

    quoted_phrase = extract_quoted_phrase(args.query)
    if args.phrase or quoted_phrase:
        phrase_text = quoted_phrase if quoted_phrase else args.query
        results = phrase_search(phrase_text, top_k=args.top)
    else:
        results = search_query(args.query, top_k=args.top)

    print_results(results, args.query)


if __name__ == "__main__":
    main()
