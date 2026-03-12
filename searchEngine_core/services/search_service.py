import re
from typing import Dict, List, Optional, Set, Tuple

from bson import ObjectId
from nltk.stem import PorterStemmer

from domain.fuzzy_matching import find_fuzzy_matches, max_fuzzy_distance
from domain.query_language import BOOLEAN_OPERATOR_PRECEDENCE, count_boolean_operators, extract_quoted_phrase, parse_query_with_operators, to_postfix
from domain.ranking import rank_documents
from services.crawl_core.indexer import _preprocess as preprocess
from infrastructure.database import Pages, Indeverted_index


porter_stemmer = PorterStemmer()

_SEARCH_CACHE: Dict[str, List[Tuple[dict, float]]] = {}
_TERM_POSTINGS_CACHE: Dict[str, List[dict]] = {}
_FUZZY_MATCH_CACHE: Dict[str, List[Tuple[str, float]]] = {}
_TERM_VOCABULARY_CACHE: List[str] = []
_TERM_VOCABULARY_BY_INITIAL: Dict[str, List[str]] = {}
_TERM_VOCABULARY_COUNT = -1

_CACHE_STATS = {
    "hits": 0,
    "misses": 0,
    "evictions": 0,
}

MAX_CACHE_SIZE = 500
MAX_FUZZY_EXPANSIONS = 3
MIN_FUZZY_TERM_LENGTH = 3


def clear_cache() -> None:
    _SEARCH_CACHE.clear()
    _TERM_POSTINGS_CACHE.clear()
    _FUZZY_MATCH_CACHE.clear()
    _TERM_VOCABULARY_CACHE.clear()
    _TERM_VOCABULARY_BY_INITIAL.clear()
    global _CACHE_STATS, _TERM_VOCABULARY_COUNT
    _CACHE_STATS = {"hits": 0, "misses": 0, "evictions": 0}
    _TERM_VOCABULARY_COUNT = -1
    print("✓ All caches cleared")


def get_cache_stats() -> Dict:
    total = _CACHE_STATS["hits"] + _CACHE_STATS["misses"]
    hit_rate = (_CACHE_STATS["hits"] / total * 100) if total > 0 else 0
    return {
        "hits": _CACHE_STATS["hits"],
        "misses": _CACHE_STATS["misses"],
        "evictions": _CACHE_STATS["evictions"],
        "hit_rate": f"{hit_rate:.1f}%",
        "search_cache_size": len(_SEARCH_CACHE),
        "postings_cache_size": len(_TERM_POSTINGS_CACHE),
        "fuzzy_cache_size": len(_FUZZY_MATCH_CACHE),
        "vocabulary_cache_size": len(_TERM_VOCABULARY_CACHE),
    }


def _get_search_result_from_cache(query: str) -> Optional[List[Tuple[dict, float]]]:
    if query in _SEARCH_CACHE:
        _CACHE_STATS["hits"] += 1
        return _SEARCH_CACHE[query]

    _CACHE_STATS["misses"] += 1
    return None


def _cache_search_result(query: str, results: List[Tuple[dict, float]]) -> None:
    if len(_SEARCH_CACHE) >= MAX_CACHE_SIZE:
        oldest_key = next(iter(_SEARCH_CACHE))
        del _SEARCH_CACHE[oldest_key]
        _CACHE_STATS["evictions"] += 1

    _SEARCH_CACHE[query] = results


def _get_term_postings_from_cache(term: str) -> Optional[List[dict]]:
    return _TERM_POSTINGS_CACHE.get(term)


def _cache_term_postings(term: str, postings: List[dict]) -> None:
    if len(_TERM_POSTINGS_CACHE) >= MAX_CACHE_SIZE:
        oldest_key = next(iter(_TERM_POSTINGS_CACHE))
        del _TERM_POSTINGS_CACHE[oldest_key]
        _CACHE_STATS["evictions"] += 1

    _TERM_POSTINGS_CACHE[term] = postings


def _cache_fuzzy_matches(term: str, matches: List[Tuple[str, float]]) -> None:
    if len(_FUZZY_MATCH_CACHE) >= MAX_CACHE_SIZE:
        oldest_key = next(iter(_FUZZY_MATCH_CACHE))
        del _FUZZY_MATCH_CACHE[oldest_key]
        _CACHE_STATS["evictions"] += 1

    _FUZZY_MATCH_CACHE[term] = matches


def _load_term_vocabulary() -> Tuple[List[str], Dict[str, List[str]]]:
    global _TERM_VOCABULARY_CACHE, _TERM_VOCABULARY_BY_INITIAL, _TERM_VOCABULARY_COUNT

    term_count = Indeverted_index.count_documents({})
    if term_count == _TERM_VOCABULARY_COUNT and _TERM_VOCABULARY_CACHE:
        return _TERM_VOCABULARY_CACHE, _TERM_VOCABULARY_BY_INITIAL

    vocabulary: List[str] = []
    buckets: Dict[str, List[str]] = {}

    for row in Indeverted_index.find({}, {"term": 1}):
        term = row.get("term")
        if not isinstance(term, str):
            continue

        normalized_term = term.strip().lower()
        if not normalized_term:
            continue

        vocabulary.append(normalized_term)
        buckets.setdefault(normalized_term[0], []).append(normalized_term)

    _TERM_VOCABULARY_CACHE = vocabulary
    _TERM_VOCABULARY_BY_INITIAL = buckets
    _TERM_VOCABULARY_COUNT = term_count
    _FUZZY_MATCH_CACHE.clear()
    return vocabulary, buckets


def get_fuzzy_term_matches(term: str) -> List[Tuple[str, float]]:
    normalized_term = term.strip().lower()
    if len(normalized_term) < MIN_FUZZY_TERM_LENGTH:
        return []

    if normalized_term in _FUZZY_MATCH_CACHE:
        return _FUZZY_MATCH_CACHE[normalized_term]

    vocabulary, buckets = _load_term_vocabulary()
    max_distance = max_fuzzy_distance(normalized_term)

    matches = find_fuzzy_matches(
        normalized_term,
        buckets.get(normalized_term[0], []),
        max_distance=max_distance,
        max_expansions=MAX_FUZZY_EXPANSIONS,
        min_term_length=MIN_FUZZY_TERM_LENGTH,
    )

    if not matches and vocabulary:
        matches = find_fuzzy_matches(
            normalized_term,
            vocabulary,
            max_distance=max_distance,
            max_expansions=MAX_FUZZY_EXPANSIONS,
            min_term_length=MIN_FUZZY_TERM_LENGTH,
        )

    _cache_fuzzy_matches(normalized_term, matches)
    return matches


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
        snippet = text[start:start + max_len]

    snippet = re.sub(r"\s+", " ", snippet).strip()
    return snippet + ("..." if len(text) > len(snippet) else "")


def get_term_postings(term: str) -> List[dict]:
    cached_postings = _get_term_postings_from_cache(term)
    if cached_postings is not None:
        return cached_postings

    row = Indeverted_index.find_one({"term": term}, {"postings": 1})
    postings = [] if not row else row.get("postings", [])
    _cache_term_postings(term, postings)
    return postings


def build_weighted_query_terms(query: str) -> Dict[str, float]:
    weighted_terms: Dict[str, float] = {}
    normalized_terms = preprocess(query)

    for term in normalized_terms:
        weighted_terms[term] = max(weighted_terms.get(term, 0.0), 1.0)

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
            if term not in weighted_terms and porter_stemmer.stem(term) == stem:
                weighted_terms[term] = 0.6

        if get_term_postings(stem):
            continue

        for fuzzy_term, fuzzy_weight in get_fuzzy_term_matches(stem):
            weighted_terms[fuzzy_term] = max(weighted_terms.get(fuzzy_term, 0.0), fuzzy_weight)

    return weighted_terms


def search_query(query: str, top_k: int = 10) -> List[Tuple[dict, float]]:
    cache_key = f"{query}::{top_k}"
    cached_results = _get_search_result_from_cache(cache_key)
    if cached_results is not None:
        return cached_results

    weighted_terms = build_weighted_query_terms(query)
    if not weighted_terms:
        results = []
    else:
        candidate_docs: Set[ObjectId] = set()
        for term in weighted_terms:
            for posting in get_term_postings(term):
                doc_id = posting.get("doc_id")
                if isinstance(doc_id, ObjectId):
                    candidate_docs.add(doc_id)

        if not candidate_docs:
            results = []
        else:
            scores = rank_documents(weighted_terms, candidate_docs)
            ranked_ids = sorted(scores.keys(), key=lambda doc_id: scores[doc_id], reverse=True)[:top_k]

            pages = {
                page["_id"]: page
                for page in Pages.find(
                    {"_id": {"$in": ranked_ids}},
                    {"title": 1, "url": 1, "content": 1},
                )
            }

            results: List[Tuple[dict, float]] = []
            for doc_id in ranked_ids:
                page = pages.get(doc_id)
                if page:
                    results.append((page, scores[doc_id]))

    _cache_search_result(cache_key, results)
    return results


def normalize_text_for_phrase_match(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def phrase_search(phrase: str, top_k: int = 10) -> List[Tuple[dict, float]]:
    cache_key = f"phrase::{phrase}::{top_k}"
    cached_results = _get_search_result_from_cache(cache_key)
    if cached_results is not None:
        return cached_results

    terms = preprocess(phrase)
    normalized_phrase = normalize_text_for_phrase_match(phrase)
    if not normalized_phrase:
        return []

    if not terms:
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
        for posting in get_term_postings(term):
            posting_map[posting["doc_id"]] = set(posting.get("pos", []))
        postings_by_term[term] = posting_map

    doc_sets = [set(postings_by_term[term].keys()) for term in terms if postings_by_term[term]]
    if not doc_sets:
        return []

    candidate_docs = set.intersection(*doc_sets)
    matched_docs: Set[ObjectId] = set()

    for doc_id in candidate_docs:
        first_positions = postings_by_term[terms[0]][doc_id]
        other_positions = [postings_by_term[term][doc_id] for term in terms[1:]]

        for start_pos in first_positions:
            if all((start_pos + index + 1) in other_positions[index] for index in range(len(other_positions))):
                matched_docs.add(doc_id)
                break

    if not matched_docs:
        return []

    pages_map = {
        page["_id"]: page
        for page in Pages.find(
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

    normal_ranked = search_query(phrase, top_k=max(top_k * 20, 200))
    results: List[Tuple[dict, float]] = []
    for page, score in normal_ranked:
        doc_id = page.get("_id")
        if doc_id in exact_phrase_docs:
            results.append((page, score))
            if len(results) >= top_k:
                break

    if len(results) < top_k:
        missing_docs = exact_phrase_docs - {result[0].get("_id") for result in results}
        if missing_docs:
            weighted_terms = {term: 1.0 for term in terms}
            extra_scores = rank_documents(weighted_terms, missing_docs)
            extra_ids = sorted(missing_docs, key=lambda doc_id: extra_scores.get(doc_id, 0.0), reverse=True)
            for doc_id in extra_ids:
                page = pages_map.get(doc_id)
                if not page:
                    continue
                results.append((page, extra_scores.get(doc_id, 0.0)))
                if len(results) >= top_k:
                    break

    _cache_search_result(cache_key, results)
    return results


def _evaluate_boolean_tokens(tokens: List[str]) -> Set[ObjectId]:
    postfix = to_postfix(tokens)
    stack: List[Set[ObjectId]] = []

    for token in postfix:
        if token not in BOOLEAN_OPERATOR_PRECEDENCE:
            stack.append(get_documents_for_query(token))
            continue

        if len(stack) < 2:
            return set()

        right_docs = stack.pop()
        left_docs = stack.pop()

        if token == "OR":
            stack.append(left_docs | right_docs)
        elif token == "AND":
            stack.append(left_docs & right_docs)
        else:
            stack.append(left_docs - right_docs)

    if len(stack) != 1:
        return set()

    return stack[0]


def get_documents_for_query(query_text: str) -> Set[ObjectId]:
    if not query_text:
        return set()

    if query_text.startswith('"') and query_text.endswith('"'):
        phrase = query_text[1:-1].strip()
        results = phrase_search(phrase, top_k=1000)
    else:
        results = search_query(query_text, top_k=1000)

    return {page.get("_id") for page, _ in results if page.get("_id")}


def search_with_operators(query: str, top_k: int = 10) -> List[Tuple[dict, float]]:
    if count_boolean_operators(query) > 2:
        raise ValueError("Maximum number of boolean operators per search is 2.")

    cache_key = f"operators::{query}::{top_k}"
    cached_results = _get_search_result_from_cache(cache_key)
    if cached_results is not None:
        return cached_results

    parsed = parse_query_with_operators(query)

    if not parsed:
        if extract_quoted_phrase(query):
            results = phrase_search(extract_quoted_phrase(query), top_k=top_k)
        else:
            results = search_query(query, top_k=top_k)
    else:
        result_docs = _evaluate_boolean_tokens(parsed["tokens"])

        if not result_docs:
            results = []
        else:
            cleaned_query = re.sub(r"\s+(?:AND|OR|NOT)\s+", " ", query, flags=re.IGNORECASE)
            weighted_terms = build_weighted_query_terms(cleaned_query)
            scores = rank_documents(weighted_terms, result_docs) if weighted_terms else {doc_id: 1.0 for doc_id in result_docs}

            ranked_ids = sorted(scores.keys(), key=lambda doc_id: scores[doc_id], reverse=True)[:top_k]

            pages = {
                page["_id"]: page
                for page in Pages.find(
                    {"_id": {"$in": ranked_ids}},
                    {"title": 1, "url": 1, "content": 1},
                )
            }

            results: List[Tuple[dict, float]] = []
            for doc_id in ranked_ids:
                page = pages.get(doc_id)
                if page:
                    results.append((page, scores[doc_id]))

    _cache_search_result(cache_key, results)
    return results
