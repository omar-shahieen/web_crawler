import math
from typing import Dict, List, Set

from bson import ObjectId

from infrastructure.database import Pages, Indeverted_index


_PAGERANK_CACHE: Dict[ObjectId, float] = {}
_PAGERANK_CACHE_DOC_COUNT: int = -1


def get_term_postings(term: str) -> List[dict]:
    row = Indeverted_index.find_one({"term": term}, {"postings": 1})
    if not row:
        return []
    return row.get("postings", [])


def compute_relevance_scores(weighted_terms: Dict[str, float], candidate_docs: Set[ObjectId]) -> Dict[ObjectId, float]:
    total_docs = max(Pages.count_documents({}), 1)
    scores: Dict[ObjectId, float] = {doc_id: 0.0 for doc_id in candidate_docs}

    for term, term_weight in weighted_terms.items():
        postings = get_term_postings(term)
        if not postings:
            continue

        df = max(len(postings), 1)
        idf = math.log(total_docs / df)

        for posting in postings:
            doc_id = posting.get("doc_id")
            if not isinstance(doc_id, ObjectId):
                continue
            if doc_id not in scores:
                continue

            tf_raw = posting.get("tf", 0)
            if not isinstance(tf_raw, (int, float)):
                continue
            page = Pages.find_one({"_id": doc_id}, {"word_count": 1})
            doc_len = max((page or {}).get("word_count", 1), 1)
            tf = tf_raw / doc_len
            scores[doc_id] += tf * idf * term_weight

    return scores


def _build_link_graph() -> tuple[Dict[ObjectId, Set[ObjectId]], Dict[ObjectId, float]]:
    pages = list(Pages.find({}, {"_id": 1, "url": 1, "out_links": 1}))
    if not pages:
        return {}, {}

    url_to_id: Dict[str, ObjectId] = {page["url"]: page["_id"] for page in pages if page.get("url")}
    adjacency: Dict[ObjectId, Set[ObjectId]] = {}

    for page in pages:
        source_id = page["_id"]
        links = page.get("out_links", [])
        targets: Set[ObjectId] = set()

        for link in links:
            normalized_link = ""
            if isinstance(link, str):
                normalized_link = link
            elif isinstance(link, (list, tuple)) and link:
                normalized_link = str(link[0])
            elif isinstance(link, dict):
                normalized_link = str(link.get("url", ""))

            if not normalized_link:
                continue

            target_id = url_to_id.get(normalized_link)
            if target_id and target_id != source_id:
                targets.add(target_id)

        adjacency[source_id] = targets

    size = len(adjacency)
    base_rank = 1.0 / size
    ranks: Dict[ObjectId, float] = {doc_id: base_rank for doc_id in adjacency}
    return adjacency, ranks


def _compute_pagerank_scores(iterations: int = 20, damping: float = 0.85) -> Dict[ObjectId, float]:
    adjacency, ranks = _build_link_graph()
    if not adjacency:
        return {}

    doc_ids = list(adjacency.keys())
    total_docs = len(doc_ids)
    base_jump = (1.0 - damping) / total_docs

    for _ in range(iterations):
        new_ranks: Dict[ObjectId, float] = {doc_id: base_jump for doc_id in doc_ids}
        sink_mass = 0.0

        for source_id, outbound_links in adjacency.items():
            source_rank = ranks[source_id]
            if not outbound_links:
                sink_mass += source_rank
                continue

            share = damping * source_rank / len(outbound_links)
            for destination_id in outbound_links:
                new_ranks[destination_id] += share

        sink_share = damping * sink_mass / total_docs
        for doc_id in doc_ids:
            new_ranks[doc_id] += sink_share

        ranks = new_ranks

    max_rank = max(ranks.values()) if ranks else 1.0
    if max_rank <= 0:
        return {doc_id: 0.0 for doc_id in ranks}

    return {doc_id: score / max_rank for doc_id, score in ranks.items()}


def compute_popularity_scores() -> Dict[ObjectId, float]:
    global _PAGERANK_CACHE
    global _PAGERANK_CACHE_DOC_COUNT

    doc_count = Pages.count_documents({})
    if doc_count == _PAGERANK_CACHE_DOC_COUNT and _PAGERANK_CACHE:
        return _PAGERANK_CACHE

    _PAGERANK_CACHE = _compute_pagerank_scores()
    _PAGERANK_CACHE_DOC_COUNT = doc_count
    return _PAGERANK_CACHE


def combine_scores(
    relevance_scores: Dict[ObjectId, float],
    popularity_scores: Dict[ObjectId, float],
    relevance_weight: float = 0.8,
    popularity_weight: float = 0.2,
) -> Dict[ObjectId, float]:
    if not relevance_scores:
        return {}

    max_rel = max(relevance_scores.values()) if relevance_scores else 1.0
    if max_rel <= 0:
        max_rel = 1.0

    final_scores: Dict[ObjectId, float] = {}
    for doc_id, relevance_score in relevance_scores.items():
        rel_norm = relevance_score / max_rel
        pop = popularity_scores.get(doc_id, 0.0)
        final_scores[doc_id] = (relevance_weight * rel_norm) + (popularity_weight * pop)

    return final_scores


def rank_documents(
    weighted_terms: Dict[str, float],
    candidate_docs: Set[ObjectId],
    relevance_weight: float = 0.8,
    popularity_weight: float = 0.2,
) -> Dict[ObjectId, float]:
    relevance_scores = compute_relevance_scores(weighted_terms, candidate_docs)
    popularity_scores = compute_popularity_scores()
    return combine_scores(
        relevance_scores,
        popularity_scores,
        relevance_weight=relevance_weight,
        popularity_weight=popularity_weight,
    )