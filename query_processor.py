from db import Indeverted_index, Pages
from indexer import preprocess
from collections import defaultdict

def search(query_text: str):
    # 1. Preprocess the query (Tokenize, Stop-words, Stemming)
    # This turns "traveling in Japan" -> ["travel", "japan"]
    query_terms = preprocess(query_text)
    
    if not query_terms:
        return []

    # 2. Retrieve postings for each term
    # Results structure: { doc_id: total_score }
    results_score = defaultdict(float)

    for term in query_terms:
        # Find the term in  MongoDB Inverted Index
        entry = Indeverted_index.find_one({"term": term})
        
        if entry:
            for posting in entry["postings"]:
                doc_id = posting["doc_id"]
                # Use Term Frequency (tf) as a basic relevance score
                # Higher tf = more relevant
                results_score[doc_id] += posting["tf"]

    # 3. Sort documents by score (descending)
    sorted_docs = sorted(results_score.items(), key=lambda item: item[1], reverse=True)

    # 4. Fetch page details (Title/URL) for the final display
    final_results = []
    for doc_id, score in sorted_docs:
        page = Pages.find_one({"_id": doc_id})
        if page:
            final_results.append({
                "title": page.get("title"),
                "url": page.get("url"),
                "score": score
            })

    return final_results


if __name__ == "__main__":
    # Example: User searches for "traveling"
    search_query = "traveling"
    print(f"--- Results for: {search_query} ---")
    
    results = search(search_query)
    
    for i, res in enumerate(results[:5]): # Show top 5
        print(f"{i+1}. {res['title']} ({res['url']}) - Score: {res['score']}")