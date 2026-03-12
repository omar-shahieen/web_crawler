import threading

from domain.auto_complete import Trie , build_autocomplete_trie
from infrastructure.database import QueryLog , Page

class TrieManager:
    def __init__(self):
        self._trie = None
        self._lock = threading.RLock()   # readers can overlap, rebuild swaps atomically

    def get(self) -> Trie:
        return self._trie

    def rebuild(self ):
        pages   = Page.load_pages()
        queries = QueryLog.load_queries()

        new_trie = build_autocomplete_trie(pages, queries)
        new_trie.prune(min_score=10)     # shed noise before swapping in

        report = new_trie.memory_report()
        print(f"Trie memory: {report['estimated_mb']} MB, nodes: {report['node_count']}")

        if report["estimated_mb"] > 512:
            # Escalate pruning aggressively before swapping in
            new_trie.prune(min_score=50)
            print(f"Emergency prune. Now: {new_trie.memory_report()['estimated_mb']} MB")

        with self._lock:
            self._trie = new_trie        # atomic swap — old trie GC'd immediately
        print(f"Trie rebuilt: {new_trie._node_count} nodes")


trie_manager =  TrieManager()
