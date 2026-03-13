import pickle
import re
from collections import Counter
from typing import Tuple


WEIGHT = {
    "query":   40,
    "title":   20,
    "body":     1,
}



class TrieNode:
    def __init__(self):
        self.children: dict[str, TrieNode] = {}
        self.is_end_of_word: bool = False
        self.score: int = 0         # cumulative priority score



class Trie:
    def __init__(self):
        self.root = TrieNode()
        self._node_count = 0 

    def insert(self, term: str, weight: int = 1) -> None:
        """Insert a term and add weight to its score."""
        node = self.root
        for char in term:
            if char not in node.children:
                node.children[char] = TrieNode()
                self._node_count += 1
            node = node.children[char]
        node.is_end_of_word = True
        node.score += weight


    def search(self, word: str) -> bool:
        """Return True if the word exists in the trie."""
        node = self._find_node(word)
        return node is not None and node.is_end_of_word

    def starts_with(self, prefix: str) -> bool:
        """Return True if any word in the trie starts with the given prefix."""
        return self._find_node(prefix) is not None

    def delete(self, word: str) -> bool:
        """Delete a word from the trie. Returns True if the word was found and deleted."""
        def _delete(node: TrieNode, word: str, depth: int) -> bool:
            if depth == len(word):
                if not node.is_end_of_word:
                    return False  # Word not found
                node.is_end_of_word = False
                return len(node.children) == 0  # Node can be deleted if it has no children

            char = word[depth]
            if char not in node.children:
                return False  # Word not found

            should_delete_child = _delete(node.children[char], word, depth + 1)
            if should_delete_child:
                del node.children[char]
                return len(node.children) == 0 and not node.is_end_of_word

            return False

        return _delete(self.root, word, 0)
    
    
    def autocomplete(self, prefix: str, limit: int = 10) -> list[str]:
        """Return up to `limit` suggestions sorted by priority score."""
        node = self._find_node(prefix)
        if not node:
            return []
        results: list[tuple[str, int]] = []
        self._dfs(node, prefix, results)
        results.sort(key=lambda x: x[1], reverse=True)
        return [term for term, _ in results[:limit]]
    
    def suggest(self, query: str, limit: int = 10) -> list[str]:
        """
        Handle multi-word queries: complete only the last word
        and restore the earlier words as context.
        """
        words = query.strip().lower().split()
        if not words:
            return []
        prefix  = words[-1]
        context = " ".join(words[:-1])
        completions = self.autocomplete(prefix, limit)
        if context:
            return [f"{context} {c}" for c in completions]
        return completions

    def prune(self, min_score: int) -> int:
        """
        Remove all terms with score below min_score.
        Useful when memory grows too large.
        Returns number of terms removed.
        """
        removed = [0]
 
        def _prune(node: TrieNode) -> bool:
            # Recurse into children, collect dead ones
            dead = [c for c, child in node.children.items() if _prune(child)]
            for c in dead:
                del node.children[c]
                self._node_count -= 1
            # Kill this node if it's a low-score leaf
            if node.is_end_of_word and node.score < min_score:
                node.is_end_of_word = False
                removed[0] += 1
            # Signal parent to delete us if we're now empty
            return not node.is_end_of_word and len(node.children) == 0
 
        _prune(self.root)
        return removed[0]
    
        # ── Private helpers ──────────────────────────────────────────────────────

    def _find_node(self, prefix: str) -> TrieNode | None:
        """Traverse to the node representing the end of the prefix."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]
        return node

    def _dfs(self, node: TrieNode, current: str, results: list[str]) -> None:
        """Depth-first search to collect all words from a given node."""
        if node.is_end_of_word:
            results.append((current, node.score))   # fix 2
        for char, child in node.children.items():
            self._dfs(child, current + char, results)
            
            
        
    # ── Persistence ──────────────────────────────────────────────────────────

    def save(self, path: str = "trie.pkl") -> None:
        with open(path, "wb") as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"Trie saved to {path}")
 
    @staticmethod
    def load(path: str = "trie.pkl") -> "Trie":
        with open(path, "rb") as f:
            return pickle.load(f)
        
        # ── Memory report ────────────────────────────────────────────────────────
 
    def memory_report(self) -> dict:
        """
        Estimate trie memory usage.
 
        Per-node cost breakdown (64-bit CPython):
          TrieNode object overhead  ~  56 bytes
          .children dict overhead   ~ 232 bytes (empty dict)
          avg dict entry (1 child)  ~  72 bytes
          .score int object         ~  28 bytes
          ─────────────────────────────────────
          typical node              ~ 388 bytes
 
        Real usage is lower than worst-case because many nodes share
        children dicts and Python interns small ints.
        """
        BYTES_PER_NODE = 388  # conservative estimate
        estimated_bytes = self._node_count * BYTES_PER_NODE
 
        return {
            "node_count":      self._node_count,
            "estimated_mb":    round(estimated_bytes / 1_048_576, 2),
            "rule_of_thumb":   "~400 bytes x node_count",
            "practical_limit": self._practical_limit(estimated_bytes),
        }
 
    @staticmethod
    def _practical_limit(current_bytes: int) -> str:
        limits = [
            (256 * 1024**2,  "Under 256 MB — fine"),
            (512 * 1024**2,  "256-512 MB — monitor closely"),
            (1024 * 1024**2, "512 MB-1 GB — prune low-score terms"),
        ]
        for cap, msg in limits:
            if current_bytes < cap:
                return msg
        return "Over 1 GB — switch to a pruned trie or external store "
    
    
def _tokenize(text: str) -> list[str]:
    """Lowercase, strip punctuation, split on whitespace."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s\-]", "", text)
    return [t for t in text.split() if len(t) > 1]


def build_autocomplete_trie(pages: list[Tuple[str,str]],queries: list[Tuple[str,int]]) -> Trie:
    """
    Build the autocomplete trie from a list of indexed pages.

    Priority order (highest to lowest):
      1. Past search queries
      2. Page titles
      3. Body text
    """
    trie = Trie()

    # 1. Past search queries — highest weight
    for query, count in queries:
        q = query.lower().strip()

        trie.insert(q, weight=count * WEIGHT["query"])

        for token in _tokenize(q):
            trie.insert(token, weight=count * WEIGHT["query"])
            
    
    for title , content in pages:
    
        # 2. Page title
        title =title.lower()
        trie.insert(title.strip(), weight=WEIGHT["title"])
        for token in _tokenize(title):
            trie.insert(token, weight=WEIGHT["title"])
 
        # 3. Body text — lowest weight, most noise
        tokens = Counter(_tokenize(content)).most_common(200)

        for token, freq in tokens.items():
            trie.insert(token, weight=freq * WEIGHT["body"])

 
    return trie


