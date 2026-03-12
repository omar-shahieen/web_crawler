# trie_manager.py
import threading
import schedule
import time
from domain.TrieManager import TrieManager


trie_manager = TrieManager()

#  search endpoint :
def autocomplete_endpoint(query: str):
    trie = trie_manager.get()
    if trie is None:
        return []
    return trie.suggest(query)

if __name__ == "__main__":
    # At startup
    trie_manager.rebuild()   # build once immediately so it's ready on launch

    schedule.every(24).hours.do(trie_manager.rebuild)   # then refresh periodically

    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)

    threading.Thread(target=run_scheduler, daemon=True).start()