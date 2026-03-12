# trie_manager.py
import threading
import schedule
import time
from domain.TrieManager import trie_manager 


if __name__ == "__main__":
    # At startup
    trie_manager.rebuild()   # build once immediately so it's ready on launch

    schedule.every(24).hours.do(trie_manager.rebuild)   # then refresh periodically

    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)

    threading.Thread(target=run_scheduler, daemon=True).start()