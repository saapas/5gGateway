import threading
import time
from collections import OrderedDict

# Databuffer with lock and deduplication

DEDUP_CACHE_MAX = 10000  # max messageIds tracked for dedup

class DataBuffer:
    def __init__(self, batch_size=10, max_wait_seconds=5):
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds

        self.buffer = []
        self.lock = threading.Lock()
        self.last_flush_time = time.time()
        self._seen_ids = OrderedDict()  # messageId dedup cache (FIFO eviction)

    def add(self, data):
        with self.lock:
            # Deduplicate by messageId if present
            msg_id = data.get("messageId")
            if msg_id:
                if msg_id in self._seen_ids:
                    return False  # duplicate, skip
                self._seen_ids[msg_id] = True
                # Evict oldest entries when cache is full
                while len(self._seen_ids) > DEDUP_CACHE_MAX:
                    self._seen_ids.popitem(last=False)
            self.buffer.append(data)
            return True

    # Check if there is enough entries to send it to the database or if enough time has passed since last addition
    def get_batch_if_ready(self):
        with self.lock:
            now = time.time()

            if (
                len(self.buffer) >= self.batch_size
                or (self.buffer and now - self.last_flush_time >= self.max_wait_seconds)
            ):
                # Return exactly batch_size items (or all if fewer remain)
                count = min(len(self.buffer), self.batch_size)
                batch = self.buffer[:count]
                self.buffer = self.buffer[count:]
                self.last_flush_time = now
                return batch

            return None

    def requeue(self, batch):
        with self.lock:
            self.buffer = batch + self.buffer
