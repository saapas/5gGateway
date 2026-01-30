import threading
import time

# Databuffer with lock

class DataBuffer:
    def __init__(self, batch_size=10, max_wait_seconds=5):
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds

        self.buffer = []
        self.lock = threading.Lock()
        self.last_flush_time = time.time()

    def add(self, data):
        with self.lock:
            self.buffer.append(data)

    # Check if there is enough entries to send it to the database or if enough time has passed since last addition
    def get_batch_if_ready(self):
        with self.lock:
            now = time.time()

            if (
                len(self.buffer) >= self.batch_size
                or (self.buffer and now - self.last_flush_time >= self.max_wait_seconds)
            ):
                batch = self.buffer.copy()
                self.buffer.clear()
                self.last_flush_time = now
                return batch

            return None

    def requeue(self, batch):
        with self.lock:
            self.buffer = batch + self.buffer
