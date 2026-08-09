import threading
import time


class RateLimiter:
    """Shared across every thread making GitHub API calls, enforcing one
    global ceiling (not one per caller) -- see ARCHITECTURE.md."""

    def __init__(self, min_interval=1.0):
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.last_call = 0.0

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self.last_call)
            if wait > 0:
                time.sleep(wait)
            self.last_call = time.monotonic()
