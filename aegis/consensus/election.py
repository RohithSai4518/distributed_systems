"""
Raft Election Timer & Timeout Manager.
Randomizes timeouts between [min_timeout_ms, max_timeout_ms] to prevent split-vote scenarios.
"""

import random
import threading
import time
from typing import Callable, Optional


class ElectionTimer:
    """
    Thread-safe randomized election timer.
    Resets upon receiving valid leader heartbeats or granting votes.
    """

    def __init__(
        self,
        min_timeout_ms: int = 150,
        max_timeout_ms: int = 300,
        on_timeout: Optional[Callable[[], None]] = None
    ):
        self.min_timeout_ms = min_timeout_ms
        self.max_timeout_ms = max_timeout_ms
        self.on_timeout = on_timeout

        self._last_reset_time = time.time()
        self._current_timeout = self._random_timeout_sec()
        self._is_running = False
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None

    def _random_timeout_sec(self) -> float:
        return random.randint(self.min_timeout_ms, self.max_timeout_ms) / 1000.0

    def start(self):
        with self._lock:
            self._is_running = True
            self.reset()
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="ElectionTimer")
            self._thread.start()

    def reset(self):
        """Resets the timer countdown."""
        with self._lock:
            self._last_reset_time = time.time()
            self._current_timeout = self._random_timeout_sec()

    def stop(self):
        with self._lock:
            self._is_running = False

    def _run_loop(self):
        while self._is_running:
            time.sleep(0.02)  # 20ms precision check
            with self._lock:
                if not self._is_running:
                    break
                elapsed = time.time() - self._last_reset_time
                if elapsed >= self._current_timeout:
                    self._last_reset_time = time.time()
                    self._current_timeout = self._random_timeout_sec()
                    if self.on_timeout and self._is_running:
                        try:
                            threading.Thread(target=self.on_timeout, daemon=True).start()
                        except (RuntimeError, Exception):
                            pass
