"""
Real-Time Stream Processing & Windowing Engine
Implements Event-Time Watermarking, Tumbling Windows, Sliding Windows, and Stateful Aggregations.
"""

import time
import threading
from typing import Dict, List, Optional, Tuple, Any, Callable

class TimeWindow:
    def __init__(self, start_time: float, end_time: float):
        self.start = start_time
        self.end = end_time

    def contains(self, ts: float) -> bool:
        return self.start <= ts < self.end

    def __repr__(self):
        return f"Window[{self.start:.2f} -> {self.end:.2f}]"


class TumblingWindowProcessor:
    """Fixed non-overlapping window aggregator."""
    def __init__(self, window_size_sec: float, aggregate_fn: Callable[[List[Any]], Any]):
        self.size = window_size_sec
        self.agg_fn = aggregate_fn
        self.buckets: Dict[int, List[Any]] = {}  # window_idx -> values
        self.watermark = 0.0
        self.lock = threading.RLock()

    def process_element(self, value: Any, timestamp: float) -> Optional[Tuple[TimeWindow, Any]]:
        with self.lock:
            self.watermark = max(self.watermark, timestamp - 1.0)
            w_idx = int(timestamp // self.size)
            self.buckets.setdefault(w_idx, []).append(value)

            # Check if previous window is ready to emit
            prev_idx = w_idx - 1
            if prev_idx in self.buckets and (prev_idx * self.size + self.size) <= self.watermark:
                items = self.buckets.pop(prev_idx)
                res = self.agg_fn(items)
                return TimeWindow(prev_idx * self.size, (prev_idx + 1) * self.size), res
            return None
