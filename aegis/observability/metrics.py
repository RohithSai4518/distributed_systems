"""
Distributed OpenTelemetry Tracing & Prometheus Metrics Engine
Features:
- Thread-safe Atomic Metric Counters, Gauges, and Histograms (Quantiles P50, P90, P99)
- Distributed Context Baggage & Span Tree Tracer
- Prometheus Exposition Formatter
"""

import time
import math
import threading
import uuid
from typing import Dict, List, Optional, Tuple, Any

class CounterMetric:
    def __init__(self, name: str, help_text: str):
        self.name = name
        self.help_text = help_text
        self.value = 0.0
        self.lock = threading.RLock()

    def inc(self, amount: float = 1.0):
        with self.lock:
            self.value += amount


class GaugeMetric:
    def __init__(self, name: str, help_text: str):
        self.name = name
        self.help_text = help_text
        self.value = 0.0
        self.lock = threading.RLock()

    def set(self, val: float):
        with self.lock:
            self.value = val


class HistogramMetric:
    """Reservoir sampled latency histogram."""
    def __init__(self, name: str, help_text: str, max_samples: int = 1000):
        self.name = name
        self.help_text = help_text
        self.samples: List[float] = []
        self.max_samples = max_samples
        self.lock = threading.RLock()

    def observe(self, val: float):
        with self.lock:
            if len(self.samples) < self.max_samples:
                self.samples.append(val)
            else:
                self.samples[int(time.time_ns()) % self.max_samples] = val

    def quantiles(self) -> Dict[str, float]:
        with self.lock:
            if not self.samples:
                return {"p50": 0.0, "p90": 0.0, "p99": 0.0}
            s = sorted(self.samples)
            n = len(s)
            return {
                "p50": s[int(n * 0.50)],
                "p90": s[int(n * 0.90)],
                "p99": s[int(n * 0.99)]
            }


class Span:
    def __init__(self, trace_id: str, name: str, parent_id: Optional[str] = None):
        self.trace_id = trace_id
        self.span_id = uuid.uuid4().hex[:16]
        self.parent_id = parent_id
        self.name = name
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.attributes: Dict[str, Any] = {}

    def end(self):
        self.end_time = time.time()

    def duration_ms(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time) * 1000.0
        return (time.time() - self.start_time) * 1000.0


class Tracer:
    """Distributed tracing manager with baggage propagation."""
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.spans: List[Span] = []
        self.lock = threading.RLock()

    def start_span(self, name: str, parent: Optional[Span] = None) -> Span:
        with self.lock:
            trace_id = parent.trace_id if parent else uuid.uuid4().hex
            parent_id = parent.span_id if parent else None
            span = Span(trace_id, name, parent_id)
            self.spans.append(span)
            return span
