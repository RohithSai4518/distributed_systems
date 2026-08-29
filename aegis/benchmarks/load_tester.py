"""
Distributed Load Testing & Latency Percentile Benchmark Harness.
Measures P50, P90, P95, P99, P99.9 latencies, operations per second,
and failure rates under configurable concurrent client workers.
"""

from concurrent.futures import ThreadPoolExecutor
import random
import statistics
import time
from typing import Dict, List, Tuple

from aegis.client.sdk import AegisClient
from aegis.common.logger import Logger


class BenchmarkResult:
    def __init__(self, total_ops: int, total_time_sec: float, latencies_ms: List[float], errors: int):
        self.total_ops = total_ops
        self.total_time_sec = total_time_sec
        self.throughput_ops_sec = total_ops / total_time_sec if total_time_sec > 0 else 0
        self.errors = errors
        self.latencies_ms = sorted(latencies_ms) if latencies_ms else [0.0]

    def summary(self) -> Dict[str, float]:
        if not self.latencies_ms:
            return {}

        n = len(self.latencies_ms)
        return {
            "Total Operations": self.total_ops,
            "Total Time (s)": round(self.total_time_sec, 3),
            "Throughput (ops/sec)": round(self.throughput_ops_sec, 2),
            "Error Count": self.errors,
            "Min Latency (ms)": round(self.latencies_ms[0], 2),
            "Avg Latency (ms)": round(statistics.mean(self.latencies_ms), 2),
            "P50 Latency (ms)": round(self.latencies_ms[int(n * 0.50)], 2),
            "P90 Latency (ms)": round(self.latencies_ms[int(n * 0.90)], 2),
            "P95 Latency (ms)": round(self.latencies_ms[int(n * 0.95)], 2),
            "P99 Latency (ms)": round(self.latencies_ms[int(min(n - 1, int(n * 0.99)))], 2),
            "Max Latency (ms)": round(self.latencies_ms[-1], 2),
        }

    def print_report(self):
        print("\n" + "=" * 55)
        print("          AEGIS DISTRIBUTED BENCHMARK REPORT         ")
        print("=" * 55)
        for k, v in self.summary().items():
            print(f"  {k.ljust(25)} : {v}")
        print("=" * 55 + "\n")


class LoadTester:
    """
    Concurrent load test generator for read/write mixes.
    """

    def __init__(self, seed_nodes: List[Tuple[str, int]], num_workers: int = 8, ops_per_worker: int = 250):
        self.seed_nodes = seed_nodes
        self.num_workers = num_workers
        self.ops_per_worker = ops_per_worker
        self.logger = Logger(node_id="BENCHMARK")

    def run_benchmark(self, read_ratio: float = 0.5) -> BenchmarkResult:
        """
        Executes parallel load test with read_ratio (e.g. 0.8 = 80% GET, 20% PUT).
        """
        self.logger.info("Launching benchmark with %d concurrent workers (%d ops/worker, %.0f%% reads)...",
                         self.num_workers, self.ops_per_worker, read_ratio * 100)

        total_ops = self.num_workers * self.ops_per_worker
        all_latencies: List[float] = []
        errors = 0

        def _worker_task(worker_id: int) -> Tuple[List[float], int]:
            client = AegisClient(seed_nodes=self.seed_nodes, client_id=f"bench-worker-{worker_id}")
            latencies = []
            err_count = 0

            for i in range(self.ops_per_worker):
                key = f"bench_key_{random.randint(1, 500)}"
                is_read = random.random() < read_ratio

                t0 = time.time()
                try:
                    if is_read:
                        found, _ = client.get(key)
                    else:
                        val = f"bench_val_{worker_id}_{i}"
                        ok = client.put(key, val)
                        if not ok:
                            err_count += 1
                except Exception:
                    err_count += 1

                elapsed_ms = (time.time() - t0) * 1000.0
                latencies.append(elapsed_ms)

            client.close()
            return latencies, err_count

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(_worker_task, w) for w in range(self.num_workers)]
            for fut in futures:
                lats, errs = fut.result()
                all_latencies.extend(lats)
                errors += errs

        total_duration = time.time() - start_time
        return BenchmarkResult(total_ops, total_duration, all_latencies, errors)
