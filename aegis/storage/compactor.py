"""
LSM-Tree Leveled Background Compaction Engine.
Performs multi-way k-merge sorting across multiple SSTables, collapses superseded key versions,
purges expired tombstones, and enforces leveled disk tiering.
"""

import heapq
import os
import threading
import time
from typing import List, Optional, Tuple

from aegis.storage.sstable import SSTableReader, SSTableWriter


class CompactionEngine:
    """
    Background worker that monitors SSTable count per level and executes compactions.
    Level 0 -> Level 1 (triggered when L0 table count > threshold)
    Level 1 -> Level 2 (size-tiered capacity compaction)
    """

    def __init__(self, db_dir: str, l0_threshold: int = 4, max_levels: int = 4):
        self.db_dir = db_dir
        self.l0_threshold = l0_threshold
        self.max_levels = max_levels
        self._lock = threading.RLock()
        self._is_running = False
        self._worker_thread: Optional[threading.Thread] = None

    def merge_sstables(self, input_paths: List[str], output_path: str, is_bottom_level: bool = False):
        """
        K-way merge sort of multiple SSTables.
        Preserves only the newest key version across tables (higher timestamp wins).
        Tombstones are purged if is_bottom_level is True.
        """
        if not input_paths:
            return

        readers = [SSTableReader(p) for p in input_paths]
        iterators = [r.scan_all() for r in readers]

        # Priority queue entry: (key, -timestamp, source_index, value, is_tombstone, timestamp)
        # We invert timestamp so highest timestamp pops first for identical keys
        heap = []
        for idx, it in enumerate(iterators):
            try:
                entry = next(it)
                # entry is (key, value, is_tombstone, ts)
                heapq.heappush(heap, (entry[0], -entry[3], idx, entry[1], entry[2], entry[3]))
            except StopIteration:
                pass

        merged_entries = []
        last_key = None

        while heap:
            key, neg_ts, src_idx, val, tombstone, ts = heapq.heappop(heap)

            # Advance the iterator that produced this entry
            try:
                next_entry = next(iterators[src_idx])
                heapq.heappush(heap, (next_entry[0], -next_entry[3], src_idx, next_entry[1], next_entry[2], next_entry[3]))
            except StopIteration:
                pass

            # If we've already processed a newer version of this key, skip older versions
            if key == last_key:
                continue

            last_key = key

            # If tombstone and at the bottom level, we can completely eliminate it
            if tombstone and is_bottom_level:
                continue

            merged_entries.append((key, val, tombstone, ts))

        # Write out the merged SSTable
        writer = SSTableWriter(output_path)
        writer.write_from_iter(iter(merged_entries))

        # Remove old input files
        for p in input_paths:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass

    def run_compact_l0(self, l0_paths: List[str], l1_dir: str) -> Optional[str]:
        """Compacts all L0 SSTables into a single new L1 SSTable."""
        with self._lock:
            if len(l0_paths) < self.l0_threshold:
                return None

            timestamp = time.time_ns()
            out_file = os.path.join(l1_dir, f"sst_l1_{timestamp}.sst")
            self.merge_sstables(l0_paths, out_file, is_bottom_level=False)
            return out_file
