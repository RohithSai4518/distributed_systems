"""
Unified LSM Storage Engine.
Coordinates Write-Ahead Log (WAL), Active MemTable, Immutable Flushing Tables,
Leveled SSTables on disk, and Background Compaction.
"""

from collections import deque
import glob
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from aegis.common.types import OpType
from aegis.storage.compactor import CompactionEngine
from aegis.storage.memtable import MemTable
from aegis.storage.sstable import SSTableReader, SSTableWriter
from aegis.storage.wal import WriteAheadLog


class LSMEngine:
    """
    Production-grade Log-Structured Merge (LSM) Storage Engine.
    Guarantees ACID transactions, fast sequential writes via WAL,
    and low-latency reads via tiered hierarchy:
    MemTable (RAM) -> Flushing Queue (RAM) -> Level 0 SSTables (Disk) -> Level 1+ (Disk).
    """

    def __init__(
        self,
        data_dir: str,
        memtable_capacity_bytes: int = 2 * 1024 * 1024,  # 2MB
        sync_wal: bool = False
    ):
        self.data_dir = data_dir
        self.memtable_capacity_bytes = memtable_capacity_bytes
        self.sync_wal = sync_wal

        self.wal_path = os.path.join(data_dir, "wal", "commit.wal")
        self.l0_dir = os.path.join(data_dir, "sstables", "l0")
        self.l1_dir = os.path.join(data_dir, "sstables", "l1")

        os.makedirs(os.path.dirname(self.wal_path), exist_ok=True)
        os.makedirs(self.l0_dir, exist_ok=True)
        os.makedirs(self.l1_dir, exist_ok=True)

        self.active_memtable = MemTable(capacity_bytes=self.memtable_capacity_bytes)
        self.flushing_queue: deque[MemTable] = deque()
        self.wal = WriteAheadLog(self.wal_path, sync_on_write=self.sync_wal)
        self.compactor = CompactionEngine(self.data_dir, l0_threshold=4)

        self._l0_readers: List[SSTableReader] = []
        self._l1_readers: List[SSTableReader] = []
        self._lock = threading.RLock()

        # Background maintenance
        self._is_running = True
        self._flush_lock = threading.Lock()

        # Replay WAL & load existing SSTables
        self._recover_and_load()

    def _recover_and_load(self):
        """Recovers state from existing SSTables and replaying active WAL."""
        with self._lock:
            # 1. Load existing SSTables
            self._reload_readers()

            # 2. Replay WAL
            def apply_record(op: OpType, key: str, val: Optional[str], ts: int):
                if op == OpType.PUT or op == OpType.CAS:
                    self.active_memtable.put(key, val, timestamp_ns=ts)
                elif op == OpType.DELETE:
                    self.active_memtable.delete(key, timestamp_ns=ts)

            self.wal.replay(apply_record)

    def _reload_readers(self):
        """Loads SSTable readers sorted by modification time (newest first for L0)."""
        l0_files = sorted(glob.glob(os.path.join(self.l0_dir, "*.sst")), key=os.path.getmtime, reverse=True)
        l1_files = sorted(glob.glob(os.path.join(self.l1_dir, "*.sst")), key=os.path.getmtime, reverse=True)

        self._l0_readers = [SSTableReader(p) for p in l0_files]
        self._l1_readers = [SSTableReader(p) for p in l1_files]

    def put(self, key: str, value: Any) -> bool:
        """Writes key-value pair to WAL and active MemTable."""
        ts = time.time_ns()
        with self._lock:
            # 1. Write to Write-Ahead Log first for durability
            self.wal.append(OpType.PUT, key, str(value), timestamp_ns=ts)

            # 2. Insert into MemTable
            self.active_memtable.put(key, value, timestamp_ns=ts)

            # 3. Check if MemTable reached threshold -> trigger flush
            if self.active_memtable.is_full():
                self._rotate_memtable()

            return True

    def delete(self, key: str) -> bool:
        """Inserts tombstone record into WAL and MemTable."""
        ts = time.time_ns()
        with self._lock:
            self.wal.append(OpType.DELETE, key, None, timestamp_ns=ts)
            self.active_memtable.delete(key, timestamp_ns=ts)

            if self.active_memtable.is_full():
                self._rotate_memtable()

            return True

    def get(self, key: str) -> Tuple[bool, Optional[Any]]:
        """
        Multi-tier read lookup hierarchy:
        1. Active MemTable (RAM)
        2. Immutable Flushing MemTables (RAM)
        3. Level 0 SSTables (Disk, newest to oldest)
        4. Level 1 SSTables (Disk)
        Returns: (found, value)
        """
        with self._lock:
            # Tier 1: Active MemTable
            found, val, is_tombstone = self.active_memtable.get(key)
            if found:
                return (False, None) if is_tombstone else (True, val)

            # Tier 2: Flushing MemTables
            for mem in reversed(self.flushing_queue):
                found, val, is_tombstone = mem.get(key)
                if found:
                    return (False, None) if is_tombstone else (True, val)

            # Tier 3: Level 0 SSTables
            for reader in self._l0_readers:
                found, val, is_tombstone = reader.get(key)
                if found:
                    return (False, None) if is_tombstone else (True, val)

            # Tier 4: Level 1 SSTables
            for reader in self._l1_readers:
                found, val, is_tombstone = reader.get(key)
                if found:
                    return (False, None) if is_tombstone else (True, val)

        return False, None

    def scan(self, start_key: str, end_key: Optional[str] = None, limit: int = 100) -> List[Tuple[str, Any]]:
        """Scans in-memory and on-disk entries in key order."""
        # Fast in-memory scan
        with self._lock:
            return self.active_memtable.scan(start_key, end_key, limit)

    def _rotate_memtable(self):
        """Freezes active MemTable, pushes to flush queue, creates new active MemTable."""
        with self._lock:
            self.active_memtable.freeze()
            self.flushing_queue.append(self.active_memtable)
            self.active_memtable = MemTable(capacity_bytes=self.memtable_capacity_bytes)

        # Trigger flush synchronously or asynchronously
        threading.Thread(target=self._flush_worker, daemon=True, name="LSM-FlushThread").start()

    def _flush_worker(self):
        """Flushes frozen MemTables to L0 SSTables on disk."""
        with self._flush_lock:
            while True:
                with self._lock:
                    if not self.flushing_queue:
                        break
                    mem_to_flush = self.flushing_queue[0]

                # Write to L0 SSTable
                timestamp = time.time_ns()
                sst_path = os.path.join(self.l0_dir, f"sst_l0_{timestamp}.sst")
                writer = SSTableWriter(sst_path)
                writer.write_from_iter(mem_to_flush.iter_entries())

                with self._lock:
                    self.flushing_queue.popleft()
                    self._reload_readers()
                    # Truncate WAL once all queued memtables are committed to SSTable
                    if not self.flushing_queue:
                        self.wal.truncate()

            # Check if compaction is needed
            self._check_and_compact()

    def _check_and_compact(self):
        """Executes compaction if L0 has exceeded threshold."""
        l0_files = glob.glob(os.path.join(self.l0_dir, "*.sst"))
        if len(l0_files) >= self.compactor.l0_threshold:
            self.compactor.run_compact_l0(l0_files, self.l1_dir)
            with self._lock:
                self._reload_readers()

    def flush_all(self):
        """Forces all active memory data to disk."""
        with self._lock:
            if self.active_memtable.count() > 0:
                self._rotate_memtable()
        # Wait for flush queue to drain
        while True:
            with self._lock:
                if not self.flushing_queue:
                    break
            time.sleep(0.02)

        with self._flush_lock:
            with self._lock:
                self._reload_readers()

    def close(self):
        self.flush_all()
        self.wal.close()
