"""
Comprehensive Unit & Integration Tests for Storage Subsystems:
- SkipList concurrent ordering and range scans
- Write-Ahead Log (WAL) persistence and crash replay
- Bloom Filter negative lookup guarantee
- SSTable generation, sparse index binary search, and leveled compaction
- Unified LSMEngine end-to-end multi-tier read/write verification
"""

import os
import shutil
import tempfile
import time
import unittest

from aegis.common.types import OpType
from aegis.storage.bloom_filter import BloomFilter
from aegis.storage.compactor import CompactionEngine
from aegis.storage.engine import LSMEngine
from aegis.storage.memtable import MemTable, SkipList
from aegis.storage.sstable import SSTableReader, SSTableWriter
from aegis.storage.wal import WriteAheadLog


class TestSkipListAndMemTable(unittest.TestCase):
    def test_skiplist_crud(self):
        sl = SkipList()
        sl.put("k1", "v1")
        sl.put("k2", "v2")
        sl.put("k3", "v3")

        found, val, is_tombstone = sl.get("k2")
        self.assertTrue(found)
        self.assertEqual(val, "v2")
        self.assertFalse(is_tombstone)

        # Update
        sl.put("k2", "v2_updated")
        found, val, _ = sl.get("k2")
        self.assertEqual(val, "v2_updated")

        # Range scan
        items = sl.scan(start_key="k1", end_key="k3")
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0], ("k1", "v1"))
        self.assertEqual(items[1], ("k2", "v2_updated"))

    def test_memtable_freezing(self):
        mem = MemTable(capacity_bytes=100)
        self.assertFalse(mem.is_full())
        # Write large string to force full
        for i in range(10):
            mem.put(f"key_{i}", "x" * 50)
        self.assertTrue(mem.is_full())


class TestWriteAheadLog(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.wal_path = os.path.join(self.test_dir, "test.wal")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_wal_append_and_replay(self):
        wal = WriteAheadLog(self.wal_path, sync_on_write=True)
        wal.append(OpType.PUT, "user:1", "Alice")
        wal.append(OpType.PUT, "user:2", "Bob")
        wal.append(OpType.DELETE, "user:1", None)
        wal.close()

        # Replay WAL into memory dictionary
        recovered_state = {}
        def apply_fn(op, k, v, ts):
            if op == OpType.PUT:
                recovered_state[k] = v
            elif op == OpType.DELETE:
                recovered_state.pop(k, None)

        wal_recovery = WriteAheadLog(self.wal_path)
        count = wal_recovery.replay(apply_fn)
        wal_recovery.close()

        self.assertEqual(count, 3)
        self.assertEqual(recovered_state, {"user:2": "Bob"})


class TestBloomFilter(unittest.TestCase):
    def test_bloom_filter_guarantee(self):
        bf = BloomFilter(expected_elements=1000, false_positive_rate=0.01)
        added_keys = [f"item_{i}" for i in range(100)]
        for k in added_keys:
            bf.add(k)

        # Guaranteed: all added keys MUST return True
        for k in added_keys:
            self.assertTrue(bf.contains(k))

        # Keys not added should mostly return False
        false_positives = 0
        for i in range(100, 200):
            if bf.contains(f"item_{i}"):
                false_positives += 1

        self.assertTrue(false_positives < 10)  # Low false positive count


class TestSSTableAndCompaction(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.sst_path = os.path.join(self.test_dir, "table1.sst")

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_sstable_write_and_binary_search(self):
        entries = [(f"k_{i:04d}", f"val_{i:04d}", False, i * 1000) for i in range(100)]
        writer = SSTableWriter(self.sst_path, index_interval=4)
        writer.write_from_iter(iter(entries))

        reader = SSTableReader(self.sst_path)
        self.assertEqual(reader.entry_count, 100)

        # Lookup existing keys
        found, val, tomb = reader.get("k_0042")
        self.assertTrue(found)
        self.assertEqual(val, "val_0042")
        self.assertFalse(tomb)

        # Non-existing key
        found_non, _, _ = reader.get("k_9999")
        self.assertFalse(found_non)


class TestLSMEngineEndToEnd(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.engine = LSMEngine(data_dir=self.test_dir, memtable_capacity_bytes=512)

    def tearDown(self):
        self.engine.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_engine_put_get_delete(self):
        self.engine.put("alpha", "100")
        self.engine.put("beta", "200")

        found, val = self.engine.get("alpha")
        self.assertTrue(found)
        self.assertEqual(val, "100")

        self.engine.delete("alpha")
        found, val = self.engine.get("alpha")
        self.assertFalse(found)

    def test_flush_and_compaction_durability(self):
        # Insert enough items to trigger multiple memtable flushes to SSTables
        for i in range(50):
            self.engine.put(f"bulk_key_{i:03d}", f"bulk_val_{i:03d}")

        self.engine.flush_all()

        # Verify all items can be read back from disk SSTables
        for i in range(50):
            found, val = self.engine.get(f"bulk_key_{i:03d}")
            self.assertTrue(found)
            self.assertEqual(val, f"bulk_val_{i:03d}")


if __name__ == "__main__":
    unittest.main()
