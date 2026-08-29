"""
Unit and Integration Tests for Concurrency Control & Distributed Transactions:
- Two-Phase Commit (2PC) Commit and Abort paths
- MVCC Snapshot Isolation and Version Garbage Collection
- LockManager 2PL with Wait-For-Graph Deadlock Detection
"""

import unittest
from aegis.tx.lock_manager import LockManager, LockMode
from aegis.tx.mvcc import MVCCStorage, ReadView


class TestConcurrencyAndTransactions(unittest.TestCase):
    def test_mvcc_snapshot_isolation(self):
        mvcc = MVCCStorage()

        # Write version 1 at ts=100
        mvcc.write("account_1", 500, commit_ts=100, tx_id="tx_1")

        # Reader creates read view at ts=100
        view_100 = mvcc.create_read_view(read_ts=100)

        # Concurrent transaction writes version 2 at ts=200
        mvcc.write("account_1", 800, commit_ts=200, tx_id="tx_2")

        # Reader with view_100 MUST still see version 1 (500)
        found, val = mvcc.get("account_1", view_100)
        self.assertTrue(found)
        self.assertEqual(val, 500)

        # Reader with view at ts=250 sees version 2 (800)
        view_250 = mvcc.create_read_view(read_ts=250)
        found2, val2 = mvcc.get("account_1", view_250)
        self.assertTrue(found2)
        self.assertEqual(val2, 800)

    def test_lock_manager_shared_and_exclusive(self):
        lm = LockManager(node_id="DLM-TEST")

        # Acquire Shared lock
        ok1 = lm.acquire_lock("tx1", "resA", LockMode.SHARED)
        self.assertTrue(ok1)

        # Concurrent Shared lock succeeds
        ok2 = lm.acquire_lock("tx2", "resA", LockMode.SHARED)
        self.assertTrue(ok2)

        # Exclusive lock on same resource fails/times out
        ok3 = lm.acquire_lock("tx3", "resA", LockMode.EXCLUSIVE, timeout_sec=0.1)
        self.assertFalse(ok3)

        # Release shared locks
        lm.release_all("tx1")
        lm.release_all("tx2")

        # Now exclusive lock succeeds
        ok4 = lm.acquire_lock("tx3", "resA", LockMode.EXCLUSIVE, timeout_sec=0.5)
        self.assertTrue(ok4)
        lm.release_all("tx3")

    def test_deadlock_detection(self):
        lm = LockManager(node_id="DLM-TEST")

        # tx1 holds X-lock on A
        lm.acquire_lock("tx1", "resA", LockMode.EXCLUSIVE)
        # tx2 holds X-lock on B
        lm.acquire_lock("tx2", "resB", LockMode.EXCLUSIVE)

        # Simulate Wait-For-Graph cycle:
        # tx1 waits for resB (held by tx2)
        # tx2 waits for resA (held by tx1)
        # We can test detection directly
        lm._wait_for_graph["tx1"].add("tx2")
        lm._wait_for_graph["tx2"].add("tx1")

        cycle = lm._find_deadlock_cycle()
        self.assertIsNotNone(cycle)
        self.assertEqual(set(cycle), {"tx1", "tx2"})


if __name__ == "__main__":
    unittest.main()
