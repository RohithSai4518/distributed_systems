"""
Integration Tests for Chaos Simulator and Linearizability Model Checker.
"""

import shutil
import tempfile
import unittest

from aegis.chaos.linearizability import LinearizabilityChecker, OpEvent, OperationType
from aegis.chaos.simulator import ChaosHarness
from aegis.server.cluster import AegisCluster


class TestChaosAndLinearizability(unittest.TestCase):
    def test_linearizability_valid_history(self):
        # Sequential write followed by read
        events = [
            OpEvent("op1", "c1", OperationType.WRITE, "x", "v1", None, start_ts=10, end_ts=20),
            OpEvent("op2", "c2", OperationType.READ, "x", None, "v1", start_ts=25, end_ts=35),
            OpEvent("op3", "c1", OperationType.WRITE, "x", "v2", None, start_ts=40, end_ts=50),
            OpEvent("op4", "c3", OperationType.READ, "x", None, "v2", start_ts=55, end_ts=65),
        ]
        is_linear, msg = LinearizabilityChecker.verify_single_key(events, initial_value=None)
        self.assertTrue(is_linear)

    def test_linearizability_stale_read_violation(self):
        # Write v2 completes before Read starts, but Read returns old v1
        events = [
            OpEvent("op1", "c1", OperationType.WRITE, "x", "v1", None, start_ts=10, end_ts=20),
            OpEvent("op2", "c1", OperationType.WRITE, "x", "v2", None, start_ts=25, end_ts=30),
            OpEvent("op3", "c2", OperationType.READ, "x", None, "v1", start_ts=35, end_ts=45),  # Stale!
        ]
        is_linear, msg = LinearizabilityChecker.verify_single_key(events, initial_value=None)
        self.assertFalse(is_linear)

    def test_cluster_chaos_failover(self):
        tmp_dir = tempfile.mkdtemp()
        cluster = AegisCluster(node_count=3, base_port=9200, base_http_port=8200, data_dir=tmp_dir)
        cluster.setup()
        cluster.start()

        harness = ChaosHarness(cluster)
        success = harness.test_leader_failover()
        self.assertTrue(success)

        cluster.stop()
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
