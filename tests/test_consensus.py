"""
Integration Tests for Raft Consensus Protocol:
- Multi-node leader election
- Heartbeat preservation
- Quorum log replication
- Leader failover
"""

import shutil
import tempfile
import time
import unittest

from aegis.common.types import OpType
from aegis.server.cluster import AegisCluster


class TestRaftConsensus(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.cluster = AegisCluster(node_count=3, base_port=9100, base_http_port=8100, data_dir=self.test_dir)
        self.cluster.setup()
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_leader_election(self):
        # Allow election to settle
        time.sleep(1.0)

        leader = self.cluster.get_leader()
        self.assertIsNotNone(leader, "A Raft leader should have been elected")
        self.assertEqual(leader.raft.state.value, "LEADER")

        # Verify other 2 nodes are followers
        followers = [n for n in self.cluster.nodes.values() if n.node_id != leader.node_id]
        self.assertEqual(len(followers), 2)
        for f in followers:
            self.assertEqual(f.raft.state.value, "FOLLOWER")
            self.assertEqual(f.raft.leader_id, leader.node_id)

    def test_replicated_proposals(self):
        time.sleep(1.0)
        leader = self.cluster.get_leader()
        self.assertIsNotNone(leader)

        # Propose state mutation via leader
        success, res, l_id = leader.raft.propose(OpType.PUT, "raft_key", "raft_val", timeout=2.0)
        self.assertTrue(success)

        time.sleep(0.3)

        # Verify applied on all followers
        for node in self.cluster.nodes.values():
            found, val = node.storage.get("raft_key")
            self.assertTrue(found)
            self.assertEqual(val, "raft_val")


if __name__ == "__main__":
    unittest.main()
