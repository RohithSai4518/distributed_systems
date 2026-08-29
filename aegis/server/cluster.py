"""
Cluster Orchestration & Life-Cycle Management.
Creates, configures, boots, and gracefully tears down multi-node distributed clusters.
"""

import shutil
import threading
import time
from typing import Dict, List, Optional

from aegis.common.logger import Logger
from aegis.common.types import PeerNode
from aegis.server.node import AegisNode


class AegisCluster:
    """
    Manages an N-node distributed cluster environment.
    """

    def __init__(self, node_count: int = 3, base_port: int = 9001, base_http_port: int = 8001, data_dir: str = "./data"):
        self.node_count = node_count
        self.base_port = base_port
        self.base_http_port = base_http_port
        self.data_dir = data_dir
        self.logger = Logger(node_id="CLUSTER")

        self.nodes: Dict[str, AegisNode] = {}
        self.peer_map: Dict[str, PeerNode] = {}
        self._is_running = False

    def setup(self):
        """Constructs peer maps and initializes all node instances."""
        # 1. Build peer directory
        for i in range(1, self.node_count + 1):
            n_id = f"node-{i}"
            port = self.base_port + i - 1
            http_port = self.base_http_port + i - 1
            self.peer_map[n_id] = PeerNode(
                node_id=n_id,
                host="127.0.0.1",
                port=port,
                http_port=http_port
            )

        # 2. Instantiate nodes with peer references
        for n_id, peer in self.peer_map.items():
            other_peers = {pid: p for pid, p in self.peer_map.items() if pid != n_id}
            node = AegisNode(
                node_id=n_id,
                host=peer.host,
                port=peer.port,
                http_port=peer.http_port,
                peers=other_peers,
                data_root_dir=self.data_dir
            )
            self.nodes[n_id] = node

        self.logger.info("Initialized %d-node distributed cluster topology.", self.node_count)

    def start(self):
        """Boots all cluster nodes."""
        self._is_running = True
        for node in self.nodes.values():
            node.start()

        self.logger.info("Cluster booted successfully! Awaiting leader election...")
        time.sleep(1.0)  # Allow consensus leader election to complete

    def get_leader(self) -> Optional[AegisNode]:
        """Finds the current Raft consensus leader node."""
        for node in self.nodes.values():
            if node.raft.state.value == "LEADER":
                return node
        return None

    def stop(self):
        """Gracefully stops all cluster nodes."""
        self._is_running = False
        for node in self.nodes.values():
            node.stop()
        self.logger.info("Distributed cluster safely terminated.")
