"""
Chaos-Aware Network Transport Layer.
Provides programmatic fault injection capabilities:
- Network Partitions (Split-brain simulation)
- Packet drops / loss rates
- Latency & Jitter injection
- Node isolation
"""

import random
import threading
import time
from typing import Dict, Optional, Set, Tuple

from aegis.common.logger import Logger
from aegis.common.types import RPCMessage
from aegis.network.client import TCPClient


class ChaosNetworkTransport:
    """
    Wraps TCP Client with a dynamic fault-injection pipeline for Jepsen-style chaos testing.
    """

    def __init__(self, node_id: str, tcp_client: TCPClient):
        self.node_id = node_id
        self.client = tcp_client
        self.logger = Logger(node_id=f"{node_id}:TRANSPORT")

        # Chaos state
        self._disabled_links: Set[Tuple[str, str]] = set()  # (from_node, to_node)
        self._isolated_nodes: Set[str] = set()
        self._packet_drop_rate: float = 0.0  # 0.0 to 1.0
        self._min_latency_ms: float = 0.0
        self._max_latency_ms: float = 0.0
        self._lock = threading.RLock()

    def set_packet_drop_rate(self, rate: float):
        """Sets simulated random packet drop probability (0.0 = 0%, 1.0 = 100%)."""
        with self._lock:
            self._packet_drop_rate = max(0.0, min(1.0, rate))

    def set_latency(self, min_ms: float, max_ms: float):
        """Injects simulated network delay."""
        with self._lock:
            self._min_latency_ms = min_ms
            self._max_latency_ms = max_ms

    def partition_nodes(self, partition_a: Set[str], partition_b: Set[str]):
        """Cuts communication bidirectionally between two sub-clusters A and B."""
        with self._lock:
            for na in partition_a:
                for nb in partition_b:
                    self._disabled_links.add((na, nb))
                    self._disabled_links.add((nb, na))
        self.logger.warn("Simulated network partition between %s and %s", partition_a, partition_b)

    def isolate_node(self, target_node: str):
        """Completely cuts off a node from all inbound and outbound communication."""
        with self._lock:
            self._isolated_nodes.add(target_node)
        self.logger.warn("Simulated full isolation of node %s", target_node)

    def heal_partitions(self):
        """Restores full network connectivity across all links."""
        with self._lock:
            self._disabled_links.clear()
            self._isolated_nodes.clear()
            self._packet_drop_rate = 0.0
            self._min_latency_ms = 0.0
            self._max_latency_ms = 0.0
        self.logger.info("Healed all network partitions and cleared fault injection.")

    def can_communicate(self, from_node: str, to_node: str) -> bool:
        with self._lock:
            if from_node in self._isolated_nodes or to_node in self._isolated_nodes:
                return False
            if (from_node, to_node) in self._disabled_links:
                return False
            if self._packet_drop_rate > 0.0 and random.random() < self._packet_drop_rate:
                return False
            return True

    def send_rpc(
        self,
        host: str,
        port: int,
        target_node_id: str,
        message: RPCMessage,
        timeout: Optional[float] = None
    ) -> Optional[RPCMessage]:
        """
        Sends an RPC through the chaos transport layer.
        Drops the packet or delays transmission if chaos conditions apply.
        """
        if not self.can_communicate(self.node_id, target_node_id):
            return None

        # Inject latency if configured
        with self._lock:
            if self._max_latency_ms > 0:
                delay = random.uniform(self._min_latency_ms, self._max_latency_ms) / 1000.0
                time.sleep(delay)

        resp = self.client.send_rpc(host, port, message, timeout=timeout)

        # Check return path communication
        if resp and not self.can_communicate(target_node_id, self.node_id):
            return None

        return resp
