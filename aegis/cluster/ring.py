"""
Consistent Hashing Ring with Virtual Nodes.
Distributes keys uniformly across an N-node cluster with minimal key movement on node join/leave.
Supports multi-replica placement ($N$ replicas), quorum calculations ($R, W$), and token ranges.
"""

import bisect
import threading
from typing import Dict, List, Optional, Set, Tuple

from aegis.common.crypto import HashAlgorithms
from aegis.common.types import PeerNode


class ConsistentHashRing:
    """
    Consistent Hash Ring implementation using $2^{64}-1$ circular key space.
    """

    def __init__(self, vnodes_per_node: int = 64, replication_factor: int = 3):
        self.vnodes_per_node = vnodes_per_node
        self.replication_factor = replication_factor

        self._ring: List[int] = []  # Sorted token hashes
        self._token_to_node: Dict[int, str] = {}  # Token hash -> Node ID
        self._nodes: Dict[str, PeerNode] = {}
        self._lock = threading.RLock()

    def add_node(self, node: PeerNode):
        """Adds a physical node and registers its virtual nodes on the hash ring."""
        with self._lock:
            self._nodes[node.node_id] = node
            for i in range(self.vnodes_per_node):
                vnode_key = f"{node.node_id}#vnode-{i}"
                token = HashAlgorithms.fnv1a_64(vnode_key)
                bisect.insort(self._ring, token)
                self._token_to_node[token] = node.node_id

    def remove_node(self, node_id: str):
        """Removes a physical node and all its virtual tokens from the ring."""
        with self._lock:
            if node_id not in self._nodes:
                return

            del self._nodes[node_id]
            for i in range(self.vnodes_per_node):
                vnode_key = f"{node_id}#vnode-{i}"
                token = HashAlgorithms.fnv1a_64(vnode_key)
                if token in self._token_to_node:
                    del self._token_to_node[token]
                idx = bisect.bisect_left(self._ring, token)
                if idx < len(self._ring) and self._ring[idx] == token:
                    self._ring.pop(idx)

    def get_node_for_key(self, key: str) -> Optional[PeerNode]:
        """Maps a key to its primary owner node on the ring."""
        nodes = self.get_preference_list(key, count=1)
        return nodes[0] if nodes else None

    def get_preference_list(self, key: str, count: Optional[int] = None) -> List[PeerNode]:
        """
        Returns the ordered preference list of distinct physical nodes responsible for replicating this key.
        Walks clockwise along the ring.
        """
        with self._lock:
            if not self._ring or not self._nodes:
                return []

            target_count = count or min(self.replication_factor, len(self._nodes))
            key_hash = HashAlgorithms.fnv1a_64(key)

            # Binary search first token >= key_hash
            idx = bisect.bisect_left(self._ring, key_hash)
            if idx == len(self._ring):
                idx = 0  # Wrap around circle

            selected_nodes: List[PeerNode] = []
            seen_node_ids: Set[str] = set()

            # Walk clockwise along the ring
            for step in range(len(self._ring)):
                ring_idx = (idx + step) % len(self._ring)
                token = self._ring[ring_idx]
                node_id = self._token_to_node[token]

                if node_id not in seen_node_ids and node_id in self._nodes:
                    seen_node_ids.add(node_id)
                    selected_nodes.append(self._nodes[node_id])

                if len(selected_nodes) >= target_count:
                    break

            return selected_nodes

    def get_all_nodes(self) -> List[PeerNode]:
        with self._lock:
            return list(self._nodes.values())
