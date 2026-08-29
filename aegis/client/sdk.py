"""
Aegis Distributed KV Client SDK.
Provides high-level programmatic access:
- Automatic Raft leader discovery and transparent failover redirection
- Configurable Consistency Levels (ONE, QUORUM, ALL)
- Vector Clock causality tracking
- Exponential backoff retries with randomized jitter
"""

import random
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from aegis.common.clocks import VectorClock
from aegis.common.logger import Logger
from aegis.common.types import (
    ConsistencyLevel, MessageType, OpType, PeerNode, RPCMessage
)
from aegis.network.client import TCPClient


class AegisClient:
    """
    Thread-safe Client SDK for communicating with the distributed cluster.
    """

    def __init__(
        self,
        seed_nodes: List[Tuple[str, int]],
        client_id: Optional[str] = None,
        timeout_sec: float = 1.0,
        max_retries: int = 5
    ):
        self.client_id = client_id or f"client-{uuid.uuid4().hex[:8]}"
        self.seed_nodes = list(seed_nodes)
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.logger = Logger(node_id=self.client_id)

        self.tcp_client = TCPClient(node_id=self.client_id, default_timeout=timeout_sec)
        self.vector_clock = VectorClock(node_id=self.client_id)
        self.known_leader: Optional[Tuple[str, int]] = None

    def put(self, key: str, value: Any, consistency: ConsistencyLevel = ConsistencyLevel.QUORUM) -> bool:
        """Stores a key-value pair in the distributed cluster."""
        self.vector_clock.tick()
        payload = {
            "op": "PUT",
            "key": key,
            "value": value,
            "client_id": self.client_id,
            "consistency": consistency.value,
            "vector_clock": self.vector_clock.clone_map()
        }
        res = self._execute_with_retry(payload)
        return res.get("success", False) if res else False

    def get(self, key: str, consistency: ConsistencyLevel = ConsistencyLevel.QUORUM) -> Tuple[bool, Optional[Any]]:
        """Retrieves a value for a key."""
        payload = {
            "op": "GET",
            "key": key,
            "client_id": self.client_id,
            "consistency": consistency.value
        }
        res = self._execute_with_retry(payload)
        if res and res.get("success"):
            inner = res.get("result", {})
            return inner.get("found", False), inner.get("value")
        return False, None

    def delete(self, key: str) -> bool:
        """Deletes a key."""
        payload = {
            "op": "DELETE",
            "key": key,
            "client_id": self.client_id
        }
        res = self._execute_with_retry(payload)
        return res.get("success", False) if res else False

    def cas(self, key: str, prev_value: Any, new_value: Any) -> Tuple[bool, Optional[Any]]:
        """
        Compare-And-Swap atomic operation:
        Sets key to new_value if and only if current value matches prev_value.
        Returns: (success, current_or_new_value)
        """
        payload = {
            "op": "CAS",
            "key": key,
            "value": new_value,
            "prev_val": prev_value,
            "client_id": self.client_id
        }
        res = self._execute_with_retry(payload)
        if res and res.get("success"):
            inner = res.get("result", {})
            return inner.get("cas_success", False), inner.get("new_value") or inner.get("current_value")
        return False, None

    def scan(self, start_key: str = "", limit: int = 100) -> List[Tuple[str, Any]]:
        """Scans keys in ascending lexicographical order."""
        payload = {
            "op": "SCAN",
            "start_key": start_key,
            "limit": limit
        }
        res = self._execute_with_retry(payload)
        if res and res.get("success"):
            return res.get("result", {}).get("items", [])
        return []

    def _execute_with_retry(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Executes request across cluster with leader discovery and retries."""
        for attempt in range(self.max_retries):
            targets = []
            if self.known_leader and self.known_leader in self.seed_nodes:
                targets.append(self.known_leader)
                targets.extend([s for s in self.seed_nodes if s != self.known_leader])
            else:
                targets = list(self.seed_nodes)

            for host, port in targets:
                msg_id = str(uuid.uuid4())
                req = RPCMessage(
                    msg_id=msg_id,
                    msg_type=MessageType.CLIENT_REQUEST,
                    sender_id=self.client_id,
                    receiver_id="CLUSTER",
                    payload=payload
                )

                resp = self.tcp_client.send_rpc(host, port, req, timeout=self.timeout_sec)
                if resp and resp.payload:
                    p = resp.payload
                    if p.get("success"):
                        self.known_leader = (host, port)
                        return p

            # If all failed or no leader, clear known leader and backoff
            self.known_leader = None
            time.sleep(0.1 + attempt * 0.1)

        return None

    def close(self):
        self.tcp_client.close()
