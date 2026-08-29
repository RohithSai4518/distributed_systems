"""
Unified Distributed Node Server.
Integrates all subsystems into a single cohesive, high-performance node:
- LSM Storage Engine (WAL + MemTable + SSTables)
- Custom Binary TCP Server & Client Connection Pool
- Raft Consensus State Machine & Election Engine
- SWIM Gossip Membership & Failure Detector
- Consistent Hash Ring & Partition Manager
- Two-Phase Commit Distributed Transactions
"""

import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from aegis.cluster.gossip import GossipManager
from aegis.cluster.ring import ConsistentHashRing
from aegis.common.clocks import HybridLogicalClock, VectorClock
from aegis.common.logger import Logger
from aegis.common.types import (
    ConsistencyLevel, LogEntry, MessageType, NodeState, NodeStatus,
    OpType, PeerNode, RPCMessage
)
from aegis.consensus.log_store import RaftLogStore
from aegis.consensus.raft import RaftNode
from aegis.consensus.state_machine import ReplicatedStateMachine
from aegis.network.client import TCPClient
from aegis.network.rpc import RPCEngine
from aegis.network.server import TCPServer
from aegis.network.transport import ChaosNetworkTransport
from aegis.storage.engine import LSMEngine
from aegis.tx.lock_manager import LockManager
from aegis.tx.two_phase_commit import (
    TwoPhaseCommitCoordinator, TwoPhaseCommitParticipant
)


class AegisNode:
    """
    Full distributed system node server.
    """

    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 9001,
        http_port: int = 8001,
        peers: Optional[Dict[str, PeerNode]] = None,
        data_root_dir: str = "./data"
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.http_port = http_port
        self.data_dir = os.path.join(data_root_dir, node_id)
        self.logger = Logger(node_id=node_id)

        self.self_peer = PeerNode(
            node_id=node_id,
            host=host,
            port=port,
            http_port=http_port,
            status=NodeStatus.ALIVE
        )

        self.peers = peers or {}

        # 1. Storage Engine
        self.storage = LSMEngine(data_dir=self.data_dir)

        # 2. Clocks
        self.hlc = HybridLogicalClock(node_id=node_id)
        self.vector_clock = VectorClock(node_id=node_id)

        # 3. Networking & RPC
        self.tcp_client = TCPClient(node_id=node_id)
        self.transport = ChaosNetworkTransport(node_id=node_id, tcp_client=self.tcp_client)
        self.rpc_engine = RPCEngine(node_id=node_id, transport=self.transport)
        self.tcp_server = TCPServer(host=host, port=port, node_id=node_id)

        # 4. State Machine & Raft Consensus
        self.state_machine = ReplicatedStateMachine(node_id=node_id, storage_engine=self.storage)
        self.log_store = RaftLogStore(log_dir=os.path.join(self.data_dir, "raft"), node_id=node_id)
        self.raft = RaftNode(
            node_id=node_id,
            peers=self.peers,
            rpc_engine=self.rpc_engine,
            log_store=self.log_store,
            state_machine=self.state_machine
        )

        # 5. Cluster & Hash Ring
        self.hash_ring = ConsistentHashRing(vnodes_per_node=32, replication_factor=3)
        self.hash_ring.add_node(self.self_peer)
        for p in self.peers.values():
            self.hash_ring.add_node(p)

        self.gossip = GossipManager(
            node_id=node_id,
            self_node=self.self_peer,
            rpc_engine=self.rpc_engine
        )
        for p in self.peers.values():
            self.gossip.add_member(p)

        # 6. Concurrency & Transactions
        self.lock_manager = LockManager(node_id=node_id)
        self.tx_coord = TwoPhaseCommitCoordinator(node_id=node_id, rpc_engine=self.rpc_engine, peers=self.peers)
        self.tx_part = TwoPhaseCommitParticipant(node_id=node_id, rpc_engine=self.rpc_engine, storage_apply_callback=self._apply_tx_op)

        # Register top-level TCP message handlers
        self._register_network_handlers()

        self._is_running = False

    def _register_network_handlers(self):
        self.tcp_server.register_handler(MessageType.CLIENT_REQUEST, self._handle_client_kv_request)
        self.tcp_server.register_handler(MessageType.CLUSTER_STATE_REQ, self._handle_cluster_state_req)
        self.tcp_server.register_handler(MessageType.REQUEST_VOTE, self.raft.handle_request_vote)
        self.tcp_server.register_handler(MessageType.APPEND_ENTRIES, self.raft.handle_append_entries)
        self.tcp_server.register_handler(MessageType.GOSSIP_PING, self.gossip._handle_gossip_ping)
        self.tcp_server.register_handler(MessageType.GOSSIP_PING_REQ, self.gossip._handle_gossip_ping_req)
        self.tcp_server.register_handler(MessageType.TX_PREPARE_REQ, self.tx_part._handle_prepare)
        self.tcp_server.register_handler(MessageType.TX_COMMIT_REQ, self.tx_part._handle_commit)
        self.tcp_server.register_handler(MessageType.TX_ABORT_REQ, self.tx_part._handle_abort)

    def _apply_tx_op(self, op: Dict[str, Any]):
        op_type = OpType(op.get("op_type", "PUT"))
        key = op.get("key", "")
        value = op.get("value")
        if op_type == OpType.PUT:
            self.storage.put(key, value)
        elif op_type == OpType.DELETE:
            self.storage.delete(key)

    def start(self):
        """Starts all node subsystems in proper dependency order."""
        self._is_running = True
        if self.storage.wal._file is None or self.storage.wal._file.closed:
            self.storage = LSMEngine(data_dir=self.data_dir)
            self.state_machine.storage = self.storage
        if not self.tcp_server._is_running:
            self.tcp_server.start()
            self._register_network_handlers()
        if not self.raft._is_running:
            self.raft.start()
        if not self.gossip._is_running:
            self.gossip.start()
        self.logger.info("Aegis Node %s fully online and operational.", self.node_id)

    def stop(self):
        self._is_running = False
        self.gossip.stop()
        self.raft.stop()
        self.tcp_server.stop()
        self.rpc_engine.shutdown()
        self.tcp_client.close()
        self.storage.close()
        self.logger.info("Aegis Node %s safely stopped.", self.node_id)

    # -------------------------------------------------------------------------
    # Client KV Request Handler (Get / Put / Delete / CAS / Scan)
    # -------------------------------------------------------------------------

    def _handle_client_kv_request(self, msg: RPCMessage) -> RPCMessage:
        payload = msg.payload
        op_name = payload.get("op", "GET")
        key = payload.get("key", "")
        value = payload.get("value")
        prev_val = payload.get("prev_val")
        consistency = ConsistencyLevel(payload.get("consistency", ConsistencyLevel.QUORUM.value))

        success = False
        result = None
        leader_hint = self.raft.leader_id

        if op_name == "GET":
            # Direct read or consensus read based on consistency level
            found, val = self.storage.get(key)
            success = True
            result = {"found": found, "key": key, "value": val}

        elif op_name in ("PUT", "DELETE", "CAS"):
            op_map = {"PUT": OpType.PUT, "DELETE": OpType.DELETE, "CAS": OpType.CAS}
            op_type = op_map[op_name]

            # Route through Raft consensus for linearizability
            success, result, leader_hint = self.raft.propose(
                op_type=op_type,
                key=key,
                value=value,
                prev_val=prev_val
            )

        elif op_name == "SCAN":
            start_k = payload.get("start_key", "")
            end_k = payload.get("end_key")
            limit = payload.get("limit", 100)
            items = self.storage.scan(start_k, end_k, limit)
            success = True
            result = {"items": items}

        return RPCMessage(
            msg_id=msg.msg_id,
            msg_type=MessageType.CLIENT_RESPONSE,
            sender_id=self.node_id,
            receiver_id=msg.sender_id,
            payload={
                "success": success,
                "result": result,
                "leader_id": leader_hint
            },
            is_response=True
        )

    def _handle_cluster_state_req(self, msg: RPCMessage) -> RPCMessage:
        return RPCMessage(
            msg_id=msg.msg_id,
            msg_type=MessageType.CLUSTER_STATE_RESP,
            sender_id=self.node_id,
            receiver_id=msg.sender_id,
            payload=self.get_telemetry(),
            is_response=True
        )

    def get_telemetry(self) -> Dict[str, Any]:
        """Provides complete real-time diagnostic telemetry for cluster visualizer."""
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "http_port": self.http_port,
            "raft": {
                "state": self.raft.state.value,
                "term": self.raft.current_term,
                "leader_id": self.raft.leader_id,
                "commit_index": self.raft.commit_index,
                "last_applied": self.raft.last_applied,
                "last_log_index": self.log_store.last_log_index(),
                "entries_count": len(self.log_store.entries) - 1,
            },
            "gossip": {
                "members": [m.to_dict() for m in self.gossip.get_alive_nodes()],
                "incarnation": self.gossip.incarnation
            },
            "storage": {
                "memtable_bytes": self.storage.active_memtable.size_bytes(),
                "memtable_count": self.storage.active_memtable.count(),
                "l0_sst_count": len(self.storage._l0_readers),
                "l1_sst_count": len(self.storage._l1_readers),
            },
            "timestamp_ns": time.time_ns()
        }
