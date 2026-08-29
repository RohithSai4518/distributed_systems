"""
Aegis Distributed Systems Core Types
Zero-dependency, production-grade distributed consensus, storage, and networking types.
"""

from dataclasses import dataclass, field
from enum import Enum, auto
import time
from typing import Any, Dict, List, Optional, Tuple, Union


class NodeState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"
    DEAD = "DEAD"


class NodeStatus(Enum):
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"
    LEFT = "LEFT"


class OpType(Enum):
    PUT = "PUT"
    GET = "GET"
    DELETE = "DELETE"
    CAS = "CAS"          # Compare-And-Swap
    TX_PREPARE = "TX_PREPARE"
    TX_COMMIT = "TX_COMMIT"
    TX_ABORT = "TX_ABORT"
    NOOP = "NOOP"


class ConsistencyLevel(Enum):
    ONE = 1
    QUORUM = 2
    ALL = 3


class IsolationLevel(Enum):
    READ_UNCOMMITTED = "READ_UNCOMMITTED"
    READ_COMMITTED = "READ_COMMITTED"
    REPEATABLE_READ = "REPEATABLE_READ"
    SNAPSHOT_ISOLATION = "SNAPSHOT_ISOLATION"
    SERIALIZABLE = "SERIALIZABLE"


class MessageType(Enum):
    # Consensus / Raft
    REQUEST_VOTE = "REQUEST_VOTE"
    REQUEST_VOTE_RESP = "REQUEST_VOTE_RESP"
    APPEND_ENTRIES = "APPEND_ENTRIES"
    APPEND_ENTRIES_RESP = "APPEND_ENTRIES_RESP"
    INSTALL_SNAPSHOT = "INSTALL_SNAPSHOT"
    INSTALL_SNAPSHOT_RESP = "INSTALL_SNAPSHOT_RESP"

    # Client KV Operations
    CLIENT_REQUEST = "CLIENT_REQUEST"
    CLIENT_RESPONSE = "CLIENT_RESPONSE"

    # Gossip & Membership
    GOSSIP_PING = "GOSSIP_PING"
    GOSSIP_ACK = "GOSSIP_ACK"
    GOSSIP_PING_REQ = "GOSSIP_PING_REQ"
    GOSSIP_SYNC = "GOSSIP_SYNC"

    # 2PC Transaction Messages
    TX_PREPARE_REQ = "TX_PREPARE_REQ"
    TX_PREPARE_RESP = "TX_PREPARE_RESP"
    TX_COMMIT_REQ = "TX_COMMIT_REQ"
    TX_COMMIT_RESP = "TX_COMMIT_RESP"
    TX_ABORT_REQ = "TX_ABORT_REQ"
    TX_ABORT_RESP = "TX_ABORT_RESP"

    # Cluster Heartbeat / Health
    HEARTBEAT = "HEARTBEAT"
    HEARTBEAT_RESP = "HEARTBEAT_RESP"
    CLUSTER_STATE_REQ = "CLUSTER_STATE_REQ"
    CLUSTER_STATE_RESP = "CLUSTER_STATE_RESP"


@dataclass
class LogEntry:
    term: int
    index: int
    op_type: OpType
    key: str
    value: Optional[Any] = None
    prev_value: Optional[Any] = None  # For CAS or undo logs
    client_id: str = ""
    sequence_num: int = 0
    timestamp_ns: int = field(default_factory=lambda: time.time_ns())
    vector_clock: Dict[str, int] = field(default_factory=dict)
    tx_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "index": self.index,
            "op_type": self.op_type.value,
            "key": self.key,
            "value": self.value,
            "prev_value": self.prev_value,
            "client_id": self.client_id,
            "sequence_num": self.sequence_num,
            "timestamp_ns": self.timestamp_ns,
            "vector_clock": self.vector_clock,
            "tx_id": self.tx_id,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LogEntry":
        return cls(
            term=d["term"],
            index=d["index"],
            op_type=OpType(d["op_type"]),
            key=d["key"],
            value=d.get("value"),
            prev_value=d.get("prev_value"),
            client_id=d.get("client_id", ""),
            sequence_num=d.get("sequence_num", 0),
            timestamp_ns=d.get("timestamp_ns", 0),
            vector_clock=d.get("vector_clock", {}),
            tx_id=d.get("tx_id"),
        )


@dataclass
class PeerNode:
    node_id: str
    host: str
    port: int
    http_port: int
    status: NodeStatus = NodeStatus.ALIVE
    incarnation: int = 0
    last_seen_ts: float = field(default_factory=time.time)

    def address(self) -> Tuple[str, int]:
        return (self.host, self.port)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "http_port": self.http_port,
            "status": self.status.value,
            "incarnation": self.incarnation,
            "last_seen_ts": self.last_seen_ts,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PeerNode":
        return cls(
            node_id=d["node_id"],
            host=d["host"],
            port=d["port"],
            http_port=d.get("http_port", d["port"] + 1000),
            status=NodeStatus(d.get("status", "ALIVE")),
            incarnation=d.get("incarnation", 0),
            last_seen_ts=d.get("last_seen_ts", time.time()),
        )


@dataclass
class RPCMessage:
    msg_id: str
    msg_type: MessageType
    sender_id: str
    receiver_id: str
    term: int = 0
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp_ns: int = field(default_factory=lambda: time.time_ns())
    is_response: bool = False
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "msg_id": self.msg_id,
            "msg_type": self.msg_type.value,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "term": self.term,
            "payload": self.payload,
            "timestamp_ns": self.timestamp_ns,
            "is_response": self.is_response,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "RPCMessage":
        return cls(
            msg_id=d["msg_id"],
            msg_type=MessageType(d["msg_type"]),
            sender_id=d["sender_id"],
            receiver_id=d["receiver_id"],
            term=d.get("term", 0),
            payload=d.get("payload", {}),
            timestamp_ns=d.get("timestamp_ns", 0),
            is_response=d.get("is_response", False),
            error=d.get("error"),
        )
