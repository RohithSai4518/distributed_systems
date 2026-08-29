"""
Distributed Partitioned Pub/Sub Broker Engine
Implements Log-Centric Publish-Subscribe with Consumer Groups, Offsets, and Heartbeats.
"""

import threading
import time
import collections
from typing import Dict, List, Optional, Tuple, Any

class MessageRecord:
    def __init__(self, offset: int, key: str, value: Any, timestamp: float):
        self.offset = offset
        self.key = key
        self.value = value
        self.timestamp = timestamp


class TopicPartition:
    def __init__(self, topic: str, partition_id: int):
        self.topic = topic
        self.partition_id = partition_id
        self.messages: List[MessageRecord] = []
        self.lock = threading.RLock()

    def append(self, key: str, value: Any) -> int:
        with self.lock:
            offset = len(self.messages)
            rec = MessageRecord(offset, key, value, time.time())
            self.messages.append(rec)
            return offset

    def read_from(self, start_offset: int, max_records: int = 100) -> List[MessageRecord]:
        with self.lock:
            if start_offset >= len(self.messages):
                return []
            return self.messages[start_offset: start_offset + max_records]


class PubSubBroker:
    """High-throughput multi-topic partitioned message broker."""
    def __init__(self, num_partitions_default: int = 4):
        self.default_partitions = num_partitions_default
        self.topics: Dict[str, List[TopicPartition]] = {}
        self.consumer_offsets: Dict[str, Dict[Tuple[str, int], int]] = {}  # group -> (topic, part) -> offset
        self.lock = threading.RLock()

    def create_topic(self, topic: str, partitions: Optional[int] = None):
        with self.lock:
            if topic not in self.topics:
                p_count = partitions or self.default_partitions
                self.topics[topic] = [TopicPartition(topic, i) for i in range(p_count)]

    def publish(self, topic: str, key: str, value: Any) -> Tuple[int, int]:
        self.create_topic(topic)
        part_list = self.topics[topic]
        # Key hash partition routing
        part_idx = hash(key) % len(part_list)
        offset = part_list[part_idx].append(key, value)
        return part_idx, offset

    def fetch(self, group_id: str, topic: str, partition_id: int, max_records: int = 50) -> List[MessageRecord]:
        with self.lock:
            self.create_topic(topic)
            group_map = self.consumer_offsets.setdefault(group_id, {})
            current_off = group_map.get((topic, partition_id), 0)
            part = self.topics[topic][partition_id]
            records = part.read_from(current_off, max_records)
            if records:
                group_map[(topic, partition_id)] = records[-1].offset + 1
            return records
