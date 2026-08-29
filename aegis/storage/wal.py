"""
Write-Ahead Log (WAL) Subsystem for Crash Resilience & Durability.
Logs every mutating transaction to disk before applying to in-memory tables.
Supports synchronous fsync, buffered logging, and deterministic recovery replay.
"""

from dataclasses import dataclass
import os
import struct
import threading
import time
from typing import Any, Callable, Iterator, List, Optional

from aegis.common.crypto import HashAlgorithms
from aegis.common.types import OpType


@dataclass
class WALRecord:
    """
    WAL Binary Record Layout:
    +-----------------------------------------------+
    | Record Length (4B: uint32)                    |
    +-----------------------------------------------+
    | CRC32 Checksum (4B: uint32)                   |
    +-----------------------------------------------+
    | Timestamp (8B: uint64 ns)                     |
    +-----------------------------------------------+
    | OpCode (1B: uint8)                            |
    +-----------------------------------------------+
    | Key Length (2B: uint16)                       |
    +-----------------------------------------------+
    | Key Bytes (var length)                        |
    +-----------------------------------------------+
    | Value Length (4B: uint32)                     |
    +-----------------------------------------------+
    | Value Bytes (var length)                      |
    +-----------------------------------------------+
    """
    op_type: OpType
    key: str
    value: Optional[str] = None
    timestamp_ns: int = 0

    HEADER_FORMAT = "!IIQBH"  # Total 19 bytes
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    OP_CODE_MAP = {
        OpType.PUT: 1,
        OpType.DELETE: 2,
        OpType.CAS: 3,
        OpType.TX_PREPARE: 4,
        OpType.TX_COMMIT: 5,
        OpType.TX_ABORT: 6,
    }
    INV_OP_CODE_MAP = {v: k for k, v in OP_CODE_MAP.items()}

    def serialize(self) -> bytes:
        if self.timestamp_ns == 0:
            self.timestamp_ns = time.time_ns()

        key_bytes = self.key.encode('utf-8')
        val_bytes = self.value.encode('utf-8') if self.value is not None else b""

        key_len = len(key_bytes)
        val_len = len(val_bytes)
        op_code = self.OP_CODE_MAP.get(self.op_type, 1)

        # Payload to checksum: Timestamp (8B) + OpCode (1B) + KeyLen (2B) + KeyBytes + ValLen (4B) + ValBytes
        payload_body = struct.pack(
            f"!QBH{key_len}sI{val_len}s",
            self.timestamp_ns,
            op_code,
            key_len,
            key_bytes,
            val_len,
            val_bytes
        )

        checksum = HashAlgorithms.crc32_custom(payload_body)
        total_rec_len = 4 + 4 + len(payload_body)  # Len field (4) + CRC (4) + body

        record_bytes = struct.pack("!II", total_rec_len, checksum) + payload_body
        return record_bytes


class WriteAheadLog:
    """
    Append-only crash recovery log manager.
    Guarantees ACID durability for LSM-Tree before memory state mutation.
    """

    def __init__(self, wal_path: str, sync_on_write: bool = True):
        self.wal_path = wal_path
        self.sync_on_write = sync_on_write
        self._file = None
        self._lock = threading.RLock()
        self._records_written = 0
        self._bytes_written = 0

        # Ensure directory exists
        os.makedirs(os.path.dirname(os.path.abspath(wal_path)), exist_ok=True)
        self._open()

    def _open(self):
        with self._lock:
            self._file = open(self.wal_path, "a+b")
            self._bytes_written = self._file.tell()

    def append(self, op_type: OpType, key: str, value: Optional[str] = None, timestamp_ns: int = 0) -> int:
        """
        Appends a record to the WAL.
        Returns the offset where the record was written.
        """
        record = WALRecord(
            op_type=op_type,
            key=key,
            value=value,
            timestamp_ns=timestamp_ns
        )
        data = record.serialize()

        with self._lock:
            offset = self._file.tell()
            self._file.write(data)
            if self.sync_on_write:
                self._file.flush()
                os.fsync(self._file.fileno())

            self._records_written += 1
            self._bytes_written += len(data)
            return offset

    def flush(self):
        with self._lock:
            if self._file and not self._file.closed:
                self._file.flush()
                os.fsync(self._file.fileno())

    def replay(self, on_record: Callable[[OpType, str, Optional[str], int], None]) -> int:
        """
        Replays all intact records from the WAL file to recover memory state on crash.
        Ignores truncated records at the tail with warning.
        Returns number of successfully replayed records.
        """
        if not os.path.exists(self.wal_path):
            return 0

        replayed_count = 0
        with open(self.wal_path, "rb") as f:
            while True:
                header_prefix = f.read(8)
                if len(header_prefix) < 8:
                    break  # Clean EOF

                total_len, checksum = struct.unpack("!II", header_prefix)
                body_len = total_len - 8

                if body_len <= 0:
                    break

                body = f.read(body_len)
                if len(body) < body_len:
                    # Incomplete tail write due to crash
                    break

                calc_checksum = HashAlgorithms.crc32_custom(body)
                if calc_checksum != checksum:
                    # Corrupted log record
                    break

                # Parse body
                ts, op_code, key_len = struct.unpack("!QBH", body[:11])
                key_bytes = body[11:11+key_len]
                val_len_offset = 11 + key_len
                val_len = struct.unpack("!I", body[val_len_offset:val_len_offset+4])[0]
                val_offset = val_len_offset + 4
                val_bytes = body[val_offset:val_offset+val_len]

                key = key_bytes.decode('utf-8', errors='replace')
                val = val_bytes.decode('utf-8', errors='replace') if val_len > 0 else None
                op_type = WALRecord.INV_OP_CODE_MAP.get(op_code, OpType.PUT)

                on_record(op_type, key, val, ts)
                replayed_count += 1

        return replayed_count

    def truncate(self):
        """Truncates WAL after a successful MemTable flush to SSTable."""
        with self._lock:
            if self._file and not self._file.closed:
                self._file.close()

            with open(self.wal_path, "wb") as f:
                f.truncate(0)

            self._open()
            self._records_written = 0
            self._bytes_written = 0

    def close(self):
        with self._lock:
            if self._file and not self._file.closed:
                self._file.flush()
                self._file.close()
