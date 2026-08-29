"""
Custom Binary & JSON Frame Serialization Layer.
Provides high-throughput framing, header validation, CRC32 integrity checks,
and structured payload serialization with zero external dependencies.
"""

import json
import struct
from typing import Any, Dict, Optional, Tuple

from aegis.common.crypto import HashAlgorithms
from aegis.common.types import RPCMessage, MessageType


class SerializationError(Exception):
    """Raised when frame serialization or deserialization fails integrity checks."""
    pass


class FrameCodec:
    """
    Binary Wire Framing Format:
    +-------------------+-------------------+-------------------+-------------------+
    | Magic (4B: AEGS)  | Version (2B: 0x01)| Flags (2B: 0x00)  | MsgType (2B)      |
    +-------------------+-------------------+-------------------+-------------------+
    | Payload Length (4B: uint32)           | Checksum (4B: CRC32 of payload)   |
    +---------------------------------------+-----------------------------------+
    | Raw Payload Bytes (variable length, UTF-8 JSON or Binary)                  |
    +---------------------------------------------------------------------------+
    Total Header Size: 18 Bytes
    """

    MAGIC_HEADER = b"AEGS"
    PROTOCOL_VERSION = 1
    HEADER_STRUCT_FORMAT = "!4sHHHII"
    HEADER_SIZE = struct.calcsize(HEADER_STRUCT_FORMAT)  # 18 bytes

    FLAG_NONE = 0x0000
    FLAG_COMPRESSED = 0x0001
    FLAG_ENCRYPTED = 0x0002

    # Map MessageType enum to numeric OpCodes
    MSG_TYPE_TO_ID = {msg_type: idx for idx, msg_type in enumerate(MessageType, start=1)}
    ID_TO_MSG_TYPE = {idx: msg_type for idx, msg_type in enumerate(MessageType, start=1)}

    @classmethod
    def encode_message(cls, message: RPCMessage, flags: int = FLAG_NONE) -> bytes:
        """
        Serializes an RPCMessage into a framed byte stream with integrity checksum.
        """
        try:
            payload_dict = message.to_dict()
            json_bytes = json.dumps(payload_dict, separators=(',', ':')).encode('utf-8')
        except Exception as e:
            raise SerializationError(f"Failed to JSON encode message: {e}")

        payload_len = len(json_bytes)
        checksum = HashAlgorithms.crc32_custom(json_bytes)
        msg_type_id = cls.MSG_TYPE_TO_ID.get(message.msg_type, 0)

        header = struct.pack(
            cls.HEADER_STRUCT_FORMAT,
            cls.MAGIC_HEADER,
            cls.PROTOCOL_VERSION,
            flags,
            msg_type_id,
            payload_len,
            checksum
        )

        return header + json_bytes

    @classmethod
    def decode_header(cls, header_bytes: bytes) -> Tuple[int, int, MessageType, int, int]:
        """
        Parses and validates the 18-byte frame header.
        Returns: (version, flags, message_type, payload_length, expected_checksum)
        """
        if len(header_bytes) < cls.HEADER_SIZE:
            raise SerializationError(
                f"Header underflow: expected {cls.HEADER_SIZE} bytes, received {len(header_bytes)}"
            )

        magic, version, flags, type_id, payload_len, checksum = struct.unpack(
            cls.HEADER_STRUCT_FORMAT, header_bytes[:cls.HEADER_SIZE]
        )

        if magic != cls.MAGIC_HEADER:
            raise SerializationError(f"Invalid protocol magic: {magic}, expected {cls.MAGIC_HEADER}")

        if version != cls.PROTOCOL_VERSION:
            raise SerializationError(f"Unsupported protocol version: {version}")

        msg_type = cls.ID_TO_MSG_TYPE.get(type_id)
        if msg_type is None:
            raise SerializationError(f"Unknown message type opcode: {type_id}")

        return version, flags, msg_type, payload_len, checksum

    @classmethod
    def decode_payload(cls, payload_bytes: bytes, expected_checksum: int) -> RPCMessage:
        """
        Validates payload CRC32 checksum and deserializes into an RPCMessage.
        """
        actual_checksum = HashAlgorithms.crc32_custom(payload_bytes)
        if actual_checksum != expected_checksum:
            raise SerializationError(
                f"Payload checksum mismatch! Corrupt packet: computed {actual_checksum:#010x}, expected {expected_checksum:#010x}"
            )

        try:
            json_str = payload_bytes.decode('utf-8')
            msg_dict = json.loads(json_str)
            return RPCMessage.from_dict(msg_dict)
        except Exception as e:
            raise SerializationError(f"Failed to parse payload body: {e}")

    @classmethod
    def decode_frame(cls, frame_bytes: bytes) -> RPCMessage:
        """
        Decodes a full frame (header + payload) into an RPCMessage.
        """
        if len(frame_bytes) < cls.HEADER_SIZE:
            raise SerializationError("Frame too short to contain header")

        header_bytes = frame_bytes[:cls.HEADER_SIZE]
        _, _, _, payload_len, expected_checksum = cls.decode_header(header_bytes)

        if len(frame_bytes) < cls.HEADER_SIZE + payload_len:
            raise SerializationError(
                f"Frame payload truncated: expected {payload_len} bytes, got {len(frame_bytes) - cls.HEADER_SIZE}"
            )

        payload_bytes = frame_bytes[cls.HEADER_SIZE:cls.HEADER_SIZE + payload_len]
        return cls.decode_payload(payload_bytes, expected_checksum)
