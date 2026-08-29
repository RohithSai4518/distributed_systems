"""
Custom Network Protocol & Wire Contract.
Defines message dispatch contracts, connection state machines, and transport primitives.
"""

from typing import Any, Callable, Dict, Optional
from aegis.common.types import MessageType, RPCMessage

# Type alias for RPC handler functions
# signature: handler(message: RPCMessage) -> RPCMessage
RPCHandlerFunc = Callable[[RPCMessage], RPCMessage]


class NetworkProtocolConstants:
    DEFAULT_PORT = 9000
    DEFAULT_BUFFER_SIZE = 64 * 1024  # 64 KB
    DEFAULT_TIMEOUT_SEC = 5.0
    KEEP_ALIVE_INTERVAL_SEC = 1.0
    MAX_FRAME_SIZE = 16 * 1024 * 1024  # 16 MB max frame safety limit
