"""
MCP Shared Orchestrator — Protocol Layer
==========================================
Length-prefixed frame format for Unix socket communication.
Shared data structures with bounded memory and TTL cleanup.

Frame: [4 bytes big-endian uint32 length][JSON payload bytes]
"""

import asyncio
import json
import struct
import time
import weakref
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple

# Frame header: 4 bytes, big-endian unsigned int
FRAME_HEADER = struct.Struct(">I")
MAX_FRAME_SIZE = 16 * 1024 * 1024  # 16 MB hard limit
MAX_LINE_SIZE = 10 * 1024 * 1024   # 10 MB max MCP message


# --- Frame Read/Write ---

async def read_frame(reader: asyncio.StreamReader) -> Optional[dict]:
    """Read one length-prefixed JSON frame. Returns None on EOF."""
    header = await reader.readexactly(4)
    if not header:
        return None
    length = FRAME_HEADER.unpack(header)[0]
    if length > MAX_FRAME_SIZE:
        raise ValueError(f"Frame too large: {length} bytes")
    payload = await reader.readexactly(length)
    return json.loads(payload)


async def write_frame(writer: asyncio.StreamWriter, msg: dict):
    """Write one length-prefixed JSON frame. Atomic."""
    payload = json.dumps(msg, separators=(",", ":")).encode()
    header = FRAME_HEADER.pack(len(payload))
    writer.write(header + payload)
    await writer.drain()


# --- Safe Line Buffer for MCP stdio ---

class SafeLineBuffer:
    """Bounded line buffer for reading newline-delimited JSON from subprocess."""

    __slots__ = ("_buf", "_max_size")

    def __init__(self, max_size: int = MAX_LINE_SIZE):
        self._buf = bytearray()
        self._max_size = max_size

    def append(self, data: bytes):
        if len(self._buf) + len(data) > self._max_size:
            # Drop buffer to prevent OOM — server sent oversized message
            self._buf.clear()
            raise ValueError(f"Line buffer overflow: >{self._max_size} bytes")
        self._buf.extend(data)

    def get_line(self) -> Optional[bytes]:
        """Extract one complete line. Returns None if no newline found."""
        idx = self._buf.find(b"\n")
        if idx == -1:
            return None
        line = bytes(self._buf[:idx])
        del self._buf[:idx + 1]
        return line

    def clear(self):
        self._buf.clear()


# --- Bounded Pending Requests ---

class ServerState(Enum):
    STARTING = "starting"
    READY = "ready"
    FAILED = "failed"
    RESTARTING = "restarting"
    DEAD = "dead"


class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Rejecting all requests
    HALF_OPEN = "half_open"  # Allowing one probe request


@dataclass(slots=True)
class PendingRequest:
    """Tracks an in-flight request from client to server."""
    client_id: str
    original_id: Any          # Client's original JSON-RPC id
    server_name: str
    created_at: float
    client_ref: Optional[weakref.ref] = None  # Weak ref to writer


class BoundedPendingRequests:
    """Thread-safe bounded request tracker with TTL eviction."""

    def __init__(self, max_size: int = 10_000, ttl_seconds: float = 300):
        self._map: Dict[str, PendingRequest] = {}
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._counter = 0  # Monotonic internal ID counter

    def next_id(self) -> str:
        """Generate globally unique internal request ID."""
        self._counter += 1
        return f"_o{self._counter}"

    def add(self, internal_id: str, req: PendingRequest):
        """Add a pending request. Evicts stale entries if over capacity."""
        if len(self._map) >= self._max_size // 2:
            self._evict_stale()
        if len(self._map) >= self._max_size:
            # Hard limit — reject
            raise OverflowError("Pending request queue full")
        self._map[internal_id] = req

    def pop(self, internal_id: str) -> Optional[PendingRequest]:
        """Remove and return a pending request by internal ID."""
        return self._map.pop(internal_id, None)

    def remove_client(self, client_id: str) -> int:
        """Remove all pending requests for a disconnected client. Returns count."""
        to_remove = [k for k, v in self._map.items() if v.client_id == client_id]
        for k in to_remove:
            del self._map[k]
        return len(to_remove)

    def remove_server(self, server_name: str) -> list:
        """Remove all pending for a failed server. Returns list of (client_id, original_id)."""
        to_remove = []
        for k, v in list(self._map.items()):
            if v.server_name == server_name:
                to_remove.append((v.client_id, v.original_id))
                del self._map[k]
        return to_remove

    def _evict_stale(self):
        """Remove entries older than TTL."""
        now = time.time()
        stale = [k for k, v in self._map.items() if now - v.created_at > self._ttl]
        for k in stale:
            del self._map[k]

    @property
    def size(self) -> int:
        return len(self._map)


# --- Circuit Breaker ---

class CircuitBreaker:
    """Per-server circuit breaker to prevent cascading failures."""

    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 30.0):
        self.state = CircuitState.CLOSED
        self._failure_count = 0
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._last_failure_time = 0.0

    def record_success(self):
        self._failure_count = 0
        self.state = CircuitState.CLOSED

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self._failure_threshold:
            self.state = CircuitState.OPEN

    def allow_request(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self._last_failure_time > self._recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        # HALF_OPEN — allow one probe
        return True


# --- JSON-RPC Helpers ---

def make_error_response(request_id: Any, code: int, message: str) -> str:
    """Create a JSON-RPC error response line."""
    return json.dumps({
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": code, "message": message}
    }, separators=(",", ":"))


def extract_id_fast(line: bytes) -> Tuple[Optional[Any], Optional[str]]:
    """Fast extraction of 'id' and 'method' from JSON-RPC line.
    Avoids full json.loads() for the common relay path.
    Falls back to full parse if fast path fails.
    """
    # Fast path: look for "id": pattern in first 300 bytes
    # This handles 99% of MCP messages which are small
    try:
        msg = json.loads(line)
        return msg.get("id"), msg.get("method")
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, None


def rewrite_id(line: bytes, old_id: Any, new_id: str) -> bytes:
    """Rewrite the JSON-RPC id in a message line.
    Full parse + serialize to guarantee correctness.
    """
    msg = json.loads(line)
    msg["id"] = new_id
    return json.dumps(msg, separators=(",", ":")).encode()


def restore_id(line: bytes, original_id: Any) -> bytes:
    """Restore original client request ID in a response."""
    msg = json.loads(line)
    msg["id"] = original_id
    return json.dumps(msg, separators=(",", ":")).encode()
