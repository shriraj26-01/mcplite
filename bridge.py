#!/usr/bin/env python3
"""
MCP Smart Bridge — Auto-starts orchestrator if not running.
Drop into kiro config and forget. Zero manual steps.

Flow:
  1. Kiro spawns this bridge
  2. Bridge checks if orchestrator socket exists
  3. If not → starts orchestrator daemon automatically
  4. Connects to orchestrator and relays MCP traffic

Usage in kiro config:
    "mongodb": {"command": "python3", "args": ["/path/to/bridge.py", "mongodb"]}
"""

import asyncio
import json
import os
import struct
import subprocess
import sys
import time
from pathlib import Path

ORCHESTRATOR_DIR = Path(__file__).parent
SOCKET_PATH = Path(os.environ.get("MCP_ORCH_SOCKET", "/tmp/mcp-orchestrator.sock"))
PID_FILE = Path("/tmp/mcp-orchestrator.pid")
ORCHESTRATOR_SCRIPT = ORCHESTRATOR_DIR / "orchestrator.py"
FRAME_HEADER = struct.Struct(">I")
MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_DELAYS = [0.5, 1.0, 1.5, 2.0, 3.0]


def is_orchestrator_running() -> bool:
    """Check if orchestrator is alive via PID file."""
    if not PID_FILE.exists():
        return False
    try:
        pid = int(PID_FILE.read_text().strip())
        os.kill(pid, 0)  # Signal 0 = check if alive
        return True
    except (ProcessLookupError, ValueError):
        # Stale PID — clean up
        PID_FILE.unlink(missing_ok=True)
        SOCKET_PATH.unlink(missing_ok=True)
        return False


def start_orchestrator():
    """Start orchestrator daemon if not already running."""
    if is_orchestrator_running():
        return True

    sys.stderr.write("[bridge] Starting orchestrator daemon...\n")
    try:
        subprocess.Popen(
            [sys.executable, str(ORCHESTRATOR_SCRIPT), "start"],
            cwd=str(ORCHESTRATOR_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Wait for socket to appear
        for _ in range(20):  # 10 seconds max
            time.sleep(0.5)
            if SOCKET_PATH.exists() and is_orchestrator_running():
                sys.stderr.write("[bridge] Orchestrator ready.\n")
                return True
        sys.stderr.write("[bridge] Orchestrator failed to start.\n")
        return False
    except Exception as e:
        sys.stderr.write(f"[bridge] Error starting orchestrator: {e}\n")
        return False


async def read_frame(reader: asyncio.StreamReader) -> dict | None:
    header = await reader.readexactly(4)
    length = FRAME_HEADER.unpack(header)[0]
    payload = await reader.readexactly(length)
    return json.loads(payload)


async def write_frame(writer: asyncio.StreamWriter, msg: dict):
    payload = json.dumps(msg, separators=(",", ":")).encode()
    writer.write(FRAME_HEADER.pack(len(payload)) + payload)
    await writer.drain()


async def connect_to_orchestrator(target: str):
    """Connect to orchestrator, auto-starting it if needed."""
    # Ensure orchestrator is running
    if not is_orchestrator_running():
        if not start_orchestrator():
            return None, None

    for attempt, delay in enumerate(RECONNECT_DELAYS):
        try:
            reader, writer = await asyncio.open_unix_connection(str(SOCKET_PATH))
            await write_frame(writer, {"type": "connect", "target": target})
            response = await asyncio.wait_for(read_frame(reader), timeout=10)
            if response and response.get("type") == "connected":
                return reader, writer
            else:
                err = response.get("msg", "Unknown error") if response else "No response"
                sys.stderr.write(f"[bridge] Error: {err}\n")
                writer.close()
                return None, None
        except (ConnectionRefusedError, FileNotFoundError, asyncio.TimeoutError, OSError):
            if attempt < len(RECONNECT_DELAYS) - 1:
                await asyncio.sleep(delay)

    sys.stderr.write(f"[bridge] Failed to connect after {MAX_RECONNECT_ATTEMPTS} attempts\n")
    return None, None


async def bridge(target: str):
    reader, writer = await connect_to_orchestrator(target)
    if not reader:
        sys.exit(1)

    async def stdin_to_socket():
        loop = asyncio.get_event_loop()
        stdin_reader = asyncio.StreamReader()
        await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(stdin_reader), sys.stdin.buffer
        )
        while True:
            line = await stdin_reader.readline()
            if not line:
                break
            data = line.rstrip(b"\n").decode("utf-8", errors="replace")
            if data:
                try:
                    await write_frame(writer, {"type": "mcp", "data": data})
                except (ConnectionResetError, BrokenPipeError):
                    break

    async def socket_to_stdout():
        while True:
            try:
                msg = await read_frame(reader)
            except (asyncio.IncompleteReadError, ConnectionResetError):
                break
            if msg is None:
                break

            msg_type = msg.get("type")
            if msg_type == "mcp":
                sys.stdout.buffer.write(msg["data"].encode() + b"\n")
                sys.stdout.buffer.flush()
            elif msg_type == "ping":
                try:
                    await write_frame(writer, {"type": "pong"})
                except (ConnectionResetError, BrokenPipeError):
                    break
            elif msg_type == "error":
                sys.stderr.write(f"[bridge] {msg.get('msg', 'error')}\n")

    try:
        await asyncio.gather(stdin_to_socket(), socket_to_stdout())
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        writer.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: bridge.py <server_name>", file=sys.stderr)
        sys.exit(1)
    try:
        asyncio.run(bridge(sys.argv[1]))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
