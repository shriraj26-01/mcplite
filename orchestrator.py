#!/usr/bin/env python3
"""
MCP Shared Orchestrator — Daemon
==================================
Runs ONE instance of each MCP server, multiplexes across unlimited kiro sessions.

Usage:
    python orchestrator.py start       # Background daemon
    python orchestrator.py stop        # Stop daemon
    python orchestrator.py status      # Health check
    python orchestrator.py foreground  # Debug mode
    python orchestrator.py restart <server>  # Restart specific server
"""

import asyncio
import fcntl
import gc
import json
import logging
import logging.handlers
import os
import signal
import sys
import time
import weakref
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from protocol import (
    BoundedPendingRequests,
    CircuitBreaker,
    CircuitState,
    PendingRequest,
    SafeLineBuffer,
    ServerState,
    extract_id_fast,
    make_error_response,
    restore_id,
    rewrite_id,
)

# --- Configuration ---

BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "config.json"
_runtime_dir = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
SOCKET_DIR = Path(os.environ.get("MCP_ORCH_SOCKET_DIR", f"{_runtime_dir}/mcp-orchestrator"))
PID_FILE = SOCKET_DIR / "orchestrator.pid"
LOG_FILE = Path("/tmp/mcp-orchestrator.log")

# Tuning
HEARTBEAT_INTERVAL = 30
REQUEST_TIMEOUT = 90
MAX_RESTART_ATTEMPTS = 5
RESTART_BACKOFF_BASE = 1.0
RESTART_BACKOFF_MAX = 30.0
IDLE_CLIENT_TIMEOUT = 3600  # 1 hour
MEMORY_WARN_MB = 40
MEMORY_CRITICAL_MB = 80
GC_INTERVAL = 300  # 5 minutes
MAX_SERVER_RSS_MB = 1024  # Auto-restart server if RSS exceeds this

# --- Helpers ---

def _get_tree_rss_kb(pid: int) -> int:
    """Get total RSS of a process and all descendants via /proc."""
    import os
    # Build parent→children map from /proc
    children_map: Dict[int, List[int]] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as f:
                parts = f.read().split()
                ppid = int(parts[3])
                children_map.setdefault(ppid, []).append(int(entry))
        except (FileNotFoundError, ValueError, IndexError):
            pass
    # Walk tree from pid
    total = 0
    stack = [pid]
    while stack:
        p = stack.pop()
        try:
            with open(f"/proc/{p}/statm") as f:
                total += int(f.read().split()[1]) * 4  # pages → KB
        except (FileNotFoundError, ValueError):
            pass
        stack.extend(children_map.get(p, []))
    return total


# --- Logging ---

def setup_logging(foreground: bool = False):
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers = []
    if foreground:
        handlers.append(logging.StreamHandler())
    else:
        h = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5)
        handlers.append(h)
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)


log = logging.getLogger("orchestrator")


# --- MCP Server Process ---

@dataclass
class MCPServer:
    name: str
    command: List[str]
    env: Dict[str, str] = field(default_factory=dict)
    state: ServerState = ServerState.STARTING
    process: Optional[asyncio.subprocess.Process] = None
    circuit: CircuitBreaker = field(default_factory=CircuitBreaker)
    init_cache: Optional[bytes] = None  # Cached initialize response
    restart_count: int = 0
    _stdin_queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=100))
    _buffer: SafeLineBuffer = field(default_factory=SafeLineBuffer)
    _reader_task: Optional[asyncio.Task] = None
    _writer_task: Optional[asyncio.Task] = None
    _stderr_task: Optional[asyncio.Task] = None


class Orchestrator:
    def __init__(self, config_path: Path):
        self.servers: Dict[str, MCPServer] = {}
        self.clients: Dict[str, "ClientConnection"] = {}
        self.pending = BoundedPendingRequests(max_size=10_000, ttl_seconds=REQUEST_TIMEOUT)
        self._running = True
        self._pid_fd: Optional[int] = None
        self._load_config(config_path)

    def _load_config(self, path: Path):
        with open(path) as f:
            config = json.load(f)
        for name, cfg in config["servers"].items():
            cmd = cfg["command"] if isinstance(cfg["command"], list) else cfg["command"].split()
            if "args" in cfg:
                cmd = [cfg["command"]] + cfg["args"]
            self.servers[name] = MCPServer(
                name=name,
                command=cmd,
                env=cfg.get("env", {}),
            )
        log.info(f"Loaded {len(self.servers)} servers: {list(self.servers.keys())}")

    # --- Server Process Management ---

    async def start_server(self, server: MCPServer):
        """Spawn a single MCP server subprocess."""
        env = {**os.environ, **server.env}
        try:
            server.process = await asyncio.create_subprocess_exec(
                *server.command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                start_new_session=True,  # Own process group for clean kill
            )
            server.state = ServerState.READY
            server.restart_count = 0
            server._buffer.clear()
            log.info(f"[{server.name}] Started (PID {server.process.pid})")

            # Concurrent reader/writer/stderr — prevents pipe deadlock
            server._reader_task = asyncio.create_task(self._server_reader(server))
            server._writer_task = asyncio.create_task(self._server_writer(server))
            server._stderr_task = asyncio.create_task(self._server_stderr(server))

        except Exception as e:
            log.error(f"[{server.name}] Failed to start: {e}")
            server.state = ServerState.FAILED
            server.circuit.record_failure()

    async def _server_reader(self, server: MCPServer):
        """Read stdout from MCP server, route responses to clients."""
        while self._running and server.process and server.process.stdout:
            try:
                chunk = await server.process.stdout.read(16384)
                if not chunk:
                    break
                try:
                    server._buffer.append(chunk)
                except ValueError:
                    log.warning(f"[{server.name}] Buffer overflow, reset")
                    continue

                while True:
                    line = server._buffer.get_line()
                    if line is None:
                        break
                    await self._route_server_response(server, line)

            except (asyncio.CancelledError, asyncio.IncompleteReadError):
                break
            except Exception as e:
                log.error(f"[{server.name}] Reader error: {e}")
                break

        # Server died
        if self._running:
            log.warning(f"[{server.name}] Process ended")
            await self._handle_server_death(server)

    async def _server_writer(self, server: MCPServer):
        """Write requests from queue to server stdin. Ensures atomic line writes."""
        while self._running and server.process and server.process.stdin:
            try:
                data = await server._stdin_queue.get()
                server.process.stdin.write(data + b"\n")
                await server.process.stdin.drain()
            except (asyncio.CancelledError, BrokenPipeError, ConnectionResetError):
                break
            except Exception as e:
                log.error(f"[{server.name}] Writer error: {e}")
                break

    async def _server_stderr(self, server: MCPServer):
        """Drain stderr to prevent 64KB pipe buffer deadlock."""
        while self._running and server.process and server.process.stderr:
            try:
                chunk = await server.process.stderr.read(4096)
                if not chunk:
                    break
                # Log first 200 chars of stderr for debugging
                text = chunk.decode("utf-8", errors="replace")[:200]
                log.debug(f"[{server.name}] stderr: {text}")
            except (asyncio.CancelledError, asyncio.IncompleteReadError):
                break

    async def _handle_server_death(self, server: MCPServer):
        """Handle server process exit — notify clients, attempt restart."""
        server.state = ServerState.FAILED
        server.circuit.record_failure()
        server.init_cache = None  # Invalidate

        # Fail all pending requests for this server
        orphans = self.pending.remove_server(server.name)
        for client_id, original_id in orphans:
            await self._send_error_to_client(client_id, original_id, -32001, f"{server.name} crashed")

        # Kill entire process tree (npm → sh → node)
        if server.process:
            try:
                os.killpg(os.getpgid(server.process.pid), signal.SIGTERM)
                await asyncio.wait_for(server.process.wait(), timeout=5)
            except (asyncio.TimeoutError, ProcessLookupError, OSError):
                try:
                    os.killpg(os.getpgid(server.process.pid), signal.SIGKILL)
                    await server.process.wait()
                except (ProcessLookupError, OSError):
                    pass

        # Attempt restart with backoff
        await self._restart_server(server)

    async def _restart_server(self, server: MCPServer):
        """Restart with exponential backoff + jitter."""
        server.restart_count += 1
        if server.restart_count > MAX_RESTART_ATTEMPTS:
            server.state = ServerState.DEAD
            log.error(f"[{server.name}] DEAD after {MAX_RESTART_ATTEMPTS} restart attempts")
            return

        backoff = min(RESTART_BACKOFF_BASE * (2 ** (server.restart_count - 1)), RESTART_BACKOFF_MAX)
        jitter = backoff * 0.3 * (2 * (hash(time.time()) % 100) / 100 - 1)  # ±30%
        delay = backoff + jitter

        server.state = ServerState.RESTARTING
        log.info(f"[{server.name}] Restarting in {delay:.1f}s (attempt {server.restart_count})")
        await asyncio.sleep(delay)

        if self._running:
            await self.start_server(server)

    async def send_to_server(self, server: MCPServer, data: bytes):
        """Queue data for server stdin. Non-blocking with backpressure."""
        if server.state not in (ServerState.READY, ServerState.STARTING):
            raise ConnectionError(f"{server.name} is {server.state.value}")
        try:
            server._stdin_queue.put_nowait(data)
        except asyncio.QueueFull:
            raise OverflowError(f"{server.name} stdin queue full (backpressure)")

    # --- Response Routing ---

    async def _route_server_response(self, server: MCPServer, line: bytes):
        """Route a response from server to the correct client."""
        msg_id, method = extract_id_fast(line)

        if msg_id is not None:
            # Response to a request — route to originating client
            internal_id = str(msg_id)
            req = self.pending.pop(internal_id)
            if req:
                server.circuit.record_success()
                # Check if this is an initialize response to cache
                if req.original_id is not None:
                    restored = restore_id(line, req.original_id)
                    # Cache init response
                    if server.init_cache is None and b'"serverInfo"' in line:
                        server.init_cache = restored
                    await self._send_to_client(req.client_id, restored)
            else:
                log.debug(f"[{server.name}] Response for unknown ID: {internal_id}")
        else:
            # Notification from server — broadcast to all clients connected to this server
            for client in list(self.clients.values()):
                if client.target_server == server.name:
                    await self._send_to_client(client.id, line)

    async def _send_to_client(self, client_id: str, data: bytes):
        """Send MCP data line to a client."""
        await self._send_line_to_client(client_id, data)

    async def _send_line_to_client(self, client_id: str, data: bytes):
        """Send raw MCP JSON line to a client via socket."""
        client = self.clients.get(client_id)
        if not client:
            return
        try:
            client.writer.write(data + b"\n")
            await client.writer.drain()
            client.last_active = time.time()
        except (ConnectionResetError, BrokenPipeError, OSError):
            await self._disconnect_client(client_id)

    async def _send_error_to_client(self, client_id: str, request_id: Any, code: int, message: str):
        """Send a JSON-RPC error response to a client."""
        error_line = make_error_response(request_id, code, message).encode()
        await self._send_line_to_client(client_id, error_line)

    # --- Client Connection Handling (per-server socket, raw JSON\n) ---

    async def handle_client(self, server: MCPServer, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a bridge connection to a specific server. Raw JSON\n protocol."""
        client_id = f"c{id(writer) % 100000}"
        client = ClientConnection(id=client_id, reader=reader, writer=writer, target_server=server.name)
        self.clients[client_id] = client
        log.info(f"Client {client_id} → {server.name} (total: {len(self.clients)})")

        buf = SafeLineBuffer()
        try:
            while self._running:
                try:
                    chunk = await reader.read(16384)
                except asyncio.CancelledError:
                    break
                if not chunk:
                    break

                try:
                    buf.append(chunk)
                except ValueError:
                    continue

                while True:
                    line = buf.get_line()
                    if line is None:
                        break
                    await self._handle_mcp_line(client, server, line)

        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, OSError):
            pass
        except Exception as e:
            log.error(f"Client {client_id} error: {e}")
        finally:
            await self._disconnect_client(client_id)

    async def _handle_mcp_line(self, client: "ClientConnection", server: MCPServer, line: bytes):
        """Handle a raw MCP JSON-RPC line from a bridge client."""
        # Circuit breaker check
        if not server.circuit.allow_request():
            msg_id, _ = extract_id_fast(line)
            if msg_id is not None:
                error = make_error_response(msg_id, -32002, f"{server.name} unavailable").encode()
                await self._send_line_to_client(client.id, error)
            return

        msg_id, method = extract_id_fast(line)

        # Handle initialize — use cache if available
        if method == "initialize" and server.init_cache is not None:
            cached = server.init_cache
            if msg_id is not None:
                cached = restore_id(cached, msg_id)
            await self._send_line_to_client(client.id, cached)
            return

        if msg_id is not None:
            # Request — needs ID rewriting
            internal_id = self.pending.next_id()
            req = PendingRequest(
                client_id=client.id,
                original_id=msg_id,
                server_name=server.name,
                created_at=time.time(),
                client_ref=weakref.ref(client.writer),
            )
            try:
                self.pending.add(internal_id, req)
            except OverflowError:
                error = make_error_response(msg_id, -32005, "Server overloaded").encode()
                await self._send_line_to_client(client.id, error)
                return

            rewritten = rewrite_id(line, msg_id, internal_id)
            try:
                await self.send_to_server(server, rewritten)
            except (ConnectionError, OverflowError) as e:
                self.pending.pop(internal_id)
                error = make_error_response(msg_id, -32002, str(e)).encode()
                await self._send_line_to_client(client.id, error)
        else:
            # Notification — forward directly
            try:
                await self.send_to_server(server, line)
            except (ConnectionError, OverflowError):
                pass

        client.last_active = time.time()

    async def _disconnect_client(self, client_id: str):
        """Clean up a disconnected client."""
        client = self.clients.pop(client_id, None)
        if not client:
            return
        removed = self.pending.remove_client(client_id)
        if removed:
            log.debug(f"Client {client_id}: dropped {removed} pending requests")
        try:
            client.writer.close()
            await asyncio.wait_for(client.writer.wait_closed(), timeout=5)
        except Exception:
            pass
        log.info(f"Client {client_id} disconnected (total: {len(self.clients)})")

    # --- Background Tasks ---

    async def _heartbeat_loop(self):
        """Check for dead client connections."""
        while self._running:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            for client in list(self.clients.values()):
                # Check if writer is dead
                if client.writer.is_closing():
                    await self._disconnect_client(client.id)

    async def _request_timeout_loop(self):
        """Clean up timed-out requests."""
        while self._running:
            await asyncio.sleep(30)
            now = time.time()
            self.pending._evict_stale()

    async def _gc_loop(self):
        """Periodic garbage collection for long-running daemon."""
        while self._running:
            await asyncio.sleep(GC_INTERVAL)
            gc.collect(2)
            # Memory check
            try:
                with open("/proc/self/statm") as f:
                    pages = int(f.read().split()[1])  # RSS in pages
                rss_mb = pages * 4096 / (1024 * 1024)
                if rss_mb > MEMORY_CRITICAL_MB:
                    log.warning(f"Memory critical: {rss_mb:.0f} MB — forcing GC")
                    gc.collect(2)
                elif rss_mb > MEMORY_WARN_MB:
                    log.info(f"Memory warning: {rss_mb:.0f} MB")
            except Exception:
                pass

    async def _server_health_loop(self):
        """Monitor server process liveness and memory usage."""
        while self._running:
            await asyncio.sleep(10)
            for server in self.servers.values():
                if server.state == ServerState.READY and server.process:
                    if server.process.returncode is not None:
                        log.warning(f"[{server.name}] Died with code {server.process.returncode}")
                        await self._handle_server_death(server)
                        continue
                    # RSS watchdog: check full process tree via /proc (no subprocess)
                    try:
                        tree_rss_kb = _get_tree_rss_kb(server.process.pid)
                        rss_mb = tree_rss_kb / 1024
                        if rss_mb > MAX_SERVER_RSS_MB:
                            log.warning(f"[{server.name}] Tree RSS {rss_mb:.0f}MB > {MAX_SERVER_RSS_MB}MB, restarting")
                            await self._handle_server_death(server)
                    except (FileNotFoundError, ProcessLookupError):
                        pass

    # --- Main Run ---

    async def run(self):
        """Start orchestrator — main entry point."""
        # Create socket directory
        SOCKET_DIR.mkdir(parents=True, exist_ok=True)
        # Clean old sockets
        for f in SOCKET_DIR.glob("*.sock"):
            f.unlink()

        # Start all MCP servers
        for server in self.servers.values():
            await self.start_server(server)

        # Create per-server listening sockets
        old_umask = os.umask(0o177)
        unix_servers = []
        for server in self.servers.values():
            sock_path = SOCKET_DIR / f"{server.name}.sock"
            handler = lambda r, w, s=server: self.handle_client(s, r, w)
            srv = await asyncio.start_unix_server(handler, path=str(sock_path))
            unix_servers.append(srv)
            log.info(f"[{server.name}] Listening on {sock_path}")
        os.umask(old_umask)

        log.info(f"Orchestrator ready | Dir: {SOCKET_DIR} | Servers: {len(self.servers)}")

        # Background tasks
        tasks = [
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._request_timeout_loop()),
            asyncio.create_task(self._gc_loop()),
            asyncio.create_task(self._server_health_loop()),
        ]

        try:
            # Keep running until stopped
            stop_event = asyncio.Event()
            await stop_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            for t in tasks:
                t.cancel()
            for srv in unix_servers:
                srv.close()
            await self._shutdown()

    async def _shutdown(self):
        """Graceful shutdown."""
        log.info("Shutting down...")

        # Disconnect clients
        for cid in list(self.clients.keys()):
            await self._disconnect_client(cid)

        # Stop servers
        for server in self.servers.values():
            if server.process and server.process.returncode is None:
                try:
                    os.killpg(server.process.pid, signal.SIGTERM)
                    await asyncio.wait_for(server.process.wait(), timeout=5)
                except (ProcessLookupError, asyncio.TimeoutError):
                    try:
                        os.killpg(server.process.pid, signal.SIGKILL)
                        await server.process.wait()
                    except ProcessLookupError:
                        pass
            # Cancel I/O tasks
            for task in (server._reader_task, server._writer_task, server._stderr_task):
                if task:
                    task.cancel()

        # Cleanup
        for f in SOCKET_DIR.glob("*.sock"):
            f.unlink()
        if self._pid_fd:
            os.close(self._pid_fd)
        PID_FILE.unlink(missing_ok=True)
        log.info("Shutdown complete")


@dataclass
class ClientConnection:
    id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    target_server: Optional[str] = None
    last_active: float = field(default_factory=time.time)
    last_pong: float = field(default_factory=time.time)


# --- Daemon Management ---

def acquire_lock() -> int:
    """Acquire exclusive PID file lock. Returns fd (keep open!)."""
    SOCKET_DIR.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(PID_FILE), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        print(f"Another orchestrator is already running (lock on {PID_FILE})")
        sys.exit(1)
    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode())
    os.fsync(fd)
    return fd


def daemonize() -> bool:
    """Fork to background. Returns True in child, False in parent."""
    if os.fork() > 0:
        return False
    os.setsid()
    if os.fork() > 0:
        sys.exit(0)
    # Redirect stdio
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)
    os.close(devnull)
    return True


def setup_signals(loop: asyncio.AbstractEventLoop):
    """Handle shutdown signals."""
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: loop.stop())


def cmd_start():
    if not CONFIG_FILE.exists():
        print(f"Config not found: {CONFIG_FILE}")
        print("Create config.json with server definitions.")
        sys.exit(1)

    if not daemonize():
        print(f"MCP Orchestrator started")
        print(f"  Sockets: {SOCKET_DIR}/")
        print(f"  Config: {CONFIG_FILE}")
        print(f"  Log:    {LOG_FILE}")
        return

    setup_logging(foreground=False)
    pid_fd = acquire_lock()
    gc.set_threshold(1000, 15, 5)

    orch = Orchestrator(CONFIG_FILE)
    orch._pid_fd = pid_fd

    loop = asyncio.new_event_loop()
    setup_signals(loop)
    try:
        loop.run_until_complete(orch.run())
    except (KeyboardInterrupt, SystemExit):
        loop.run_until_complete(orch._shutdown())
    finally:
        loop.close()


def cmd_foreground():
    if not CONFIG_FILE.exists():
        print(f"Config not found: {CONFIG_FILE}")
        sys.exit(1)

    setup_logging(foreground=True)
    pid_fd = acquire_lock()
    gc.set_threshold(1000, 15, 5)

    orch = Orchestrator(CONFIG_FILE)
    orch._pid_fd = pid_fd

    loop = asyncio.new_event_loop()
    setup_signals(loop)
    try:
        loop.run_until_complete(orch.run())
    except KeyboardInterrupt:
        loop.run_until_complete(orch._shutdown())
    finally:
        loop.close()


def cmd_stop():
    if not PID_FILE.exists():
        print("Not running.")
        return
    pid = int(PID_FILE.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Stopped (PID {pid})")
    except ProcessLookupError:
        print("Not running (stale PID)")
    PID_FILE.unlink(missing_ok=True)
    for f in SOCKET_DIR.glob("*.sock"):
        f.unlink()


def cmd_status():
    if not PID_FILE.exists():
        print("NOT RUNNING")
        return
    pid = int(PID_FILE.read_text().strip())
    try:
        os.kill(pid, 0)
        print(f"RUNNING (PID {pid})")
        print(f"  Sockets: {SOCKET_DIR}/")
        print(f"  Config: {CONFIG_FILE}")
        if CONFIG_FILE.exists():
            with open(CONFIG_FILE) as f:
                cfg = json.load(f)
            print(f"  Servers: {list(cfg['servers'].keys())}")
    except ProcessLookupError:
        print("STALE (cleaning up)")
        PID_FILE.unlink()
        for f in SOCKET_DIR.glob("*.sock"):
            f.unlink()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: orchestrator.py {start|stop|status|foreground}")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "start":
        cmd_start()
    elif cmd == "stop":
        cmd_stop()
    elif cmd == "status":
        cmd_status()
    elif cmd == "foreground":
        cmd_foreground()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
