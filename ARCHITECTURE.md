# MCP Shared Orchestrator — Production Architecture (v2)

> Synthesized from 3 independent design reviews: Systems Architect, SRE, Memory Specialist

---

## Executive Summary

| Metric | Before (5 terminals) | After (orchestrator) |
|--------|----------------------|----------------------|
| Processes | 35 MCP servers | 7 servers + 1 daemon |
| Memory | ~10 GB (RAM + swap) | ~1.5 GB |
| Startup per terminal | ~5s (spawn 7 procs) | ~100ms (socket connect) |
| Max terminals | Limited by RAM | Unlimited (~2MB each) |

---

## Protocol (Confirmed)

- MCP stdio: **Newline-delimited JSON** (`JSON\n`) — both Python SDK v1.26 and Node SDK v1.29
- Internal socket: **Length-prefixed frames** (4-byte big-endian uint32 + payload)
- Reason: MCP data contains newlines; mixing control+data on same delimiter = ambiguity

---

## Core Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATOR DAEMON                         │
│                                                                │
│  ┌──────────────────┐  ┌────────────────┐  ┌──────────────┐  │
│  │  Socket Listener │  │ Request Router │  │ Server Pool  │  │
│  │                  │  │                │  │              │  │
│  │ Accept clients   │→│ ID rewrite     │→│ mongodb (1)  │  │
│  │ Auth (uid match) │  │ Route response │  │ gitlab (1)   │  │
│  │ Heartbeat ping   │  │ TTL cleanup    │  │ postgres (1) │  │
│  │ Backpressure     │  │ Timeout detect │  │ jira (1)     │  │
│  └──────────────────┘  └────────────────┘  │ jenkins (1)  │  │
│                                             │ http (1)     │  │
│  ┌──────────────────┐  ┌────────────────┐  │ debug (1)    │  │
│  │  Init Cache      │  │ Circuit Breaker│  └──────────────┘  │
│  │                  │  │ (per server)   │                     │
│  │ Cache init resp  │  │ CLOSED→OPEN→   │  ┌──────────────┐  │
│  │ Replay to new    │  │ HALF_OPEN      │  │ Health Check  │  │
│  │ clients          │  │                │  │ /proc/self    │  │
│  └──────────────────┘  └────────────────┘  │ RSS, FDs     │  │
│                                             └──────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

---

## Memory Management (Budget: <50 MB)

### Memory Budget Breakdown
| Component | Allocation |
|-----------|-----------|
| Base Python + asyncio | 15 MB |
| 7 server subprocess handles + buffers | 0.5 MB |
| 20 client connections × 73KB | 1.5 MB |
| 10,000 pending requests × 152 bytes | 1.5 MB |
| JSON workspace (transient) | 2 MB |
| Init cache (7 responses) | 0.1 MB |
| Metrics ring buffer | 0.5 MB |
| **Total** | **~21 MB** |

### Anti-Leak Strategies

1. **BoundedPendingRequests** — OrderedDict with:
   - Max capacity: 10,000 entries
   - TTL: 5 minutes per entry
   - Cleanup sweep: every 30s OR when >50% full on insert
   - `__slots__` on entry dataclass (40% less memory)

2. **WeakRef for client tracking** — PendingRequest stores `weakref.ref(client)`. When bridge disconnects, GC automatically invalidates pending entries.

3. **SafeLineBuffer** — `bytearray` with:
   - Max 10 MB per connection (prevents one giant mongo result from OOM-ing)
   - In-place deletion (`del buf[:consumed]`) — no copy
   - Clear on disconnect

4. **GC Tuning for long-running:**
   ```python
   gc.set_threshold(1000, 15, 5)  # Less frequent gen0, aggressive gen2
   # Every 5 minutes during idle: gc.collect(2)
   # MemoryWatchdog: warn at 40MB, force GC at 60MB, restart at 100MB
   ```

5. **Zero-Copy Fast Path** — Most messages are small (<1KB). For messages >64KB:
   - Read in 16KB chunks
   - Only parse first 200 bytes (to extract/rewrite `"id"` field)
   - Stream remaining bytes directly to destination socket
   - Never hold full 10MB response in memory

---

## Connection Lifecycle (State Machines)

### Client Connection
```
CONNECTING → HANDSHAKE → READY ⇄ ACTIVE → CLOSING → CLOSED
     │            │         │                  │
     └── timeout ─┘    idle 1hr ──────────────┘
                        heartbeat fail ────────┘
```

### Server Process
```
NONE → STARTING → READY ⇄ ACTIVE → FAILED → RESTARTING → READY
                     │                  │          │
                     └── health ok ─────┘     5 fails → DEAD
                                                   │
                                              manual restart only
```

### Circuit Breaker (per server)
```
CLOSED (normal) ──3 failures──→ OPEN (reject all) ──30s cooldown──→ HALF_OPEN (allow 1)
    ↑                                                                      │
    └──────────────────── success ─────────────────────────────────────────┘
                          failure → back to OPEN
```

---

## Request Routing & ID Collision Prevention

### Problem
Client A sends `{"id": 1, "method": "tools/list"}` 
Client B sends `{"id": 1, "method": "tools/call"}`
Server sees two `id:1` → chaos.

### Solution: Monotonic Internal IDs

```
Client A (id:1) → Orchestrator assigns internal_id: "_o1" → Server gets id:"_o1"
Client B (id:1) → Orchestrator assigns internal_id: "_o2" → Server gets id:"_o2"

Server responds id:"_o1" → Orchestrator lookup: "_o1" → (ClientA, original_id:1) → Client A gets id:1
Server responds id:"_o2" → Orchestrator lookup: "_o2" → (ClientB, original_id:1) → Client B gets id:1
```

Monotonic counter (`_o1, _o2, _o3...`) is:
- Unique across all clients (no collision possible)
- Fast to generate (no UUID overhead)
- Easy to debug in logs
- Bounded by TTL cleanup (counter can wrap at 2^63)

---

## Failure Modes & Recovery

### 1. MCP Server Crashes (exit/signal)
```
Detect:   stdout EOF (immediate)
Action:   
  1. Mark server FAILED, open circuit breaker
  2. Return JSON-RPC error (-32001) to ALL pending requests for this server
  3. Queue new requests (max 50, max 10s)
  4. Restart with backoff: 1s × 2^attempt (max 30s) + ±30% jitter
  5. After restart: clear init cache, re-initialize on next client request
  6. After 5 consecutive failures: mark DEAD, immediate errors
Recovery: `mcp-orchestrator restart mongodb` or full daemon restart
```

### 2. Server Hangs (no response)
```
Detect:   Request timeout (90s per request)
Action:
  1. Return timeout error to waiting client
  2. Increment failure counter
  3. If 3 timeouts in 60s: kill -9 server, trigger restart flow
  4. Track p99 latency; alert if consistently >30s
```

### 3. Bridge Loses Socket Connection
```
Detect:   Write fails or read EOF
Action:
  1. Buffer stdin messages (max 20 messages, max 5s)
  2. Reconnect: 100ms, 200ms, 500ms, 1s, 2s (5 attempts)
  3. On reconnect: re-send target selection, drain queue
  4. After 5 failures: write JSON-RPC error to stdout, exit
     → Kiro shows tool error to user (acceptable UX)
```

### 4. Orchestrator Crash
```
Detect:   Bridges get connection refused
Recovery: systemd Restart=always (restart within 3s)
          Bridges auto-reconnect (strategy #3)
          All servers restart fresh
          Brief ~5s interruption
```

### 5. Pipe Buffer Deadlock
```
Problem:  Server stdout buffer full (64KB) while orchestrator is writing to server stdin
          → Both block → deadlock
Solution: NEVER do sequential I/O. Always:
          - Separate reader coroutine for stdout
          - Separate writer coroutine for stdin  
          - Connected via asyncio.Queue(maxsize=100)
          - Drain stderr separately (prevents 64KB pipe deadlock)
```

### 6. Partial JSON / Corrupt Data
```
Problem:  Bridge killed mid-write → server gets partial JSON → parser broken forever
Solution: Server stdin writes MUST be atomic:
          - Compose full line in memory
          - Single write() call
          - If write would exceed pipe buffer: queue and retry
```

---

## Process Supervision

### Zombie Prevention
```python
# Set process group for clean tree kill
process = await asyncio.create_subprocess_exec(
    *cmd, start_new_session=True,  # New session = can kill group
    stdin=PIPE, stdout=PIPE, stderr=PIPE
)

# On shutdown/restart:
os.killpg(process.pid, signal.SIGTERM)
try:
    await asyncio.wait_for(process.wait(), timeout=5)
except TimeoutError:
    os.killpg(process.pid, signal.SIGKILL)
    await process.wait()  # Reap zombie
```

### FD Leak Prevention
- `close_fds=True` on subprocess creation
- Dedicated stderr drain coroutine (never accumulates)
- Track open FD count via `/proc/self/fd` every 60s
- Alert at 80% of ulimit

### Signal Handling
| Signal | Action |
|--------|--------|
| SIGTERM | Graceful shutdown (30s drain → close → cleanup) |
| SIGINT | Same as SIGTERM |
| SIGHUP | Reload config (restart changed servers only) |
| SIGCHLD | Zombie reaping via os.waitpid |

---

## Graceful Degradation

- Server failures are **isolated** — mongodb crash doesn't affect gitlab
- Circuit breaker returns instant error (no timeout waiting)
- Health reports: `healthy` (all up) / `degraded` (some up) / `unhealthy` (all down)
- Back-pressure: when pending queue >80% full, pause reading from clients (flow control)

---

## Operational Safety

### PID File with flock()
```python
# Kernel auto-releases lock on process death — no stale lock files
pid_fd = open(PID_FILE, 'w')
try:
    fcntl.flock(pid_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    sys.exit("Another orchestrator is already running")
pid_fd.write(str(os.getpid()))
pid_fd.flush()
# Keep fd open for lifetime of process
```

### Socket Permissions
```python
old_umask = os.umask(0o177)  # Creates socket as 0600
server = await asyncio.start_unix_server(handler, path=SOCKET_PATH)
os.umask(old_umask)
```

### Startup Order
1. Acquire flock (fail fast if duplicate)
2. Set up signal handlers
3. Start health check socket
4. Start MCP servers in parallel (continue on partial failure)
5. Open main socket (ready for clients)
6. Notify systemd: `sd_notify("READY=1")`

### Shutdown Order (SIGTERM)
1. Stop accepting new connections
2. Close main socket
3. Wait 30s for in-flight requests to complete
4. Disconnect remaining clients (send EOF)
5. SIGTERM servers → wait 5s → SIGKILL
6. Remove socket + PID file

### Log Rotation
- RotatingFileHandler: 10 MB × 5 backups (stdlib)
- Or: log to stderr → systemd journal handles rotation

---

## Initialize Caching

### Why
MCP `initialize` handshake takes 1-5s (server spawns, connects to DB, etc.)
Without caching: each new bridge waits. With caching: instant.

### Implementation
```python
class InitCache:
    def __init__(self):
        self._cache: Dict[str, bytes] = {}  # server_name → init response line
    
    def get(self, server: str) -> Optional[bytes]:
        return self._cache.get(server)
    
    def set(self, server: str, response: bytes):
        self._cache[server] = response
    
    def invalidate(self, server: str):
        self._cache.pop(server, None)  # Called on server restart
```

### Flow
1. First client: real init → server → cache response → return to client
2. Subsequent clients: return cached response immediately
3. Server restart: invalidate cache → next client triggers fresh init

---

## Internal Socket Frame Protocol

```
┌────────────────────────────────────────────┐
│ 4 bytes: payload length (big-endian u32)   │
│ N bytes: JSON payload                      │
└────────────────────────────────────────────┘
```

### Message Types (Bridge → Orchestrator)
```json
{"type": "connect", "target": "mongodb"}     // First msg: select server
{"type": "mcp", "data": "<raw JSON line>"}    // MCP request/notification
{"type": "pong"}                              // Heartbeat response
```

### Message Types (Orchestrator → Bridge)
```json
{"type": "connected", "server": "mongodb"}    // Confirm target
{"type": "mcp", "data": "<raw JSON line>"}    // MCP response/notification  
{"type": "ping"}                              // Heartbeat (every 30s)
{"type": "error", "code": "...", "msg": ".."}  // Orchestrator error
{"type": "backpressure", "retry_ms": 1000}    // Slow down
```

---

## File Structure

```
mcp-shared-proxy/
├── orchestrator.py              # Main daemon (~500 lines)
├── bridge.py                    # Stdio ↔ socket adapter (~80 lines)
├── protocol.py                  # Frame read/write, message types (~100 lines)
├── config.json                  # Server definitions
├── install.sh                   # One-command setup
├── uninstall.sh                 # Clean removal
├── mcp-orchestrator.service     # systemd unit
├── ARCHITECTURE.md              # This file
└── README.md                    # User-facing docs
```

---

## What's Explicitly NOT in v1

- Multi-user support (single uid)
- Remote/TCP connections
- Hot config reload (restart required for new servers)
- Web dashboard / Prometheus export
- Windows support
- Dynamic server scaling (1 instance per server, always)

---

## Implementation Order

1. `protocol.py` — frame read/write (foundation)
2. `orchestrator.py` — daemon with server pool + router
3. `bridge.py` — drop-in kiro adapter
4. Health monitoring + circuit breaker
5. Test with real servers
6. systemd + install script
