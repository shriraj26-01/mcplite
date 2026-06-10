# mcplite 🪶

**The zero-dependency MCP multiplexer. 10 GB → 450 MB.**

Share MCP server processes across all your terminal sessions. No HTTP server. No config files. Just Unix sockets.

## The Problem

Every AI coding assistant (Kiro, Claude Code, Cursor) spawns a **full copy** of each MCP server per terminal session:

```
5 terminals × 7 MCP servers = 35 processes → 10+ GB RAM
```

After a few hours, orphaned processes pile up and your machine grinds to a halt.

## The Solution

mcplite runs **one instance** of each MCP server and shares it across all sessions:

```
5 terminals × 7 bridges (3 MB each) + 7 shared servers = 450 MB total
```

## Install

```bash
git clone https://github.com/YOUR_USERNAME/mcplite.git
cd mcplite
bash install.sh
```

## Usage

**Zero manual steps.** After install:

1. Open any terminal → run `kiro-cli`
2. First session: orchestrator auto-starts (~5s)
3. Every subsequent session: connects instantly (~0.3s)
4. Close all terminals → orchestrator stays alive
5. Open kiro again → instant connect, servers still warm

## Results

| | Before | After |
|---|--------|-------|
| Processes (5 terminals) | 35 | 6 servers + 5 tiny bridges |
| Memory | 10 GB | 420 MB |
| Startup (warm) | 3-5s | 0.2-0.4s |
| Each new terminal | +1.2 GB | +7.5 MB |
| Bridge memory | N/A | 1.5 MB each (compiled C) |

## How It Works

```
┌────────────┐       ┌───────────────────┐       ┌──────────────────┐
│  Kiro CLI  │──────▶│  mcp-bridge (C)   │──────▶│                  │
│ Terminal 1  │ stdio │  17KB, 1.5MB RAM  │ sock  │   Orchestrator   │──── mongodb  (1)
└────────────┘       └───────────────────┘       │     daemon       │──── gitlab   (1)
                                                  │                  │──── jira     (1)
┌────────────┐       ┌───────────────────┐       │   (18 MB RAM)    │──── jenkins  (1)
│  Kiro CLI  │──────▶│  mcp-bridge (C)   │──────▶│                  │──── postgres (1)
│ Terminal 2  │ stdio │  17KB, 1.5MB RAM  │ sock  │                  │
└────────────┘       └───────────────────┘       └──────────────────┘
```

- **Bridge**: 17KB compiled C binary. Relays stdin/stdout ↔ Unix socket. 1.5 MB RAM.
- **Orchestrator**: Daemon with per-server listening sockets. Routes requests via ID rewriting.
- **Auto-start**: Bridge auto-launches orchestrator if not running. Zero manual steps.
- **Protocol**: Raw MCP JSON\n on per-server sockets. No framing overhead.

## Features

- **Zero dependencies** — Python stdlib + 17KB compiled C bridge
- **Zero config** — auto-starts, auto-detects, auto-reconnects
- **Survives sleep/wake** — bridge reconnects transparently after laptop suspend
- **Init caching** — subsequent terminals get instant responses
- **Circuit breaker** — per-server failure isolation
- **Auto-restart** — crashed servers restart with exponential backoff
- **Memory watchdog** — monitors RSS, prevents unbounded growth
- **Bounded queues** — 10K pending requests max with 5-min TTL
- **Graceful shutdown** — SIGTERM drains requests, then cleans up
- **flock() PID** — kernel handles crash cleanup (no stale locks)

## Configuration

Edit `~/.mcp-orchestrator/config.json`:

```json
{
  "servers": {
    "mongodb": {
      "command": "npx",
      "args": ["--prefer-offline", "-y", "mongodb-mcp-server", "--connectionString", "YOUR_URI"]
    },
    "gitlab": {
      "command": "npx",
      "args": ["--prefer-offline", "-y", "@zereight/mcp-gitlab"],
      "env": {"GITLAB_PERSONAL_ACCESS_TOKEN": "YOUR_TOKEN"}
    }
  }
}
```

## Commands

```bash
python3 ~/.mcp-orchestrator/orchestrator.py status     # Check health
python3 ~/.mcp-orchestrator/orchestrator.py stop       # Stop daemon
python3 ~/.mcp-orchestrator/orchestrator.py start      # Manual start
tail -f /tmp/mcp-orchestrator.log                      # Live logs
```

## Reverting

```bash
cp ~/.kiro/settings/mcp.json.bak ~/.kiro/settings/mcp.json
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full production design including:
- Length-prefixed frame protocol
- Monotonic ID rewriting for collision prevention
- Circuit breaker state machine
- Failure modes and recovery strategies
- Memory management (bounded to <50 MB)

## Comparison

| | mcplite | avelino/mcp | mcp-proxy |
|---|---------|-------------|-----------|
| Dependencies | **0** | 47 | 12 |
| Setup | **Auto** | Config file | Config file |
| Protocol | Unix socket | HTTP/SSE | HTTP/SSE |
| Target | Local dev | Enterprise | Bridge |
| Language | Python | Rust | Python |
| Memory (daemon) | 15 MB | ~50 MB | N/A |

## Requirements

- Python 3.8+
- Linux/macOS (Unix sockets)
- Any MCP-compatible CLI (Kiro, Claude Code, Cursor)

## License

MIT
