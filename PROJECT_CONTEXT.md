# MCP Shared Orchestrator

## Tech Stack
- Language: Python 3.10+
- Framework: asyncio (pure stdlib, zero external deps)
- Protocol: Newline-delimited JSON (MCP) + length-prefixed frames (internal)
- IPC: Unix domain socket

## Architecture Overview
Daemon that holds single instances of MCP servers, multiplexes across unlimited kiro sessions via Unix socket. Bridge scripts replace direct MCP spawns in kiro config.

## Directory Structure
```
mcp-shared-proxy/
├── orchestrator.py      # Main daemon (695 lines)
├── bridge.py            # Auto-start bridge for kiro (177 lines)
├── protocol.py          # Frame R/W, data structures (231 lines)
├── config.json          # Server definitions (6 servers)
├── kiro-mcp.json        # Kiro MCP config (bridge-based)
├── test_e2e.py          # Integration test
├── ARCHITECTURE.md      # Full design doc
├── INSTRUCTIONS.md      # User-facing setup guide
└── .kiro/               # Project memory
```

## Key Files
- Entry point: `orchestrator.py` (start/stop/status/foreground)
- Bridge: `bridge.py <server_name>` (spawned by kiro)
- Config: `config.json` (server command/args/env)
- Socket: `/tmp/mcp-orchestrator.sock`
- PID: `/tmp/mcp-orchestrator.pid`
- Log: `/tmp/mcp-orchestrator.log`

## Common Commands
- Start: `python3 orchestrator.py start`
- Stop: `python3 orchestrator.py stop`
- Status: `python3 orchestrator.py status`
- Test: `python3 test_e2e.py --all`
- Revert kiro: `cp ~/.kiro/settings/mcp.json.bak ~/.kiro/settings/mcp.json`
