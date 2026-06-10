# MCP Shared Orchestrator — Quick Reference

## How It Works (Fully Automatic)

1. You open a new kiro terminal
2. Kiro spawns `mcp-bridge mongodb` (17 KB compiled C binary, 1.5 MB RAM)
3. Bridge checks: is orchestrator socket available?
   - **YES** → connects, ready in ~1ms
   - **NO** → auto-starts orchestrator → connects → ready in ~5s
4. Orchestrator stays running even after all kiro sessions close
5. Bridge survives sleep/wake — auto-reconnects if socket drops

## Architecture (v2)

```
Per-server Unix sockets:
  /tmp/mcp-orchestrator/mongodb.sock
  /tmp/mcp-orchestrator/gitlab.sock
  /tmp/mcp-orchestrator/jira.sock
  /tmp/mcp-orchestrator/jenkins.sock
  /tmp/mcp-orchestrator/http-client.sock
  /tmp/mcp-orchestrator/postgres.sock

Protocol: Raw MCP JSON\n (no framing, no overhead)
Bridge: C binary, poll()-based bidirectional relay with reconnect
```

## Memory Usage

| Component | Memory |
|-----------|--------|
| Orchestrator daemon | 18 MB |
| 6 MCP servers (shared) | ~350 MB |
| Per bridge (C) | 1.5 MB |
| Per terminal (5 bridges) | 7.5 MB |
| **Total (5 terminals)** | **~420 MB** |

## Commands

```bash
cd ~/Desktop/mcp-shared-proxy

# Status
python3 orchestrator.py status

# Restart orchestrator
python3 orchestrator.py stop && python3 orchestrator.py start

# View logs
tail -f /tmp/mcp-orchestrator.log

# Recompile bridge (after code changes)
gcc -O2 -o mcp-bridge bridge.c

# Test all servers
python3 test_e2e.py --all

# Revert to original kiro config
cp ~/.kiro/settings/mcp.json.bak ~/.kiro/settings/mcp.json
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| "transport closed" | Restart kiro session (old bridge binary, no reconnect) |
| Server not responding | `python3 orchestrator.py stop && python3 orchestrator.py start` |
| Orchestrator won't start | Check `cat /tmp/mcp-orchestrator.log` |
| Socket permission denied | `rm /tmp/mcp-orchestrator/*.sock` then restart |

## Adding a New MCP Server

1. Edit `config.json` — add server entry
2. Restart orchestrator: `python3 orchestrator.py stop && python3 orchestrator.py start`
3. Update kiro config: add `{"command": "/path/to/mcp-bridge", "args": ["new-server"]}`
