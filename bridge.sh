#!/bin/bash
# MCP Bridge — connects kiro stdio to orchestrator's per-server socket.
# Usage: bridge.sh <server_name>
# Memory: ~1 MB (vs 18 MB for Python bridge)

SERVER="${1:?Usage: bridge.sh <server_name>}"
SOCKET_DIR="${MCP_ORCH_SOCKET_DIR:-/tmp/mcp-orchestrator}"
SOCKET="${SOCKET_DIR}/${SERVER}.sock"
ORCH_SCRIPT="$(dirname "$0")/orchestrator.py"

# Auto-start orchestrator if not running
if [ ! -S "$SOCKET" ]; then
    python3 "$ORCH_SCRIPT" start >/dev/null 2>&1
    # Wait for socket to appear (max 10s)
    for i in $(seq 1 20); do
        [ -S "$SOCKET" ] && break
        sleep 0.5
    done
fi

if [ ! -S "$SOCKET" ]; then
    echo '{"jsonrpc":"2.0","error":{"code":-32001,"message":"Orchestrator failed to start"}}' >&2
    exit 1
fi

exec socat - UNIX-CONNECT:"$SOCKET"
