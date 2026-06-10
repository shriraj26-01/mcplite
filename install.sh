#!/usr/bin/env bash
set -e

INSTALL_DIR="${HOME}/.mcp-orchestrator"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== mcplite — Install ==="
echo ""

# 1. Copy source files
echo "→ Installing to ${INSTALL_DIR}..."
mkdir -p "$INSTALL_DIR"
cp "$SCRIPT_DIR"/{orchestrator.py,protocol.py,bridge.c,bridge.sh} "$INSTALL_DIR/"

# 2. Compile C bridge
echo "→ Compiling bridge..."
if command -v gcc &>/dev/null; then
    gcc -O2 -o "$INSTALL_DIR/mcp-bridge" "$INSTALL_DIR/bridge.c"
    echo "   ✓ Compiled mcp-bridge ($(du -h "$INSTALL_DIR/mcp-bridge" | cut -f1))"
else
    echo "   ⚠ gcc not found. Using shell bridge (needs socat)."
    chmod +x "$INSTALL_DIR/bridge.sh"
fi

# 3. Config
if [ ! -f "$INSTALL_DIR/config.json" ]; then
    if [ -f "$SCRIPT_DIR/config.json" ]; then
        cp "$SCRIPT_DIR/config.json" "$INSTALL_DIR/"
        echo "→ Copied config.json (⚠ EDIT YOUR CREDENTIALS)"
    else
        cp "$SCRIPT_DIR/config.example.json" "$INSTALL_DIR/config.json"
        echo "→ Created config.json from example (⚠ EDIT YOUR CREDENTIALS)"
    fi
else
    echo "→ config.json already exists (not overwritten)"
fi

# 4. Backup kiro config
KIRO_MCP="$HOME/.kiro/settings/mcp.json"
mkdir -p "$HOME/.kiro/settings"
if [ -f "$KIRO_MCP" ] && [ ! -f "$KIRO_MCP.bak" ]; then
    cp "$KIRO_MCP" "$KIRO_MCP.bak"
    echo "→ Backed up kiro config to mcp.json.bak"
fi

# 5. Generate kiro config
echo "→ Generating kiro MCP config..."
BRIDGE="$INSTALL_DIR/mcp-bridge"
[ ! -f "$BRIDGE" ] && BRIDGE="$INSTALL_DIR/bridge.sh"

python3 -c "
import json
config_path = '${INSTALL_DIR}/config.json'
bridge = '${BRIDGE}'
with open(config_path) as f:
    cfg = json.load(f)
mcp = {'mcpServers': {}}
for name in cfg['servers']:
    mcp['mcpServers'][name] = {'command': bridge, 'args': [name]}
with open('${KIRO_MCP}', 'w') as f:
    json.dump(mcp, f, indent=2)
print(f'   ✓ {len(mcp[\"mcpServers\"])} servers configured')
"

# 6. systemd (optional)
echo ""
read -p "→ Install systemd user service for auto-start on login? (y/N) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    mkdir -p "$HOME/.config/systemd/user"
    cat > "$HOME/.config/systemd/user/mcp-orchestrator.service" <<EOF
[Unit]
Description=MCP Shared Orchestrator

[Service]
Type=forking
PIDFile=/tmp/mcp-orchestrator.pid
ExecStart=/usr/bin/python3 ${INSTALL_DIR}/orchestrator.py start
ExecStop=/usr/bin/python3 ${INSTALL_DIR}/orchestrator.py stop
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
EOF
    systemctl --user daemon-reload
    systemctl --user enable mcp-orchestrator
    echo "   ✓ Enabled (starts on login)"
fi

echo ""
echo "=== Done! ==="
echo ""
echo "Next steps:"
echo "  1. Edit credentials: vi $INSTALL_DIR/config.json"
echo "  2. Open a new terminal and run: kiro-cli"
echo "     (orchestrator auto-starts on first use)"
echo ""
echo "Commands:"
echo "  python3 $INSTALL_DIR/orchestrator.py status"
echo "  python3 $INSTALL_DIR/orchestrator.py stop"
echo "  tail -f /tmp/mcp-orchestrator.log"
echo ""
echo "Revert: cp ~/.kiro/settings/mcp.json.bak ~/.kiro/settings/mcp.json"
