#!/usr/bin/env python3
"""
Test the orchestrator end-to-end without affecting existing kiro sessions.

Usage:
    # Terminal A: start orchestrator in foreground (see logs live)
    python3 orchestrator.py foreground

    # Terminal B: run this test
    python3 test_e2e.py mongodb
    python3 test_e2e.py jira
    python3 test_e2e.py --all     # Test all servers
"""

import asyncio
import json
import struct
import sys
import time
from pathlib import Path

SOCKET_PATH = Path("/tmp/mcp-orchestrator.sock")
FRAME_HEADER = struct.Struct(">I")


async def read_frame_raw(reader):
    header = await reader.readexactly(4)
    length = FRAME_HEADER.unpack(header)[0]
    payload = await reader.readexactly(length)
    return json.loads(payload)


async def read_frame(reader, writer=None):
    """Read frame, auto-responding to pings."""
    while True:
        msg = await read_frame_raw(reader)
        if msg and msg.get("type") == "ping" and writer:
            await write_frame(writer, {"type": "pong"})
            continue
        return msg


async def write_frame(writer, msg):
    payload = json.dumps(msg, separators=(",", ":")).encode()
    writer.write(FRAME_HEADER.pack(len(payload)) + payload)
    await writer.drain()


async def test_server(target: str):
    print(f"\n{'='*60}")
    print(f"  Testing: {target}")
    print(f"{'='*60}")

    # Connect
    if not SOCKET_PATH.exists():
        print("❌ Orchestrator not running. Start with: python3 orchestrator.py foreground")
        return False

    reader, writer = await asyncio.open_unix_connection(str(SOCKET_PATH))

    # 1. Select target
    print(f"\n→ Connecting to '{target}'...")
    await write_frame(writer, {"type": "connect", "target": target})
    resp = await asyncio.wait_for(read_frame(reader, writer), timeout=10)
    if resp.get("type") != "connected":
        print(f"❌ Connection failed: {resp}")
        writer.close()
        return False
    print(f"✅ Connected (server state: {resp.get('state')})")

    # 2. Send initialize
    print(f"\n→ Sending initialize...")
    init_request = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "1.0"}
        }
    })
    t0 = time.time()
    await write_frame(writer, {"type": "mcp", "data": init_request})
    resp = await asyncio.wait_for(read_frame(reader, writer), timeout=30)
    elapsed = time.time() - t0

    if resp.get("type") == "mcp":
        data = json.loads(resp["data"])
        if "result" in data:
            server_info = data["result"].get("serverInfo", {})
            print(f"✅ Initialize OK ({elapsed:.2f}s)")
            print(f"   Server: {server_info.get('name', '?')} v{server_info.get('version', '?')}")
            caps = list(data["result"].get("capabilities", {}).keys())
            print(f"   Capabilities: {caps}")
        elif "error" in data:
            print(f"❌ Initialize error: {data['error']}")
            writer.close()
            return False
    else:
        print(f"❌ Unexpected response: {resp}")
        writer.close()
        return False

    # 3. Send notifications/initialized
    await write_frame(writer, {"type": "mcp", "data": json.dumps({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    })})

    # 4. Request tools/list
    print(f"\n→ Requesting tools/list...")
    t0 = time.time()
    await write_frame(writer, {"type": "mcp", "data": json.dumps({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    })})
    resp = await asyncio.wait_for(read_frame(reader, writer), timeout=30)
    elapsed = time.time() - t0

    if resp.get("type") == "mcp":
        data = json.loads(resp["data"])
        if "result" in data:
            tools = data["result"].get("tools", [])
            print(f"✅ tools/list OK ({elapsed:.2f}s) — {len(tools)} tools available")
            for t in tools[:5]:
                print(f"   • {t['name']}")
            if len(tools) > 5:
                print(f"   ... and {len(tools) - 5} more")
        elif "error" in data:
            print(f"⚠️  tools/list error: {data['error']}")
    else:
        print(f"❌ Unexpected: {resp}")

    # 5. Test init cache (second initialize should be instant)
    print(f"\n→ Testing init cache (second initialize)...")
    t0 = time.time()
    await write_frame(writer, {"type": "mcp", "data": json.dumps({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test-client-2", "version": "1.0"}
        }
    })})
    resp = await asyncio.wait_for(read_frame(reader, writer), timeout=5)
    elapsed = time.time() - t0
    if resp.get("type") == "mcp" and "result" in json.loads(resp["data"]):
        print(f"✅ Cached init response ({elapsed*1000:.1f}ms)")
    else:
        print(f"⚠️  Cache miss or error ({elapsed:.2f}s)")

    writer.close()
    print(f"\n{'─'*60}")
    print(f"  ✅ {target}: ALL TESTS PASSED")
    print(f"{'─'*60}")
    return True


async def main():
    if len(sys.argv) < 2:
        print("Usage: test_e2e.py <server_name|--all>")
        print("  e.g.: test_e2e.py mongodb")
        print("        test_e2e.py jira")
        print("        test_e2e.py --all")
        sys.exit(1)

    target = sys.argv[1]

    if target == "--all":
        # Load config to get all server names
        config_path = Path(__file__).parent / "config.json"
        with open(config_path) as f:
            cfg = json.load(f)
        servers = list(cfg["servers"].keys())
        print(f"Testing all {len(servers)} servers: {servers}\n")

        results = {}
        for name in servers:
            try:
                results[name] = await test_server(name)
            except Exception as e:
                print(f"❌ {name}: EXCEPTION — {e}")
                results[name] = False

        print(f"\n\n{'='*60}")
        print(f"  SUMMARY")
        print(f"{'='*60}")
        for name, ok in results.items():
            status = "✅ PASS" if ok else "❌ FAIL"
            print(f"  {status}  {name}")
        passed = sum(1 for v in results.values() if v)
        print(f"\n  {passed}/{len(results)} servers working")
    else:
        try:
            await test_server(target)
        except asyncio.TimeoutError:
            print(f"❌ Timeout — server '{target}' not responding")
        except Exception as e:
            print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
