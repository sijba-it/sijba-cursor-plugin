#!/usr/bin/env python3
"""
External sijba Plugin Client for Cursor.

Run on any machine with Cursor installed that can reach sijba via WebSocket:
    python3 cursor_plugin_client.py

Environment variables:
    SIJBA_URL        - sijba WebSocket URL (default: wss://sijba.edv-bartl.at)
    SIJBA_TOKEN      - Auth token (required)
    PLUGIN_ID        - Unique plugin identifier (default: auto-generated)
    CURSOR_CMD       - Cursor CLI command (default: cursor)
    WORK_DIR         - Working directory for Cursor (default: /workspace)
    CURSOR_ARGS      - Extra args for cursor (default: "")
"""
import asyncio
import json
import os
import signal
import uuid
import websockets
from pathlib import Path

# Config from environment
SIJBA_URL = os.environ.get("SIJBA_URL", "wss://sijba.edv-bartl.at")
SIJBA_TOKEN = os.environ.get("SIJBA_TOKEN", "")
PLUGIN_ID = os.environ.get("PLUGIN_ID", f"cursor-{uuid.uuid4().hex[:8]}")
CURSOR_CMD = os.environ.get("CURSOR_CMD", "cursor-agent")
WORK_DIR = Path(os.environ.get("WORK_DIR", "/workspace"))
CURSOR_ARGS = os.environ.get("CURSOR_ARGS", "").split()

if not SIJBA_TOKEN:
    print("ERROR: SIJBA_TOKEN environment variable required")
    exit(1)

# Ensure work dir exists
WORK_DIR.mkdir(parents=True, exist_ok=True)


class CursorSession:
    """One sijba channel's ongoing Cursor conversation.

    `cursor-agent -p` has no long-lived process to attach to — instead each
    turn is a fresh invocation chained via `--resume <cursor_session_id>`,
    which cursor-agent resolves server-side to the full prior conversation.
    From the sijba user's point of view this behaves like one continuous
    chat: earlier context (including any question Cursor asked) carries
    forward to the next message in the same channel.
    """

    def __init__(self, session_id: str, work_dir: Path):
        self.session_id = session_id  # sijba channel id, for logging only
        self.work_dir = work_dir
        self.cursor_session_id: str | None = None  # cursor-agent's own session id

    async def start(self):
        print(f"[{self.session_id}] Cursor session ready (conversation mode)")

    async def send(self, text: str, request_id: int, on_chunk=None) -> dict:
        """Run one turn, forwarding incremental text to `on_chunk(delta)` as
        Cursor streams it, and returning the authoritative final reply.

        `--force` auto-approves every tool call cursor-agent wants to make
        (shell, file writes, ...) — headless mode has no TTY to prompt on,
        so there is no permission gate here by design (matches how
        cursor-agent is used elsewhere in this environment). If Cursor needs
        clarification instead of acting, it just says so in its reply text;
        the user's next message in the same sijba channel continues this
        same --resume'd session, so that reads as a normal back-and-forth.
        """
        cmd = [CURSOR_CMD, "-p", "--force", "--output-format", "stream-json", "--stream-partial-output"]
        if self.cursor_session_id:
            cmd += ["--resume", self.cursor_session_id]
        cmd += CURSOR_ARGS + [text]

        print(f"[{self.session_id}] Running: {' '.join(cmd[:-1])} <prompt> (in {self.work_dir})")

        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.work_dir,
            )

            final_text = ""
            stderr_buf = bytearray()

            async def _pump_stdout() -> None:
                nonlocal final_text
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    etype = event.get("type")

                    if etype == "assistant":
                        # Alongside real deltas, cursor-agent emits a recap
                        # event that repeats the full accumulated text — once
                        # before each tool call (tagged with model_call_id)
                        # and once at the very end (missing timestamp_ms
                        # entirely). Only forward genuine deltas, or the
                        # recap doubles what was already streamed.
                        is_delta = "timestamp_ms" in event and "model_call_id" not in event
                        if not is_delta:
                            continue
                        for part in event.get("message", {}).get("content", []):
                            delta = part.get("text", "")
                            if delta and on_chunk:
                                await on_chunk(delta)

                    elif etype == "tool_call" and event.get("subtype") == "started":
                        tool_call = event.get("tool_call") or {}
                        tool_name = next(iter(tool_call), "tool")
                        if on_chunk:
                            await on_chunk(f"\n_[{tool_name}]_\n")

                    elif etype == "result":
                        final_text = event.get("result", "")
                        if event.get("session_id"):
                            self.cursor_session_id = event["session_id"]

            async def _pump_stderr() -> None:
                while True:
                    chunk = await proc.stderr.read(4096)
                    if not chunk:
                        break
                    stderr_buf.extend(chunk)

            await asyncio.wait_for(
                asyncio.gather(_pump_stdout(), _pump_stderr()), timeout=600
            )
            await proc.wait()

            if stderr_buf:
                print(f"[{self.session_id}] stderr: {bytes(stderr_buf).decode(errors='replace').strip()}")

            return {"text": final_text or "(no output)", "request_id": request_id}

        except asyncio.TimeoutError:
            if proc and proc.returncode is None:
                proc.kill()
            return {"text": "Error: Cursor timeout (600s)", "request_id": request_id}
        except Exception as exc:
            if proc and proc.returncode is None:
                proc.kill()
            return {"text": f"Error: {exc}", "request_id": request_id}

    async def close(self):
        print(f"[{self.session_id}] Cursor session closed")


class SijbaPluginClient:
    """Connects to sijba via WebSocket and manages plugin channels."""

    def __init__(self):
        self.ws = None
        self.sessions: dict[str, CursorSession] = {}
        self.running = False
        self.shutdown_event = asyncio.Event()
        self._reconnect_backoff = 5

    async def connect(self):
        # Convert HTTPS/WSS URL
        ws_url = SIJBA_URL.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/api/plugin/ws?token={SIJBA_TOKEN}"

        print(f"Connecting to {ws_url} as {PLUGIN_ID}")
        self.ws = await websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )
        print("Connected!")

        # Register plugin
        await self.ws.send(json.dumps({
            "type": "register",
            "plugin_id": PLUGIN_ID,
            "capabilities": ["conversation", "tools", "files", "code", "agent"],
            "channels": {
                "cursor-main": {
                    "label": "Cursor Agent",
                    "hint": "AI-powered coding agent. Trigger: 'cursor ...' or '@cursor ...'",
                    "triggers": ["cursor", "@cursor"],
                    "capabilities": ["code", "analysis", "debugging", "files", "terminal", "refactor", "test"],
                    "metadata": {
                        "editor": "cursor",
                        "working_dir": str(WORK_DIR),
                        "mode": "agent",
                        "custom": {"team": "backend", "project": "sijba"}
                    }
                }
            }
        }))

        self.running = True
        self._reconnect_backoff = 5
        await self.message_loop()

    async def message_loop(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                mtype = msg.get("type")

                if mtype == "registered":
                    print(f"Plugin registered: {msg['plugin_id']}")
                elif mtype == "channel_registered":
                    print(f"Channel registered: {msg['channel_id']} (ok={msg.get('ok')})")
                elif mtype == "message":
                    await self.handle_message(msg)
                elif mtype == "ping":
                    await self.ws.send(json.dumps({"type": "pong"}))
                elif mtype == "pong":
                    pass
                elif mtype == "error":
                    print(f"Server error: {msg.get('message')}")
        except websockets.ConnectionClosed:
            print("Connection closed")
        except Exception as exc:
            print(f"Message loop error: {exc}")
        finally:
            self.running = False

    async def handle_message(self, msg: dict):
        channel_id = msg["channel"]
        payload = msg["payload"]
        envelope_id = payload["envelope_id"]
        text = payload["text"]
        metadata = payload.get("channel_metadata", {})

        # Remove trigger prefix
        for trigger in ["cursor", "@cursor"]:
            if text.lower().startswith(trigger):
                text = text[len(trigger):].strip()
                break

        if not text:
            return

        # Get or create session for this channel
        session = self.sessions.get(channel_id)
        if not session:
            work_dir = Path(metadata.get("working_dir", str(WORK_DIR)))
            session = CursorSession(channel_id, work_dir)
            await session.start()
            self.sessions[channel_id] = session

        async def _on_chunk(delta: str) -> None:
            try:
                await self.ws.send(json.dumps({
                    "type": "reply_chunk",
                    "channel": channel_id,
                    "envelope_id": envelope_id,
                    "text": delta,
                }))
            except Exception as exc:
                print(f"Failed to send reply_chunk: {exc}")

        # Send to Cursor
        try:
            request_id = len(self.sessions)  # simple unique ID
            response = await session.send(text, request_id, on_chunk=_on_chunk)
            reply_text = response.get("text", "")
        except Exception as exc:
            reply_text = f"Error: {exc}"

        # Send reply back to sijba
        try:
            await self.ws.send(json.dumps({
                "type": "reply",
                "channel": channel_id,
                "envelope_id": envelope_id,
                "text": reply_text,
                "continue": True
            }))
        except Exception as exc:
            print(f"Failed to send reply: {exc}")

    async def run_forever(self):
        """Main loop with auto-reconnect."""
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.shutdown_event.set)
            except NotImplementedError:
                pass  # Windows

        while not self.shutdown_event.is_set():
            try:
                await self.connect()
            except Exception as exc:
                print(f"Connection failed: {exc}")
            if self.shutdown_event.is_set():
                break
            print(f"Reconnecting in {self._reconnect_backoff}s...")
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=self._reconnect_backoff)
            except asyncio.TimeoutError:
                pass
            self._reconnect_backoff = min(self._reconnect_backoff * 2, 300)

        # Cleanup
        for session in self.sessions.values():
            await session.close()
        print("Shutdown complete")


async def main():
    client = SijbaPluginClient()
    await client.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
