#!/usr/bin/env python3
"""Cursor Agent plugin for sijba — standalone, no dependency on
sijba-agent-controller's own code. Runnable on its own (env vars below) or
spawned by the controller as a `kind: git` catalog plugin.

Plugin contract (documented in sijba-agent-controller/README.md):
  - read SIJBA_URL / SIJBA_TOKEN / PROJECT_NAME / WORK_DIR / REGISTRY_FILE
    from the environment
  - self-register exactly one sijba channel over its own WebSocket connection
  - handle /archive and /pause locally: write REGISTRY_FILE's own status
    entry directly (no shared import — see _set_registry_status below) and
    exit with the reserved code (90 archived / 91 paused) *before* the
    process actually stops, so the controller can tell "stopped on
    purpose" apart from "crashed" purely from the exit code
  - handle /log [n] locally from an in-memory ring buffer

Environment variables
----------------------
SIJBA_URL       sijba WebSocket URL (default: wss://sijba.edv-bartl.at)
SIJBA_TOKEN     Auth token (required)
PROJECT_NAME    Registry key / trigger word (required) — e.g. "sijba-web"
WORK_DIR        Working directory for Cursor (required)
REGISTRY_FILE   Path to the controller's registry.json (required)
CURSOR_CMD      Cursor CLI command (default: cursor-agent)
CURSOR_ARGS     Extra args for cursor-agent (default: "")
"""
from __future__ import annotations

import asyncio
import collections
import fcntl
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

# Exit-code contract with the controller — see sijba-agent-controller/exit_codes.py.
EXIT_ARCHIVED = 90
EXIT_PAUSED = 91

PLUGIN_NAME = "cursor"  # this repo *is* the cursor plugin — no TOOL switch needed

SIJBA_URL = os.environ.get("SIJBA_URL", "wss://sijba.edv-bartl.at")
SIJBA_TOKEN = os.environ.get("SIJBA_TOKEN", "")
PROJECT_NAME = os.environ.get("PROJECT_NAME", "")
WORK_DIR = Path(os.environ.get("WORK_DIR", ""))
REGISTRY_FILE = os.environ.get("REGISTRY_FILE", "")
CURSOR_CMD = os.environ.get("CURSOR_CMD", "cursor-agent")
CURSOR_ARGS = os.environ.get("CURSOR_ARGS", "").split()

if not SIJBA_TOKEN:
    print("ERROR: SIJBA_TOKEN environment variable required", file=sys.stderr)
    sys.exit(1)
if not PROJECT_NAME:
    print("ERROR: PROJECT_NAME environment variable required", file=sys.stderr)
    sys.exit(1)
if not WORK_DIR.is_dir():
    print(f"ERROR: WORK_DIR {WORK_DIR} does not exist", file=sys.stderr)
    sys.exit(1)
if not REGISTRY_FILE:
    print("ERROR: REGISTRY_FILE environment variable required", file=sys.stderr)
    sys.exit(1)

PLUGIN_ID = f"{PLUGIN_NAME}-{PROJECT_NAME}"
CHANNEL_ID = f"{PLUGIN_NAME}:{PROJECT_NAME}"

_LOG_BUFFER: collections.deque[str] = collections.deque(maxlen=200)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"{ts} [{PROJECT_NAME}] {msg}"
    _LOG_BUFFER.append(line)
    print(line, flush=True)


def _set_registry_status(name: str, status: str) -> None:
    """Read-modify-write REGISTRY_FILE directly — same JSON shape as
    sijba-agent-controller/registry.py, but no import of it: this repo has
    to stand on its own, same as any other git-sourced plugin would."""
    path = Path(REGISTRY_FILE)
    with open(path, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            raw = f.read().strip()
            data = json.loads(raw) if raw else {"roots": [], "projects": {}}
            data.setdefault("projects", {}).setdefault(name, {})["status"] = status
            data["projects"][name]["status_changed_at"] = time.time()
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp, path)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


# ---------------------------------------------------------------------------
# Cursor backend (one-shot `cursor-agent -p --resume` per turn, streamed)
# ---------------------------------------------------------------------------

def _fmt_tokens(n: int) -> str:
    return f"{n / 1000:.1f}k" if n >= 1000 else str(n)


def _describe_tool_call(tool_call: dict) -> str:
    for key, val in tool_call.items():
        if not key.endswith("ToolCall") or not isinstance(val, dict):
            continue
        label = key[: -len("ToolCall")]
        args = val.get("args", {})
        detail = (
            args.get("command") or args.get("path")
            or args.get("globPattern") or args.get("query") or args.get("description")
        )
        return f"{label}: {detail}" if detail else label
    return "tool"


class CursorSession:
    """One sijba channel's ongoing Cursor conversation.

    `cursor-agent -p` has no long-lived process to attach to — instead each
    turn is a fresh invocation chained via `--resume <cursor_session_id>`,
    which cursor-agent resolves server-side to the full prior conversation.
    """

    def __init__(self, work_dir: Path):
        self.work_dir = work_dir
        self.cursor_session_id: str | None = None

    async def start(self):
        _log("Cursor session ready (conversation mode)")

    async def send(self, text: str, request_id: int, on_chunk=None) -> dict:
        cmd = [CURSOR_CMD, "-p", "--force", "--output-format", "stream-json", "--stream-partial-output"]
        if self.cursor_session_id:
            cmd += ["--resume", self.cursor_session_id]
        cmd += CURSOR_ARGS + [text]

        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.work_dir,
            )
            final_text = ""
            model = "?"
            usage: dict = {}
            stderr_buf = bytearray()

            async def _pump_stdout() -> None:
                nonlocal final_text, model, usage
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    etype = event.get("type")
                    if etype == "system" and event.get("subtype") == "init":
                        model = event.get("model", model)
                    elif etype == "assistant":
                        is_delta = "timestamp_ms" in event and "model_call_id" not in event
                        if not is_delta:
                            continue
                        for part in event.get("message", {}).get("content", []):
                            delta = part.get("text", "")
                            if delta and on_chunk:
                                await on_chunk(delta)
                    elif etype == "tool_call" and event.get("subtype") == "started":
                        tool_call = event.get("tool_call") or {}
                        if on_chunk:
                            await on_chunk(f"\n_[{_describe_tool_call(tool_call)}]_\n")
                    elif etype == "result":
                        final_text = event.get("result", "")
                        usage = event.get("usage") or {}
                        if event.get("session_id"):
                            self.cursor_session_id = event["session_id"]

            async def _pump_stderr() -> None:
                while True:
                    chunk = await proc.stderr.read(4096)
                    if not chunk:
                        break
                    stderr_buf.extend(chunk)

            await asyncio.wait_for(asyncio.gather(_pump_stdout(), _pump_stderr()), timeout=600)
            await proc.wait()

            if stderr_buf:
                _log(f"stderr: {bytes(stderr_buf).decode(errors='replace').strip()}")
            _log(
                f"usage model={model} in={_fmt_tokens(usage.get('inputTokens', 0))} "
                f"cached={_fmt_tokens(usage.get('cacheReadTokens', 0))} out={_fmt_tokens(usage.get('outputTokens', 0))}"
            )
            return {"text": final_text or "(no output)", "request_id": request_id, "model": model, "usage": usage}
        except asyncio.TimeoutError:
            if proc and proc.returncode is None:
                proc.kill()
            return {"text": "Error: Cursor timeout (600s)", "request_id": request_id}
        except Exception as exc:
            if proc and proc.returncode is None:
                proc.kill()
            return {"text": f"Error: {exc}", "request_id": request_id}

    async def close(self):
        _log("Cursor session closed")


# ---------------------------------------------------------------------------
# sijba plugin protocol (single project channel, self-registered)
# ---------------------------------------------------------------------------

class PluginClient:
    def __init__(self):
        self.ws = None
        self.session = CursorSession(WORK_DIR)
        self.session_started = False
        self.shutdown_event = asyncio.Event()
        self.exit_code = 0
        self._reconnect_backoff = 5
        self._request_id = 0

    async def connect(self):
        ws_url = SIJBA_URL.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/api/plugin/ws?token={SIJBA_TOKEN}"
        _log(f"Connecting to {ws_url} as {PLUGIN_ID}")
        self.ws = await websockets.connect(ws_url, ping_interval=20, ping_timeout=10, close_timeout=5)
        _log("Connected!")

        await self.ws.send(json.dumps({
            "type": "register",
            "plugin_id": PLUGIN_ID,
            "capabilities": ["conversation", "tools", "files", "code", "agent"],
            "channels": {
                CHANNEL_ID: {
                    "label": f"Cursor: {PROJECT_NAME}",
                    "hint": (
                        f"Cursor Agent for project '{PROJECT_NAME}' at {WORK_DIR}. "
                        f"Trigger: '{PROJECT_NAME} ...'. In-channel commands: /archive, /pause, /log [n]."
                    ),
                    "triggers": [PROJECT_NAME, CHANNEL_ID],
                    "capabilities": ["code", "analysis", "debugging", "files", "terminal", "refactor", "test"],
                    "metadata": {"plugin": PLUGIN_NAME, "project": PROJECT_NAME, "working_dir": str(WORK_DIR)},
                }
            },
        }))
        self._reconnect_backoff = 5
        await self.message_loop()

    async def message_loop(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                mtype = msg.get("type")
                if mtype == "registered":
                    _log(f"Plugin registered: {msg['plugin_id']}")
                elif mtype == "channel_registered":
                    _log(f"Channel registered: {msg['channel_id']} (ok={msg.get('ok')})")
                elif mtype == "message":
                    await self.handle_message(msg)
                elif mtype == "ping":
                    await self.ws.send(json.dumps({"type": "pong"}))
                elif mtype == "error":
                    _log(f"Server error: {msg.get('message')}")
        except websockets.ConnectionClosed:
            _log("Connection closed")
        except Exception as exc:
            _log(f"Message loop error: {exc}")

    async def _reply(self, channel_id: str, envelope_id: str, text: str) -> None:
        try:
            await self.ws.send(json.dumps({
                "type": "reply", "channel": channel_id, "envelope_id": envelope_id,
                "text": text, "continue": True,
            }))
        except Exception as exc:
            _log(f"Failed to send reply: {exc}")

    async def _handle_local_command(self, channel_id: str, envelope_id: str, text: str) -> bool:
        cmd, _, rest = text[1:].partition(" ")
        cmd = cmd.lower().strip()

        if cmd == "log":
            n = 10
            if rest.strip().isdigit():
                n = min(int(rest.strip()), len(_LOG_BUFFER) or 10)
            lines = list(_LOG_BUFFER)[-n:]
            await self._reply(channel_id, envelope_id, "\n".join(lines) or "(no logs yet)")
            return True

        if cmd in ("archive", "pause"):
            status = "archived" if cmd == "archive" else "paused"
            code = EXIT_ARCHIVED if cmd == "archive" else EXIT_PAUSED
            try:
                _set_registry_status(PROJECT_NAME, status)
            except Exception as exc:
                await self._reply(channel_id, envelope_id, f"Error updating registry: {exc}")
                return True
            await self._reply(channel_id, envelope_id, f"{PROJECT_NAME} {status}. Stopping.")
            _log(f"{status} via /{cmd} — shutting down (exit {code})")
            self.exit_code = code
            self.shutdown_event.set()
            await self.ws.close()
            return True

        if cmd:
            # Slash commands meant for cursor-agent itself (/help, /model, ...)
            # hang indefinitely in headless mode — fail fast instead.
            await self._reply(
                channel_id, envelope_id,
                "Unsupported here: /archive, /pause, /log [n]. Anything else — just plain text.",
            )
            return True
        return False

    async def handle_message(self, msg: dict):
        channel_id = msg["channel"]
        payload = msg["payload"]
        envelope_id = payload["envelope_id"]
        text = payload["text"]

        for trigger in (PROJECT_NAME, CHANNEL_ID):
            if text.lower().startswith(trigger.lower()):
                text = text[len(trigger):].strip()
                break
        if not text:
            return

        if text.lstrip().startswith("/"):
            if await self._handle_local_command(channel_id, envelope_id, text.lstrip()):
                return

        if not self.session_started:
            await self.session.start()
            self.session_started = True

        async def _on_chunk(delta: str) -> None:
            try:
                await self.ws.send(json.dumps({
                    "type": "reply_chunk", "channel": channel_id, "envelope_id": envelope_id, "text": delta,
                }))
            except Exception as exc:
                _log(f"Failed to send reply_chunk: {exc}")

        _log(f"handling envelope={envelope_id} text={text[:120]!r}")
        self._request_id += 1
        response: dict = {}
        try:
            response = await self.session.send(text, request_id=self._request_id, on_chunk=_on_chunk)
            reply_text = response.get("text", "")
        except Exception as exc:
            reply_text = f"Error: {exc}"

        if response.get("model"):
            try:
                await self.ws.send(json.dumps({
                    "type": "channel_update", "channel_id": channel_id,
                    "metadata": {"model": response["model"], "context_tokens": (response.get("usage") or {}).get("inputTokens", 0)},
                }))
            except Exception as exc:
                _log(f"Failed to send channel_update: {exc}")

        await self._reply(channel_id, envelope_id, reply_text)

    async def run_forever(self):
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.shutdown_event.set)
            except NotImplementedError:
                pass

        while not self.shutdown_event.is_set():
            try:
                await self.connect()
            except Exception as exc:
                _log(f"Connection failed: {exc}")
            if self.shutdown_event.is_set():
                break
            _log(f"Reconnecting in {self._reconnect_backoff}s...")
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=self._reconnect_backoff)
            except asyncio.TimeoutError:
                pass
            self._reconnect_backoff = min(self._reconnect_backoff * 2, 300)

        await self.session.close()
        _log("Shutdown complete")


async def main() -> int:
    client = PluginClient()
    await client.run_forever()
    return client.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
