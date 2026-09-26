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
  - handle /model [name] locally: persists a per-project override to
    WORK_DIR/.cursor-agent.json, read fresh every turn (see CONFIG_PATH)

Environment variables
----------------------
SIJBA_URL       sijba WebSocket URL (default: wss://sijba.edv-bartl.at)
SIJBA_TOKEN     Auth token (required)
PROJECT_NAME    Registry key / trigger word (required) — e.g. "sijba-web"
WORK_DIR        Working directory for Cursor (required)
REGISTRY_FILE   Path to the controller's registry.json (required)
CURSOR_CMD      Cursor CLI command (default: cursor-agent)
CURSOR_ARGS     Extra args for cursor-agent (default: "")
CURSOR_MODEL    Catalog-wide default model passed as --model (default: unset,
                i.e. cursor-agent's own default). Per-project override via
                the in-channel /model [name] command — see CONFIG_PATH below.
"""
from __future__ import annotations

import asyncio
import collections
import fcntl
import json
import os
import re
import signal
import sys
import time
import urllib.error
import urllib.request
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
DEFAULT_MODEL = os.environ.get("CURSOR_MODEL", "")

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

# Per-project model override — same shape/rationale as sijba-pi-plugin's
# .pi.json: one cursor-agent project might want a stronger/cheaper model
# than another, so it lives in the project's own WORK_DIR rather than the
# shared catalog entry (an env var can't express "just this one channel").
CONFIG_PATH = WORK_DIR / ".cursor-agent.json"

_LOG_BUFFER: collections.deque[str] = collections.deque(maxlen=200)

# Env vars the spawned cursor-agent subprocess (and anything its own bash
# tool runs) has no legitimate need for. Found live: asyncio's
# create_subprocess_exec inherits the *full* parent environment by default
# when no env= is given — so without this, every cursor-agent turn (and
# any shell command the agent itself decides to run) got a free copy of
# SIJBA_TOKEN, the one shared bearer token that also authorizes sijba
# Core's entire HTTP API (queue/transcript/message/store — not just this
# plugin's own WebSocket). An LLM-driven agent executing arbitrary,
# potentially prompt-injected shell commands must not carry that around.
_AGENT_ENV_STRIP = ("SIJBA_TOKEN", "SIJBA_URL", "REGISTRY_FILE")


def _agent_env() -> dict:
    return {k: v for k, v in os.environ.items() if k not in _AGENT_ENV_STRIP}


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"{ts} [{PROJECT_NAME}] {msg}"
    _LOG_BUFFER.append(line)
    print(line, flush=True)


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        _log(f"ignoring unreadable {CONFIG_PATH}: {exc}")
        return {}


def _save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2))


def current_model() -> str:
    """Empty string means "no --model flag" — cursor-agent picks its own
    default (whatever the account's normally-selected model is), same as
    today's behavior for every project that never sets one."""
    return _load_config().get("model") or DEFAULT_MODEL


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


# Chat bubbles aren't the place for a raw multi-KB/MB blob (a tool result
# embedding a big file read, a huge diff, ...) — cap what actually reaches
# sijba, independent of how large the underlying event was.
MAX_CHARS_TO_UI = 8000


def _truncate(text: str, limit: int = MAX_CHARS_TO_UI) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n…[truncated, {len(text) - limit} more chars]"


async def _iter_json_lines(stream: asyncio.StreamReader):
    """Yield complete newline-delimited lines with no hard size limit.

    `StreamReader.readline()` has a fixed buffer limit (64KiB by default)
    and *raises* LimitOverrunError if a single line exceeds it before the
    separator is found — which happened live: a JSON-lines event with a
    large embedded file/diff blew past that, and the exception replaced
    the whole in-progress reply with an error instead of just being one
    long event. Read raw chunks and split on our own unbounded buffer
    instead — a huge line just accumulates in memory (bounded only by
    what's actually available, not an arbitrary fixed ceiling) rather than
    blowing up the read.
    """
    buf = bytearray()
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            if buf:
                yield bytes(buf)
            return
        buf.extend(chunk)
        while True:
            idx = buf.find(b"\n")
            if idx < 0:
                break
            yield bytes(buf[:idx])
            del buf[: idx + 1]


def _http_base() -> str:
    return SIJBA_URL.replace("wss://", "https://").replace("ws://", "http://")


def _download_attachment(url: str, token: str, dest: Path) -> None:
    # Explicit User-Agent required: urllib's default ("Python-urllib/x.y")
    # gets a 403 from the WAF in front of sijba before the request ever
    # reaches Core's own auth check — verified live (curl with the same
    # bearer token but its own UA reaches the app fine and gets a normal
    # 401/404; the bare Python UA gets 403 regardless of the header).
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "User-Agent": "sijba-cursor-plugin/1.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        dest.write_bytes(resp.read())


async def _fetch_attachments(attachments: list, work_dir: Path) -> list[str]:
    """Download attachments from Core into work_dir/sijba-uploads — Cursor's
    plugin process runs on its own host, no filesystem shared with Core, so
    the file has to be pulled over HTTP before cursor-agent can see it."""
    if not attachments:
        return []
    dest_dir = work_dir / "sijba-uploads"
    dest_dir.mkdir(exist_ok=True)
    fetched = []
    for att in attachments:
        filename = Path(att.get("filename") or "file").name
        dest = dest_dir / filename
        url = f"{_http_base()}/api/upload/{att['id']}"
        try:
            await asyncio.to_thread(_download_attachment, url, SIJBA_TOKEN, dest)
            fetched.append(f"sijba-uploads/{filename}")
            _log(f"downloaded attachment: {filename}")
        except (urllib.error.URLError, OSError, KeyError) as exc:
            _log(f"attachment download failed ({filename}): {exc}")
    return fetched


_INJECT_BLOCK_RE = re.compile(r"```sijba-inject\s+(\S+)\n(.*?)\n```", re.DOTALL)


def _extract_inject_blocks(text: str) -> list[tuple[str, str]]:
    """Find ` ```sijba-inject <channel_id>\\n...\\n``` ` blocks in a
    *complete* reply (never partial/streamed text — a marker split across
    two text_delta chunks would just fail to match, which is fine, but
    matching on partial content risks matching a half-written block that
    later turns out different once the model finishes). Deliberately does
    NOT strip the block from the text it was found in — the human should
    see what their agent asked to forward and where, not have it vanish
    into a hidden side channel."""
    return [(target, content.strip()) for target, content in _INJECT_BLOCK_RE.findall(text)]


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

    async def reset(self) -> None:
        """Drop the remembered cursor-agent session id — the next turn omits
        --resume, so cursor-agent starts a brand-new conversation instead of
        continuing the old one."""
        self.cursor_session_id = None
        _log("Cursor session reset")

    async def send(self, text: str, request_id: int, on_chunk=None) -> dict:
        cmd = [CURSOR_CMD, "-p", "--force", "--output-format", "stream-json", "--stream-partial-output"]
        if self.cursor_session_id:
            cmd += ["--resume", self.cursor_session_id]
        model = current_model()
        if model:
            cmd += ["--model", model]
        cmd += CURSOR_ARGS + [text]

        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.work_dir,
                env=_agent_env(),
            )
            final_text = ""
            model = "?"
            usage: dict = {}
            stderr_buf = bytearray()

            async def _pump_stdout() -> None:
                nonlocal final_text, model, usage
                async for line in _iter_json_lines(proc.stdout):
                    try:
                        event = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
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
                                await on_chunk(_truncate(delta))
                    elif etype == "tool_call" and event.get("subtype") == "started":
                        tool_call = event.get("tool_call") or {}
                        if on_chunk:
                            await on_chunk(f"\n_[{_describe_tool_call(tool_call)}]_\n")
                    elif etype == "result":
                        final_text = _truncate(event.get("result", ""))
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

            stderr_text = bytes(stderr_buf).decode(errors="replace").strip()
            if stderr_text:
                _log(f"stderr: {stderr_text}")
            _log(
                f"usage model={model} in={_fmt_tokens(usage.get('inputTokens', 0))} "
                f"cached={_fmt_tokens(usage.get('cacheReadTokens', 0))} out={_fmt_tokens(usage.get('outputTokens', 0))}"
            )

            # cursor-agent failed (login expired, quota, crash, broken
            # --resume, ...): it exits non-zero and emits no `result`, so
            # `final_text` stays empty. Surface the real error to the channel
            # instead of a mystifying "(no output)", and drop the session id so
            # the next turn starts clean rather than re-resuming a bad state.
            if proc.returncode != 0 and not final_text:
                self.cursor_session_id = None
                detail = stderr_text or f"exited {proc.returncode}, no output"
                _log(f"invocation failed (exit {proc.returncode}) — session reset")
                return {
                    "text": f"Error: cursor-agent failed — {_truncate(detail, 800)}",
                    "request_id": request_id,
                    "model": model,
                }

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
        # Keep the WS read loop free to answer pings while a turn runs —
        # blocking `await handle_message` caused keepalive timeouts and
        # dropped follow-up messages when the socket then died.
        self._msg_queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        # envelope_id -> Event (set when the turn finishes). Lets a Core
        # redelivery wait for the in-flight turn instead of double-running.
        self._inflight: dict[str, asyncio.Event] = {}
        # envelope_id -> final reply text (resend on redelivery, no re-run).
        self._completed_replies: dict[str, str] = {}

    async def connect(self):
        ws_url = SIJBA_URL.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/api/plugin/ws?token={SIJBA_TOKEN}"
        _log(f"Connecting to {ws_url} as {PLUGIN_ID}")
        # Generous ping_timeout: long agent turns + proxies that delay
        # protocol pongs. App-level JSON ping/pong is handled in message_loop.
        self.ws = await websockets.connect(
            ws_url, ping_interval=30, ping_timeout=60, close_timeout=5,
        )
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
                        f"Trigger: '@{PROJECT_NAME} ...'. In-channel commands: "
                        f"/archive, /pause, /reset, /model [name], /log [n]."
                    ),
                    # Core only routes message text on an explicit @mention, so
                    # a bare project name cannot hijack a sentence that merely
                    # starts with that word.
                    "triggers": [f"@{PROJECT_NAME}", f"@{CHANNEL_ID}"],
                    "capabilities": ["code", "analysis", "debugging", "files", "terminal", "refactor", "test"],
                    "metadata": {"plugin": PLUGIN_NAME, "project": PROJECT_NAME, "working_dir": str(WORK_DIR)},
                }
            },
        }))
        self._reconnect_backoff = 5
        self._worker_task = asyncio.create_task(self._message_worker())
        try:
            await self.message_loop()
        finally:
            if self._worker_task is not None:
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass
                self._worker_task = None

    async def _message_worker(self) -> None:
        while True:
            msg = await self._msg_queue.get()
            try:
                await self.handle_message(msg)
            except Exception as exc:
                _log(f"handle_message error: {exc}")
            finally:
                self._msg_queue.task_done()

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
                    await self._msg_queue.put(msg)
                elif mtype == "ping":
                    await self.ws.send(json.dumps({"type": "pong"}))
                elif mtype == "error":
                    _log(f"Server error: {msg.get('message')}")
        except websockets.ConnectionClosed:
            _log("Connection closed")
        except Exception as exc:
            _log(f"Message loop error: {exc}")

    def _remember_reply(self, envelope_id: str, text: str) -> None:
        self._completed_replies[envelope_id] = text
        # Bound memory — keep the most recent ~50 completed turns.
        while len(self._completed_replies) > 50:
            oldest = next(iter(self._completed_replies))
            self._completed_replies.pop(oldest, None)

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
            reply = "\n".join(lines) or "(no logs yet)"
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd == "model":
            new_model = rest.strip()
            if not new_model:
                cfg_model = _load_config().get("model")
                if cfg_model:
                    source = f"set for this project ({CONFIG_PATH.name})"
                elif DEFAULT_MODEL:
                    source = f"catalog default (CURSOR_MODEL={DEFAULT_MODEL})"
                else:
                    source = "cursor-agent's own default (no override set)"
                reply = f"Current model: {current_model() or '(cursor-agent default)'} — {source}"
            else:
                cfg = _load_config()
                cfg["model"] = new_model
                _save_config(cfg)
                reply = f"Model for {PROJECT_NAME} set to {new_model} (next turn uses it)."
                _log(f"model changed to {new_model!r} via /model")
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd in ("archive", "pause"):
            status = "archived" if cmd == "archive" else "paused"
            code = EXIT_ARCHIVED if cmd == "archive" else EXIT_PAUSED
            try:
                _set_registry_status(PROJECT_NAME, status)
            except Exception as exc:
                reply = f"Error updating registry: {exc}"
                self._remember_reply(envelope_id, reply)
                await self._reply(channel_id, envelope_id, reply)
                return True
            reply = f"{PROJECT_NAME} {status}. Stopping."
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            _log(f"{status} via /{cmd} — shutting down (exit {code})")
            self.exit_code = code
            self.shutdown_event.set()
            await self.ws.close()
            return True

        if cmd == "reset":
            await self.session.reset()
            self.session_started = False
            reply = f"{PROJECT_NAME} reset — next message starts fresh, no prior context."
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd:
            # Slash commands meant for cursor-agent itself (/help, ...) hang
            # indefinitely in headless mode — fail fast instead. /model is
            # handled above, locally, precisely because passing it straight
            # through would hit this same hang.
            reply = (
                "Unsupported here: /archive, /pause, /reset, /model [name], /log [n]. "
                "Anything else — just plain text."
            )
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True
        return False

    async def handle_message(self, msg: dict):
        channel_id = msg["channel"]
        payload = msg["payload"]
        envelope_id = payload["envelope_id"]
        text = payload["text"]
        attachments = payload.get("attachments") or []

        # Core redelivers unanswered envelopes after reconnect. If we already
        # finished this turn, just resend the cached reply. If it's still
        # running, wait — don't start a second cursor-agent.
        if envelope_id in self._completed_replies:
            _log(f"redelivery of completed envelope={envelope_id} — resending reply")
            await self._reply(channel_id, envelope_id, self._completed_replies[envelope_id])
            return
        if envelope_id in self._inflight:
            _log(f"redelivery of in-flight envelope={envelope_id} — waiting")
            await self._inflight[envelope_id].wait()
            if envelope_id in self._completed_replies:
                await self._reply(channel_id, envelope_id, self._completed_replies[envelope_id])
            return

        done = asyncio.Event()
        self._inflight[envelope_id] = done
        try:
            await self._handle_message_inner(channel_id, envelope_id, text, attachments)
        finally:
            done.set()
            self._inflight.pop(envelope_id, None)

    async def _handle_message_inner(
        self, channel_id: str, envelope_id: str, text: str, attachments: list,
    ) -> None:
        for trigger in (PROJECT_NAME, CHANNEL_ID):
            if text.lower().startswith(trigger.lower()):
                text = text[len(trigger):].strip()
                break
        if not text and not attachments:
            # Still ack so Core clears the pending entry.
            self._remember_reply(envelope_id, "")
            await self._reply(channel_id, envelope_id, "")
            return

        if text.lstrip().startswith("/"):
            if await self._handle_local_command(channel_id, envelope_id, text.lstrip()):
                return

        fetched = await _fetch_attachments(attachments, WORK_DIR)
        note = ""
        if fetched:
            note += f"[Angehängte Datei(en): {', '.join(fetched)}]\n\n"
        if len(fetched) < len(attachments):
            # Surfaced in-chat rather than only in the plugin's own log — a
            # silently-dropped attachment otherwise looks indistinguishable
            # from "the agent just ignored the file".
            note += f"[Achtung: {len(attachments) - len(fetched)} Anhang/Anhänge konnten nicht geladen werden]\n\n"
        text = f"{note}{text}".strip()
        if not text:
            self._remember_reply(envelope_id, "")
            await self._reply(channel_id, envelope_id, "")
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

        for target, inject_text in _extract_inject_blocks(reply_text):
            try:
                await self.ws.send(json.dumps({
                    "type": "inject", "channel": channel_id, "target": target, "text": inject_text,
                }))
                _log(f"inject -> {target!r} ({len(inject_text)} chars)")
            except Exception as exc:
                _log(f"Failed to send inject to {target!r}: {exc}")

        self._remember_reply(envelope_id, reply_text)
        await self._reply(channel_id, envelope_id, reply_text)

    async def run_forever(self):
        loop = asyncio.get_event_loop()

        def _request_shutdown() -> None:
            # Setting the event alone doesn't unblock message_loop()'s
            # `async for raw in self.ws` below — close the socket too, or
            # shutdown just hangs until the connection drops on its own.
            self.shutdown_event.set()
            if self.ws is not None:
                asyncio.ensure_future(self.ws.close())

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
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
