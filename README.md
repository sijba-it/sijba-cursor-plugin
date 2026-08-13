# sijba-cursor-plugin

External plugin for sijba that connects Cursor Agent as a channel.

## Quick Start

```bash
# 1. Create venv & install deps
python3 -m venv .venv
source .venv/bin/activate
pip install websockets

# 2. Set environment variables
export SIJBA_URL=https://sijba.edv-bartl.at
export SIJBA_TOKEN=your-token-here
export PLUGIN_ID=cursor-dev-vm
export WORK_DIR=/home/robert/projects

# 3. Run
python cursor_plugin_client.py
```

## Usage in sijba

Once connected, send messages to Cursor via sijba:

```
cursor refactor this function to use async/await
@cursor write a test for the login handler
cursor explain this codebase structure
```

## Trigger Words

- `cursor ...`
- `@cursor ...`

## Channel Metadata

The plugin registers with metadata that sijba passes through:

```json
{
  "editor": "cursor",
  "working_dir": "/home/robert/projects",
  "mode": "agent",
  "custom": {"team": "backend", "project": "sijba"}
}
```

## Cursor CLI Requirements

- Cursor must be installed and in PATH (or set `CURSOR_CMD`)
- Uses `cursor --agent -p "prompt"` for one-shot agent mode
- Extra args via `CURSOR_ARGS` env var

## systemd Service

```bash
sudo cp sijba-cursor-plugin.service /etc/systemd/system/
# Edit Environment= values
sudo systemctl enable --now sijba-cursor-plugin
```
