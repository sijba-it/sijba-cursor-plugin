# sijba-cursor-plugin

Standalone Cursor Agent plugin for sijba — a `kind: git` catalog plugin for
[sijba-agent-controller](https://REDACTED-GIT-HOST/sijba/sijba-agent-controller),
but runnable on its own too. No dependency on the controller's own code;
just the documented env-var contract.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install websockets

export SIJBA_URL=https://sijba.edv-bartl.at
export SIJBA_TOKEN=your-token-here
export PROJECT_NAME=my-project
export WORK_DIR=/path/to/my-project
export REGISTRY_FILE=/path/to/registry.json   # shared with the controller, for /archive and /pause

python main.py
```

## Usage in sijba

Registers one channel, `cursor:<PROJECT_NAME>`, triggered by the project
name itself:

```
my-project refactor this function to use async/await
```

In-channel commands (handled locally, never reach the `cursor-agent` CLI):

- `/archive` — write `REGISTRY_FILE`'s status for this project to
  `archived`, then exit with code `90` and stop. No auto-restart.
- `/pause` — same, status `paused`, exit code `91`.
- `/log [n]` — last `n` (default 10) of this process's own log lines.

## Cursor CLI requirements

- Cursor must be installed and in PATH (or set `CURSOR_CMD`)
- Uses `cursor-agent -p --force --resume <session>` per turn (headless,
  auto-approves tool calls — no TTY to prompt on)
- Extra args via `CURSOR_ARGS`

## Managed by the controller

Registered in `sijba-agent-controller`'s catalog as:

```
catalog add cursor --git ssh://git@REDACTED-GIT-HOST:7999/sijba/sijba-cursor-plugin.git
```

The controller clones this repo into its own `_plugins/cursor/`, then runs
`main.py` with `SIJBA_URL`/`SIJBA_TOKEN`/`PROJECT_NAME`/`WORK_DIR`/`REGISTRY_FILE`
set for whichever project it's supervising.
