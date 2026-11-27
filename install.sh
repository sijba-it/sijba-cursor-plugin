#!/usr/bin/env bash
# Installer for sijba-cursor-plugin (idempotent - safe to re-run for updates / token changes).
#
#   curl -fsSL https://raw.githubusercontent.com/sijba-it/sijba-cursor-plugin/main/install.sh | \
#     SIJBA_URL=https://<account>.sijba.eu SIJBA_TOKEN=... PROJECT_NAME=my-project \
#     WORK_DIR=$HOME/workspaces/my-project bash
#
# Required env: SIJBA_URL, SIJBA_TOKEN
# Optional env: PROJECT_NAME (default: my-project), WORK_DIR (default: ~/workspaces/$PROJECT_NAME),
#               AGENT_HOME (default: ~/sijba-agents/$PROJECT_NAME), REPO_URL, REPO_REF
# Needs: git, python3 (+venv), systemd --user, cursor-agent
# Linux user service, no sudo.
set -euo pipefail

main() {
  local REPO_NAME="sijba-cursor-plugin" NEEDS=(cursor-agent)
  local name="${PROJECT_NAME:-my-project}"
  name="${name//[^A-Za-z0-9_.-]/-}"
  local work="${WORK_DIR:-$HOME/workspaces/$name}"
  local home="${AGENT_HOME:-$HOME/sijba-agents/$name}"
  local repo_url="${REPO_URL:-https://github.com/sijba-it/$REPO_NAME.git}"
  local unit="sijba-agent-$name"
  local envf="$HOME/.config/$unit.env"
  local unitf="$HOME/.config/systemd/user/$unit.service"

  say()  { printf '==> %s\n' "$*"; }
  die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
  retry() { local i; for i in 1 2 3; do "$@" && return 0; echo "  retry $i/3: $*" >&2; sleep 3; done; return 1; }

  [ -n "${SIJBA_URL:-}" ]   || die "SIJBA_URL is not set"
  [ -n "${SIJBA_TOKEN:-}" ] || die "SIJBA_TOKEN is not set"
  local c
  for c in git python3 systemctl "${NEEDS[@]}"; do
    command -v "$c" >/dev/null || die "missing command: $c (install it first)"
  done
  python3 -c 'import venv' 2>/dev/null || die "python3 venv module missing (apt install python3-venv)"
  systemctl --user show-environment >/dev/null 2>&1 \
    || die "no systemd user session (run from a login shell / ssh, not via plain su)"

  mkdir -p "$home" "$work" "$HOME/.config/systemd/user"

  # code: clone, or update an existing checkout (existing dir is fine)
  if [ -d "$home/plugin/.git" ]; then
    say "updating existing checkout in $home/plugin"
    retry git -C "$home/plugin" fetch -q origin || echo "  warning: fetch failed, keeping current code" >&2
    git -C "$home/plugin" merge -q --ff-only "@{u}" 2>/dev/null || echo "  note: not fast-forwarding (local changes?), keeping current code" >&2
  else
    [ -e "$home/plugin" ] && [ -n "$(ls -A "$home/plugin" 2>/dev/null)" ] \
      && die "$home/plugin exists but is not a git checkout - move it away or set AGENT_HOME"
    say "cloning $repo_url"
    retry git clone -q ${REPO_REF:+--branch "$REPO_REF"} "$repo_url" "$home/plugin"
  fi

  # venv
  if [ ! -x "$home/.venv/bin/python" ]; then
    say "creating venv"
    python3 -m venv "$home/.venv"
  fi
  say "installing python deps"
  retry "$home/.venv/bin/pip" install -q --upgrade websockets

  # registry: never overwrite existing state
  [ -s "$home/registry.json" ] || echo '{"projects":{}}' > "$home/registry.json"

  say "writing $envf and $unitf"
  ( umask 077
    cat > "$envf" <<ENV
SIJBA_URL=$SIJBA_URL
SIJBA_TOKEN=$SIJBA_TOKEN
PROJECT_NAME=$name
WORK_DIR=$work
REGISTRY_FILE=$home/registry.json
ENV
  )
  cat > "$unitf" <<UNIT
[Unit]
Description=SIJBA agent $name
After=network-online.target
[Service]
WorkingDirectory=$home/plugin
EnvironmentFile=$envf
Environment=PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin
Delegate=yes
KillMode=process
ExecStart=$home/.venv/bin/python main.py
Restart=always
RestartSec=5
RestartPreventExitStatus=90 91
[Install]
WantedBy=default.target
UNIT

  systemctl --user daemon-reload
  systemctl --user enable "$unit" >/dev/null
  systemctl --user restart "$unit"
  loginctl enable-linger "$USER" 2>/dev/null || echo "  note: could not enable linger (service stops at logout)" >&2

  sleep 2
  if systemctl --user is-active --quiet "$unit"; then
    say "done - $unit is running. logs: journalctl --user -u $unit -f"
  else
    systemctl --user status "$unit" --no-pager -n 15 >&2 || true
    die "$unit did not start"
  fi
}

main "$@"
