#!/usr/bin/env bash
# Optional helper: generate a *user-level* service unit that runs this repo in place.
# Does not install system packages or write under /opt|/var|/usr.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLATFORM=""
CONFIG=""
NAME="catalog-capture"

usage() {
  cat << 'EOF'
Usage: scripts/optional-user-service.sh --platform launchd|systemd --config <toml> [--name <id>]

Generates a user-level service that runs catalog-capture from this repository
checkout (WorkingDirectory = repo root). Catalogs still come from the TOML
(output.catalog_uri, typically file://./data/...).

  launchd  → writes ~/Library/LaunchAgents/com.github.<name>.plist
  systemd  → writes ~/.config/systemd/user/<name>.service and prints enable commands

This is optional. Preferred: make build-release && ./scripts/run-capture-service.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --platform) PLATFORM="$2"; shift 2 ;;
    --config) CONFIG="$2"; shift 2 ;;
    --name) NAME="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$PLATFORM" || -z "$CONFIG" ]]; then
  usage >&2
  exit 2
fi

if [[ "$CONFIG" != /* ]]; then
  CONFIG="$ROOT/$CONFIG"
fi
if [[ ! -f "$CONFIG" ]]; then
  echo "Config not found: $CONFIG" >&2
  exit 1
fi

BIN="$ROOT/target/release/catalog-capture-cli"
RUNNER="$ROOT/scripts/run-capture-service.sh"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

case "$PLATFORM" in
  launchd)
    LABEL="com.github.${NAME}"
    OUT="${HOME}/Library/LaunchAgents/${LABEL}.plist"
    mkdir -p "$(dirname "$OUT")"
    cat >"$OUT" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>${RUNNER}</string>
    <string>--config</string>
    <string>${CONFIG}</string>
    <string>--release</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${ROOT}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>CATALOG_CAPTURE_LOG_DIR</key>
    <string>${LOG_DIR}</string>
    <key>PATH</key>
    <string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/launchd.stdout.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/launchd.stderr.log</string>
</dict>
</plist>
EOF
    echo "Wrote $OUT"
    echo "Load:   launchctl load $OUT"
    echo "Unload: launchctl unload $OUT"
    echo "Binary is built on first run via --release (or: make build-release)."
    ;;
  systemd)
    OUT="${HOME}/.config/systemd/user/${NAME}.service"
    mkdir -p "$(dirname "$OUT")"
    cat >"$OUT" <<EOF
[Unit]
Description=Catalog Capture (${NAME})
After=network-online.target

[Service]
Type=simple
WorkingDirectory=${ROOT}
Environment=RUSTUP_TOOLCHAIN=1.97.1
Environment=CATALOG_CAPTURE_LOG_DIR=${LOG_DIR}
ExecStart=${RUNNER} --config ${CONFIG} --release
Restart=on-failure
RestartSec=30
KillSignal=SIGTERM
TimeoutStopSec=60

[Install]
WantedBy=default.target
EOF
    echo "Wrote $OUT"
    echo "Enable: systemctl --user daemon-reload && systemctl --user enable --now ${NAME}.service"
    echo "Logs:   journalctl --user -u ${NAME}.service -f"
    echo "Note: keep a user session (or linger) so the unit can run."
    ;;
  *)
    echo "platform must be launchd or systemd" >&2
    exit 2
    ;;
esac

# silence unused
: "${BIN}"
