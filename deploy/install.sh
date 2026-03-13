#!/usr/bin/env bash
# install.sh — Download and install HTB-Replicator from GitHub Releases.
#
# One-line install (always latest):
#   curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash
#
# One-line install (specific version):
#   curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash -s -- --version v1.2.0
#
# Flags:
#   --version  <v1.2.0>        install a specific release tag   (default: latest)
#   --config   </path/to/cfg>  config file destination          (default: /etc/htb-replicator/config.yaml)
#   --data-dir </path>         data / metadata directory        (default: /var/lib/htb-replicator)
#   --user     <name>          service user (Linux only)        (default: replicator)
#   --no-service               install binary only, skip service setup
#   --uninstall                remove binary, service, and config

set -euo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED="\033[0;31m"; GREEN="\033[0;32m"; YELLOW="\033[0;33m"
CYAN="\033[0;36m"; BOLD="\033[1m"; RESET="\033[0m"

info()    { echo -e "${GREEN}${BOLD}[INFO]${RESET}  $*"; }
warn()    { echo -e "${YELLOW}${BOLD}[WARN]${RESET}  $*"; }
error()   { echo -e "${RED}${BOLD}[ERROR]${RESET} $*" >&2; exit 1; }
step()    { echo -e "\n${CYAN}${BOLD}▶ $*${RESET}"; }
require() { command -v "$1" &>/dev/null || error "'$1' is required but not installed."; }

# ── Constants ──────────────────────────────────────────────────────────────────
REPO="htb/htb-replicator"
BINARY_NAME="htb-replicator"
BINARY_DST="/usr/local/bin/${BINARY_NAME}"
CONFIG_DST="/etc/htb-replicator/config.yaml"
DATA_DIR="/var/lib/htb-replicator"
SERVICE_USER="replicator"
VERSION="latest"
NO_SERVICE=false
UNINSTALL=false

# ── Argument parsing ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)    VERSION="$2";    shift 2 ;;
    --config)     CONFIG_DST="$2"; shift 2 ;;
    --data-dir)   DATA_DIR="$2";   shift 2 ;;
    --user)       SERVICE_USER="$2"; shift 2 ;;
    --no-service) NO_SERVICE=true; shift ;;
    --uninstall)  UNINSTALL=true;  shift ;;
    *) error "Unknown argument: $1" ;;
  esac
done

# ── Platform detection ─────────────────────────────────────────────────────────
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
  Linux)  GOOS="linux" ;;
  Darwin) GOOS="darwin" ;;
  *) error "Unsupported OS: $OS  (supported: Linux, macOS)" ;;
esac

case "$ARCH" in
  x86_64)          GOARCH="amd64" ;;
  aarch64|arm64)   GOARCH="arm64" ;;
  armv7l)          GOARCH="armv7" ;;
  *) error "Unsupported architecture: $ARCH  (supported: x86_64, aarch64, arm64, armv7l)" ;;
esac

ASSET_NAME="${BINARY_NAME}-${GOOS}-${GOARCH}"

# ── Uninstall ──────────────────────────────────────────────────────────────────
if [[ "$UNINSTALL" == true ]]; then
  step "Uninstalling HTB-Replicator"

  if [[ "$OS" == "Linux" ]] && command -v systemctl &>/dev/null; then
    systemctl stop    htb-replicator 2>/dev/null || true
    systemctl disable htb-replicator 2>/dev/null || true
    rm -f /etc/systemd/system/htb-replicator.service
    systemctl daemon-reload
    info "systemd service removed."
  fi

  if [[ "$OS" == "Darwin" ]]; then
    PLIST="$HOME/Library/LaunchAgents/com.htb.htb-replicator.plist"
    launchctl unload "$PLIST" 2>/dev/null || true
    rm -f "$PLIST"
    info "launchd plist removed."
  fi

  rm -f "$BINARY_DST"
  info "Binary removed: $BINARY_DST"

  warn "Config and data directories were NOT removed."
  warn "  Config : $(dirname "$CONFIG_DST")"
  warn "  Data   : $DATA_DIR"
  warn "Remove them manually if no longer needed."
  exit 0
fi

# ── Pre-flight ─────────────────────────────────────────────────────────────────
[[ "$EUID" -eq 0 ]] || error "This script must be run as root (use sudo)."
require curl

step "HTB-Replicator Installer"
echo "  OS      : $OS ($ARCH)"
echo "  Asset   : $ASSET_NAME"
echo "  Version : $VERSION"
echo "  Binary  : $BINARY_DST"
echo "  Config  : $CONFIG_DST"
echo "  Data    : $DATA_DIR"

# ── Resolve version & download URLs ───────────────────────────────────────────
step "Resolving release"

if [[ "$VERSION" == "latest" ]]; then
  require grep
  API_URL="https://api.github.com/repos/${REPO}/releases/latest"
  VERSION="$(curl -fsSL "$API_URL" | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')"
  [[ -n "$VERSION" ]] || error "Could not determine latest version from GitHub API."
  info "Latest version: $VERSION"
fi

BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
BINARY_URL="${BASE_URL}/${ASSET_NAME}"
CHECKSUM_URL="${BASE_URL}/checksums.txt"

# ── Download ───────────────────────────────────────────────────────────────────
step "Downloading binary"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

TMP_BIN="${TMP_DIR}/${ASSET_NAME}"
TMP_SUM="${TMP_DIR}/checksums.txt"

info "Fetching: $BINARY_URL"
curl -fsSL --progress-bar -o "$TMP_BIN" "$BINARY_URL" \
  || error "Download failed. Check the version tag and your internet connection."

# ── Checksum verification ──────────────────────────────────────────────────────
step "Verifying checksum"
if curl -fsSL -o "$TMP_SUM" "$CHECKSUM_URL" 2>/dev/null; then
  if command -v sha256sum &>/dev/null; then
    EXPECTED="$(grep "${ASSET_NAME}$" "$TMP_SUM" | awk '{print $1}')"
    ACTUAL="$(sha256sum "$TMP_BIN" | awk '{print $1}')"
  elif command -v shasum &>/dev/null; then
    EXPECTED="$(grep "${ASSET_NAME}$" "$TMP_SUM" | awk '{print $1}')"
    ACTUAL="$(shasum -a 256 "$TMP_BIN" | awk '{print $1}')"
  else
    warn "No sha256sum/shasum found — skipping checksum verification."
    EXPECTED=""
    ACTUAL=""
  fi

  if [[ -n "$EXPECTED" ]]; then
    if [[ "$EXPECTED" == "$ACTUAL" ]]; then
      info "Checksum OK: $ACTUAL"
    else
      error "Checksum MISMATCH!\n  expected: $EXPECTED\n  actual  : $ACTUAL"
    fi
  else
    warn "Asset not found in checksums.txt — skipping verification."
  fi
else
  warn "Could not fetch checksums.txt — skipping verification."
fi

# ── Install binary ─────────────────────────────────────────────────────────────
step "Installing binary"
chmod 755 "$TMP_BIN"
mv "$TMP_BIN" "$BINARY_DST"
info "Installed: $BINARY_DST"
info "Version  : $("$BINARY_DST" --version 2>/dev/null || echo "$VERSION")"

if [[ "$NO_SERVICE" == true ]]; then
  echo ""
  info "Done (--no-service: skipped service setup)."
  info "Run: ${BINARY_DST} --help"
  exit 0
fi

# ── Config ─────────────────────────────────────────────────────────────────────
step "Setting up configuration"
mkdir -p "$(dirname "$CONFIG_DST")"

if [[ ! -f "$CONFIG_DST" ]]; then
  # Write a minimal example config
  cat > "$CONFIG_DST" <<'YAML'
# HTB-Replicator configuration
# Full reference: https://github.com/htb/htb-replicator#configuration

source:
  type: s3
  opts:
    endpoint: ""          # leave empty for AWS S3; set for MinIO etc.
    region: "us-east-1"
    bucket: "my-source-bucket"
    access_key_id: "CHANGE_ME"
    secret_access_key: "CHANGE_ME"
    path_style: false

# Run every day at 02:00 AM
schedule: "0 2 * * *"

destinations:
  - id: "local-backup"
    type: "local"
    opts:
      path: "/data/backups"

workers: 5
log_level: "info"
metrics_port: 2112
YAML
  chmod 640 "$CONFIG_DST"
  info "Example config written: $CONFIG_DST"
  warn "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  warn " Edit $CONFIG_DST before starting the service!"
  warn "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
else
  info "Config already exists — skipping: $CONFIG_DST"
fi

# ── Data directory ─────────────────────────────────────────────────────────────
mkdir -p "$DATA_DIR"

# ── Linux — systemd ────────────────────────────────────────────────────────────
if [[ "$OS" == "Linux" ]]; then
  require systemctl

  step "Creating service user"
  if ! id "$SERVICE_USER" &>/dev/null; then
    useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
    info "Created user: $SERVICE_USER"
  else
    info "User already exists: $SERVICE_USER"
  fi

  chown -R "${SERVICE_USER}:${SERVICE_USER}" "$DATA_DIR"
  chown "root:${SERVICE_USER}" "$CONFIG_DST"
  chmod 640 "$CONFIG_DST"

  step "Installing systemd service"
  UNIT="/etc/systemd/system/htb-replicator.service"
  cat > "$UNIT" <<EOF
[Unit]
Description=HTB-Replicator Replication Service
Documentation=https://github.com/htb/htb-replicator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
ExecStart=${BINARY_DST} start --config ${CONFIG_DST}
Restart=on-failure
RestartSec=10s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=htb-replicator
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ReadWritePaths=${DATA_DIR}
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable htb-replicator
  info "systemd service installed and enabled."

  echo ""
  echo -e "  ${GREEN}${BOLD}Next steps:${RESET}"
  echo -e "  1. Edit config    : ${BOLD}sudo \$EDITOR $CONFIG_DST${RESET}"
  echo -e "  2. Start service  : ${BOLD}sudo systemctl start htb-replicator${RESET}"
  echo -e "  3. View logs      : ${BOLD}journalctl -u htb-replicator -f${RESET}"
  echo -e "  4. Check status   : ${BOLD}systemctl status htb-replicator${RESET}"
  echo -e "  5. Health check   : ${BOLD}${BINARY_DST} health --config $CONFIG_DST${RESET}"
fi

# ── macOS — launchd ────────────────────────────────────────────────────────────
if [[ "$OS" == "Darwin" ]]; then
  # Prefer system-level daemon (requires root) so it runs at boot, not login.
  PLIST_DIR="/Library/LaunchDaemons"
  LOG_DIR="/var/log/htb-replicator"
  mkdir -p "$PLIST_DIR" "$LOG_DIR" "$DATA_DIR"

  PLIST="${PLIST_DIR}/com.htb.htb-replicator.plist"

  step "Installing launchd daemon"
  cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.htb.htb-replicator</string>

  <key>ProgramArguments</key>
  <array>
    <string>${BINARY_DST}</string>
    <string>start</string>
    <string>--config</string>
    <string>${CONFIG_DST}</string>
  </array>

  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key>
  <dict><key>SuccessfulExit</key><false/></dict>
  <key>ThrottleInterval</key><integer>10</integer>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/stdout.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/stderr.log</string>

  <key>WorkingDirectory</key>
  <string>${DATA_DIR}</string>
</dict>
</plist>
EOF

  launchctl load "$PLIST"
  info "launchd daemon installed and loaded."

  echo ""
  echo -e "  ${GREEN}${BOLD}Next steps:${RESET}"
  echo -e "  1. Edit config    : ${BOLD}sudo \$EDITOR $CONFIG_DST${RESET}"
  echo -e "  2. Start service  : ${BOLD}sudo launchctl start com.htb.htb-replicator${RESET}"
  echo -e "  3. View logs      : ${BOLD}tail -f $LOG_DIR/stdout.log${RESET}"
  echo -e "  4. Health check   : ${BOLD}${BINARY_DST} health --config $CONFIG_DST${RESET}"
fi

# ── Done ───────────────────────────────────────────────────────────────────────
echo ""
info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
info " HTB-Replicator ${VERSION} installed successfully!"
info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
