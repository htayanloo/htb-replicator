# HTB-Replicator

> Production-grade multi-source S3 replication engine — replicate to local, S3, FTP, and SFTP with scheduling, deduplication, retention policies, alerts, and Prometheus metrics.

---

## Quick Install

**Linux & macOS — one command, auto-detects OS and architecture:**

```bash
curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash
```

**Install a specific version:**

```bash
curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash -s -- --version v1.2.0
```

**Binary only (skip service setup):**

```bash
curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash -s -- --no-service
```

**Uninstall:**

```bash
curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash -s -- --uninstall
```

> After install, edit `/etc/htb-replicator/config.yaml` then start the service.

---

## Features

- Multi-destination replication with per-object ETag deduplication
- Cron schedule (`"0 2 * * *"`) or fixed interval (`interval_seconds: 300`)
- SQLite metadata store — tracks sync state across restarts
- Worker pool with configurable concurrency and backpressure
- Atomic writes — no partial files on disk or SFTP
- Retention policy — auto-delete aged objects from source and/or destinations
- Alerts — Telegram, Slack, email (SMTP), and generic HTTP webhook
- Prometheus metrics on `:2112/metrics`
- Colorful sync-run history with `logs` command
- System service — systemd (Linux), launchd (macOS), NSSM (Windows)
- Multi-arch releases: Linux amd64/arm64/armv7, macOS amd64/arm64, Windows amd64/arm64

---

## Table of Contents

1. [Quick Install](#quick-install)
2. [Requirements](#requirements)
3. [Build from Source](#build-from-source)
4. [Configuration](#configuration)
5. [Running Manually](#running-manually)
6. [Install as a System Service](#install-as-a-system-service)
   - [Linux — systemd](#linux--systemd)
   - [macOS — launchd](#macos--launchd)
   - [Windows — NSSM](#windows--nssm)
7. [Docker / Docker Compose](#docker--docker-compose)
8. [Prometheus Metrics](#prometheus-metrics)
9. [CLI Reference](#cli-reference)
10. [Environment Variables](#environment-variables)
11. [Troubleshooting](#troubleshooting)
12. [Project Layout](#project-layout)

---

## Requirements

| Requirement | Version |
|-------------|---------|
| Go | 1.23+ *(build from source only)* |
| Linux / macOS / Windows | any |
| SQLite | bundled (pure Go, no CGO required) |

---

## Build from Source

### 1. Clone the repository

```bash
git clone https://github.com/htb/htb-replicator.git
cd htb-replicator
```

### 2. Download dependencies

```bash
go mod download
```

### 3. Build the binary

```bash
# Standard build
go build -o htb-replicator ./cmd/replicator

# Production build (stripped, reproducible)
go build \
  -ldflags="-s -w" \
  -trimpath \
  -o htb-replicator \
  ./cmd/replicator
```

### 4. Verify

```bash
./htb-replicator --help
```

### Cross-compile

```bash
# Linux amd64
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -trimpath -o htb-replicator-linux-amd64 ./cmd/replicator

# Linux arm64 (Raspberry Pi 4, AWS Graviton)
GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -trimpath -o htb-replicator-linux-arm64 ./cmd/replicator

# macOS Apple Silicon
GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -trimpath -o htb-replicator-darwin-arm64 ./cmd/replicator
```

Pre-built binaries for all platforms are available on the [Releases](https://github.com/htb/htb-replicator/releases) page.

---

## Configuration

Copy the example file and edit it:

```bash
cp config.example.yaml config.yaml
$EDITOR config.yaml
```

### Full config reference

```yaml
# ── Source ────────────────────────────────────────────────────────────────────
source:
  type: s3
  opts:
    endpoint: "http://minio:9000"       # leave empty for AWS S3
    region: "us-east-1"
    bucket: "source-bucket"
    access_key_id: "minioadmin"
    secret_access_key: "minioadmin"
    path_style: true                    # required for MinIO
    prefix: ""                          # optional key prefix filter

# ── Scheduling ────────────────────────────────────────────────────────────────
# Option A — cron expression (recommended for scheduled backups)
schedule: "0 2 * * *"

# Option B — fixed interval in seconds (used when schedule is empty)
# interval_seconds: 300

# ── Destinations ──────────────────────────────────────────────────────────────
destinations:
  - id: "local-backup"
    type: "local"
    opts:
      path: "/data/backups"

  - id: "s3-replica"
    type: "s3"
    opts:
      endpoint: "http://minio-replica:9000"
      region: "us-east-1"
      bucket: "replica-bucket"
      access_key_id: "minioadmin"
      secret_access_key: "minioadmin"
      path_style: true

  - id: "sftp-backup"
    type: "sftp"
    opts:
      host: "sftp.example.com"
      port: 22
      username: "backup"
      password: "secret"
      base_path: "/backup"

  - id: "ftp-backup"
    type: "ftp"
    opts:
      host: "ftp.example.com"
      port: 21
      username: "backup"
      password: "secret"
      base_path: "/backup"

# ── Worker Pool ───────────────────────────────────────────────────────────────
workers: 5

# ── Metadata Store ────────────────────────────────────────────────────────────
metadata_db: "/var/lib/htb-replicator/replicator.db"

# ── Prometheus Metrics ────────────────────────────────────────────────────────
metrics_port: 2112       # set to 0 to disable

# ── Logging ───────────────────────────────────────────────────────────────────
log_level: "info"        # debug | info | warn | error

# ── Alerting ──────────────────────────────────────────────────────────────────
alerts:
  error_threshold: 5     # failures before alert fires
  cooldown_minutes: 15   # min gap between repeated alerts

  telegram:
    token: "YOUR_BOT_TOKEN"
    chat_id: -1001234567890

  slack:
    webhook_url: "https://hooks.slack.com/services/XXX/YYY/ZZZ"

  email:
    host: "smtp.example.com"
    port: 587
    username: "alerts@example.com"
    password: "smtp-password"
    from: "alerts@example.com"
    to:
      - "ops@example.com"

  webhook:
    url: "https://alerting.example.com/hooks/backup"
    headers:
      Authorization: "Bearer your-token"

# ── Retention Policy ──────────────────────────────────────────────────────────
retention:
  source_days: 90          # delete from source after 90 days (0 = never)
  destination_days: 365    # delete from destinations after 365 days (0 = never)
```

---

### Scheduling

The service supports two scheduling modes. Set **one** of them:

#### Cron expression (`schedule`)

Standard 5-field cron (minute-level precision) plus descriptors:

| Expression | Meaning |
|------------|---------|
| `"0 2 * * *"` | Every day at **02:00 AM** |
| `"0 */6 * * *"` | Every **6 hours** |
| `"0 0 * * 0"` | Every **Sunday midnight** |
| `"30 3 * * 1-5"` | **03:30 AM** on weekdays |
| `"@hourly"` | Every hour |
| `"@daily"` | Every day at midnight |
| `"@every 30m"` | Every 30 minutes |
| `"@every 6h"` | Every 6 hours |

Cron expression is validated at startup — the service refuses to start with an invalid expression.

#### Fixed interval (`interval_seconds`)

```yaml
interval_seconds: 300   # run every 5 minutes
```

Used as a fallback when `schedule` is not set. The first cycle always runs immediately at startup.

---

## Running Manually

### Start the daemon

```bash
./htb-replicator start --config config.yaml
```

### Run a single backup cycle and exit

```bash
./htb-replicator sync-once --config config.yaml
# Exit 0 = all objects synced
# Exit 1 = one or more failures
```

Useful for cron jobs managed externally (e.g. system cron, Kubernetes CronJob):

```cron
0 2 * * * /usr/local/bin/htb-replicator sync-once --config /etc/htb-replicator/config.yaml
```

### Check sync status

```bash
./htb-replicator status --config config.yaml
```

### View sync-run history (colorful)

```bash
# Last 10 runs with per-destination results
./htb-replicator logs --config config.yaml

# Last 20 runs
./htb-replicator logs --config config.yaml --last 20

# Filter to a single destination
./htb-replicator logs --config config.yaml --dest sftp-backup
```

Sample output:

```
┌────────────────────────────────────────────────────────────────────────┐
│  HTB-Replicator — Sync History  (showing 2 of last 10)               │
└────────────────────────────────────────────────────────────────────────┘

  #1   2026-03-13 10:30:00  duration: 45s  [  OK  ]
       listed: 1248    submitted: 23
       source: s3
         → local-backup        [local]  ✓   12 synced   ✗    0 failed  18.4 MB
         → s3-replica          [s3  ]   ✓   11 synced   ✗    0 failed  16.2 MB
         → sftp-backup         [sftp]   ✓    9 synced   ✗    2 failed

  #2   2026-03-12 10:30:00  duration: 1m2s  [  OK  ]
       listed: 1225    submitted: 5
       source: s3
         → local-backup        [local]  ✓    5 synced   ✗    0 failed   4.1 MB
         → s3-replica          [s3  ]   ✓    5 synced   ✗    0 failed   4.1 MB
         → sftp-backup         [sftp]   ✓    5 synced   ✗    0 failed   4.1 MB
```

### Health check

```bash
# Quick health check
./htb-replicator health --config config.yaml

# Include full source bucket stats (object count, total size — slower)
./htb-replicator health --config config.yaml --full-stats
```

Sample output:

```
┌─────────────────────────────────────────────────────────────────┐
│ htb-replicator health report — 2026-03-13 10:30:00             │
└─────────────────────────────────────────────────────────────────┘

  STATUS  COMPONENT                   DETAIL
  ──────  ─────────                   ──────
  [ OK ]  Config                      OK  workers=5  cron: "0 2 * * *"  destinations=3
  [WARN]  Service (systemd)           enabled but NOT running — run: systemctl start htb-replicator
  [ OK ]  Metadata DB                 OK  path=/var/lib/htb-replicator/replicator.db  latency=2ms
  [ OK ]  Source (S3)                 OK  http://minio:9000 / source-bucket  latency=18ms
  [ OK ]  Dest (local)   [local-bkp]  OK  local:///data/backups  latency=1ms
  [FAIL]  Dest (sftp)    [sftp-bkp]  UNREACHABLE  sftp://backup-server:22/backup
```

### Verify destination consistency

```bash
./htb-replicator verify --config config.yaml
# Exit 0 = consistent
# Exit 1 = discrepancies found
```

---

## Install as a System Service

### Linux — systemd

#### Option A: one-line installer (recommended)

Downloads the correct binary for your architecture, installs it, creates the service user, writes the systemd unit, and enables it at boot:

```bash
curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash
```

#### Option B: manual

```bash
# Copy binary
sudo cp htb-replicator /usr/local/bin/htb-replicator
sudo chmod 755 /usr/local/bin/htb-replicator

# Config
sudo mkdir -p /etc/htb-replicator
sudo cp config.yaml /etc/htb-replicator/config.yaml

# Service (from repo)
sudo cp deploy/systemd/htb-replicator.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable htb-replicator
```

#### Managing the service

```bash
sudo systemctl start   htb-replicator
sudo systemctl stop    htb-replicator
sudo systemctl restart htb-replicator
sudo systemctl status  htb-replicator

# View logs
journalctl -u htb-replicator -f
journalctl -u htb-replicator --since "1 hour ago"
```

#### Uninstall

```bash
curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash -s -- --uninstall
```

---

### macOS — launchd

#### Option A: one-line installer (recommended)

```bash
curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash
```

Installs a system-level LaunchDaemon to `/Library/LaunchDaemons/` (starts at boot, not just login).

#### Option B: manual plist

```bash
sudo cp deploy/launchd/com.htb.htb-replicator.plist /Library/LaunchDaemons/
sudo launchctl load /Library/LaunchDaemons/com.htb.htb-replicator.plist
```

#### Managing the service

```bash
sudo launchctl start  com.htb.htb-replicator
sudo launchctl stop   com.htb.htb-replicator
launchctl list        com.htb.htb-replicator

# View logs
tail -f /var/log/htb-replicator/stdout.log
tail -f /var/log/htb-replicator/stderr.log
```

#### Uninstall

```bash
curl -sSL https://github.com/htb/htb-replicator/releases/latest/download/install.sh | sudo bash -s -- --uninstall
```

---

### Windows — NSSM

[NSSM](https://nssm.cc/) wraps any executable as a Windows service.

#### Install

```powershell
# Install NSSM first
winget install NSSM.NSSM

# Run PowerShell as Administrator
.\deploy\windows\install-service.ps1 `
  -Binary "C:\Program Files\htb-replicator\htb-replicator.exe" `
  -Config "C:\ProgramData\htb-replicator\config.yaml"
```

#### Managing the service

```powershell
nssm start  htb-replicator
nssm stop   htb-replicator
nssm status htb-replicator

# View logs
Get-Content "C:\ProgramData\htb-replicator\stdout.log" -Wait
```

#### Uninstall

```powershell
nssm stop htb-replicator
nssm remove htb-replicator confirm
```

---

## Docker / Docker Compose

### Build the Docker image

```bash
docker build -t htb-replicator:latest .
```

### Run with Docker Compose

```bash
# Start all services (replicator + MinIO source)
docker compose up -d

# View logs
docker compose logs -f replicator

# Run a single cycle
docker compose run --rm replicator sync-once --config /config/config.yaml

# Check status
docker compose run --rm replicator status --config /config/config.yaml
```

Mount your config:

```yaml
# docker-compose.yml override
services:
  replicator:
    volumes:
      - ./config.yaml:/config/config.yaml:ro
      - replicator-data:/data
```

---

## Prometheus Metrics

Exposed at `http://localhost:2112/metrics` when the service is running.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `replication_objects_total` | counter | `destination`, `status` | Objects processed |
| `replication_bytes_total` | counter | `destination` | Bytes transferred |
| `replication_errors_total` | counter | `destination`, `error_type` | Errors by type |
| `replication_queue_size` | gauge | — | Tasks waiting in queue |
| `replication_workers_active` | gauge | — | Goroutines actively syncing |

```bash
# Quick check
curl -s localhost:2112/metrics | grep replication_

# Useful queries in Prometheus/Grafana
replication_objects_total{status="synced"}
rate(replication_bytes_total[5m])
replication_errors_total{error_type="write"}
```

---

## CLI Reference

```
htb-replicator [command] [flags]

Commands:
  start        Start the continuous replication daemon
  sync-once    Run exactly one sync cycle and exit (exit 0 = success, 1 = failure)
  status       Print sync statistics from the metadata store
  logs         Show colorful sync-run history with per-destination results
    -n, --last int     Number of most-recent runs to show (default 10)
        --dest string  Filter output to a single destination ID
  verify       Compare destination contents against metadata; exit 1 if mismatch
  health       Full system health check — config, service, source, DB, destinations
    --full-stats  Enumerate all source objects (count + size; slow on large buckets)
  service      Manage the system service
    install    Register and enable the service
    uninstall  Remove the service
    start      Start the service
    stop       Stop the service
    status     Show service status

Global flags:
  -c, --config string      Path to YAML config file (default: config.yaml)
      --log-level string   Override log level: debug | info | warn | error
```

### Exit codes

| Command | Exit 0 | Exit 1 |
|---------|--------|--------|
| `sync-once` | All objects synced | One or more failures |
| `health` | All checks passed | Any `[FAIL]` check |
| `verify` | Destinations consistent | Discrepancies found |

> `[WARN]` in health output (e.g. service not running) does **not** cause exit 1 — only `[FAIL]` does.

---

## Environment Variables

Every config key can be overridden via environment variable with the `REPLICATOR_` prefix. Nested keys use `_` as separator.

| Environment Variable | Config key |
|----------------------|-----------|
| `REPLICATOR_WORKERS` | `workers` |
| `REPLICATOR_SCHEDULE` | `schedule` |
| `REPLICATOR_INTERVAL_SECONDS` | `interval_seconds` |
| `REPLICATOR_METADATA_DB` | `metadata_db` |
| `REPLICATOR_METRICS_PORT` | `metrics_port` |
| `REPLICATOR_LOG_LEVEL` | `log_level` |
| `REPLICATOR_SOURCE_BUCKET` | `source.bucket` |
| `REPLICATOR_SOURCE_ACCESS_KEY_ID` | `source.access_key_id` |
| `REPLICATOR_SOURCE_SECRET_ACCESS_KEY` | `source.secret_access_key` |
| `REPLICATOR_ALERTS_TELEGRAM_TOKEN` | `alerts.telegram.token` |

```bash
export REPLICATOR_SOURCE_ACCESS_KEY_ID=mykey
export REPLICATOR_SOURCE_SECRET_ACCESS_KEY=mysecret
./htb-replicator start --config config.yaml
```

In systemd:

```ini
[Service]
Environment=REPLICATOR_SOURCE_SECRET_ACCESS_KEY=mysecret
EnvironmentFile=/etc/htb-replicator/secrets.env
```

---

## Troubleshooting

### Service fails to start

```bash
journalctl -u htb-replicator -n 50
/usr/local/bin/htb-replicator sync-once --config /etc/htb-replicator/config.yaml --log-level debug
```

### `database is locked` errors

```bash
ps aux | grep htb-replicator
rm /var/lib/htb-replicator/replicator.db-wal
rm /var/lib/htb-replicator/replicator.db-shm
```

### Objects not syncing

```bash
./htb-replicator sync-once --config config.yaml --log-level debug
./htb-replicator status --config config.yaml
./htb-replicator verify --config config.yaml
```

### Reset and re-sync everything

```bash
sudo systemctl stop htb-replicator
rm /var/lib/htb-replicator/replicator.db
sudo systemctl start htb-replicator
```

### Cron expression not firing

```bash
./htb-replicator sync-once --config config.yaml
./htb-replicator start --config config.yaml --log-level debug 2>&1 | head -5
```

---

## Project Layout

```
htb-replicator/
├── cmd/replicator/main.go
├── cli/
│   ├── root.go                     config loading, logger init
│   ├── start.go                    daemon loop (cron or ticker) + banner
│   ├── sync_once.go                single-shot cycle
│   ├── status.go                   metadata stats table
│   ├── logs.go                     colorful sync-run history
│   ├── verify.go                   consistency check
│   ├── health.go                   full system health report
│   └── service.go                  systemd/launchd/NSSM installer
├── config/                         config struct + validation
├── metrics/                        Prometheus metric definitions
├── internal/
│   ├── replicator/                 orchestration + sync loop
│   ├── destinations/               Destination interface + implementations
│   │   ├── local/
│   │   ├── s3/
│   │   ├── ftp/
│   │   └── sftp/
│   ├── sources/                    Source implementations (s3, ftp, sftp, local)
│   ├── metadata/                   SQLite store + auto-migrations
│   ├── worker/                     bounded goroutine pool
│   ├── alerts/                     Telegram, Slack, email, webhook
│   └── retention/                  age-based deletion policy
├── pkg/
│   ├── checksum/                   ETag normalizer
│   ├── backoff/                    exponential backoff
│   └── stream/                     counting + limited readers
├── deploy/
│   ├── install.sh                  one-line installer (auto-download from GitHub)
│   ├── systemd/htb-replicator.service
│   ├── launchd/com.htb.htb-replicator.plist
│   └── windows/install-service.ps1
├── .github/workflows/build.yml     multi-arch CI + release automation
├── Dockerfile
└── docker-compose.yml
```
