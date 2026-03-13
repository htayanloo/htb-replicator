# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

# Install build dependencies. CGO is disabled because we use modernc.org/sqlite
# (pure Go), so no C toolchain is required.
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \

    GONOSUMCHECK=* \
    GONOSUMDB=* \
    GONOPROXY=""

WORKDIR /src

# Cache dependency downloads separately from the build.
COPY go.mod go.sum ./
RUN go mod download

# Copy the full source tree and build.
COPY . .
RUN go build \
    -ldflags="-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -trimpath \
    -o /bin/replicator \
    ./cmd/replicator

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM alpine:3.20

# ca-certificates: needed for HTTPS calls to S3 / Telegram / Slack / etc.
# tzdata: needed for time zone aware retention enforcement.
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S replicator && \
    adduser  -S replicator -G replicator

WORKDIR /app

COPY --from=builder /bin/replicator /usr/local/bin/replicator

# Default data and config directories.
RUN mkdir -p /data /config && \
    chown -R replicator:replicator /data /config /app

USER replicator

# Prometheus metrics endpoint.
EXPOSE 2112

# Data volume for the SQLite metadata database and local destination backups.
VOLUME ["/data"]

ENTRYPOINT ["replicator"]
CMD ["start", "--config", "/config/config.yaml"]
