PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS objects (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    object_key     TEXT    NOT NULL UNIQUE,
    etag           TEXT    NOT NULL,
    size           INTEGER NOT NULL,
    last_modified  DATETIME NOT NULL,
    sync_status    TEXT    NOT NULL DEFAULT 'pending'
                   CHECK(sync_status IN ('pending','syncing','synced','failed','retrying')),
    retry_count    INTEGER NOT NULL DEFAULT 0,
    synced_at      DATETIME,
    created_at     DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at     DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_objects_sync_status   ON objects(sync_status);
CREATE INDEX IF NOT EXISTS idx_objects_last_modified ON objects(last_modified);

CREATE TABLE IF NOT EXISTS destination_statuses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    object_key      TEXT    NOT NULL,
    destination_id  TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','syncing','synced','failed','retrying')),
    etag            TEXT,
    bytes_written   INTEGER,
    error_message   TEXT,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    last_attempt_at DATETIME,
    synced_at       DATETIME,
    created_at      DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at      DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    UNIQUE(object_key, destination_id)
);

CREATE INDEX IF NOT EXISTS idx_dest_object_key ON destination_statuses(object_key);
CREATE INDEX IF NOT EXISTS idx_dest_status     ON destination_statuses(status);

CREATE TABLE IF NOT EXISTS sync_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      DATETIME NOT NULL,
    finished_at     DATETIME,
    objects_listed  INTEGER  NOT NULL DEFAULT 0,
    tasks_submitted INTEGER  NOT NULL DEFAULT 0,
    status          TEXT     NOT NULL DEFAULT 'running'
                    CHECK(status IN ('running','completed','failed')),
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);
