package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Store defines the interface for all metadata persistence operations.
type Store interface {
	// Object operations
	GetObject(ctx context.Context, key string) (*ObjectRecord, error)
	UpsertObject(ctx context.Context, obj ObjectRecord) error
	MarkObjectSynced(ctx context.Context, key string) error
	MarkObjectFailed(ctx context.Context, key string, retryCount int) error
	ListObjectsByStatus(ctx context.Context, status SyncStatus, limit int) ([]ObjectRecord, error)
	DeleteObject(ctx context.Context, key string) error

	// Destination status operations
	GetDestinationStatus(ctx context.Context, key, destID string) (*DestinationRecord, error)
	UpsertDestinationStatus(ctx context.Context, rec DestinationRecord) error
	MarkDestinationSynced(ctx context.Context, key, destID, etag string, bytesWritten int64) error
	MarkDestinationFailed(ctx context.Context, key, destID, errMsg string, retryCount int) error
	ListSyncedKeysForDestination(ctx context.Context, destID string) ([]string, error)

	// Sync run operations
	CreateSyncRun(ctx context.Context, startedAt time.Time) (int64, error)
	FinishSyncRun(ctx context.Context, id int64, objectsListed, tasksSubmitted int, status, errMsg string) error
	ListSyncRuns(ctx context.Context, limit int) ([]SyncRun, error)
	GetDestinationStatsInWindow(ctx context.Context, from, to time.Time) ([]DestinationRunStat, error)

	// Stats
	GetStats(ctx context.Context) (StoreStats, error)

	// Lifecycle
	Close() error
}

// sqliteStore is the SQLite-backed implementation of Store.
type sqliteStore struct {
	db *sql.DB
}

// NewSQLiteStore opens (or creates) the SQLite database at the given path
// and runs schema migrations.
func NewSQLiteStore(ctx context.Context, path string) (Store, error) {
	// Ensure the parent directory exists so SQLite doesn't fail with
	// "unable to open database file: out of memory" on a missing dir.
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create db directory %q: %w", dir, err)
		}
	}

	// Use a single write connection to avoid SQLITE_BUSY errors.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Limit to one connection for the write path.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	// Apply pragmas immediately after open.
	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = NORMAL",
	}
	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("pragma %q: %w", p, err)
		}
	}

	s := &sqliteStore{db: db}

	if err := runMigrations(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrations: %w", err)
	}

	return s, nil
}

func (s *sqliteStore) Close() error {
	return s.db.Close()
}

// ─── Object operations ────────────────────────────────────────────────────────

func (s *sqliteStore) GetObject(ctx context.Context, key string) (*ObjectRecord, error) {
	const q = `
		SELECT id, object_key, etag, size, last_modified, sync_status,
		       retry_count, synced_at, created_at, updated_at
		FROM objects WHERE object_key = ?`
	row := s.db.QueryRowContext(ctx, q, key)
	return scanObject(row)
}

func (s *sqliteStore) UpsertObject(ctx context.Context, obj ObjectRecord) error {
	const q = `
		INSERT INTO objects (object_key, etag, size, last_modified, sync_status, retry_count, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ','now'))
		ON CONFLICT(object_key) DO UPDATE SET
			etag         = excluded.etag,
			size         = excluded.size,
			last_modified= excluded.last_modified,
			sync_status  = excluded.sync_status,
			retry_count  = excluded.retry_count,
			updated_at   = strftime('%Y-%m-%dT%H:%M:%SZ','now')`
	_, err := s.db.ExecContext(ctx, q,
		obj.ObjectKey, obj.ETag, obj.Size,
		obj.LastModified.UTC().Format(time.RFC3339),
		string(obj.SyncStatus), obj.RetryCount,
	)
	if err != nil {
		return fmt.Errorf("upsert object %q: %w", obj.ObjectKey, err)
	}
	return nil
}

func (s *sqliteStore) MarkObjectSynced(ctx context.Context, key string) error {
	const q = `
		UPDATE objects
		SET sync_status = 'synced',
		    synced_at   = strftime('%Y-%m-%dT%H:%M:%SZ','now'),
		    updated_at  = strftime('%Y-%m-%dT%H:%M:%SZ','now')
		WHERE object_key = ?`
	_, err := s.db.ExecContext(ctx, q, key)
	if err != nil {
		return fmt.Errorf("mark object synced %q: %w", key, err)
	}
	return nil
}

func (s *sqliteStore) MarkObjectFailed(ctx context.Context, key string, retryCount int) error {
	status := StatusFailed
	if retryCount > 0 {
		status = StatusRetrying
	}
	const q = `
		UPDATE objects
		SET sync_status = ?,
		    retry_count = ?,
		    updated_at  = strftime('%Y-%m-%dT%H:%M:%SZ','now')
		WHERE object_key = ?`
	_, err := s.db.ExecContext(ctx, q, string(status), retryCount, key)
	if err != nil {
		return fmt.Errorf("mark object failed %q: %w", key, err)
	}
	return nil
}

func (s *sqliteStore) ListObjectsByStatus(ctx context.Context, status SyncStatus, limit int) ([]ObjectRecord, error) {
	const q = `
		SELECT id, object_key, etag, size, last_modified, sync_status,
		       retry_count, synced_at, created_at, updated_at
		FROM objects WHERE sync_status = ?
		ORDER BY updated_at ASC
		LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, string(status), limit)
	if err != nil {
		return nil, fmt.Errorf("list objects by status: %w", err)
	}
	defer rows.Close()
	return scanObjects(rows)
}

func (s *sqliteStore) DeleteObject(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM destination_statuses WHERE object_key = ?`, key)
	if err != nil {
		return fmt.Errorf("delete destination statuses for %q: %w", key, err)
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM objects WHERE object_key = ?`, key)
	if err != nil {
		return fmt.Errorf("delete object %q: %w", key, err)
	}
	return nil
}

// ─── Destination status operations ───────────────────────────────────────────

func (s *sqliteStore) GetDestinationStatus(ctx context.Context, key, destID string) (*DestinationRecord, error) {
	const q = `
		SELECT id, object_key, destination_id, status, etag, bytes_written,
		       error_message, retry_count, last_attempt_at, synced_at, created_at, updated_at
		FROM destination_statuses
		WHERE object_key = ? AND destination_id = ?`
	row := s.db.QueryRowContext(ctx, q, key, destID)
	return scanDestination(row)
}

func (s *sqliteStore) UpsertDestinationStatus(ctx context.Context, rec DestinationRecord) error {
	now := time.Now().UTC().Format(time.RFC3339)
	var lastAttempt interface{}
	if rec.LastAttemptAt != nil {
		lastAttempt = rec.LastAttemptAt.UTC().Format(time.RFC3339)
	}
	const q = `
		INSERT INTO destination_statuses
		    (object_key, destination_id, status, etag, bytes_written, error_message, retry_count, last_attempt_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(object_key, destination_id) DO UPDATE SET
		    status          = excluded.status,
		    etag            = excluded.etag,
		    bytes_written   = excluded.bytes_written,
		    error_message   = excluded.error_message,
		    retry_count     = excluded.retry_count,
		    last_attempt_at = excluded.last_attempt_at,
		    updated_at      = excluded.updated_at`
	_, err := s.db.ExecContext(ctx, q,
		rec.ObjectKey, rec.DestinationID, string(rec.Status),
		rec.ETag, rec.BytesWritten, rec.ErrorMessage,
		rec.RetryCount, lastAttempt, now,
	)
	if err != nil {
		return fmt.Errorf("upsert destination status %q/%q: %w", rec.ObjectKey, rec.DestinationID, err)
	}
	return nil
}

func (s *sqliteStore) MarkDestinationSynced(ctx context.Context, key, destID, etag string, bytesWritten int64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	const q = `
		INSERT INTO destination_statuses
		    (object_key, destination_id, status, etag, bytes_written, last_attempt_at, synced_at, updated_at)
		VALUES (?, ?, 'synced', ?, ?, ?, ?, ?)
		ON CONFLICT(object_key, destination_id) DO UPDATE SET
		    status        = 'synced',
		    etag          = excluded.etag,
		    bytes_written = excluded.bytes_written,
		    last_attempt_at = excluded.last_attempt_at,
		    synced_at     = excluded.synced_at,
		    updated_at    = excluded.updated_at`
	_, err := s.db.ExecContext(ctx, q, key, destID, etag, bytesWritten, now, now, now)
	if err != nil {
		return fmt.Errorf("mark destination synced %q/%q: %w", key, destID, err)
	}
	return nil
}

func (s *sqliteStore) MarkDestinationFailed(ctx context.Context, key, destID, errMsg string, retryCount int) error {
	now := time.Now().UTC().Format(time.RFC3339)
	status := StatusFailed
	if retryCount > 0 {
		status = StatusRetrying
	}
	const q = `
		INSERT INTO destination_statuses
		    (object_key, destination_id, status, error_message, retry_count, last_attempt_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(object_key, destination_id) DO UPDATE SET
		    status          = excluded.status,
		    error_message   = excluded.error_message,
		    retry_count     = excluded.retry_count,
		    last_attempt_at = excluded.last_attempt_at,
		    updated_at      = excluded.updated_at`
	_, err := s.db.ExecContext(ctx, q, key, destID, string(status), errMsg, retryCount, now, now)
	if err != nil {
		return fmt.Errorf("mark destination failed %q/%q: %w", key, destID, err)
	}
	return nil
}

func (s *sqliteStore) ListSyncedKeysForDestination(ctx context.Context, destID string) ([]string, error) {
	const q = `SELECT object_key FROM destination_statuses WHERE destination_id = ? AND status = 'synced'`
	rows, err := s.db.QueryContext(ctx, q, destID)
	if err != nil {
		return nil, fmt.Errorf("list synced keys for destination %q: %w", destID, err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// ─── Sync run operations ──────────────────────────────────────────────────────

func (s *sqliteStore) CreateSyncRun(ctx context.Context, startedAt time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO sync_runs (started_at, status) VALUES (?, 'running')`,
		startedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return 0, fmt.Errorf("create sync run: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}
	return id, nil
}

func (s *sqliteStore) FinishSyncRun(ctx context.Context, id int64, objectsListed, tasksSubmitted int, status, errMsg string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	const q = `
		UPDATE sync_runs
		SET finished_at     = ?,
		    objects_listed  = ?,
		    tasks_submitted = ?,
		    status          = ?,
		    error_message   = ?
		WHERE id = ?`
	_, err := s.db.ExecContext(ctx, q, now, objectsListed, tasksSubmitted, status, errMsg, id)
	if err != nil {
		return fmt.Errorf("finish sync run %d: %w", id, err)
	}
	return nil
}

func (s *sqliteStore) ListSyncRuns(ctx context.Context, limit int) ([]SyncRun, error) {
	const q = `
		SELECT id, started_at, finished_at, objects_listed, tasks_submitted, status, COALESCE(error_message,'')
		FROM sync_runs
		ORDER BY started_at DESC
		LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list sync runs: %w", err)
	}
	defer rows.Close()

	var runs []SyncRun
	for rows.Next() {
		var r SyncRun
		var startedAt string
		var finishedAt sql.NullString
		if err := rows.Scan(&r.ID, &startedAt, &finishedAt, &r.ObjectsListed, &r.TasksSubmitted, &r.Status, &r.ErrorMessage); err != nil {
			return nil, fmt.Errorf("scan sync run: %w", err)
		}
		r.StartedAt, _ = time.Parse(time.RFC3339, startedAt)
		if finishedAt.Valid {
			t, _ := time.Parse(time.RFC3339, finishedAt.String)
			r.FinishedAt = &t
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

func (s *sqliteStore) GetDestinationStatsInWindow(ctx context.Context, from, to time.Time) ([]DestinationRunStat, error) {
	const q = `
		SELECT
			destination_id,
			COALESCE(SUM(CASE WHEN status='synced' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status='failed' OR status='retrying' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status='synced' THEN bytes_written ELSE 0 END), 0)
		FROM destination_statuses
		WHERE updated_at >= ? AND updated_at <= ?
		GROUP BY destination_id
		ORDER BY destination_id`
	rows, err := s.db.QueryContext(ctx, q,
		from.UTC().Format(time.RFC3339),
		to.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return nil, fmt.Errorf("get destination stats in window: %w", err)
	}
	defer rows.Close()

	var stats []DestinationRunStat
	for rows.Next() {
		var d DestinationRunStat
		if err := rows.Scan(&d.DestinationID, &d.Synced, &d.Failed, &d.BytesWritten); err != nil {
			return nil, fmt.Errorf("scan destination stat: %w", err)
		}
		stats = append(stats, d)
	}
	return stats, rows.Err()
}

// ─── Stats ────────────────────────────────────────────────────────────────────

func (s *sqliteStore) GetStats(ctx context.Context) (StoreStats, error) {
	var stats StoreStats

	objQuery := `
		SELECT
			COUNT(*) AS total,
			COALESCE(SUM(CASE WHEN sync_status='pending'  THEN 1 ELSE 0 END), 0) AS pending,
			COALESCE(SUM(CASE WHEN sync_status='syncing'  THEN 1 ELSE 0 END), 0) AS syncing,
			COALESCE(SUM(CASE WHEN sync_status='synced'   THEN 1 ELSE 0 END), 0) AS synced,
			COALESCE(SUM(CASE WHEN sync_status='failed'   THEN 1 ELSE 0 END), 0) AS failed,
			COALESCE(SUM(CASE WHEN sync_status='retrying' THEN 1 ELSE 0 END), 0) AS retrying
		FROM objects`
	row := s.db.QueryRowContext(ctx, objQuery)
	if err := row.Scan(
		&stats.TotalObjects,
		&stats.PendingObjects,
		&stats.SyncingObjects,
		&stats.SyncedObjects,
		&stats.FailedObjects,
		&stats.RetryingObjects,
	); err != nil {
		return stats, fmt.Errorf("get object stats: %w", err)
	}

	destQuery := `
		SELECT
			COUNT(*) AS total,
			COALESCE(SUM(CASE WHEN status='synced' THEN 1 ELSE 0 END), 0) AS synced,
			COALESCE(SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END), 0) AS failed
		FROM destination_statuses`
	row = s.db.QueryRowContext(ctx, destQuery)
	if err := row.Scan(
		&stats.TotalDestStatuses,
		&stats.SyncedDestStatuses,
		&stats.FailedDestStatuses,
	); err != nil {
		return stats, fmt.Errorf("get destination stats: %w", err)
	}

	return stats, nil
}

// ─── Scan helpers ─────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...interface{}) error
}

func scanObject(row scanner) (*ObjectRecord, error) {
	var rec ObjectRecord
	var lastMod, createdAt, updatedAt string
	var syncedAt sql.NullString

	err := row.Scan(
		&rec.ID, &rec.ObjectKey, &rec.ETag, &rec.Size,
		&lastMod, &rec.SyncStatus, &rec.RetryCount,
		&syncedAt, &createdAt, &updatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan object: %w", err)
	}

	rec.LastModified, _ = time.Parse(time.RFC3339, lastMod)
	rec.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	rec.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
	if syncedAt.Valid {
		t, _ := time.Parse(time.RFC3339, syncedAt.String)
		rec.SyncedAt = &t
	}
	return &rec, nil
}

func scanObjects(rows *sql.Rows) ([]ObjectRecord, error) {
	var result []ObjectRecord
	for rows.Next() {
		var rec ObjectRecord
		var lastMod, createdAt, updatedAt string
		var syncedAt sql.NullString

		err := rows.Scan(
			&rec.ID, &rec.ObjectKey, &rec.ETag, &rec.Size,
			&lastMod, &rec.SyncStatus, &rec.RetryCount,
			&syncedAt, &createdAt, &updatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan object row: %w", err)
		}
		rec.LastModified, _ = time.Parse(time.RFC3339, lastMod)
		rec.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		rec.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
		if syncedAt.Valid {
			t, _ := time.Parse(time.RFC3339, syncedAt.String)
			rec.SyncedAt = &t
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}

func scanDestination(row scanner) (*DestinationRecord, error) {
	var rec DestinationRecord
	var createdAt, updatedAt string
	var lastAttemptAt, syncedAt sql.NullString
	var status string

	err := row.Scan(
		&rec.ID, &rec.ObjectKey, &rec.DestinationID,
		&status, &rec.ETag, &rec.BytesWritten,
		&rec.ErrorMessage, &rec.RetryCount,
		&lastAttemptAt, &syncedAt,
		&createdAt, &updatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan destination: %w", err)
	}

	rec.Status = SyncStatus(status)
	rec.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	rec.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
	if lastAttemptAt.Valid {
		t, _ := time.Parse(time.RFC3339, lastAttemptAt.String)
		rec.LastAttemptAt = &t
	}
	if syncedAt.Valid {
		t, _ := time.Parse(time.RFC3339, syncedAt.String)
		rec.SyncedAt = &t
	}
	return &rec, nil
}
