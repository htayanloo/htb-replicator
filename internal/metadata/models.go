package metadata

import "time"

// SyncStatus represents the replication state of an object or destination record.
type SyncStatus string

const (
	StatusPending  SyncStatus = "pending"
	StatusSyncing  SyncStatus = "syncing"
	StatusSynced   SyncStatus = "synced"
	StatusFailed   SyncStatus = "failed"
	StatusRetrying SyncStatus = "retrying"
)

// ObjectRecord represents a source object and its overall sync state.
type ObjectRecord struct {
	ID           int64
	ObjectKey    string
	ETag         string
	Size         int64
	LastModified time.Time
	SyncStatus   SyncStatus
	RetryCount   int
	SyncedAt     *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// DestinationRecord represents the sync state of an object for a specific destination.
type DestinationRecord struct {
	ID            int64
	ObjectKey     string
	DestinationID string
	Status        SyncStatus
	ETag          string
	BytesWritten  int64
	ErrorMessage  string
	RetryCount    int
	LastAttemptAt *time.Time
	SyncedAt      *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// SyncRun records a single execution of the sync cycle.
type SyncRun struct {
	ID             int64
	StartedAt      time.Time
	FinishedAt     *time.Time
	ObjectsListed  int
	TasksSubmitted int
	Status         string
	ErrorMessage   string
}

// DestinationRunStat holds per-destination counts for a single sync run window.
type DestinationRunStat struct {
	DestinationID string
	Synced        int64
	Failed        int64
	BytesWritten  int64
}

// StoreStats aggregates counts from the metadata store for status reporting.
type StoreStats struct {
	TotalObjects      int64
	PendingObjects    int64
	SyncingObjects    int64
	SyncedObjects     int64
	FailedObjects     int64
	RetryingObjects   int64
	TotalDestStatuses int64
	SyncedDestStatuses int64
	FailedDestStatuses int64
}
