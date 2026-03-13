package destinations

import (
	"context"
	"io"
	"time"
)

// Object carries the metadata for a source object being written to a destination.
type Object struct {
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
	ContentType  string
}

// WriteResult holds the outcome of a Write call.
type WriteResult struct {
	ETag         string
	BytesWritten int64
}

// Destination defines the interface every replication target must implement.
type Destination interface {
	// ID returns the unique identifier configured for this destination.
	ID() string

	// Type returns the destination type string (local, s3, ftp, sftp).
	Type() string

	// Write streams obj body from r into the destination, returning ETag and bytes written.
	Write(ctx context.Context, obj Object, r io.Reader) (WriteResult, error)

	// Exists checks whether the object identified by key is already present.
	// Returns the stored ETag and whether it was found.
	Exists(ctx context.Context, key string) (etag string, exists bool, err error)

	// Delete removes the object identified by key from the destination.
	Delete(ctx context.Context, key string) error

	// ListKeys enumerates all object keys currently held by the destination.
	ListKeys(ctx context.Context) ([]string, error)

	// Ping verifies that the destination is reachable.
	Ping(ctx context.Context) error

	// Close releases any held resources (connections, file handles, etc.).
	Close() error
}

