// Package source defines the generic read-only interface that every source
// type (S3, SFTP, FTP, local) must implement.
package source

import (
	"context"
	"io"
	"time"
)

// SourceObject carries metadata for one object from the source.
type SourceObject struct {
	Key          string
	ETag         string    // real MD5 for S3; "<size>-<mtime_ns>" for sftp/ftp/local
	Size         int64
	LastModified time.Time
}

// Source is the read-only interface every source type must implement.
type Source interface {
	// ListAll returns all objects visible under the configured path/prefix.
	ListAll(ctx context.Context) ([]SourceObject, error)

	// GetObject returns a streaming body and content length for the given key.
	// The caller is responsible for closing the returned ReadCloser.
	GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error)

	// HeadObject returns metadata for the given key without downloading its body.
	HeadObject(ctx context.Context, key string) (SourceObject, error)

	// DeleteObject permanently removes the object identified by key.
	DeleteObject(ctx context.Context, key string) error

	// Ping verifies that the source is reachable (lightweight connectivity check).
	Ping(ctx context.Context) error

	// Close releases any held resources (connections, file handles, etc.).
	Close() error
}
