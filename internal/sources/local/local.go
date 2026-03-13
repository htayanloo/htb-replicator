// Package local provides a local filesystem (or NFS mount) source implementation.
package local

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/htb/htb-replicator/internal/source"
)

// localSource reads objects from a local filesystem directory.
type localSource struct {
	basePath string
}

// New creates a local filesystem source.
// Required opts: path.
func New(opts map[string]interface{}) (source.Source, error) {
	pathVal, ok := opts["path"]
	if !ok {
		return nil, fmt.Errorf("local source: opts.path is required")
	}
	basePath, ok := pathVal.(string)
	if !ok || basePath == "" {
		return nil, fmt.Errorf("local source: opts.path must be a non-empty string")
	}
	return &localSource{basePath: basePath}, nil
}

// pseudoETag generates a pseudo-ETag from file size and mtime.
// Format "<size>-<mtime_ns>" looks like a multipart S3 ETag (contains "-"),
// so checksum.IsMultipartETag() returns true and MD5 verification is skipped.
func pseudoETag(size int64, mtime time.Time) string {
	return fmt.Sprintf("%d-%d", size, mtime.UnixNano())
}

// ListAll walks the base path and returns a SourceObject for every file.
// Symlinks are NOT followed to avoid infinite loops.
func (s *localSource) ListAll(ctx context.Context) ([]source.SourceObject, error) {
	var objects []source.SourceObject

	err := filepath.WalkDir(s.basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() || d.Type()&fs.ModeSymlink != 0 {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat %q: %w", path, err)
		}

		rel, err := filepath.Rel(s.basePath, path)
		if err != nil {
			return err
		}
		key := strings.ReplaceAll(rel, string(filepath.Separator), "/")

		objects = append(objects, source.SourceObject{
			Key:          key,
			ETag:         pseudoETag(info.Size(), info.ModTime()),
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("local source list: %w", err)
	}
	return objects, nil
}

// GetObject opens the file and returns a ReadCloser and its size.
func (s *localSource) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	fullPath := filepath.Join(s.basePath, filepath.FromSlash(key))
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, 0, fmt.Errorf("local source open %q: %w", key, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("local source stat %q: %w", key, err)
	}
	return f, info.Size(), nil
}

// HeadObject returns metadata for the given key by calling os.Stat.
func (s *localSource) HeadObject(ctx context.Context, key string) (source.SourceObject, error) {
	fullPath := filepath.Join(s.basePath, filepath.FromSlash(key))
	info, err := os.Stat(fullPath)
	if err != nil {
		return source.SourceObject{}, fmt.Errorf("local source head %q: %w", key, err)
	}
	return source.SourceObject{
		Key:          key,
		ETag:         pseudoETag(info.Size(), info.ModTime()),
		Size:         info.Size(),
		LastModified: info.ModTime(),
	}, nil
}

// DeleteObject removes the file at the given key path.
func (s *localSource) DeleteObject(ctx context.Context, key string) error {
	fullPath := filepath.Join(s.basePath, filepath.FromSlash(key))
	if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local source delete %q: %w", key, err)
	}
	return nil
}

// Ping verifies that the base path is accessible.
func (s *localSource) Ping(ctx context.Context) error {
	if _, err := os.Stat(s.basePath); err != nil {
		return fmt.Errorf("local source unreachable %q: %w", s.basePath, err)
	}
	return nil
}

// Close is a no-op for the local source.
func (s *localSource) Close() error { return nil }
