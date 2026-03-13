// Package ftp provides an FTP source implementation.
package ftp

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	goftp "github.com/jlaffaye/ftp"

	"github.com/htb/htb-replicator/internal/source"
)

// ftpSource reads objects from an FTP server.
// FTP connections are NOT goroutine-safe; all operations are serialised through
// a mutex. The source is called from the main sync goroutine, not workers, so
// contention is minimal.
type ftpSource struct {
	host     string
	port     int
	username string
	password string
	basePath string
	timeout  time.Duration

	mu   sync.Mutex
	conn *goftp.ServerConn
}

// New creates an FTP source.
// Required opts: host. Optional: port (default 21), username, password, base_path.
func New(opts map[string]interface{}) (source.Source, error) {
	getString := func(key string) string {
		v, _ := opts[key]
		s, _ := v.(string)
		return s
	}

	host := getString("host")
	if host == "" {
		return nil, fmt.Errorf("ftp source: opts.host is required")
	}

	port := 21
	if pv, ok := opts["port"]; ok {
		switch v := pv.(type) {
		case int:
			port = v
		case float64:
			port = int(v)
		}
	}

	basePath := getString("base_path")
	if basePath == "" {
		basePath = "/"
	}

	return &ftpSource{
		host:     host,
		port:     port,
		username: getString("username"),
		password: getString("password"),
		basePath: basePath,
		timeout:  30 * time.Second,
	}, nil
}

// pseudoETag generates a pseudo-ETag from file size and mtime.
// When mtime is zero (server does not provide it) only size is used.
func pseudoETag(size int64, mtime time.Time) string {
	if mtime.IsZero() {
		return fmt.Sprintf("%d", size)
	}
	return fmt.Sprintf("%d-%d", size, mtime.UnixNano())
}

// connect (re)establishes the FTP connection if not already alive.
// Caller must hold f.mu.
func (f *ftpSource) connect() error {
	if f.conn != nil {
		if err := f.conn.NoOp(); err == nil {
			return nil
		}
		_ = f.conn.Quit()
		f.conn = nil
	}

	addr := fmt.Sprintf("%s:%d", f.host, f.port)
	conn, err := goftp.Dial(addr, goftp.DialWithTimeout(f.timeout))
	if err != nil {
		return fmt.Errorf("ftp source dial %s: %w", addr, err)
	}

	if f.username != "" {
		if err := conn.Login(f.username, f.password); err != nil {
			_ = conn.Quit()
			return fmt.Errorf("ftp source login %s: %w", addr, err)
		}
	}

	f.conn = conn
	return nil
}

// withConn executes fn with an active FTP connection, reconnecting on error.
func (f *ftpSource) withConn(fn func(*goftp.ServerConn) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.connect(); err != nil {
		return err
	}
	if err := fn(f.conn); err != nil {
		// Attempt one reconnect.
		_ = f.conn.Quit()
		f.conn = nil
		if err2 := f.connect(); err2 != nil {
			return fmt.Errorf("ftp source reconnect failed (%v): %w", err2, err)
		}
		return fn(f.conn)
	}
	return nil
}

// remotePath builds the full remote path for an object key.
func (f *ftpSource) remotePath(key string) string {
	return path.Join(f.basePath, key)
}

// ListAll enumerates all files under the base path using MLSD (recursive).
// Falls back to NLST if MLSD is not supported by the server.
func (f *ftpSource) ListAll(ctx context.Context) ([]source.SourceObject, error) {
	var objects []source.SourceObject

	err := f.withConn(func(conn *goftp.ServerConn) error {
		return f.walkDir(ctx, conn, f.basePath, &objects)
	})
	if err != nil {
		return nil, fmt.Errorf("ftp source list: %w", err)
	}
	return objects, nil
}

// walkDir recursively lists a directory using MLSD.
func (f *ftpSource) walkDir(ctx context.Context, conn *goftp.ServerConn, dir string, objects *[]source.SourceObject) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entries, err := conn.List(dir)
	if err != nil {
		return fmt.Errorf("ftp list %q: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.Name == "." || entry.Name == ".." {
			continue
		}

		fullPath := path.Join(dir, entry.Name)

		switch entry.Type {
		case goftp.EntryTypeFolder:
			if err := f.walkDir(ctx, conn, fullPath, objects); err != nil {
				return err
			}
		case goftp.EntryTypeFile:
			rel := strings.TrimPrefix(fullPath, f.basePath)
			rel = strings.TrimPrefix(rel, "/")

			*objects = append(*objects, source.SourceObject{
				Key:          rel,
				ETag:         pseudoETag(int64(entry.Size), entry.Time),
				Size:         int64(entry.Size),
				LastModified: entry.Time,
			})
		}
	}
	return nil
}

// GetObject retrieves a file from the FTP server and returns a ReadCloser.
func (f *ftpSource) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	var (
		rc   io.ReadCloser
		size int64
	)
	err := f.withConn(func(conn *goftp.ServerConn) error {
		resp, err := conn.Retr(f.remotePath(key))
		if err != nil {
			return fmt.Errorf("ftp retr %q: %w", key, err)
		}
		rc = resp

		// Try to get size via entry listing.
		entries, listErr := conn.List(f.remotePath(key))
		if listErr == nil && len(entries) == 1 {
			size = int64(entries[0].Size)
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return rc, size, nil
}

// HeadObject returns metadata for the given key via MLST (single entry stat).
func (f *ftpSource) HeadObject(ctx context.Context, key string) (source.SourceObject, error) {
	var obj source.SourceObject
	err := f.withConn(func(conn *goftp.ServerConn) error {
		entries, err := conn.List(f.remotePath(key))
		if err != nil {
			return fmt.Errorf("ftp stat %q: %w", key, err)
		}
		if len(entries) == 0 {
			return fmt.Errorf("ftp: object %q not found", key)
		}
		e := entries[0]
		obj = source.SourceObject{
			Key:          key,
			ETag:         pseudoETag(int64(e.Size), e.Time),
			Size:         int64(e.Size),
			LastModified: e.Time,
		}
		return nil
	})
	return obj, err
}

// DeleteObject removes the remote file.
func (f *ftpSource) DeleteObject(ctx context.Context, key string) error {
	return f.withConn(func(conn *goftp.ServerConn) error {
		if err := conn.Delete(f.remotePath(key)); err != nil {
			return fmt.Errorf("ftp delete %q: %w", key, err)
		}
		return nil
	})
}

// Ping verifies connectivity via a NOOP command.
func (f *ftpSource) Ping(ctx context.Context) error {
	return f.withConn(func(conn *goftp.ServerConn) error {
		return conn.NoOp()
	})
}

// Close terminates the FTP session.
func (f *ftpSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.conn != nil {
		err := f.conn.Quit()
		f.conn = nil
		return err
	}
	return nil
}
