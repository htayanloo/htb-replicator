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
	"github.com/htb/htb-replicator/config"
	"github.com/htb/htb-replicator/internal/destinations"
)

// ftpDest writes replicated objects to an FTP server.
// FTP connections are NOT goroutine-safe, so a sync.Pool is used to manage
// a pool of reusable connections.
type ftpDest struct {
	id       string
	host     string
	port     int
	username string
	password string
	basePath string
	timeout  time.Duration

	mu   sync.Mutex
	pool []*goftp.ServerConn
}

// New creates an FTP destination from DestinationConfig.
// Required opts: host. Optional: port (default 21), username, password, base_path.
func New(cfg config.DestinationConfig) (destinations.Destination, error) {
	hostVal, ok := cfg.Opts["host"]
	if !ok {
		return nil, fmt.Errorf("ftp destination %q: opts.host is required", cfg.ID)
	}
	host, ok := hostVal.(string)
	if !ok || host == "" {
		return nil, fmt.Errorf("ftp destination %q: opts.host must be a non-empty string", cfg.ID)
	}

	port := 21
	if pv, ok := cfg.Opts["port"]; ok {
		switch v := pv.(type) {
		case int:
			port = v
		case float64:
			port = int(v)
		}
	}

	username, _ := cfg.Opts["username"].(string)
	password, _ := cfg.Opts["password"].(string)
	basePath, _ := cfg.Opts["base_path"].(string)
	if basePath == "" {
		basePath = "/"
	}

	timeout := 30 * time.Second

	return &ftpDest{
		id:       cfg.ID,
		host:     host,
		port:     port,
		username: username,
		password: password,
		basePath: basePath,
		timeout:  timeout,
	}, nil
}

func (d *ftpDest) ID() string   { return d.id }
func (d *ftpDest) Type() string { return "ftp" }

// getConn retrieves a connection from the pool or dials a new one.
func (d *ftpDest) getConn() (*goftp.ServerConn, error) {
	d.mu.Lock()
	if len(d.pool) > 0 {
		conn := d.pool[len(d.pool)-1]
		d.pool = d.pool[:len(d.pool)-1]
		d.mu.Unlock()
		// Verify it is still alive with NOOP.
		if err := conn.NoOp(); err == nil {
			return conn, nil
		}
		// Connection is stale; close and dial fresh.
		_ = conn.Quit()
	} else {
		d.mu.Unlock()
	}

	return d.dial()
}

// putConn returns a connection back to the pool.
func (d *ftpDest) putConn(conn *goftp.ServerConn) {
	if conn == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pool = append(d.pool, conn)
}

// dial opens a new authenticated FTP connection.
func (d *ftpDest) dial() (*goftp.ServerConn, error) {
	addr := fmt.Sprintf("%s:%d", d.host, d.port)
	conn, err := goftp.Dial(addr, goftp.DialWithTimeout(d.timeout))
	if err != nil {
		return nil, fmt.Errorf("ftp dial %s: %w", addr, err)
	}

	user := d.username
	if user == "" {
		user = "anonymous"
	}
	if err := conn.Login(user, d.password); err != nil {
		_ = conn.Quit()
		return nil, fmt.Errorf("ftp login to %s: %w", addr, err)
	}
	return conn, nil
}

// remotePath builds the full remote path for an object key.
func (d *ftpDest) remotePath(key string) string {
	return path.Join(d.basePath, key)
}

// ensureDir creates all parent directories needed for the given remote path.
func (d *ftpDest) ensureDir(conn *goftp.ServerConn, remotePath string) error {
	dir := path.Dir(remotePath)
	parts := strings.Split(strings.TrimPrefix(dir, "/"), "/")
	current := "/"
	for _, part := range parts {
		if part == "" {
			continue
		}
		current = path.Join(current, part)
		// MakeDir succeeds even if the dir exists on most FTP servers.
		_ = conn.MakeDir(current)
	}
	return nil
}

// Write uploads an object to the FTP server using an atomic temp+rename pattern.
func (d *ftpDest) Write(ctx context.Context, obj destinations.Object, r io.Reader) (destinations.WriteResult, error) {
	conn, err := d.getConn()
	if err != nil {
		return destinations.WriteResult{}, fmt.Errorf("ftp get conn: %w", err)
	}
	defer d.putConn(conn)

	dest := d.remotePath(obj.Key)
	tmpPath := dest + ".tmp"

	if err := d.ensureDir(conn, dest); err != nil {
		return destinations.WriteResult{}, err
	}

	if err := conn.Stor(tmpPath, r); err != nil {
		return destinations.WriteResult{}, fmt.Errorf("ftp stor %q: %w", tmpPath, err)
	}

	// Rename temp file to final destination (atomic on most FTP servers).
	if err := conn.Rename(tmpPath, dest); err != nil {
		// Best-effort cleanup.
		_ = conn.Delete(tmpPath)
		return destinations.WriteResult{}, fmt.Errorf("ftp rename %q→%q: %w", tmpPath, dest, err)
	}

	return destinations.WriteResult{BytesWritten: obj.Size}, nil
}

// Exists checks if the key already exists on the FTP server.
func (d *ftpDest) Exists(ctx context.Context, key string) (string, bool, error) {
	conn, err := d.getConn()
	if err != nil {
		return "", false, fmt.Errorf("ftp get conn: %w", err)
	}
	defer d.putConn(conn)

	dest := d.remotePath(key)
	size, err := conn.FileSize(dest)
	if err != nil {
		// FileSize returns an error for non-existent files.
		return "", false, nil
	}
	// FTP does not provide an ETag; return size as a surrogate.
	return fmt.Sprintf("size-%d", size), true, nil
}

// Delete removes the object from the FTP server.
func (d *ftpDest) Delete(ctx context.Context, key string) error {
	conn, err := d.getConn()
	if err != nil {
		return fmt.Errorf("ftp get conn: %w", err)
	}
	defer d.putConn(conn)

	if err := conn.Delete(d.remotePath(key)); err != nil {
		return fmt.Errorf("ftp delete %q: %w", key, err)
	}
	return nil
}

// ListKeys lists all files under the base path recursively.
func (d *ftpDest) ListKeys(ctx context.Context) ([]string, error) {
	conn, err := d.getConn()
	if err != nil {
		return nil, fmt.Errorf("ftp get conn: %w", err)
	}
	defer d.putConn(conn)

	return d.walkDir(conn, d.basePath)
}

func (d *ftpDest) walkDir(conn *goftp.ServerConn, dir string) ([]string, error) {
	entries, err := conn.List(dir)
	if err != nil {
		return nil, fmt.Errorf("ftp list %q: %w", dir, err)
	}

	var keys []string
	for _, entry := range entries {
		if entry.Name == "." || entry.Name == ".." {
			continue
		}
		fullPath := path.Join(dir, entry.Name)
		if entry.Type == goftp.EntryTypeFolder {
			sub, err := d.walkDir(conn, fullPath)
			if err != nil {
				return nil, err
			}
			keys = append(keys, sub...)
		} else {
			// Return key relative to basePath.
			rel := strings.TrimPrefix(fullPath, d.basePath)
			rel = strings.TrimPrefix(rel, "/")
			keys = append(keys, rel)
		}
	}
	return keys, nil
}

// Ping verifies that the FTP server is reachable by dialing and immediately
// returning the connection to the pool.
func (d *ftpDest) Ping(ctx context.Context) error {
	conn, err := d.getConn()
	if err != nil {
		return err
	}
	d.putConn(conn)
	return nil
}

// Close shuts down all pooled FTP connections.
func (d *ftpDest) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var lastErr error
	for _, conn := range d.pool {
		if err := conn.Quit(); err != nil {
			lastErr = err
		}
	}
	d.pool = nil
	return lastErr
}
