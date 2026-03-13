package sftp

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"path"
	"strings"
	"sync"
	"time"

	gossh "golang.org/x/crypto/ssh"
	gosftp "github.com/pkg/sftp"
	"github.com/htb/htb-replicator/config"
	"github.com/htb/htb-replicator/internal/destinations"
)

// sftpDest writes replicated objects to an SFTP server.
// It maintains a single persistent SSH/SFTP session with automatic reconnect.
type sftpDest struct {
	id       string
	host     string
	port     int
	username string
	password string
	basePath string

	mu     sync.Mutex
	ssh    *gossh.Client
	client *gosftp.Client
}

// New creates an SFTP destination from DestinationConfig.
// Required opts: host. Optional: port (default 22), username, password, base_path.
func New(cfg config.DestinationConfig) (destinations.Destination, error) {
	hostVal, ok := cfg.Opts["host"]
	if !ok {
		return nil, fmt.Errorf("sftp destination %q: opts.host is required", cfg.ID)
	}
	host, ok := hostVal.(string)
	if !ok || host == "" {
		return nil, fmt.Errorf("sftp destination %q: opts.host must be a non-empty string", cfg.ID)
	}

	port := 22
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

	d := &sftpDest{
		id:       cfg.ID,
		host:     host,
		port:     port,
		username: username,
		password: password,
		basePath: basePath,
	}

	return d, nil
}

func (d *sftpDest) ID() string   { return d.id }
func (d *sftpDest) Type() string { return "sftp" }

// connect (re)establishes the SSH+SFTP session if not already alive.
// Caller must hold d.mu.
func (d *sftpDest) connect() error {
	// Test if existing connection is still alive.
	if d.client != nil {
		if _, err := d.client.Getwd(); err == nil {
			return nil
		}
		_ = d.client.Close()
		_ = d.ssh.Close()
		d.client = nil
		d.ssh = nil
	}

	addr := net.JoinHostPort(d.host, strconv.Itoa(d.port))
	sshCfg := &gossh.ClientConfig{
		User: d.username,
		Auth: []gossh.AuthMethod{
			gossh.Password(d.password),
		},
		HostKeyCallback: gossh.InsecureIgnoreHostKey(), // In production, use a known-hosts verifier.
		Timeout:         30 * time.Second,
	}

	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		return fmt.Errorf("sftp dial %s: %w", addr, err)
	}

	sshConn, chans, reqs, err := gossh.NewClientConn(conn, addr, sshCfg)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("sftp ssh handshake %s: %w", addr, err)
	}
	sshClient := gossh.NewClient(sshConn, chans, reqs)

	sftpClient, err := gosftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return fmt.Errorf("sftp new client %s: %w", addr, err)
	}

	d.ssh = sshClient
	d.client = sftpClient
	return nil
}

// withClient executes fn with an active SFTP client, reconnecting if necessary.
func (d *sftpDest) withClient(fn func(*gosftp.Client) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.connect(); err != nil {
		return err
	}

	if err := fn(d.client); err != nil {
		// Attempt reconnect once on failure.
		_ = d.client.Close()
		_ = d.ssh.Close()
		d.client = nil
		d.ssh = nil

		if err2 := d.connect(); err2 != nil {
			return fmt.Errorf("sftp reconnect failed (%v): %w", err2, err)
		}
		return fn(d.client)
	}
	return nil
}

// remotePath builds the full remote path for an object key.
func (d *sftpDest) remotePath(key string) string {
	return path.Join(d.basePath, key)
}

// ensureDir creates parent directories on the remote SFTP server.
func ensureDir(client *gosftp.Client, remotePath string) error {
	dir := path.Dir(remotePath)
	parts := strings.Split(strings.TrimPrefix(dir, "/"), "/")
	current := "/"
	for _, part := range parts {
		if part == "" {
			continue
		}
		current = path.Join(current, part)
		_ = client.MkdirAll(current)
	}
	return nil
}

// Write uploads an object via SFTP using an atomic temp+rename pattern.
func (d *sftpDest) Write(ctx context.Context, obj destinations.Object, r io.Reader) (destinations.WriteResult, error) {
	var result destinations.WriteResult

	err := d.withClient(func(client *gosftp.Client) error {
		dest := d.remotePath(obj.Key)
		tmpPath := dest + ".tmp"

		if err := ensureDir(client, dest); err != nil {
			return err
		}

		f, err := client.Create(tmpPath)
		if err != nil {
			// Create may fail if parent dir does not exist.
			_ = ensureDir(client, dest)
			f, err = client.Create(tmpPath)
			if err != nil {
				return fmt.Errorf("sftp create tmp %q: %w", tmpPath, err)
			}
		}

		written, copyErr := io.Copy(f, r)
		_ = f.Close()

		if copyErr != nil {
			_ = client.Remove(tmpPath)
			return fmt.Errorf("sftp write %q: %w", tmpPath, copyErr)
		}

		if err := client.Rename(tmpPath, dest); err != nil {
			_ = client.Remove(tmpPath)
			return fmt.Errorf("sftp rename %q→%q: %w", tmpPath, dest, err)
		}

		result = destinations.WriteResult{BytesWritten: written}
		return nil
	})
	return result, err
}

// Exists checks if the key exists on the SFTP server.
func (d *sftpDest) Exists(ctx context.Context, key string) (string, bool, error) {
	var etag string
	var exists bool

	err := d.withClient(func(client *gosftp.Client) error {
		info, err := client.Stat(d.remotePath(key))
		if err != nil {
			exists = false
			return nil
		}
		exists = true
		// SFTP has no native ETag; use size+mtime as surrogate.
		etag = fmt.Sprintf("size-%d-mtime-%d", info.Size(), info.ModTime().Unix())
		return nil
	})
	return etag, exists, err
}

// Delete removes the object from the SFTP server.
func (d *sftpDest) Delete(ctx context.Context, key string) error {
	return d.withClient(func(client *gosftp.Client) error {
		if err := client.Remove(d.remotePath(key)); err != nil {
			return fmt.Errorf("sftp remove %q: %w", key, err)
		}
		return nil
	})
}

// ListKeys lists all files under the base path recursively.
func (d *sftpDest) ListKeys(ctx context.Context) ([]string, error) {
	var keys []string

	err := d.withClient(func(client *gosftp.Client) error {
		walker := client.Walk(d.basePath)
		for walker.Step() {
			if err := walker.Err(); err != nil {
				return err
			}
			if walker.Stat().IsDir() {
				continue
			}
			p := walker.Path()
			rel := strings.TrimPrefix(p, d.basePath)
			rel = strings.TrimPrefix(rel, "/")
			keys = append(keys, rel)
		}
		return nil
	})
	return keys, err
}

// Ping verifies connectivity by calling Getwd on the SFTP client.
func (d *sftpDest) Ping(ctx context.Context) error {
	return d.withClient(func(client *gosftp.Client) error {
		_, err := client.Getwd()
		return err
	})
}

// Close shuts down the SFTP client and underlying SSH connection.
func (d *sftpDest) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var lastErr error
	if d.client != nil {
		if err := d.client.Close(); err != nil {
			lastErr = err
		}
		d.client = nil
	}
	if d.ssh != nil {
		if err := d.ssh.Close(); err != nil {
			lastErr = err
		}
		d.ssh = nil
	}
	return lastErr
}
