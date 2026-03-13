// Package sftp provides an SFTP source implementation.
package sftp

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	gosftp "github.com/pkg/sftp"
	gossh "golang.org/x/crypto/ssh"

	"github.com/htb/htb-replicator/internal/source"
)

// sftpSource reads objects from an SFTP server.
// It maintains a single persistent SSH+SFTP session with automatic reconnect.
// All operations are serialised through a mutex because the source is called
// from the main sync goroutine (not from concurrent workers).
type sftpSource struct {
	host     string
	port     int
	username string
	password string
	keyFile  string
	basePath string

	mu     sync.Mutex
	ssh    *gossh.Client
	client *gosftp.Client
}

// New creates an SFTP source.
// Required opts: host. Optional: port (default 22), username, password, key_file, base_path.
func New(opts map[string]interface{}) (source.Source, error) {
	getString := func(key string) string {
		v, _ := opts[key]
		s, _ := v.(string)
		return s
	}

	host := getString("host")
	if host == "" {
		return nil, fmt.Errorf("sftp source: opts.host is required")
	}

	port := 22
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

	return &sftpSource{
		host:     host,
		port:     port,
		username: getString("username"),
		password: getString("password"),
		keyFile:  getString("key_file"),
		basePath: basePath,
	}, nil
}

// pseudoETag generates a pseudo-ETag from file size and mtime.
func pseudoETag(size int64, mtime time.Time) string {
	return fmt.Sprintf("%d-%d", size, mtime.UnixNano())
}

// connect (re)establishes the SSH+SFTP session if not already alive.
// Caller must hold s.mu.
func (s *sftpSource) connect() error {
	if s.client != nil {
		if _, err := s.client.Getwd(); err == nil {
			return nil
		}
		_ = s.client.Close()
		_ = s.ssh.Close()
		s.client = nil
		s.ssh = nil
	}

	addr := net.JoinHostPort(s.host, fmt.Sprintf("%d", s.port))

	authMethods := []gossh.AuthMethod{}
	if s.password != "" {
		authMethods = append(authMethods, gossh.Password(s.password))
	}

	sshCfg := &gossh.ClientConfig{
		User:            s.username,
		Auth:            authMethods,
		HostKeyCallback: gossh.InsecureIgnoreHostKey(), //nolint:gosec — document in README
		Timeout:         30 * time.Second,
	}

	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		return fmt.Errorf("sftp source dial %s: %w", addr, err)
	}

	sshConn, chans, reqs, err := gossh.NewClientConn(conn, addr, sshCfg)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("sftp source ssh handshake %s: %w", addr, err)
	}
	sshClient := gossh.NewClient(sshConn, chans, reqs)

	sftpClient, err := gosftp.NewClient(sshClient)
	if err != nil {
		_ = sshClient.Close()
		return fmt.Errorf("sftp source new client %s: %w", addr, err)
	}

	s.ssh = sshClient
	s.client = sftpClient
	return nil
}

// withClient executes fn with an active SFTP client, reconnecting on error.
func (s *sftpSource) withClient(fn func(*gosftp.Client) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.connect(); err != nil {
		return err
	}
	if err := fn(s.client); err != nil {
		// Attempt one reconnect.
		_ = s.client.Close()
		_ = s.ssh.Close()
		s.client = nil
		s.ssh = nil

		if err2 := s.connect(); err2 != nil {
			return fmt.Errorf("sftp source reconnect failed (%v): %w", err2, err)
		}
		return fn(s.client)
	}
	return nil
}

// remotePath builds the full remote path for an object key.
func (s *sftpSource) remotePath(key string) string {
	return s.basePath + "/" + key
}

// ListAll walks the base path recursively and returns a SourceObject per file.
func (s *sftpSource) ListAll(ctx context.Context) ([]source.SourceObject, error) {
	var objects []source.SourceObject

	err := s.withClient(func(client *gosftp.Client) error {
		walker := client.Walk(s.basePath)
		for walker.Step() {
			if err := walker.Err(); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			info := walker.Stat()
			if info.IsDir() {
				continue
			}

			p := walker.Path()
			rel := strings.TrimPrefix(p, s.basePath)
			rel = strings.TrimPrefix(rel, "/")

			objects = append(objects, source.SourceObject{
				Key:          rel,
				ETag:         pseudoETag(info.Size(), info.ModTime()),
				Size:         info.Size(),
				LastModified: info.ModTime(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sftp source list: %w", err)
	}
	return objects, nil
}

// GetObject opens a remote file and returns a ReadCloser and its size.
func (s *sftpSource) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	var (
		rc   io.ReadCloser
		size int64
	)
	err := s.withClient(func(client *gosftp.Client) error {
		f, err := client.Open(s.remotePath(key))
		if err != nil {
			return fmt.Errorf("sftp open %q: %w", key, err)
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("sftp stat %q: %w", key, err)
		}
		rc = f
		size = info.Size()
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return rc, size, nil
}

// HeadObject fetches metadata for the given key via sftp.Stat.
func (s *sftpSource) HeadObject(ctx context.Context, key string) (source.SourceObject, error) {
	var obj source.SourceObject
	err := s.withClient(func(client *gosftp.Client) error {
		info, err := client.Stat(s.remotePath(key))
		if err != nil {
			return fmt.Errorf("sftp stat %q: %w", key, err)
		}
		obj = source.SourceObject{
			Key:          key,
			ETag:         pseudoETag(info.Size(), info.ModTime()),
			Size:         info.Size(),
			LastModified: info.ModTime(),
		}
		return nil
	})
	return obj, err
}

// DeleteObject removes the remote file.
func (s *sftpSource) DeleteObject(ctx context.Context, key string) error {
	return s.withClient(func(client *gosftp.Client) error {
		if err := client.Remove(s.remotePath(key)); err != nil {
			return fmt.Errorf("sftp remove %q: %w", key, err)
		}
		return nil
	})
}

// Ping verifies connectivity by stat-ing the base path.
func (s *sftpSource) Ping(ctx context.Context) error {
	return s.withClient(func(client *gosftp.Client) error {
		_, err := client.Stat(s.basePath)
		return err
	})
}

// Close shuts down the SFTP client and underlying SSH connection.
func (s *sftpSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var lastErr error
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			lastErr = err
		}
		s.client = nil
	}
	if s.ssh != nil {
		if err := s.ssh.Close(); err != nil {
			lastErr = err
		}
		s.ssh = nil
	}
	return lastErr
}
