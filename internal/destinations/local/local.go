package local

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/htb/htb-replicator/config"
	"github.com/htb/htb-replicator/internal/destinations"
)

// localDest writes replicated objects to the local filesystem.
type localDest struct {
	id   string
	root string
}

// New creates a new local filesystem destination. The root directory path
// must be provided as opts["path"].
func New(cfg config.DestinationConfig) (destinations.Destination, error) {
	pathVal, ok := cfg.Opts["path"]
	if !ok {
		return nil, fmt.Errorf("local destination %q: opts.path is required", cfg.ID)
	}
	root, ok := pathVal.(string)
	if !ok || root == "" {
		return nil, fmt.Errorf("local destination %q: opts.path must be a non-empty string", cfg.ID)
	}

	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("local destination %q: create root dir %q: %w", cfg.ID, root, err)
	}

	return &localDest{id: cfg.ID, root: root}, nil
}

func (d *localDest) ID() string   { return d.id }
func (d *localDest) Type() string { return "local" }

// Write streams r into the destination using an atomic write pattern:
// data is written to a temp file (.tmp/<uuid>-filename) then renamed into place.
func (d *localDest) Write(ctx context.Context, obj destinations.Object, r io.Reader) (destinations.WriteResult, error) {
	destPath := filepath.Join(d.root, filepath.FromSlash(obj.Key))

	// Ensure parent directories exist, removing any stale marker-files that
	// block directory creation (left over from pre-fix writes of "folder" keys).
	if err := ensureDir(filepath.Dir(destPath)); err != nil {
		return destinations.WriteResult{}, fmt.Errorf("mkdir parent for %q: %w", obj.Key, err)
	}

	// Create the .tmp directory next to the destination.
	tmpDir := filepath.Join(filepath.Dir(destPath), ".tmp")
	if err := ensureDir(tmpDir); err != nil {
		return destinations.WriteResult{}, fmt.Errorf("mkdir tmp for %q: %w", obj.Key, err)
	}

	tmpName := filepath.Join(tmpDir, randomHex(8)+"-"+filepath.Base(destPath))
	tmpFile, err := os.OpenFile(tmpName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return destinations.WriteResult{}, fmt.Errorf("create tmp file for %q: %w", obj.Key, err)
	}

	// Always clean up the temp file on error.
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	hasher := md5.New()
	mw := io.MultiWriter(tmpFile, hasher)

	written, err := copyWithContext(ctx, mw, r)
	if err != nil {
		_ = tmpFile.Close()
		return destinations.WriteResult{}, fmt.Errorf("write tmp file for %q: %w", obj.Key, err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return destinations.WriteResult{}, fmt.Errorf("sync tmp file for %q: %w", obj.Key, err)
	}
	if err := tmpFile.Close(); err != nil {
		return destinations.WriteResult{}, fmt.Errorf("close tmp file for %q: %w", obj.Key, err)
	}

	// Atomic rename into the final position.
	if err := os.Rename(tmpName, destPath); err != nil {
		return destinations.WriteResult{}, fmt.Errorf("rename tmp→dest for %q: %w", obj.Key, err)
	}
	committed = true

	etag := hex.EncodeToString(hasher.Sum(nil))
	return destinations.WriteResult{ETag: etag, BytesWritten: written}, nil
}

// Exists checks whether the object key is already present on disk and returns
// its MD5 ETag (computed by reading the file).
func (d *localDest) Exists(ctx context.Context, key string) (string, bool, error) {
	// Directory markers (keys ending with "/") are never stored as files.
	if strings.HasSuffix(key, "/") {
		return "", false, nil
	}

	destPath := filepath.Join(d.root, filepath.FromSlash(key))
	f, err := os.Open(destPath)
	if os.IsNotExist(err) || isNotDirErr(err) {
		// isNotDirErr: a path component is a stale marker-file, not a directory.
		// Treat as not-present; Write() will repair the path via ensureDir.
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("open %q for existence check: %w", key, err)
	}
	defer f.Close()

	// Guard against a directory existing where a file is expected.
	fi, err := f.Stat()
	if err != nil {
		return "", false, fmt.Errorf("stat %q: %w", key, err)
	}
	if fi.IsDir() {
		return "", false, nil
	}

	hasher := md5.New()
	if _, err := copyWithContext(ctx, hasher, f); err != nil {
		return "", false, fmt.Errorf("compute md5 for %q: %w", key, err)
	}
	etag := hex.EncodeToString(hasher.Sum(nil))
	return etag, true, nil
}

// Delete removes the object from the local filesystem.
func (d *localDest) Delete(ctx context.Context, key string) error {
	destPath := filepath.Join(d.root, filepath.FromSlash(key))
	if err := os.Remove(destPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	return nil
}

// ListKeys walks the root directory and returns relative paths of all files,
// excluding .tmp directories.
func (d *localDest) ListKeys(ctx context.Context) ([]string, error) {
	var keys []string

	err := filepath.Walk(d.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Skip .tmp directories.
		if info.IsDir() && info.Name() == ".tmp" {
			return filepath.SkipDir
		}
		if info.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(d.root, path)
		if err != nil {
			return err
		}
		// Normalise to forward-slash keys.
		keys = append(keys, strings.ReplaceAll(rel, string(filepath.Separator), "/"))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	return keys, nil
}

// Ping verifies that the root directory is accessible.
func (d *localDest) Ping(_ context.Context) error {
	if _, err := os.Stat(d.root); err != nil {
		return fmt.Errorf("local destination %q unreachable: %w", d.root, err)
	}
	return nil
}

// Close is a no-op for the local destination.
func (d *localDest) Close() error { return nil }

// isNotDirErr reports whether err is an ENOTDIR error, which occurs when a
// regular file sits at a path component that the OS expects to be a directory.
func isNotDirErr(err error) bool {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return errors.Is(pe.Err, syscall.ENOTDIR)
	}
	return false
}

// ensureDir is a drop-in replacement for os.MkdirAll that also handles the
// case where a regular file (a stale S3 directory-marker write) is blocking
// creation of a path component. The blocking file is removed and MkdirAll is
// retried once.
func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err == nil {
		return nil
	}
	// Walk each component and remove the first regular file we find where a
	// directory is required.
	parts := strings.Split(filepath.ToSlash(dir), "/")
	cur := ""
	for _, part := range parts {
		if part == "" {
			cur = "/"
			continue
		}
		if cur == "" || cur == "/" {
			cur += part
		} else {
			cur = cur + "/" + part
		}
		fi, statErr := os.Lstat(cur)
		if os.IsNotExist(statErr) {
			break
		}
		if statErr != nil {
			return statErr
		}
		if !fi.IsDir() {
			if rmErr := os.Remove(cur); rmErr != nil {
				return fmt.Errorf("remove stale marker file %q: %w", cur, rmErr)
			}
			break
		}
	}
	return os.MkdirAll(dir, 0o755)
}

// randomHex returns a hex-encoded random string of n bytes.
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// copyWithContext copies from src to dst but checks for context cancellation
// between 32 KiB chunks.
func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			written, writeErr := dst.Write(buf[:n])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
		}
		if readErr == io.EOF {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}
