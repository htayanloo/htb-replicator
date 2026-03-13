package checksum

import (
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"
	"strings"
)

// ChecksumReader wraps an io.Reader and computes an MD5 checksum as bytes
// flow through it. This allows checksum verification without a second pass.
type ChecksumReader struct {
	r    io.Reader
	hash hash.Hash
}

// NewMD5Reader creates a new ChecksumReader backed by MD5.
func NewMD5Reader(r io.Reader) *ChecksumReader {
	return &ChecksumReader{
		r:    r,
		hash: md5.New(),
	}
}

// Read reads from the underlying reader and simultaneously updates the hash.
func (c *ChecksumReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		// hash.Write never returns an error.
		_, _ = c.hash.Write(p[:n])
	}
	return n, err
}

// MD5 returns the hex-encoded MD5 checksum of all bytes read so far.
func (c *ChecksumReader) MD5() string {
	return hex.EncodeToString(c.hash.Sum(nil))
}

// Sum returns the raw MD5 bytes.
func (c *ChecksumReader) Sum() []byte {
	return c.hash.Sum(nil)
}

// NormalizeETag strips surrounding double-quotes from an S3 ETag value.
// S3 returns ETags in the form `"abc123"`, but comparisons should be done
// against the raw hex string.
func NormalizeETag(etag string) string {
	return strings.Trim(etag, `"`)
}

// IsMultipartETag returns true if the ETag was produced by a multipart upload.
// Multipart ETags have the form "<md5>-<partCount>" and cannot be verified
// by computing the MD5 of the full object — only string equality is valid.
func IsMultipartETag(etag string) bool {
	return strings.Contains(etag, "-")
}

// MD5Hex computes the MD5 hex digest of a byte slice.
func MD5Hex(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}
