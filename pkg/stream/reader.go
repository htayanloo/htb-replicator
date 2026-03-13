package stream

import (
	"errors"
	"io"
)

// ErrLimitExceeded is returned when a read would exceed the configured limit.
var ErrLimitExceeded = errors.New("stream: read limit exceeded")

// LimitedReader wraps an io.Reader and returns ErrLimitExceeded once the
// cumulative bytes read exceeds maxBytes. Unlike io.LimitedReader, it returns
// an error instead of silently stopping at EOF.
type LimitedReader struct {
	r        io.Reader
	maxBytes int64
	read     int64
}

// NewLimitedReader creates a LimitedReader that allows at most maxBytes to be read.
func NewLimitedReader(r io.Reader, maxBytes int64) *LimitedReader {
	return &LimitedReader{r: r, maxBytes: maxBytes}
}

// Read reads from the underlying reader, enforcing the byte limit.
func (l *LimitedReader) Read(p []byte) (int, error) {
	if l.read >= l.maxBytes {
		return 0, ErrLimitExceeded
	}

	remaining := l.maxBytes - l.read
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	n, err := l.r.Read(p)
	l.read += int64(n)

	if l.read >= l.maxBytes && err == nil {
		// We've hit the limit; signal so on next call.
		return n, nil
	}
	return n, err
}

// BytesRead returns the number of bytes read so far.
func (l *LimitedReader) BytesRead() int64 {
	return l.read
}

// CountingReader wraps an io.Reader and counts the total bytes read.
// It is safe to read the count at any time via BytesRead().
type CountingReader struct {
	r    io.Reader
	n    int64
}

// NewCountingReader creates a CountingReader backed by the given reader.
func NewCountingReader(r io.Reader) *CountingReader {
	return &CountingReader{r: r}
}

// Read reads from the underlying reader and increments the byte counter.
func (c *CountingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// BytesRead returns the total number of bytes read through this reader.
func (c *CountingReader) BytesRead() int64 {
	return c.n
}

// ProgressReader wraps an io.Reader and calls a callback after each read
// with the number of bytes read so far. Useful for progress bars or metrics.
type ProgressReader struct {
	r        io.Reader
	total    int64
	callback func(bytesRead int64)
}

// NewProgressReader creates a ProgressReader. The callback is called after each
// successful Read with the cumulative bytes read.
func NewProgressReader(r io.Reader, callback func(bytesRead int64)) *ProgressReader {
	return &ProgressReader{r: r, callback: callback}
}

// Read reads from the underlying reader and invokes the progress callback.
func (p *ProgressReader) Read(buf []byte) (int, error) {
	n, err := p.r.Read(buf)
	if n > 0 {
		p.total += int64(n)
		if p.callback != nil {
			p.callback(p.total)
		}
	}
	return n, err
}

// BytesRead returns the total bytes read through this reader.
func (p *ProgressReader) BytesRead() int64 {
	return p.total
}
