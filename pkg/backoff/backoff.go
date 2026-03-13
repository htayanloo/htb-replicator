package backoff

import (
	"context"
	"fmt"
	"time"

	cbackoff "github.com/cenkalti/backoff/v4"
)

// Config holds parameters for the exponential backoff strategy.
type Config struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	MaxRetries      uint64
}

// DefaultConfig returns sensible production defaults:
// initial=1s, max=60s, multiplier=2, maxRetries=5.
func DefaultConfig() Config {
	return Config{
		InitialInterval: time.Second,
		MaxInterval:     60 * time.Second,
		Multiplier:      2.0,
		MaxRetries:      5,
	}
}

// RetryFunc is the signature for operations passed to Retry.
type RetryFunc func() error

// Retry executes fn with exponential backoff until it succeeds, the retry
// limit is reached, or the context is cancelled.
func Retry(ctx context.Context, cfg Config, fn RetryFunc) error {
	b := newExponential(cfg)
	b.Reset()

	var attempt uint64
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled: %w", err)
		}

		if err := fn(); err != nil {
			attempt++
			if cfg.MaxRetries > 0 && attempt >= cfg.MaxRetries {
				return fmt.Errorf("max retries (%d) exceeded, last error: %w", cfg.MaxRetries, err)
			}

			wait := b.NextBackOff()
			if wait == cbackoff.Stop {
				return fmt.Errorf("backoff stopped, last error: %w", err)
			}

			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
			case <-time.After(wait):
				// continue to next attempt
			}
			continue
		}

		return nil
	}
}

// RetryWithNotify executes fn with exponential backoff, calling notify on each
// failure with the error and next wait duration.
func RetryWithNotify(ctx context.Context, cfg Config, fn RetryFunc, notify func(err error, wait time.Duration)) error {
	b := newExponential(cfg)
	b.Reset()

	var attempt uint64
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled: %w", err)
		}

		if err := fn(); err != nil {
			attempt++
			if cfg.MaxRetries > 0 && attempt >= cfg.MaxRetries {
				return fmt.Errorf("max retries (%d) exceeded, last error: %w", cfg.MaxRetries, err)
			}

			wait := b.NextBackOff()
			if wait == cbackoff.Stop {
				return fmt.Errorf("backoff stopped, last error: %w", err)
			}

			if notify != nil {
				notify(err, wait)
			}

			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during backoff: %w", ctx.Err())
			case <-time.After(wait):
			}
			continue
		}

		return nil
	}
}

func newExponential(cfg Config) *cbackoff.ExponentialBackOff {
	b := cbackoff.NewExponentialBackOff()
	b.InitialInterval = cfg.InitialInterval
	b.MaxInterval = cfg.MaxInterval
	b.Multiplier = cfg.Multiplier
	b.MaxElapsedTime = 0 // rely on MaxRetries instead
	return b
}
