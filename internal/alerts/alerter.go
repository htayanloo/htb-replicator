package alerts

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"github.com/htb/htb-replicator/config"
)

// Alert carries the details of a notification event.
type Alert struct {
	// Level is one of "error", "warning", or "info".
	Level string

	// Destination is the destination ID that triggered the alert.
	Destination string

	// Message is a human-readable description of the event.
	Message string

	// ObjectKey is the S3 key of the affected object (may be empty).
	ObjectKey string

	// Error is the underlying error, if any.
	Error error
}

// Alerter defines the interface for sending alerts.
type Alerter interface {
	// Send dispatches the alert to all configured backends.
	Send(ctx context.Context, alert Alert) error

	// Close releases any resources held by the alerter.
	Close() error
}

// multiAlerter fans out alerts to a list of child Alerters and enforces
// per-destination cooldown to prevent alert storms.
type multiAlerter struct {
	backends  []Alerter
	cooldown  time.Duration
	logger    *zap.Logger

	mu         sync.Mutex
	lastSentAt map[string]time.Time // keyed by destination ID
}

// NewMultiAlerter builds a fan-out Alerter from the application config.
// Each configured backend (Telegram, Slack, Email, Webhook) is added.
func NewMultiAlerter(cfg config.AlertsConfig, logger *zap.Logger) (Alerter, error) {
	cooldown := time.Duration(cfg.CooldownMins) * time.Minute
	if cooldown <= 0 {
		cooldown = 15 * time.Minute
	}

	var backends []Alerter

	if cfg.Telegram != nil {
		t, err := NewTelegramAlerter(*cfg.Telegram)
		if err != nil {
			return nil, fmt.Errorf("telegram alerter: %w", err)
		}
		backends = append(backends, t)
	}

	if cfg.Slack != nil {
		backends = append(backends, NewSlackAlerter(*cfg.Slack))
	}

	if cfg.Email != nil {
		e, err := NewEmailAlerter(*cfg.Email)
		if err != nil {
			return nil, fmt.Errorf("email alerter: %w", err)
		}
		backends = append(backends, e)
	}

	if cfg.Webhook != nil {
		backends = append(backends, NewWebhookAlerter(*cfg.Webhook))
	}

	return &multiAlerter{
		backends:   backends,
		cooldown:   cooldown,
		logger:     logger,
		lastSentAt: make(map[string]time.Time),
	}, nil
}

// Send dispatches the alert to all configured backends, respecting the cooldown
// period per destination. During cooldown, alerts are silently dropped.
func (m *multiAlerter) Send(ctx context.Context, alert Alert) error {
	if len(m.backends) == 0 {
		return nil
	}

	// Enforce cooldown per destination.
	m.mu.Lock()
	lastSent, seen := m.lastSentAt[alert.Destination]
	if seen && time.Since(lastSent) < m.cooldown {
		m.mu.Unlock()
		m.logger.Debug("alert suppressed by cooldown",
			zap.String("destination", alert.Destination),
			zap.Duration("remaining", m.cooldown-time.Since(lastSent)),
		)
		return nil
	}
	m.lastSentAt[alert.Destination] = time.Now()
	m.mu.Unlock()

	var lastErr error
	for _, backend := range m.backends {
		if err := backend.Send(ctx, alert); err != nil {
			m.logger.Error("alert backend error",
				zap.String("destination", alert.Destination),
				zap.Error(err),
			)
			lastErr = err
		}
	}
	return lastErr
}

// Close shuts down all alert backends.
func (m *multiAlerter) Close() error {
	var lastErr error
	for _, b := range m.backends {
		if err := b.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// noopAlerter discards all alerts. Used when no alerting is configured.
type noopAlerter struct{}

// NewNoopAlerter returns an Alerter that does nothing.
func NewNoopAlerter() Alerter { return &noopAlerter{} }

func (n *noopAlerter) Send(_ context.Context, _ Alert) error { return nil }
func (n *noopAlerter) Close() error                          { return nil }

// formatAlert renders an Alert to a human-readable string.
func formatAlert(a Alert) string {
	msg := fmt.Sprintf("[%s] Destination: %s\n%s", a.Level, a.Destination, a.Message)
	if a.ObjectKey != "" {
		msg += fmt.Sprintf("\nObject: %s", a.ObjectKey)
	}
	if a.Error != nil {
		msg += fmt.Sprintf("\nError: %v", a.Error)
	}
	return msg
}
