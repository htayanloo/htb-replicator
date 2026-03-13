package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/htb/htb-replicator/config"
)

// webhookAlerter sends alerts via HTTP POST to a configurable URL.
type webhookAlerter struct {
	cfg        config.WebhookConfig
	httpClient *http.Client
}

// NewWebhookAlerter creates a webhook alerter from the given config.
func NewWebhookAlerter(cfg config.WebhookConfig) Alerter {
	return &webhookAlerter{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// webhookBody is the JSON payload sent to the webhook endpoint.
type webhookBody struct {
	Level       string `json:"level"`
	Destination string `json:"destination"`
	Message     string `json:"message"`
	ObjectKey   string `json:"object_key,omitempty"`
	Error       string `json:"error,omitempty"`
	Timestamp   string `json:"timestamp"`
}

// Send dispatches an HTTP POST with a JSON alert payload to the webhook URL.
func (w *webhookAlerter) Send(ctx context.Context, alert Alert) error {
	payload := webhookBody{
		Level:       alert.Level,
		Destination: alert.Destination,
		Message:     alert.Message,
		ObjectKey:   alert.ObjectKey,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
	}
	if alert.Error != nil {
		payload.Error = alert.Error.Error()
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("webhook marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Apply any custom headers from the configuration.
	for k, v := range w.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("webhook send: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook unexpected status %d for URL %s", resp.StatusCode, w.cfg.URL)
	}
	return nil
}

// Close is a no-op for the webhook alerter.
func (w *webhookAlerter) Close() error { return nil }
