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

// slackAlerter posts alerts to a Slack Incoming Webhook URL.
type slackAlerter struct {
	webhookURL string
	httpClient *http.Client
}

// NewSlackAlerter creates a Slack alerter from the given config.
func NewSlackAlerter(cfg config.SlackConfig) Alerter {
	return &slackAlerter{
		webhookURL: cfg.WebhookURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// slackPayload is the JSON body sent to the Slack webhook.
type slackPayload struct {
	Text        string            `json:"text"`
	Attachments []slackAttachment `json:"attachments,omitempty"`
}

type slackAttachment struct {
	Color  string       `json:"color"`
	Fields []slackField `json:"fields"`
}

type slackField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// Send posts a formatted Slack message to the webhook URL.
func (s *slackAlerter) Send(ctx context.Context, alert Alert) error {
	color := slackColor(alert.Level)

	fields := []slackField{
		{Title: "Level", Value: alert.Level, Short: true},
		{Title: "Destination", Value: alert.Destination, Short: true},
		{Title: "Message", Value: alert.Message, Short: false},
	}
	if alert.ObjectKey != "" {
		fields = append(fields, slackField{Title: "Object Key", Value: alert.ObjectKey, Short: false})
	}
	if alert.Error != nil {
		fields = append(fields, slackField{Title: "Error", Value: alert.Error.Error(), Short: false})
	}

	payload := slackPayload{
		Text: "*S3 Replicator Alert*",
		Attachments: []slackAttachment{
			{Color: color, Fields: fields},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("slack marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("slack create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("slack send: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack unexpected status %d", resp.StatusCode)
	}
	return nil
}

// Close is a no-op for the Slack alerter.
func (s *slackAlerter) Close() error { return nil }

func slackColor(level string) string {
	switch level {
	case "error":
		return "danger"
	case "warning":
		return "warning"
	case "info":
		return "good"
	default:
		return "#cccccc"
	}
}
