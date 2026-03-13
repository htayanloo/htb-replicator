package alerts

import (
	"context"
	"fmt"

	gomail "github.com/wneessen/go-mail"
	"github.com/htb/htb-replicator/config"
)

// emailAlerter sends alerts via SMTP using the go-mail library.
type emailAlerter struct {
	cfg    config.EmailConfig
	client *gomail.Client
}

// NewEmailAlerter creates an SMTP-backed alerter from the given config.
func NewEmailAlerter(cfg config.EmailConfig) (Alerter, error) {
	if cfg.Host == "" {
		return nil, fmt.Errorf("email alerter: host is required")
	}
	if cfg.From == "" {
		return nil, fmt.Errorf("email alerter: from is required")
	}
	if len(cfg.To) == 0 {
		return nil, fmt.Errorf("email alerter: at least one recipient is required")
	}

	port := cfg.Port
	if port == 0 {
		port = 587
	}

	opts := []gomail.Option{
		gomail.WithPort(port),
		gomail.WithSMTPAuth(gomail.SMTPAuthPlain),
		gomail.WithUsername(cfg.Username),
		gomail.WithPassword(cfg.Password),
	}

	// Use TLS on port 465, STARTTLS otherwise.
	if port == 465 {
		opts = append(opts, gomail.WithSSLPort(true))
	} else {
		opts = append(opts, gomail.WithTLSPortPolicy(gomail.TLSMandatory))
	}

	client, err := gomail.NewClient(cfg.Host, opts...)
	if err != nil {
		return nil, fmt.Errorf("email alerter: create client: %w", err)
	}

	return &emailAlerter{cfg: cfg, client: client}, nil
}

// Send dispatches an alert email to all configured recipients.
func (e *emailAlerter) Send(ctx context.Context, alert Alert) error {
	subject := fmt.Sprintf("[S3 Replicator] [%s] Alert: %s", alert.Level, alert.Destination)

	body := fmt.Sprintf(
		"S3 Replicator Alert\n\nLevel: %s\nDestination: %s\nMessage: %s",
		alert.Level, alert.Destination, alert.Message,
	)
	if alert.ObjectKey != "" {
		body += fmt.Sprintf("\nObject Key: %s", alert.ObjectKey)
	}
	if alert.Error != nil {
		body += fmt.Sprintf("\nError: %v", alert.Error)
	}

	msg := gomail.NewMsg()
	if err := msg.From(e.cfg.From); err != nil {
		return fmt.Errorf("email set from: %w", err)
	}
	if err := msg.To(e.cfg.To...); err != nil {
		return fmt.Errorf("email set to: %w", err)
	}
	msg.Subject(subject)
	msg.SetBodyString(gomail.TypeTextPlain, body)

	if err := e.client.DialAndSendWithContext(ctx, msg); err != nil {
		return fmt.Errorf("email send: %w", err)
	}
	return nil
}

// Close is a no-op; the go-mail client does not hold persistent connections.
func (e *emailAlerter) Close() error { return nil }
