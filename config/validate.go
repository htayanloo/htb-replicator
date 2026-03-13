package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/robfig/cron/v3"
)

// Validate checks that all required fields are present and values are sane.
func (c *Config) Validate() error {
	var errs []string

	// Source validation
	validSourceTypes := map[string]bool{"s3": true, "sftp": true, "ftp": true, "local": true}
	if c.Source.Type == "" {
		errs = append(errs, "source.type is required — migrate your config: move source fields under source.opts and add source.type: s3")
	} else if !validSourceTypes[c.Source.Type] {
		errs = append(errs, fmt.Sprintf("source.type %q is not valid (must be s3, sftp, ftp, or local)", c.Source.Type))
	} else {
		requireOpt := func(key, context string) {
			if _, ok := c.Source.Opts[key]; !ok {
				errs = append(errs, fmt.Sprintf("source (%s) requires opts.%s", context, key))
			}
		}
		switch c.Source.Type {
		case "s3":
			requireOpt("bucket", "s3")
			if _, hasRegion := c.Source.Opts["region"]; !hasRegion {
				if _, hasEndpoint := c.Source.Opts["endpoint"]; !hasEndpoint {
					errs = append(errs, "source (s3) requires opts.region or opts.endpoint")
				}
			}
		case "sftp":
			requireOpt("host", "sftp")
		case "ftp":
			requireOpt("host", "ftp")
		case "local":
			requireOpt("path", "local")
		}
	}

	// Destinations validation
	if len(c.Destinations) == 0 {
		errs = append(errs, "at least one destination is required")
	}
	seenIDs := make(map[string]bool)
	for i, dst := range c.Destinations {
		if dst.ID == "" {
			errs = append(errs, fmt.Sprintf("destinations[%d].id is required", i))
		} else if seenIDs[dst.ID] {
			errs = append(errs, fmt.Sprintf("destinations[%d].id %q is duplicated", i, dst.ID))
		} else {
			seenIDs[dst.ID] = true
		}
		if dst.Type == "" {
			errs = append(errs, fmt.Sprintf("destinations[%d].type is required", i))
		} else {
			switch dst.Type {
			case "local", "s3", "ftp", "sftp":
				// valid
			default:
				errs = append(errs, fmt.Sprintf("destinations[%d].type %q is not valid (must be local, s3, ftp, or sftp)", i, dst.Type))
			}
		}

		// Type-specific validation
		switch dst.Type {
		case "local":
			if _, ok := dst.Opts["path"]; !ok {
				errs = append(errs, fmt.Sprintf("destinations[%d] (local) requires opts.path", i))
			}
		case "s3":
			if _, ok := dst.Opts["bucket"]; !ok {
				errs = append(errs, fmt.Sprintf("destinations[%d] (s3) requires opts.bucket", i))
			}
		case "ftp":
			if _, ok := dst.Opts["host"]; !ok {
				errs = append(errs, fmt.Sprintf("destinations[%d] (ftp) requires opts.host", i))
			}
		case "sftp":
			if _, ok := dst.Opts["host"]; !ok {
				errs = append(errs, fmt.Sprintf("destinations[%d] (sftp) requires opts.host", i))
			}
		}
	}

	// Worker pool validation
	if c.Workers <= 0 {
		errs = append(errs, "workers must be greater than 0")
	}
	if c.Workers > 200 {
		errs = append(errs, "workers must not exceed 200")
	}

	// Schedule / interval validation.
	// Either schedule (cron expression) or interval_seconds must be set.
	if c.Schedule != "" {
		// Use the standard 5-field parser (minute hour dom month dow) plus
		// descriptors like @hourly, @daily, @every 6h.
		// Do NOT include cron.Second — that would require a 6-field expression.
		p := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		if _, err := p.Parse(c.Schedule); err != nil {
			errs = append(errs, fmt.Sprintf("schedule %q is not a valid cron expression: %v", c.Schedule, err))
		}
	} else if c.IntervalSecs <= 0 {
		errs = append(errs, "either schedule (cron expression) or interval_seconds (> 0) must be set")
	}

	if c.IntervalSecs < 0 {
		errs = append(errs, "interval_seconds must be >= 0")
	}

	// Metrics port validation
	if c.MetricsPort != 0 && (c.MetricsPort < 1024 || c.MetricsPort > 65535) {
		errs = append(errs, "metrics_port must be between 1024 and 65535 (or 0 to disable)")
	}

	// Log level validation
	if c.LogLevel != "" {
		validLevels := map[string]bool{
			"debug": true, "info": true, "warn": true, "error": true,
		}
		if !validLevels[strings.ToLower(c.LogLevel)] {
			errs = append(errs, fmt.Sprintf("log_level %q is not valid (must be debug, info, warn, or error)", c.LogLevel))
		}
	}

	// Alerts validation
	if c.Alerts.ErrorThreshold < 0 {
		errs = append(errs, "alerts.error_threshold must be >= 0")
	}
	if c.Alerts.CooldownMins < 0 {
		errs = append(errs, "alerts.cooldown_minutes must be >= 0")
	}
	if c.Alerts.Telegram != nil {
		if c.Alerts.Telegram.Token == "" {
			errs = append(errs, "alerts.telegram.token is required when telegram alerting is configured")
		}
		if c.Alerts.Telegram.ChatID == 0 {
			errs = append(errs, "alerts.telegram.chat_id is required when telegram alerting is configured")
		}
	}
	if c.Alerts.Slack != nil {
		if c.Alerts.Slack.WebhookURL == "" {
			errs = append(errs, "alerts.slack.webhook_url is required when slack alerting is configured")
		}
	}
	if c.Alerts.Email != nil {
		if c.Alerts.Email.Host == "" {
			errs = append(errs, "alerts.email.host is required when email alerting is configured")
		}
		if c.Alerts.Email.From == "" {
			errs = append(errs, "alerts.email.from is required when email alerting is configured")
		}
		if len(c.Alerts.Email.To) == 0 {
			errs = append(errs, "alerts.email.to requires at least one recipient when email alerting is configured")
		}
	}
	if c.Alerts.Webhook != nil {
		if c.Alerts.Webhook.URL == "" {
			errs = append(errs, "alerts.webhook.url is required when webhook alerting is configured")
		}
	}

	// Retention validation
	if c.Retention.SourceDays < 0 {
		errs = append(errs, "retention.source_days must be >= 0")
	}
	if c.Retention.DestinationDays < 0 {
		errs = append(errs, "retention.destination_days must be >= 0")
	}

	if len(errs) > 0 {
		return errors.New("configuration validation failed:\n  - " + strings.Join(errs, "\n  - "))
	}
	return nil
}
