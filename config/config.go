package config

import (
	"time"
)

// Config is the top-level configuration for the replicator service.
type Config struct {
	Source       SourceConfig        `yaml:"source"       mapstructure:"source"`
	Destinations []DestinationConfig `yaml:"destinations" mapstructure:"destinations"`
	Workers      int                 `yaml:"workers"      mapstructure:"workers"`
	// IntervalSecs defines a fixed polling interval (seconds). Ignored when Schedule is set.
	IntervalSecs int    `yaml:"interval_seconds" mapstructure:"interval_seconds"`
	// Schedule is a cron expression (5 or 6 fields) that overrides IntervalSecs.
	// Examples: "0 2 * * *" (daily at 02:00), "@every 6h", "@hourly".
	// When set, the service runs exactly on the cron schedule. When empty,
	// IntervalSecs is used as a fixed-interval ticker.
	Schedule   string `yaml:"schedule"     mapstructure:"schedule"`
	MetadataDB string `yaml:"metadata_db"  mapstructure:"metadata_db"`
	MetricsPort  int                 `yaml:"metrics_port" mapstructure:"metrics_port"`
	LogLevel     string              `yaml:"log_level"    mapstructure:"log_level"`
	Alerts       AlertsConfig        `yaml:"alerts"       mapstructure:"alerts"`
	Retention    RetentionConfig     `yaml:"retention"    mapstructure:"retention"`
}

// SourceConfig holds generic configuration for the source, matching the
// type+opts pattern used by DestinationConfig.
// Supported types: s3, sftp, ftp, local.
type SourceConfig struct {
	Type string                 `yaml:"type" mapstructure:"type"`
	Opts map[string]interface{} `yaml:"opts" mapstructure:"opts"`
}

// DestinationConfig holds configuration for a single replication destination.
type DestinationConfig struct {
	ID   string                 `yaml:"id"   mapstructure:"id"`
	Type string                 `yaml:"type" mapstructure:"type"` // local, s3, ftp, sftp
	Opts map[string]interface{} `yaml:"opts" mapstructure:"opts"`
}

// AlertsConfig holds configuration for all alerting backends.
type AlertsConfig struct {
	ErrorThreshold int             `yaml:"error_threshold"  mapstructure:"error_threshold"`
	CooldownMins   int             `yaml:"cooldown_minutes" mapstructure:"cooldown_minutes"`
	Telegram       *TelegramConfig `yaml:"telegram"         mapstructure:"telegram"`
	Slack          *SlackConfig    `yaml:"slack"            mapstructure:"slack"`
	Email          *EmailConfig    `yaml:"email"            mapstructure:"email"`
	Webhook        *WebhookConfig  `yaml:"webhook"          mapstructure:"webhook"`
}

// TelegramConfig holds Telegram bot configuration.
type TelegramConfig struct {
	Token  string `yaml:"token"   mapstructure:"token"`
	ChatID int64  `yaml:"chat_id" mapstructure:"chat_id"`
}

// SlackConfig holds Slack webhook configuration.
type SlackConfig struct {
	WebhookURL string `yaml:"webhook_url" mapstructure:"webhook_url"`
}

// EmailConfig holds SMTP email configuration.
type EmailConfig struct {
	Host     string   `yaml:"host"     mapstructure:"host"`
	Port     int      `yaml:"port"     mapstructure:"port"`
	Username string   `yaml:"username" mapstructure:"username"`
	Password string   `yaml:"password" mapstructure:"password"`
	From     string   `yaml:"from"     mapstructure:"from"`
	To       []string `yaml:"to"       mapstructure:"to"`
}

// WebhookConfig holds generic webhook configuration.
type WebhookConfig struct {
	URL     string            `yaml:"url"     mapstructure:"url"`
	Headers map[string]string `yaml:"headers" mapstructure:"headers"`
}

// RetentionConfig defines how long objects are retained in source and destinations.
type RetentionConfig struct {
	SourceDays      int `yaml:"source_days"      mapstructure:"source_days"`
	DestinationDays int `yaml:"destination_days" mapstructure:"destination_days"`
}

// DefaultConfig returns a Config with safe defaults.
func DefaultConfig() *Config {
	return &Config{
		Workers:      5,
		IntervalSecs: 300,
		MetadataDB:   "replicator.db",
		MetricsPort:  2112,
		LogLevel:     "info",
		Alerts: AlertsConfig{
			ErrorThreshold: 5,
			CooldownMins:   15,
		},
	}
}

// Interval returns the sync interval as a time.Duration.
func (c *Config) Interval() time.Duration {
	return time.Duration(c.IntervalSecs) * time.Second
}
