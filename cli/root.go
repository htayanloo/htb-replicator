package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"github.com/htb/htb-replicator/config"
)

// isTTY returns true when f is connected to an interactive terminal.
func isTTY(f *os.File) bool {
	stat, err := f.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

var (
	cfgFile string
	logger  *zap.Logger
	cfg     *config.Config
)

// rootCmd is the top-level Cobra command.
var rootCmd = &cobra.Command{
	Use:   "replicator",
	Short: "HTB-Replicator — multi-source replication engine",
	Long: `HTB-Replicator replicates objects from an S3-compatible source bucket
to one or more destinations (local, S3, FTP, SFTP) with full observability,
alerting, and retention enforcement.`,
	SilenceUsage:  true,
	SilenceErrors: true,
}

// Execute is the entry point called from main.go.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "config.yaml",
		"path to the YAML configuration file")
	rootCmd.PersistentFlags().String("log-level", "",
		"log verbosity: debug, info, warn, error (overrides config)")

	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))

	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(syncOnceCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(verifyCmd)
	rootCmd.AddCommand(serviceCmd)
	rootCmd.AddCommand(healthCmd)
	rootCmd.AddCommand(logsCmd)
}

// initConfig loads the YAML configuration via viper and initialises the logger.
func initConfig() {
	defaults := config.DefaultConfig()

	viper.SetDefault("workers", defaults.Workers)
	viper.SetDefault("interval_seconds", defaults.IntervalSecs)
	viper.SetDefault("metadata_db", defaults.MetadataDB)
	viper.SetDefault("metrics_port", defaults.MetricsPort)
	viper.SetDefault("log_level", defaults.LogLevel)
	viper.SetDefault("alerts.error_threshold", defaults.Alerts.ErrorThreshold)
	viper.SetDefault("alerts.cooldown_minutes", defaults.Alerts.CooldownMins)

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
	}

	viper.SetEnvPrefix("REPLICATOR")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not read config file %q: %v\n", cfgFile, err)
	}

	cfg = &config.Config{}
	if err := viper.Unmarshal(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: failed to parse config: %v\n", err)
		os.Exit(1)
	}

	logger = buildLogger(cfg.LogLevel)
}

// buildLogger constructs a zap.Logger.
//
//   - When stdout is an interactive terminal (TTY): coloured, human-readable
//     console format.
//   - Otherwise (piped to file, systemd, Docker): plain JSON for log
//     aggregators.
func buildLogger(level string) *zap.Logger {
	var zapLevel zapcore.Level
	switch strings.ToLower(level) {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	if isTTY(os.Stdout) {
		return buildConsoleLogger(zapLevel)
	}
	return buildJSONLogger(zapLevel)
}

// buildConsoleLogger returns a human-readable, coloured logger for TTY output.
func buildConsoleLogger(level zapcore.Level) *zap.Logger {
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "",   // omit caller in console mode
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder, // coloured INFO / WARN / ERROR
		EncodeTime:     zapcore.TimeEncoderOfLayout("15:04:05"),
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderCfg),
		zapcore.AddSync(os.Stdout),
		zap.NewAtomicLevelAt(level),
	)
	return zap.New(core)
}

// buildJSONLogger returns a structured JSON logger for non-TTY output.
func buildJSONLogger(level zapcore.Level) *zap.Logger {
	zapCfg := zap.NewProductionConfig()
	zapCfg.Level = zap.NewAtomicLevelAt(level)
	zapCfg.EncoderConfig.TimeKey = "ts"
	zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	l, err := zapCfg.Build()
	if err != nil {
		return zap.NewNop()
	}
	return l
}
