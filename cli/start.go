package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/htb/htb-replicator/internal/alerts"
	"github.com/htb/htb-replicator/internal/destfactory"
	"github.com/htb/htb-replicator/internal/destinations"
	"github.com/htb/htb-replicator/internal/metadata"
	"github.com/htb/htb-replicator/internal/replicator"
	"github.com/htb/htb-replicator/internal/sourcefactory"
	"github.com/htb/htb-replicator/metrics"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the continuous replication service",
	Long: `Starts the replication service which continuously polls the source
S3 bucket and replicates objects to all destinations.

Scheduling modes:
  schedule: "0 2 * * *"   — cron expression (runs at fixed calendar times)
  interval_seconds: 300   — fixed interval ticker (fallback when schedule is empty)

Handles SIGTERM/SIGINT for graceful shutdown.`,
	RunE: runStart,
}

func runStart(cmd *cobra.Command, args []string) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	printBanner()

	scheduleMode := "interval"
	if cfg.Schedule != "" {
		scheduleMode = "cron"
	}

	logger.Info("starting HTB-Replicator",
		zap.Int("workers", cfg.Workers),
		zap.String("schedule_mode", scheduleMode),
		zap.String("schedule", cfg.Schedule),
		zap.Duration("interval", cfg.Interval()),
		zap.String("metadata_db", cfg.MetadataDB),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wire up all dependencies.
	rep, err := buildReplicator(ctx)
	if err != nil {
		return fmt.Errorf("build replicator: %w", err)
	}

	// Start Prometheus metrics server.
	if cfg.MetricsPort > 0 {
		metrics.StartServer(cfg.MetricsPort, logger)
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
		cancel()
	}()

	// runCycle executes a single sync cycle with a per-cycle timeout.
	runCycle := func() {
		// Give each cycle at most the configured interval (or 1h for cron mode).
		timeout := cfg.Interval()
		if cfg.Schedule != "" || timeout <= 0 {
			timeout = time.Hour
		}
		cycleCtx, cycleCancel := context.WithTimeout(ctx, timeout)
		defer cycleCancel()

		if err := rep.RunSyncCycle(cycleCtx); err != nil {
			logger.Error("sync cycle error", zap.Error(err))
		}
	}

	// Run the first cycle immediately at startup.
	runCycle()

	if cfg.Schedule != "" {
		// ── Cron-based scheduling ──────────────────────────────────────────────
		return runWithCron(ctx, rep, runCycle)
	}

	// ── Interval-based scheduling (ticker) ────────────────────────────────────
	return runWithTicker(ctx, rep, runCycle)
}

// runWithCron uses robfig/cron to fire runCycle on a cron schedule.
func runWithCron(ctx context.Context, rep *replicator.Replicator, runCycle func()) error {
	// Standard 5-field cron (minute hour dom month dow) + descriptors.
	// Must match the parser used in config.Validate().
	c := cron.New()

	entryID, err := c.AddFunc(cfg.Schedule, func() {
		if ctx.Err() != nil {
			return // context cancelled — skip
		}
		runCycle()
	})
	if err != nil {
		return fmt.Errorf("invalid cron schedule %q: %w", cfg.Schedule, err)
	}

	c.Start()
	logger.Info("cron scheduler started",
		zap.String("schedule", cfg.Schedule),
		zap.Any("entry_id", entryID),
	)

	<-ctx.Done() // wait for shutdown signal

	logger.Info("stopping cron scheduler")
	stopCtx := c.Stop() // returns context that completes when running jobs finish
	<-stopCtx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer shutdownCancel()
	return rep.Shutdown(shutdownCtx)
}

// runWithTicker uses a time.Ticker for fixed-interval scheduling.
func runWithTicker(ctx context.Context, rep *replicator.Replicator, runCycle func()) error {
	ticker := time.NewTicker(cfg.Interval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("context done, initiating graceful shutdown")
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer shutdownCancel()
			return rep.Shutdown(shutdownCtx)

		case <-ticker.C:
			runCycle()
		}
	}
}

// buildReplicator wires up all dependencies and returns a ready Replicator.
func buildReplicator(ctx context.Context) (*replicator.Replicator, error) {
	// Metadata store.
	store, err := metadata.NewSQLiteStore(ctx, cfg.MetadataDB)
	if err != nil {
		return nil, fmt.Errorf("open metadata store: %w", err)
	}

	// Source client (type determined by cfg.Source.Type).
	src, err := sourcefactory.New(cfg.Source)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("create source client: %w", err)
	}

	// Destinations.
	var dests []destinations.Destination
	for _, dcfg := range cfg.Destinations {
		d, err := destfactory.New(dcfg)
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("create destination %q: %w", dcfg.ID, err)
		}
		// Verify connectivity at startup.
		if pingErr := d.Ping(ctx); pingErr != nil {
			logger.Warn("destination ping failed (will retry during sync)",
				zap.String("destination", dcfg.ID),
				zap.Error(pingErr),
			)
		}
		dests = append(dests, d)
	}

	// Alerter.
	var alerter alerts.Alerter
	alerter, err = alerts.NewMultiAlerter(cfg.Alerts, logger)
	if err != nil {
		logger.Warn("failed to build alerter, using noop", zap.Error(err))
		alerter = alerts.NewNoopAlerter()
	}

	return replicator.New(cfg, src, dests, store, alerter, logger), nil
}

// printBanner prints a coloured ASCII banner when stdout is a terminal.
func printBanner() {
	if !isTTY(os.Stdout) {
		return
	}

	const (
		cyan   = "\033[36m"
		green  = "\033[32m"
		yellow = "\033[33m"
		bold   = "\033[1m"
		reset  = "\033[0m"
	)

	fmt.Print(cyan + bold + `
  ██╗  ██╗████████╗██████╗       ██████╗ ███████╗██████╗ ██╗     ██╗ ██████╗ █████╗ ████████╗ ██████╗ ██████╗
  ██║  ██║╚══██╔══╝██╔══██╗      ██╔══██╗██╔════╝██╔══██╗██║     ██║██╔════╝██╔══██╗╚══██╔══╝██╔═══██╗██╔══██╗
  ███████║   ██║   ██████╔╝█████╗██████╔╝█████╗  ██████╔╝██║     ██║██║     ███████║   ██║   ██║   ██║██████╔╝
  ██╔══██║   ██║   ██╔══██╗╚════╝██╔══██╗██╔══╝  ██╔═══╝ ██║     ██║██║     ██╔══██║   ██║   ██║   ██║██╔══██╗
  ██║  ██║   ██║   ██████╔╝      ██║  ██║███████╗██║     ███████╗██║╚██████╗██║  ██║   ██║   ╚██████╔╝██║  ██║
  ╚═╝  ╚═╝   ╚═╝   ╚═════╝       ╚═╝  ╚═╝╚══════╝╚═╝     ╚══════╝╚═╝ ╚═════╝╚═╝  ╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝
` + reset)

	fmt.Printf("  %s%s HTB-Replicator%s  —  multi-source replication engine\n", bold, green, reset)
	fmt.Printf("  %ssource: %-8s  workers: %-3d  destinations: %d%s\n\n",
		yellow,
		cfg.Source.Type,
		cfg.Workers,
		len(cfg.Destinations),
		reset,
	)
}
