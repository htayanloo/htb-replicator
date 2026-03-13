package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var syncOnceCmd = &cobra.Command{
	Use:   "sync-once",
	Short: "Run a single sync cycle and exit",
	Long: `Performs exactly one full source→destination replication pass.
Exits with code 0 if all objects were synced successfully,
or exits with code 1 if any failures were encountered.`,
	RunE: runSyncOnce,
}

func runSyncOnce(cmd *cobra.Command, args []string) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	ctx := context.Background()

	rep, err := buildReplicator(ctx)
	if err != nil {
		return fmt.Errorf("build replicator: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30_000_000_000) // 30s
		defer cancel()
		_ = rep.Shutdown(shutdownCtx)
	}()

	logger.Info("running single sync cycle")

	if err := rep.RunSyncCycle(ctx); err != nil {
		logger.Error("sync cycle failed", zap.Error(err))
		os.Exit(1)
	}

	// Check metadata store for any failures.
	stats, err := rep.Store().GetStats(ctx)
	if err != nil {
		logger.Warn("failed to read stats", zap.Error(err))
	} else {
		logger.Info("sync-once complete",
			zap.Int64("total", stats.TotalObjects),
			zap.Int64("synced", stats.SyncedObjects),
			zap.Int64("failed", stats.FailedObjects),
			zap.Int64("pending", stats.PendingObjects),
		)

		if stats.FailedObjects > 0 || stats.FailedDestStatuses > 0 {
			fmt.Fprintf(os.Stderr, "sync-once: %d object(s) and %d destination status(es) in failed state\n",
				stats.FailedObjects, stats.FailedDestStatuses)
			os.Exit(1)
		}
	}

	fmt.Println("sync-once: completed successfully")
	return nil
}
