package cli

import (
	"context"
	"fmt"
	"text/tabwriter"
	"os"

	"github.com/spf13/cobra"
	"github.com/htb/htb-replicator/internal/metadata"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show replication status from the metadata store",
	Long:  `Queries the metadata database and prints a table of object counts by sync status.`,
	RunE:  runStatus,
}

func runStatus(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	store, err := metadata.NewSQLiteStore(ctx, cfg.MetadataDB)
	if err != nil {
		return fmt.Errorf("open metadata store: %w", err)
	}
	defer store.Close()

	stats, err := store.GetStats(ctx)
	if err != nil {
		return fmt.Errorf("get stats: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	fmt.Fprintln(w, "── Object Status ────────────────────────────")
	fmt.Fprintf(w, "  %-20s\t%d\n", "Total Objects", stats.TotalObjects)
	fmt.Fprintf(w, "  %-20s\t%d\n", "Pending", stats.PendingObjects)
	fmt.Fprintf(w, "  %-20s\t%d\n", "Syncing", stats.SyncingObjects)
	fmt.Fprintf(w, "  %-20s\t%d\n", "Synced", stats.SyncedObjects)
	fmt.Fprintf(w, "  %-20s\t%d\n", "Failed", stats.FailedObjects)
	fmt.Fprintf(w, "  %-20s\t%d\n", "Retrying", stats.RetryingObjects)

	fmt.Fprintln(w, "\n── Destination Status ───────────────────────")
	fmt.Fprintf(w, "  %-20s\t%d\n", "Total", stats.TotalDestStatuses)
	fmt.Fprintf(w, "  %-20s\t%d\n", "Synced", stats.SyncedDestStatuses)
	fmt.Fprintf(w, "  %-20s\t%d\n", "Failed", stats.FailedDestStatuses)

	_ = w.Flush()

	if stats.FailedObjects > 0 || stats.FailedDestStatuses > 0 {
		fmt.Fprintf(os.Stderr, "\nwarning: %d object(s) in failed state, %d destination record(s) in failed state\n",
			stats.FailedObjects, stats.FailedDestStatuses)
	}

	return nil
}
