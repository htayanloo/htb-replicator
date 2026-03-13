package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"github.com/htb/htb-replicator/internal/destfactory"
	"github.com/htb/htb-replicator/internal/destinations"
	"github.com/htb/htb-replicator/internal/metadata"
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify that destinations match the metadata store",
	Long: `For each configured destination, lists its actual contents and compares
against the set of synced objects recorded in the metadata store.
Prints any discrepancies and exits with code 1 if any are found.`,
	RunE: runVerify,
}

func runVerify(cmd *cobra.Command, args []string) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	ctx := context.Background()

	store, err := metadata.NewSQLiteStore(ctx, cfg.MetadataDB)
	if err != nil {
		return fmt.Errorf("open metadata store: %w", err)
	}
	defer store.Close()

	// Build all configured destinations.
	var dests []destinations.Destination
	for _, dcfg := range cfg.Destinations {
		d, err := destfactory.New(dcfg)
		if err != nil {
			return fmt.Errorf("create destination %q: %w", dcfg.ID, err)
		}
		dests = append(dests, d)
	}
	defer func() {
		for _, d := range dests {
			_ = d.Close()
		}
	}()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	totalDiscrepancies := 0

	for _, dest := range dests {
		logger.Info("verifying destination", zap.String("destination", dest.ID()))

		// Objects the metadata store believes are synced for this destination.
		syncedKeys, err := store.ListSyncedKeysForDestination(ctx, dest.ID())
		if err != nil {
			logger.Error("failed to list synced keys", zap.String("destination", dest.ID()), zap.Error(err))
			continue
		}
		syncedSet := make(map[string]bool, len(syncedKeys))
		for _, k := range syncedKeys {
			syncedSet[k] = true
		}

		// Objects actually present on the destination.
		actualKeys, err := dest.ListKeys(ctx)
		if err != nil {
			logger.Error("failed to list destination keys", zap.String("destination", dest.ID()), zap.Error(err))
			continue
		}
		actualSet := make(map[string]bool, len(actualKeys))
		for _, k := range actualKeys {
			actualSet[k] = true
		}

		// Find objects in metadata store but missing from destination.
		var missing []string
		for k := range syncedSet {
			if !actualSet[k] {
				missing = append(missing, k)
			}
		}

		// Find objects on destination not tracked in metadata store.
		var extra []string
		for k := range actualSet {
			if !syncedSet[k] {
				extra = append(extra, k)
			}
		}

		fmt.Fprintf(w, "\n── Destination: %s ─────────────────────────────\n", dest.ID())
		fmt.Fprintf(w, "  %-30s\t%d\n", "Synced (metadata)", len(syncedKeys))
		fmt.Fprintf(w, "  %-30s\t%d\n", "Actual (destination)", len(actualKeys))
		fmt.Fprintf(w, "  %-30s\t%d\n", "Missing from destination", len(missing))
		fmt.Fprintf(w, "  %-30s\t%d\n", "Extra (not in metadata)", len(extra))

		if len(missing) > 0 {
			fmt.Fprintln(w, "\n  Missing objects (in metadata, not on destination):")
			for _, k := range missing {
				fmt.Fprintf(w, "    - %s\n", k)
			}
		}
		if len(extra) > 0 {
			fmt.Fprintln(w, "\n  Extra objects (on destination, not in metadata):")
			for _, k := range extra {
				fmt.Fprintf(w, "    + %s\n", k)
			}
		}

		totalDiscrepancies += len(missing) + len(extra)
	}

	_ = w.Flush()

	if totalDiscrepancies > 0 {
		fmt.Fprintf(os.Stderr, "\nverify: found %d discrepancy(ies) across all destinations\n", totalDiscrepancies)
		os.Exit(1)
	}

	fmt.Println("\nverify: all destinations match metadata store")
	return nil
}
