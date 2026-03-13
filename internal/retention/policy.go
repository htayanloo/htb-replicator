package retention

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"github.com/htb/htb-replicator/internal/destinations"
	"github.com/htb/htb-replicator/internal/metadata"
	"github.com/htb/htb-replicator/internal/source"
)

// Policy enforces time-based retention rules on source and destination objects.
type Policy struct {
	// SourceDays is the number of days to retain objects in the source.
	// Zero means no source deletion.
	SourceDays int

	// DestinationDays is the number of days to retain objects in destinations.
	// Zero means no destination deletion.
	DestinationDays int

	Source       source.Source
	Destinations []destinations.Destination
	Store        metadata.Store
	Logger       *zap.Logger
}

// Enforce applies retention rules:
//  1. Lists all synced objects from the metadata store.
//  2. Deletes from the source if last_modified is older than SourceDays.
//  3. Deletes from each destination if synced_at is older than DestinationDays.
//  4. Removes the metadata record when all copies have been deleted.
func (p *Policy) Enforce(ctx context.Context) error {
	if p.SourceDays == 0 && p.DestinationDays == 0 {
		p.Logger.Debug("retention policy disabled, skipping")
		return nil
	}

	p.Logger.Info("running retention policy",
		zap.Int("source_days", p.SourceDays),
		zap.Int("destination_days", p.DestinationDays),
	)

	// Fetch all synced objects for retention evaluation.
	synced, err := p.Store.ListObjectsByStatus(ctx, metadata.StatusSynced, 100_000)
	if err != nil {
		return fmt.Errorf("retention: list synced objects: %w", err)
	}

	now := time.Now().UTC()
	var errs []error

	for _, obj := range synced {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("retention: context cancelled: %w", err)
		}

		// ── Source retention ──────────────────────────────────────────────
		if p.SourceDays > 0 {
			cutoff := now.AddDate(0, 0, -p.SourceDays)
			if obj.LastModified.Before(cutoff) {
				p.Logger.Info("deleting from source (retention)",
					zap.String("key", obj.ObjectKey),
					zap.Time("last_modified", obj.LastModified),
				)
				if err := p.Source.DeleteObject(ctx, obj.ObjectKey); err != nil {
					p.Logger.Error("failed to delete from source",
						zap.String("key", obj.ObjectKey),
						zap.Error(err),
					)
					errs = append(errs, fmt.Errorf("source delete %q: %w", obj.ObjectKey, err))
				}
			}
		}

		// ── Destination retention ─────────────────────────────────────────
		if p.DestinationDays > 0 {
			cutoff := now.AddDate(0, 0, -p.DestinationDays)

			for _, dest := range p.Destinations {
				rec, err := p.Store.GetDestinationStatus(ctx, obj.ObjectKey, dest.ID())
				if err != nil {
					errs = append(errs, fmt.Errorf("get dest status %q/%q: %w", obj.ObjectKey, dest.ID(), err))
					continue
				}
				if rec == nil || rec.SyncedAt == nil {
					continue
				}

				if rec.SyncedAt.Before(cutoff) {
					p.Logger.Info("deleting from destination (retention)",
						zap.String("key", obj.ObjectKey),
						zap.String("destination", dest.ID()),
						zap.Time("synced_at", *rec.SyncedAt),
					)
					if err := dest.Delete(ctx, obj.ObjectKey); err != nil {
						p.Logger.Error("failed to delete from destination",
							zap.String("key", obj.ObjectKey),
							zap.String("destination", dest.ID()),
							zap.Error(err),
						)
						errs = append(errs, fmt.Errorf("dest delete %q/%q: %w", obj.ObjectKey, dest.ID(), err))
					}
				}
			}
		}

		// ── Metadata cleanup ─────────────────────────────────────────────
		// If the object has been purged from all destinations and the source
		// retention also applied, remove the metadata record to keep the DB lean.
		if p.SourceDays > 0 && p.DestinationDays > 0 {
			sourceCutoff := now.AddDate(0, 0, -p.SourceDays)
			destCutoff := now.AddDate(0, 0, -p.DestinationDays)

			if obj.LastModified.Before(sourceCutoff) && obj.SyncedAt != nil && obj.SyncedAt.Before(destCutoff) {
				if err := p.Store.DeleteObject(ctx, obj.ObjectKey); err != nil {
					p.Logger.Error("failed to delete metadata record",
						zap.String("key", obj.ObjectKey),
						zap.Error(err),
					)
					errs = append(errs, fmt.Errorf("metadata delete %q: %w", obj.ObjectKey, err))
				}
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("retention policy completed with %d errors (first: %w)", len(errs), errs[0])
	}

	p.Logger.Info("retention policy complete", zap.Int("evaluated", len(synced)))
	return nil
}
