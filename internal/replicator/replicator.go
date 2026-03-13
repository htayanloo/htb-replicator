package replicator

import (
	"context"

	"go.uber.org/zap"
	"github.com/htb/htb-replicator/config"
	"github.com/htb/htb-replicator/internal/alerts"
	"github.com/htb/htb-replicator/internal/destinations"
	"github.com/htb/htb-replicator/internal/metadata"
	"github.com/htb/htb-replicator/internal/retention"
	"github.com/htb/htb-replicator/internal/source"
	"github.com/htb/htb-replicator/internal/worker"
)

// Replicator orchestrates the full replication lifecycle: listing,
// diffing, transferring, alerting, and retention enforcement.
type Replicator struct {
	cfg          *config.Config
	source       source.Source
	destinations []destinations.Destination
	store        metadata.Store
	pool         worker.Pool
	alerter      alerts.Alerter
	retention    *retention.Policy
	logger       *zap.Logger

	// errorCounters tracks per-destination consecutive error counts for alerting.
	errorCounters map[string]int
}

// New constructs a Replicator with all its dependencies wired up.
func New(
	cfg *config.Config,
	source source.Source,
	dests []destinations.Destination,
	store metadata.Store,
	alerter alerts.Alerter,
	logger *zap.Logger,
) *Replicator {
	r := &Replicator{
		cfg:           cfg,
		source:        source,
		destinations:  dests,
		store:         store,
		alerter:       alerter,
		logger:        logger,
		errorCounters: make(map[string]int),
	}

	r.retention = &retention.Policy{
		SourceDays:      cfg.Retention.SourceDays,
		DestinationDays: cfg.Retention.DestinationDays,
		Source:          source,
		Destinations:    dests,
		Store:           store,
		Logger:          logger,
	}

	// Build the worker pool with the executeTask handler.
	r.pool = worker.NewPool(cfg.Workers, func(ctx context.Context, task worker.Task) {
		r.executeTask(ctx, task)
	})

	return r
}

// Shutdown gracefully drains the worker pool and closes all resources.
func (r *Replicator) Shutdown(ctx context.Context) error {
	r.logger.Info("shutting down replicator")

	if err := r.pool.Shutdown(ctx); err != nil {
		r.logger.Error("worker pool shutdown error", zap.Error(err))
	}

	if err := r.source.Close(); err != nil {
		r.logger.Error("source close error", zap.Error(err))
	}

	for _, dest := range r.destinations {
		if err := dest.Close(); err != nil {
			r.logger.Error("destination close error",
				zap.String("destination", dest.ID()),
				zap.Error(err),
			)
		}
	}

	if err := r.alerter.Close(); err != nil {
		r.logger.Error("alerter close error", zap.Error(err))
	}

	if err := r.store.Close(); err != nil {
		r.logger.Error("metadata store close error", zap.Error(err))
	}

	r.logger.Info("replicator shutdown complete")
	return nil
}

// Destinations returns the slice of configured destinations.
func (r *Replicator) Destinations() []destinations.Destination {
	return r.destinations
}

// Store returns the metadata store reference.
func (r *Replicator) Store() metadata.Store {
	return r.store
}
