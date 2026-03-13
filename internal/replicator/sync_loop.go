package replicator

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"github.com/htb/htb-replicator/internal/alerts"
	"github.com/htb/htb-replicator/internal/destinations"
	"github.com/htb/htb-replicator/internal/metadata"
	"github.com/htb/htb-replicator/internal/worker"
	"github.com/htb/htb-replicator/metrics"
	"github.com/htb/htb-replicator/pkg/backoff"
	"github.com/htb/htb-replicator/pkg/checksum"
)

// RunSyncCycle performs one full source→destination replication pass.
// It lists all objects from the source, submits work for any that need
// syncing, then enforces retention.
func (r *Replicator) RunSyncCycle(ctx context.Context) error {
	start := time.Now()

	runID, err := r.store.CreateSyncRun(ctx, start)
	if err != nil {
		r.logger.Warn("failed to create sync run record", zap.Error(err))
	}

	r.logger.Info("starting sync cycle")

	objects, err := r.source.ListAll(ctx)
	if err != nil {
		errMsg := fmt.Sprintf("list source objects: %v", err)
		if runID > 0 {
			_ = r.store.FinishSyncRun(ctx, runID, 0, 0, "failed", errMsg)
		}
		metrics.SyncCyclesTotal.WithLabelValues("failed").Inc()
		return fmt.Errorf("sync cycle: %w", err)
	}

	metrics.ObjectsListed.Set(float64(len(objects)))
	r.logger.Info("source listing complete", zap.Int("objects", len(objects)))

	tasksSubmitted := 0

	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("sync cycle cancelled: %w", err)
		}

		normalizedETag := checksum.NormalizeETag(obj.ETag)
		needsSync := false

		existing, err := r.store.GetObject(ctx, obj.Key)
		if err != nil {
			r.logger.Warn("failed to get object metadata, will sync",
				zap.String("key", obj.Key), zap.Error(err))
			needsSync = true
		} else if existing == nil {
			// New object, never seen before.
			needsSync = true
		} else if checksum.NormalizeETag(existing.ETag) != normalizedETag {
			// ETag changed → object was modified.
			needsSync = true
		} else {
			// ETag is the same; check if all destinations are already synced.
			allSynced := true
			for _, dest := range r.destinations {
				rec, err := r.store.GetDestinationStatus(ctx, obj.Key, dest.ID())
				if err != nil || rec == nil || rec.Status != metadata.StatusSynced {
					allSynced = false
					break
				}
			}
			needsSync = !allSynced
		}

		if !needsSync {
			continue
		}

		// Mark object as pending in the store.
		if err := r.store.UpsertObject(ctx, metadata.ObjectRecord{
			ObjectKey:    obj.Key,
			ETag:         obj.ETag,
			Size:         obj.Size,
			LastModified: obj.LastModified,
			SyncStatus:   metadata.StatusPending,
		}); err != nil {
			r.logger.Warn("failed to upsert object record",
				zap.String("key", obj.Key), zap.Error(err))
		}

		task := worker.Task{
			ObjectKey: obj.Key,
			ETag:      obj.ETag,
			Size:      obj.Size,
		}

		if err := r.pool.Submit(ctx, task); err != nil {
			r.logger.Error("failed to submit task",
				zap.String("key", obj.Key), zap.Error(err))
			continue
		}
		tasksSubmitted++
	}

	r.logger.Info("all tasks submitted, enforcing retention",
		zap.Int("tasks", tasksSubmitted))

	if err := r.retention.Enforce(ctx); err != nil {
		r.logger.Error("retention enforcement error", zap.Error(err))
	}

	duration := time.Since(start)
	metrics.SyncCycleDuration.Observe(duration.Seconds())
	metrics.SyncCyclesTotal.WithLabelValues("completed").Inc()

	if runID > 0 {
		_ = r.store.FinishSyncRun(ctx, runID, len(objects), tasksSubmitted, "completed", "")
	}

	r.logger.Info("sync cycle complete",
		zap.Int("listed", len(objects)),
		zap.Int("submitted", tasksSubmitted),
		zap.Duration("duration", duration),
	)
	return nil
}

// executeTask handles the replication of a single object to all destinations.
// Each destination is attempted independently with exponential backoff.
// NOTE: the object is downloaded separately for each destination — not shared
// via TeeReader — to avoid blocking slow destinations from starving fast ones.
func (r *Replicator) executeTask(ctx context.Context, task worker.Task) {
	logger := r.logger.With(
		zap.String("key", task.ObjectKey),
		zap.String("etag", task.ETag),
	)
	logger.Debug("executing task")

	allSynced := true

	for _, dest := range r.destinations {
		destLogger := logger.With(zap.String("destination", dest.ID()))

		if err := r.syncToDestination(ctx, task, dest, destLogger); err != nil {
			allSynced = false
			destLogger.Error("destination sync failed", zap.Error(err))

			// Increment per-destination error counter and alert if threshold exceeded.
			r.errorCounters[dest.ID()]++
			if r.cfg.Alerts.ErrorThreshold > 0 &&
				r.errorCounters[dest.ID()] >= r.cfg.Alerts.ErrorThreshold {
				_ = r.alerter.Send(ctx, alerts.Alert{
					Level:       "error",
					Destination: dest.ID(),
					Message:     fmt.Sprintf("replication failure threshold reached (%d errors)", r.errorCounters[dest.ID()]),
					ObjectKey:   task.ObjectKey,
					Error:       err,
				})
				r.errorCounters[dest.ID()] = 0 // Reset after alerting.
			}
		} else {
			// Reset error counter on success.
			r.errorCounters[dest.ID()] = 0
		}
	}

	if allSynced {
		if err := r.store.MarkObjectSynced(ctx, task.ObjectKey); err != nil {
			logger.Warn("failed to mark object synced", zap.Error(err))
		}
	} else {
		existing, _ := r.store.GetObject(ctx, task.ObjectKey)
		retryCount := 0
		if existing != nil {
			retryCount = existing.RetryCount + 1
		}
		if err := r.store.MarkObjectFailed(ctx, task.ObjectKey, retryCount); err != nil {
			logger.Warn("failed to mark object failed", zap.Error(err))
		}
	}
}

// syncToDestination replicates one object to one destination with retry logic.
func (r *Replicator) syncToDestination(
	ctx context.Context,
	task worker.Task,
	dest destinations.Destination,
	logger *zap.Logger,
) error {
	boCfg := backoff.DefaultConfig()

	var lastErr error
	err := backoff.RetryWithNotify(ctx, boCfg, func() error {
		// Mark this attempt in the store.
		now := time.Now()
		_ = r.store.UpsertDestinationStatus(ctx, metadata.DestinationRecord{
			ObjectKey:     task.ObjectKey,
			DestinationID: dest.ID(),
			Status:        metadata.StatusSyncing,
			LastAttemptAt: &now,
		})

		// Check whether the destination already has the correct version.
		existingETag, exists, err := dest.Exists(ctx, task.ObjectKey)
		if err != nil {
			return fmt.Errorf("exists check: %w", err)
		}

		if exists {
			srcETag := checksum.NormalizeETag(task.ETag)
			dstETag := checksum.NormalizeETag(existingETag)

			if srcETag == dstETag || checksum.IsMultipartETag(srcETag) {
				// Already in sync; mark synced without re-transferring.
				logger.Debug("destination already up-to-date, skipping transfer")
				if err := r.store.MarkDestinationSynced(ctx, task.ObjectKey, dest.ID(), existingETag, 0); err != nil {
					logger.Warn("failed to mark destination synced", zap.Error(err))
				}
				metrics.ObjectsTotal.WithLabelValues(dest.ID(), "skipped").Inc()
				return nil
			}
		}

		// Download a fresh copy of the object for this destination.
		body, size, err := r.source.GetObject(ctx, task.ObjectKey)
		if err != nil {
			return fmt.Errorf("get source object: %w", err)
		}
		defer body.Close()

		obj := destinations.Object{
			Key:          task.ObjectKey,
			ETag:         task.ETag,
			Size:         size,
			LastModified: time.Now(),
		}

		result, err := dest.Write(ctx, obj, body)
		if err != nil {
			return fmt.Errorf("write to destination: %w", err)
		}

		// ETag verification (skip for multipart ETags as MD5 does not apply).
		srcETag := checksum.NormalizeETag(task.ETag)
		if !checksum.IsMultipartETag(srcETag) && result.ETag != "" {
			dstETag := checksum.NormalizeETag(result.ETag)
			if srcETag != dstETag {
				// Non-fatal warning: S3 multipart and content-encoding can shift ETags.
				logger.Warn("ETag mismatch after write",
					zap.String("src_etag", srcETag),
					zap.String("dst_etag", dstETag),
				)
			}
		}

		if err := r.store.MarkDestinationSynced(ctx, task.ObjectKey, dest.ID(), result.ETag, result.BytesWritten); err != nil {
			logger.Warn("failed to mark destination synced", zap.Error(err))
		}

		metrics.ObjectsTotal.WithLabelValues(dest.ID(), "synced").Inc()
		metrics.BytesTotal.WithLabelValues(dest.ID()).Add(float64(result.BytesWritten))

		logger.Info("object synced",
			zap.Int64("bytes", result.BytesWritten),
			zap.String("dest_etag", result.ETag),
		)
		return nil

	}, func(err error, wait time.Duration) {
		lastErr = err
		logger.Warn("retrying after error",
			zap.Error(err),
			zap.Duration("wait", wait),
		)
		retryCount := 1
		_ = r.store.MarkDestinationFailed(ctx, task.ObjectKey, dest.ID(), err.Error(), retryCount)
		metrics.ErrorsTotal.WithLabelValues(dest.ID(), "retry").Inc()
	})

	if err != nil {
		_ = r.store.MarkDestinationFailed(ctx, task.ObjectKey, dest.ID(), err.Error(), 0)
		metrics.ErrorsTotal.WithLabelValues(dest.ID(), "permanent").Inc()
		if lastErr != nil {
			return lastErr
		}
		return err
	}

	return nil
}
