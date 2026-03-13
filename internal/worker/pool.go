package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/htb/htb-replicator/metrics"
)

// Handler is the function signature called by the worker pool for each Task.
type Handler func(ctx context.Context, task Task)

// Pool manages a fixed-size pool of worker goroutines that process Tasks.
type Pool interface {
	// Submit enqueues a task for processing. It blocks when the internal queue
	// is full and returns an error if the context is cancelled before a slot
	// becomes available.
	Submit(ctx context.Context, task Task) error

	// ActiveWorkers returns the number of goroutines currently executing a task.
	ActiveWorkers() int

	// QueueSize returns the number of tasks currently waiting in the queue.
	QueueSize() int

	// Shutdown drains the queue and waits for all in-flight tasks to complete,
	// or until the context deadline is exceeded.
	Shutdown(ctx context.Context) error
}

// workerPool is the concrete implementation of Pool.
type workerPool struct {
	queue   chan Task
	handler Handler
	wg      sync.WaitGroup
	active  atomic.Int64
	once    sync.Once
	done    chan struct{}
}

// NewPool creates a workerPool with the given number of goroutines.
// The internal queue capacity is workers * 4 so that Submit does not block
// until all workers are busy AND the buffer is full.
func NewPool(workers int, handler Handler) Pool {
	if workers <= 0 {
		workers = 1
	}
	capacity := workers * 4
	p := &workerPool{
		queue:   make(chan Task, capacity),
		handler: handler,
		done:    make(chan struct{}),
	}

	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.run()
	}

	return p
}

// run is the main loop for a single worker goroutine.
func (p *workerPool) run() {
	defer p.wg.Done()
	for task := range p.queue {
		p.active.Add(1)
		metrics.WorkersActive.Inc()
		metrics.QueueSize.Dec()

		// Use a background context for the handler so that a single task
		// cancellation does not terminate sibling tasks.
		p.handler(context.Background(), task)

		p.active.Add(-1)
		metrics.WorkersActive.Dec()
	}
}

// Submit enqueues task for processing. Blocks when the queue is full.
// Returns an error if ctx is cancelled before a slot is available,
// or if the pool has been shut down.
func (p *workerPool) Submit(ctx context.Context, task Task) error {
	select {
	case <-p.done:
		return fmt.Errorf("worker pool is shut down")
	default:
	}

	select {
	case p.queue <- task:
		metrics.QueueSize.Inc()
		return nil
	case <-ctx.Done():
		return fmt.Errorf("submit cancelled: %w", ctx.Err())
	case <-p.done:
		return fmt.Errorf("worker pool shut down while waiting to submit")
	}
}

// ActiveWorkers returns the count of goroutines currently executing a task.
func (p *workerPool) ActiveWorkers() int {
	return int(p.active.Load())
}

// QueueSize returns the number of tasks currently buffered in the queue channel.
func (p *workerPool) QueueSize() int {
	return len(p.queue)
}

// Shutdown signals no more tasks will be submitted, closes the queue channel,
// and waits for all in-flight tasks to finish (or for ctx to expire).
func (p *workerPool) Shutdown(ctx context.Context) error {
	var err error
	p.once.Do(func() {
		close(p.done)
		close(p.queue)

		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// All workers finished cleanly.
		case <-ctx.Done():
			err = fmt.Errorf("shutdown timed out waiting for workers: %w", ctx.Err())
		}
	})
	return err
}
