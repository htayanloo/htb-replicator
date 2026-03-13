package worker

// Task represents a single object replication job submitted to the worker pool.
type Task struct {
	// ObjectKey is the S3 key of the object to replicate.
	ObjectKey string

	// ETag is the source ETag at the time the task was created.
	ETag string

	// Size is the object size in bytes at the time the task was created.
	Size int64
}
