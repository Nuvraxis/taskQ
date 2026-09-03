package taskq

// EnqueueOption configures a single Enqueue call.
type EnqueueOption func(*enqueueConfig)

type enqueueConfig struct {
	maxRetry int
}

// WithMaxRetry overrides, for a single Enqueue call, the maximum number of
// redelivery attempts before the worker pool gives up on a task.
func WithMaxRetry(n int) EnqueueOption {
	return func(c *enqueueConfig) {
		c.maxRetry = n
	}
}

// QueueOption configures a Queue[T] at construction time; these become
// defaults that EnqueueOption can override per call.
type QueueOption func(*queueConfig)

type queueConfig struct {
	defaultMaxRetry int
}

// WithDefaultMaxRetry sets the default MaxRetry applied to every task
// enqueued through this Queue, unless overridden per call with WithMaxRetry.
func WithDefaultMaxRetry(n int) QueueOption {
	return func(c *queueConfig) {
		c.defaultMaxRetry = n
	}
}
