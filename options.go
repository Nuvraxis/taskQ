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

// PoolOption configures a Pool[T] at construction time.
type PoolOption func(*poolConfig)

type poolConfig struct {
	concurrency int
	backoff     BackoffStrategy
}

// WithConcurrency sets the number of worker goroutines a Pool runs
// concurrently. Values less than 1 are treated as 1.
func WithConcurrency(n int) PoolOption {
	return func(c *poolConfig) {
		if n < 1 {
			n = 1
		}
		c.concurrency = n
	}
}

// WithBackoffStrategy overrides the default backoff strategy used to space
// out redeliveries after a failed Handler call.
func WithBackoffStrategy(s BackoffStrategy) PoolOption {
	return func(c *poolConfig) { c.backoff = s }
}
