package taskq

type EnqueueOption func(*enqueueConfig)

type enqueueConfig struct {
	maxRetry int
}

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

func WithDefaultMaxRetry(n int) QueueOption {
	return func(c *queueConfig) {
		c.defaultMaxRetry = n
	}
}
