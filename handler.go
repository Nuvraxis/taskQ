package taskq

import "context"

// Handler processes a single Task[T]. Returning a non-nil error causes the
// worker pool to Nack the task for redelivery, subject to MaxRetry.
type Handler[T any] func(ctx context.Context, task Task[T]) error
