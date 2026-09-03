package taskq

import "context"

type Handler[T any] func(ctx context.Context, task Task[T]) error
